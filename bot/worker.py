# bot/worker.py
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque, deque as Deque

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg
from exec.sltp import compute_sltp, SLTPPlan  # план цен SL/TP (paper)

# мягкий импорт настроек — чтобы smoke работал без падений
try:
    from bot.core.config import get_settings  # type: ignore
except Exception:
    get_settings = None  # type: ignore

logger = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


# --- вспомогательные структуры ---

@dataclass
class SymbolSpec:
    symbol: str
    price_tick: float
    qty_step: float


class MarketHub:
    """Хранит последнюю лучшую пару bid/ask по символу."""
    def __init__(self, symbols: List[str]) -> None:
        self._best: Dict[str, Tuple[float, float]] = {s: (0.0, 0.0) for s in symbols}
        self._lock = asyncio.Lock()

    async def update(self, t: TickerTick) -> None:
        async with self._lock:
            self._best[t.symbol] = (t.bid, t.ask)

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        return self._best.get(symbol, (0.0, 0.0))


@dataclass
class PositionState:
    state: str            # "FLAT" | "ENTERING" | "OPEN" | "EXITING"
    side: Optional[Side]  # BUY/SELL
    qty: float
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    opened_ts_ms: int
    timeout_ms: int       # 3–5 мин


class Worker:
    """
    Оркестратор:
      WS (BinanceWS) -> Coalescer -> MarketHub(best bid/ask) -> Executors(per-symbol, paper)
    Плюс:
      - история тиков (deque per-symbol),
      - FSM позиции per-symbol,
      - SL/TP план + watchdog (таймаут и защита).
    """

    def __init__(
        self,
        symbols: List[str],
        *,
        futures: bool = True,
        coalesce_ms: int = 75,
        history_maxlen: int = 4000,  # ~ несколько минут при активном рынке
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.ws = BinanceWS(self.symbols, futures=futures, mark_interval_1s=True)
        self.coal = Coalescer(coalesce_ms=coalesce_ms)
        self.hub = MarketHub(self.symbols)

        # история тиков (последние N по символу)
        self._hist: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=history_maxlen) for s in self.symbols}
        self._history_maxlen = history_maxlen
        self._hist_lock = asyncio.Lock()

        # спецификации шагов (дефолты для перпов)
        self.specs: Dict[str, SymbolSpec] = {
            "BTCUSDT": SymbolSpec("BTCUSDT", price_tick=0.1,   qty_step=0.001),
            "ETHUSDT": SymbolSpec("ETHUSDT", price_tick=0.01,  qty_step=0.001),
            "SOLUSDT": SymbolSpec("SOLUSDT", price_tick=0.001, qty_step=0.1),
        }
        for s in self.symbols:
            if s not in self.specs:
                self.specs[s] = SymbolSpec(s, price_tick=0.01, qty_step=0.001)

        # per-symbol executors
        self.execs: Dict[str, Executor] = {}

        # FSM + мьютексы
        self._locks: Dict[str, asyncio.Lock] = {s: asyncio.Lock() for s in self.symbols}
        self._pos: Dict[str, PositionState] = {
            s: PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=180_000
            )
            for s in self.symbols
        }

        # таски жизненного цикла
        self._tasks: List[asyncio.Task] = []
        self._wd_task: Optional[asyncio.Task] = None
        self._started = False

    # ---------- lifecycle ----------

    async def flatten(self, symbol: str) -> Dict[str, Any]:
        """Форс-закрыть позицию (paper), сбросить FSM в FLAT."""
        symbol = symbol.upper()
        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "OPEN":
                # уже FLAT
                self._pos[symbol] = PositionState(
                    state="FLAT", side=None, qty=0.0, entry_px=0.0,
                    sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
                )
                return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "no_open_position"}
            await self._paper_close(symbol, pos)
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}

    def set_timeout_ms(self, symbol: str, timeout_ms: int) -> Dict[str, Any]:
        """Поменять таймаут позиции для отладки (например, 20_000 мс)."""
        symbol = symbol.upper()
        self._pos[symbol].timeout_ms = max(5_000, int(timeout_ms))
        return {"ok": True, "symbol": symbol, "timeout_ms": self._pos[symbol].timeout_ms}

    
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        def _best(sym: str) -> Tuple[float, float]:
            return self.hub.best_bid_ask(sym)

        # конфиг экзекьютора из settings (если есть)
        limit_offset_ticks = 1
        limit_timeout_ms = 500
        time_in_force = "GTC"
        max_slippage_bp = 6.0

        if get_settings:
            try:
                s = get_settings()
                limit_offset_ticks = int(getattr(s.execution, "limit_offset_ticks", limit_offset_ticks))
                limit_timeout_ms = int(getattr(s.execution, "limit_timeout_ms", limit_timeout_ms))
                max_slippage_bp = float(getattr(s.execution, "max_slippage_bp", max_slippage_bp))
                time_in_force = str(getattr(s.execution, "time_in_force", time_in_force))
            except Exception:
                logger.debug("settings not available, use defaults")

        for sym in self.symbols:
            spec = self.specs[sym]
            cfg = ExecCfg(
                price_tick=spec.price_tick,
                qty_step=spec.qty_step,
                limit_offset_ticks=limit_offset_ticks,
                limit_timeout_ms=limit_timeout_ms,
                time_in_force=time_in_force,
            )
            adapter = PaperAdapter(_best, max_slippage_bp=max_slippage_bp)
            self.execs[sym] = Executor(adapter=adapter, cfg=cfg, get_best=_best)

        # WS → Coalescer
        await self.ws.connect(self._on_ws_raw)
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))

        # watchdog
        self._wd_task = asyncio.create_task(self._watchdog_loop(), name="watchdog")

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        await self.coal.close()
        await self.ws.close()
        if self._wd_task:
            self._wd_task.cancel()
            self._wd_task = None
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    # ---------- pipeline handlers ----------

    async def _on_ws_raw(self, raw: Dict[str, Any]) -> None:
        await self.coal.on_event(raw)

    async def _on_tick(self, tick: TickerTick) -> None:
        # 1) обновляем best bid/ask
        await self.hub.update(tick)
        # 2) сохраняем в историю (компактный dict)
        item = {
            "symbol": tick.symbol,
            "ts_ms": tick.ts_ms,
            "price": tick.price,
            "bid": tick.bid,
            "ask": tick.ask,
            "bid_size": tick.bid_size,
            "ask_size": tick.ask_size,
            "mark_price": tick.mark_price,
        }
        async with self._hist_lock:
            dq = self._hist.get(tick.symbol)
            if dq is not None:
                dq.append(item)
            else:
                self._hist[tick.symbol] = deque([item], maxlen=self._history_maxlen)

        # здесь позже появится Risk/Strategy → Candidate → Sizing → Executor.place_entry()

    # ---------- public ops ----------

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        return self.hub.best_bid_ask(symbol)

    def latest_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Последний тик из истории; если истории нет — None (пусть API сделает фолбэк)."""
        dq = self._hist.get(symbol.upper())
        if not dq:
            return None
        return dq[-1] if len(dq) > 0 else None

    def history_ticks(self, symbol: str, since_ms: int, limit: int) -> List[Dict[str, Any]]:
        """
        Возвращает тики с ts_ms >= since_ms, обрезанные до 'limit' последних.
        Работает быстро (deque), без копирования сверх необходимого.
        """
        sym = symbol.upper()
        dq = self._hist.get(sym)
        if not dq:
            return []
        result: List[Dict[str, Any]] = [t for t in dq if t.get("ts_ms", 0) >= since_ms]
        if len(result) > limit:
            result = result[-limit:]
        return result

    async def place_entry(self, symbol: str, side: Side, qty: float) -> Dict[str, Any]:
        """
        Прямой тестовый вход (обходит стратегии) + постановка SL/TP (paper-план).
        FSM: FLAT -> ENTERING -> OPEN; таймаут -> EXITING -> FLAT.
        """
        symbol = symbol.upper()
        if symbol not in self.execs:
            raise ValueError(f"Unsupported symbol: {symbol}")

        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "FLAT":
                return {"ok": False, "reason": f"busy:{pos.state}", "symbol": symbol, "side": side, "qty": qty}

            # ENTERING
            pos.state = "ENTERING"
            pos.side = side

            rep = await self.execs[symbol].place_entry(symbol, side, qty)

            if rep.status not in ("FILLED", "PARTIAL"):
                # не вошли — откат в FLAT
                self._pos[symbol] = PositionState(
                    state="FLAT", side=None, qty=0.0, entry_px=0.0,
                    sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=180_000
                )
                return {
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "report": {
                        "status": rep.status,
                        "filled_qty": rep.filled_qty,
                        "avg_px": rep.avg_px,
                        "limit_oid": rep.limit_oid,
                        "market_oid": rep.market_oid,
                        "steps": rep.steps,
                        "ts": rep.ts,
                    },
                }

            # позиция открыта на rep.avg_px количеством rep.filled_qty
            entry_px = rep.avg_px
            qty_open = rep.filled_qty

            # оценка стоп-дистанции: минимум 2 тика, либо текущий спред в тиках (консервативно)
            spec = self.specs[symbol]
            bid, ask = self.hub.best_bid_ask(symbol)
            spread_ticks = max(1.0, (ask - bid) / max(spec.price_tick, 1e-9))
            sl_distance_px = max(2.0 * spec.price_tick, spread_ticks * spec.price_tick)

            # рассчитать SL/TP (paper-план)
            plan: SLTPPlan = compute_sltp(
                side=side, entry_px=entry_px, qty=qty_open,
                price_tick=spec.price_tick, sl_distance_px=sl_distance_px, rr=1.8
            )

            # считаем защиту "поставленной" в состоянии (в live тут — REST reduceOnly)
            self._pos[symbol] = PositionState(
                state="OPEN", side=side, qty=qty_open, entry_px=entry_px,
                sl_px=plan.sl_px, tp_px=plan.tp_px, opened_ts_ms=self._now_ms(), timeout_ms=180_000
            )

            return {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "report": {
                    "status": rep.status,
                    "filled_qty": rep.filled_qty,
                    "avg_px": rep.avg_px,
                    "limit_oid": rep.limit_oid,
                    "market_oid": rep.market_oid,
                    "steps": rep.steps + [f"protection_set:SL@{plan.sl_px} TP@{plan.tp_px}"],
                    "ts": rep.ts,
                },
            }

    # ---------- watchdog ----------

    async def _watchdog_loop(self) -> None:
        """
        Каждые 250мс: гарантировать, что открытая позиция имеет SL/TP;
        по таймауту закрывать позицию market (paper).
        """
        try:
            while True:
                await asyncio.sleep(0.25)
                now = self._now_ms()
                for sym in self.symbols:
                    lock = self._locks[sym]
                    async with lock:
                        pos = self._pos[sym]
                        if pos.state == "OPEN":
                            # Таймаут позиции
                            if pos.opened_ts_ms and (now - pos.opened_ts_ms) >= pos.timeout_ms:
                                await self._paper_close(sym, pos)
                                continue
                            # Контроль защит (в paper — просто проверка, что они записаны)
                            if pos.sl_px is None or pos.tp_px is None:
                                await self._paper_close(sym, pos)
        except asyncio.CancelledError:
            return

    async def _paper_close(self, sym: str, pos: PositionState) -> None:
        """Аварийное закрытие позиции в paper: market reduceOnly (эмулируем входом противоположной стороной)."""
        try:
            ex = self.execs[sym]
            close_side: Side = "SELL" if pos.side == "BUY" else "BUY"
            _ = await ex.place_entry(sym, close_side, pos.qty)  # market эмулируется внутри executor/adapter
        finally:
            self._pos[sym] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=180_000
            )

    # ---------- diag ----------

    def diag(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "symbols": list(self.symbols),
            "ws_detail": self.ws.diag(),
            "coal": self.coal.stats(),
            "best": {s: self.hub.best_bid_ask(s) for s in self.symbols},
            "exec_cfg": {
                s: {
                    "price_tick": self.execs[s].c.price_tick,
                    "qty_step": self.execs[s].c.qty_step,
                    "limit_offset_ticks": self.execs[s].c.limit_offset_ticks,
                    "limit_timeout_ms": self.execs[s].c.limit_timeout_ms,
                    "time_in_force": self.execs[s].c.time_in_force,
                }
                for s in self.symbols
            },
            "history": {
                s: {
                    "len": len(self._hist.get(s, [])),
                    "maxlen": self._history_maxlen,
                    "latest_ts_ms": (self._hist[s][-1]["ts_ms"] if self._hist.get(s) and len(self._hist[s]) > 0 else 0),
                }
                for s in self.symbols
            },
            "positions": {
                s: {
                    "state": self._pos[s].state,
                    "side": self._pos[s].side,
                    "qty": self._pos[s].qty,
                    "entry_px": self._pos[s].entry_px,
                    "sl_px": self._pos[s].sl_px,
                    "tp_px": self._pos[s].tp_px,
                    "opened_ts_ms": self._pos[s].opened_ts_ms,
                    "timeout_ms": self._pos[s].timeout_ms,
                }
                for s in self.symbols
            },
        }
        return d

    # ---------- utils ----------

    @staticmethod
    def _now_ms() -> int:
        import time
        return int(time.time() * 1000)


# ---------- модульный smoke-тест ----------

async def _smoke() -> None:
    syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    w = Worker(syms, futures=True, coalesce_ms=75, history_maxlen=2000)
    await w.start()
    print("[smoke] started, warming up ticks...")
    await asyncio.sleep(2.5)

    bid, ask = w.best_bid_ask("BTCUSDT")
    print(f"[smoke] BTC best bid/ask = {bid} / {ask}")

    if bid == 0.0 or ask == 0.0:
        print("[smoke] no best price yet, wait a bit more...")
        await asyncio.sleep(2.0)

    rep = await w.place_entry("BTCUSDT", "BUY", 0.001)
    print("[smoke] EXEC REPORT:", rep["report"])

    # проверим историю
    hist = w.history_ticks("BTCUSDT", since_ms=0, limit=5)
    print("[smoke] last ticks:", len(hist), "example:", hist[-1] if hist else None)

    print("[smoke] diag:", w.diag())
    await w.stop()


def _run_smoke() -> None:
    try:
        asyncio.run(_smoke())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    _run_smoke()
