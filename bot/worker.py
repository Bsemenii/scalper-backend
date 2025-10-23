# bot/worker.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
from collections import deque, deque as Deque

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg, ExecutionReport
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
    """Хранит последнюю лучшую пару bid/ask по символу (потокобезопасно)."""
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
      - SL/TP план + watchdog (таймаут и защита),
      - учёт сделок и дневной PnL (best-effort),
      - счётчики исполнения (для /metrics),
      - комиссии (maker/taker) в paper-режиме.
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

        # Исполнение: агрегированные счётчики (для /metrics)
        self._exec_counters: Dict[str, int] = {
            "limit_total": 0,
            "market_total": 0,
            "cancel_total": 0,
            "reject_total": 0,
        }

        # Учёт сделок (in-memory) и дневная сводка (best-effort)
        self._trades: Dict[str, Deque[Dict[str, Any]]] = {s: deque(maxlen=1000) for s in self.symbols}
        self._pnl_day: Dict[str, Any] = {
            "day": self._day_str(),   # сразу инициализируем текущим днём
            "trades": 0,
            "winrate": None,
            "avg_r": None,
            "pnl_r": 0.0,
            "pnl_usd": 0.0,
            "max_dd_r": None,
        }
        self._pnl_r_equity: float = 0.0  # для подсчёта max drawdown в R

        # причины блокировок входа (если будем наполнять позже)
        self._block_reasons: Dict[str, int] = {}

        # для оценки комиссий входа: сохраняем steps открытия
        self._entry_steps: Dict[str, List[str]] = {}

        # таски жизненного цикла
        self._tasks: List[asyncio.Task] = []
        self._wd_task: Optional[asyncio.Task] = None
        self._started = False

    # ---------- lifecycle ----------

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

        # Остановить watchdog
        if self._wd_task:
            self._wd_task.cancel()
            with contextlib.suppress(Exception):
                await self._wd_task
            self._wd_task = None

        # Остановить коалесцер и WS
        with contextlib.suppress(Exception):
            await self.coal.close()
        with contextlib.suppress(Exception):
            await self.ws.close()

        # Погасить фоновые таски
        for t in self._tasks:
            t.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*self._tasks, return_exceptions=True)
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
        Возвращает словарь с ключом "report" для совместимости с API.
        """
        symbol = symbol.upper()
        if symbol not in self.execs:
            raise ValueError(f"Unsupported symbol: {symbol}")

        lock = self._locks[symbol]
        async with lock:
            pos = self._pos[symbol]
            if pos.state != "FLAT":
                self._inc_block_reason("busy_" + pos.state)
                return {"ok": False, "reason": f"busy:{pos.state}", "symbol": symbol, "side": side, "qty": qty}

            # ENTERING
            pos.state = "ENTERING"
            pos.side = side

            rep: ExecutionReport = await self.execs[symbol].place_entry(symbol, side, qty)
            self._accumulate_exec_counters(rep.steps)

            if rep.status not in ("FILLED", "PARTIAL") or rep.filled_qty <= 0.0 or rep.avg_px is None:
                # не вошли — откат в FLAT
                self._pos[symbol] = PositionState(
                    state="FLAT", side=None, qty=0.0, entry_px=0.0,
                    sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
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

            # позиция открыта
            entry_px = float(rep.avg_px)
            qty_open = float(rep.filled_qty)

            # оценка стоп-дистанции: минимум min_stop_ticks тиков или текущий спред в тиках
            spec = self.specs[symbol]
            bid, ask = self.hub.best_bid_ask(symbol)
            spread_ticks = max(1.0, (ask - bid) / max(spec.price_tick, 1e-9))
            min_stop_ticks = self._min_stop_ticks_default()  # по умолчанию 2 тика (как было)
            sl_distance_px = max(min_stop_ticks * spec.price_tick, spread_ticks * spec.price_tick)

            # рассчитать SL/TP (paper-план)
            plan: SLTPPlan = compute_sltp(
                side=side, entry_px=entry_px, qty=qty_open,
                price_tick=spec.price_tick, sl_distance_px=sl_distance_px, rr=1.8
            )

            # считаем защиту "поставленной" в состоянии (в live тут — REST reduceOnly)
            self._pos[symbol] = PositionState(
                state="OPEN", side=side, qty=qty_open, entry_px=entry_px,
                sl_px=plan.sl_px, tp_px=plan.tp_px, opened_ts_ms=self._now_ms(), timeout_ms=pos.timeout_ms
            )
            # сохраним шаги входа для расчёта комиссий при выходе
            self._entry_steps[symbol] = list(rep.steps)

            return {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "report": {
                    "status": rep.status,
                    "filled_qty": qty_open,
                    "avg_px": entry_px,
                    "limit_oid": rep.limit_oid,
                    "market_oid": rep.market_oid,
                    "steps": rep.steps + [f"protection_set:SL@{plan.sl_px} TP@{plan.tp_px}"],
                    "ts": self._now_ms(),
                },
            }

    # ---------- watchdog / closing / trades ----------

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
                                await self._paper_close(sym, pos, reason="timeout")
                                continue
                            # Контроль защит (в paper — просто проверка, что они записаны)
                            if pos.sl_px is None or pos.tp_px is None:
                                await self._paper_close(sym, pos, reason="no_protection")
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("watchdog loop error")

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
            await self._paper_close(symbol, pos, reason="flatten_forced")
            return {"ok": True, "symbol": symbol, "state": "FLAT", "reason": "flatten_forced"}

    def set_timeout_ms(self, symbol: str, timeout_ms: int) -> Dict[str, Any]:
        """Поменять таймаут позиции для отладки (например, 20_000 мс)."""
        symbol = symbol.upper()
        self._pos[symbol].timeout_ms = max(5_000, int(timeout_ms))
        return {"ok": True, "symbol": symbol, "timeout_ms": self._pos[symbol].timeout_ms}

    async def _paper_close(self, sym: str, pos: PositionState, *, reason: str) -> None:
        """Закрытие позиции в paper: маркетом через executor (эмулируется в адаптере)."""
        close_side: Side = "SELL" if pos.side == "BUY" else "BUY"
        try:
            ex = self.execs[sym]
            rep: ExecutionReport = await ex.place_entry(sym, close_side, pos.qty)  # market эмулируется внутри executor/adapter
            self._accumulate_exec_counters(rep.steps)

            exit_px = rep.avg_px
            if exit_px is not None:
                entry_steps = self._entry_steps.get(sym, [])
                trade = self._build_trade_record(
                    sym, pos, float(exit_px),
                    reason=reason, entry_steps=entry_steps, exit_steps=list(rep.steps),
                )
                self._register_trade(trade)
        finally:
            # Сброс позиции в FLAT
            self._pos[sym] = PositionState(
                state="FLAT", side=None, qty=0.0, entry_px=0.0,
                sl_px=None, tp_px=None, opened_ts_ms=0, timeout_ms=pos.timeout_ms
            )
            # очищаем запомненные шаги входа
            self._entry_steps.pop(sym, None)

    # ---------- учёт/PNL ----------

    def _build_trade_record(
        self,
        sym: str,
        pos: PositionState,
        exit_px: float,
        *,
        reason: str,
        entry_steps: Optional[List[str]] = None,
        exit_steps: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        side = pos.side or "BUY"
        qty = float(pos.qty)
        entry = float(pos.entry_px)

        # PnL USD (USDT-перпы: pnl = (exit - entry) * qty для LONG; обратный знак для SHORT)
        if side == "BUY":
            pnl_usd_gross = (exit_px - entry) * qty
            pnl_risk_usd = max(1e-9, (entry - float(pos.sl_px or entry)) * qty)
        else:
            pnl_usd_gross = (entry - exit_px) * qty
            pnl_risk_usd = max(1e-9, (float(pos.sl_px or entry) - entry) * qty)

        # Комиссии (на вход и выход) — в USD
        entry_bps = self._fee_bps_for_steps(entry_steps or [])
        exit_bps  = self._fee_bps_for_steps(exit_steps or [])
        fees_usd = (entry * qty * entry_bps + exit_px * qty * exit_bps) / 10_000.0

        # Итоговый PnL после комиссий
        pnl_usd = pnl_usd_gross - fees_usd

        # защищаемся от микроскопического риска — нормализуем на пол
        risk_usd = max(pnl_risk_usd, self._min_risk_usd_floor())
        pnl_r = pnl_usd / risk_usd

        trade = {
            "symbol": sym,
            "side": side,
            "opened_ts": pos.opened_ts_ms,
            "closed_ts": self._now_ms(),
            "qty": qty,
            "entry_px": entry,
            "exit_px": float(exit_px),
            "sl_px": pos.sl_px,
            "tp_px": pos.tp_px,
            "pnl_usd": round(pnl_usd, 6),
            "pnl_r": round(pnl_r, 6),
            "reason": reason,
            "fees": round(fees_usd, 6),
        }
        return trade

    def _register_trade(self, trade: Dict[str, Any]) -> None:
        sym = trade["symbol"]
        dq = self._trades.get(sym)
        if dq is None:
            dq = self._trades[sym] = deque(maxlen=1000)
        dq.appendleft(trade)  # свежие впереди

        # Обновление дневной сводки
        day = self._day_str()
        if self._pnl_day["day"] != day:
            # новый день — сбрасываем статы
            self._pnl_day = {"day": day, "trades": 0, "winrate": None, "avg_r": None, "pnl_r": 0.0, "pnl_usd": 0.0, "max_dd_r": None}
            self._pnl_r_equity = 0.0

        self._pnl_day["trades"] += 1
        self._pnl_day["pnl_usd"] += trade["pnl_usd"]
        self._pnl_day["pnl_r"] += trade["pnl_r"]

        # winrate/avg_r
        wins = int(round((self._pnl_day.get("winrate_raw", 0.0) or 0.0) * max(self._pnl_day["trades"] - 1, 0)))
        if trade["pnl_r"] > 0:
            wins += 1
        self._pnl_day["winrate_raw"] = wins / max(self._pnl_day["trades"], 1)
        self._pnl_day["winrate"] = round(self._pnl_day["winrate_raw"], 4)
        self._pnl_day["avg_r"] = round(self._pnl_day["pnl_r"] / max(self._pnl_day["trades"], 1), 6)

        # max drawdown по equity в R
        self._pnl_r_equity += trade["pnl_r"]
        peak = self._pnl_day.get("peak_r", 0.0)
        trough = self._pnl_day.get("trough_r", 0.0)
        if self._pnl_r_equity > peak:
            peak = self._pnl_r_equity
        if self._pnl_r_equity < trough:
            trough = self._pnl_r_equity
        self._pnl_day["peak_r"] = peak
        self._pnl_day["trough_r"] = trough
        self._pnl_day["max_dd_r"] = round(peak - trough if peak - trough > 0 else 0.0, 6)

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
            # новые блоки для API:
            "exec_counters": dict(self._exec_counters),
            "pnl_day": {
                k: (round(v, 6) if isinstance(v, float) else v)
                for k, v in self._pnl_day.items() if not k.endswith("_raw")
            },
            "trades": {s: list(self._trades.get(s, [])) for s in self.symbols},
            "block_reasons": dict(self._block_reasons),
        }
        return d

    # ---------- utils ----------

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _day_str() -> Optional[str]:
        try:
            return time.strftime("%Y-%m-%d", time.gmtime())
        except Exception:
            return None

    def _accumulate_exec_counters(self, steps: Optional[List[str]]) -> None:
        """Парсинг steps из отчёта исполнения для счётчиков."""
        if not steps:
            return
        for s in steps:
            if s.startswith("limit_submit"):
                self._exec_counters["limit_total"] += 1
            elif s.startswith("market_submit") or s.startswith("market_filled"):
                self._exec_counters["market_total"] += 1
            elif s.startswith("limit_cancel"):
                self._exec_counters["cancel_total"] += 1
            elif "rejected" in s:
                self._exec_counters["reject_total"] += 1

    def _inc_block_reason(self, key: str) -> None:
        self._block_reasons[key] = self._block_reasons.get(key, 0) + 1

    def _min_risk_usd_floor(self) -> float:
        """
        Минимальный размер риска (в USD) для расчёта R, чтобы избегать аномальных R
        при микроскопических стоп-дистанциях (paper). Берётся из settings.risk.min_risk_usd_floor, если есть.
        """
        default = 0.25
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "risk", object()), "min_risk_usd_floor", None)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
            except Exception:
                pass
        return default

    def _min_stop_ticks_default(self) -> float:
        """
        Минимум тиков для SL-дистанции при расчёте защиты. По умолчанию 2 тика (как было).
        Можно задать settings.execution.min_stop_ticks.
        """
        default = 2.0
        if get_settings:
            try:
                s = get_settings()
                val = getattr(getattr(s, "execution", object()), "min_stop_ticks", None)
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
            except Exception:
                pass
        return default

    def _fee_bps_for_steps(self, steps: List[str]) -> float:
        """
        Определяем maker/taker по шагам:
          - есть 'market_' ИЛИ 'limit_filled_immediate' → taker
          - иначе → maker
        Возвращаем ставку в bps, из settings.execution или дефолты.
        """
        taker_like = any(s.startswith("market_") or s.startswith("limit_filled_immediate") for s in steps)
        maker_bps = 1.0   # 0.01%
        taker_bps = 3.5   # 0.035%
        if get_settings:
            try:
                s = get_settings()
                maker_bps = float(getattr(s.execution, "fee_bps_maker", maker_bps))
                taker_bps = float(getattr(s.execution, "fee_bps_taker", taker_bps))
            except Exception:
                pass
        return taker_bps if taker_like else maker_bps

    # совместимость с /status фолбэком: чтобы app мог обратиться как worker.coalescer.stats()
    @property
    def coalescer(self) -> Coalescer:
        return self.coal


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

    # проверим flatten → запись трейда
    await w.flatten("BTCUSDT")
    diag = w.diag()
    print("[smoke] trades recorded:", len(diag.get("trades", {}).get("BTCUSDT", [])))
    print("[smoke] pnl_day:", diag.get("pnl_day"))

    # проверим историю
    hist = w.history_ticks("BTCUSDT", since_ms=0, limit=5)
    print("[smoke] last ticks:", len(hist), "example:", hist[-1] if hist else None)

    print("[smoke] diag keys:", list(diag.keys()))
    await w.stop()


def _run_smoke() -> None:
    try:
        asyncio.run(_smoke())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    _run_smoke()
