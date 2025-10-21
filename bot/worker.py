# bot/worker.py
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal

from adapters.binance_ws import BinanceWS
from stream.coalescer import Coalescer, TickerTick
from adapters.binance_rest import PaperAdapter
from exec.executor import Executor, ExecCfg

# Если у тебя уже есть get_settings() — импортируй его;
# здесь делаем мягкий импорт и фолбэк на дефолт, чтобы smoke-скрипт работал автономно.
try:
    from bot.core.config import get_settings  # type: ignore
except Exception:
    get_settings = None  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

Side = Literal["BUY", "SELL"]


# --- вспомогательные структуры ---

@dataclass
class SymbolSpec:
    symbol: str
    price_tick: float
    qty_step: float


# Мини-хаб цен: хранит последнюю лучшую пару bid/ask на символ
class MarketHub:
    def __init__(self, symbols: List[str]) -> None:
        self._best: Dict[str, Tuple[float, float]] = {s: (0.0, 0.0) for s in symbols}
        self._lock = asyncio.Lock()

    async def update(self, t: TickerTick) -> None:
        async with self._lock:
            self._best[t.symbol] = (t.bid, t.ask)

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        return self._best.get(symbol, (0.0, 0.0))


class Worker:
    """
    Высокоуровневый оркестратор:
      WS (BinanceWS) -> Coalescer -> MarketHub(best bid/ask) -> Executors(per-symbol, paper)
    В следующих коммитах сюда легко вклиним Risk/Strategy/FSM/SLTP.
    """

    def __init__(self, symbols: List[str], *, futures: bool = True, coalesce_ms: int = 75) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.ws = BinanceWS(self.symbols, futures=futures, mark_interval_1s=True)
        self.coal = Coalescer(coalesce_ms=coalesce_ms)
        self.hub = MarketHub(self.symbols)

        # per-symbol спецификации (tick/step) — безопасные дефолты для USDT-перпов Binance Futures
        # при желании — подтяни exchangeInfo и заполни автоматически (следующая неделя)
        self.specs: Dict[str, SymbolSpec] = {
            "BTCUSDT": SymbolSpec("BTCUSDT", price_tick=0.1,   qty_step=0.001),
            "ETHUSDT": SymbolSpec("ETHUSDT", price_tick=0.01,  qty_step=0.001),
            "SOLUSDT": SymbolSpec("SOLUSDT", price_tick=0.001, qty_step=0.1),
        }
        # на случай других символов — разумный фолбэк
        for s in self.symbols:
            if s not in self.specs:
                self.specs[s] = SymbolSpec(s, price_tick=0.01, qty_step=0.001)

        # per-symbol executors
        self.execs: Dict[str, Executor] = {}

        # таски жизненного цикла
        self._tasks: List[asyncio.Task] = []
        self._started = False

    # ---------- lifecycle ----------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # подготовим executors per-symbol (paper)
        def _best(sym: str) -> Tuple[float, float]:
            return self.hub.best_bid_ask(sym)

        # загружаем конфиг если есть
        limit_offset_ticks = 1
        limit_timeout_ms = 500
        time_in_force = "GTC"
        max_slippage_bp = 6.0
        price_tick_overrides: Dict[str, float] = {}
        qty_step_overrides: Dict[str, float] = {}

        if get_settings:
            try:
                s = get_settings()
                limit_offset_ticks = int(s.execution.get("limit_offset_ticks", limit_offset_ticks))
                limit_timeout_ms = int(s.execution.get("limit_timeout_ms", limit_timeout_ms))
                max_slippage_bp = float(s.execution.get("max_slippage_bp", max_slippage_bp))
                time_in_force = str(s.execution.get("time_in_force", time_in_force))
            except Exception:
                logger.debug("settings not available, use defaults")

        for sym in self.symbols:
            spec = self.specs[sym]
            cfg = ExecCfg(
                price_tick=price_tick_overrides.get(sym, spec.price_tick),
                qty_step=qty_step_overrides.get(sym, spec.qty_step),
                limit_offset_ticks=limit_offset_ticks,
                limit_timeout_ms=limit_timeout_ms,
                time_in_force=time_in_force,
            )
            adapter = PaperAdapter(_best, max_slippage_bp=max_slippage_bp)
            self.execs[sym] = Executor(adapter=adapter, cfg=cfg, get_best=_best)

        # WS → Coalescer
        await self.ws.connect(self._on_ws_raw)
        self._tasks.append(asyncio.create_task(self.coal.run(self._on_tick), name="coal.run"))

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        # гасим коалесцер
        await self.coal.close()
        # гасим WS
        await self.ws.close()
        # таски
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    # ---------- pipeline handlers ----------

    async def _on_ws_raw(self, raw: Dict[str, Any]) -> None:
        # отдаём в коалесцер «как есть»
        await self.coal.on_event(raw)

    async def _on_tick(self, tick: TickerTick) -> None:
        # обновляем best bid/ask
        await self.hub.update(tick)
        # здесь (чуть позже) будет Risk/Strategy → Candidate → Sizing → Executor.place_entry()

    # ---------- public ops ----------

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        return self.hub.best_bid_ask(symbol)

    async def place_entry(self, symbol: str, side: Side, qty: float) -> Dict[str, Any]:
        """
        Прямой тестовый вход (обходит стратегии).
        Возвращает dict с ExecutionReport + echo-полями.
        """
        symbol = symbol.upper()
        if symbol not in self.execs:
            raise ValueError(f"Unsupported symbol: {symbol}")
        rep = await self.execs[symbol].place_entry(symbol, side, qty)
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
        }
        return d


# ---------- модульный smoke-тест ----------

async def _smoke() -> None:
    """
    Быстрый автономный тест (без FastAPI):
      1) стартуем worker на BTC/ETH/SOL
      2) ждём поток тиков 2.5с
      3) исполняем market-добиение через Executor (BUY 0.001 BTC)
      4) печатаем отчёт и гасим
    Запуск: python -m bot.worker
    """
    syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    w = Worker(syms, futures=True, coalesce_ms=75)
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

    print("[smoke] diag:", w.diag())
    await w.stop()


def _run_smoke() -> None:
    try:
        asyncio.run(_smoke())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    _run_smoke()
