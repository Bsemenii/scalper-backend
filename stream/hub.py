# stream/hub.py
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional

from adapters.binance_ws import BinanceWS          # адаптер должен отдавать raw dict-события
from stream.coalescer import Coalescer, TickerTick # TickerTick определён в coalescer (НЕ в hub) — чтобы не было циклов


@dataclass(frozen=True)
class WSStats:
    count: int
    last_rx_ts_ms: Optional[int]


@dataclass(frozen=True)
class CoalescerStats:
    push_count: int
    last_tick_ts_ms: Optional[int]


class TickerHub:
    """
    Централизованный узел:
      - принимает raw WS события (через BinanceWS),
      - прокидывает их в Coalescer,
      - хранит последнюю «согласованную» котировку и историю на символ,
      - предоставляет быстрые геттеры и статистику.
    """

    def __init__(self, symbols: Iterable[str], coalesce_ms: int = 75, history_maxlen: int = 4096) -> None:
        self._symbols: List[str] = list(symbols)
        self._ws = BinanceWS(self._symbols)
        self._coal = Coalescer(coalesce_ms=coalesce_ms)

        # Счётчики
        self._ws_count: int = 0
        self._ws_last_ts_ms: Optional[int] = None
        self._coal_push_count: int = 0
        self._coal_last_tick_ts_ms: Optional[int] = None

        # Последний тик и история по символам
        self._last: Dict[str, TickerTick] = {}
        self._hist: Dict[str, Deque[TickerTick]] = defaultdict(lambda: deque(maxlen=history_maxlen))

        self._tasks: List[asyncio.Task] = []
        self._stopping = asyncio.Event()

    # ---------------- Lifecycle ----------------

    async def start(self) -> None:
        """Запуск приёмника WS и коалесцера."""
        await self._ws.connect(on_message=self._on_ws_message)
        # Запускаем цикл коалесцера (он будет слать TickerTick в наш _on_tick)
        self._tasks.append(asyncio.create_task(self._coal.run(self._on_tick)))
        # Необязательный heartbeat — для будущих health-check'ов
        self._tasks.append(asyncio.create_task(self._heartbeat()))

    async def stop(self) -> None:
        """Корректная остановка фоновых задач."""
        self._stopping.set()
        await self._ws.close()
        await self._coal.close()
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    async def _heartbeat(self) -> None:
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    # ---------------- Callbacks ----------------

    async def _on_ws_message(self, msg: dict) -> None:
        """Прилетело сырое событие из адаптера WS."""
        self._ws_count += 1
        self._ws_last_ts_ms = int(time.time() * 1000)
        # В коалесцер передаём сырое событие — он агрегирует в TickerTick
        await self._coal.on_event(msg)

    async def _on_tick(self, tick: TickerTick) -> None:
        """Согласованный тик от коалесцера."""
        self._coal_push_count += 1
        self._coal_last_tick_ts_ms = tick.ts_ms
        self._last[tick.symbol] = tick
        self._hist[tick.symbol].append(tick)

    # ---------------- Public API ----------------

    def ws_stats(self) -> dict:
        return {"count": self._ws_count, "last_rx_ts_ms": self._ws_last_ts_ms}

    def coalescer_stats(self) -> dict:
        return {"push_count": self._coal_push_count, "last_tick_ts_ms": self._coal_last_tick_ts_ms}

    def symbols(self) -> List[str]:
        return list(self._symbols)

    def latest(self, symbol: str) -> Optional[TickerTick]:
        return self._last.get(symbol)

    def get_last(self, symbol: str) -> Optional[TickerTick]:
        # alias для совместимости с твоими ручками
        return self.latest(symbol)

    def history(self, symbol: str, n: int = 512) -> List[TickerTick]:
        dq = self._hist.get(symbol)
        if not dq:
            return []
        if n <= 0:
            return []
        # возвращаем хвост последних n
        k = min(n, len(dq))
        if k == 0:
            return []
        return list(list(dq)[-k:])

    def history_window(self, symbol: str, since_ms: int) -> List[TickerTick]:
        dq = self._hist.get(symbol)
        if not dq:
            return []
        return [t for t in dq if t.ts_ms >= since_ms]
