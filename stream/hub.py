# stream/hub.py
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional
from bisect import bisect_left

from adapters.binance_ws import BinanceWS          # отдаёт raw dict-события
from stream.coalescer import Coalescer, TickerTick # стабилизированный тик (без циклических импортов)


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
      - принимает сырые WS-события (BinanceWS),
      - агрегирует их в Coalescer до TickerTick с частотой coalesce_ms,
      - хранит последнюю котировку и «скользящую» историю по символам,
      - предоставляет быстрые геттеры и статистику.
    """

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        coalesce_ms: int = 75,
        history_seconds: int = 600,  # ~10 минут истории по умолчанию
    ) -> None:
        self._symbols: List[str] = [s.upper() for s in symbols]
        self._coalesce_ms: int = max(10, int(coalesce_ms))
        self._ws = BinanceWS(self._symbols)                 # futures=True по умолчанию
        self._coal = Coalescer(coalesce_ms=self._coalesce_ms)

        # Счётчики/диагностика
        self._ws_count: int = 0
        self._ws_last_ts_ms: Optional[int] = None
        self._coal_push_count: int = 0
        self._coal_last_tick_ts_ms: Optional[int] = None

        # Последний тик и история по символам
        ticks_per_sec = max(1, int(round(1000 / self._coalesce_ms)))
        history_maxlen = max(1, int(history_seconds) * ticks_per_sec)
        self._last: Dict[str, TickerTick] = {}
        self._hist: Dict[str, Deque[TickerTick]] = defaultdict(lambda: deque(maxlen=history_maxlen))

        # Фоновые задачи/жизненный цикл
        self._tasks: List[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self._started = False

    # ---------------- Lifecycle ----------------

    async def start(self) -> None:
        """Запускает WS и коалесцер (идемпотентно)."""
        if self._started:
            return
        self._stopping.clear()
        await self._ws.connect(on_message=self._on_ws_message)
        self._tasks.append(asyncio.create_task(self._coal.run(self._on_tick)))
        self._tasks.append(asyncio.create_task(self._heartbeat()))
        self._started = True

    async def stop(self) -> None:
        """Останавливает все фоновые задачи (идемпотентно)."""
        if not self._started:
            return
        self._stopping.set()
        await self._ws.close()
        await self._coal.close()
        # Корректно отменяем таски
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            except Exception:
                pass
        self._tasks.clear()
        self._started = False

    async def _heartbeat(self) -> None:
        """Простой heartbeat для будущих health-check’ов/метрик."""
        try:
            while not self._stopping.is_set():
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    # ---------------- Internal callbacks ----------------

    async def _on_ws_message(self, msg: dict) -> None:
        """
        Сырое событие от адаптера WS.
        Пробрасываем в коалесцер, который выдаст стабилизированный TickerTick.
        """
        self._ws_count += 1
        self._ws_last_ts_ms = int(time.time() * 1000)
        await self._coal.on_event(msg)

    async def _on_tick(self, tick: TickerTick) -> None:
        """Согласованный тик от коалесцера."""
        self._coal_push_count += 1
        self._coal_last_tick_ts_ms = tick.ts_ms
        self._last[tick.symbol] = tick
        self._hist[tick.symbol].append(tick)

    # ---------------- Public API ----------------

    def ws_stats(self) -> Dict[str, Optional[int]]:
        """Короткие статусы WS (для /debug/ws и /status)."""
        return {"count": self._ws_count, "last_rx_ts_ms": self._ws_last_ts_ms}

    def ws_diag(self) -> Dict[str, object]:
        """Расширенная диагностика WS-адаптера (опционально подключай в /debug/diag)."""
        try:
            return self._ws.diag()  # type: ignore[attr-defined]
        except Exception:
            return {}

    def coalescer_stats(self) -> Dict[str, Optional[int]]:
        """Статусы коалесцера (для /debug/diag и /status)."""
        return {"push_count": self._coal_push_count, "last_tick_ts_ms": self._coal_last_tick_ts_ms}

    def symbols(self) -> List[str]:
        return list(self._symbols)

    def latest(self, symbol: str) -> Optional[TickerTick]:
        return self._last.get(symbol.upper())

    # alias для совместимости с ручками
    def get_last(self, symbol: str) -> Optional[TickerTick]:
        return self.latest(symbol)

    def history(self, symbol: str, n: int = 512) -> List[TickerTick]:
        """Последние n тиков по символу (хвост)."""
        dq = self._hist.get(symbol.upper())
        if not dq or n <= 0:
            return []
        k = min(n, len(dq))
        if k == 0:
            return []
        # Преобразуем только хвост — дешевле чем list(dq) целиком, но читаемо:
        return list(list(dq)[-k:])

    def history_window(self, symbol: str, since_ms: int) -> List[TickerTick]:
        """
        Тики по символу, чьи ts_ms >= since_ms.
        Используем bisect по массиву времен для ускорения поиска начала окна.
        """
        dq = self._hist.get(symbol.upper())
        if not dq:
            return []
        arr = list(dq)  # O(n)
        if not arr:
            return []
        ts_arr = [t.ts_ms for t in arr]  # O(n)
        i = bisect_left(ts_arr, since_ms)  # O(log n)
        return arr[i:]
