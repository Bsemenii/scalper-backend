# stream/coalescer.py
from __future__ import annotations

from typing import AsyncIterator, List, Optional

from stream.hub import TickerHub, Tick


class Coalescer:
    """
    Мини-коалесер: принимает поток Tick и прокидывает в hub.
    Если у тебя есть более сложная агрегация (batched 75ms), добавь её
    внутри run(), но не забудь обновлять _push_count/_last_tick_ts_ms при push().
    """

    def __init__(self, hub: TickerHub, symbols: List[str], interval_ms: int = 75):
        self.hub = hub
        self.symbols = symbols
        self.interval_ms = interval_ms

        # Счётчики для /status
        self._push_count: int = 0
        self._last_tick_ts_ms: Optional[int] = None

    async def run(self, ws_stream: AsyncIterator[Tick]) -> None:
        """
        Читает из ws_stream нормализованные Tick и кладёт их в hub.
        """
        async for tick in ws_stream:
            # Здесь может быть твоя логика согласования/сэмплинга по 75мс.
            # Сейчас — прозрачная прокладка.
            self.hub.push(tick)
            self._push_count += 1
            self._last_tick_ts_ms = tick.ts_ms

    def ticker(self):
        """
        Заглушка под потенциальный таймер коалесера.
        Если понадобится свой asyncio.Task — вернёшь здесь корутину.
        Сейчас не используется.
        """
        return None

    def stats(self) -> dict:
        """Статистика коалесера для /status."""
        return {
            "push_count": self._push_count,
            "last_tick_ts_ms": self._last_tick_ts_ms,
        }
