# stream/hub.py
from __future__ import annotations

import collections
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional


@dataclass
class Tick:
    """
    Унифицированный снэп тика после коалесера.
    ts_ms      — монотонный timestamp в миллисекундах
    price      — mid/last для визуализаций (можно класть last или mid, как ты решишь)
    bid/ask    — best bid/ask
    bid_size/ask_size — объём на best уровнях
    mark_price — mark из биржи (если нет — можно дублировать price)
    """
    ts_ms: int
    symbol: str
    price: float
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    mark_price: float


class TickerHub:
    """
    Лёгкий in-proc «хаб» для последних снапов и короткой истории по каждому символу.
    Коалесер кладёт сюда снапы через push().
    """

    def __init__(self, maxlen: int = 4096):
        self.maxlen = maxlen
        self._latest: Dict[str, Tick] = {}
        self._hist: Dict[str, Deque[Tick]] = collections.defaultdict(
            lambda: collections.deque(maxlen=maxlen)
        )

        # Счётчики для /status
        self._ws_count: int = 0
        self._last_rx_ts_ms: Optional[int] = None

    # ------------------------------------------------------------------ API

    def push(self, tick: Tick) -> None:
        """Положить новый снэп по символу (вызывается коалесером)."""
        self._latest[tick.symbol] = tick
        self._hist[tick.symbol].append(tick)

        # обновим статусы потока
        self._ws_count += 1
        self._last_rx_ts_ms = tick.ts_ms

    def latest(self, symbol: str) -> Optional[Tick]:
        """Последний снэп по символу (или None)."""
        return self._latest.get(symbol)

    def peek(self, symbol: str, ms: int) -> List[Tick]:
        """
        Вернуть все тики за последние ms миллисекунд (простая фильтрация хвоста).
        Предполагаем, что ts_ms растёт.
        """
        dq = self._hist.get(symbol)
        if not dq:
            return []
        cutoff = dq[-1].ts_ms - ms
        # Для небольших окон простая фильтрация ок
        return [t for t in dq if t.ts_ms >= cutoff]

    def ws_stats(self) -> dict:
        """Статистика поступивших снэпов для /status."""
        return {"count": self._ws_count, "last_rx_ts_ms": self._last_rx_ts_ms}
