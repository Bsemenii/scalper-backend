# stream/coalescer.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional


@dataclass(frozen=True)
class TickerTick:
    symbol: str
    ts_ms: int
    price: float
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    mark_price: Optional[float] = None


class Coalescer:
    """
    Принимает raw-события с адаптера (dict) и преобразует в стабилизированные TickerTick.
    НИКАКИХ импортов из hub тут быть не должно.
    """

    def __init__(self, coalesce_ms: int = 75) -> None:
        self._coalesce_ms = coalesce_ms
        self._queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self._stopping = asyncio.Event()

    async def on_event(self, raw: Dict[str, Any]) -> None:
        """Вызывается hub'ом: пробрасываем raw событие в очередь коалесцера."""
        await self._queue.put(raw)

    async def run(self, on_tick: Callable[[TickerTick], Awaitable[None]]) -> None:
        """
        Основной цикл агрегации. Забирает raw dict'ы, собирает TickerTick и отдаёт наружу.
        """
        try:
            while not self._stopping.is_set():
                raw = await self._queue.get()

                # поддерживаем bookTicker / markPrice оба кейса
                ev = raw.get("e")
                if ev == "bookTicker":
                    t = self._from_book_ticker(raw)
                    if t:
                        await on_tick(t)
                elif ev == "markPriceUpdate":
                    # если приходит только markPrice — пропускаем (без bid/ask не собрать тик)
                    # но если это объединённое событие твоего адаптера — он уже был «склеен» в одно dict
                    t = self._from_combined(raw)
                    if t:
                        await on_tick(t)
                else:
                    # fallback: возможно адаптер уже склеил всё в единый dict с нужными полями
                    t = self._from_combined(raw)
                    if t:
                        await on_tick(t)
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        self._stopping.set()

    # ---------- parsers ----------

    def _from_book_ticker(self, d: Dict[str, Any]) -> Optional[TickerTick]:
        try:
            symbol = d["s"]
            ts_ms = int(d.get("E") or d.get("T") or d["ts_ms"])
            bid = float(d["b"])
            ask = float(d["a"])
            bid_sz = float(d.get("B", 0.0))
            ask_sz = float(d.get("A", 0.0))
            # mid как price; mark_price если есть
            price = (bid + ask) / 2.0
            mark = d.get("mark_price")
            mark_price = float(mark) if mark is not None else None
            return TickerTick(
                symbol=symbol,
                ts_ms=ts_ms,
                price=price,
                bid=bid,
                ask=ask,
                bid_size=bid_sz,
                ask_size=ask_sz,
                mark_price=mark_price,
            )
        except Exception:
            return None

    def _from_combined(self, d: Dict[str, Any]) -> Optional[TickerTick]:
        """
        Универсальный парсер, если адаптер сразу отдаёт dict со всеми полями:
        {symbol, ts_ms, price, bid, ask, bid_size, ask_size, mark_price?}
        """
        try:
            symbol = d["symbol"] if "symbol" in d else d["s"]
            ts_ms = int(d.get("ts_ms") or d.get("E") or d.get("T"))
            price = float(d.get("price") or (float(d["b"]) + float(d["a"])) / 2.0)
            bid = float(d.get("bid") or d["b"])
            ask = float(d.get("ask") or d["a"])
            bid_sz = float(d.get("bid_size") or d.get("B", 0.0))
            ask_sz = float(d.get("ask_size") or d.get("A", 0.0))
            mark_price = d.get("mark_price")
            mark = float(mark_price) if mark_price is not None else None

            return TickerTick(
                symbol=symbol,
                ts_ms=ts_ms,
                price=price,
                bid=bid,
                ask=ask,
                bid_size=bid_sz,
                ask_size=ask_sz,
                mark_price=mark,
            )
        except Exception:
            return None
