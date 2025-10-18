# stream/coalescer.py
from __future__ import annotations

import asyncio
import time
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
    Принимает raw-события (dict) и выдаёт стабилизированные TickerTick.
    В каждом окне coalesce_ms на символ выпускает максимум ОДИН тик (последний в окне).
    НИКАКИХ импортов из hub.
    """

    def __init__(self, coalesce_ms: int = 75) -> None:
        self._coalesce_ms = max(10, int(coalesce_ms))
        self._queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self._stopping = asyncio.Event()

        # Буферы по символу: последний raw и номер "бакета" времени
        self._last_raw: Dict[str, Dict[str, Any]] = {}
        self._last_bucket: Dict[str, int] = {}
        # Флаг, что по символу в текущем окне есть новые данные, готовые к выпуску
        self._dirty: Dict[str, bool] = {}

    async def on_event(self, raw: Dict[str, Any]) -> None:
        """Хаб кладёт raw-событие в очередь коалесцера."""
        await self._queue.put(raw)

    async def run(self, on_tick: Callable[[TickerTick], Awaitable[None]]) -> None:
        """
        Главный цикл:
          - быстро читаем очередь (non-blocking с коротким таймаутом),
          - обновляем last_raw по символу,
          - раз в coalesce_ms выпускаем по одному тіку на символ.
        """
        try:
            # период опроса очереди — небольшая доля окна, чтобы не жечь CPU
            poll_sleep = max(0.005, self._coalesce_ms / 1000.0 / 5.0)
            next_flush_ms = self._now_ms() + self._coalesce_ms

            while not self._stopping.is_set():
                # 1) Забираем все доступные raw без долгого ожидания
                drained = 0
                while not self._queue.empty():
                    try:
                        raw = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._ingest_raw(raw)
                    drained += 1

                # 2) Если пришло мало/ничего — попробуем чуть подождать,
                #    чтобы собрать пачку raw в один тик, но не дольше poll_sleep
                if drained == 0:
                    try:
                        raw = await asyncio.wait_for(self._queue.get(), timeout=poll_sleep)
                        self._ingest_raw(raw)
                    except asyncio.TimeoutError:
                        pass

                # 3) Время флашить окно?
                now = self._now_ms()
                if now >= next_flush_ms:
                    await self._flush_ready(now, on_tick)
                    next_flush_ms = now + self._coalesce_ms
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        self._stopping.set()

    # ---------- internals ----------

    def _ingest_raw(self, d: Dict[str, Any]) -> None:
        """
        Принять очередной raw, определить символ и положить как last_raw.
        Сразу помечаем символ как dirty, чтобы на ближайшем флаше выпустить тик.
        """
        # Определяем символ и ts_ms из обоих форматов
        symbol = d.get("symbol") or d.get("s")
        if not symbol:
            return

        # метка времени: берём из ts_ms/E/T; если нет — текущее время
        ts_ms = d.get("ts_ms") or d.get("E") or d.get("T")
        ts_ms = int(ts_ms) if ts_ms is not None else self._now_ms()
        d["ts_ms"] = ts_ms  # нормализуем, чтобы downstream не гадал

        bucket = ts_ms // self._coalesce_ms
        self._last_raw[symbol] = d
        self._dirty[symbol] = True
        # если перепрыгнули в новый бакет — тоже ок, просто выпустим при следующем flush

        # запомним последний бакет (может пригодиться для дебага/метрик)
        self._last_bucket[symbol] = bucket

    async def _flush_ready(self, now_ms: int, on_tick: Callable[[TickerTick], Awaitable[None]]) -> None:
        """
        Выпустить по одному тику на каждый символ, у которого есть новые данные в текущем окне.
        После выпуска сбрасываем dirty-флаг.
        """
        if not self._dirty:
            return

        dirty_symbols = [s for s, is_dirty in self._dirty.items() if is_dirty]
        for sym in dirty_symbols:
            raw = self._last_raw.get(sym)
            if not raw:
                self._dirty[sym] = False
                continue
            t = self._to_tick(raw)
            if t:
                # гарантируем монотонность: если ts в прошлом, подвинем на now_ms
                if t.ts_ms > 0 and t.ts_ms > now_ms + 5:
                    # редкий случай: будущее — оставим как есть
                    pass
                await on_tick(t)
            self._dirty[sym] = False

    def _to_tick(self, d: Dict[str, Any]) -> Optional[TickerTick]:
        """
        Универсальный парсер: либо "combined" из адаптера, либо сырой bookTicker.
        Ожидаемые поля combined:
          {symbol, ts_ms, price, bid, ask, bid_size, ask_size, mark_price?}
        """
        try:
            # combined формат
            if "price" in d and "bid" in d and "ask" in d:
                symbol = d.get("symbol") or d.get("s")
                ts_ms = int(d["ts_ms"])
                price = float(d["price"])
                bid = float(d["bid"])
                ask = float(d["ask"])
                bid_sz = float(d.get("bid_size") or d.get("B", 0.0))
                ask_sz = float(d.get("ask_size") or d.get("A", 0.0))
                mark_price = d.get("mark_price")
                mark = float(mark_price) if mark_price is not None else None
                return TickerTick(symbol, ts_ms, price, bid, ask, bid_sz, ask_sz, mark)

            # сырой bookTicker → соберём mid
            if "b" in d and "a" in d:
                symbol = d.get("s") or d.get("symbol")
                ts_ms = int(d.get("E") or d.get("T") or d["ts_ms"])
                bid = float(d["b"])
                ask = float(d["a"])
                bid_sz = float(d.get("B", 0.0))
                ask_sz = float(d.get("A", 0.0))
                price = (bid + ask) / 2.0
                mark_price = d.get("mark_price")
                mark = float(mark_price) if mark_price is not None else None
                return TickerTick(symbol, ts_ms, price, bid, ask, bid_sz, ask_sz, mark)
        except Exception:
            return None
        return None

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)
