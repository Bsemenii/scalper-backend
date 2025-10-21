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
    Стабилизатор тиков:
      • Принимает raw dict события (WS/хаб).
      • Раз в окно coalesce_ms выпускает по ОДНОМУ тіку на символ (последний в окне).
      • Никаких зависимостей от hub/fastapi/и т.п.

    Требуемые поля raw (combined-формат от BinanceWS):
      symbol(str), ts_ms(int|str), bid(float), ask(float), [price=float], [bid_size, ask_size], [mark_price]
    Допускается сырой bookTicker (b/a/B/A/E/s) — тогда mid рассчитывается на лету.
    """

    def __init__(self, coalesce_ms: int = 75) -> None:
        self._coalesce_ms = max(10, int(coalesce_ms))
        self._q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self._stop = asyncio.Event()

        # Последнее сырое событие по символу и флаг «грязно» для текущего окна.
        self._last_raw: Dict[str, Dict[str, Any]] = {}
        self._dirty: Dict[str, bool] = {}

        # Диагностика
        self._push_count: int = 0
        self._emit_count: int = 0
        self._last_tick_ts_ms: int = 0
        self._start_monotonic: float = time.monotonic()

    # ---------- публичный API ----------

    async def on_event(self, raw: Dict[str, Any]) -> None:
        """Положить raw-событие в очередь коалесцера."""
        await self._q.put(raw)
        self._push_count += 1

    async def run(self, on_tick: Callable[[TickerTick], Awaitable[None]]) -> None:
        """
        Главный цикл:
          — ждём новые события до конца ближайшего окна;
          — по таймауту окна эмитим по одному тіку на все «грязные» символы.
        """
        try:
            period = self._coalesce_ms / 1000.0
            next_flush = time.monotonic() + period

            while not self._stop.is_set():
                timeout = max(0.0, next_flush - time.monotonic())
                try:
                    # получаем одно событие с таймаутом до конца окна…
                    raw = await asyncio.wait_for(self._q.get(), timeout=timeout)
                    self._ingest_raw(raw)
                    # …а затем сглатываем все, что уже накопилось без ожидания
                    while True:
                        try:
                            raw2 = self._q.get_nowait()
                            self._ingest_raw(raw2)
                        except asyncio.QueueEmpty:
                            break
                except asyncio.TimeoutError:
                    # конец окна → эмитим «грязные» символы
                    if self._dirty:
                        for sym, fl in list(self._dirty.items()):
                            if not fl:
                                continue
                            r = self._last_raw.get(sym)
                            if not r:
                                self._dirty[sym] = False
                                continue
                            tick = self._to_tick(r)
                            if tick and self._spread_ok(tick):
                                await on_tick(tick)
                                self._emit_count += 1
                                self._last_tick_ts_ms = int(tick.ts_ms)
                            self._dirty[sym] = False
                    # планируем следующее окно от «сейчас» (без дрейфа)
                    next_flush = time.monotonic() + period
        except asyncio.CancelledError:
            # мягкое завершение
            pass

    async def close(self) -> None:
        """Остановить коалесцер (завершит run-петлю)."""
        self._stop.set()

    def stats(self) -> Dict[str, Any]:
        """Короткая диагностика для /status."""
        elapsed = max(1e-6, time.monotonic() - (self._start_monotonic or time.monotonic()))
        emits_per_sec = self._emit_count / elapsed
        return {
            "push_count": self._push_count,
            "emit_count": self._emit_count,
            "emits_per_sec": round(emits_per_sec, 3),
            "last_tick_ts_ms": self._last_tick_ts_ms,
            "window_ms": self._coalesce_ms,
        }

    # ---------- внутреннее ----------

    def _ingest_raw(self, d: Dict[str, Any]) -> None:
        """Нормализуем и кладём последнее raw по символу, помечаем «грязно»."""
        sym = (d.get("symbol") or d.get("s") or "").upper()
        if not sym:
            return

        ts_ms = d.get("ts_ms") or d.get("E") or d.get("T")
        d["ts_ms"] = int(ts_ms) if ts_ms is not None else int(time.time() * 1000)

        self._last_raw[sym] = d
        self._dirty[sym] = True

    @staticmethod
    def _spread_ok(t: TickerTick) -> bool:
        """Базовая санитация спреда, чтобы не пускать мусор вниз по пайплайну."""
        try:
            return (t.bid > 0.0) and (t.ask > 0.0) and (t.ask >= t.bid)
        except Exception:
            return False

    def _to_tick(self, d: Dict[str, Any]) -> Optional[TickerTick]:
        """
        Поддерживает два формата:
          1) combined: symbol, ts_ms, bid, ask, [price], [bid_size, ask_size], [mark_price]
          2) сырой bookTicker: s, E, b, a, B, A
        """
        try:
            # combined
            if "bid" in d and "ask" in d:
                symbol = (d.get("symbol") or d.get("s") or "").upper()
                ts_ms = int(d.get("ts_ms") or int(time.time() * 1000))
                bid = float(d["bid"])
                ask = float(d["ask"])
                bid_sz = float(d.get("bid_size", d.get("B", 0.0)))
                ask_sz = float(d.get("ask_size", d.get("A", 0.0)))
                price = float(d.get("price", (bid + ask) / 2.0))
                mark = d.get("mark_price")
                mark_price = float(mark) if mark is not None else None
                return TickerTick(symbol, ts_ms, price, bid, ask, bid_sz, ask_sz, mark_price)

            # сырой bookTicker
            if "b" in d and "a" in d:
                symbol = (d.get("s") or d.get("symbol") or "").upper()
                ts_ms = int(d.get("E") or d.get("T") or d.get("ts_ms") or int(time.time() * 1000))
                bid = float(d["b"])
                ask = float(d["a"])
                bid_sz = float(d.get("B", 0.0))
                ask_sz = float(d.get("A", 0.0))
                price = (bid + ask) / 2.0
                mark = d.get("mark_price")
                mark_price = float(mark) if mark is not None else None
                return TickerTick(symbol, ts_ms, price, bid, ask, bid_sz, ask_sz, mark_price)
        except Exception:
            return None
        return None
