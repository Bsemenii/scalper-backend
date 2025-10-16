# adapters/binance_ws.py
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp


def _now_ms() -> int:
    return int(time.time() * 1000)


class BinanceWS:
    """
    Тонкий WS-адаптер Binance:
      - подписка на {symbol}@bookTicker и {symbol}@markPrice(@1s)
      - хранит последние book/mark по каждому символу
      - при любом апдейте эмитит ОДНО объединённое событие:
        {
          "symbol": "BTCUSDT", "ts_ms": 123, "price": mid,
          "bid": ..., "ask": ..., "bid_size": ..., "ask_size": ...,
          "mark_price":  ...
        }
      - никаких импортов из stream.hub / coalescer — только raw dict → внешний on_message()
    """

    def __init__(
        self,
        symbols: List[str],
        *,
        futures: bool = True,
        mark_interval_1s: bool = True,
        reconnect_min_delay_s: float = 1.0,
        reconnect_max_delay_s: float = 8.0,
        http_timeout_s: float = 20.0,
    ) -> None:
        self._symbols = [s.upper() for s in symbols]
        self._futures = futures
        self._mark_suffix = "@markPrice@1s" if mark_interval_1s else "@markPrice"
        self._reconnect_min = reconnect_min_delay_s
        self._reconnect_max = reconnect_max_delay_s
        self._http_timeout_s = http_timeout_s

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._runner_task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

        # кэш последних значений для мерджа
        self._book: Dict[str, Dict[str, float]] = {}  # {sym: {"bid":..., "ask":..., "bid_size":..., "ask_size":...}}
        self._mark: Dict[str, float] = {}             # {sym: mark_price}
        self._lock = asyncio.Lock()

        # внешний колбэк
        self._on_message: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None

    # ------------- public API -------------

    async def connect(self, on_message: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        """Старт фонового процесса с авто-переподключением."""
        self._on_message = on_message
        self._stopping.clear()
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._http_timeout_s))
        self._runner_task = asyncio.create_task(self._runner())

    async def close(self) -> None:
        """Остановка и закрытие WS/HTTP."""
        self._stopping.set()
        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    # ------------- internal -------------

    def _build_url(self) -> str:
        base = "wss://fstream.binance.com/stream" if self._futures else "wss://stream.binance.com:9443/stream"
        parts: List[str] = []
        for s in self._symbols:
            parts.append(f"{s.lower()}@bookTicker")
        for s in self._symbols:
            # для спота тоже доступен markPrice@1s, оставим универсально
            parts.append(f"{s.lower()}{self._mark_suffix}")
        streams = "/".join(parts)
        return f"{base}?streams={streams}"

    async def _runner(self) -> None:
        """Основной цикл с переподключением и чтением сообщений."""
        backoff = self._reconnect_min
        assert self._session is not None

        while not self._stopping.is_set():
            url = self._build_url()
            try:
                async with self._session.ws_connect(url, heartbeat=30) as ws:
                    self._ws = ws
                    backoff = self._reconnect_min  # сбросить бэкофф после успешного коннекта
                    async for msg in ws:
                        if self._stopping.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_ws_text(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            # бинанс иногда шлёт gzip — aiohttp уже разжимает в TEXT,
                            # на всякий случай поддержим бинарь как текст
                            try:
                                payload = msg.data.decode("utf-8")
                                await self._handle_ws_text(payload)
                            except Exception:
                                continue
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            break
            except asyncio.CancelledError:
                break
            except Exception:
                # тихо подождём и переподключимся
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._reconnect_max)
            finally:
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

        # выход
        return

    async def _handle_ws_text(self, text: str) -> None:
        """
        Binance combined streams формат:
          {"stream": "btcusdt@bookTicker", "data": {...}}
        Нас интересуют два типа событий: bookTicker и markPriceUpdate.
        """
        try:
            obj = json.loads(text)
        except Exception:
            return

        data = obj.get("data", obj)
        if not isinstance(data, dict):
            return

        ev = data.get("e")
        if ev == "bookTicker":
            await self._on_book_ticker(data)
        elif ev == "markPriceUpdate":
            await self._on_mark_price(data)
        else:
            # некоторые реализации возвращают "miniTicker" или иные — игнорим
            return

    # ---------- parsers & merge ----------

    async def _on_book_ticker(self, d: Dict[str, Any]) -> None:
        """
        Пример payload:
          {
            "e":"bookTicker","E":1680000000000,"u":..., "s":"BTCUSDT",
            "b":"28000.10","B":"1.23","a":"28000.20","A":"2.34"
          }
        """
        symbol = str(d.get("s", "")).upper()
        if not symbol:
            return

        try:
            bid = float(d["b"])
            ask = float(d["a"])
            bid_sz = float(d.get("B", 0.0))
            ask_sz = float(d.get("A", 0.0))
        except Exception:
            return

        async with self._lock:
            self._book[symbol] = {
                "bid": bid,
                "ask": ask,
                "bid_size": bid_sz,
                "ask_size": ask_sz,
            }
            await self._emit_merged(symbol, ts_ms=int(d.get("E") or _now_ms()))

    async def _on_mark_price(self, d: Dict[str, Any]) -> None:
        """
        Futures markPriceUpdate payload:
          {"e":"markPriceUpdate","E":1680000000000,"s":"BTCUSDT","p":"28001.12", ...}
        Иногда поле может называться 'p' или 'P' (строка). Поддержим оба.
        """
        symbol = str(d.get("s", "")).upper()
        if not symbol:
            return

        p = d.get("p", d.get("P"))
        try:
            mark_price = float(p) if p is not None else None
        except Exception:
            mark_price = None

        async with self._lock:
            if mark_price is not None:
                self._mark[symbol] = mark_price
            # эмитим только если уже есть book — иначе нечем заполнить bid/ask
            await self._emit_merged(symbol, ts_ms=int(d.get("E") or _now_ms()))

    async def _emit_merged(self, symbol: str, ts_ms: int) -> None:
        """
        Собирает объединённый raw-снап и отдаёт его наружу через on_message().
        Если нет book — ничего не посылаем (coalescer не соберёт тик).
        """
        if self._on_message is None:
            return

        book = self._book.get(symbol)
        if not book:
            return

        bid = book["bid"]
        ask = book["ask"]
        bid_sz = book["bid_size"]
        ask_sz = book["ask_size"]
        mid = (bid + ask) / 2.0

        raw = {
            "symbol": symbol,
            "ts_ms": ts_ms,
            "price": mid,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_sz,
            "ask_size": ask_sz,
        }

        mark = self._mark.get(symbol)
        if mark is not None:
            raw["mark_price"] = mark

        try:
            await self._on_message(raw)
        except Exception:
            # не роняем поток из-за ошибки внешнего обработчика
            pass
