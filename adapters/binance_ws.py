# adapters/binance_ws.py
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class BinanceWS:
    """
    Тонкий WS-адаптер Binance (spot/futures) для комбинированных потоков:
      - подписка на {symbol}@bookTicker и {symbol}@markPrice(@1s)
      - хранит последние book/mark по каждому символу
      - при любом апдейте эмитит ОДНО объединённое событие:
        {
          "symbol": "BTCUSDT",
          "ts_ms": 1739820000000, "ts": 1739820000,
          "price": mid,
          "bid": ..., "ask": ..., "bid_size": ..., "ask_size": ...,
          "mark_price":  ... (опционально)
        }
      - отдаёт raw dict через внешний on_message(raw: dict) -> Awaitable[None]
      - устойчивые переподключения: экспоненциальный backoff с джиттером и cap
    """

    # ------------- lifecycle -------------

    def __init__(
        self,
        symbols: List[str],
        *,
        futures: bool = True,
        mark_interval_1s: bool = True,
        reconnect_min_delay_s: float = 1.0,
        reconnect_max_delay_s: float = 30.0,
        http_timeout_s: float = 20.0,
        heartbeat_s: int = 20,
    ) -> None:
        self._symbols = [s.upper() for s in symbols]
        self._futures = futures
        self._mark_suffix = "@markPrice@1s" if mark_interval_1s else "@markPrice"
        self._reconnect_min = max(0.1, float(reconnect_min_delay_s))
        self._reconnect_max = max(self._reconnect_min, float(reconnect_max_delay_s))
        self._http_timeout_s = float(http_timeout_s)
        self._heartbeat_s = int(heartbeat_s)

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._runner_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        # кэш последних значений для мерджа
        self._book: Dict[str, Dict[str, float]] = {}  # {sym: {"bid":..., "ask":..., "bid_size":..., "ask_size":...}}
        self._mark: Dict[str, float] = {}             # {sym: mark_price}
        self._lock = asyncio.Lock()

        # внешний колбэк
        self._on_message: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None

        # диагностика
        self._diag: Dict[str, Any] = {
            "connects": 0,
            "messages": 0,
            "book_updates": 0,
            "mark_updates": 0,
            "merged_emits": 0,
            "errors": 0,
            "last_rx_ms": 0,
            "last_emit_ms": 0,
            "url": "",
            "symbols": list(self._symbols),
            "last_error": "",
        }

    async def connect(self, on_message: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        """
        Старт фонового процесса с авто-переподключением.
        Совместимо с имеющимся кодом (вызывает внутренний runner).
        """
        self._on_message = on_message
        self._stop.clear()
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._http_timeout_s))
        if self._runner_task is None or self._runner_task.done():
            self._runner_task = asyncio.create_task(self._runner(), name="binance-ws-runner")

    async def close(self) -> None:
        """Остановка и закрытие WS/HTTP."""
        self._stop.set()
        if self._runner_task:
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
            self._runner_task = None
        await self._close_ws()
        # ВАЖНО: закрываем и HTTP-сессию
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    async def _close_ws(self) -> None:
        # Корректно закрываем только вебсокет; сессию — в close()
        if self._ws and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None


    # ------------- helpers -------------

    def diag(self) -> Dict[str, Any]:
        """Снимок диагностической информации (для /status|/debug/diag)."""
        # без мьютекса: только копия простых значений
        d = dict(self._diag)
        d["symbols"] = list(self._symbols)
        return d

    def set_symbols(self, symbols: List[str]) -> None:
        """
        Позволяет поменять набор символов (на следующем переподключении URL обновится).
        """
        self._symbols = [s.upper() for s in symbols]
        self._diag["symbols"] = list(self._symbols)

    # ------------- internal -------------

    def _build_url(self) -> str:
        base = "wss://fstream.binance.com/stream" if self._futures else "wss://stream.binance.com:9443/stream"
        parts: List[str] = []
        for s in self._symbols:
            parts.append(f"{s.lower()}@bookTicker")
        for s in self._symbols:
            parts.append(f"{s.lower()}{self._mark_suffix}")  # на spot тоже поддерживается @markPrice@1s
        return f"{base}?streams={'/'.join(parts)}"

    async def _runner(self) -> None:
        """Основной цикл с переподключением и чтением сообщений (jittered backoff)."""
        backoff = self._reconnect_min
        assert self._session is not None

        while not self._stop.is_set():
            url = self._build_url()
            self._diag["url"] = url
            try:
                async with self._session.ws_connect(url, heartbeat=self._heartbeat_s) as ws:
                    self._ws = ws
                    self._diag["connects"] += 1
                    backoff = self._reconnect_min  # сброс после успешного коннекта
                    async for msg in ws:
                        if self._stop.is_set():
                            break
                        self._diag["last_rx_ms"] = _now_ms()
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._diag["messages"] += 1
                            await self._handle_ws_text(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            # крайне редко — на всякий случай поддержим и бинарный текст
                            try:
                                payload = msg.data.decode("utf-8", errors="ignore")
                                self._diag["messages"] += 1
                                await self._handle_ws_text(payload)
                            except Exception as e:
                                self._diag["errors"] += 1
                                self._diag["last_error"] = f"BINARY decode: {type(e).__name__}"
                                logger.exception("WS binary decode failed")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                # мягкий бэкофф с джиттером, cap по максимуму
                self._diag["errors"] += 1
                self._diag["last_error"] = f"WS loop: {type(e).__name__}"
                logger.exception("WS loop error")
                sleep_base = min(backoff, self._reconnect_max)
                jitter = random.uniform(0.0, sleep_base * 0.3)
                await asyncio.sleep(sleep_base + jitter)
                backoff = min(backoff * 2.0, self._reconnect_max)
            finally:
                await self._close_ws()

    async def _close_ws(self) -> None:
        if self._ws and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None
        if self._session:
            # не закрываем сессию каждый цикл — она шарится;
            # закрытие делаем в close() или при остановке runner'а в finally
            pass

    async def _handle_ws_text(self, text: str) -> None:
        """
        Combined streams формат:
          {"stream": "btcusdt@bookTicker", "data": {...}}
        Нас интересуют bookTicker и markPriceUpdate. Бывает, что 'e' отсутствует —
        тогда определяем по набору полей.
        """
        try:
            obj = json.loads(text)
        except Exception as e:
            self._diag["errors"] += 1
            self._diag["last_error"] = f"JSON: {type(e).__name__}"
            return

        data = obj.get("data", obj)
        if not isinstance(data, dict):
            return

        ev = data.get("e")
        if ev == "bookTicker" or self._looks_like_book_ticker(data):
            await self._on_book_ticker(data)
            return

        if ev == "markPriceUpdate" or self._looks_like_mark_price(data):
            await self._on_mark_price(data)
            return

        # прочие события игнорируем

    @staticmethod
    def _looks_like_book_ticker(d: Dict[str, Any]) -> bool:
        # bookTicker обычно содержит ключи: s,b,a (и B,A для сайза)
        return "s" in d and "b" in d and "a" in d

    @staticmethod
    def _looks_like_mark_price(d: Dict[str, Any]) -> bool:
        # markPriceUpdate содержит s и p/P; иногда ещё i=индекс. Цена — p/P
        return "s" in d and ("p" in d or "P" in d)

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
        except Exception as e:
            self._diag["errors"] += 1
            self._diag["last_error"] = f"book parse: {type(e).__name__}"
            return

        async with self._lock:
            self._book[symbol] = {
                "bid": bid,
                "ask": ask,
                "bid_size": bid_sz,
                "ask_size": ask_sz,
            }
            self._diag["book_updates"] += 1
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

        if mark_price is not None:
            async with self._lock:
                self._mark[symbol] = mark_price
                self._diag["mark_updates"] += 1

        # эмитим только если уже есть book — иначе нечем заполнить bid/ask
        await self._emit_merged(symbol, ts_ms=int(d.get("E") or _now_ms()))

    async def _emit_merged(self, symbol: str, ts_ms: int) -> None:
        """
        Собирает объединённый raw-снап и отдаёт его наружу через on_message().
        Если нет book — ничего не посылаем (coalescer не соберёт тик).
        """
        if self._on_message is None:
            return

        async with self._lock:
            book = self._book.get(symbol)
            mark = self._mark.get(symbol)

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
            "ts": ts_ms // 1000,   # для совместимости
            "price": mid,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_sz,
            "ask_size": ask_sz,
        }
        if mark is not None:
            raw["mark_price"] = mark

        try:
            await self._on_message(raw)
            self._diag["merged_emits"] += 1
            self._diag["last_emit_ms"] = ts_ms
        except Exception as e:
            # не роняем поток из-за ошибки внешнего обработчика
            self._diag["errors"] += 1
            self._diag["last_error"] = f"on_message: {type(e).__name__}"
            logger.exception("on_message handler failed")
