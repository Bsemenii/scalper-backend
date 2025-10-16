# adapters/binance_ws.py
from __future__ import annotations

import json
import time
from typing import AsyncIterator, Iterable

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from stream.hub import Tick

# Binance Futures combined stream endpoint
FSTREAM = "wss://fstream.binance.com/stream"


def _sym(s: str) -> str:
    return s.lower()


def _streams(symbols: Iterable[str]) -> str:
    """BTCUSDT,ETHUSDT -> 'btcusdt@bookTicker/btcusdt@aggTrade/btcusdt@markPrice@1s/...'"""
    parts: list[str] = []
    for s in symbols:
        ls = _sym(s)
        parts.append(f"{ls}@bookTicker")
        parts.append(f"{ls}@aggTrade")
        parts.append(f"{ls}@markPrice@1s")
    return "/".join(parts)


# --- простая диагностика для /debug/ws ---
_last_rx_ts_ms: int | None = None
_msg_counter: int = 0


def ws_diag() -> dict:
    return {"count": _msg_counter, "last_rx_ts_ms": _last_rx_ts_ms}


@retry(stop=stop_after_attempt(10), wait=wait_exponential_jitter(0.5, 3.0))
async def binance_stream(symbols: Iterable[str]) -> AsyncIterator[Tick]:
    """
    Реальный комбинированный WS поток Binance Futures:
    bookTicker + aggTrade + markPrice@1s.
    Формируем нормализованные тик-снэпы Tick.
    """
    global _last_rx_ts_ms, _msg_counter

    streams = _streams(symbols)
    url = f"{FSTREAM}?streams={streams}"

    # кэши последних значений по символам
    last_bid: dict[str, float] = {}
    last_ask: dict[str, float] = {}
    last_bsz: dict[str, float] = {}
    last_asz: dict[str, float] = {}
    last_trade: dict[str, float] = {}
    last_mark: dict[str, float] = {}

    timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(url, heartbeat=20) as ws:
            async for msg in ws:
                if msg.type is aiohttp.WSMsgType.CLOSED:
                    break
                if msg.type is not aiohttp.WSMsgType.TEXT:
                    continue

                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                data = payload.get("data")
                if not data:
                    continue

                _msg_counter += 1
                _last_rx_ts_ms = int(time.time() * 1000)

                # В комбинированном стриме внутри "data":
                # bookTicker: иногда БЕЗ "e" (просто поля b/B, a/A), иногда c e="bookTicker"
                # aggTrade: e="aggTrade"
                # markPrice: e="markPriceUpdate"
                stype = data.get("e")
                sym = (data.get("s") or data.get("symbol") or "").upper()
                if not sym:
                    continue

                # --- FIX: принимаем оба варианта bookTicker ---
                is_book_ticker = (
                    (stype is None and "b" in data and "a" in data)  # без e
                    or (stype == "bookTicker")  # с e="bookTicker"
                )

                if is_book_ticker:
                    try:
                        bid = float(data["b"])
                        ask = float(data["a"])
                        bsz = float(data.get("B", 0.0))
                        asz = float(data.get("A", 0.0))
                    except Exception:
                        continue
                    last_bid[sym], last_ask[sym] = bid, ask
                    last_bsz[sym], last_asz[sym] = bsz, asz

                elif stype == "aggTrade":
                    # последняя цена сделки
                    p = data.get("p")
                    if p is not None:
                        try:
                            last_trade[sym] = float(p)
                        except Exception:
                            pass

                elif stype == "markPriceUpdate":
                    # марк-цена
                    p = data.get("p") or data.get("P")
                    if p is not None:
                        try:
                            last_mark[sym] = float(p)
                        except Exception:
                            pass

                # Собираем тик только когда есть bid/ask и хоть какая-то цена
                price = (
                    last_trade.get(sym)
                    or last_mark.get(sym)
                    or (
                        (last_bid.get(sym, 0.0) + last_ask.get(sym, 0.0)) / 2
                        if sym in last_bid and sym in last_ask
                        else None
                    )
                )
                bid = last_bid.get(sym)
                ask = last_ask.get(sym)
                if price is None or bid is None or ask is None:
                    continue

                yield Tick(
                    ts_ms=_last_rx_ts_ms,
                    symbol=sym,
                    price=float(price),
                    bid=float(bid),
                    ask=float(ask),
                    bid_size=float(last_bsz.get(sym, 0.0)),
                    ask_size=float(last_asz.get(sym, 0.0)),
                    mark_price=last_mark.get(sym),
                )
