from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import ssl
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode

import aiohttp
import certifi

logger = logging.getLogger(__name__)


# -----------------------------
#  Общие структуры / исключения
# -----------------------------


@dataclass
class OrderReq:
    """
    Структура запроса на создание ордера.
    Совместима с Binance Futures USDT-M.

    ВАЖНО:
    - executor может передавать сюда объект с полем stop_price или stop_px,
      адаптер это учитывает мягко.
    - client_order_id используется как newClientOrderId.
    """

    symbol: str
    side: str          # "BUY" / "SELL"
    type: str          # "LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET", ...
    qty: float
    price: Optional[float] = None
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    stop_price: Optional[float] = None   # для STOP/TP ордеров
    close_position: bool = False         # для STOP_MARKET / TP_MARKET closePosition=true

    @property
    def stop_px(self) -> Optional[float]:
        """Алиас: stop_px == stop_price (для обратной совместимости)."""
        return self.stop_price


class BinanceRestError(RuntimeError):
    """
    Ошибка REST-вызова к Binance.
    Включает HTTP-статус и, если есть, binance code/msg.
    """

    def __init__(
        self,
        message: str,
        http_status: int,
        binance_code: Optional[int] = None,
        binance_msg: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.binance_code = binance_code
        self.binance_msg = binance_msg

    def __str__(self) -> str:
        base = f"[HTTP {self.http_status}] {super().__str__()}"
        if self.binance_code is not None:
            base += f" (code={self.binance_code}, msg={self.binance_msg})"
        return base


# -----------------------------
#  Binance USDT-M REST Adapter
# -----------------------------


class BinanceUSDTMAdapter:
    """
    REST-адаптер для Binance USDT-M Futures.

    - aiohttp + SSL-контекст на базе certifi
    - подписанные запросы (ордеры, аккаунт, позиции, userTrades)
    - базовые методы: баланс, аккаунт, позиции, ордера, маржа, плечо.

    ВАЖНО:
    - интерфейс совместим с текущим executor/worker;
    - create_order аккуратно обрабатывает типы ордеров и stopPrice/workingType;
    - все параметры и подпись пересобираются на каждом ретрае.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        recv_window: int = 60_000,
        request_timeout: int = 10,
        default_working_type: str = "MARK_PRICE",
    ) -> None:
        """
        recv_window поднят до 60_000 мс, чтобы уменьшить чувствительность
        к небольшим расхождениям по времени.

        base_url по умолчанию — основной фьючерсный endpoint.
        Для тестнета можно передать другой URL извне.

        default_working_type:
          - "MARK_PRICE" (рекомендуется)
          - "CONTRACT_PRICE"
        Используется для стоп-ордеров, если не задан working_type в req.
        """
        self._api_key = api_key
        self._api_secret = api_secret.encode("utf-8")
        self._base_url = base_url.rstrip("/")
        self._recv_window = recv_window
        self._request_timeout = request_timeout
        self._default_working_type = default_working_type.upper()

        self._session: Optional[aiohttp.ClientSession] = None

        # SSL-контекст c certifi, чтобы не было CERTIFICATE_VERIFY_FAILED
        self._ssl_ctx = ssl.create_default_context(cafile=certifi.where())

        # Смещение локального времени относительно serverTime Binance (мс)
        self._time_offset_ms: int = 0
        # Когда в последний раз синхали время (локальное время, мс)
        self._last_time_sync_ms: int = 0
        # Как часто обновлять время (по умолчанию раз в минуту)
        self._time_sync_interval_ms: int = 60_000

    # ---------- Внутренняя инфраструктура ----------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """
        Гарантируем один живой ClientSession с правильным SSL-контекстом.
        """
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=self._ssl_ctx)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self) -> None:
        """
        Аккуратно закрыть HTTP-сессию.
        """
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _sync_time(self) -> None:
        """
        Жёсткая синхронизация времени с /fapi/v1/time.
        """
        url = f"{self._base_url}/fapi/v1/time"
        sess = await self._ensure_session()

        try:
            async with sess.get(url, timeout=self._request_timeout) as resp:
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    logger.warning("[binance_rest] time sync non-JSON response: %s", text)
                    return

                server_time = int(data.get("serverTime"))
        except Exception as e:
            logger.warning("[binance_rest] failed to sync time: %s", e)
            return

        local_ms = int(time.time() * 1000)
        self._time_offset_ms = server_time - local_ms
        self._last_time_sync_ms = local_ms
        logger.info(
            "[binance_rest] time sync: server=%s local=%s offset_ms=%s",
            server_time,
            local_ms,
            self._time_offset_ms,
        )

    async def _ensure_time_synced(self) -> None:
        """
        Периодически обновляет смещение времени.
        Вызывается перед важными подписанными запросами и при -1021.
        """
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_time_sync_ms > self._time_sync_interval_ms:
            await self._sync_time()

    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Добавить timestamp, recvWindow и signature к параметрам.
        Timestamp берётся с учётом _time_offset_ms.
        ВНИМАНИЕ: не мутирует исходный dict.
        """
        p = dict(params)

        if "timestamp" not in p:
            local_ms = int(time.time() * 1000)
            p["timestamp"] = local_ms + int(self._time_offset_ms)

        if "recvWindow" not in p and self._recv_window:
            p["recvWindow"] = self._recv_window

        query = urlencode(p, doseq=True)
        sig = hmac.new(self._api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    @staticmethod
    def _fmt_decimal(x: float) -> str:
        """
        Форматирование числа без экспоненты, с сохранением точности.
        """
        d = Decimal(str(x))
        d = d.normalize()
        return format(d, "f")

    @staticmethod
    def _bool_to_str(v: bool) -> str:
        """
        Binance ожидает true/false в query, но в конечном итоге это строки "true"/"false".
        """
        return "true" if v else "false"

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Базовый REST-запрос к Binance Futures.

        - Если signed=True — ensure_time_synced() + подпись.
        - Для GET/DELETE параметры идут в query.
        - Для POST/PUT тоже используем query-параметры (совместимо с Binance).
        - При code = -1021 один раз синхронизируем время и повторяем запрос.
        - При таймаутах/сетевых ошибках — до 2 попыток.
        """
        url = f"{self._base_url}{path}"
        sess = await self._ensure_session()
        base_params: Dict[str, Any] = dict(params or {})

        headers: Dict[str, str] = {}
        # API-ключ можно слать и для публичных методов — безопасно и удобно
        if self._api_key:
            headers["X-MBX-APIKEY"] = self._api_key

        def _prepare_kwargs(p: Dict[str, Any]) -> Dict[str, Any]:
            # Binance предпочитает query-параметры и для POST/DELETE
            return {"params": p}

        attempt = 0
        max_attempts = 2  # основная попытка + один ретрай (например, после -1021)

        while True:
            attempt += 1

            # Подпись/время делаем на КАЖДОМ ретрае, чтобы timestamp всегда был свежий
            params_to_send = dict(base_params)
            if signed:
                await self._ensure_time_synced()
                params_to_send = self._sign_params(params_to_send)

            kwargs = _prepare_kwargs(params_to_send)

            try:
                async with sess.request(
                    method,
                    url,
                    headers=headers,
                    timeout=self._request_timeout,
                    **kwargs,
                ) as resp:
                    text = await resp.text()

                    try:
                        data: Union[Dict[str, Any], List[Any], str] = json.loads(text)
                    except json.JSONDecodeError:
                        data = text

                    if 200 <= resp.status < 300:
                        # успех
                        return data  # type: ignore[return-value]

                    # обработка ошибок Binance
                    binance_code: Optional[int] = None
                    binance_msg: Optional[str] = None
                    if isinstance(data, dict):
                        try:
                            binance_code = int(data.get("code")) if "code" in data else None
                        except Exception:
                            binance_code = None
                        if "msg" in data:
                            binance_msg = str(data.get("msg"))

                    # спец-случай: -1021 (timestamp)
                    if binance_code == -1021 and attempt < max_attempts:
                        logger.warning(
                            "[binance_rest] timestamp error (-1021), syncing time and retrying: %s",
                            binance_msg,
                        )
                        await self._sync_time()
                        continue

                    msg = f"Binance REST error: {data}"
                    raise BinanceRestError(
                        message=msg,
                        http_status=resp.status,
                        binance_code=binance_code,
                        binance_msg=binance_msg,
                    )

            except asyncio.TimeoutError as e:
                logger.warning(
                    "[binance_rest] request timeout %s %s params=%s attempt=%s: %s",
                    method,
                    path,
                    base_params,
                    attempt,
                    e,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                raise BinanceRestError(
                    message="Binance REST request timeout",
                    http_status=0,
                ) from e
            except (aiohttp.ClientError, OSError) as e:
                logger.warning(
                    "[binance_rest] network error %s %s params=%s attempt=%s: %s",
                    method,
                    path,
                    base_params,
                    attempt,
                    e,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                raise BinanceRestError(
                    message=f"Binance REST network error: {e}",
                    http_status=0,
                ) from e

    # ---------- Публичные методы адаптера (аккаунт / позиции) ----------

    async def get_balance(self) -> List[Dict[str, Any]]:
        """
        /fapi/v2/balance — баланс по активам.
        Возвращает СЫРОЙ ответ Binance (список dict).
        """
        return await self._request("GET", "/fapi/v2/balance", signed=True)  # type: ignore[return-value]

    async def get_account(self) -> Dict[str, Any]:
        """
        /fapi/v2/account — сводная информация по аккаунту.
        Возвращает СЫРОЙ dict Binance.
        """
        return await self._request("GET", "/fapi/v2/account", signed=True)  # type: ignore[return-value]

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        /fapi/v2/positionRisk — список позиций по всем символам.
        Возвращает СЫРОЙ список dict.
        """
        return await self._request("GET", "/fapi/v2/positionRisk", signed=True)  # type: ignore[return-value]

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        GET /fapi/v1/ticker/price — текущая цена символа.
        Используется для конвертации комиссий из других активов в USDT.
        
        Возвращает цену как float или None при ошибке.
        """
        try:
            data = await self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol}, signed=False)  # type: ignore[assignment]
            if isinstance(data, dict) and "price" in data:
                return float(data["price"])
            return None
        except Exception as e:
            logger.warning("[binance_rest] get_price failed for %s: %s", symbol, e)
            return None

    # ---------- Ордера ----------

    async def create_order(self, req: Any) -> Dict[str, Any]:
        """
        POST /fapi/v1/order — создание ордера.

        Поддерживает:
        - LIMIT / MARKET
        - STOP / TAKE_PROFIT (лимитные)
        - STOP_MARKET / TAKE_PROFIT_MARKET (маркет по триггеру)
        - reduceOnly / closePosition
        - workingType (MARK_PRICE / CONTRACT_PRICE) для стопов

        ВАЖНО:
        - req может быть любым объектом с нужными атрибутами
          (qty, price, stop_price/stop_px, time_in_force, reduce_only, close_position, working_type, client_order_id).
        - Возвращает СЫРОЙ ответ Binance.
        """
        symbol = getattr(req, "symbol")
        side = getattr(req, "side")
        otype = getattr(req, "type")
        otype_upper = str(otype).upper()

        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": otype_upper,
        }

        # Кол-во
        qty = getattr(req, "qty", None)
        if qty is not None:
            params["quantity"] = self._fmt_decimal(float(qty))

        # Цена и TIF: для LIMIT / STOP / TAKE_PROFIT (лимитные триггеры)
        price = getattr(req, "price", None)
        tif = getattr(req, "time_in_force", None)
        tif_upper = str(tif).upper() if tif is not None else None

        if otype_upper in ("LIMIT", "STOP", "TAKE_PROFIT"):
            if price is not None:
                params["price"] = self._fmt_decimal(float(price))
            if tif_upper is not None:
                params["timeInForce"] = tif_upper

        # Для MARKET / STOP_MARKET / TAKE_PROFIT_MARKET price/timeInForce НЕ отправляем
        # (это чистые маркет-ордера по триггеру или немедленные MARKET)

        # reduceOnly
        reduce_only = bool(getattr(req, "reduce_only", False))
        if reduce_only:
            params["reduceOnly"] = self._bool_to_str(True)

        # client order id
        coid = getattr(req, "client_order_id", None)
        if coid:
            params["newClientOrderId"] = coid

        # stop_price / stop_px
        stop_px = getattr(req, "stop_price", None)
        if stop_px is None:
            stop_px = getattr(req, "stop_px", None)

        if otype_upper in ("STOP", "TAKE_PROFIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"):
            if stop_px is not None:
                params["stopPrice"] = self._fmt_decimal(float(stop_px))
            else:
                logger.warning(
                    "[binance_rest] create_order %s %s without stopPrice",
                    symbol,
                    otype_upper,
                )

        # workingType: тип цены для стопа
        working_type = getattr(req, "working_type", None)
        if working_type is not None:
            wt = str(working_type).upper()
        else:
            # Если это стоп-ордер и working_type не задан — используем default_working_type
            if otype_upper in ("STOP", "TAKE_PROFIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"):
                wt = self._default_working_type
            else:
                wt = ""

        if wt:
            params["workingType"] = wt

        # closePosition: только для STOP_MARKET / TAKE_PROFIT_MARKET
        close_position = bool(getattr(req, "close_position", False))
        if close_position and otype_upper in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            params["closePosition"] = self._bool_to_str(True)

        logger.debug("[binance_rest] create_order params=%s", params)
        return await self._request("POST", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        DELETE /fapi/v1/order — отмена ордера.
        Можно указать либо orderId, либо origClientOrderId.

        Возвращает СЫРОЙ ответ Binance.
        """
        params: Dict[str, Any] = {"symbol": symbol}

        if order_id is not None:
            params["orderId"] = order_id

        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        logger.debug("[binance_rest] cancel_order params=%s", params)
        return await self._request("DELETE", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]

    async def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        GET /fapi/v1/order — получить информацию по конкретному ордеру.

        Возвращает СЫРОЙ dict Binance.
        """
        params: Dict[str, Any] = {"symbol": symbol}

        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        logger.debug("[binance_rest] get_order params=%s", params)
        return await self._request("GET", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]

    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/openOrders — список открытых ордеров.
        Можно ограничить одним символом.

        Возвращает СЫРОЙ список dict.
        """
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol

        logger.debug("[binance_rest] get_open_orders params=%s", params)
        return await self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)  # type: ignore[return-value]

    async def get_trades_for_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/userTrades — список трейдов по символу.
        Если order_id передан — фильтруем по нему на клиенте.

        Это основной источник правды по комиссиям (commission, commissionAsset)
        и фактическому PnL.

        Возвращает СЫРОЙ список dict Binance.
        """
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id

        trades = await self._request("GET", "/fapi/v1/userTrades", params=params, signed=True)  # type: ignore[assignment]
        if order_id is not None:
            filtered: List[Dict[str, Any]] = []
            for t in trades:
                try:
                    if int(t.get("orderId", -1)) == int(order_id):
                        filtered.append(t)
                except Exception:
                    continue
            trades = filtered
        return trades  # type: ignore[return-value]

    # ---------- Публичные методы адаптера (маржа / плечо) ----------

    async def set_margin_type(self, symbol: str, margin_type: str) -> Dict[str, Any]:
        """
        POST /fapi/v1/marginType — установка типа маржи для символа.

        margin_type: "ISOLATED" или "CROSSED"

        Возвращает СЫРОЙ dict Binance.
        """
        params = {
            "symbol": symbol,
            "marginType": margin_type.upper(),
        }
        logger.debug("[binance_rest] set_margin_type params=%s", params)
        return await self._request("POST", "/fapi/v1/marginType", params=params, signed=True)  # type: ignore[return-value]

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        POST /fapi/v1/leverage — установка плеча для символа.

        Возвращает СЫРОЙ dict Binance.
        """
        params = {
            "symbol": symbol,
            "leverage": leverage,
        }
        logger.debug("[binance_rest] set_leverage params=%s", params)
        return await self._request("POST", "/fapi/v1/leverage", params=params, signed=True)  # type: ignore[return-value]


# -----------------------------
#  Paper Adapter
# -----------------------------


class PaperAdapter:
    """
    Простейший paper-адаптер с тем же интерфейсом, что у BinanceUSDTMAdapter.
    Ничего никуда не шлёт, всё в памяти, но имитирует основные ответы.

    ВАЖНО:
    - Формат ответов максимально похож на Binance.
    - get_trades_for_order сейчас возвращает пустой список —
      executor в таком случае fallback'ается на расчёт комиссий по bps.
    """

    def __init__(self) -> None:
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._last_id: int = 0

        # Баланс и позиции можно дорабатывать под свою логику симуляции
        self._balance_usdt: float = 1000.0
        self._positions: Dict[str, Dict[str, Any]] = {}  # symbol -> positionRisk-like
        self._margin_type: Dict[str, str] = {}           # symbol -> "ISOLATED"/"CROSSED"
        self._leverage: Dict[str, int] = {}              # symbol -> leverage

    async def close(self) -> None:
        return None

    # ---------- Аккаунт / баланс / позиции ----------

    async def get_balance(self) -> List[Dict[str, Any]]:
        return [
            {
                "asset": "USDT",
                "balance": f"{self._balance_usdt:.8f}",
                "availableBalance": f"{self._balance_usdt:.8f}",
            }
        ]

    async def get_account(self) -> Dict[str, Any]:
        """
        Упрощённый /fapi/v2/account для paper:
        баланс + позиции.
        """
        return {
            "assets": [
                {
                    "asset": "USDT",
                    "walletBalance": f"{self._balance_usdt:.8f}",
                    "unrealizedProfit": "0.00000000",
                    "marginBalance": f"{self._balance_usdt:.8f}",
                    "maintMargin": "0.00000000",
                    "initialMargin": "0.00000000",
                    "positionInitialMargin": "0.00000000",
                    "openOrderInitialMargin": "0.00000000",
                }
            ],
            "positions": list(self._positions.values()),
        }

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Упрощённый /fapi/v2/positionRisk.
        Для paper — просто то, что лежит в self._positions.
        """
        return list(self._positions.values())

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Paper-версия get_price: возвращает примерную цену для конвертации комиссий.
        Для простоты возвращаем фиксированные значения для популярных пар.
        """
        # Примерные цены для популярных активов в USDT
        prices = {
            "BNBUSDT": 600.0,
            "BTCUSDT": 40000.0,
            "ETHUSDT": 2500.0,
        }
        return prices.get(symbol.upper())

    # ---------- Ордера ----------

    async def create_order(self, req: Any) -> Dict[str, Any]:
        """
        Paper-версия create_order: имитирует структуру ответа Binance.
        Для простоты ордера остаются в статусе NEW — исполнением занимается executor.
        """
        self._last_id += 1
        oid = self._last_id

        qty = float(getattr(req, "qty", 0.0))
        price = getattr(req, "price", None)

        coid = getattr(req, "client_order_id", None) or f"paper-{oid}"

        # мягко берём stop_price/stop_px
        stop_px = getattr(req, "stop_price", None)
        if stop_px is None:
            stop_px = getattr(req, "stop_px", None)

        order = {
            "symbol": getattr(req, "symbol"),
            "side": getattr(req, "side"),
            "type": getattr(req, "type"),
            "origQty": str(qty),
            "price": str(price) if price is not None else "0",
            "status": "NEW",
            "orderId": oid,
            "clientOrderId": coid,
            "reduceOnly": bool(getattr(req, "reduce_only", False)),
            "stopPrice": str(stop_px) if stop_px is not None else "0",
        }

        self._orders[coid] = order
        return order

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if client_order_id and client_order_id in self._orders:
            order = self._orders.pop(client_order_id)
            order["status"] = "CANCELED"
            return order

        if order_id is not None:
            for coid, od in list(self._orders.items()):
                if od.get("orderId") == order_id and od.get("symbol") == symbol:
                    od["status"] = "CANCELED"
                    self._orders.pop(coid, None)
                    return od

        # если ничего не нашли — возвращаем "UNKNOWN", чтобы Exec/FSM могли отреагировать
        return {
            "symbol": symbol,
            "orderId": order_id or -1,
            "clientOrderId": client_order_id or "",
            "status": "UNKNOWN",
        }

    async def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if client_order_id and client_order_id in self._orders:
            return self._orders[client_order_id]

        if order_id is not None:
            for od in self._orders.values():
                if od.get("orderId") == order_id and od.get("symbol") == symbol:
                    return od

        return {
            "symbol": symbol,
            "orderId": order_id or -1,
            "clientOrderId": client_order_id or "",
            "status": "UNKNOWN",
        }

    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for od in self._orders.values():
            if od.get("status") == "NEW":
                if symbol is None or od.get("symbol") == symbol:
                    result.append(od)
        return result

    async def get_trades_for_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Paper-версия userTrades: сейчас ничего не эмулируем и возвращаем пустой список.

        Executor, увидев пустой список, сможет fallback'нуться на расчёт комиссий
        по фиксированным bps. При желании сюда можно прикрутить полноценную симуляцию.
        """
        _ = (symbol, order_id, from_id, limit)
        return []

    # ---------- Маржа / плечо ----------

    async def set_margin_type(self, symbol: str, margin_type: str) -> Dict[str, Any]:
        margin_type = margin_type.upper()
        if margin_type not in ("ISOLATED", "CROSSED"):
            raise ValueError(f"Invalid margin_type for paper: {margin_type}")
        self._margin_type[symbol] = margin_type
        return {
            "symbol": symbol,
            "marginType": margin_type,
        }

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        if leverage < 1:
            raise ValueError("leverage must be >= 1 for paper")
        self._leverage[symbol] = leverage
        return {
            "symbol": symbol,
            "leverage": leverage,
        }