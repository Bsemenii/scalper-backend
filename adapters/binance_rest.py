from __future__ import annotations

import asyncio  # <<< нужно для asyncio.TimeoutError
import hmac
import hashlib
import time
import ssl
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, List, Union

import aiohttp
import certifi
from urllib.parse import urlencode


# -----------------------------
#  Общие структуры / исключения
# -----------------------------


@dataclass
class OrderReq:
    """
    Структура запроса на создание ордера.
    Совместима с Binance Futures USDT-M.

    ВАЖНО: exeсutor может передавать сюда объект с полем
    либо stop_price, либо stop_px — адаптер это учитывает.
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

    # Мягкий алиас: если кто-то спросит stop_px — вернуть stop_price.
    @property
    def stop_px(self) -> Optional[float]:
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
#  Binance USDT-M Adapter
# -----------------------------


class BinanceUSDTMAdapter:
    """
    REST-адаптер для Binance USDT-M Futures.

    - Использует aiohttp с SSL-контекстом на базе certifi
    - Подписывает приватные запросы (ордеры, баланс, аккаунт и т.п.)
    - Поддерживает основные методы, нужные для прод-бота:
      баланс, аккаунт, позиции, ордера, плечо, маржин-тип.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        recv_window: int = 5_000,
        request_timeout: int = 10,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret.encode("utf-8")
        self._base_url = base_url.rstrip("/")
        self._recv_window = recv_window
        self._request_timeout = request_timeout

        self._session: Optional[aiohttp.ClientSession] = None

        # SSL-контекст c certifi, чтобы не было CERTIFICATE_VERIFY_FAILED
        self._ssl_ctx = ssl.create_default_context(cafile=certifi.where())

    # ---------- Внутренняя инфраструктура ----------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """
        Гарантируем, что есть один живой ClientSession
        с правильным SSL-контекстом.
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

    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Добавить timestamp, recvWindow и signature к параметрам.
        """
        p = dict(params)
        p.setdefault("timestamp", int(time.time() * 1000))
        p.setdefault("recvWindow", self._recv_window)

        query = urlencode(p, doseq=True)
        sig = hmac.new(self._api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Базовый REST-запрос к Binance Futures.

        - Если signed=True — добавляем подпись (timestamp, recvWindow, signature).
        - Для GET/DELETE параметры идут в query.
        - Для POST используем query-параметры (совместимо с Binance).
        """
        url = f"{self._base_url}{path}"
        sess = await self._ensure_session()
        params = params or {}

        if signed:
            params = self._sign_params(params)

        headers = {}
        if signed:
            headers["X-MBX-APIKEY"] = self._api_key
        else:
            if self._api_key:
                headers["X-MBX-APIKEY"] = self._api_key

        try:
            async with sess.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=self._request_timeout,
            ) as resp:
                text = await resp.text()
                # Пытаемся распарсить JSON всегда, даже при ошибке
                try:
                    data = await resp.json()
                except Exception:
                    data = None

                if resp.status != 200:
                    code = None
                    msg = None
                    if isinstance(data, dict):
                        code = data.get("code")
                        msg = data.get("msg")
                    raise BinanceRestError(
                        message=f"Binance REST error: {text}",
                        http_status=resp.status,
                        binance_code=code,
                        binance_msg=msg,
                    )

                if data is None:
                    raise BinanceRestError(
                        message=f"Binance REST non-JSON response: {text}",
                        http_status=resp.status,
                    )

                return data

        except asyncio.TimeoutError:
            raise BinanceRestError(
                message="Binance REST request timeout",
                http_status=0,
            )

    @staticmethod
    def _fmt_decimal(x: float) -> str:
        """
        Форматирование числа без экспоненты, с сохранением точности.
        """
        d = Decimal(str(x))
        return format(d.normalize(), "f")

    # ---------- Публичные методы адаптера (аккаунт / позиции) ----------

    async def get_balance(self) -> List[Dict[str, Any]]:
        """
        /fapi/v2/balance — баланс по активам.

        Обычно интересен USDT:
          [
            {
              "accountAlias": ...,
              "asset": "USDT",
              "balance": "1000.00000000",
              "availableBalance": "1000.00000000",
              ...
            },
            ...
          ]
        """
        return await self._request("GET", "/fapi/v2/balance", signed=True)  # type: ignore[return-value]

    async def get_account(self) -> Dict[str, Any]:
        """
        /fapi/v2/account — сводная информация по аккаунту:
        балансы, позиции, маржа, PnL и т.п.
        """
        return await self._request("GET", "/fapi/v2/account", signed=True)  # type: ignore[return-value]

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        /fapi/v2/positionRisk — список позиций по всем символам.
        """
        return await self._request("GET", "/fapi/v2/positionRisk", signed=True)  # type: ignore[return-value]

    # ---------- Публичные методы адаптера (ордеры) ----------

    async def create_order(self, req: Any) -> Dict[str, Any]:
        """
        POST /fapi/v1/order — создание ордера.

        Поддерживает:
        - LIMIT / MARKET
        - STOP_MARKET / TAKE_PROFIT_MARKET (через stopPrice)
        - reduceOnly / closePosition

        ВАЖНО: req может быть любым объектом с нужными атрибутами
        (qty, price, stop_price/stop_px, и т.д.), не обязательно именно OrderReq.
        """
        params: Dict[str, Any] = {
            "symbol": getattr(req, "symbol"),
            "side": getattr(req, "side"),
            "type": getattr(req, "type"),
        }

        qty = getattr(req, "qty", None)
        if qty is not None:
            params["quantity"] = self._fmt_decimal(float(qty))

        price = getattr(req, "price", None)
        if price is not None:
            params["price"] = self._fmt_decimal(float(price))

        tif = getattr(req, "time_in_force", None)
        if tif is not None:
            params["timeInForce"] = tif

        reduce_only = bool(getattr(req, "reduce_only", False))
        if reduce_only:
            params["reduceOnly"] = "true"

        coid = getattr(req, "client_order_id", None)
        if coid:
            params["newClientOrderId"] = coid

        # --- КРИТИЧЕСКОЕ МЕСТО: stop_price / stop_px ---
        stop_px = getattr(req, "stop_price", None)
        if stop_px is None:
            stop_px = getattr(req, "stop_px", None)

        if stop_px is not None:
            params["stopPrice"] = self._fmt_decimal(float(stop_px))

        close_position = bool(getattr(req, "close_position", False))
        if close_position:
            # для STOP_MARKET / TAKE_PROFIT_MARKET
            params["closePosition"] = "true"

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
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
        }

        if order_id is not None:
            params["orderId"] = order_id

        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        return await self._request("DELETE", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]

    async def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        GET /fapi/v1/order — получить информацию по конкретному ордеру.
        """
        params: Dict[str, Any] = {"symbol": symbol}

        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        return await self._request("GET", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]

    async def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        GET /fapi/v1/openOrders — список открытых ордеров.
        Можно ограничить одним символом.
        """
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol

        return await self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)  # type: ignore[return-value]

    # ---------- Публичные методы адаптера (маржа / плечо) ----------

    async def set_margin_type(self, symbol: str, margin_type: str) -> Dict[str, Any]:
        """
        POST /fapi/v1/marginType — установка типа маржи для символа.

        margin_type: "ISOLATED" или "CROSSED"
        """
        params = {
            "symbol": symbol,
            "marginType": margin_type.upper(),
        }
        return await self._request("POST", "/fapi/v1/marginType", params=params, signed=True)  # type: ignore[return-value]

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        POST /fapi/v1/leverage — установка плеча для символа.
        """
        params = {
            "symbol": symbol,
            "leverage": leverage,
        }
        return await self._request("POST", "/fapi/v1/leverage", params=params, signed=True)  # type: ignore[return-value]


# -----------------------------
#  Paper Adapter
# -----------------------------


class PaperAdapter:
    """
    Простейший paper-адаптер с тем же интерфейсом, что у BinanceUSDTMAdapter.
    Ничего никуда не шлёт, всё в памяти, но имитирует основные ответы.
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

    # ---------- Ордера ----------

    async def create_order(self, req: Any) -> Dict[str, Any]:
        """
        Paper-версия create_order: имитирует структуру ответа Binance.
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
