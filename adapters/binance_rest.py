from __future__ import annotations

import hmac
import hashlib
import time
import ssl
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

import aiohttp
import certifi
from urllib.parse import urlencode


# -----------------------------
#  Общие структуры
# -----------------------------


@dataclass
class OrderReq:
    """
    Структура запроса на создание ордера.
    Совместима с Binance Futures USDT-M.
    """

    symbol: str
    side: str          # "BUY" / "SELL"
    type: str          # "LIMIT", "MARKET", ...
    qty: float
    price: Optional[float] = None
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None


# -----------------------------
#  Binance USDT-M Adapter
# -----------------------------


class BinanceUSDTMAdapter:
    """
    REST-адаптер для Binance USDT-M Futures.

    - Использует aiohttp с SSL-контекстом на базе certifi
    - Подписывает приватные запросы (ордеры, баланс и т.п.)
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        recv_window: int = 5_000,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret.encode("utf-8")
        self._base_url = base_url.rstrip("/")
        self._recv_window = recv_window

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
        Добавить timestamp, recvWindow и signature.
        """
        p = dict(params)
        p.setdefault("timestamp", int(time.time() * 1000))
        p.setdefault("recvWindow", self._recv_window)

        query = urlencode(p, doseq=True)
        sig = hmac.new(self._api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    async def _request(self, method: str, path: str, params: Dict[str, Any]) -> Any:
        """
        Базовый приватный REST-запрос к Binance Futures.
        Используется для /fapi/v1/order, /fapi/v2/balance и т.п.
        """
        url = f"{self._base_url}{path}"
        sess = await self._ensure_session()

        signed_params = self._sign_params(params)

        headers = {
            "X-MBX-APIKEY": self._api_key,
        }

        async with sess.request(method, url, params=signed_params, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Binance REST error {resp.status}: {text}")
            try:
                data = await resp.json()
            except Exception:
                raise RuntimeError(f"Binance REST non-JSON response: {text}")
            return data

    @staticmethod
    def _fmt_decimal(x: float) -> str:
        """
        Форматирование числа без экспоненты.
        """
        d = Decimal(str(x))
        return format(d.normalize(), "f")

    # ---------- Публичные методы адаптера ----------

    async def get_balance(self) -> Any:
        """
        /fapi/v2/balance — баланс по активам.
        """
        return await self._request("GET", "/fapi/v2/balance", {})

    async def create_order(self, req: OrderReq) -> Any:
        """
        POST /fapi/v1/order — создание ордера.
        """
        params: Dict[str, Any] = {
            "symbol": req.symbol,
            "side": req.side,
            "type": req.type,
        }

        if req.qty is not None:
            params["quantity"] = self._fmt_decimal(req.qty)

        if req.price is not None:
            params["price"] = self._fmt_decimal(req.price)

        if req.time_in_force is not None:
            params["timeInForce"] = req.time_in_force

        if req.reduce_only:
            params["reduceOnly"] = "true"

        if req.client_order_id:
            params["newClientOrderId"] = req.client_order_id

        return await self._request("POST", "/fapi/v1/order", params)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Any:
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

        return await self._request("DELETE", "/fapi/v1/order", params)


# -----------------------------
#  Paper Adapter
# -----------------------------


class PaperAdapter:
    """
    Простейший paper-адаптер с тем же интерфейсом, что у BinanceUSDTMAdapter.
    Ничего никуда не шлёт, всё в памяти.
    """

    def __init__(self) -> None:
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._last_id: int = 0

    async def close(self) -> None:
        return None

    async def get_balance(self) -> Any:
        return [
            {
                "asset": "USDT",
                "balance": "1000.00000000",
                "availableBalance": "1000.00000000",
            }
        ]

    async def create_order(self, req: OrderReq) -> Any:
        self._last_id += 1
        oid = self._last_id
        coid = req.client_order_id or f"paper-{oid}"

        order = {
            "symbol": req.symbol,
            "side": req.side,
            "type": req.type,
            "origQty": str(req.qty),
            "price": str(req.price) if req.price is not None else "0",
            "status": "NEW",
            "orderId": oid,
            "clientOrderId": coid,
            "reduceOnly": req.reduce_only,
        }

        self._orders[coid] = order
        return order

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Any:
        if client_order_id and client_order_id in self._orders:
            order = self._orders.pop(client_order_id)
            order["status"] = "CANCELED"
            return order

        if order_id is not None:
            for coid, od in list(self._orders.items()):
                if od.get("orderId") == order_id:
                    od["status"] = "CANCELED"
                    self._orders.pop(coid, None)
                    return od

        return {
            "symbol": symbol,
            "orderId": order_id or -1,
            "clientOrderId": client_order_id or "",
            "status": "UNKNOWN",
        }
