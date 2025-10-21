# exec/executor.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass
from typing import Optional, Literal, Protocol, Callable, Tuple

logger = logging.getLogger(__name__)
Side = Literal["BUY", "SELL"]


# ====== Контракты низкого уровня (не Pydantic) ======

@dataclass
class OrderReq:
    symbol: str
    side: Side
    type: Literal["LIMIT", "MARKET"]
    qty: float
    price: Optional[float] = None
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    client_order_id: Optional[str] = None


@dataclass
class OrderResp:
    order_id: str
    status: Literal["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED"]
    filled_qty: float
    avg_px: Optional[float]
    ts: int


class ExecAdapter(Protocol):
    """Мини-контракт исполнителя, который покрывают Paper/Live-адаптеры."""
    async def create_order(self, req: OrderReq) -> OrderResp: ...
    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderResp: ...


# ====== Конфиг и отчёт ======

@dataclass
class ExecCfg:
    price_tick: float = 0.1      # шаг цены
    qty_step: float = 0.001      # шаг количества
    limit_offset_ticks: int = 1  # на сколько тиков от best
    limit_timeout_ms: int = 500  # ожидание лимита
    time_in_force: str = "GTC"   # GTC/IOC/FOK


@dataclass
class ExecutionReport:
    status: Literal["FILLED", "PARTIAL", "CANCELED"]
    filled_qty: float
    avg_px: Optional[float]
    limit_oid: Optional[str]
    market_oid: Optional[str]
    steps: list[str]
    ts: int


# ====== Утилиты ======

def _now_ms() -> int:
    return int(time.time() * 1000)


def _round_to_step(val: float, step: float, mode: Literal["down", "up", "nearest"] = "down") -> float:
    if step <= 0:
        return float(val)
    if mode == "down":
        return math.floor(val / step) * step
    if mode == "up":
        return math.ceil(val / step) * step
    # nearest
    return round(val / step) * step


# ====== Исполнитель ======

class Executor:
    """
    Политика входа:
      1) limit@best±offset → ждем limit_timeout_ms
      2) cancel limit
      3) market остатка
      Консолидация частичных исполнений в VWAP.
    """

    def __init__(
        self,
        adapter: ExecAdapter,
        cfg: ExecCfg,
        get_best: Callable[[str], Tuple[float, float]],
    ) -> None:
        self.a = adapter
        self.c = cfg
        self.get_best = get_best  # (symbol) -> (bid, ask)

    async def place_entry(self, symbol: str, side: Side, qty: float) -> ExecutionReport:
        steps: list[str] = []

        # 1) округляем qty к шагу
        qty_rounded = max(0.0, _round_to_step(float(qty), self.c.qty_step, "down"))
        if qty_rounded <= 0:
            return ExecutionReport(
                status="CANCELED",
                filled_qty=0.0,
                avg_px=None,
                limit_oid=None,
                market_oid=None,
                steps=["qty_rounded_to_zero"],
                ts=_now_ms(),
            )

        # 2) целевой лимит от best (ask для BUY, bid для SELL)
        bid, ask = self.get_best(symbol)
        ref_px = ask if side == "BUY" else bid
        offset = self.c.price_tick * self.c.limit_offset_ticks * (1 if side == "BUY" else -1)
        limit_px = ref_px + offset

        # округление цены в сторону благоприятную исполнению
        limit_px = _round_to_step(limit_px, self.c.price_tick, "down")
        if limit_px <= 0:
            limit_px = self.c.price_tick

        limit_coid = f"lim-{symbol}-{_now_ms()}"
        limit_req = OrderReq(
            symbol=symbol,
            side=side,
            type="LIMIT",
            qty=qty_rounded,
            price=float(limit_px),
            time_in_force=self.c.time_in_force,
            client_order_id=limit_coid,
        )

        steps.append(f"limit_submit:{limit_px} qty:{qty_rounded}")
        lim = await self.a.create_order(limit_req)

        filled_qty = 0.0
        vw_num = 0.0
        vw_den = 0.0
        limit_oid: Optional[str] = lim.order_id
        market_oid: Optional[str] = None

        if lim.status in ("PARTIALLY_FILLED", "FILLED") and lim.filled_qty > 0:
            filled_qty += float(lim.filled_qty)
            if lim.avg_px:
                vw_num += float(lim.avg_px) * float(lim.filled_qty)
                vw_den += float(lim.filled_qty)
            steps.append(f"limit_filled:{lim.filled_qty}@{lim.avg_px}")

        # 3) ждём таймаут лимита
        await asyncio.sleep(self.c.limit_timeout_ms / 1000.0)

        # 4) отменяем лимит, если не FILLED
        if filled_qty < qty_rounded and lim.status != "FILLED":
            steps.append("limit_cancel")
            with contextlib.suppress(Exception):
                await self.a.cancel_order(symbol, limit_coid)

        # 5) добиваем остаток маркетом
        remaining = max(0.0, qty_rounded - filled_qty)
        if remaining > 0:
            steps.append(f"market_submit:{remaining}")
            m = await self.a.create_order(OrderReq(symbol=symbol, side=side, type="MARKET", qty=remaining))
            market_oid = m.order_id
            if m.status in ("PARTIALLY_FILLED", "FILLED") and m.filled_qty > 0:
                filled_qty += float(m.filled_qty)
                if m.avg_px:
                    vw_num += float(m.avg_px) * float(m.filled_qty)
                    vw_den += float(m.filled_qty)
                steps.append(f"market_filled:{m.filled_qty}@{m.avg_px}")

        avg_px = (vw_num / vw_den) if vw_den > 0 else None
        if filled_qty <= 0:
            status: Literal["FILLED", "PARTIAL", "CANCELED"] = "CANCELED"
        elif filled_qty < qty_rounded:
            status = "PARTIAL"
        else:
            status = "FILLED"

        return ExecutionReport(
            status=status,
            filled_qty=float(filled_qty),
            avg_px=(float(avg_px) if avg_px is not None else None),
            limit_oid=limit_oid,
            market_oid=market_oid,
            steps=steps,
            ts=_now_ms(),
        )
