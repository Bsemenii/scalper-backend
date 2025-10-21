# adapters/binance_rest.py
from __future__ import annotations

import itertools
import time
from dataclasses import dataclass
from typing import Optional, Literal, Callable, Tuple

Side = Literal["BUY", "SELL"]


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


class PaperAdapter:
    """
    Простой paper-эмулятор:
      - MARKET: FILLED целиком по лучшей цене ± slippage (bp)
      - LIMIT: если пересекается с лучшей ценой — FILLED сразу; иначе NEW
      - cancel_order: всегда CANCELED
    """

    def __init__(self, best_price_fn: Callable[[str], Tuple[float, float]], max_slippage_bp: float = 6.0) -> None:
        self.best = best_price_fn
        self._oid = itertools.count(1)
        self.slip_bp = float(max_slippage_bp)

    async def create_order(self, req: OrderReq) -> OrderResp:
        bid, ask = self.best(req.symbol)
        now = int(time.time() * 1000)

        if req.type == "MARKET":
            px = ask if req.side == "BUY" else bid
            # Простейшая модель проскальзывания
            px *= 1 + (self.slip_bp / 1e4) * (1 if req.side == "BUY" else -1)
            return OrderResp(f"oid-{next(self._oid)}", "FILLED", float(req.qty), float(px), now)

        if req.type == "LIMIT":
            cross = (req.side == "BUY" and (req.price or 0) >= ask) or (req.side == "SELL" and (req.price or 0) <= bid)
            if cross:
                avg = ask if req.side == "BUY" else bid
                return OrderResp(f"oid-{next(self._oid)}", "FILLED", float(req.qty), float(avg), now)
            else:
                return OrderResp(f"oid-{next(self._oid)}", "NEW", 0.0, None, now)

        return OrderResp(f"oid-{next(self._oid)}", "REJECTED", 0.0, None, now)

    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderResp:
        now = int(time.time() * 1000)
        oid = client_order_id or f"oid-{next(self._oid)}"
        return OrderResp(oid, "CANCELED", 0.0, None, now)
