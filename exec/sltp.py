# exec/sltp.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

Side = Literal["BUY", "SELL"]

@dataclass
class SLTPPlan:
    sl_px: float
    tp_px: float
    sl_qty: float
    tp_qty: float

def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return round(round(x / step) * step, 10)

def _is_long(side: Side) -> bool:
    return side == "BUY"

def compute_sltp(
    *,
    side: Side,
    entry_px: float,
    qty: float,
    price_tick: float,
    sl_distance_px: float,
    rr: float = 1.8,
) -> SLTPPlan:
    """
    Простая схема SL/TP:
      SL = entry_px -/+ sl_distance_px (в сторону риска)
      TP = entry_px + rr * sl_distance_px (в сторону профита)
    Всегда reduceOnly, qty = вся позиция.
    """
    assert entry_px > 0 and qty > 0 and sl_distance_px > 0
    if _is_long(side):
        sl = max(0.0, entry_px - sl_distance_px)
        tp = entry_px + rr * sl_distance_px
    else:
        sl = entry_px + sl_distance_px
        tp = entry_px - rr * sl_distance_px

    sl = _round_step(sl, price_tick)
    tp = _round_step(tp, price_tick)
    return SLTPPlan(sl_px=sl, tp_px=tp, sl_qty=qty, tp_qty=qty)
