# exec/sltp.py
from __future__ import annotations
from typing import Optional
from bot.core.types import Side

def compute_sl_tp(
    side: Side,
    entry_px: float,
    stop_distance: float,  # в цене (USD)
    tp_r: Optional[float], # множитель R
    fees_bp: float = 2.0   # грубо "туда-сюда"
) -> tuple[float, Optional[float], float]:
    """
    Возвращает (sl_px, tp_px, rr)
    rr рассчитывается как (target - entry)/stop_distance с поправкой на комиссии.
    """
    if stop_distance <= 0:
        raise ValueError("stop_distance must be > 0")

    if side == "BUY" or getattr(side, "value", "") == "BUY":
        sl = entry_px - stop_distance
        tp = entry_px + (tp_r * stop_distance) if tp_r else None
    else:
        sl = entry_px + stop_distance
        tp = entry_px - (tp_r * stop_distance) if tp_r else None

    rr = (tp_r or 0.0)
    # учтём грубо комиссии: уменьшим rr на двойные комиссии в R-эквиваленте
    rr = max(0.0, rr - (fees_bp/1e4)*2)
    return float(sl), (float(tp) if tp is not None else None), float(rr)
