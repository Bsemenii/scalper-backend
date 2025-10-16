# risk/sizing.py
from __future__ import annotations

def size_by_r(risk_usd: float, stop_distance: float, lot_step: float) -> float:
    """
    qty = risk / stop; далее округляем вниз под шаг лота.
    """
    if stop_distance <= 0:
        return 0.0
    raw = risk_usd / stop_distance
    k = max(lot_step, 1e-12)
    return max(0.0, (raw // k) * k)
