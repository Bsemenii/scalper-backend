# risk/sizing.py
from __future__ import annotations
from typing import Tuple

def round_down_to_step(value: float, step: float) -> float:
    if step <= 0:
        return float(value)
    return (value // step) * step

def calc_qty(
    *,
    equity_usd: float,
    risk_per_trade_pct: float,
    stop_usd: float,
    min_qty: float,
    step_qty: float,
) -> float:
    """
    qty = (equity * risk%) / stop$
    округление вниз к шагу и проверка min_qty
    """
    if stop_usd <= 0:
        return 0.0
    risk_usd = equity_usd * (risk_per_trade_pct / 100.0)
    raw_qty = risk_usd / stop_usd
    q = round_down_to_step(raw_qty, step_qty)
    if q < min_qty:
        return 0.0
    return float(q)
