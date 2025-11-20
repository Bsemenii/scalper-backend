# risk/sizing.py
from __future__ import annotations
from typing import Tuple
import math

def round_down_to_step(value: float, step: float) -> float:
    if step <= 0: return float(value)
    return math.floor(value/step)*step

def calc_qty(
    *,
    equity_usd: float,
    risk_per_trade_pct: float,  # 0.20 → 0.2%
    stop_usd: float,
    min_qty: float,
    step_qty: float,
) -> float:
    """
    qty = (R$) / stop_usd;  R$ = equity * risk_pct / 100
    """
    if stop_usd <= 0: return 0.0
    r_dollars = equity_usd * (risk_per_trade_pct / 100.0)
    raw = r_dollars / stop_usd
    qty = max(min_qty, round_down_to_step(raw, step_qty))
    return float(qty)

def calc_qty_leveraged(
    *,
    equity_usd: float,
    leverage: float,
    price: float,
    qty_step: float,
    min_qty: float,
    max_notional_usd: float | None = None,
) -> float:
    """
    Верхняя граница позиции по плечу (не для стопа, а для лимита экспозиции).
    """
    if price <= 0: return 0.0
    max_qty = (equity_usd * leverage) / price
    if max_notional_usd:
        max_qty = min(max_qty, max_notional_usd / price)
    max_qty = round_down_to_step(max_qty, qty_step)
    return max(min_qty, max_qty)
