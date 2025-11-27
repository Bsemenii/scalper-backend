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


def calc_qty_from_stop_distance_usd(
    *,
    risk_per_trade_usd: float,
    stop_distance_usd: float,
    qty_step: float,
    min_qty: float,
    max_qty: float | None = None,
    min_notional_usd: float = 0.0,
    price: float | None = None,
) -> float | None:
    """
    Position size based on fixed USD risk per trade and strategy-provided stop distance (R-scaling).
    
    Formula: qty = risk_per_trade_usd / stop_distance_usd
    
    For linear USDT-M futures, stop_distance_usd is already in USD terms (multiplier = 1).
    The function normalizes qty to symbol constraints (step size, min/max qty, min notional).
    
    Args:
        risk_per_trade_usd: Fixed USD risk per trade (e.g., 20 or 30 USD)
        stop_distance_usd: Stop distance in USD (from EntryCandidate.stop_distance_usd)
        qty_step: Symbol's quantity step size / precision
        min_qty: Minimum allowed quantity for the symbol
        max_qty: Maximum allowed quantity (optional, for safety)
        min_notional_usd: Minimum notional value in USD (optional)
        price: Current price (required if min_notional_usd > 0)
    
    Returns:
        Calculated quantity, or None if:
        - stop_distance_usd is missing or too small (<= 0)
        - qty rounds to zero after applying step size
        - qty violates min_notional constraint
        - qty exceeds max_qty (if provided)
    """
    # Safety check: stop_distance_usd must be positive
    if stop_distance_usd is None or stop_distance_usd <= 0:
        return None
    
    # Safety check: risk_per_trade_usd must be positive
    if risk_per_trade_usd <= 0:
        return None
    
    # Core formula: qty = risk_per_trade_usd / stop_distance_usd
    raw_qty = risk_per_trade_usd / stop_distance_usd
    
    # Normalize to step size (round down to avoid exceeding risk)
    if qty_step <= 0:
        return None
    
    qty = round_down_to_step(raw_qty, qty_step)
    
    # Check if qty rounds to zero
    if qty <= 0:
        return None
    
    # Enforce minimum quantity
    if qty < min_qty:
        # Try to use min_qty if it's within reasonable bounds
        # But if min_qty would exceed risk, reject the trade
        if min_qty * stop_distance_usd > risk_per_trade_usd * 1.1:  # 10% tolerance
            return None
        qty = min_qty
    
    # Enforce maximum quantity (if provided)
    if max_qty is not None and qty > max_qty:
        qty = max_qty
        # Verify that even max_qty doesn't exceed risk too much
        if qty * stop_distance_usd > risk_per_trade_usd * 1.1:
            return None
    
    # Check minimum notional (if provided)
    if min_notional_usd > 0:
        if price is None or price <= 0:
            return None
        notional = qty * price
        if notional < min_notional_usd:
            return None
    
    return float(qty)
