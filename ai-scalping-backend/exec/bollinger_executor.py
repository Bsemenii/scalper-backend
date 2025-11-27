# exec/bollinger_executor.py
"""
Bollinger Scalping Strategy Execution Helper

Provides dedicated execution functions for Bollinger scalping strategy:
- Entry: limit order with timeout fallback to market
- Exit: reduceOnly market order
- SL/TP: percentage-based levels with monitoring
"""
from __future__ import annotations

import logging
import time
from typing import Optional, Tuple, Dict, Any, Literal
from decimal import Decimal, ROUND_DOWN

from exec.executor import Executor, ExecutionReport
from exec.position_state import PositionState, PositionSide, get_position_state, save_position_state

logger = logging.getLogger(__name__)

Side = Literal["BUY", "SELL"]


def compute_tp_sl_levels(
    entry_price: float,
    side: Literal["LONG", "SHORT"],
    tp_pct: float,
    sl_pct: float,
) -> Tuple[float, float]:
    """
    Compute TP and SL levels from percentages.
    
    Args:
        entry_price: Entry price
        side: "LONG" or "SHORT"
        tp_pct: Take profit percentage (e.g., 0.004 = 0.4%)
        sl_pct: Stop loss percentage (e.g., 0.0025 = 0.25%)
    
    Returns:
        (tp_price, sl_price)
    """
    entry = float(entry_price)
    tp_p = float(tp_pct)
    sl_p = float(sl_pct)
    
    if side == "LONG":
        tp_price = entry * (1.0 + tp_p)
        sl_price = entry * (1.0 - sl_p)
    else:  # SHORT
        tp_price = entry * (1.0 - tp_p)
        sl_price = entry * (1.0 + sl_p)
    
    return (float(tp_price), float(sl_price))


def compute_size_from_usd(
    entry_usd: float,
    leverage: float,
    price: float,
    qty_step: float = 0.001,
) -> float:
    """
    Compute position size from USD risk and leverage.
    
    Formula: size = (entry_usd * leverage) / price
    
    Args:
        entry_usd: Entry risk in USD (e.g., 25.0)
        leverage: Leverage multiplier (e.g., 10.0)
        price: Entry price
        qty_step: Minimum quantity step (for rounding)
    
    Returns:
        Rounded size in base currency
    """
    if price <= 0.0 or qty_step <= 0.0:
        return 0.0
    
    nominal_size = (float(entry_usd) * float(leverage)) / float(price)
    
    # Round down to qty_step
    if qty_step > 0.0:
        d = Decimal(str(nominal_size))
        s = Decimal(str(qty_step))
        q = (d / s).to_integral_value(rounding=ROUND_DOWN) * s
        return float(q)
    
    return float(nominal_size)


async def place_entry_order(
    symbol: str,
    side: Literal["LONG", "SHORT"],
    nominal_size_usd: float,
    price: float,
    executor: Executor,
    cfg: Dict[str, Any],
) -> Tuple[Optional[float], float]:
    """
    Place entry order with limit → timeout → market fallback.
    
    Args:
        symbol: Trading symbol
        side: "LONG" or "SHORT"
        nominal_size_usd: Entry risk in USD
        price: Target entry price
        executor: Executor instance
        cfg: Configuration dict with:
            - leverage: float
            - qty_step: float
            - limit_timeout_ms: int (optional, defaults to 500)
    
    Returns:
        (avg_fill_price, filled_size)
        Returns (None, 0.0) on failure
    """
    sym = str(symbol).upper()
    leverage = float(cfg.get("leverage", 10.0))
    qty_step = float(cfg.get("qty_step", 0.001))
    
    # Convert side to BUY/SELL
    exec_side: Side = "BUY" if side == "LONG" else "SELL"
    
    # Compute size
    size = compute_size_from_usd(nominal_size_usd, leverage, price, qty_step)
    if size <= 0.0:
        logger.warning("[EXEC] entry-request %s %s: size rounded to zero (usd=%.2f, price=%.6f)", sym, side, nominal_size_usd, price)
        return (None, 0.0)
    
    logger.info("[EXEC] entry-request %s %s size=%.6f usd=%.2f leverage=%.1f price=%.6f", sym, side, size, nominal_size_usd, leverage, price)
    
    # Use executor's place_entry (handles limit → timeout → market)
    try:
        report: ExecutionReport = await executor.place_entry(
            symbol=sym,
            side=exec_side,
            qty=size,
            reduce_only=False,
        )
        
        if report.status in ("FILLED", "PARTIAL") and report.filled_qty > 0.0:
            avg_px = report.avg_px
            filled = float(report.filled_qty)
            
            if "market" in str(report.steps):
                logger.info("[EXEC] entry-fill %s %s: filled=%.6f avg_px=%.6f (fallback-to-market)", sym, side, filled, avg_px or 0.0)
            else:
                logger.info("[EXEC] entry-fill %s %s: filled=%.6f avg_px=%.6f", sym, side, filled, avg_px or 0.0)
            
            return (avg_px, filled)
        else:
            logger.warning("[EXEC] entry-fill %s %s: status=%s filled=%.6f", sym, side, report.status, report.filled_qty)
            return (None, 0.0)
    
    except Exception as e:
        logger.exception("[EXEC] entry-request %s %s failed: %s", sym, side, e)
        return (None, 0.0)


async def place_exit_order(
    symbol: str,
    side: Literal["LONG", "SHORT"],
    size: float,
    executor: Executor,
) -> Tuple[Optional[float], float]:
    """
    Place exit order (reduceOnly market).
    
    Args:
        symbol: Trading symbol
        side: Current position side ("LONG" or "SHORT")
        size: Position size to close
        executor: Executor instance
    
    Returns:
        (avg_fill_price, filled_size)
        Returns (None, 0.0) on failure
    """
    sym = str(symbol).upper()
    
    # Convert side to opposite for exit
    exec_side: Side = "SELL" if side == "LONG" else "BUY"
    
    logger.info("[EXEC] exit-request %s %s size=%.6f", sym, side, size)
    
    # Use executor's place_exit (reduceOnly market)
    try:
        report: ExecutionReport = await executor.place_exit(
            symbol=sym,
            side=exec_side,
            qty=size,
        )
        
        if report.status in ("FILLED", "PARTIAL") and report.filled_qty > 0.0:
            avg_px = report.avg_px
            filled = float(report.filled_qty)
            logger.info("[EXEC] exit-fill %s %s: filled=%.6f avg_px=%.6f", sym, side, filled, avg_px or 0.0)
            return (avg_px, filled)
        else:
            logger.warning("[EXEC] exit-fill %s %s: status=%s filled=%.6f", sym, side, report.status, report.filled_qty)
            return (None, 0.0)
    
    except Exception as e:
        logger.exception("[EXEC] exit-request %s %s failed: %s", sym, side, e)
        return (None, 0.0)


def attach_sl_tp(
    symbol: str,
    position_state: PositionState,
    tp_price: float,
    sl_price: float,
) -> PositionState:
    """
    Store SL/TP levels in PositionState.
    
    Args:
        symbol: Trading symbol
        position_state: PositionState instance
        tp_price: Take profit price
        sl_price: Stop loss price
    
    Returns:
        Updated PositionState with SL/TP levels
    """
    sym = str(symbol).upper()
    logger.info("[EXEC] sl/tp-set %s side=%s entry=%.6f sl=%.6f tp=%.6f", 
                sym, position_state.side.value, position_state.entry_price or 0.0, sl_price, tp_price)
    
    # Update PositionState with SL/TP
    position_state.sl_price = float(sl_price)
    position_state.tp_price = float(tp_price)
    return position_state


def check_sl_tp_hit(
    symbol: str,
    price: float,
    position_state: PositionState,
) -> Optional[Literal["exit"]]:
    """
    Check if current price hits SL or TP.
    
    Args:
        symbol: Trading symbol
        price: Current market price
        position_state: PositionState instance (must have sl_price and tp_price set)
    
    Returns:
        "exit" if SL or TP hit, None otherwise
    """
    if position_state.is_flat():
        return None
    
    if position_state.entry_price is None:
        return None
    
    if position_state.sl_price is None or position_state.tp_price is None:
        return None
    
    px = float(price)
    tp = float(position_state.tp_price)
    sl = float(position_state.sl_price)
    
    if position_state.is_long():
        # LONG: exit if price >= TP or price <= SL
        if px >= tp:
            logger.info("[EXEC] sl/tp-hit %s LONG: price=%.6f >= tp=%.6f", symbol, px, tp)
            return "exit"
        if px <= sl:
            logger.info("[EXEC] sl/tp-hit %s LONG: price=%.6f <= sl=%.6f", symbol, px, sl)
            return "exit"
    
    elif position_state.is_short():
        # SHORT: exit if price <= TP or price >= SL
        if px <= tp:
            logger.info("[EXEC] sl/tp-hit %s SHORT: price=%.6f <= tp=%.6f", symbol, px, tp)
            return "exit"
        if px >= sl:
            logger.info("[EXEC] sl/tp-hit %s SHORT: price=%.6f >= sl=%.6f", symbol, px, sl)
            return "exit"
    
    return None

