# strategy/bollinger_basic.py
"""
Bollinger Band Scalping Strategy

A simple scalping strategy based on Bollinger Bands:
- LONG when price is at or below lower band
- SHORT when price is at or above upper band
- Uses percentage-based TP/SL targets
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal
import logging

from .types import StrategyDecision, Side

_logger = logging.getLogger(__name__)


@dataclass
class BollingerBasicCfg:
    """Configuration for Bollinger Basic strategy."""
    tp_pct: float = 0.004  # 0.4% take profit
    sl_pct: float = 0.0025  # 0.25% stop loss
    entry_threshold_pct: float = 0.0005  # 0.05% threshold for entry (price must be within this of band)
    require_bandwidth_min: Optional[float] = None  # Minimum bandwidth (optional filter)
    entry_usd: float = 25.0  # Entry risk in USD (20-30 USD range, configurable)
    leverage: float = 10.0  # Leverage multiplier (10x default, configurable)


def signal_bollinger_basic(
    *,
    symbol: str,
    indi: dict,
    current_position_side: Optional[Literal["LONG", "SHORT"]] = None,
    cfg: BollingerBasicCfg = None,
) -> Optional[StrategyDecision]:
    """
    Bollinger Band scalping strategy.
    
    Entry Logic:
    - When FLAT: 
        - LONG if price <= lower_band (or within threshold)
        - SHORT if price >= upper_band (or within threshold)
    - When in position: returns None (relies on TP/SL from executor)
    
    Args:
        symbol: Trading symbol
        indi: Indicator dictionary containing:
            - boll_low: lower Bollinger band
            - boll_up: upper Bollinger band
            - boll_mid: middle band (SMA)
            - boll_bandwidth: (optional) bandwidth metric
            - price or close: current price (prefer 'price', fallback to 'close')
        current_position_side: Current position side ("LONG", "SHORT", or None for FLAT)
        cfg: Strategy configuration
    
    Returns:
        StrategyDecision or None if no signal
    """
    if cfg is None:
        cfg = BollingerBasicCfg()
    
    # Extract Bollinger bands
    boll_low = indi.get("boll_low")
    boll_up = indi.get("boll_up")
    boll_mid = indi.get("boll_mid")
    boll_bandwidth = indi.get("boll_bandwidth")
    
    # Extract price (prefer 'price', fallback to 'close')
    price = indi.get("price") or indi.get("close")
    
    # Validate required fields
    if boll_low is None or boll_up is None or boll_mid is None:
        _logger.debug(f"[bollinger_basic:{symbol}] skip - missing Bollinger bands")
        return None
    
    if price is None:
        _logger.debug(f"[bollinger_basic:{symbol}] skip - missing price")
        return None
    
    try:
        price = float(price)
        boll_low = float(boll_low)
        boll_up = float(boll_up)
        boll_mid = float(boll_mid)
    except (ValueError, TypeError):
        _logger.debug(f"[bollinger_basic:{symbol}] skip - invalid numeric values")
        return None
    
    # Bandwidth filter (optional)
    if cfg.require_bandwidth_min is not None and boll_bandwidth is not None:
        try:
            bw = float(boll_bandwidth)
            if bw < cfg.require_bandwidth_min:
                _logger.debug(
                    f"[bollinger_basic:{symbol}] skip - bandwidth too small: {bw:.6f} < {cfg.require_bandwidth_min:.6f}"
                )
                return None
        except (ValueError, TypeError):
            pass  # Skip bandwidth check if invalid
    
    # If already in position, don't generate new entry signals
    if current_position_side is not None:
        _logger.debug(f"[bollinger_basic:{symbol}] skip - already in position: {current_position_side}")
        return None
    
    # Entry threshold in price units (cfg.entry_threshold_pct is already a decimal, e.g., 0.0005 = 0.05%)
    entry_threshold_px = price * cfg.entry_threshold_pct if price > 0 else 0.0
    
    # LONG signal: price at or below lower band (within threshold)
    if price <= (boll_low + entry_threshold_px):
        _logger.info(
            f"[bollinger_basic:{symbol}] LONG signal: price={price:.6f} <= lower_band={boll_low:.6f} "
            f"(threshold={entry_threshold_px:.6f})"
        )
        return StrategyDecision(
            symbol=symbol,
            signal="LONG",
            tp_pct=cfg.tp_pct,
            sl_pct=cfg.sl_pct,
            reason=f"Bollinger: price at lower band (price={price:.6f}, lower={boll_low:.6f})",
            reason_tags=["bollinger", "long", "lower_band"],
        )
    
    # SHORT signal: price at or above upper band (within threshold)
    if price >= (boll_up - entry_threshold_px):
        _logger.info(
            f"[bollinger_basic:{symbol}] SHORT signal: price={price:.6f} >= upper_band={boll_up:.6f} "
            f"(threshold={entry_threshold_px:.6f})"
        )
        return StrategyDecision(
            symbol=symbol,
            signal="SHORT",
            tp_pct=cfg.tp_pct,
            sl_pct=cfg.sl_pct,
            reason=f"Bollinger: price at upper band (price={price:.6f}, upper={boll_up:.6f})",
            reason_tags=["bollinger", "short", "upper_band"],
        )
    
    # No signal
    _logger.debug(
        f"[bollinger_basic:{symbol}] no signal: price={price:.6f}, "
        f"lower={boll_low:.6f}, upper={boll_up:.6f}, mid={boll_mid:.6f}"
    )
    return None

