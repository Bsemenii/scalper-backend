# exec/position_state.py
"""
Unified Position State Management

Provides a clean abstraction for tracking current position state per symbol.
This is the single source of truth for whether we are FLAT / LONG / SHORT.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Literal
import logging

logger = logging.getLogger(__name__)


class PositionSide(str, Enum):
    """Position side enum: FLAT, LONG, or SHORT."""
    FLAT = "flat"
    LONG = "long"
    SHORT = "short"


@dataclass
class PositionState:
    """
    Unified position state for a symbol.
    
    This represents the current snapshot of position state:
    - FLAT: no position
    - LONG: long position with entry_price and size
    - SHORT: short position with entry_price and size
    """
    symbol: str
    side: PositionSide = PositionSide.FLAT
    entry_price: Optional[float] = None
    size: Optional[float] = None
    last_update_ts: float = 0.0
    # SL/TP levels (optional, for Bollinger and other strategies)
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    
    def is_flat(self) -> bool:
        """Check if position is flat."""
        return self.side == PositionSide.FLAT
    
    def is_long(self) -> bool:
        """Check if position is long."""
        return self.side == PositionSide.LONG
    
    def is_short(self) -> bool:
        """Check if position is short."""
        return self.side == PositionSide.SHORT
    
    def to_str_side(self) -> Optional[Literal["LONG", "SHORT"]]:
        """
        Convert to string side format used by strategy/router.
        Returns "LONG", "SHORT", or None (for FLAT).
        """
        if self.side == PositionSide.LONG:
            return "LONG"
        elif self.side == PositionSide.SHORT:
            return "SHORT"
        return None


# Global in-memory storage for position states
# This is the single source of truth for position state
_position_states: Dict[str, PositionState] = {}


def get_position_state(symbol: str) -> PositionState:
    """
    Get position state for a symbol.
    Returns FLAT state if not found.
    
    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
    
    Returns:
        PositionState for the symbol
    """
    symbol_upper = symbol.upper()
    if symbol_upper not in _position_states:
        _position_states[symbol_upper] = PositionState(
            symbol=symbol_upper,
            side=PositionSide.FLAT,
            entry_price=None,
            size=None,
            last_update_ts=0.0,
        )
    return _position_states[symbol_upper]


def save_position_state(state: PositionState) -> None:
    """
    Save position state for a symbol.
    
    Args:
        state: PositionState to save
    """
    symbol_upper = state.symbol.upper()
    old_state = _position_states.get(symbol_upper)
    
    _position_states[symbol_upper] = state
    
    # Log transition
    if old_state:
        old_side = old_state.side.value
        new_side = state.side.value
        if old_side != new_side:
            logger.info(
                "[POS] symbol=%s, from=%s to=%s, entry_price=%s, size=%s",
                symbol_upper,
                old_side,
                new_side,
                state.entry_price,
                state.size,
            )
    else:
        logger.info(
            "[POS] symbol=%s, side=%s, entry_price=%s, size=%s",
            symbol_upper,
            state.side.value,
            state.entry_price,
            state.size,
        )


def flat_state(symbol: str, ts: Optional[float] = None) -> PositionState:
    """
    Create a FLAT position state for a symbol.
    
    Args:
        symbol: Trading symbol
        ts: Timestamp (defaults to current time if None)
    
    Returns:
        PositionState with FLAT side
    """
    import time
    return PositionState(
        symbol=symbol.upper(),
        side=PositionSide.FLAT,
        entry_price=None,
        size=None,
        last_update_ts=ts if ts is not None else time.time(),
    )


def with_long(symbol: str, entry_price: float, size: float, ts: Optional[float] = None) -> PositionState:
    """
    Create a LONG position state for a symbol.
    
    Args:
        symbol: Trading symbol
        entry_price: Entry price
        size: Position size (quantity)
        ts: Timestamp (defaults to current time if None)
    
    Returns:
        PositionState with LONG side
    """
    import time
    return PositionState(
        symbol=symbol.upper(),
        side=PositionSide.LONG,
        entry_price=float(entry_price),
        size=float(size),
        last_update_ts=ts if ts is not None else time.time(),
    )


def with_short(symbol: str, entry_price: float, size: float, ts: Optional[float] = None) -> PositionState:
    """
    Create a SHORT position state for a symbol.
    
    Args:
        symbol: Trading symbol
        entry_price: Entry price
        size: Position size (quantity)
        ts: Timestamp (defaults to current time if None)
    
    Returns:
        PositionState with SHORT side
    """
    import time
    return PositionState(
        symbol=symbol.upper(),
        side=PositionSide.SHORT,
        entry_price=float(entry_price),
        size=float(size),
        last_update_ts=ts if ts is not None else time.time(),
    )


def sync_from_worker_pos(worker_pos: Dict[str, Any]) -> None:
    """
    Sync position state from Worker's _pos dict.
    
    This bridges the existing Worker._pos structure to the unified PositionState.
    
    Args:
        worker_pos: Dict mapping symbol -> Worker PositionState dataclass
    """
    # Import Side for type checking
    try:
        from bot.core.types import Side as CoreSide
    except ImportError:
        CoreSide = None
    
    for symbol, pos in worker_pos.items():
        if pos is None:
            continue
        
        state_str = getattr(pos, "state", "FLAT")
        side_raw = getattr(pos, "side", None)
        qty = getattr(pos, "qty", 0.0)
        entry_px = getattr(pos, "entry_px", 0.0)
        
        # Convert Worker PositionState to unified PositionState
        if state_str == "FLAT" or qty <= 1e-9:
            new_state = flat_state(symbol)
        elif side_raw == "BUY" or (CoreSide and side_raw == CoreSide.BUY):
            new_state = with_long(symbol, entry_px, qty)
        elif side_raw == "SELL" or (CoreSide and side_raw == CoreSide.SELL):
            new_state = with_short(symbol, entry_px, qty)
        else:
            new_state = flat_state(symbol)
        
        # Only update if different to avoid excessive logging
        current = get_position_state(symbol)
        if (current.side != new_state.side or 
            abs(current.entry_price or 0.0 - new_state.entry_price or 0.0) > 1e-9 or
            abs(current.size or 0.0 - new_state.size or 0.0) > 1e-9):
            save_position_state(new_state)

