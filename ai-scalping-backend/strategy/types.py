# strategy/types.py
"""
Shared types for strategy layer.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, List

Side = Literal["LONG", "SHORT"]


@dataclass
class StrategyDecision:
    """
    Typed strategy decision model.
    
    Encodes:
    - symbol: trading symbol
    - signal: LONG, SHORT, or None for FLAT/EXIT
    - tp_pct: take profit percentage (e.g., 0.003 = 0.3%)
    - sl_pct: stop loss percentage (e.g., 0.002 = 0.2%)
    - reason: human-readable reason for the decision
    """
    symbol: str
    signal: Optional[Side]  # None means no signal (FLAT/EXIT)
    tp_pct: float
    sl_pct: float
    reason: str
    # Optional metadata
    reason_tags: List[str] = None
    
    def __post_init__(self):
        if self.reason_tags is None:
            self.reason_tags = []

