# features/bollinger.py
from __future__ import annotations
from typing import Dict, Optional
from collections import deque
import math
import logging

logger = logging.getLogger(__name__)


def compute_bollinger_features(
    close_series: deque | list,
    window: int = 20,
    k: float = 2.0,
) -> Dict[str, Optional[float]]:
    """
    Compute Bollinger Bands for the last point of the close_series.
    
    Args:
        close_series: Sequence of close prices (deque or list)
        window: Rolling window size for SMA and std calculation
        k: Number of standard deviations for bands (default 2.0)
    
    Returns:
        Dict with:
        - 'boll_mid': middle band (SMA) or None if insufficient data
        - 'boll_up': upper band or None if insufficient data
        - 'boll_low': lower band or None if insufficient data
        - 'boll_bandwidth': (boll_up - boll_low) / boll_mid or None if insufficient data
        - 'boll_position': position of last close between low and up in [0, 1] or None if insufficient data
    """
    if len(close_series) < window:
        return {
            "boll_mid": None,
            "boll_up": None,
            "boll_low": None,
            "boll_bandwidth": None,
            "boll_position": None,
        }
    
    # Get the last window prices
    prices = list(close_series)[-window:]
    last_close = prices[-1]
    
    # Compute SMA (middle band)
    sma = sum(prices) / len(prices)
    
    # Compute standard deviation
    variance = sum((x - sma) ** 2 for x in prices) / (len(prices) - 1) if len(prices) > 1 else 0.0
    std = math.sqrt(variance)
    
    # Compute bands
    boll_mid = sma
    boll_up = sma + k * std
    boll_low = sma - k * std
    
    # Compute bandwidth: (upper - lower) / middle
    boll_bandwidth = None
    if boll_mid > 0:
        boll_bandwidth = (boll_up - boll_low) / boll_mid
    
    # Compute position: where last close is between low and up [0, 1]
    # 0 = at lower band, 1 = at upper band
    boll_position = None
    band_range = boll_up - boll_low
    if band_range > 0:
        boll_position = (last_close - boll_low) / band_range
        # Clamp to [0, 1]
        boll_position = max(0.0, min(1.0, boll_position))
    
    return {
        "boll_mid": boll_mid,
        "boll_up": boll_up,
        "boll_low": boll_low,
        "boll_bandwidth": boll_bandwidth,
        "boll_position": boll_position,
    }


class BollingerEngine:
    """
    Engine for computing Bollinger Bands features per symbol.
    Maintains a rolling window of prices and computes Bollinger Bands on each update.
    
    Similar to IndiEngine, but focused specifically on Bollinger Bands features.
    """
    
    def __init__(self, window: int = 20, k: float = 2.0):
        """
        Args:
            window: Rolling window size for SMA and std calculation (default 20)
            k: Number of standard deviations for bands (default 2.0)
        """
        self.window = window
        self.k = k
        # Store prices in a deque with maxlen to maintain rolling window
        self._prices = deque(maxlen=max(window, 100))  # Keep at least window size, but allow more for flexibility
    
    def update(self, *, price: float) -> Dict[str, Optional[float]]:
        """
        Update with a new price and compute Bollinger Bands features.
        
        Args:
            price: Current close/mid price
            
        Returns:
            Dict with boll_mid, boll_up, boll_low, boll_bandwidth, boll_position
        """
        self._prices.append(price)
        return compute_bollinger_features(self._prices, window=self.window, k=self.k)

