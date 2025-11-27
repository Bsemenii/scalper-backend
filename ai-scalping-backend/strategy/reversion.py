# strategy/reversion.py
"""
Bollinger Band Reversion Strategy

This strategy implements a clean Bollinger-band reversion pattern:
- Detects when price pierces outside the band then closes back inside
- Uses well-defined SL/TP based on band geometry and risk-reward ratio
- Filters out strong trends via mid-band slope constraint

TIMEFRAME: This strategy is designed to work with 1-minute timeframe data.
The indicators (bb_z, ema21, etc.) should be computed from 1-minute candles or
1-minute equivalent tick windows (approximately 60 ticks per minute for active symbols).
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, List
import logging

# Try to import get_settings for fee configuration
try:
    from bot.core.config import get_settings
    HAS_SETTINGS = True
except Exception:
    HAS_SETTINGS = False
    get_settings = None

Side = Literal["LONG","SHORT"]

# Logger for strategy diagnostics
_logger = logging.getLogger(__name__)

@dataclass
class ReversionCfg:
    bb_k: float = 2.0              # band distance in sigmas (default 2.0 = standard BB)
    bb_z_trigger: float = 1.2      # z-score threshold for overbought/oversold detection
    stop_k_sigma: float = 0.45     # SL slightly beyond the band (in sigma units)
    tp_rr_main: float = 1.6        # main risk-reward in R (TP distance = R * SL distance)
    max_mid_slope_bp: float = 45.0 # max allowed slope of the mid band over N candles, in basis points
    entry_band_buffer_bp: float = 20.0  # entry buffer in basis points: entries only when price is within this distance from the band
    min_rr_after_fees: float = 0.9  # minimum risk-reward ratio after accounting for fees
    allow_micro_pierce: bool = True  # if price touches OR slightly pierces the band by ≤ 0.05%, treat it as valid re-entry
    # Legacy field for backward compatibility (now maps to bb_z_trigger)
    bb_z: float = 2.0              # deprecated: use bb_z_trigger instead

@dataclass
class EntryCandidate:
    symbol: str
    side: Side
    stop_distance_usd: float
    reason_tags: List[str]
    # Strategy-provided SL/TP prices (required for automatic exchange order placement)
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    # Risk-reward metrics (for logging and analysis)
    rr_gross: Optional[float] = None
    rr_after_fees: Optional[float] = None


def _estimate_bb_bands(
    price: float,
    bb_z: float,
    realized_vol_bp: float,
    bb_k: float,
) -> tuple[float, float, float, float]:
    """
    Estimate Bollinger Bands from available indicators.
    
    Since we have bb_z = (price - mean) / std, we can estimate:
    - std ≈ price * realized_vol_bp / 10000 (assuming log-normal approximation)
    - mean ≈ price - bb_z * std
    - upper = mean + bb_k * std
    - lower = mean - bb_k * std
    
    Args:
        price: Current price (mid)
        bb_z: Current z-score (price - mean) / std
        realized_vol_bp: Realized volatility in basis points
        bb_k: Band width multiplier (default 2.0)
    
    Returns:
        (mid, upper, lower, sigma) tuple
    """
    if price <= 0:
        return (price, price, price, 0.0)
    
    # Estimate sigma from realized volatility
    # realized_vol_bp = std(log-returns) * 10000
    # For price std: std(price) ≈ price * std(log-returns) = price * realized_vol_bp / 10000
    sigma = price * max(realized_vol_bp, 10.0) / 10_000.0
    
    # Estimate mean from bb_z: bb_z = (price - mean) / sigma
    # mean = price - bb_z * sigma
    mid_est = price - bb_z * sigma
    
    # Compute bands
    upper = mid_est + bb_k * sigma
    lower = mid_est - bb_k * sigma
    
    return (mid_est, upper, lower, sigma)


def _estimate_mid_slope_bp(
    ema21: Optional[float],
    ema_slope_ticks: Optional[float],
    price_tick: float,
    mid_est: float,
) -> float:
    """
    Estimate mid-band slope in basis points.
    
    Since we don't have historical mid-band values, we approximate using:
    - EMA21 as proxy for mid-band trend
    - ema_slope_ticks from indicators (difference between ema9 and ema21 in ticks)
    
    The slope is converted to basis points relative to the mid price.
    
    Note: This is an approximation. For a more accurate slope, we would need
    historical mid-band values over N candles (e.g. 20).
    
    Args:
        ema21: EMA21 value (proxy for mid-band)
        ema_slope_ticks: Slope in ticks from indicators
        price_tick: Price tick size
        mid_est: Estimated mid-band value
    
    Returns:
        Slope in basis points
    """
    if ema_slope_ticks is not None and price_tick > 0 and mid_est > 0:
        # Convert tick slope to price slope, then to basis points
        slope_price = ema_slope_ticks * price_tick
        slope_bp = (slope_price / mid_est) * 10_000.0
        return slope_bp
    
    # Fallback: use a conservative estimate if data unavailable
    # Assume flat trend if we can't compute
    return 0.0


def signal_reversion(
    *,
    symbol: str,
    micro: dict,
    indi: dict,
    cfg: ReversionCfg,
) -> Optional[EntryCandidate]:
    """
    Bollinger Band Reversion Strategy
    
    TIMEFRAME: Reversion strategy for GALAUSDT uses 1-minute candles.
    All indicators (bb_z, ema21, rsi, realized_vol_bp) are computed from 1-minute timeframe data.
    
    Entry Logic:
    - LONG: Price was oversold beyond lower band (z_prev <= -bb_z_trigger),
            now re-enters the band (price >= lower)
    - SHORT: Price was overbought beyond upper band (z_prev >= bb_z_trigger),
             now re-enters the band (price <= upper)
    
    SL/TP:
    - SL is placed slightly beyond the band (stop_k_sigma * sigma beyond)
    - TP is set at a risk-reward multiple (tp_rr_main) of the SL distance
    - TP is optionally clamped near the opposite band
    
    Risk Management:
    - stop_distance_usd is computed for position sizing: qty = risk_per_trade_usd / stop_distance_usd
    - Trend filter: blocks entries if mid-band slope exceeds max_mid_slope_bp
    - Entry buffer: entries only when price is within entry_band_buffer_bp of the band
    
    Note: This implementation uses current indicators to approximate previous candle conditions.
    For a production system, consider maintaining historical mid-band and z-score values.
    """
    import time
    
    # Extract required data
    price_now = micro.get("mid")
    bb_z_now = indi.get("bb_z")
    rv_bp = indi.get("realized_vol_bp")
    ema21 = indi.get("ema21")
    ema_slope_ticks = indi.get("ema_slope_ticks")
    spread_ticks = micro.get("spread_ticks")
    top_liq_usd = micro.get("top5_liq_usd") or micro.get("top_liq_usd")
    
    # Required fields check
    if price_now is None or bb_z_now is None:
        _logger.debug(f"[reversion:{symbol}] skip - missing data: price_now={price_now}, bb_z_now={bb_z_now}")
        return None
    
    price_now = float(price_now)
    bb_z_now = float(bb_z_now)
    
    # Initial DEBUG log with symbol, price, and z-score
    _logger.debug(
        f"[reversion:{symbol}] START eval: price={price_now:.6f}, bb_z={bb_z_now:.3f}"
    )
    
    # Use bb_z_trigger from cfg, fallback to legacy bb_z for backward compatibility
    bb_z_trigger = getattr(cfg, 'bb_z_trigger', cfg.bb_z)
    bb_k = getattr(cfg, 'bb_k', 2.0)
    stop_k_sigma = getattr(cfg, 'stop_k_sigma', 0.45)
    tp_rr_main = getattr(cfg, 'tp_rr_main', 1.6)
    max_mid_slope_bp = getattr(cfg, 'max_mid_slope_bp', 45.0)
    entry_band_buffer_bp = getattr(cfg, 'entry_band_buffer_bp', 20.0)
    min_rr_after_fees = getattr(cfg, 'min_rr_after_fees', 0.9)
    allow_micro_pierce = getattr(cfg, 'allow_micro_pierce', True)
    
    # Get fee configuration from account settings
    fee_rate = 0.0005  # default taker rate
    if HAS_SETTINGS and get_settings:
        try:
            settings = get_settings()
            account_cfg = getattr(settings, 'account', None)
            if account_cfg:
                fees_cfg = getattr(account_cfg, 'fees', None)
                if fees_cfg:
                    fee_rate = getattr(fees_cfg, 'taker_rate', 0.0005)
        except Exception:
            pass  # Use default if settings unavailable
    
    # Estimate Bollinger Bands
    mid, upper, lower, sigma = _estimate_bb_bands(
        price=price_now,
        bb_z=bb_z_now,
        realized_vol_bp=float(rv_bp or 50.0),
        bb_k=bb_k,
    )
    
    if sigma <= 0:
        _logger.debug(f"[reversion:{symbol}] REJECT - invalid sigma: {sigma:.6f}")
        return None
    
    # Log basic setup info: mid, upper, lower, sigma
    _logger.debug(
        f"[reversion:{symbol}] bands: mid={mid:.6f}, upper={upper:.6f}, lower={lower:.6f}, sigma={sigma:.6f}"
    )
    
    # Approximate previous z-score for reversion detection
    # Since we don't have historical data, we use current z-score as proxy
    # This means we're looking for cases where price is currently at the edge
    # and was previously outside. In practice, this approximation works when
    # price is re-entering the band after a pierce.
    z_prev = bb_z_now  # Approximation: assume z hasn't changed dramatically
    
    # Spread/liquidity filters (reuse existing safety checks)
    _logger.debug(
        f"[reversion:{symbol}] spread/liquidity: spread_ticks={spread_ticks}, top_liq_usd={top_liq_usd:.0f}"
    )
    if spread_ticks is not None and spread_ticks > 10:
        _logger.debug(f"[reversion:{symbol}] REJECT - spread too wide: {spread_ticks:.2f} ticks > 10")
        return None
    if top_liq_usd is not None and top_liq_usd < 100_000:
        _logger.debug(f"[reversion:{symbol}] REJECT - insufficient liquidity: {top_liq_usd:.0f} USD < 100000")
        return None
    
    # Estimate price tick for slope calculation (approximate from price)
    # For most crypto, tick is 0.01 for prices < 10, 0.1 for prices < 100, etc.
    price_tick = 0.01
    if price_now >= 100:
        price_tick = 0.1
    if price_now >= 1000:
        price_tick = 1.0
    
    # Trend filter: check mid-band slope (relaxed)
    slope_bp = _estimate_mid_slope_bp(
        ema21=ema21,
        ema_slope_ticks=ema_slope_ticks,
        price_tick=price_tick,
        mid_est=mid,
    )
    
    _logger.debug(
        f"[reversion:{symbol}] slope: slope_bp={slope_bp:.2f}, max_allowed={max_mid_slope_bp:.2f}, "
        f"ema21={ema21}, ema_slope_ticks={ema_slope_ticks}"
    )
    
    # Calculate entry buffer in price units (basis points -> price)
    entry_buffer_px = price_now * (entry_band_buffer_bp / 10_000.0)
    
    # Relaxed slope filter: allow if price is within entry buffer even if slope is slightly above
    slope_ok = abs(slope_bp) <= max_mid_slope_bp
    slope_bypass = False
    if not slope_ok and abs(slope_bp) <= max_mid_slope_bp * 1.2:  # Allow 20% tolerance
        # Check if price is near band (within buffer)
        distance_from_lower = price_now - lower
        distance_from_upper = upper - price_now
        near_band = (distance_from_lower >= 0 and distance_from_lower <= entry_buffer_px * 1.5) or \
                    (distance_from_upper >= 0 and distance_from_upper <= entry_buffer_px * 1.5)
        if near_band:
            slope_ok = True
            slope_bypass = True
            _logger.debug(
                f"[REVERSION] bypass slope filter (price near band): slope_bp={slope_bp:.2f}, "
                f"max={max_mid_slope_bp:.2f}"
            )
    
    if not slope_ok:
        _logger.debug(
            f"[reversion:{symbol}] REJECT - slope too steep: |{slope_bp:.2f}| bp > {max_mid_slope_bp:.2f} bp"
        )
        return None
    
    # Micro pierce threshold (used for both LONG and SHORT)
    micro_pierce_threshold = 0.0005  # 0.05%
    
    # Initialize variables for both LONG and SHORT (for final rejection log)
    distance_from_lower = price_now - lower
    distance_from_upper = upper - price_now
    was_oversold = False
    was_overbought = False
    micro_pierce_long = False
    micro_pierce_short = False
    near_lower_band = False
    near_upper_band = False
    
    # LONG entry conditions (Bollinger reversion from lower band)
    # Micro pierce logic: treat price <= lower*(1 + 0.0005) as oversold
    lower_with_micro_pierce = lower * (1.0 + micro_pierce_threshold)
    
    if allow_micro_pierce and price_now <= lower_with_micro_pierce:
        # Micro pierce detected
        micro_pierce_long = True
        was_oversold = True
        _logger.debug(
            f"[reversion:{symbol}] MICRO-PIERCE-LONG: price={price_now:.6f} <= lower_with_pierce={lower_with_micro_pierce:.6f} "
            f"(lower={lower:.6f})"
        )
    elif price_now <= lower:
        # Standard pierce
        was_oversold = True
    elif z_prev <= -bb_z_trigger:
        # Z-score trigger
        was_oversold = True
    
    # Entry buffer check (relaxed for micro pierce)
    entry_buffer_px_expanded = entry_buffer_px * 1.5 if micro_pierce_long else entry_buffer_px
    near_lower_band = (distance_from_lower >= -entry_buffer_px_expanded) and (distance_from_lower <= entry_buffer_px)
    now_reentering_long = near_lower_band or micro_pierce_long
    
    # Check band buffer distance for LONG (relaxed for micro pierce)
    if was_oversold and not micro_pierce_long and distance_from_lower > entry_buffer_px:
        _logger.debug(
            f"[reversion:{symbol}] REJECT LONG: too far from band (dist={distance_from_lower:.6f} buffer_px={entry_buffer_px:.6f})"
        )
        return None
    
    _logger.debug(
        f"[reversion:{symbol}] LONG conditions: "
        f"was_oversold={was_oversold} (z_prev={z_prev:.3f}, trigger=-{bb_z_trigger:.3f}), "
        f"micro_pierce_long={micro_pierce_long}, "
        f"distance_from_lower={distance_from_lower:.6f}, entry_buffer_px={entry_buffer_px:.6f} "
        f"(buffer_bp={entry_band_buffer_bp:.2f}), "
        f"near_lower_band={near_lower_band}, now_reentering_long={now_reentering_long}"
    )
    
    if was_oversold and now_reentering_long:
        # LONG entry
        entry_price = price_now
        
        # SL slightly beyond the lower band
        sl_price = lower - stop_k_sigma * sigma
        stop_distance_abs = entry_price - sl_price
        
        # Ensure minimum stop distance (safety check)
        if stop_distance_abs <= 0:
            _logger.debug(
                f"[reversion:{symbol}] REJECT LONG - stop_distance_abs invalid: {stop_distance_abs:.6f}"
            )
            return None
        
        # TP at risk-reward multiple
        tp_price = entry_price + tp_rr_main * stop_distance_abs
        
        # Allow TP slightly beyond band (up to 3*sigma for flexibility)
        tp_price_clamped = min(tp_price, mid + 3 * sigma)
        if tp_price != tp_price_clamped:
            _logger.debug(
                f"[reversion:{symbol}] LONG TP clamped: {tp_price:.6f} -> {tp_price_clamped:.6f} "
                f"(mid={mid:.6f}, upper={upper:.6f}, sigma={sigma:.6f})"
            )
        tp_price = tp_price_clamped
        
        # Compute risk-reward and fees
        risk_abs = abs(entry_price - sl_price)
        reward_abs = abs(tp_price - entry_price)
        
        if risk_abs <= 0 or reward_abs <= 0:
            _logger.debug(
                f"[reversion:{symbol}] REJECT LONG: invalid risk/reward - risk_abs={risk_abs:.6f}, reward_abs={reward_abs:.6f}"
            )
            return None
        
        # Compute gross RR
        rr_gross = reward_abs / risk_abs
        
        # Compute estimated fees (assume taker entry + taker exit)
        fees_usd_per_qty = entry_price * fee_rate + tp_price * fee_rate
        fees_r = fees_usd_per_qty / risk_abs
        
        # Compute RR after fees
        rr_after_fees = rr_gross - fees_r
        
        # Check minimum RR after fees (relaxed for micro pierce)
        if rr_after_fees < min_rr_after_fees:
            if micro_pierce_long and allow_micro_pierce:
                # Allow lower RR for micro pierce conditions
                _logger.debug(
                    f"[REVERSION] rr_after_fees low but micro-pierce conditions allow entry: "
                    f"rr_gross={rr_gross:.3f} fees_r={fees_r:.3f} "
                    f"rr_after_fees={rr_after_fees:.3f} < min_rr_after_fees={min_rr_after_fees:.3f}"
                )
                # Continue with entry
            else:
                _logger.debug(
                    f"[reversion:{symbol}] REJECT LONG: rr_gross={rr_gross:.3f} fees_r={fees_r:.3f} "
                    f"rr_after_fees={rr_after_fees:.3f} < min_rr_after_fees={min_rr_after_fees:.3f}"
                )
                return None
        
        # Convert stop distance from price units to USD (for linear contracts, multiplier = 1)
        stop_distance_usd = stop_distance_abs
        
        # Final safety check
        if stop_distance_usd <= 0.01:
            _logger.debug(
                f"[reversion:{symbol}] REJECT LONG - stop_distance_usd too small: {stop_distance_usd:.6f}"
            )
            return None
        
        # Aggregated log showing all key metrics
        _logger.info(
            f"[reversion:{symbol}] ACCEPT LONG: price={price_now:.6f} mid={mid:.6f} upper={upper:.6f} lower={lower:.6f} "
            f"entry_buffer_px={entry_buffer_px:.6f} micro_pierce={micro_pierce_long} "
            f"rr_gross={rr_gross:.3f} rr_after_fees={rr_after_fees:.3f} "
            f"sl={sl_price:.6f} tp={tp_price:.6f}"
        )
        
        tags = ["rev", "bb_lo", "long_revert"]
        if micro_pierce_long:
            tags.append("micro_pierce")
        return EntryCandidate(
            symbol=symbol,
            side="LONG",
            stop_distance_usd=stop_distance_usd,
            reason_tags=tags,
            sl_price=sl_price,
            tp_price=tp_price,
            rr_gross=rr_gross,
            rr_after_fees=rr_after_fees,
        )
    
    # SHORT entry conditions (Bollinger reversion from upper band)
    # Micro pierce logic: treat price >= upper*(1 - 0.0005) as overbought
    upper_with_micro_pierce = upper * (1.0 - micro_pierce_threshold)
    
    if allow_micro_pierce and price_now >= upper_with_micro_pierce:
        # Micro pierce detected
        micro_pierce_short = True
        was_overbought = True
        _logger.debug(
            f"[reversion:{symbol}] MICRO-PIERCE-SHORT: price={price_now:.6f} >= upper_with_pierce={upper_with_micro_pierce:.6f} "
            f"(upper={upper:.6f})"
        )
    elif price_now >= upper:
        # Standard pierce
        was_overbought = True
    elif z_prev >= bb_z_trigger:
        # Z-score trigger
        was_overbought = True
    
    # Entry buffer check (relaxed for micro pierce)
    entry_buffer_px_expanded = entry_buffer_px * 1.5 if micro_pierce_short else entry_buffer_px
    near_upper_band = (distance_from_upper >= -entry_buffer_px_expanded) and (distance_from_upper <= entry_buffer_px)
    now_reentering_short = near_upper_band or micro_pierce_short
    
    # Check band buffer distance for SHORT (relaxed for micro pierce)
    if was_overbought and not micro_pierce_short and distance_from_upper > entry_buffer_px:
        _logger.debug(
            f"[reversion:{symbol}] REJECT SHORT: too far from band (dist={distance_from_upper:.6f} buffer_px={entry_buffer_px:.6f})"
        )
        return None
    
    _logger.debug(
        f"[reversion:{symbol}] SHORT conditions: "
        f"was_overbought={was_overbought} (z_prev={z_prev:.3f}, trigger={bb_z_trigger:.3f}), "
        f"micro_pierce_short={micro_pierce_short}, "
        f"distance_from_upper={distance_from_upper:.6f}, entry_buffer_px={entry_buffer_px:.6f} "
        f"(buffer_bp={entry_band_buffer_bp:.2f}), "
        f"near_upper_band={near_upper_band}, now_reentering_short={now_reentering_short}"
    )
    
    if was_overbought and now_reentering_short:
        # SHORT entry
        entry_price = price_now
        
        # SL slightly beyond the upper band
        sl_price = upper + stop_k_sigma * sigma
        stop_distance_abs = sl_price - entry_price
        
        # Ensure minimum stop distance (safety check)
        if stop_distance_abs <= 0:
            _logger.debug(
                f"[reversion:{symbol}] REJECT SHORT - stop_distance_abs invalid: {stop_distance_abs:.6f}"
            )
            return None
        
        # TP at risk-reward multiple
        tp_price = entry_price - tp_rr_main * stop_distance_abs
        
        # Allow TP slightly beyond band (up to 3*sigma for flexibility)
        tp_price_clamped = max(tp_price, mid - 3 * sigma)
        if tp_price != tp_price_clamped:
            _logger.debug(
                f"[reversion:{symbol}] SHORT TP clamped: {tp_price:.6f} -> {tp_price_clamped:.6f} "
                f"(mid={mid:.6f}, lower={lower:.6f}, sigma={sigma:.6f})"
            )
        tp_price = tp_price_clamped
        
        # Compute risk-reward and fees
        risk_abs = abs(entry_price - sl_price)
        reward_abs = abs(tp_price - entry_price)
        
        if risk_abs <= 0 or reward_abs <= 0:
            _logger.debug(
                f"[reversion:{symbol}] REJECT SHORT: invalid risk/reward - risk_abs={risk_abs:.6f}, reward_abs={reward_abs:.6f}"
            )
            return None
        
        # Compute gross RR
        rr_gross = reward_abs / risk_abs
        
        # Compute estimated fees (assume taker entry + taker exit)
        fees_usd_per_qty = entry_price * fee_rate + tp_price * fee_rate
        fees_r = fees_usd_per_qty / risk_abs
        
        # Compute RR after fees
        rr_after_fees = rr_gross - fees_r
        
        # Check minimum RR after fees (relaxed for micro pierce)
        if rr_after_fees < min_rr_after_fees:
            if micro_pierce_short and allow_micro_pierce:
                # Allow lower RR for micro pierce conditions
                _logger.debug(
                    f"[REVERSION] rr_after_fees low but micro-pierce conditions allow entry: "
                    f"rr_gross={rr_gross:.3f} fees_r={fees_r:.3f} "
                    f"rr_after_fees={rr_after_fees:.3f} < min_rr_after_fees={min_rr_after_fees:.3f}"
                )
                # Continue with entry
            else:
                _logger.debug(
                    f"[reversion:{symbol}] REJECT SHORT: rr_gross={rr_gross:.3f} fees_r={fees_r:.3f} "
                    f"rr_after_fees={rr_after_fees:.3f} < min_rr_after_fees={min_rr_after_fees:.3f}"
                )
                return None
        
        # Convert stop distance from price units to USD (for linear contracts, multiplier = 1)
        stop_distance_usd = stop_distance_abs
        
        # Final safety check
        if stop_distance_usd <= 0.01:
            _logger.debug(
                f"[reversion:{symbol}] REJECT SHORT - stop_distance_usd too small: {stop_distance_usd:.6f}"
            )
            return None
        
        # Aggregated log showing all key metrics
        _logger.info(
            f"[reversion:{symbol}] ACCEPT SHORT: price={price_now:.6f} mid={mid:.6f} upper={upper:.6f} lower={lower:.6f} "
            f"entry_buffer_px={entry_buffer_px:.6f} micro_pierce={micro_pierce_short} "
            f"rr_gross={rr_gross:.3f} rr_after_fees={rr_after_fees:.3f} "
            f"sl={sl_price:.6f} tp={tp_price:.6f}"
        )
        
        tags = ["rev", "bb_hi", "short_revert"]
        if micro_pierce_short:
            tags.append("micro_pierce")
        return EntryCandidate(
            symbol=symbol,
            side="SHORT",
            stop_distance_usd=stop_distance_usd,
            reason_tags=tags,
            sl_price=sl_price,
            tp_price=tp_price,
            rr_gross=rr_gross,
            rr_after_fees=rr_after_fees,
        )
    
    # No valid signal - aggregated log with all decision factors
    _logger.debug(
        f"[reversion:{symbol}] REJECT - no signal: "
        f"price={price_now:.6f} mid={mid:.6f} upper={upper:.6f} lower={lower:.6f} "
        f"entry_buffer_px={entry_buffer_px:.6f} "
        f"LONG: was_oversold={was_oversold}, micro_pierce_long={micro_pierce_long}, "
        f"near_lower={near_lower_band}, distance_from_lower={distance_from_lower:.6f}; "
        f"SHORT: was_overbought={was_overbought}, micro_pierce_short={micro_pierce_short}, "
        f"near_upper={near_upper_band}, distance_from_upper={distance_from_upper:.6f}"
    )
    return None
