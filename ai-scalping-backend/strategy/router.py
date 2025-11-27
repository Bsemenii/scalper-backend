# strategy/router.py
from __future__ import annotations
from typing import Optional, Literal
from dataclasses import dataclass, field
import logging

from .momentum import signal_momentum, MomentumCfg, EntryCandidate as MomCand
from .reversion import signal_reversion, ReversionCfg, EntryCandidate as RevCand
from .bollinger_basic import signal_bollinger_basic, BollingerBasicCfg
from .types import StrategyDecision

# Use reversion EntryCandidate as it has all optional fields (sl_price, tp_price, etc.)
EntryCandidate = RevCand

_logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class RegimeCfg:
    vol_window_s: int = 60  # хук под определение режима; пока не используем

@dataclass(frozen=True)
class StrategyCfg:
    regime: RegimeCfg = field(default_factory=RegimeCfg)
    momentum: MomentumCfg = field(default_factory=MomentumCfg)
    reversion: ReversionCfg = field(default_factory=ReversionCfg)
    bollinger_basic: BollingerBasicCfg = field(default_factory=BollingerBasicCfg)
    mode: str = "reversion_only"  # Strategy routing mode: "reversion_only" | "momentum_first" | "bollinger_basic" | etc.
    
    @classmethod
    def from_pydantic(cls, pydantic_cfg) -> "StrategyCfg":
        """
        Convert from Pydantic StrategyCfg (from bot.core.config) to dataclass StrategyCfg.
        """
        # Import here to avoid circular dependency
        from bot.core.config import StrategyCfg as PydanticStrategyCfg, BollingerBasicCfg as PydanticBollingerBasicCfg
        
        # Convert BollingerBasicCfg if present (backward compatible if missing)
        bollinger_cfg = BollingerBasicCfg()
        if hasattr(pydantic_cfg, 'bollinger_basic') and pydantic_cfg.bollinger_basic is not None:
            try:
                bb = pydantic_cfg.bollinger_basic
                bollinger_cfg = BollingerBasicCfg(
                    tp_pct=getattr(bb, 'tp_pct', 0.004),
                    sl_pct=getattr(bb, 'sl_pct', 0.0025),
                    entry_threshold_pct=getattr(bb, 'entry_threshold_pct', 0.0005),
                    require_bandwidth_min=getattr(bb, 'require_bandwidth_min', None),
                    entry_usd=getattr(bb, 'entry_usd', 25.0),
                    leverage=getattr(bb, 'leverage', 10.0),
                )
            except Exception:
                pass  # Use defaults if conversion fails
        
        return cls(
            regime=RegimeCfg(vol_window_s=pydantic_cfg.regime.vol_window_s),
            momentum=MomentumCfg(
                obi_t=pydantic_cfg.momentum.obi_t,
                tp_r=pydantic_cfg.momentum.tp_r,
                stop_k_sigma=pydantic_cfg.momentum.stop_k_sigma,
            ),
            reversion=ReversionCfg(
                bb_z=pydantic_cfg.reversion.bb_z,
                bb_z_trigger=pydantic_cfg.reversion.bb_z_trigger,
                bb_k=pydantic_cfg.reversion.bb_k,
                stop_k_sigma=pydantic_cfg.reversion.stop_k_sigma,
                tp_rr_main=pydantic_cfg.reversion.tp_rr_main,
                max_mid_slope_bp=pydantic_cfg.reversion.max_mid_slope_bp,
                entry_band_buffer_bp=pydantic_cfg.reversion.entry_band_buffer_bp,
                min_rr_after_fees=pydantic_cfg.reversion.min_rr_after_fees,
                allow_micro_pierce=pydantic_cfg.reversion.allow_micro_pierce,
            ),
            bollinger_basic=bollinger_cfg,
            mode=pydantic_cfg.mode,
        )

def _strategy_decision_to_entry_candidate(
    decision: StrategyDecision,
    entry_price: float,
) -> EntryCandidate:
    """
    Convert StrategyDecision to EntryCandidate.
    
    Computes SL/TP prices from percentage-based targets.
    """
    if decision.signal == "LONG":
        sl_price = entry_price * (1.0 - decision.sl_pct)
        tp_price = entry_price * (1.0 + decision.tp_pct)
        stop_distance_usd = entry_price - sl_price
    elif decision.signal == "SHORT":
        sl_price = entry_price * (1.0 + decision.sl_pct)
        tp_price = entry_price * (1.0 - decision.tp_pct)
        stop_distance_usd = sl_price - entry_price
    else:
        raise ValueError(f"Invalid signal: {decision.signal}")
    
    # Compute risk-reward ratios
    risk_abs = abs(entry_price - sl_price)
    reward_abs = abs(tp_price - entry_price)
    rr_gross = (reward_abs / risk_abs) if risk_abs > 0 else 0.0
    
    # Estimate fees (simplified - assume taker rate)
    try:
        from bot.core.config import get_settings
        settings = get_settings()
        fee_rate = getattr(settings.account.fees, 'taker_rate', 0.0005)
    except Exception:
        fee_rate = 0.0005
    
    fees_usd_per_qty = entry_price * fee_rate + tp_price * fee_rate
    fees_r = fees_usd_per_qty / risk_abs if risk_abs > 0 else 0.0
    rr_after_fees = rr_gross - fees_r
    
    return EntryCandidate(
        symbol=decision.symbol,
        side=decision.signal,
        stop_distance_usd=stop_distance_usd,
        reason_tags=decision.reason_tags or [],
        sl_price=sl_price,
        tp_price=tp_price,
        rr_gross=rr_gross,
        rr_after_fees=rr_after_fees,
    )


def pick_candidate(
    *,
    symbol: str,
    micro: dict,
    indi: dict,
    safety_ok: bool,
    limits_ok: bool,
    cfg: StrategyCfg,
    current_position_side: Optional[Literal["LONG", "SHORT"]] = None,
) -> Optional[EntryCandidate]:
    """
    Pick a trading candidate based on active strategy mode.
    
    Args:
        symbol: Trading symbol
        micro: Microstructure features dict
        indi: Indicators dict (includes Bollinger fields: boll_mid, boll_up, boll_low, etc.)
        safety_ok: Safety checks passed
        limits_ok: Risk limits checks passed
        cfg: Strategy configuration
        current_position_side: Current position side ("LONG", "SHORT", or None for FLAT)
    
    Returns:
        EntryCandidate or None
    """
    if not safety_ok or not limits_ok:
        _logger.debug(f"[router] no candidate for symbol={symbol}: safety_ok={safety_ok}, limits_ok={limits_ok}")
        return None

    # Bollinger Basic strategy mode
    if cfg.mode == "bollinger_basic":
        # Ensure price is in indi dict (from micro if needed)
        if "price" not in indi and "close" not in indi:
            mid = micro.get("mid")
            if mid is not None:
                indi = dict(indi)  # Copy to avoid mutation
                indi["price"] = float(mid)
        
        decision = signal_bollinger_basic(
            symbol=symbol,
            indi=indi,
            current_position_side=current_position_side,
            cfg=cfg.bollinger_basic,
        )
        
        if decision:
            entry_price = micro.get("mid") or indi.get("price") or indi.get("close") or 0.0
            if entry_price <= 0:
                _logger.debug(f"[router] no candidate for symbol={symbol}: invalid entry_price={entry_price}")
                return None
            
            candidate = _strategy_decision_to_entry_candidate(decision, float(entry_price))
            _logger.info(
                f"[WORKER] candidate -> side={candidate.side} entry={entry_price:.6f} "
                f"sl={candidate.sl_price:.6f} tp={candidate.tp_price:.6f} "
                f"rr_after_fees={candidate.rr_after_fees:.3f} reason={decision.reason}"
            )
            return candidate
        else:
            _logger.debug(f"[router] no candidate for symbol={symbol} (bollinger_basic mode)")
            return None

    # TEMP: single-strategy mode — Bollinger reversion only
    if cfg.mode == "reversion_only":
        candidate = signal_reversion(symbol=symbol, micro=micro, indi=indi, cfg=cfg.reversion)
        if candidate:
            # Log candidate details when received in auto mode
            entry_price = micro.get("mid") or 0.0
            sl_px = getattr(candidate, 'sl_price', None)
            tp_px = getattr(candidate, 'tp_price', None)
            rr_gross = getattr(candidate, 'rr_gross', None)
            rr_after_fees = getattr(candidate, 'rr_after_fees', None)
            # Check for micro_pierce tag
            tags = getattr(candidate, 'reason_tags', [])
            micro_pierce = 'micro_pierce' in tags if tags else False
            _logger.info(
                f"[WORKER] candidate -> side={candidate.side} entry={entry_price:.6f} "
                f"sl={sl_px:.6f} tp={tp_px:.6f} rr_after_fees={rr_after_fees:.3f} "
                f"micro={micro_pierce}"
            )
        else:
            _logger.debug(f"[router] no candidate for symbol={symbol} (reversion only mode)")
        return candidate

    # Legacy routing: сначала Momentum, если нет — Reversion
    # (disabled when mode == "reversion_only")
    c = signal_momentum(symbol=symbol, micro=micro, indi=indi, cfg=cfg.momentum)
    if c:
        entry_price = micro.get("mid") or 0.0
        sl_px = getattr(c, 'sl_price', None)
        tp_px = getattr(c, 'tp_price', None)
        rr_gross = getattr(c, 'rr_gross', None)
        rr_after_fees = getattr(c, 'rr_after_fees', None)
        tags = getattr(c, 'reason_tags', [])
        micro_pierce = 'micro_pierce' in tags if tags else False
        _logger.info(
            f"[WORKER] candidate -> side={c.side} entry={entry_price:.6f} "
            f"sl={sl_px:.6f} tp={tp_px:.6f} rr_after_fees={rr_after_fees:.3f} "
            f"micro={micro_pierce}"
        )
        return c
    c = signal_reversion(symbol=symbol, micro=micro, indi=indi, cfg=cfg.reversion)
    if c:
        entry_price = micro.get("mid") or 0.0
        sl_px = getattr(c, 'sl_price', None)
        tp_px = getattr(c, 'tp_price', None)
        rr_gross = getattr(c, 'rr_gross', None)
        rr_after_fees = getattr(c, 'rr_after_fees', None)
        tags = getattr(c, 'reason_tags', [])
        micro_pierce = 'micro_pierce' in tags if tags else False
        _logger.info(
            f"[WORKER] candidate -> side={c.side} entry={entry_price:.6f} "
            f"sl={sl_px:.6f} tp={tp_px:.6f} rr_after_fees={rr_after_fees:.3f} "
            f"micro={micro_pierce}"
        )
    return c
