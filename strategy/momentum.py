# strategy/momentum.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, List

Side = Literal["LONG", "SHORT"]

@dataclass
class MomentumCfg:
    obi_t: float = 0.10
    stop_k_sigma: float = 1.0
    tp_r: float = 1.8

@dataclass
class EntryCandidate:
    symbol: str
    side: Side
    stop_distance_usd: float
    reason_tags: List[str]

def _stop_from_vol_bp(mid: float, realized_vol_bp: float, k: float) -> float:
    # realized_vol_bp — базисные пункты *за окно*, стоп = k * vol_bp * mid / 10000
    if realized_vol_bp is None:
        realized_vol_bp = 10.0
    return max(0.01, k * (realized_vol_bp / 10_000.0) * mid)

def signal_momentum(
    *,
    symbol: str,
    micro: dict,
    indi: dict,
    cfg: MomentumCfg,
) -> Optional[EntryCandidate]:
    """
    Условия (упрощённые и надёжные):
    - LONG: micro.obi > t и ema9 > ema21 и vwap_drift>=0
    - SHORT: micro.obi < -t и ema9 < ema21 и vwap_drift<=0
    """
    if not micro or not indi:
        return None

    mid = micro.get("mid")
    obi = micro.get("obi")
    ema9 = indi.get("ema9")
    ema21 = indi.get("ema21")
    vwap_drift = indi.get("vwap_drift")
    rv_bp = indi.get("realized_vol_bp")

    if not all(x is not None for x in [mid, obi, ema9, ema21, vwap_drift]):
        return None

    tags: List[str] = []
    if obi > cfg.obi_t and ema9 > ema21 and (vwap_drift is None or vwap_drift >= 0):
        tags = ["mom", "obi+", "ema_up", "vwap_aligned"]
        stop = _stop_from_vol_bp(mid, rv_bp, cfg.stop_k_sigma)
        return EntryCandidate(symbol=symbol, side="LONG", stop_distance_usd=stop, reason_tags=tags)

    if obi < -cfg.obi_t and ema9 < ema21 and (vwap_drift is None or vwap_drift <= 0):
        tags = ["mom", "obi-", "ema_dn", "vwap_aligned"]
        stop = _stop_from_vol_bp(mid, rv_bp, cfg.stop_k_sigma)
        return EntryCandidate(symbol=symbol, side="SHORT", stop_distance_usd=stop, reason_tags=tags)

    return None
