# strategy/reversion.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, List

Side = Literal["LONG","SHORT"]

@dataclass
class ReversionCfg:
    bb_z: float = 2.0  # порог z-score Bollinger (по модулю)
    stop_k_sigma: float = 1.0

@dataclass
class EntryCandidate:
    symbol: str
    side: Side
    stop_distance_usd: float
    reason_tags: List[str]

def signal_reversion(
    *,
    symbol: str,
    micro: dict,
    indi: dict,
    cfg: ReversionCfg,
) -> Optional[EntryCandidate]:
    mid = micro.get("mid")
    z = indi.get("bb_z")
    rv_bp = indi.get("realized_vol_bp")

    if mid is None or z is None:
        return None

    tags: List[str] = []
    if z >= cfg.bb_z:
        # перекупленность → SHORT reversion
        stop = max(0.01, cfg.stop_k_sigma * (rv_bp or 10.0) / 10_000.0 * mid)
        tags = ["rev", "bb_hi"]
        return EntryCandidate(symbol=symbol, side="SHORT", stop_distance_usd=stop, reason_tags=tags)
    if z <= -cfg.bb_z:
        # перепроданность → LONG reversion
        stop = max(0.01, cfg.stop_k_sigma * (rv_bp or 10.0) / 10_000.0 * mid)
        tags = ["rev", "bb_lo"]
        return EntryCandidate(symbol=symbol, side="LONG", stop_distance_usd=stop, reason_tags=tags)

    return None
