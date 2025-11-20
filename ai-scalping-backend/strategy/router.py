# strategy/router.py
from __future__ import annotations
from typing import Optional
from dataclasses import dataclass, field

from .momentum import signal_momentum, MomentumCfg, EntryCandidate as MomCand
from .reversion import signal_reversion, ReversionCfg, EntryCandidate as RevCand

# У обоих EntryCandidate одинаковые поля — используем любой как alias
EntryCandidate = MomCand

@dataclass(frozen=True)
class RegimeCfg:
    vol_window_s: int = 60  # хук под определение режима; пока не используем

@dataclass(frozen=True)
class StrategyCfg:
    regime: RegimeCfg = field(default_factory=RegimeCfg)
    momentum: MomentumCfg = field(default_factory=MomentumCfg)
    reversion: ReversionCfg = field(default_factory=ReversionCfg)

def pick_candidate(
    *,
    symbol: str,
    micro: dict,
    indi: dict,
    safety_ok: bool,
    limits_ok: bool,
    cfg: StrategyCfg,
) -> Optional[EntryCandidate]:
    if not safety_ok or not limits_ok:
        return None

    # Простой роутинг: сначала Momentum, если нет — Reversion
    c = signal_momentum(symbol=symbol, micro=micro, indi=indi, cfg=cfg.momentum)
    if c:
        return c
    c = signal_reversion(symbol=symbol, micro=micro, indi=indi, cfg=cfg.reversion)
    return c
