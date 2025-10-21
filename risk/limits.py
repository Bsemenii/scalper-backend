# risk/limits.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class DayState:
    pnl_r: float = 0.0
    losses_streak: int = 0
    trading_enabled: bool = True
    cooldown_until_ms: int = 0

@dataclass
class LimitCfg:
    daily_stop_r: float = -10.0
    daily_target_r: float = 15.0
    max_consec_losses: int = 3
    cooldown_after_sl_s: int = 120

def on_trade_close(state: DayState, trade_r: float, cfg: LimitCfg, now_ms: int) -> None:
    state.pnl_r += trade_r
    if trade_r < 0:
        state.losses_streak += 1
        state.cooldown_until_ms = now_ms + cfg.cooldown_after_sl_s * 1000
    else:
        state.losses_streak = 0
    # отключения
    if state.pnl_r <= cfg.daily_stop_r:
        state.trading_enabled = False
    if state.pnl_r >= cfg.daily_target_r:
        state.trading_enabled = False

def can_enter(state: DayState, cfg: LimitCfg, now_ms: int) -> bool:
    if not state.trading_enabled:
        return False
    if state.losses_streak >= cfg.max_consec_losses:
        return False
    if now_ms < state.cooldown_until_ms:
        return False
    return True
