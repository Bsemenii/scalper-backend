# risk/limits.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class DayState:
    pnl_r: float = 0.0
    losses_streak: int = 0
    trading_enabled: bool = True

@dataclass
class LimitCfg:
    daily_stop_r: float = -2.0
    daily_target_r: float = 4.0
    max_consec_losses: int = 3

def on_trade_close(state: DayState, trade_r: float, cfg: LimitCfg) -> None:
    state.pnl_r += trade_r
    state.losses_streak = state.losses_streak + 1 if trade_r < 0 else 0
    if state.pnl_r <= cfg.daily_stop_r or state.losses_streak >= cfg.max_consec_losses:
        state.trading_enabled = False

def allow_new_trade(state: DayState, cfg: LimitCfg) -> bool:
    return state.trading_enabled and state.pnl_r < cfg.daily_target_r
