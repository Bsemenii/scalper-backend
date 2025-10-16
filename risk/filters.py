# risk/filters.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional
import time

@dataclass
class SafetyCfg:
    max_spread_ticks: float = 3.0
    min_top5_liquidity_usd: float = 300_000.0
    skip_funding_minute: bool = True
    skip_minute_zero: bool = True
    min_liq_buffer_sl_mult: float = 3.0  # запас ликвидности относительно стопа

@dataclass
class LimitsCfg:
    daily_stop_r: float = -2.0
    daily_target_r: float = 4.0
    max_consec_losses: int = 3
    cooldown_after_sl_s: int = 120

@dataclass
class LimitsState:
    daily_pnl_r: float = 0.0
    consec_losses: int = 0
    cooldown_until_ts_ms: int = 0

    def can_trade(self, now_ms: int, cfg: LimitsCfg) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        if self.daily_pnl_r <= cfg.daily_stop_r:
            reasons.append("daily_stop")
        if self.daily_pnl_r >= cfg.daily_target_r:
            reasons.append("daily_target")
        if self.consec_losses >= cfg.max_consec_losses:
            reasons.append("max_consec_losses")
        if now_ms < self.cooldown_until_ts_ms:
            reasons.append("cooldown")
        return (len(reasons) == 0, reasons)

    def on_trade_closed(self, pnl_r: float, now_ms: int, cfg: LimitsCfg) -> None:
        self.daily_pnl_r += pnl_r
        if pnl_r < 0:
            self.consec_losses += 1
            self.cooldown_until_ts_ms = max(
                self.cooldown_until_ts_ms, now_ms + cfg.cooldown_after_sl_s * 1000
            )
        else:
            self.consec_losses = 0

def evaluate_safety(
    *, micro: dict, indi: dict, safety: SafetyCfg, now_ts_ms: Optional[int] = None, sl_usd_est: Optional[float] = None
) -> Tuple[bool, List[str]]:
    """
    micro = {
      'spread_ticks': float,
      'top_liq_usd': float,
      'mid': float,
    }
    indi = not strictly needed here, but kept for future checks
    """
    reasons: List[str] = []
    if micro.get("spread_ticks") is None or micro.get("top_liq_usd") is None:
        return False, ["no_micro"]

    if micro["spread_ticks"] > safety.max_spread_ticks:
        reasons.append("spread")
    if micro["top_liq_usd"] < safety.min_top5_liquidity_usd:
        reasons.append("liq")

    # Минутные запреты: funding и :00
    ts = now_ts_ms or int(time.time() * 1000)
    sec = (ts // 1000) % 60
    if safety.skip_minute_zero and sec == 0:
        reasons.append("m:00")
    # funding (~каждые 8ч, обычно в :00) — мы уже ловим m:00, оставим hook:
    if safety.skip_funding_minute:
        pass

    # Запас ликвидности относительно стопа (если предоставлен sl_usd_est)
    if sl_usd_est is not None and micro["top_liq_usd"] < safety.min_liq_buffer_sl_mult * sl_usd_est:
        reasons.append("liq_buffer")

    return (len(reasons) == 0, reasons)
