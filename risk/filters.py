# risk/filters.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class RiskParams:
    max_spread_ticks: float = 3.0
    min_top5_liq_usd: float = 300000.0
    skip_minute_zero: bool = True
    min_liq_buffer_sl_mult: float = 3.0  # top_liq_usd >= sl_usd * mult

def allow_trade(mf, now_ts_ms:int, p:RiskParams, sl_distance_usd: float | None = None) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    ok = True
    if mf.spread_ticks > p.max_spread_ticks:
        ok = False; reasons.append("spread")
    if mf.top_liq_usd < p.min_top5_liq_usd:
        ok = False; reasons.append("liq")
    if sl_distance_usd is not None:
        if mf.top_liq_usd < sl_distance_usd * p.min_liq_buffer_sl_mult:
            ok = False; reasons.append("liq_buffer")
    mm = (now_ts_ms // 1000) % 60
    if mm == 0 and p.skip_minute_zero:
        ok = False; reasons.append("minute_zero")
    return ok, reasons
