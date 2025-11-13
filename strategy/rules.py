# bot/strategy/rules.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal, Dict, Any

Side = Literal["BUY", "SELL"]

@dataclass
class SignalCfg:
    # фильтры безопасности (пусть воркер тоже проверяет)
    max_spread_ticks: int = 3
    min_top_liq_usd: float = 300_000.0
    # моментум:
    obi_t: float = 0.12
    drift_t: float = 0.0      # microprice_drift > 0 для лонга
    vel_t: float = 0.0        # tick_velocity > 0 для лонга
    # реверсия:
    bb_z_t: float = 2.0       # |bb_z|>2
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0

def decide_signal(
    micro: Dict[str, Any],  # {spread_ticks, top5_liq_usd/top_liq_usd, microprice_drift, tick_velocity, obi, ...}
    indi: Optional[Dict[str, Any]] = None,  # {bb_z, rsi, realized_vol_bp, ...}
) -> Optional[Side]:
    """
    Возвращает "BUY" | "SELL" | None. Лёгкие, объяснимые правила.
    """
    cfg = SignalCfg()

    # --- safety: спред / ликвидность ---
    spread_raw = micro.get("spread_ticks", micro.get("spread", None))
    try:
        spread = float(spread_raw) if spread_raw is not None else 999.0
    except Exception:
        spread = 999.0

    # поддерживаем и новый, и старый ключ ликвидности
    top_raw = micro.get("top5_liq_usd", micro.get("top_liq_usd", None))
    try:
        top_liq = float(top_raw) if top_raw is not None else 0.0
    except Exception:
        top_liq = 0.0

    if spread > cfg.max_spread_ticks:
        return None
    if top_liq < cfg.min_top_liq_usd:
        return None

    # --- микроструктура ---
    drift = float(micro.get("microprice_drift", 0.0) or 0.0)
    vel   = float(micro.get("tick_velocity", 0.0) or 0.0)
    obi   = float(micro.get("obi", 0.0) or 0.0)

    # Моментум: все три признака «за»
    if drift > cfg.drift_t and vel > cfg.vel_t and obi > cfg.obi_t:
        return "BUY"
    if drift < -cfg.drift_t and vel < -cfg.vel_t and obi < -cfg.obi_t:
        return "SELL"

    # Реверсия (если индикаторы есть)
    if indi is not None:
        bb_z = indi.get("bb_z")
        rsi  = indi.get("rsi")

        try:
            bb_z_val = float(bb_z) if bb_z is not None else 0.0
        except Exception:
            bb_z_val = 0.0

        # перепроданность -> BUY
        if bb_z_val < -cfg.bb_z_t or (rsi is not None and rsi < cfg.rsi_oversold):
            return "BUY"
        # перекупленность -> SELL
        if bb_z_val > cfg.bb_z_t or (rsi is not None and rsi > cfg.rsi_overbought):
            return "SELL"

    return None
