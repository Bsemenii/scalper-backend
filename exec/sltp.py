from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional, Mapping

Side = Literal["BUY", "SELL"]


@dataclass
class SLTPPlan:
    sl_px: float
    tp_px: float
    sl_qty: float
    tp_qty: float


def _round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return round(round(x / step) * step, 10)


def _is_long(side: Side) -> bool:
    return side == "BUY"


def compute_sltp(
    *,
    side: Side,
    entry_px: float,
    qty: float,
    price_tick: float,
    sl_distance_px: float,
    rr: float = 1.8,
) -> SLTPPlan:
    """
    Базовый расчёт SL/TP:
      SL = entry ± sl_distance_px
      TP = entry ∓ rr * sl_distance_px
    """
    assert entry_px > 0 and qty > 0 and sl_distance_px > 0

    if _is_long(side):
        sl = max(0.0, entry_px - sl_distance_px)
        tp = entry_px + rr * sl_distance_px
    else:
        sl = entry_px + sl_distance_px
        tp = entry_px - rr * sl_distance_px

    sl = _round_step(sl, price_tick)
    tp = _round_step(tp, price_tick)

    return SLTPPlan(
        sl_px=sl,
        tp_px=tp,
        sl_qty=qty,
        tp_qty=qty,
    )


# ---------- АДАПТИВНЫЙ ПЛАНЕР ----------

@dataclass
class AdaptiveSLTPCfg:
    # RR
    rr_momentum_base: float = 2.0
    rr_momentum_max: float = 2.6
    rr_reversion: float = 1.6

    # SL по расстоянию
    min_sl_ticks: float = 4.0
    max_sl_ticks: float = 80.0

    # SL в bp от цены (1 bp = 0.01%)
    min_sl_bps: float = 0.5      # не ставим стоп меньше 0.5 bp
    max_sl_bps: float = 8.0      # не раздуваем больше 8 bp

    # влияние спреда и волатильности
    spread_k: float = 1.5        # SL ≥ spread_k * спред
    vol_k: float = 0.7           # SL ≥ vol_k * σ (по bp)

    # усиление для сильных сигналов
    strong_score: float = 1.5
    strong_boost: float = 0.25   # добавка к RR за (score - strong_score)


def compute_sltp_adaptive(
    *,
    side: Side,
    entry_px: float,
    qty: float,
    price_tick: float,
    micro: Optional[Mapping[str, float]] = None,   # spread_ticks, ...
    indi: Optional[Mapping[str, float]] = None,    # realized_vol_bp, ...
    decision: Optional[Mapping[str, float]] = None,  # kind, score
    cfg: AdaptiveSLTPCfg = AdaptiveSLTPCfg(),
) -> SLTPPlan:
    """
    Адаптивный SL/TP:

    - SL учитывает:
        * спред в тиках,
        * локальную волатильность (realized_vol_bp),
        * минимальный размер в тиках и bp.
    - RR зависит от типа сигнала (momentum/reversion) и его score.
    """
    assert entry_px > 0 and qty > 0 and price_tick > 0

    m = dict(micro or {})
    ind = dict(indi or {})

    # --- спред ---
    try:
        spread_ticks = float(m.get("spread_ticks", 1.0) or 1.0)
    except Exception:
        spread_ticks = 1.0
    spread_ticks = max(spread_ticks, 1.0)

    # --- волатильность в bp ---
    try:
        vol_bp = float(ind.get("realized_vol_bp", 0.0) or 0.0)
    except Exception:
        vol_bp = 0.0

    # не даём воле быть ниже min_sl_bps, чтобы не получить микроскопический SL
    vol_bp = max(vol_bp, cfg.min_sl_bps)
    vol_px = entry_px * (vol_bp / 10_000.0)

    # --- компоненты SL ---
    sl_from_spread_px = spread_ticks * price_tick * cfg.spread_k
    sl_from_vol_px = vol_px * cfg.vol_k
    sl_min_ticks_px = cfg.min_sl_ticks * price_tick
    sl_min_bps_px = entry_px * (cfg.min_sl_bps / 10_000.0)

    sl_px = max(sl_from_spread_px, sl_from_vol_px, sl_min_ticks_px, sl_min_bps_px)

    # ограничение сверху
    sl_max_ticks_px = cfg.max_sl_ticks * price_tick
    sl_max_bps_px = entry_px * (cfg.max_sl_bps / 10_000.0)
    sl_px = min(sl_px, sl_max_ticks_px, sl_max_bps_px)

    if sl_px <= 0:
        sl_px = sl_min_ticks_px if sl_min_ticks_px > 0 else price_tick

    # --- RR по типу сигнала ---
    kind = None
    score = 1.0
    if decision:
        kind = decision.get("kind")
        try:
            score = float(decision.get("score", 1.0) or 1.0)
        except Exception:
            score = 1.0

    if kind == "reversion":
        rr = cfg.rr_reversion
    else:
        # default + буст для сильного momentum
        rr = cfg.rr_momentum_base
        if score >= cfg.strong_score:
            rr = min(cfg.rr_momentum_max, rr + cfg.strong_boost * (score - cfg.strong_score))

    return compute_sltp(
        side=side,
        entry_px=entry_px,
        qty=qty,
        price_tick=price_tick,
        sl_distance_px=sl_px,
        rr=rr,
    )
