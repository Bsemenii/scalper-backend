# exec/sltp.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

Side = Literal["BUY", "SELL"]

# =========================
# Public data structures
# =========================

@dataclass
class SLTPPlan:
    """
    Единый план защиты и профита:
    - sl_px / tp_px — ценовые уровни Stop Loss и Take Profit;
    - sl_qty / tp_qty — объёмы под срабатывание (по умолчанию — весь объём).
    """
    sl_px: float
    tp_px: float
    sl_qty: float
    tp_qty: float


# =========================
# Internal helpers
# =========================

def _is_long(side: Side) -> bool:
    # side приходит как "BUY"/"SELL" (bot.core.types.Side), но на всякий:
    return str(side).upper() == "BUY"


def _q_down(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    import math
    return float(math.floor(x / step) * step)


def _q_up(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    import math
    # небольшой эпсилон, чтобы исключить залипание на границе
    return float(math.ceil((x - 1e-15) / step) * step)


def _q_near(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    import math
    return float(round(x / step) * step)


def _clamp_pos(x: float) -> float:
    return float(x) if x > 0 else 0.0


def _bp(v: float) -> float:
    # безопасно перевести bps в коэффициент
    try:
        return float(v) / 10_000.0
    except Exception:
        return 0.0


def _ensure_min_sl_distance_px(
    *,
    raw_sl_dist_px: float,
    price_tick: float,
    min_stop_ticks: int = 8,
    spread_px: float = 0.0,
    spread_mult: float = 1.5,
    min_sl_bps: float = 12.0,
    ref_price: float = 0.0,
) -> float:
    """
    Жёсткие «ограждения» для SL-дистанции:
    - не меньше min_stop_ticks * tick,
    - не меньше spread_px * spread_mult,
    - не меньше min_sl_bps (в bps) от референсной цены (entry/мид).
    Дистанция квантуется ВВЕРХ по тик-шагу, чтобы не оказаться слишком близко.
    """
    tick = float(price_tick if price_tick > 0 else 0.1)
    floor_ticks = max(int(min_stop_ticks or 0), 1) * tick
    floor_spread = float(spread_px) * float(spread_mult) if spread_px > 0 else 0.0
    floor_bps = float(ref_price) * _bp(min_sl_bps) if (ref_price > 0 and min_sl_bps > 0) else 0.0

    dist = max(float(raw_sl_dist_px), floor_ticks, floor_spread, floor_bps, tick)

    import math
    dist = math.ceil(dist / tick) * tick
    return float(dist)


# ===========================================================
# Fee-aware целеуказание для TP с решением в замкнутой форме
# ===========================================================

def _solve_tp_price_for_min_net_rr_long(
    *,
    entry_px: float,
    sl_px: float,
    min_net_rr: float,
    exit_fee_bps_eff: float,
    entry_fee_bps: float,
) -> float:
    """
    Для лонга решаем:
      (tp - E) - (E*m + tp*n) >= R*(E - S)
    где: E=entry, S=sl, m=entry_fee_bps/1e4, n=exit_fee_bps_eff/1e4, R=min_net_rr.
    Решение:
      tp >= [ R*(E - S) + E*(1 + m) ] / (1 - n)
    """
    E, S, R = float(entry_px), float(sl_px), float(min_net_rr)
    m, n = _bp(entry_fee_bps), _bp(exit_fee_bps_eff)
    denom = (1.0 - n)
    if denom <= 1e-12:  # патологический случай, ставим далеко
        denom = 1e-12
    num = R * (E - S) + E * (1.0 + m)
    return float(num / denom)


def _solve_tp_price_for_min_net_rr_short(
    *,
    entry_px: float,
    sl_px: float,
    min_net_rr: float,
    exit_fee_bps_eff: float,
    entry_fee_bps: float,
) -> float:
    """
    Для шорта решаем:
      (E - tp) - (E*m + tp*n) >= R*(S - E)
    =>
      tp <= [ E*(1 - m) - R*(S - E) ] / (1 + n)
    """
    E, S, R = float(entry_px), float(sl_px), float(min_net_rr)
    m, n = _bp(entry_fee_bps), _bp(exit_fee_bps_eff)
    denom = (1.0 + n)
    if denom <= 1e-12:
        denom = 1.0
    num = E * (1.0 - m) - R * (S - E)
    return float(num / denom)


# ===========================================================
# Public API — Fee-aware планировщик SL/TP
# ===========================================================

def compute_sltp_fee_aware(
    *,
    side: Side,
    entry_px: float,
    qty: float,
    price_tick: float,
    # --- базовая геометрия SL/TP ---
    sl_distance_px: float,
    rr_target: float = 1.6,
    # --- рынок и комиссии/скольжение/спред ---
    maker_bps: float = 2.0,
    taker_bps: float = 4.0,
    entry_taker_like: bool = True,
    exit_taker_like: bool = True,
    addl_exit_bps: float = 0.0,  # половинка спреда/ожидаемое проскальзывание в bps
    # --- «ограждения» для SL ---
    min_stop_ticks: int = 10,
    spread_px: float = 0.0,
    spread_mult: float = 1.5,
    min_sl_bps: float = 12.0,
    # --- требования к чистому R ---
    min_net_rr: float = 1.2,
    allow_expand_tp: bool = True,
    # --- опционально заданный SL ---
    sl_px_hint: Optional[float] = None,
) -> SLTPPlan:
    """
    Строит SL/TP так, чтобы:
      1) SL имел адекватную дистанцию (тики/спред/минимум в bps),
      2) TP удовлетворял *чистому* R (после комиссий/надбавок) не ниже `min_net_rr`,
      3) не был хуже целевого `rr_target` (gross), если это возможно.

    Возвращает SLTPPlan (полный объём на SL/TP).
    """
    if not (entry_px > 0 and qty > 0 and sl_distance_px > 0 and price_tick > 0):
        raise ValueError("invalid inputs for compute_sltp_fee_aware")

    # 1) нормализуем SL дистанцию с «ограждениями»
    sl_dist_px = _ensure_min_sl_distance_px(
        raw_sl_dist_px=float(sl_distance_px),
        price_tick=float(price_tick),
        min_stop_ticks=int(min_stop_ticks),
        spread_px=float(spread_px),
        spread_mult=float(spread_mult),
        min_sl_bps=float(min_sl_bps),
        ref_price=float(entry_px),
    )

    # 2) SL из entry и дистанции, направленное округление
    if _is_long(side):
        sl_px = _q_down(max(0.0, entry_px - sl_dist_px), price_tick)
    else:
        sl_px = _q_up(entry_px + sl_dist_px, price_tick)

    # внешняя подсказка SL, но не ближе минимума
    if isinstance(sl_px_hint, (int, float)) and sl_px_hint > 0:
        if _is_long(side):
            min_ok = _q_down(max(0.0, entry_px - sl_dist_px), price_tick)
            sl_px = max(min_ok, min(sl_px_hint, entry_px))
            sl_px = _q_down(sl_px, price_tick)
        else:
            min_ok = _q_up(entry_px + sl_dist_px, price_tick)
            sl_px = max(min_ok, max(sl_px_hint, entry_px))
            sl_px = _q_up(sl_px, price_tick)

    # 3) базовый TP по gross RR
    rr_target = max(float(rr_target or 0.0), 1.2)
    if _is_long(side):
        base_tp = entry_px + rr_target * (entry_px - sl_px)
        tp_gross = _q_up(base_tp, price_tick)
    else:
        base_tp = entry_px - rr_target * (sl_px - entry_px)
        tp_gross = _q_down(base_tp, price_tick)

    # 4) комиссии и надбавки
    entry_bps = float(taker_bps if entry_taker_like else maker_bps)
    exit_bps_eff = float(taker_bps if exit_taker_like else maker_bps) + float(addl_exit_bps)

    # 5) минимальный TP под min_net_rr
    min_net_rr = max(float(min_net_rr or 0.0), 1.0)

    if _is_long(side):
        tp_min_net = _solve_tp_price_for_min_net_rr_long(
            entry_px=entry_px,
            sl_px=sl_px,
            min_net_rr=min_net_rr,
            exit_fee_bps_eff=exit_bps_eff,
            entry_fee_bps=entry_bps,
        )
        tp_min_net = _q_up(tp_min_net, price_tick)
        tp_px = max(tp_gross, tp_min_net) if allow_expand_tp else max(
            tp_gross,
            _q_up(entry_px, price_tick),
        )
        # safety: TP ДОЛЖЕН быть выше entry
        tp_px = max(tp_px, _q_up(entry_px + price_tick, price_tick))
    else:
        tp_min_net = _solve_tp_price_for_min_net_rr_short(
            entry_px=entry_px,
            sl_px=sl_px,
            min_net_rr=min_net_rr,
            exit_fee_bps_eff=exit_bps_eff,
            entry_fee_bps=entry_bps,
        )
        tp_min_net = _q_down(tp_min_net, price_tick)
        tp_px = min(tp_gross, tp_min_net) if allow_expand_tp else min(
            tp_gross,
            _q_down(entry_px, price_tick),
        )
        # safety: TP ДОЛЖЕН быть ниже entry
        tp_px = min(tp_px, _q_down(entry_px - price_tick, price_tick))

    return SLTPPlan(
        sl_px=float(_clamp_pos(sl_px)),
        tp_px=float(_clamp_pos(tp_px)),
        sl_qty=float(qty),
        tp_qty=float(qty),
    )


# ===========================================================
# Backward-compatible simple planner (legacy signature)
# ===========================================================

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
    Простой планировщик (обратная совместимость).

    Улучшения:
    - SL-дистанция приводится к «разумному минимуму» (тики, 12 bps).
    - Направленные округления: SL long — вниз, TP long — вверх (и наоборот для short).
    - TP целится в rr (gross) и не «липнет» к entry.

    Для учёта комиссий и чистого R используй compute_sltp_fee_aware().
    """
    if not (entry_px > 0 and qty > 0 and sl_distance_px > 0):
        raise ValueError("invalid inputs for compute_sltp")

    tick = float(price_tick if price_tick > 0 else 0.1)
    sl_dist = _ensure_min_sl_distance_px(
        raw_sl_dist_px=float(sl_distance_px),
        price_tick=tick,
        min_stop_ticks=10,
        spread_px=0.0,
        spread_mult=1.5,
        min_sl_bps=12.0,
        ref_price=float(entry_px),
    )

    rr = max(float(rr or 0.0), 1.2)
    if _is_long(side):
        sl = _q_down(max(0.0, entry_px - sl_dist), tick)
        tp = _q_up(entry_px + rr * (entry_px - sl), tick)
        tp = max(tp, _q_up(entry_px + tick, tick))
    else:
        sl = _q_up(entry_px + sl_dist, tick)
        tp = _q_down(entry_px - rr * (sl - entry_px), tick)
        tp = min(tp, _q_down(entry_px - tick, tick))

    return SLTPPlan(
        sl_px=float(sl),
        tp_px=float(tp),
        sl_qty=float(qty),
        tp_qty=float(qty),
    )


# ===========================================================
# Convenience: перезадание TP под требуемый net-R (если SL задан)
# ===========================================================

def retarget_tp_for_min_net_rr(
    *,
    side: Side,
    entry_px: float,
    sl_px: float,
    price_tick: float,
    min_net_rr: float,
    maker_bps: float = 2.0,
    taker_bps: float = 4.0,
    entry_taker_like: bool = True,
    exit_taker_like: bool = True,
    addl_exit_bps: float = 0.0,
) -> float:
    """
    Быстрый пересчёт TP под заданный минимальный «чистый» R (без пересчёта SL).
    Возвращает скорректированный tp_px (с правильным направленным округлением).
    """
    entry_bps = float(taker_bps if entry_taker_like else maker_bps)
    exit_bps_eff = float(taker_bps if exit_taker_like else maker_bps) + float(addl_exit_bps)

    if _is_long(side):
        tp = _solve_tp_price_for_min_net_rr_long(
            entry_px=float(entry_px),
            sl_px=float(sl_px),
            min_net_rr=float(min_net_rr),
            exit_fee_bps_eff=exit_bps_eff,
            entry_fee_bps=entry_bps,
        )
        return _q_up(tp, float(price_tick))
    else:
        tp = _solve_tp_price_for_min_net_rr_short(
            entry_px=float(entry_px),
            sl_px=float(sl_px),
            min_net_rr=float(min_net_rr),
            exit_fee_bps_eff=exit_bps_eff,
            entry_fee_bps=entry_bps,
        )
        return _q_down(tp, float(price_tick))
