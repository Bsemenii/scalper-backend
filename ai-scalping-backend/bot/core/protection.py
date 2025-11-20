# bot/core/protection.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal

Side = Literal["BUY", "SELL"]

@dataclass
class ProtectionCfg:
    tp_r: float = 1.6          # тейк в R
    be_after_r: float = 1.0    # перенос SL в BE при +1R
    trail_after_r: float = 1.2 # начать трейлить с этого R
    trail_k_sigma: float = 1.0 # шаг трейла как k * sigma (если доступна оценка)
    min_tick: float = 0.1      # шаг цены
    lock_in_r: float = 0.2     # когда трейлим, фиксируем хотя бы 0.2R

@dataclass
class PositionSnap:
    side: Side
    entry_px: float
    sl_px: Optional[float]
    tp_px: Optional[float]
    qty: float
    # вспомогательные для планировщика
    last_sigma_px: Optional[float] = None   # оценка σ (px) из индикаторов/волатильности
    r_value_px: Optional[float] = None      # 1R в px ( = |entry - sl_at_entry| )

def _round_to_step(px: float, step: float) -> float:
    if step <= 0:
        return px
    return round(px / step) * step

def compute_r_value_px(pos: PositionSnap) -> Optional[float]:
    # 1R — это расстояние от entry до исходного SL
    if pos.sl_px is None:
        return None
    return abs(pos.entry_px - pos.sl_px)

def plan_next_protection(
    pos: PositionSnap,
    mid_px: float,
    cfg: ProtectionCfg,
) -> dict:
    """
    Возвращает план обновлений: { "sl_px": float|None, "tp_px": float|None, "actions":[...], "r_now": float|None }
    Ничего не делает, если не может посчитать R.
    """
    actions: list[str] = []
    r_px = pos.r_value_px or compute_r_value_px(pos)
    if not r_px or r_px <= 0:
        return {"sl_px": None, "tp_px": None, "actions": actions, "r_now": None}

    # текущая прибыль в R
    if pos.side == "BUY":
        r_now = (mid_px - pos.entry_px) / r_px
        be_px = pos.entry_px
        tp_target = pos.entry_px + cfg.tp_r * r_px
        # базовый SL — не ниже текущего
        current_sl = pos.sl_px if pos.sl_px is not None else pos.entry_px - r_px
        new_sl = current_sl
        # BE
        if r_now >= cfg.be_after_r and (pos.sl_px or 0.0) < be_px:
            new_sl = max(new_sl, be_px)
            actions.append(f"sl_to_BE@{_round_to_step(be_px, cfg.min_tick)}")
        # трейл после порога
        if r_now >= cfg.trail_after_r:
            # шаг трейла — либо sigma*k, либо r_px * lock_in_r (минимум)
            trail_step = (pos.last_sigma_px or r_px) * cfg.trail_k_sigma
            lock_in = cfg.lock_in_r * r_px
            trail_to = mid_px - max(trail_step, lock_in)
            if trail_to > new_sl:
                new_sl = trail_to
                actions.append(f"sl_trail@{_round_to_step(trail_to, cfg.min_tick)}")
        # tp — фиксированный по R
        new_tp = tp_target
    else:
        r_now = (pos.entry_px - mid_px) / r_px
        be_px = pos.entry_px
        tp_target = pos.entry_px - cfg.tp_r * r_px
        current_sl = pos.sl_px if pos.sl_px is not None else pos.entry_px + r_px
        new_sl = current_sl
        if r_now >= cfg.be_after_r and (pos.sl_px or 1e18) > be_px:
            new_sl = min(new_sl, be_px)
            actions.append(f"sl_to_BE@{_round_to_step(be_px, cfg.min_tick)}")
        if r_now >= cfg.trail_after_r:
            trail_step = (pos.last_sigma_px or r_px) * cfg.trail_k_sigma
            lock_in = cfg.lock_in_r * r_px
            trail_to = mid_px + max(trail_step, lock_in)
            if trail_to < new_sl:
                new_sl = trail_to
                actions.append(f"sl_trail@{_round_to_step(trail_to, cfg.min_tick)}")
        new_tp = tp_target

    # квантуем по шагу
    new_sl_q = _round_to_step(new_sl, cfg.min_tick)
    new_tp_q = _round_to_step(new_tp, cfg.min_tick)

    # не возвращаем те же самые уровни
    out = {"sl_px": None, "tp_px": None, "actions": actions, "r_now": r_now}
    if pos.sl_px is None or abs(new_sl_q - pos.sl_px) >= cfg.min_tick:
        out["sl_px"] = new_sl_q
    if pos.tp_px is None or abs(new_tp_q - pos.tp_px) >= cfg.min_tick:
        out["tp_px"] = new_tp_q
    return out
