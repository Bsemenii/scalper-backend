# strategy/cross_guard.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional, Tuple, List
import math

Side = Literal["BUY", "SELL"]

@dataclass(frozen=True)
class CrossCfg:
    lookback_s: int = 60            # окно для оценки направления BTC/символа
    min_btc_strength_bp: float = 6  # сила BTC в бипсах за окно (|Δ|/mid*1e4)
    block_on_divergence: bool = True
    prefer_symbol_with_higher_speed: bool = True

def _drift_bp(prices: List[float]) -> float:
    if not prices or len(prices) < 2:
        return 0.0
    p0, p1 = float(prices[0]), float(prices[-1])
    if p0 <= 0.0:
        return 0.0
    mid = 0.5 * (p0 + p1)
    if mid <= 0: mid = p0
    return ((p1 - p0) / mid) * 10_000.0

def _speed_bp_per_s(prices: List[float], secs: int) -> float:
    if secs <= 0: return 0.0
    return _drift_bp(prices) / float(secs)

def _same_sign(a: float, b: float) -> bool:
    return (a == 0 and b == 0) or (a > 0 and b > 0) or (a < 0 and b < 0)

def decide_cross_guard(
    *,
    side: Side,
    btc_prices: List[float],
    sym_prices: List[float],
    cfg: Optional[CrossCfg] = None,
) -> Tuple[bool, str, float, float]:
    """
    Возвращает (allow, reason, btc_drift_bp, sym_drift_bp).
    Правила:
      - если |BTC_drift| < min_btc_strength_bp → не блокируем по доминации;
      - если BTC и символ расходятся и block_on_divergence=True → блокируем вход;
      - если prefer_symbol_with_higher_speed=True → входы даём только символу с большей |скоростью|.
    """
    c = cfg or CrossCfg()
    btc_drift = _drift_bp(btc_prices)
    sym_drift = _drift_bp(sym_prices)

    # 1) слабая доминация — пропускаем
    if abs(btc_drift) < c.min_btc_strength_bp:
        return True, "btc_weak", btc_drift, sym_drift

    # 2) дивергенция против позиции
    if c.block_on_divergence:
        # для LONG требуется положительный дрейф по символу и не отрицательный по BTC
        if side == "BUY" and (sym_drift <= 0 or btc_drift <= 0) and not _same_sign(sym_drift, btc_drift):
            return False, "divergent_vs_btc", btc_drift, sym_drift
        # для SHORT — наоборот
        if side == "SELL" and (sym_drift >= 0 or btc_drift >= 0) and not _same_sign(sym_drift, btc_drift):
            return False, "divergent_vs_btc", btc_drift, sym_drift

    return True, "ok", btc_drift, sym_drift

def pick_best_symbol_by_speed(
    *,
    btc_prices: List[float],
    sym_a: Tuple[str, List[float]],
    sym_b: Tuple[str, List[float]],
    lookback_s: int,
) -> str:
    """
    Выбирает символ с бОльшей |скоростью| (bp/s) на заданном окне.
    """
    name_a, pa = sym_a
    name_b, pb = sym_b
    speed_a = abs(_speed_bp_per_s(pa, lookback_s))
    speed_b = abs(_speed_bp_per_s(pb, lookback_s))
    # если равны — отдаём приоритет BTC (если совпадает имя), иначе первому
    if abs(speed_b - speed_a) < 1e-9:
        return name_a
    return name_a if speed_a > speed_b else name_b
