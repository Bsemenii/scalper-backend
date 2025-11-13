# risk/filters.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional

# --------- Конфиг (минимум, без зависимостей от Pydantic) ---------

@dataclass(frozen=True)
class SafetyCfg:
    max_spread_ticks: int = 3
    min_top5_liquidity_usd: float = 300_000.0
    skip_funding_minute: bool = True
    skip_minute_zero: bool = True
    min_liq_buffer_sl_mult: float = 3.0  # ликвидация должна быть >= 3× дальше, чем SL


@dataclass(frozen=True)
class RiskCfg:
    risk_per_trade_pct: float = 0.25   # проценты (0.25 => 0.25%)
    daily_stop_r: float = -10.0
    daily_target_r: float = 15.0
    max_consec_losses: int = 3
    cooldown_after_sl_s: int = 120
    min_risk_usd_floor: float = 0.25


# --------- Входной контекст для фильтров ---------

@dataclass(frozen=True)
class MicroCtx:
    """Мини-контекст микроструктуры на момент сигнала."""
    spread_ticks: float
    top5_liq_usd: float


@dataclass(frozen=True)
class TimeCtx:
    """Временной контекст в миллисекундах (UTC)."""
    ts_ms: int
    server_time_offset_ms: int = 0  # если известен смещение серверного времени


@dataclass(frozen=True)
class PositionalCtx:
    """Контекст позиции/сайзинга, чтобы оценить ликвидационный буфер относительно SL."""
    entry_px: float
    sl_px: float
    leverage: float = 15.0
    mark_px: Optional[float] = None


@dataclass(frozen=True)
class DayState:
    """Текущие дневные лимиты/результаты в R."""
    pnl_r_day: float = 0.0
    consec_losses: int = 0
    trading_disabled: bool = False  # внешняя блокировка (kill-switch)


# --------- Результаты проверки ---------

@dataclass(frozen=True)
class RiskDecision:
    allow: bool
    reasons: List[str]


# --------- Вспомогательные проверки ---------

def _is_minute_zero(ts_ms: int, window_s: int = 5) -> bool:
    """
    True, если секунда в минуте близка к 0 в окне ±window_s.
    """
    if window_s <= 0:
        window_s = 1
    sec = (ts_ms // 1000) % 60
    return (sec <= window_s) or (sec >= 60 - window_s)

def _is_funding_minute_utc(ts_ms: int, window_s: int = 60) -> bool:
    """
    Эвристика funding minute для Binance Perp: каждые 8 часов в XX:00 UTC.
    Считаем «опасным» интервал ±window_s секунд вокруг отметки.
    """
    if window_s <= 0:
        window_s = 30
    total_sec = ts_ms // 1000
    # цикл 8 часов
    minutes_in_cycle = (total_sec // 60) % (8 * 60)
    seconds_in_minute = total_sec % 60
    # близко к :00 и внутри funding-часа
    return (minutes_in_cycle == 0) and (seconds_in_minute < window_s)

def _safe_offset(ts_ms: int, offset_ms: int) -> int:
    # ограничим offset разумными пределами, чтобы «время» не улетало
    try:
        off = int(offset_ms)
    except Exception:
        off = 0
    off = max(-300_000, min(300_000, off))
    return ts_ms + off

def _liq_price(entry_px: float, side: str, leverage: float) -> float:
    """
    Очень грубая оценка уровня ликвидации: entry / (1 ± 1/leverage).
    Эвристика для paper-фильтра (реальную формулу даёт биржа).
    """
    lev = max(1.0, float(leverage))
    if str(side).upper() == "BUY":
        return entry_px * (1.0 - 1.0 / lev)
    return entry_px * (1.0 + 1.0 / lev)

def _liq_buffer_mult(entry_px: float, sl_px: float, side: str, leverage: float) -> float:
    """
    Во сколько раз ликвидация дальше от входа, чем SL (чем больше — тем безопаснее).
    """
    liq = _liq_price(entry_px, side, leverage)
    dist_liq = abs(liq - entry_px)
    dist_sl = abs(sl_px - entry_px)
    if dist_sl <= 0:
        return math.inf
    return dist_liq / dist_sl


# --------- Основные фильтры входа ---------

def check_entry_safety(
    side: str,
    micro: MicroCtx,
    time_ctx: TimeCtx,
    pos_ctx: Optional[PositionalCtx],
    safety: SafetyCfg,
) -> RiskDecision:
    """
    Быстрые «сейфти»-фильтры: спред/ликвидность/временные окна/ликвидационный буфер.
    Возвращает allow=False и список reasons, если блокировать вход.
    """
    reasons: List[str] = []

    # 1) spread / liquidity
    try:
        if float(micro.spread_ticks) > float(safety.max_spread_ticks):
            reasons.append(f"spread>{int(safety.max_spread_ticks)}")
    except Exception:
        reasons.append("spread:bad_value")

    try:
        if float(micro.top5_liq_usd) < float(safety.min_top5_liquidity_usd):
            reasons.append(f"liq<{int(safety.min_top5_liquidity_usd)}")
    except Exception:
        reasons.append("liq:bad_value")

    # 2) time gates (с аккуратным server_time_offset)
    ts = _safe_offset(int(time_ctx.ts_ms), int(time_ctx.server_time_offset_ms))
    if bool(safety.skip_minute_zero) and _is_minute_zero(ts, window_s=5):
        reasons.append("minute_zero")
    if bool(safety.skip_funding_minute) and _is_funding_minute_utc(ts, window_s=60):
        reasons.append("funding_minute")

    # 3) liquidation buffer vs SL
    if pos_ctx is not None:
        buf = _liq_buffer_mult(pos_ctx.entry_px, pos_ctx.sl_px, side, pos_ctx.leverage)
        if buf < float(safety.min_liq_buffer_sl_mult):
            reasons.append(f"liq_buffer<{float(safety.min_liq_buffer_sl_mult):g}")

    return RiskDecision(allow=(len(reasons) == 0), reasons=reasons)


# --------- Дневные лимиты и дисциплина ---------

def check_day_limits(
    state: DayState,
    risk: RiskCfg,
) -> RiskDecision:
    """
    Глобальные дневные ограничения: daily stop / daily target / серия лоссов / kill-switch.
    """
    reasons: List[str] = []
    if state.trading_disabled:
        reasons.append("disabled")
    if state.pnl_r_day <= risk.daily_stop_r:
        reasons.append("daily_stop")
    if state.pnl_r_day >= risk.daily_target_r:
        reasons.append("daily_target")
    if state.consec_losses >= risk.max_consec_losses:
        reasons.append("cooldown_required")
    return RiskDecision(allow=(len(reasons) == 0), reasons=reasons)


# --------- Утилита для нормализации R ---------

def normalize_r(pnl_usd: float, entry_px: float, sl_px: float, qty: float, risk: RiskCfg) -> float:
    """
    Пересчёт PnL в R: делим на фактический риск (|entry-sl|*qty), но не ниже пола min_risk_usd_floor.
    """
    try:
        risk_usd = abs(float(entry_px) - float(sl_px)) * max(float(qty), 0.0)
    except Exception:
        risk_usd = 0.0
    risk_usd = max(risk_usd, max(1e-9, float(risk.min_risk_usd_floor)))
    return float(pnl_usd) / risk_usd
