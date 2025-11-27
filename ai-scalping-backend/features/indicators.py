# features/indicators.py
from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional
import math


@dataclass
class Indi:
    ema9: float
    ema21: float
    ema_slope_ticks: float
    vwap_drift: float
    bb_z: float
    rsi: float
    realized_vol_bp: float  # волатильность в базисных пунктах за окно (стд отклонение лог-доходностей)


class IndiEngine:
    """
    Лёгкий набор индикаторов на последнем mid/price.
    Важно: считаем волатильность корректно как std(log-returns) и возвращаем в bp.
    
    TIMEFRAME: Reversion strategy for GALAUSDT uses 1-minute candles.
    The default bb_win=60 represents approximately 1 minute of tick data for active symbols
    (~60 ticks per minute). All indicators (bb_z, ema21, rsi, realized_vol_bp) are computed
    from 1-minute timeframe data.
    """

    def __init__(self, price_step: float, ema_fast=9, ema_slow=21, bb_win=60, rsi_win=14):
        self.step = price_step

        # серии для индикаторов
        self._prices = deque(maxlen=max(bb_win, rsi_win, 120))
        self._vol_win = 60  # окно для волы (примерно ~60 тиков)
        self._returns = deque(maxlen=600)

        # EMA
        self._ema9 = None
        self._ema21 = None
        self._k9 = 2 / (ema_fast + 1)
        self._k21 = 2 / (ema_slow + 1)

        # RSI
        self._rsi_win = rsi_win
        self._gains = deque(maxlen=rsi_win)
        self._losses = deque(maxlen=rsi_win)

        # VWAP за короткое окно (простой вариант без объёма — среднее цен)
        self._vwap_win = 60
        self._vwap_buf = deque(maxlen=self._vwap_win)

        # Bollinger Bands (через mean/std)
        self._bb_win = bb_win

    def _std(self, arr):
        n = len(arr)
        if n < 2:
            return 0.0
        m = sum(arr) / n
        var = sum((x - m) ** 2 for x in arr) / (n - 1)
        return math.sqrt(var)

    def update(self, *, price: float) -> Indi:
        # --- цены ---
        self._prices.append(price)
        self._vwap_buf.append(price)

        # --- EMA9/21 ---
        if self._ema9 is None:
            self._ema9 = price
        else:
            self._ema9 = self._ema9 + self._k9 * (price - self._ema9)

        if self._ema21 is None:
            self._ema21 = price
        else:
            self._ema21 = self._ema21 + self._k21 * (price - self._ema21)

        # slope в тиках: разница EMA21(t) - EMA21(t-1) / шаг
        ema_slope_ticks = 0.0
        # грубая оценка: разница текущей EMA21 и цены как прокси тренда
        if self._ema21 is not None and self.step > 0:
            ema_slope_ticks = (self._ema9 - self._ema21) / self.step

        # --- VWAP (упрощённо средняя цена окна) ---
        vwap = sum(self._vwap_buf) / len(self._vwap_buf) if self._vwap_buf else price
        vwap_drift = price - vwap

        # --- Bollinger Z ---
        bb_z = 0.0
        if len(self._prices) >= self._bb_win:
            mean = sum(self._prices) / len(self._prices)
            std = self._std(self._prices)
            if std > 0:
                bb_z = (price - mean) / std

        # --- RSI ---
        rsi = 50.0
        if len(self._prices) >= 2:
            chg = self._prices[-1] - self._prices[-2]
            self._gains.append(max(chg, 0.0))
            self._losses.append(abs(min(chg, 0.0)))
            if len(self._gains) == self._rsi_win and len(self._losses) == self._rsi_win:
                avg_gain = sum(self._gains) / self._rsi_win
                avg_loss = sum(self._losses) / self._rsi_win
                if avg_loss == 0:
                    rsi = 100.0
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))

        # --- Волатильность: std лог-доходностей в bp ---
        if len(self._prices) >= 2:
            p0, p1 = self._prices[-2], self._prices[-1]
            if p0 > 0 and p1 > 0:
                r = math.log(p1 / p0)
                self._returns.append(r)

        realized_vol_bp = 0.0
        if len(self._returns) >= 5:
            # std на последних self._vol_win наблюдениях
            window = list(self._returns)[-self._vol_win:]
            std_r = self._std(window)
            realized_vol_bp = std_r * 10_000  # в bp

        return Indi(
            ema9=self._ema9,
            ema21=self._ema21,
            ema_slope_ticks=ema_slope_ticks,
            vwap_drift=vwap_drift,
            bb_z=bb_z,
            rsi=rsi,
            realized_vol_bp=realized_vol_bp,
        )
