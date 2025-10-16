# features/microstructure.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from collections import deque


@dataclass
class MicroFeatures:
    spread_ticks: float
    mid: float
    top_liq_usd: float
    microprice_drift: float
    tick_velocity: Optional[float]  # None, если нет данных для оценки
    aggressor_ratio: Optional[float]  # None без потока трейдов
    obi: float  # order-book imbalance [-1..+1]


class MicroFeatureEngine:
    """
    Микроструктура отталкивается от доступных данных: bid/ask/bid_size/ask_size/mark.
    Без потока трейдов (aggTrade) некоторые фичи даём как None.
    """

    def __init__(self, tick_size: float, lot_usd: float, vel_window: int = 20):
        self.tick_size = tick_size
        self.lot_usd = lot_usd

        # Для оценки дрейфа mid и скорости тиков
        self._mids = deque(maxlen=max(vel_window, 5))
        self._vel_window = vel_window

        # Если позже подключим trades — можно будет считать aggressor_ratio по ним
        self._aggr = None

    def update(
        self,
        *,
        price: float,     # используем как усреднённое (mid/last)
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
    ) -> MicroFeatures:
        # базовые величины
        mid = (bid + ask) / 2.0
        spread = max(ask - bid, 0.0)
        spread_ticks = spread / self.tick_size if self.tick_size > 0 else 0.0

        # «ликвидность топ-уровня» в $
        # берём суммарную $ стоимость на best bid + best ask
        top_liq_usd = bid_sz * bid + ask_sz * ask

        # microprice ~ взвешенное среднее, чем больше объём на стороне, тем ближе к ней цена
        denom = (bid_sz + ask_sz) if (bid_sz + ask_sz) > 0 else 1.0
        microprice = (ask * bid_sz + bid * ask_sz) / denom

        # дрейф microprice относительно mid в тиках
        microprice_drift = (microprice - mid) / self.tick_size if self.tick_size > 0 else 0.0

        # OBI = (bid_sz - ask_sz) / (bid_sz + ask_sz)  → [-1; +1]
        obi = (bid_sz - ask_sz) / denom

        # скорость тиков как средний абсолютный приращение mid за окно (в тиках/шаг)
        self._mids.append(mid)
        tick_velocity = None
        if len(self._mids) >= 3:
            diffs = [abs(self._mids[i] - self._mids[i - 1]) for i in range(1, len(self._mids))]
            avg = sum(diffs) / len(diffs)
            tick_velocity = (avg / self.tick_size) if self.tick_size > 0 else 0.0

        # aggressor_ratio без трейдов посчитать нельзя честно — отдаём None
        aggressor_ratio = None

        return MicroFeatures(
            spread_ticks=spread_ticks,
            mid=mid,
            top_liq_usd=top_liq_usd,
            microprice_drift=microprice_drift,
            tick_velocity=tick_velocity,
            aggressor_ratio=aggressor_ratio,
            obi=obi,
        )
