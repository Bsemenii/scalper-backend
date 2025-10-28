# strategy/candidates.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

from features.microstructure import MicroFeatureEngine
from features.indicators import IndiEngine


@dataclass
class Decision:
    side: Optional[str]   # "BUY" | "SELL" | None
    confidence: float     # 0..1
    reason: str


class CandidateEngine:
    """
    Лёгкий, быстрый генератор сигналов по последнему тіку.
    Правила (минимальный, но боевой набор):
      - спред <= 3 тика
      - достаточная верхняя ликвидность (минимум ~$300k)
      - Momentum: OBI > +t и EMA-слайп вверх → BUY; OBI < -t и EMA-вниз → SELL
      - Reversion: BB_Z < -z → отскок BUY; BB_Z > +z → отскок SELL
    Возвращает один кандидат с причиной и уверенностью.
    """

    def __init__(
        self,
        price_tick: float,
        top_liq_threshold_usd: float = 300_000.0,
        obi_t: float = 0.12,
        bb_z_abs: float = 2.0,
    ) -> None:
        self.price_tick = max(price_tick, 1e-9)
        self.top_liq_threshold_usd = float(top_liq_threshold_usd)
        self.obi_t = float(obi_t)
        self.bb_z_abs = float(bb_z_abs)

        # пер-символьные движки
        self._micro = MicroFeatureEngine(tick_size=self.price_tick, lot_usd=10_000.0)
        self._indi = IndiEngine(price_step=self.price_tick)

        # последняя диагностика
        self._last: Dict[str, float] = {}

    def update(
        self,
        *,
        price: float,
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
    ) -> Decision:
        now = int(time.time() * 1000)

        # обновляем микроструктуру и индикаторы
        m = self._micro.update(price=price, bid=bid, ask=ask, bid_sz=bid_sz, ask_sz=ask_sz)
        i = self._indi.update(price=price)

        # быстрые отсеки (спред и верхняя ликвидность)
        if m.spread_ticks is not None and m.spread_ticks > 3:
            return Decision(None, 0.0, f"skip:spread>{m.spread_ticks:.1f}t")

        if m.top_liq_usd is not None and m.top_liq_usd < self.top_liq_threshold_usd:
            return Decision(None, 0.0, f"skip:topliq<{int(m.top_liq_usd)}")

        # Momentum
        if m.obi is not None and i.ema_slope_ticks is not None:
            if m.obi >= +self.obi_t and i.ema_slope_ticks > 0:
                conf = min(1.0, 0.5 + 0.5 * min(1.0, (m.obi - self.obi_t) / self.obi_t))
                return Decision("BUY", conf, f"mom:obi={m.obi:.2f},ema_slope={i.ema_slope_ticks:.1f}t")
            if m.obi <= -self.obi_t and i.ema_slope_ticks < 0:
                conf = min(1.0, 0.5 + 0.5 * min(1.0, (-m.obi - self.obi_t) / self.obi_t))
                return Decision("SELL", conf, f"mom:obi={m.obi:.2f},ema_slope={i.ema_slope_ticks:.1f}t")

        # Reversion
        if i.bb_z is not None:
            if i.bb_z <= -self.bb_z_abs:
                conf = min(1.0, (self.bb_z_abs - max(-self.bb_z_abs, i.bb_z)) / self.bb_z_abs)
                return Decision("BUY", conf, f"rev:bb_z={i.bb_z:.2f}")
            if i.bb_z >= +self.bb_z_abs:
                conf = min(1.0, (i.bb_z - self.bb_z_abs) / self.bb_z_abs)
                return Decision("SELL", conf, f"rev:bb_z={i.bb_z:.2f}")

        return Decision(None, 0.0, "skip:no_rule")
