from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal, Deque, Dict, Any
from collections import deque

Side = Literal["BUY", "SELL"]


@dataclass
class Decision:
    """
    Результат работы CandidateEngine.

    side:
      - "BUY"/"SELL" → есть сигнал
      - None         → нет входа
    kind:
      - "momentum" / "reversion" / "none"
    score:
      - относительная сила сигнала (>=1.0 → допускаем)
    reason:
      - текстовое объяснение (уходит в block_reasons)
    """
    side: Optional[Side]
    kind: str = "none"
    score: float = 0.0
    reason: str = "no_signal"


class CandidateEngine:
    """
    Лёгкий, но адекватный генератор сигналов.

    Идея:
      - Используем mid-price + spread + OBI.
      - Умеем смотреть в сторону микрофич (micro) и индикаторов (indi), если их прокинул Worker.
      - Два типа сигналов:
          1) momentum: импульс + подтверждение объёмом/OBI
          2) reversion: откат от перекупленности/перепроданности + контр-OBI
      - Жёсткий фильтр:
          - warmup по истории
          - ограничение по спреду
          - OBI в сторону входа
          - score < 1.0 → нет входа
    """

    def __init__(
        self,
        *,
        price_tick: float,
        max_history: int = 256,

        # momentum
        mom_lookback: int = 6,
        mom_thr_ticks: float = 5.0,
        obi_mom_thr: float = 0.35,

        # reversion
        rev_window: int = 48,
        rev_thr_ticks: float = 8.0,
        obi_rev_thr: float = 0.30,

        # spread / качество
        spread_max_ticks: float = 6.0,
    ) -> None:
        self.price_tick = max(float(price_tick), 1e-9)

        self.max_history = int(max_history)

        self.mom_lookback = int(mom_lookback)
        self.mom_thr_ticks = float(mom_thr_ticks)
        self.obi_mom_thr = float(obi_mom_thr)

        self.rev_window = int(rev_window)
        self.rev_thr_ticks = float(rev_thr_ticks)
        self.obi_rev_thr = float(obi_rev_thr)

        self.spread_max_ticks = float(spread_max_ticks)

        self._mid: Deque[float] = deque(maxlen=self.max_history)
        self._bid_sz: Deque[float] = deque(maxlen=self.max_history)
        self._ask_sz: Deque[float] = deque(maxlen=self.max_history)

    # ---- low-level helpers ----

    def _spread_ticks(self, bid: float, ask: float) -> float:
        if bid > 0.0 and ask > 0.0:
            return max(0.0, (ask - bid) / self.price_tick)
        return 0.0

    @staticmethod
    def _obi_from_sizes(bid_sz: float, ask_sz: float) -> float:
        b = max(bid_sz, 0.0)
        a = max(ask_sz, 0.0)
        s = b + a
        if s <= 0.0:
            return 0.0
        v = (b - a) / s
        return max(-1.0, min(1.0, v))

    # ---- core ----

    def update(
        self,
        *,
        price: float,
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
        micro: Optional[Dict[str, Any]] = None,
        indi: Optional[Dict[str, Any]] = None,
    ) -> Decision:
        """
        micro: dict от MicroFeatureEngine (spread_ticks, mid, microprice_drift, tick_velocity, obi, ...)
        indi:  dict от IndiEngine (bb_z, rsi, vwap_drift, ...)
        """

        # 1) mid price
        mid = 0.0
        if bid > 0.0 and ask > 0.0:
            mid = 0.5 * (bid + ask)
        elif price > 0.0:
            mid = float(price)

        if mid <= 0.0:
            return Decision(side=None, kind="none", score=0.0, reason="no_price")

        # 2) обновляем историю
        self._mid.append(mid)
        self._bid_sz.append(max(bid_sz, 0.0))
        self._ask_sz.append(max(ask_sz, 0.0))

        min_hist = max(self.mom_lookback + 1, self.rev_window // 2)
        if len(self._mid) < min_hist:
            return Decision(side=None, kind="none", score=0.0, reason="warmup")

        # 3) spread guard
        spread_t = self._spread_ticks(bid, ask)
        if spread_t > self.spread_max_ticks:
            return Decision(side=None, kind="none", score=0.0,
                            reason=f"spread>{self.spread_max_ticks:.1f}t")

        # 4) OBI: сначала из micro, потом из bid/ask size
        obi_micro = 0.0
        if micro is not None:
            try:
                obi_micro = float(micro.get("obi", 0.0) or 0.0)
            except Exception:
                obi_micro = 0.0

        obi_sizes = self._obi_from_sizes(bid_sz, ask_sz)
        obi = obi_micro if obi_micro != 0.0 else obi_sizes

        # 5) Индикаторы (если есть, используются как дополнительные фильтры)
        bb_z = None
        rsi = None
        if indi is not None:
            try:
                bb_z = float(indi.get("bb_z"))
            except Exception:
                bb_z = None
            try:
                rsi = float(indi.get("rsi"))
            except Exception:
                rsi = None

        # ---- Momentum ----
        mom_side: Optional[Side] = None
        mom_score = 0.0

        if len(self._mid) > self.mom_lookback:
            base = self._mid[-1 - self.mom_lookback]
            if base > 0.0:
                move_ticks = (mid - base) / self.price_tick

                # ап-тренд + спрос (OBI >= порога)
                if move_ticks >= self.mom_thr_ticks and obi >= self.obi_mom_thr:
                    # если есть microprice_drift / tick_velocity — требуем, чтобы не шли против
                    drift_ok = True
                    if micro is not None:
                        mpd = float(micro.get("microprice_drift", 0.0) or 0.0)
                        tv = float(micro.get("tick_velocity", 0.0) or 0.0)
                        if mpd <= 0 or tv <= 0:
                            drift_ok = False
                    if drift_ok:
                        mom_side = "BUY"
                        mom_score = (move_ticks / max(self.mom_thr_ticks, 1e-9)) * \
                                    (max(obi, 0.0) / max(self.obi_mom_thr, 1e-9))

                # даун-тренд + предложение (OBI <= -порог)
                elif move_ticks <= -self.mom_thr_ticks and obi <= -self.obi_mom_thr:
                    drift_ok = True
                    if micro is not None:
                        mpd = float(micro.get("microprice_drift", 0.0) or 0.0)
                        tv = float(micro.get("tick_velocity", 0.0) or 0.0)
                        if mpd >= 0 or tv <= 0:
                            drift_ok = False
                    if drift_ok:
                        mom_side = "SELL"
                        mom_score = (abs(move_ticks) / max(self.mom_thr_ticks, 1e-9)) * \
                                    (max(-obi, 0.0) / max(self.obi_mom_thr, 1e-9))

        # ---- Reversion ----
        rev_side: Optional[Side] = None
        rev_score = 0.0

        if len(self._mid) >= self.rev_window:
            window = list(self._mid)[-self.rev_window:]
            mean = sum(window) / len(window)
            dev_ticks = (mid - mean) / self.price_tick

            # переКУПлен → SELL, нужен контр-OBI
            if dev_ticks >= self.rev_thr_ticks and obi <= -self.obi_rev_thr:
                # если есть bb_z/rsi — подтверждаем перекупленность (bb_z>0, rsi>50+)
                if (bb_z is None or bb_z > 0.0) and (rsi is None or rsi >= 55.0):
                    rev_side = "SELL"
                    rev_score = (dev_ticks / max(self.rev_thr_ticks, 1e-9)) * \
                                (max(-obi, 0.0) / max(self.obi_rev_thr, 1e-9))

            # переПРОДан → BUY, нужен контр-OBI
            elif dev_ticks <= -self.rev_thr_ticks and obi >= self.obi_rev_thr:
                if (bb_z is None or bb_z < 0.0) and (rsi is None or rsi <= 45.0):
                    rev_side = "BUY"
                    rev_score = (abs(dev_ticks) / max(self.rev_thr_ticks, 1e-9)) * \
                                (max(obi, 0.0) / max(self.obi_rev_thr, 1e-9))

        # ---- Выбор лучшего ----

        best_side: Optional[Side] = None
        best_kind = "none"
        best_score = 0.0
        best_reason = "no_signal"

        if mom_side and mom_score >= 1.0 and mom_score >= rev_score:
            best_side = mom_side
            best_kind = "momentum"
            best_score = float(mom_score)
            best_reason = f"momentum_{mom_side.lower()}_{mom_score:.2f}"

        elif rev_side and rev_score >= 1.0 and rev_score > mom_score:
            best_side = rev_side
            best_kind = "reversion"
            best_score = float(rev_score)
            best_reason = f"reversion_{rev_side.lower()}_{rev_score:.2f}"

        if not best_side:
            return Decision(side=None, kind="none", score=0.0, reason=best_reason)

        return Decision(
            side=best_side,
            kind=best_kind,
            score=best_score,
            reason=best_reason,
        )
