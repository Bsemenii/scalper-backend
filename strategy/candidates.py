# strategy/candidates.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal
from collections import deque

Side = Literal["BUY", "SELL"]


@dataclass
class Decision:
    """
    Результат работы CandidateEngine.

    side:
      - "BUY"/"SELL" → есть сигнал
      - None         → нет входа (reason объясняет почему)
    kind:
      - "momentum" / "reversion" / "none"
    score:
      - относительная сила сигнала (>=1.0 → достаточно сильный)
    reason:
      - короткое текстовое объяснение (идёт в block_reasons через Worker)
    """
    side: Optional[Side]
    kind: str = "none"
    score: float = 0.0
    reason: str = "no_signal"


class CandidateEngine:
    """
    Лёгкий генератор сигналов для авто-стратегии.

    Идея:
      - используем только то, что уже есть в Worker: mid-price, spread, bid/ask size.
      - два типа сигналов:
          1) momentum: пробой + поддержка со стороны OBI
          2) reversion: откат от локального экстремума + контр-OBI
      - сильно фильтруем:
          - warmup: ждём пока накопится история
          - max spread по тиковой сетке
          - слабый OBI → нет входа
          - score < 1.0 → нет входа (сигнал недостаточно сильный)
      - частоту входов сверху контролирует сам Worker через cooldown/min_flat.
    """

    def __init__(
        self,
        price_tick: float,
        max_history: int = 256,
        mom_lookback: int = 6,
        mom_thr_ticks: float = 5.0,
        rev_window: int = 48,
        rev_thr_ticks: float = 8.0,
        obi_mom_thr: float = 0.35,
        obi_rev_thr: float = 0.30,
        spread_max_ticks: float = 6.0,
    ) -> None:
        self.price_tick = max(float(price_tick), 1e-6)

        self.max_history = int(max_history)

        # momentum
        self.mom_lookback = int(mom_lookback)
        self.mom_thr_ticks = float(mom_thr_ticks)
        self.obi_mom_thr = float(obi_mom_thr)

        # reversion
        self.rev_window = int(rev_window)
        self.rev_thr_ticks = float(rev_thr_ticks)
        self.obi_rev_thr = float(obi_rev_thr)

        # spread guard
        self.spread_max_ticks = float(spread_max_ticks)

        self._mid = deque(maxlen=self.max_history)
        self._bid_sz = deque(maxlen=self.max_history)
        self._ask_sz = deque(maxlen=self.max_history)

    # ---- helpers ----

    def _spread_ticks(self, bid: float, ask: float) -> float:
        if bid > 0.0 and ask > 0.0:
            return max(0.0, (ask - bid) / self.price_tick)
        return 0.0

    def _obi(self, bid_sz: float, ask_sz: float) -> float:
        b = max(bid_sz, 0.0)
        a = max(ask_sz, 0.0)
        s = b + a
        if s <= 0.0:
            return 0.0
        v = (b - a) / s
        # слегка подрежем фантазию
        return max(-1.0, min(1.0, v))

    # ---- core ----

    def update(
        self,
        price: float,
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
    ) -> Decision:
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

        # 3) spread guard — не торгуем широкий спред
        spread_t = self._spread_ticks(bid, ask)
        if spread_t > self.spread_max_ticks:
            return Decision(
                side=None,
                kind="none",
                score=0.0,
                reason=f"spread>{self.spread_max_ticks:.1f}t",
            )

        # 4) текущий OBI
        obi = self._obi(bid_sz, ask_sz)

        # 5) Momentum-сигнал
        mom_side: Optional[Side] = None
        mom_score = 0.0
        if len(self._mid) > self.mom_lookback:
            base = self._mid[-1 - self.mom_lookback]
            if base > 0.0:
                move_ticks = (mid - base) / self.price_tick

                # ап-тренд + перевес по бид-сайзу → BUY
                if move_ticks >= self.mom_thr_ticks and obi >= self.obi_mom_thr:
                    mom_side = "BUY"
                    mom_score = (move_ticks / self.mom_thr_ticks) * (
                        max(0.0, obi) / max(self.obi_mom_thr, 1e-9)
                    )

                # даун-тренд + перевес по аск-сайзу → SELL
                elif move_ticks <= -self.mom_thr_ticks and obi <= -self.obi_mom_thr:
                    mom_side = "SELL"
                    mom_score = (abs(move_ticks) / self.mom_thr_ticks) * (
                        max(0.0, -obi) / max(self.obi_mom_thr, 1e-9)
                    )

        # 6) Reversion-сигнал (откат от локального экстремума к среднему)
        rev_side: Optional[Side] = None
        rev_score = 0.0
        if len(self._mid) >= self.rev_window:
            window = list(self._mid)[-self.rev_window:]
            mean = sum(window) / len(window)
            dev_ticks = (mid - mean) / self.price_tick

            # цена перекуплена + продавец доминирует → шорт
            if dev_ticks >= self.rev_thr_ticks and obi <= -self.obi_rev_thr:
                rev_side = "SELL"
                rev_score = (dev_ticks / self.rev_thr_ticks) * (
                    max(0.0, -obi) / max(self.obi_rev_thr, 1e-9)
                )

            # цена перепродана + покупатель доминирует → лонг
            elif dev_ticks <= -self.rev_thr_ticks and obi >= self.obi_rev_thr:
                rev_side = "BUY"
                rev_score = (abs(dev_ticks) / self.rev_thr_ticks) * (
                    max(0.0, obi) / max(self.obi_rev_thr, 1e-9)
                )

        # 7) Выбор лучшего сигнала
        best_side: Optional[Side] = None
        best_kind = "none"
        best_score = 0.0
        best_reason = "no_signal"

        # Требуем score >= 1.0, чтобы не стрелять из каждого чиха.
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
