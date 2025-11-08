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
      - относительная сила сигнала (>= min_score → достаточно сильный)
    reason:
      - короткое текстовое объяснение (летит в block_reasons через Worker)
    """
    side: Optional[Side]
    kind: str = "none"
    score: float = 0.0
    reason: str = "no_signal"


class CandidateEngine:
    """
    Лёгкий генератор сигналов для авто-стратегии.

    Быстрый, без внешних зависимостей, использует только то, что уже даёт Worker:
      - best bid/ask
      - mid-price
      - bid/ask size (L1)

    Идея:
      - momentum: пробой + сильный OBI в сторону пробоя
      - reversion: крайность относительно локального среднего + контр-OBI
      - сигналы только если:
          * spread вменяемый
          * есть история (warmup)
          * движение явно превышает локальный шум (edge над vol)
          * score >= min_score
      - если условий нет — возвращаем side=None с говорящим reason.
        Частоту входов дополнительно контролирует Worker (cooldown, day_limits).
    """

    def __init__(
        self,
        price_tick: float,
        max_history: int = 256,

        # momentum
        mom_lookback: int = 6,
        mom_thr_ticks: float = 7.0,
        obi_mom_thr: float = 0.45,

        # reversion
        rev_window: int = 64,
        rev_thr_ticks: float = 10.0,
        obi_rev_thr: float = 0.35,

        # базовые фильтры
        spread_max_ticks: float = 6.0,
        min_score: float = 1.25,

        # edge-критерий (насколько сигнал сильнее шума)
        min_edge_ticks: float = 1.5,
        vol_window: int = 32,
        vol_floor_ticks: float = 1.0,
        edge_vol_factor: float = 0.75,
    ) -> None:
        self.price_tick = max(float(price_tick), 1e-9)
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

        # общий порог качества
        self.min_score = float(min_score)

        # edge vs local vol
        self.min_edge_ticks = float(min_edge_ticks)
        self.vol_window = int(vol_window)
        self.vol_floor_ticks = float(vol_floor_ticks)
        self.edge_vol_factor = float(edge_vol_factor)

        # история
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
        return max(-1.0, min(1.0, v))

    def _local_vol_ticks(self) -> float:
        """
        Грубая оценка локального шума: диапазон mid за vol_window.
        Нужна, чтобы не входить, если движение сигнала не выбивается из шума.
        """
        n = min(len(self._mid), self.vol_window)
        if n <= 2:
            return self.vol_floor_ticks

        window = list(self._mid)[-n:]
        rng = max(window) - min(window)
        vol_t = rng / self.price_tick
        return max(self.vol_floor_ticks, vol_t)

    # ---- core ----

    def update(
        self,
        price: float,
        bid: float,
        ask: float,
        bid_sz: float,
        ask_sz: float,
    ) -> Decision:
        # 1) mid
        if bid > 0.0 and ask > 0.0:
            mid = 0.5 * (bid + ask)
        elif price > 0.0:
            mid = float(price)
        else:
            return Decision(side=None, kind="none", score=0.0, reason="no_price")

        # 2) история
        self._mid.append(mid)
        self._bid_sz.append(max(bid_sz, 0.0))
        self._ask_sz.append(max(ask_sz, 0.0))

        min_hist = max(self.mom_lookback + 1, self.rev_window // 2)
        if len(self._mid) < min_hist:
            return Decision(side=None, kind="none", score=0.0, reason="warmup")

        # 3) spread guard
        spread_t = self._spread_ticks(bid, ask)
        if spread_t > self.spread_max_ticks:
            return Decision(
                side=None,
                kind="none",
                score=0.0,
                reason=f"spread>{self.spread_max_ticks:.1f}t",
            )

        # 4) OBI + локальная волатильность
        obi = self._obi(bid_sz, ask_sz)
        vol_t = self._local_vol_ticks()

        # ---- Momentum ----
        mom_side: Optional[Side] = None
        mom_score = 0.0
        mom_reason = "no_momentum"

        if len(self._mid) > self.mom_lookback:
            base = self._mid[-1 - self.mom_lookback]
            if base > 0.0:
                move_ticks = (mid - base) / self.price_tick

                # Ап-тренд + сильный bid-OBI
                if move_ticks >= self.mom_thr_ticks and obi >= self.obi_mom_thr:
                    edge_ticks = move_ticks - self.mom_thr_ticks
                    if edge_ticks >= max(self.min_edge_ticks, vol_t * self.edge_vol_factor):
                        mom_side = "BUY"
                        mom_score = (move_ticks / self.mom_thr_ticks) * (
                            max(0.0, obi) / max(self.obi_mom_thr, 1e-9)
                        )
                        mom_reason = f"momentum_buy_mv{move_ticks:.1f}t_obi{obi:.2f}"
                    else:
                        mom_reason = "momentum_buy_weak_edge"

                # Даун-тренд + сильный ask-OBI
                elif move_ticks <= -self.mom_thr_ticks and obi <= -self.obi_mom_thr:
                    edge_ticks = abs(move_ticks) - self.mom_thr_ticks
                    if edge_ticks >= max(self.min_edge_ticks, vol_t * self.edge_vol_factor):
                        mom_side = "SELL"
                        mom_score = (abs(move_ticks) / self.mom_thr_ticks) * (
                            max(0.0, -obi) / max(self.obi_mom_thr, 1e-9)
                        )
                        mom_reason = f"momentum_sell_mv{move_ticks:.1f}t_obi{obi:.2f}"
                    else:
                        mom_reason = "momentum_sell_weak_edge"

        # ---- Reversion ----
        rev_side: Optional[Side] = None
        rev_score = 0.0
        rev_reason = "no_reversion"

        if len(self._mid) >= self.rev_window:
            window = list(self._mid)[-self.rev_window:]
            mean = sum(window) / len(window)
            dev_ticks = (mid - mean) / self.price_tick

            # перекупленность + продавец доминирует → шорт
            if dev_ticks >= self.rev_thr_ticks and obi <= -self.obi_rev_thr:
                edge_ticks = dev_ticks - self.rev_thr_ticks
                if edge_ticks >= max(self.min_edge_ticks, vol_t * self.edge_vol_factor):
                    rev_side = "SELL"
                    rev_score = (dev_ticks / self.rev_thr_ticks) * (
                        max(0.0, -obi) / max(self.obi_rev_thr, 1e-9)
                    )
                    rev_reason = f"reversion_sell_dev{dev_ticks:.1f}t_obi{obi:.2f}"
                else:
                    rev_reason = "reversion_sell_weak_edge"

            # перепроданность + покупатель доминирует → лонг
            elif dev_ticks <= -self.rev_thr_ticks and obi >= self.obi_rev_thr:
                edge_ticks = abs(dev_ticks) - self.rev_thr_ticks
                if edge_ticks >= max(self.min_edge_ticks, vol_t * self.edge_vol_factor):
                    rev_side = "BUY"
                    rev_score = (abs(dev_ticks) / self.rev_thr_ticks) * (
                        max(0.0, obi) / max(self.obi_rev_thr, 1e-9)
                    )
                    rev_reason = f"reversion_buy_dev{dev_ticks:.1f}t_obi{obi:.2f}"
                else:
                    rev_reason = "reversion_buy_weak_edge"

        # ---- Выбор лучшего ----
        best_side: Optional[Side] = None
        best_kind = "none"
        best_score = 0.0
        best_reason = "no_signal"

        if mom_side and mom_score >= self.min_score and mom_score >= rev_score:
            best_side = mom_side
            best_kind = "momentum"
            best_score = float(mom_score)
            best_reason = mom_reason

        elif rev_side and rev_score >= self.min_score and rev_score > mom_score:
            best_side = rev_side
            best_kind = "reversion"
            best_score = float(rev_score)
            best_reason = rev_reason

        if not best_side:
            # Вернём максимально полезное объяснение, почему фильтрануло.
            if mom_reason != "no_momentum":
                best_reason = mom_reason
            elif rev_reason != "no_reversion":
                best_reason = rev_reason
            else:
                best_reason = "no_signal"

            return Decision(side=None, kind="none", score=0.0, reason=best_reason)

        return Decision(
            side=best_side,
            kind=best_kind,
            score=best_score,
            reason=best_reason,
        )
