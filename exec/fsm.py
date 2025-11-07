# exec/fsm.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from bot.core.types import PosState, PositionState, Side


@dataclass
class PosRuntime:
    """
    Внутреннее состояние позиции для FSM (не светим наружу напрямую).
    """
    state: PosState = PosState.FLAT
    entry_px: Optional[float] = None
    qty: float = 0.0
    sl: Optional[float] = None
    tp: Optional[float] = None
    rr: float = 0.0
    opened_ts: Optional[int] = None

    # расширения для аудита/БД (опционально):
    side: Optional[Side] = None          # BUY/SELL
    trade_id: Optional[str] = None       # uuid записи в trades (SQLite)
    symbol: Optional[str] = None         # тикер для связки в БД


class PositionFSM:
    """
    Минимальная машина состояний позиции:
      FLAT -> ENTERING -> OPEN -> EXITING -> FLAT

    Совместимость:
    - API методов не ломает старый код (аргументы расширены только опционально).
    - Если модуль storage.repo недоступен — запись в БД тихо пропускается.
    - Типы берём из bot.core.types (PosState/PositionState/Side).

    Поведение:
    - on_open(...) при наличии symbol/side попробует создать запись сделки (open_trade).
    - on_close(...) попытается закрыть сделку (close_trade) и обновить дневную сводку.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.pos = PosRuntime()

    # ---------- helpers ----------

    @staticmethod
    def _calc_r(side: Optional[Side], entry_px: float, exit_px: float, sl_px: Optional[float]) -> float:
        """
        Унифицированный расчёт R:
        R = PnL / |Entry - SL|
        Для SHORT denom = (SL - Entry), для LONG denom = (Entry - SL).
        Фолбэки: если SL неизвестен или denom==0 — R=0.
        """
        if side is None or sl_px is None:
            return 0.0
        try:
            if side == Side.BUY:
                denom = (entry_px - sl_px)
                if denom == 0:
                    return 0.0
                return (exit_px - entry_px) / denom
            else:
                denom = (sl_px - entry_px)
                if denom == 0:
                    return 0.0
                return (entry_px - exit_px) / denom
        except Exception:
            return 0.0

    @staticmethod
    def _calc_pnl_usd(side: Optional[Side], entry_px: float, exit_px: float, qty: float) -> float:
        """
        Валютный PnL без учёта комиссий (их учитывает верхний уровень).
        Для LONG: (exit - entry) * qty
        Для SHORT: (entry - exit) * qty
        """
        if side is None:
            return 0.0
        try:
            if side == Side.BUY:
                return (exit_px - entry_px) * qty
            else:
                return (entry_px - exit_px) * qty
        except Exception:
            return 0.0

    def _reset(self) -> None:
        """Мгновенный сброс в FLAT (внутренний, без БД)."""
        self.pos = PosRuntime()

    # ---------- external API ----------

    async def can_enter(self) -> bool:
        """
        Допуск на вход: true только из FLAT.
        """
        # отдельный лок тут не обязателен — решение верхнего уровня всё равно
        # принимает вход под своим мьютексом; но оставим для семантики.
        async with self._lock:
            return self.pos.state == PosState.FLAT

    async def on_entering(self) -> None:
        """
        Переход в ENTERING перед фактическим исполнением входа.
        """
        async with self._lock:
            self.pos.state = PosState.ENTERING

    async def on_open(
        self,
        entry_px: float,
        qty: float,
        sl: float,
        tp: Optional[float],
        rr: float,
        opened_ts: int,
        *,
        # новые опциональные параметры (не ломают старые вызовы):
        symbol: Optional[str] = None,
        side: Optional[Side] = None,
        reason_open: str = "auto",
        meta: Optional[dict] = None,
    ) -> None:
        """
        Фиксация открытия позиции.
        Если symbol и side переданы — пытаемся записать сделку в SQLite (если доступен storage.repo).
        """
        async with self._lock:
            self.pos.state = PosState.OPEN
            self.pos.entry_px = float(entry_px)
            self.pos.qty = float(qty)
            self.pos.sl = float(sl) if sl is not None else None
            self.pos.tp = float(tp) if tp is not None else None
            self.pos.rr = float(rr)
            self.pos.opened_ts = int(opened_ts)

            # расширения:
            if symbol:
                self.pos.symbol = symbol
            if side:
                self.pos.side = side

            # Пишем в БД, если можем
            if self.pos.symbol and self.pos.side:
                try:
                    from storage.repo import TradeOpenCtx, open_trade  # type: ignore
                    side_ls = "LONG" if self.pos.side == Side.BUY else "SHORT"
                    tid = open_trade(
                        TradeOpenCtx(
                            symbol=self.pos.symbol,
                            side=side_ls,
                            qty=float(self.pos.qty),
                            entry_px=float(self.pos.entry_px),
                            sl_px=(float(self.pos.sl) if self.pos.sl is not None else None),
                            tp_px=(float(self.pos.tp) if self.pos.tp is not None else None),
                            reason_open=(reason_open or ""),
                            meta=(meta or {}),
                        )
                    )
                    self.pos.trade_id = tid
                except ModuleNotFoundError:
                    # хранилище не подключено — тихо пропускаем
                    pass
                except Exception as e:
                    # не роняем стратегию
                    print(f"[sqlite] open_trade failed: {e}")

    async def on_exiting(self) -> None:
        """
        Переход в EXITING перед фактическим исполнением выхода.
        """
        async with self._lock:
            self.pos.state = PosState.EXITING

    async def on_close(
        self,
        exit_px: float,
        reason_close: str,
        closed_ts: Optional[int] = None,
    ) -> None:
        """
        Корректное закрытие позиции.
        Считает R и PnL и пытается записать закрытие в SQLite.
        После записи переводит в FLAT.
        """
        async with self._lock:
            entry_px = float(self.pos.entry_px or 0.0)
            sl_px = float(self.pos.sl) if self.pos.sl is not None else None
            qty = float(self.pos.qty)
            side = self.pos.side

            r = self._calc_r(side, entry_px, float(exit_px), sl_px)
            pnl_usd = self._calc_pnl_usd(side, entry_px, float(exit_px), qty)

            if self.pos.trade_id:
                try:
                    from storage.repo import close_trade, upsert_daily_stats, day_utc_from_ms  # type: ignore
                    close_trade(self.pos.trade_id, exit_px=float(exit_px), r=float(r), pnl_usd=float(pnl_usd), reason_close=reason_close)

                    ts_ms = int(closed_ts if closed_ts is not None else time.time() * 1000)
                    day_utc = day_utc_from_ms(ts_ms)
                    # win флаг используем как r > 0 (комиссии уже учтёт верхний слой, если нужно)
                    upsert_daily_stats(day_utc, delta_r=float(r), delta_usd=float(pnl_usd), win=(r > 0.0))
                except ModuleNotFoundError:
                    pass
                except Exception as e:
                    print(f"[sqlite] close_trade/upsert_daily failed: {e}")

            # перевод в FLAT
            self._reset()

    async def on_flat(self) -> None:
        """
        Принудительно перевести в FLAT (используется для откатов/финализации).
        """
        async with self._lock:
            self._reset()

    # ---------- snapshots / compatibility ----------

    def snapshot(self) -> PositionState:
        """
        Наружный снепшот в типе bot.core.types.PositionState (как и раньше).
        Внимание: symbol здесь не заполняем — его проставляет верхний уровень (per-symbol map).
        """
        return PositionState(
            symbol="",
            state=self.pos.state,
            entry_px=float(self.pos.entry_px or 0.0),
            qty=float(self.pos.qty),
            sl=(float(self.pos.sl) if self.pos.sl is not None else None),
            tp=(float(self.pos.tp) if self.pos.tp is not None else None),
            rr=float(self.pos.rr),
            opened_ts=int(self.pos.opened_ts or 0),
        )
