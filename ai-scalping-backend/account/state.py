"""
AccountState service for live trading decisions.

This module provides read-only access to account state (equity, margin, PnL, trades)
for both live (Binance) and paper trading modes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Protocol, Tuple

from pydantic import BaseModel
from sqlalchemy import text

from storage.db import get_engine
from storage.repo import now_ms  # day_utc_from_ms можно использовать позже, пока не нужно

logger = logging.getLogger(__name__)


# ========= Models =========


class PositionSnapshot(BaseModel):
    """Snapshot of an open position."""

    symbol: str
    side: str  # "LONG" | "SHORT"
    qty: float
    entry_px: float
    mark_px: Optional[float] = None
    unrealized_pnl: Optional[float] = None


class AccountSnapshot(BaseModel):
    """Snapshot of account state at a point in time."""

    ts: datetime
    equity: float  # total wallet balance in USDT
    available_margin: float  # free balance available for trading
    realized_pnl_today: float  # realized PnL for today (UTC)
    num_trades_today: int  # number of closed trades today (UTC)
    unrealized_pnl_now: float  # total unrealized PnL across all open positions
    open_positions: Dict[str, PositionSnapshot]  # symbol -> position


# ========= Adapter Protocol =========


class ExecAdapter(Protocol):
    """Protocol for execution adapters (BinanceUSDTMAdapter or PaperAdapter)."""

    async def get_account(self) -> Dict[str, Any]:
        """Get account information."""
        ...

    async def get_balance(self) -> list[Dict[str, Any]]:
        """Get balance information."""
        ...

    async def get_positions(self) -> list[Dict[str, Any]]:
        """Get open positions (Binance positionRisk-style)."""
        ...


class PriceProvider(Protocol):
    """Protocol for getting current market prices (e.g., hub with best_bid_ask)."""

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        """Get current best bid and ask for a symbol."""
        ...


# ========= AccountState Service =========


class AccountState:
    """
    Read-only account state service.

    Provides live account data for trading decisions:
    - Position sizing (equity)
    - Daily stop/target (realized_pnl_today)
    - Max trades per day (num_trades_today)

    In live mode: fetches data from Binance API.
    In paper mode: computes from local DB and PaperAdapter state.
    """

    def __init__(
        self,
        adapter: ExecAdapter,
        is_live: bool = False,
        price_provider: Optional[PriceProvider] = None,
    ) -> None:
        """
        Initialize AccountState.

        Args:
            adapter: Execution adapter (BinanceUSDTMAdapter or PaperAdapter)
            is_live: True for live trading, False for paper trading
            price_provider: Optional hub/provider for getting current mid prices
        """
        self._adapter = adapter
        self._is_live = is_live
        self._price_provider = price_provider
        self._snapshot: Optional[AccountSnapshot] = None

        # Технические поля для контроля качества стейта
        self._last_refresh_ms: int = 0      # время последнего успешного refresh()
        self._error_count: int = 0          # количество подряд неуспешных refresh'ей

    # ======= Публичный API =======

    async def refresh(self) -> AccountSnapshot:
        """
        Refresh account snapshot from adapter and DB.

        На успех:
          - обновляет self._snapshot
          - сбрасывает _error_count
        На ошибке:
          - инкрементирует _error_count
          - возвращает последний известный снапшот (или пустой, если его ещё нет)
        """
        try:
            # Only live mode is supported (paper trading removed)
            if not self._is_live:
                raise RuntimeError("Only live mode is supported. Paper trading has been removed.")
            snapshot = await self._refresh_live()

            self._snapshot = snapshot
            self._last_refresh_ms = now_ms()
            self._error_count = 0
            return snapshot
        except Exception as e:
            mode = "live"
            logger.error(
                "[AccountState] Error refreshing %s account: %s", mode, e, exc_info=True
            )
            self._error_count += 1

            # Если снапшот уже был — возвращаем его как degraded-состояние
            if self._snapshot is not None:
                return self._snapshot

            # Иначе — отдаём безопасное пустое состояние
            empty = AccountSnapshot(
                ts=datetime.now(timezone.utc),
                equity=0.0,
                available_margin=0.0,
                realized_pnl_today=0.0,
                num_trades_today=0,
                unrealized_pnl_now=0.0,
                open_positions={},
            )
            self._snapshot = empty
            return empty

    def get_snapshot(self) -> Optional[AccountSnapshot]:
        """
        Get the last refreshed snapshot (without fetching new data).

        Returns:
            Last AccountSnapshot or None if never refreshed
        """
        return self._snapshot

    def snapshot_age_ms(self) -> Optional[int]:
        """
        Возраст текущего снапшота в миллисекундах, или None, если снапшота ещё нет.
        """
        snap = self._snapshot
        if snap is None:
            return None
        delta = datetime.now(timezone.utc) - snap.ts
        return int(delta.total_seconds() * 1000)

    def is_fresh(self, max_age_s: int = 5) -> bool:
        """
        True, если снапшот не старше max_age_s секунд.
        Worker может блокировать входы, если стейт устарел.
        """
        age_ms = self.snapshot_age_ms()
        if age_ms is None:
            return False
        return age_ms <= max_age_s * 1000

    @property
    def consecutive_errors(self) -> int:
        """
        Количество подряд неуспешных refresh'ей.
        Если растёт — Worker должен выключить авто-режим.
        """
        return self._error_count

    # ======= Внутренние методы refresh =======

    def _compute_position_upnl(
        self,
        entry_px: float,
        position_amt: float,
        current_px: float,
    ) -> float:
        """
        Compute unrealized PnL for a single position.

        Args:
            entry_px: Entry price
            position_amt: Position amount (positive for long, negative for short)
            current_px: Current market price (mid or mark)

        Returns:
            Unrealized PnL in USDT
        """
        if abs(position_amt) < 1e-8 or entry_px <= 0.0 or current_px <= 0.0:
            return 0.0

        # For USDT-M futures:
        # Long (position_amt > 0): uPnL = (current_px - entry_px) * position_amt
        # Short (position_amt < 0): uPnL = (entry_px - current_px) * abs(position_amt)
        #                                 = (current_px - entry_px) * position_amt (same formula)
        return (current_px - entry_px) * position_amt

    def _get_current_price(self, symbol: str, fallback_mark_px: Optional[float] = None) -> float:
        """
        Get current price for a symbol.

        Priority (updated to match Binance Futures UI):
        1. Mark price from position data (primary)
        2. Mid price from price_provider (hub) as fallback
        3. 0.0 if neither available

        Args:
            symbol: Trading symbol
            fallback_mark_px: Mark price from position data

        Returns:
            Current price or 0.0 if not available
        """
        # Try to use mark price first (matches Binance Futures UI)
        if fallback_mark_px is not None and fallback_mark_px > 0.0:
            return fallback_mark_px

        # Fallback to mid price from hub
        if self._price_provider is not None:
            try:
                bid, ask = self._price_provider.best_bid_ask(symbol)
                if bid > 0.0 and ask > 0.0:
                    return (bid + ask) / 2.0
            except Exception:
                pass

        return 0.0

    async def _refresh_live(self) -> AccountSnapshot:
        """
        Refresh from Binance API (live mode).

        Здесь НЕТ try/except — ошибки ловятся в публичном refresh().
        """
        # 1) Берём данные по аккаунту и позициям с адаптера
        # Используем тот же метод get_positions, что и Worker (вызывает /fapi/v2/positionRisk)
        account_data = await self._adapter.get_account()
        balance_data = await self._adapter.get_balance()
        positions_data = await self._adapter.get_positions()

        # 2) Считаем equity и available_margin
        equity = 0.0
        available_margin = 0.0

        # Пробуем взять из account_data.assets (фьючерсный аккаунт)
        assets = account_data.get("assets", [])
        for asset in assets:
            if asset.get("asset") == "USDT":
                equity = float(asset.get("walletBalance", 0.0))
                available_margin = float(asset.get("availableBalance", equity))
                break

        # Fallback: если по какой-то причине не нашли assets — пробуем balance_data
        if equity == 0.0:
            for bal in balance_data:
                if bal.get("asset") == "USDT":
                    equity = float(bal.get("balance", 0.0))
                    available_margin = float(bal.get("availableBalance", equity))
                    break

        # 3) Парсим открытые позиции из positionRisk и считаем unrealized PnL
        open_positions: Dict[str, PositionSnapshot] = {}
        total_unrealized_pnl = 0.0

        for pos in positions_data:
            symbol = pos.get("symbol", "")
            if not symbol:
                continue

            position_amt = float(pos.get("positionAmt", 0.0))
            if abs(position_amt) < 1e-8:
                continue

            side_str = "LONG" if position_amt > 0 else "SHORT"
            entry_px = float(pos.get("entryPrice", 0.0))
            
            # Extract mark price as fallback
            mark_px = None
            if pos.get("markPrice") is not None:
                try:
                    mark_px = float(pos.get("markPrice"))
                except Exception:
                    mark_px = None

            # Compute unrealized PnL using entry_px, positionAmt, and current price
            # Priority: mid price from hub, fallback to mark price
            current_px = self._get_current_price(symbol, fallback_mark_px=mark_px)
            unrealized_pnl = self._compute_position_upnl(entry_px, position_amt, current_px)
            
            # Accumulate total unrealized PnL
            total_unrealized_pnl += unrealized_pnl

            open_positions[symbol] = PositionSnapshot(
                symbol=symbol,
                side=side_str,
                qty=abs(position_amt),
                entry_px=entry_px,
                mark_px=mark_px,
                unrealized_pnl=unrealized_pnl,
            )

        # 4) Берём дневной PnL и число сделок из локальной БД
        eng = get_engine()
        with eng.begin() as conn:
            realized_pnl_today, num_trades_today = self._get_today_stats_from_db(conn)

        # 5) Собираем снапшот
        return AccountSnapshot(
            ts=datetime.now(timezone.utc),
            equity=equity,
            available_margin=available_margin,
            realized_pnl_today=realized_pnl_today,
            num_trades_today=num_trades_today,
            unrealized_pnl_now=total_unrealized_pnl,
            open_positions=open_positions,
        )

    async def _refresh_paper(self) -> AccountSnapshot:
        """
        Refresh from PaperAdapter and local DB (paper mode).

        Логика аналогична live, но использует paper-адаптер.
        """
        account_data = await self._adapter.get_account()
        balance_data = await self._adapter.get_balance()
        positions_data = await self._adapter.get_positions()

        equity = 0.0
        available_margin = 0.0

        assets = account_data.get("assets", [])
        for asset in assets:
            if asset.get("asset") == "USDT":
                equity = float(asset.get("walletBalance", 0.0))
                available_margin = float(asset.get("availableBalance", equity))
                break

        if equity == 0.0:
            for bal in balance_data:
                if bal.get("asset") == "USDT":
                    equity = float(bal.get("balance", 0.0))
                    available_margin = float(bal.get("availableBalance", equity))
                    break

        # Parse positions and compute unrealized PnL
        open_positions: Dict[str, PositionSnapshot] = {}
        total_unrealized_pnl = 0.0

        for pos in positions_data:
            symbol = pos.get("symbol", "")
            if not symbol:
                continue

            position_amt = float(pos.get("positionAmt", 0.0))
            if abs(position_amt) < 1e-8:
                continue

            side_str = "LONG" if position_amt > 0 else "SHORT"
            entry_px = float(pos.get("entryPrice", 0.0))
            
            # Extract mark price as fallback
            mark_px = None
            if pos.get("markPrice") is not None:
                try:
                    mark_px = float(pos.get("markPrice"))
                except Exception:
                    mark_px = None

            # Compute unrealized PnL using entry_px, positionAmt, and current price
            current_px = self._get_current_price(symbol, fallback_mark_px=mark_px)
            unrealized_pnl = self._compute_position_upnl(entry_px, position_amt, current_px)
            
            # Accumulate total unrealized PnL
            total_unrealized_pnl += unrealized_pnl

            open_positions[symbol] = PositionSnapshot(
                symbol=symbol,
                side=side_str,
                qty=abs(position_amt),
                entry_px=entry_px,
                mark_px=mark_px,
                unrealized_pnl=unrealized_pnl,
            )

        eng = get_engine()
        with eng.begin() as conn:
            realized_pnl_today, num_trades_today = self._get_today_stats_from_db(conn)

        return AccountSnapshot(
            ts=datetime.now(timezone.utc),
            equity=equity,
            available_margin=available_margin,
            realized_pnl_today=realized_pnl_today,
            num_trades_today=num_trades_today,
            unrealized_pnl_now=total_unrealized_pnl,
            open_positions=open_positions,
        )

    # ======= Работа с БД (дневной PnL) =======

    def _get_today_stats_from_db(self, conn) -> tuple[float, int]:
        """
        Best-effort: читаем дневную статистику из таблицы daily_stats.

        Возвращает:
            (realized_pnl_today_usd, num_trades_today)

        Гарантии:
        - НИКОГДА не кидает исключения наружу.
        - Если строки за сегодня нет — возвращает (0.0, 0).
        - Корректно работает и с SQLAlchemy Row, и с plain tuple.
        """
        from storage.repo import day_utc_from_ms, now_ms  # уже есть в repo.py

        try:
            day_utc = day_utc_from_ms(now_ms())
        except Exception:
            # Fallback: если вдруг что-то не так с импортом/временем
            from datetime import datetime, timezone

            day_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            row = conn.execute(
                text(
                    """
                    SELECT
                      COALESCE(pnl_usd, 0.0)  AS pnl,
                      COALESCE(trades, 0)     AS cnt
                      FROM daily_stats
                     WHERE day_utc = :d
                    """
                ),
                {"d": day_utc},
            ).fetchone()
        except Exception:
            # Любая проблема с SQL → считаем, что за сегодня 0 / 0
            return 0.0, 0

        if not row:
            return 0.0, 0

        # --- Вариант 1: SQLAlchemy Row с маппингом ---
        try:
            mapping = getattr(row, "_mapping", None)
            if mapping is not None:
                pnl = float(mapping.get("pnl", 0.0) or 0.0)
                cnt = int(mapping.get("cnt", 0) or 0)
                return pnl, cnt
        except Exception:
            # если вдруг что-то пошло не так — идём в tuple-fallback
            pass

        # --- Вариант 2: plain tuple / последовательность ---
        try:
            # пробуем распаковать как (pnl, cnt)
            pnl_val, cnt_val = row
            pnl = float(pnl_val or 0.0)
            cnt = int(cnt_val or 0)
            return pnl, cnt
        except Exception:
            # последний fallback через индексы
            try:
                pnl = float((row[0] if len(row) > 0 else 0.0) or 0.0)
                cnt = int((row[1] if len(row) > 1 else 0) or 0)
                return pnl, cnt
            except Exception:
                return 0.0, 0

    def reconcile_realized_pnl_with_db(self, day_str: str) -> Optional[float]:
        """
        Reconcile realized PnL from memory with DB for a given day.

        Args:
            day_str: Day in format 'YYYY-MM-DD' (UTC)

        Returns:
            Difference (db_sum - realized_pnl_today) or None if failed

        This method is read-only and never crashes the main flow.
        """
        try:
            snapshot = self._snapshot
            if snapshot is None:
                return None

            eng = get_engine()
            with eng.begin() as conn:
                # Query sum of pnl_usd from trades for the given day
                row = conn.execute(
                    text(
                        """
                        SELECT COALESCE(SUM(pnl_usd), 0.0) AS db_sum
                          FROM trades
                         WHERE DATE(exit_ts_ms / 1000.0, 'unixepoch') = :day
                           AND exit_ts_ms > 0
                        """
                    ),
                    {"day": day_str},
                ).fetchone()

                if row is None:
                    return None

                # Extract db_sum (handle both Row and tuple)
                db_sum = 0.0
                try:
                    if hasattr(row, "_mapping"):
                        db_sum = float(row._mapping.get("db_sum", 0.0) or 0.0)
                    else:
                        db_sum = float(row[0] or 0.0)
                except Exception:
                    db_sum = 0.0

                # Compare with snapshot's realized_pnl_today
                realized_pnl_today = float(snapshot.realized_pnl_today)
                diff = db_sum - realized_pnl_today

                # Log if difference is significant
                if abs(diff) > 0.10:
                    log_level = logger.warning if abs(diff) > 1.0 else logger.info
                    log_level(
                        "[AccountState] PnL reconciliation for %s: "
                        "DB=%.2f, Memory=%.2f, Diff=%.2f",
                        day_str,
                        db_sum,
                        realized_pnl_today,
                        diff,
                    )

                return diff

        except Exception as e:
            logger.warning(
                "[AccountState] reconcile_realized_pnl_with_db failed for %s: %s",
                day_str,
                e,
            )
            return None
