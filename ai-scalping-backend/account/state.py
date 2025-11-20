"""
AccountState service for live trading decisions.

This module provides read-only access to account state (equity, margin, PnL, trades)
for both live (Binance) and paper trading modes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Protocol

from pydantic import BaseModel

from storage.db import get_engine
from storage.repo import day_utc_from_ms, now_ms

logger = logging.getLogger(__name__)


# ========= Models =========


class PositionSnapshot(BaseModel):
    """Snapshot of an open position."""

    symbol: str
    side: str  # "LONG" | "SHORT" | "BUY" | "SELL"
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
        """Get open positions."""
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
    ) -> None:
        """
        Initialize AccountState.

        Args:
            adapter: Execution adapter (BinanceUSDTMAdapter or PaperAdapter)
            is_live: True for live trading, False for paper trading
        """
        self._adapter = adapter
        self._is_live = is_live
        self._snapshot: Optional[AccountSnapshot] = None

        # Технические поля для контроля качества стейта
        self._last_refresh_ms: int = 0      # время последнего успешного refresh()
        self._error_count: int = 0          # количество подряд неуспешных refresh'ей

    async def refresh(self) -> AccountSnapshot:
        """
        Refresh account snapshot from adapter and DB.

        Returns:
            Fresh AccountSnapshot
        """
        if self._is_live:
            snapshot = await self._refresh_live()
        else:
            snapshot = await self._refresh_paper()

        self._snapshot = snapshot
        # успешный refresh — обновляем время и сбрасываем счётчик ошибок
        self._last_refresh_ms = now_ms()
        self._error_count = 0
        return snapshot

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
        # Используем timestamp снапшота, а не локальное поле, чтобы не страдать от дрейфа часов
        delta = datetime.now(timezone.utc) - snap.ts
        return int(delta.total_seconds() * 1000)

    def is_fresh(self, max_age_s: int = 5) -> bool:
        """
        True, если снапшот не старше max_age_s секунд.
        Worker будет блокировать входы, если стейт устарел.
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


    async def _refresh_live(self) -> AccountSnapshot:
        """Refresh from Binance API (live mode)."""
        try:
            # Fetch account and balance data
            account_data = await self._adapter.get_account()
            balance_data = await self._adapter.get_balance()
            positions_data = await self._adapter.get_positions()

            # Extract equity (wallet balance) from account
            equity = 0.0
            available_margin = 0.0

            # Try to get USDT balance from account assets
            assets = account_data.get("assets", [])
            for asset in assets:
                if asset.get("asset") == "USDT":
                    equity = float(asset.get("walletBalance", 0.0))
                    # marginBalance = walletBalance + unrealizedProfit
                    # availableBalance is what we can use for new positions
                    available_margin = float(asset.get("availableBalance", equity))
                    break

            # Fallback to balance endpoint if account didn't have USDT
            if equity == 0.0:
                for bal in balance_data:
                    if bal.get("asset") == "USDT":
                        equity = float(bal.get("balance", 0.0))
                        available_margin = float(bal.get("availableBalance", equity))
                        break

            # Parse open positions
            open_positions: Dict[str, PositionSnapshot] = {}
            for pos in positions_data:
                symbol = pos.get("symbol", "")
                position_amt = float(pos.get("positionAmt", 0.0))

                # Only include positions with non-zero size
                if abs(position_amt) < 1e-8:
                    continue

                side_str = "LONG" if position_amt > 0 else "SHORT"
                entry_px = float(pos.get("entryPrice", 0.0))
                mark_px = float(pos.get("markPrice", 0.0)) if pos.get("markPrice") else None
                unrealized_pnl = float(pos.get("unRealizedProfit", 0.0)) if pos.get("unRealizedProfit") else None

                open_positions[symbol] = PositionSnapshot(
                    symbol=symbol,
                    side=side_str,
                    qty=abs(position_amt),
                    entry_px=entry_px,
                    mark_px=mark_px,
                    unrealized_pnl=unrealized_pnl,
                )

            # Get today's realized PnL and trade count from local DB
            # (Binance income endpoint could be used, but local DB is more reliable)
            realized_pnl_today, num_trades_today = self._get_today_stats_from_db()

            return AccountSnapshot(
                ts=datetime.now(timezone.utc),
                equity=equity,
                available_margin=available_margin,
                realized_pnl_today=realized_pnl_today,
                num_trades_today=num_trades_today,
                open_positions=open_positions,
            )

        except Exception as e:
            logger.error("[AccountState] Error refreshing live account: %s", e, exc_info=True)
            # фиксируем подряд идущую ошибку refresh'а
            self._error_count += 1
            # Возвращаем безопасный снапшот, но воркер должен это считать degraded-состоянием
            return AccountSnapshot(
                ts=datetime.now(timezone.utc),
                equity=0.0,
                available_margin=0.0,
                realized_pnl_today=0.0,
                num_trades_today=0,
                open_positions={},
            )


    async def _refresh_paper(self) -> AccountSnapshot:
        """Refresh from PaperAdapter and local DB (paper mode)."""
        try:
            # Get account data from PaperAdapter
            account_data = await self._adapter.get_account()
            balance_data = await self._adapter.get_balance()
            positions_data = await self._adapter.get_positions()

            # Extract equity and available margin
            equity = 0.0
            available_margin = 0.0

            # Try account assets first
            assets = account_data.get("assets", [])
            for asset in assets:
                if asset.get("asset") == "USDT":
                    equity = float(asset.get("walletBalance", 0.0))
                    available_margin = float(asset.get("availableBalance", equity))
                    break

            # Fallback to balance endpoint
            if equity == 0.0:
                for bal in balance_data:
                    if bal.get("asset") == "USDT":
                        equity = float(bal.get("balance", 0.0))
                        available_margin = float(bal.get("availableBalance", equity))
                        break

            # Parse open positions
            open_positions: Dict[str, PositionSnapshot] = {}
            for pos in positions_data:
                symbol = pos.get("symbol", "")
                position_amt = float(pos.get("positionAmt", 0.0))

                if abs(position_amt) < 1e-8:
                    continue

                side_str = "LONG" if position_amt > 0 else "SHORT"
                entry_px = float(pos.get("entryPrice", 0.0))
                mark_px = float(pos.get("markPrice", 0.0)) if pos.get("markPrice") else None
                unrealized_pnl = float(pos.get("unRealizedProfit", 0.0)) if pos.get("unRealizedProfit") else None

                open_positions[symbol] = PositionSnapshot(
                    symbol=symbol,
                    side=side_str,
                    qty=abs(position_amt),
                    entry_px=entry_px,
                    mark_px=mark_px,
                    unrealized_pnl=unrealized_pnl,
                )

            # Get today's stats from DB
            realized_pnl_today, num_trades_today = self._get_today_stats_from_db()

            return AccountSnapshot(
                ts=datetime.now(timezone.utc),
                equity=equity,
                available_margin=available_margin,
                realized_pnl_today=realized_pnl_today,
                num_trades_today=num_trades_today,
                open_positions=open_positions,
            )

        except Exception as e:
            logger.error("[AccountState] Error refreshing paper account: %s", e, exc_info=True)
            self._error_count += 1
            return AccountSnapshot(
                ts=datetime.now(timezone.utc),
                equity=0.0,
                available_margin=0.0,
                realized_pnl_today=0.0,
                num_trades_today=0,
                open_positions={},
            )

    def _get_today_stats_from_db(self) -> tuple[float, int]:
        """
        Get today's realized PnL and trade count from local database.

        Returns:
            Tuple of (realized_pnl_today, num_trades_today)
        """
        try:
            today_utc = day_utc_from_ms(now_ms())
            today_start_ms = int(
                datetime.strptime(today_utc, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )

            engine = get_engine()
            with engine.begin() as conn:
                from sqlalchemy import text

                # Query trades closed today
                result = conn.execute(
                    text(
                        """
                        SELECT 
                            COUNT(*) as trade_count,
                            COALESCE(SUM(pnl_usd), 0.0) as total_pnl
                        FROM trades
                        WHERE exit_ts_ms >= :start_ms
                          AND exit_ts_ms > 0
                        """
                    ),
                    {"start_ms": today_start_ms},
                ).fetchone()

                if result:
                    num_trades = int(result[0] or 0)
                    realized_pnl = float(result[1] or 0.0)
                    return (realized_pnl, num_trades)

            return (0.0, 0)

        except Exception as e:
            logger.warning("[AccountState] Error querying today's stats from DB: %s", e)
            return (0.0, 0)

