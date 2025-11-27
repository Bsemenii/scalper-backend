"""
Minimal tests for storage/repo.py DB operations.

Tests:
- open_trade and close_trade with correct column names
- SQL queries used in /trades and /pnl/now endpoints
- Verifies pnl_r, fees_usd, entry_ts_ms columns exist and work correctly
"""
import sys
import pathlib
import tempfile
import os
from pathlib import Path

# Add project root to path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import pytest
from sqlalchemy import create_engine, text
from storage.repo import (
    open_trade,
    close_trade,
    get_trade,
    load_recent_trades,
    save_order,
    save_fill,
    TradeOpenCtx,
)
from storage.db import init_db, get_engine


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    # Create a temporary file
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    
    # Override DB_PATH in storage.db module
    import storage.db
    original_path = storage.db.DB_PATH
    storage.db.DB_PATH = Path(db_path)
    storage.db._engine = None  # Reset engine
    
    try:
        # Initialize DB with schema
        init_db()
        yield db_path
    finally:
        # Cleanup
        storage.db.DB_PATH = original_path
        storage.db._engine = None
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_open_trade(temp_db):
    """Test that open_trade creates a trade with correct columns."""
    ctx = TradeOpenCtx(
        symbol="BTCUSDT",
        side="LONG",
        qty=0.001,
        entry_px=50000.0,
        sl_px=49000.0,
        tp_px=51000.0,
        reason_open="test_entry",
        meta={"test": True},
    )
    
    trade_id = open_trade(ctx)
    assert trade_id is not None
    
    # Verify trade was created
    trade = get_trade(trade_id)
    assert trade is not None
    assert trade["symbol"] == "BTCUSDT"
    assert trade["side"] == "LONG"
    assert trade["qty"] == 0.001
    assert trade["entry_px"] == 50000.0
    assert trade["entry_ts_ms"] > 0
    assert trade["exit_ts_ms"] == 0  # Not closed yet
    assert trade["pnl_usd"] == 0.0
    assert trade["pnl_r"] == 0.0
    assert trade["fees_usd"] == 0.0
    assert "entry_ts_ms" in trade  # Verify column exists
    assert "pnl_r" in trade
    assert "fees_usd" in trade


def test_close_trade_with_fees(temp_db):
    """Test that close_trade updates pnl_usd, pnl_r, fees_usd correctly."""
    # Open a trade
    ctx = TradeOpenCtx(
        symbol="ETHUSDT",
        side="SHORT",
        qty=0.1,
        entry_px=3000.0,
        sl_px=3100.0,
        tp_px=2900.0,
        reason_open="test",
    )
    trade_id = open_trade(ctx)
    
    # Create orders and fills with fees
    entry_order_id = "entry_order_1"
    exit_order_id = "exit_order_1"
    
    save_order({
        "id": entry_order_id,
        "trade_id": trade_id,
        "symbol": "ETHUSDT",
        "side": "SELL",
        "type": "MARKET",
        "reduce_only": False,
        "qty": 0.1,
        "status": "FILLED",
    })
    
    save_order({
        "id": exit_order_id,
        "trade_id": trade_id,
        "symbol": "ETHUSDT",
        "side": "BUY",
        "type": "MARKET",
        "reduce_only": True,
        "qty": 0.1,
        "status": "FILLED",
    })
    
    # Create fills with fees
    save_fill({
        "id": "fill_entry_1",
        "order_id": entry_order_id,
        "px": 3000.0,
        "qty": 0.1,
        "fee_usd": 0.6,  # 0.1 * 3000 * 0.002 (maker fee)
    })
    
    save_fill({
        "id": "fill_exit_1",
        "order_id": exit_order_id,
        "px": 2900.0,
        "qty": 0.1,
        "fee_usd": 0.58,  # 0.1 * 2900 * 0.002
    })
    
    # Close the trade
    # SHORT: profit when exit < entry
    # PnL gross = (3000 - 2900) * 0.1 = 10.0
    # Fees = 0.6 + 0.58 = 1.18
    # PnL net = 10.0 - 1.18 = 8.82
    # R = 8.82 / (risk_usd), but we'll use a simple r value
    close_trade(
        trade_id=trade_id,
        exit_px=2900.0,
        r=1.5,  # Example R value
        pnl_usd=8.82,  # Fallback, but should be recalculated
        reason_close="test_close",
    )
    
    # Verify trade was updated
    trade = get_trade(trade_id)
    assert trade is not None
    assert trade["exit_ts_ms"] > 0
    assert trade["exit_px"] == 2900.0
    assert trade["pnl_r"] == 1.5  # Should be set to r
    assert trade["fees_usd"] == pytest.approx(1.18, abs=0.01)  # Sum of both fills
    # pnl_usd should be recalculated: gross - fees = 10.0 - 1.18 = 8.82
    assert trade["pnl_usd"] == pytest.approx(8.82, abs=0.01)
    assert trade["reason_close"] == "test_close"


def test_load_recent_trades(temp_db):
    """Test that load_recent_trades returns trades with correct columns."""
    # Create multiple trades
    for i in range(3):
        ctx = TradeOpenCtx(
            symbol="BTCUSDT",
            side="LONG",
            qty=0.001,
            entry_px=50000.0 + i * 100,
            reason_open=f"test_{i}",
        )
        trade_id = open_trade(ctx)
        close_trade(
            trade_id=trade_id,
            exit_px=50100.0 + i * 100,
            r=0.5,
            pnl_usd=0.1,
            reason_close="test",
        )
    
    trades = load_recent_trades(limit=10)
    assert len(trades) == 3
    
    # Verify all required columns exist
    for trade in trades:
        assert "entry_ts_ms" in trade
        assert "exit_ts_ms" in trade
        assert "pnl_usd" in trade
        assert "pnl_r" in trade
        assert "fees_usd" in trade
        assert trade["entry_ts_ms"] > 0
        assert trade["exit_ts_ms"] > 0


def test_trades_endpoint_sql(temp_db):
    """Test that the SQL query used in /trades endpoint works."""
    # Create a trade
    ctx = TradeOpenCtx(
        symbol="SOLUSDT",
        side="LONG",
        qty=1.0,
        entry_px=100.0,
        reason_open="test",
    )
    trade_id = open_trade(ctx)
    close_trade(
        trade_id=trade_id,
        exit_px=101.0,
        r=1.0,
        pnl_usd=1.0,
        reason_close="test",
    )
    
    # Execute the same SQL query as /trades endpoint
    eng = get_engine()
    with eng.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT
                  t.id,
                  t.symbol,
                  t.side,
                  t.qty,
                  t.entry_px,
                  t.exit_px,
                  t.pnl_usd,
                  t.pnl_r,
                  t.entry_ts_ms,
                  t.exit_ts_ms,
                  t.reason_open,
                  t.reason_close,
                  COALESCE(t.fees_usd, 0.0) AS fees_usd
                FROM trades t
                ORDER BY t.exit_ts_ms DESC, t.entry_ts_ms DESC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": 10, "offset": 0},
        ).fetchall()
    
    assert len(rows) == 1
    row = rows[0]
    # Verify we can access all columns
    assert row._mapping["id"] == trade_id
    assert row._mapping["symbol"] == "SOLUSDT"
    assert row._mapping["pnl_usd"] == 1.0
    assert row._mapping["pnl_r"] == 1.0
    assert row._mapping["fees_usd"] == 0.0
    assert row._mapping["entry_ts_ms"] > 0
    assert row._mapping["exit_ts_ms"] > 0


def test_pnl_now_sql(temp_db):
    """Test that the SQL query used in /pnl/now endpoint works."""
    from datetime import datetime, timezone
    
    # Create a trade closed today
    ctx = TradeOpenCtx(
        symbol="BTCUSDT",
        side="LONG",
        qty=0.001,
        entry_px=50000.0,
        reason_open="test",
    )
    trade_id = open_trade(ctx)
    close_trade(
        trade_id=trade_id,
        exit_px=50100.0,
        r=2.0,
        pnl_usd=0.1,
        reason_close="test",
    )
    
    # Get today's date string
    day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Execute the same SQL query as /pnl/now endpoint
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                COUNT(*) AS trades,
                COALESCE(SUM(pnl_usd), 0.0) AS pnl_usd_sum,
                COALESCE(SUM(pnl_r), 0.0) AS pnl_r_sum,
                COALESCE(SUM(fees_usd), 0.0) AS fees_usd_sum,
                COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 0) AS wins,
                COALESCE(SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END), 0) AS losses
                FROM trades
                WHERE DATE(exit_ts_ms / 1000.0, 'unixepoch') = :day
                AND exit_ts_ms > 0
            """),
            {"day": day_str},
        ).fetchone()
    
    assert row is not None
    m = row._mapping
    assert m["trades"] == 1
    assert m["pnl_usd_sum"] == pytest.approx(0.1, abs=0.01)
    assert m["pnl_r_sum"] == pytest.approx(2.0, abs=0.01)
    assert m["fees_usd_sum"] == pytest.approx(0.0, abs=0.01)
    assert m["wins"] == 1
    assert m["losses"] == 0


def test_daily_stats_update(temp_db):
    """Test that close_trade updates daily_stats correctly."""
    from storage.repo import upsert_daily_stats
    
    # Create and close a trade
    ctx = TradeOpenCtx(
        symbol="ETHUSDT",
        side="LONG",
        qty=0.1,
        entry_px=3000.0,
        reason_open="test",
    )
    trade_id = open_trade(ctx)
    close_trade(
        trade_id=trade_id,
        exit_px=3010.0,
        r=1.5,
        pnl_usd=1.0,
        reason_close="test",
    )
    
    # Verify daily_stats was created/updated
    from datetime import datetime, timezone
    day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(
            text("SELECT trades, wins, losses, pnl_usd, pnl_r, max_dd_r FROM daily_stats WHERE day_utc = :d"),
            {"d": day_str},
        ).fetchone()
    
    assert row is not None
    assert row._mapping["trades"] == 1
    assert row._mapping["wins"] == 1
    assert row._mapping["losses"] == 0
    assert row._mapping["pnl_usd"] == pytest.approx(1.0, abs=0.01)
    assert row._mapping["pnl_r"] == pytest.approx(1.5, abs=0.01)

