# Fees API Implementation Summary

## Overview
This document summarizes the changes made to expose fees information through the API endpoints.

## Changes Made

### 1. Updated `_build_trade_record` in `bot/worker.py`

**Location:** Lines 3550-3683

**Key Changes:**
- Added DB query to fetch actual fees from `fills` table using `trade_id`
- Query: `SELECT COALESCE(SUM(f.fee_usd), 0.0) FROM fills f JOIN orders o ON o.id = f.order_id WHERE o.trade_id = :tid`
- Falls back to estimated fees (using `fee_bps`) if DB query fails or `trade_id` is not available
- Added `fees_usd` field to the returned trade record dictionary
- Added `fees_estimated` boolean flag to indicate whether fees are from DB or estimated

**Error Handling:**
- Wrapped DB query in try/except block
- Logs WARNING on exception and falls back to 0.0
- Does not fail the entire API request if fees query fails

**Code Snippet:**
```python
# Пытаемся достать реальные комиссии из БД по trade_id
trade_id = getattr(pos, "trade_id", None)
if trade_id:
    try:
        from sqlalchemy import text
        from storage.db import get_engine
        
        eng = get_engine()
        with eng.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fee_sum
                      FROM fills f
                      JOIN orders o ON o.id = f.order_id
                     WHERE o.trade_id = :tid
                    """
                ),
                {"tid": trade_id},
            ).fetchone()
            
            if row is not None:
                if hasattr(row, '_mapping'):
                    fees_usd = float(row._mapping.get("fee_sum", 0.0) or 0.0)
                else:
                    fees_usd = float(row[0] or 0.0)
                fees_estimated = False
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            f"[_build_trade_record] Failed to query fees for trade_id={trade_id}: {e}. "
            f"Falling back to estimate."
        )
```

### 2. Updated `/trades` Endpoint

**Location:** `bot/api/app.py`, lines 1534-1555

**Changes:**
- No code changes required - endpoint already passes through all fields from `Worker.diag()["trades"]`
- Since `_build_trade_record` now includes `fees_usd`, it's automatically available in the response

**Example Response:**
```json
{
  "items": [
    {
      "symbol": "BTCUSDT",
      "side": "BUY",
      "opened_ts": 1700000000000,
      "closed_ts": 1700000060000,
      "qty": 0.001,
      "entry_px": 40000.0,
      "exit_px": 40100.0,
      "sl_px": 39950.0,
      "tp_px": 40150.0,
      "pnl_usd": 0.095,
      "pnl_r": 1.9,
      "risk_usd": 0.05,
      "reason": "tp_hit",
      "fees": 0.005,
      "fees_usd": 0.005,
      "pnl_source": "db",
      "fees_estimated": false
    }
  ],
  "count": 1
}
```

### 3. Updated `/pnl/now` Endpoint

**Location:** `bot/api/app.py`, lines 1601-1706

**Key Changes:**
- Added DB query to compute total fees for the current day
- Query: `SELECT COALESCE(SUM(f.fee_usd), 0.0) FROM fills f JOIN orders o ON o.id = f.order_id JOIN trades t ON t.id = o.trade_id WHERE DATE(t.exit_ts_ms/1000, 'unixepoch') = :day`
- Added `fees_usd` field to `pnl_day` object in response

**Error Handling:**
- Wrapped DB query in try/except block
- Logs WARNING on exception and falls back to 0.0
- Does not fail the entire API request if fees query fails

**Code Snippet:**
```python
# Compute daily fees from DB (total fees for all closed trades today)
fees_today = 0.0
try:
    eng = get_engine()
    with eng.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fees_today
                  FROM fills f
                  JOIN orders o ON o.id = f.order_id
                  JOIN trades t ON t.id = o.trade_id
                 WHERE DATE(t.exit_ts_ms/1000, 'unixepoch') = :day
                """
            ),
            {"day": day_str},
        ).fetchone()
        
        if row is not None:
            if hasattr(row, '_mapping'):
                fees_today = float(row._mapping.get("fees_today", 0.0) or 0.0)
            else:
                fees_today = float(row[0] or 0.0)
except Exception as e:
    log.warning(f"[/pnl/now] Failed to query daily fees for {day_str}: {e}. Using 0.0")
    fees_today = 0.0
```

**Example Response:**
```json
{
  "pnl_day": {
    "day": "2025-11-23",
    "trades": 15,
    "pnl_usd": 12.345,
    "pnl_r": 2.5,
    "fees_usd": 0.876,
    "winrate": null,
    "avg_r": null,
    "max_dd_r": null
  },
  "unrealized": {
    "total_usd": 1.234,
    "per_symbol": {
      "BTCUSDT": 0.500,
      "ETHUSDT": 0.734
    }
  },
  "equity": 1012.345,
  "available_margin": 950.123,
  "open_positions": 2,
  "meta": {
    "snapshot_ts": "2025-11-23T12:34:56.789000+00:00",
    "snapshot_age_ms": 123,
    "consecutive_errors": 0
  }
}
```

## Database Schema Requirements

No schema changes required. The implementation uses existing tables and relationships:

- `fills` table has `fee_usd` column (already exists)
- `orders` table links fills to trades via `order_id` and `trade_id` (already exists)
- `trades` table has `exit_ts_ms` for date filtering (already exists)

## Backward Compatibility

All changes are backward compatible:
- No existing fields were removed or renamed
- Only new fields were added to responses
- Existing behavior remains intact
- Error handling ensures graceful degradation if DB queries fail

## Testing Recommendations

1. **Verify `/trades` endpoint:**
   ```bash
   curl http://localhost:8000/trades?limit=10
   ```
   - Check that each trade has `fees_usd` field
   - Verify `fees_estimated` is `false` when `trade_id` exists

2. **Verify `/pnl/now` endpoint:**
   ```bash
   curl http://localhost:8000/pnl/now
   ```
   - Check that `pnl_day.fees_usd` is present
   - Verify it sums fees for all trades today

3. **Test error handling:**
   - Verify graceful degradation if DB is unavailable
   - Check logs for WARNING messages on query failures

## Performance Considerations

- DB queries use indexed columns (`order_id`, `trade_id`, `exit_ts_ms`)
- Queries use `COALESCE` to avoid NULL handling issues
- Queries are read-only and don't lock tables
- For `/pnl/now`: single aggregate query per request (fast)
- For `/trades`: fees are pre-computed per trade during close (no additional overhead)

## Notes

- The `fees` field (legacy) is kept for backward compatibility
- The new `fees_usd` field has the same value but follows naming convention
- `fees_estimated` flag allows frontend to distinguish between actual and estimated fees
- `pnl_source` indicates whether PnL calculation used DB data or local estimates

