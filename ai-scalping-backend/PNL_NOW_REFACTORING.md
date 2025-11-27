# `/pnl/now` Endpoint Refactoring

## Summary

The `/pnl/now` endpoint has been refactored to use `AccountState` as the **single source of truth** for PnL information, ensuring consistency and reliability across the application.

## Changes Made

### Backend (`bot/api/app.py`)

#### Before
- Manually computed PnL from `Worker.diag()`
- Calculated unrealized PnL by iterating positions if not available in diag
- Used multiple fallback mechanisms
- Silently handled errors with empty defaults

#### After
- **Uses `AccountState.refresh()` as single source of truth**
- Reads pre-computed values from AccountState snapshot:
  - `equity` / `balance` from adapter
  - `unrealized_pnl_now` computed using `adapter.get_positions()` + mid-prices from hub
  - `realized_pnl_today` and `num_trades_today` from `daily_stats` table
- **Errors are logged clearly and returned as HTTP 503**
- No silent error swallowing

### AccountState Flow (already implemented correctly)

#### `AccountState.refresh()` workflow:

1. **For Live Mode (`_refresh_live`)**:
   - Calls `adapter.get_positions()` to get position data
   - Uses `price_provider.best_bid_ask()` to get current mid-prices (or falls back to mark price)
   - Computes `unrealized_pnl` per position: `(current_px - entry_px) * position_amt`
   - Queries `daily_stats` table to get `realized_pnl_today` and `num_trades_today`
   - Returns complete `AccountSnapshot`

2. **For Paper Mode (`_refresh_paper`)**:
   - Same logic as live mode but uses paper adapter

3. **Error Handling**:
   - On success: updates snapshot, resets error count
   - On failure: increments error count, returns last known snapshot or empty snapshot
   - Errors logged with context

## New Response Structure

```json
{
  "pnl_day": {
    "day": "2025-11-23",
    "trades": 5,
    "pnl_usd": 42.50,
    "pnl_r": 2.83,
    "winrate": null,      // Can be computed from trade history if needed
    "avg_r": null,        // Can be computed from trade history if needed
    "max_dd_r": null      // Can be computed from trade history if needed
  },
  "unrealized": {
    "total_usd": 12.34,
    "per_symbol": {
      "BTCUSDT": 8.50,
      "ETHUSDT": 3.84
    }
  },
  "equity": 1050.00,
  "available_margin": 980.50,
  "open_positions": 2,
  "meta": {
    "snapshot_ts": "2025-11-23T14:30:45.123456Z",
    "snapshot_age_ms": 250,
    "consecutive_errors": 0
  }
}
```

### New Fields (backwards compatible)

- `equity`: Total wallet balance in USDT
- `available_margin`: Free balance available for trading
- `meta.snapshot_ts`: Timestamp when snapshot was taken
- `meta.snapshot_age_ms`: Age of snapshot in milliseconds
- `meta.consecutive_errors`: Number of consecutive refresh errors (0 = healthy)

## Frontend Compatibility

✅ **Fully backwards compatible** - the frontend (`PaperPage.jsx`) uses:
- `pnl_day.day`, `pnl_day.pnl_usd`, `pnl_day.pnl_r`, `pnl_day.trades`
- `pnl_day.winrate`, `pnl_day.avg_r`, `pnl_day.max_dd_r`
- `unrealized.total_usd`
- `open_positions`

All these fields are still present in the new response structure.

## Error Handling Improvements

### Before
```python
try:
    d = w.diag() or {}
except Exception:
    d = {}  # Silent failure
```

### After
```python
try:
    snap = await acc_state.refresh()
except Exception as e:
    log.error(f"[/pnl/now] AccountState.refresh() failed: {e}", exc_info=True)
    raise HTTPException(
        status_code=503,
        detail=f"Failed to refresh account state: {e}"
    )
```

**Benefits**:
- Errors are logged with full traceback
- Client receives meaningful HTTP 503 error
- Frontend can display appropriate error message
- Debugging is much easier

## Data Flow

```
Frontend → GET /pnl/now
                ↓
        app.py → AccountState.refresh()
                       ↓
              ┌────────┴────────┐
              ↓                 ↓
        adapter.get_positions()  daily_stats table
        + hub.best_bid_ask()    (realized PnL)
              ↓                 ↓
        unrealized_pnl_now   realized_pnl_today
              ↓                 ↓
         AccountSnapshot ←──────┘
                ↓
         Response JSON → Frontend
```

## Testing Recommendations

1. **Test with AccountState not configured**:
   ```bash
   curl http://localhost:8000/pnl/now
   # Should return 503 with clear error message
   ```

2. **Test with healthy AccountState**:
   ```bash
   curl http://localhost:8000/pnl/now
   # Should return full PnL snapshot with meta.consecutive_errors = 0
   ```

3. **Test with degraded state** (simulate by causing DB or adapter errors):
   - Should see `meta.consecutive_errors > 0`
   - Should see warning in logs
   - Should still return last known snapshot

4. **Frontend integration**:
   - Existing PaperPage should work without changes
   - New fields (`equity`, `available_margin`, `meta`) are optional for frontend

## Benefits

1. **Single Source of Truth**: All PnL data comes from one place (AccountState)
2. **No Manual Calculations**: PnL is computed once in AccountState, not recalculated in endpoint
3. **Better Error Handling**: Errors are logged and surfaced, not hidden
4. **More Information**: Additional fields (equity, margin, metadata) available for frontend
5. **Consistency**: Same calculation logic used everywhere
6. **Testability**: Easier to test and debug PnL calculations

## Migration Notes

- ✅ No frontend changes required
- ✅ No database schema changes required
- ✅ Backwards compatible response structure
- ⚠️ Requires `AccountState` to be properly initialized in Worker
- ⚠️ Requires `daily_stats` table to exist (already created in schema)

## Related Endpoints

Other endpoints still use `Worker.diag()` for diagnostic/telemetry purposes:
- `/debug/diag` - Full diagnostic snapshot
- `/demo/digest` - Compact dashboard digest
- `/metrics` - Lightweight metrics
- `/pnl/daily` - Daily PnL summary
- `/stream/sse` - Real-time SSE stream

These endpoints serve different purposes (diagnostics, monitoring) and don't need to be changed. The `/pnl/now` endpoint is specifically the **frontend's primary PnL data source**.

