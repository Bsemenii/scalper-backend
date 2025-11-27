# Fees API - Example Responses

## Before vs After Comparison

### `/trades` Endpoint

#### BEFORE (without fees_usd):
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
      "fees": 0.005
    }
  ],
  "count": 1
}
```

#### AFTER (with fees_usd):
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
      "fees_usd": 0.005,          // ← NEW: Same as fees, for consistency
      "pnl_source": "db",         // ← NEW: "db" or "local_estimate"
      "fees_estimated": false     // ← NEW: false when from DB, true when estimated
    }
  ],
  "count": 1
}
```

### `/pnl/now` Endpoint

#### BEFORE (without fees_usd in pnl_day):
```json
{
  "pnl_day": {
    "day": "2025-11-23",
    "trades": 15,
    "pnl_usd": 12.345,
    "pnl_r": 2.5,
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

#### AFTER (with fees_usd in pnl_day):
```json
{
  "pnl_day": {
    "day": "2025-11-23",
    "trades": 15,
    "pnl_usd": 12.345,
    "pnl_r": 2.5,
    "fees_usd": 0.876,          // ← NEW: Total fees for today
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

## Field Descriptions

### In `/trades` Response

| Field | Type | Description |
|-------|------|-------------|
| `fees` | float | Legacy fees field (kept for backward compatibility) |
| `fees_usd` | float | **NEW:** Actual fees in USD from DB, or estimated if not available |
| `pnl_source` | string | **NEW:** `"db"` if fees from database, `"local_estimate"` if calculated |
| `fees_estimated` | boolean | **NEW:** `false` if fees from DB, `true` if estimated from fee_bps |

### In `/pnl/now` Response

| Field | Type | Description |
|-------|------|-------------|
| `pnl_day.fees_usd` | float | **NEW:** Total fees (sum of all fills) for all trades closed today |

## Data Flow

### For Individual Trade Fees (`/trades`):

```
1. Trade closes
2. _build_trade_record() is called
3. If pos.trade_id exists:
   ├─ Query DB: SELECT SUM(f.fee_usd) FROM fills f JOIN orders o WHERE o.trade_id = ?
   ├─ If query succeeds: fees_usd = DB result, fees_estimated = false
   └─ If query fails: fees_usd = estimate, fees_estimated = true
4. Trade record includes fees_usd
5. /trades endpoint returns trade with fees_usd
```

### For Daily Total Fees (`/pnl/now`):

```
1. /pnl/now is called
2. Get current day_str from AccountState snapshot
3. Query DB: 
   SELECT SUM(f.fee_usd) 
   FROM fills f 
   JOIN orders o ON o.id = f.order_id
   JOIN trades t ON t.id = o.trade_id
   WHERE DATE(t.exit_ts_ms/1000, 'unixepoch') = ?
4. If query succeeds: fees_today = DB result
5. If query fails: fees_today = 0.0 (logged)
6. Response includes pnl_day.fees_usd
```

## SQL Queries Used

### Query 1: Per-Trade Fees (in `_build_trade_record`)

```sql
SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fee_sum
  FROM fills f
  JOIN orders o ON o.id = f.order_id
 WHERE o.trade_id = :tid
```

**Parameters:**
- `:tid` - trade_id from PositionState

**Returns:** Total fees for all fills associated with this trade

### Query 2: Daily Total Fees (in `/pnl/now`)

```sql
SELECT COALESCE(SUM(f.fee_usd), 0.0) AS fees_today
  FROM fills f
  JOIN orders o ON o.id = f.order_id
  JOIN trades t ON t.id = o.trade_id
 WHERE DATE(t.exit_ts_ms/1000, 'unixepoch') = :day
```

**Parameters:**
- `:day` - day string in format 'YYYY-MM-DD'

**Returns:** Total fees for all fills from trades closed on this day

## Error Handling Examples

### Scenario 1: Database Unavailable

**Request:** `GET /trades?limit=1`

**Response:** (gracefully degraded)
```json
{
  "items": [
    {
      "symbol": "BTCUSDT",
      "pnl_usd": 0.095,
      "fees_usd": 0.005,          // Falls back to estimate
      "fees_estimated": true,      // Indicates estimate was used
      "pnl_source": "local_estimate"
    }
  ],
  "count": 1
}
```

**Log Output:**
```
WARNING - [_build_trade_record] Failed to query fees for trade_id=abc123: connection refused. Falling back to estimate.
```

### Scenario 2: No Fills for Trade

**Request:** `GET /trades?limit=1`

**Response:**
```json
{
  "items": [
    {
      "symbol": "BTCUSDT",
      "pnl_usd": 0.095,
      "fees_usd": 0.0,            // COALESCE returns 0.0
      "fees_estimated": false,    // Query succeeded, just no data
      "pnl_source": "db"
    }
  ],
  "count": 1
}
```

### Scenario 3: DB Query Fails for Daily Fees

**Request:** `GET /pnl/now`

**Response:**
```json
{
  "pnl_day": {
    "day": "2025-11-23",
    "trades": 15,
    "pnl_usd": 12.345,
    "pnl_r": 2.5,
    "fees_usd": 0.0,             // Falls back to 0.0
    "winrate": null
  }
}
```

**Log Output:**
```
WARNING - [/pnl/now] Failed to query daily fees for 2025-11-23: table not found. Using 0.0
```

## Frontend Integration Tips

### Displaying Per-Trade Fees

```javascript
// Check if fees are estimated
function displayTradeFees(trade) {
  const fees = trade.fees_usd || 0;
  const isEstimated = trade.fees_estimated;
  
  return (
    <span>
      Fees: ${fees.toFixed(4)}
      {isEstimated && <span className="estimated-badge">~</span>}
    </span>
  );
}
```

### Displaying Daily Total Fees

```javascript
// Display total fees for the day
function displayDailyFees(pnlData) {
  const fees = pnlData.pnl_day.fees_usd || 0;
  const pnl = pnlData.pnl_day.pnl_usd || 0;
  const netPnl = pnl; // Already net of fees in our system
  
  return (
    <div>
      <div>Gross P&L: ${(netPnl + fees).toFixed(2)}</div>
      <div>Fees: -${fees.toFixed(2)}</div>
      <div>Net P&L: ${netPnl.toFixed(2)}</div>
    </div>
  );
}
```

### Calculating Fee Percentage

```javascript
// Show fees as percentage of volume
function calculateFeePercentage(trade) {
  const entryNotional = trade.entry_px * trade.qty;
  const exitNotional = trade.exit_px * trade.qty;
  const totalNotional = entryNotional + exitNotional;
  const fees = trade.fees_usd || 0;
  
  if (totalNotional === 0) return 0;
  
  const feePct = (fees / totalNotional) * 100;
  return feePct.toFixed(3); // e.g., "0.025%"
}
```

## Performance Impact

### Query Execution Time (Expected)

- **Per-trade fees query:** < 5ms (indexed on `order_id`, `trade_id`)
- **Daily total fees query:** < 20ms (indexed on `exit_ts_ms`, aggregates ~10-100 trades)

### Memory Impact

- **Per-trade:** +24 bytes (3 new fields: `fees_usd`, `pnl_source`, `fees_estimated`)
- **Per-request:** Negligible (single aggregate query)

### Caching Recommendations

- **`/trades` endpoint:** Consider caching closed trades (immutable)
- **`/pnl/now` endpoint:** Already designed for frequent polling, no additional caching needed
- **Daily fees:** Could cache per-day after market close (optional optimization)

