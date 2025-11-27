# Trading Bot SPEC

## 1. Goals

- Stable, predictable trading bot for Binance USDT-M futures.
- Clean separation:
  - Exchange sync & state tracking (positions, orders, PnL),
  - Risk/sizing,
  - Strategy (entry logic like Bollinger, later).
- No ML, no overcomplicated strategy logic for now.

---

## 2. Exchange Sync & State

### 2.1 Source of Truth

- The **single source of truth** for live positions is:
  - `adapter.get_positions()` → `/fapi/v2/positionRisk`.
- The source of truth for open protective orders is:
  - `adapter.get_open_orders(symbol=...)` → `/fapi/v1/openOrders`.

### 2.2 Local Position State (`Worker._pos`)

- For each symbol `sym` there is a `PositionState` object with fields:
  - `state` ∈ {"FLAT", "OPEN"}
  - `side` ∈ {"BUY", "SELL"} (direction on exchange)
  - `qty` > 0 when `state == "OPEN"`
  - `entry_px` > 0 when `state == "OPEN"`
  - `sl_px`, `tp_px` ∈ {None, >0}
  - `sl_order_id`, `tp_order_id` ∈ {None, str}
  - `trade_id` ∈ {None, str} — link to the current open trade in DB
- Invariants:
  - If `state == "FLAT"`:
    - `qty == 0`,
    - `side is None`,
    - `trade_id is None`,
    - `sl_px`, `tp_px`, `sl_order_id`, `tp_order_id` are None.
  - If `state == "OPEN"`:
    - `qty > 0`,
    - `entry_px > 0`,
    - `side` is "BUY" or "SELL",
    - `trade_id` is either None (for auto-synced positions) or a valid trade ID.

### 2.3 Sync Rules (`_sync_symbol_from_exchange`)

- On each sync for symbol `sym`:
  - Read all positions via `adapter.get_positions()`, find the row for `sym`.
  - Read all open orders via `adapter.get_open_orders(symbol=sym)`.

#### Case A: Exchange has NO position (flat)

- Condition: `pos_row is None` OR `abs(positionAmt) ≤ 1e-9`.
- If local `prev.state == "OPEN"` and `prev.qty > 0` and `prev.trade_id is not None`:
  - This is an **external/manual close**.
  - Compute `exit_px`:
    - Prefer mid price from `self.hub.best_bid_ask(sym)` if available,
    - Fallback to `entryPrice` from position row,
    - Fallback to `prev.entry_px`.
  - Call repository-level `close_trade` with:
    - `trade_id = prev.trade_id`,
    - `exit_px = exit_px`,
    - `reason_close = "external_close"`,
    - let repo compute PnL based on DB data.
  - Build a trade record via `_build_trade_record(...)` and feed it into `_register_trade(...)` to update in-memory stats.
- Regardless of whether there was an open trade:
  - Set `self._pos[sym]` to FLAT `PositionState` (all fields zero/None as per invariants).
  - Log `"[exchange_sync] {sym} -> FLAT (no position on exchange)"`.

#### Case B: Exchange HAS a position

- Condition: `abs(positionAmt) > 1e-9`.
- Derive:
  - `ext_amt = positionAmt`,
  - `entry_px = entryPrice`,
  - `side = "BUY"` if `ext_amt > 0` else `"SELL"`.
- Read open orders and find reduceOnly SL/TP:
  - `STOP_MARKET` → SL (`sl_px`, `sl_order_id`),
  - `TAKE_PROFIT_MARKET` or `LIMIT` with reduceOnly → TP (`tp_px`, `tp_order_id`).
- If local `prev.state == "FLAT"` or `prev is None`:
  - This is **auto-open** from exchange.
  - Optionally, create an `open_trade` entry in DB and attach `trade_id` to the new `PositionState` (or leave `trade_id=None` if we don't want to recreate DB trades).
- In all cases:
  - Set `self._pos[sym]` to `state="OPEN"` with:
    - `side`, `qty=abs(ext_amt)`, `entry_px`, `sl_px`, `tp_px`, `sl_order_id`, `tp_order_id`,
    - `trade_id` preserved from `prev` if it existed and is still relevant.

---

## 3. Trades, Orders, Fills, PnL

### 3.1 Trades Table

- `trades.side` ∈ {"LONG", "SHORT"} (position direction, not entry order side).
- Mapping:
  - For Binance:
    - `BUY` position side → `LONG`,
    - `SELL` position side → `SHORT`.

### 3.2 Orders and Fills

- `orders.trade_id` is NOT NULL for all orders belonging to a trade.
- `fills.fee_usd` stores commission in USDT-equivalent.

### 3.3 close_trade Logic

- Input: `trade_id`, `exit_px`, `r`, `pnl_usd`, `reason_close`.
- Steps:
  1. Load `entry_px`, `qty`, `side` from `trades`.
  2. Normalize side:
     - `"LONG"` → BUY,
     - `"SHORT"` → SELL.
  3. Compute `pnl_gross` from entry/exit/qty and direction.
  4. Sum `fee_usd` from all `fills` linked via `orders.trade_id`.
  5. Compute `pnl_net = pnl_gross - fee_sum_usd`.
  6. Update `trades` row with:
     - `exit_ts_ms`, `exit_px`, `r`, `pnl_usd = pnl_net`, `reason_close`.

---

## 4. Account Snapshot & uPnL

- `AccountState.refresh()`:
  - Calls adapter to fetch:
    - balance / equity,
    - positions (same endpoint as Worker),
  - Calls DB to fetch daily stats (realized PnL, number of trades).
- Unrealized PnL:
  - For each open position:
    - Use `entry_px`, `qty`, direction and current mid price,
    - Sum across symbols.
- The diagnostic API should:
  - Return:
    - `equity_now`,
    - `realized_pnl_today`,
    - `unrealized_pnl_now`,
    - number of open positions,
    - list of open trades (if available).

---

## 5. Risk & Sizing (simplified, for later)

- Each trade uses:
  - `margin_per_trade = 30 USDT`,
  - `leverage = 10x` (or 15x if configured).
- Notional target: `notional_target = margin_per_trade * leverage`.
- Compute `qty = notional_target / mid_price`, rounded down to `qty_step`.
- Risk check:
  - `risk_usd = sl_distance_px * qty`.
  - If `risk_usd > margin_per_trade` → skip trade (return qty=0).

---

## 6. Strategy (Bollinger, later)

- Single simple strategy module:
  - Uses Bollinger Bands (period 20, std 2) on 1m/5m candles.
  - LONG: close below lower band; SHORT: close above upper band.
  - SL: outside the band; TP: at middle/upper band; RR ≥ 2:1 where possible.
- No ML, no structural/complex pattern logic at this stage.
