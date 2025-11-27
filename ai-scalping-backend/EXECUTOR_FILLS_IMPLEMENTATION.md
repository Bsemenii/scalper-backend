# Executor Fills Implementation - Summary

## Overview
This document describes the implementation of real execution price and fee tracking from Binance trades/fills, ensuring that all PnL calculations are based on actual exchange data stored in the database.

## Changes Made

### 1. Fixed Critical Bugs in `exec/executor.py`

#### Bug #1: Incorrect method call (line 775)
- **Issue**: Used `self._repo.save_fill()` instead of `self._repo_save_fill()`
- **Impact**: Would cause AttributeError during limit order partial fills
- **Fix**: Changed to use the correct wrapper method `self._repo_save_fill()`

#### Bug #2: Incorrect order_id reference (line 438, 496)
- **Issue**: Used `order_id = str(t.get("orderId") or client_order_id)` which would try to use exchange orderId
- **Impact**: Fills would be saved with incorrect order_id references
- **Fix**: Always use `client_order_id` to reference `orders.id` (clientOrderId)

### 2. Enhanced Fee Conversion in `exec/executor.py`

#### Added comprehensive comments
- Clarified that synthetic fees (based on bps) are **only used in paper/sim mode** when `_has_trades_api == False`
- For live Binance mode with `_has_trades_api == True`, **always use real data from Binance userTrades API**

#### Updated `_fetch_and_save_trades()` method
- Fetches real trades/fills from Binance userTrades API for each filled order
- Extracts actual execution price, quantity, and commission from each trade
- Converts commission from any asset (BNB, USDT, etc.) to USDT using current exchange rates
- Saves each fill to the database with:
  - `order_id`: clientOrderId (references `orders.id`)
  - `px`: actual execution price from Binance
  - `qty`: actual executed quantity from Binance
  - `fee_usd`: commission converted to USDT
  - `ts_ms`: trade timestamp from Binance

### 3. Added Missing `get_price()` Method to Adapters

#### In `adapters/binance_rest.py` - BinanceUSDTMAdapter
```python
async def get_price(self, symbol: str) -> Optional[float]:
    """
    GET /fapi/v1/ticker/price — текущая цена символа.
    Используется для конвертации комиссий из других активов в USDT.
    """
```
- Uses Binance Futures public endpoint `/fapi/v1/ticker/price`
- Returns current price for symbol (e.g., "BNBUSDT" → 600.0)
- Returns None if price lookup fails (with warning log)

#### In `adapters/binance_rest.py` - PaperAdapter
```python
async def get_price(self, symbol: str) -> Optional[float]:
    """
    Paper-версия get_price: возвращает примерную цену для конвертации комиссий.
    """
```
- Returns approximate prices for popular trading pairs
- Used in paper mode when Binance API is not available

### 4. Execution Flow for Fills

#### Entry Orders (`place_entry`)
1. Execute limit order (with optional market fallback)
2. If `_has_trades_api == True` (live Binance):
   - Call `_fetch_and_save_trades()` for limit order
   - Call `_fetch_and_save_trades()` for market order (if used)
   - Each method fetches real trades and saves fills with actual fees
3. If `_has_trades_api == False` (paper mode):
   - Use synthetic fees based on `fee_bps_maker` / `fee_bps_taker`

#### Exit Orders (`place_exit`)
- Calls `place_entry()` with `reduce_only=True`
- Inherits all fill-fetching logic from `place_entry()`
- Used for:
  - Manual flattens via API
  - SL/TP triggers detected by worker
  - Timeout exits
  - Any other exit scenarios

#### Bracket Orders (`place_bracket`)
- Creates SL/TP orders on the exchange
- **Does not fetch fills** at creation time (orders are not filled yet)
- When SL/TP triggers:
  - Worker detects trigger via `_price_touch_check()`
  - Worker calls `_close_position()`
  - `_close_position()` calls `place_exit()`
  - `place_exit()` fetches and saves fills

### 5. Database Integration

#### Repository Layer (`storage/repo.py`)
The executor uses these repository functions:
- `save_order()`: Saves order metadata (clientOrderId, symbol, side, type, status, etc.)
- `save_fill()`: Saves individual fill records (order_id, px, qty, fee_usd, ts_ms)

#### Fills Table Schema
```sql
CREATE TABLE fills (
    id          TEXT PRIMARY KEY,     -- UUID
    order_id    TEXT NOT NULL,        -- References orders.id (clientOrderId)
    ts_ms       INTEGER NOT NULL,     -- Fill timestamp from Binance
    px          REAL NOT NULL,        -- Execution price from Binance
    qty         REAL NOT NULL,        -- Executed quantity from Binance
    fee_usd     REAL NOT NULL,        -- Commission converted to USDT
    FOREIGN KEY(order_id) REFERENCES orders(id)
);
```

#### PnL Calculation (`close_trade` in `storage/repo.py`)
- Reads `entry_px`, `qty`, `side` from `trades` table
- Calculates gross PnL: `(exit_px - entry_px) * qty` for LONG, `(entry_px - exit_px) * qty` for SHORT
- Sums all `fee_usd` from `fills` joined with `orders` by `trade_id`
- Calculates net PnL: `pnl_net = pnl_gross - fee_sum_usd`
- Stores final `pnl_usd` in `trades` table

## Execution Modes

### Live Binance Mode
- `_has_trades_api = True`
- **Always uses real data from Binance userTrades API**
- Fetches actual execution prices, quantities, and fees
- Converts fees from any asset to USDT using current exchange rates
- No synthetic fee calculations

### Paper/Simulation Mode
- `_has_trades_api = False`
- Uses synthetic fees based on configured bps rates
- `fee_bps_maker` (default: 2.0 bps)
- `fee_bps_taker` (default: 4.0 bps)
- Useful for backtesting and paper trading

## Benefits

1. **Accurate PnL**: All PnL calculations use actual execution data from Binance
2. **Fee Transparency**: Real fees in actual assets, converted to USDT for consistent accounting
3. **Audit Trail**: Complete fill history in database for compliance and analysis
4. **No Estimation**: No need for synthetic fee calculations in live mode
5. **Database-Driven**: PnL computation happens in `close_trade()` using DB data, not in Executor

## Testing Recommendations

1. **Live Mode Testing**:
   - Verify fills are saved for entry orders (both limit and market)
   - Verify fills are saved for exit orders (manual, SL, TP, timeout)
   - Check fee conversion for different commission assets (BNB, USDT, etc.)
   - Verify PnL calculation in `close_trade()` matches actual P&L

2. **Paper Mode Testing**:
   - Verify synthetic fees are applied correctly
   - Check that fills are saved with calculated fees

3. **Edge Cases**:
   - Orders with multiple fills (partial executions)
   - Orders with zero fees (unlikely but possible)
   - Commission in exotic assets
   - Failed API calls (should not break execution flow)

## Future Enhancements (Optional)

1. **Bracket Order Monitoring**: Add periodic check for filled SL/TP orders that may have triggered on the exchange without worker detection
2. **Fill Deduplication**: Add logic to prevent duplicate fill records if same trade is fetched multiple times
3. **Historical Fill Sync**: Add tool to backfill fills for historical orders
4. **Fee Asset Cache**: Cache exchange rates for fee assets to reduce API calls

## Conclusion

All execution prices and fees now come from Binance trades/fills and are properly saved to the database. The `close_trade()` function can compute PnL purely from DB data without relying on any synthetic calculations in live mode.

