# Critical Issues Analysis & Fix Plan

## Executive Summary

After comprehensive code review, I've identified **11 critical issues** that could cause:
- **Loss of money** (over-leveraging, missing stops, wrong position sizing)
- **System crashes** (missing initialization, race conditions)
- **Unprofitable trading** (wrong equity calculations, ignored daily limits)

## Critical Issues (Must Fix Before Live Trading)

### 1. ⚠️ CRITICAL: Missing `_stop_event` Initialization
**Location:** `bot/worker.py:926`
**Problem:** `_exchange_consistency_loop` uses `self._stop_event.is_set()` but `_stop_event` is never initialized
**Impact:** Will crash with `AttributeError` immediately, breaking position synchronization
**Fix:** Initialize `asyncio.Event()` in `__init__`

### 2. ⚠️ CRITICAL: Position Sizing Uses Simulated Equity
**Location:** `bot/worker.py:2248`, `bot/worker.py:4302`
**Problem:** Uses `_starting_equity_usd + _pnl_day["pnl_usd"]` instead of real Binance account balance
**Impact:** 
- Over-leveraging if account lost money
- Under-sizing if account gained money
- Risk calculations completely wrong
**Fix:** Integrate `AccountState` service to get real equity from Binance

### 3. ⚠️ CRITICAL: Daily Limits Use Local PnL, Not Real Account
**Location:** `risk/filters.py:192`, `bot/worker.py:1220`
**Problem:** `check_day_limits` uses `state.pnl_r_day` from local `_pnl_day`, not real Binance account
**Impact:** Could continue trading when account already hit daily stop, causing massive losses
**Fix:** Use `AccountState.realized_pnl_today` converted to R for daily limit checks

### 4. ⚠️ CRITICAL: No Margin Check Before Entry
**Location:** `bot/worker.py:place_entry`, `bot/worker.py:place_entry_auto`
**Problem:** No verification that `qty * price * leverage <= available_margin` before placing orders
**Impact:** Orders rejected by Binance, or worse - margin calls if calculation is wrong
**Fix:** Add margin check using `AccountState.available_margin` before entry

### 5. ⚠️ CRITICAL: SL/TP Fallback Without `reduce_only` Safety
**Location:** `exec/executor.py:1091-1122`
**Problem:** When `place_bracket` gets `-2022` error, fallback removes `reduce_only=True`, which could open new positions
**Impact:** Could accidentally double position size or open opposite position
**Fix:** Add safety check - if fallback is used, verify position size doesn't increase

## High Priority Issues

### 6. HIGH: No Verification SL/TP Orders Actually Placed
**Location:** `bot/worker.py:2131-2145`
**Problem:** After `place_bracket`, no check that orders actually exist on exchange
**Impact:** Position could be unprotected if order placement silently failed
**Fix:** Add `get_open_orders` check after `place_bracket` to verify orders exist

### 7. HIGH: Max Trades Per Day Not Enforced
**Location:** `bot/worker.py:_strategy_loop`, `risk/filters.py`
**Problem:** No check for `max_trades_per_day` limit anywhere in code
**Impact:** Could exceed exchange limits or strategy rules
**Fix:** Add check using `AccountState.num_trades_today` before entry

### 8. HIGH: Position Size Not Validated Against Margin
**Location:** `bot/worker.py:_compute_auto_qty`, `bot/worker.py:place_entry_auto`
**Problem:** Calculated qty doesn't verify it fits within available margin
**Impact:** Could calculate position that exceeds margin, causing rejections
**Fix:** Add validation: `qty * entry_px <= available_margin * leverage`

## Medium Priority Issues

### 9. MEDIUM: Race Condition in Position Synchronization
**Location:** `bot/worker.py:_exchange_consistency_loop`, `bot/worker.py:place_entry`
**Problem:** Both update `_pos[symbol]` asynchronously without proper coordination
**Impact:** Could lead to double entries or missed exits
**Fix:** Use same lock for both operations

### 10. MEDIUM: No Error Recovery for Failed SL/TP
**Location:** `bot/worker.py:2131-2145`
**Problem:** If `place_bracket` fails, position is left unprotected
**Impact:** Unprotected positions can lead to large losses
**Fix:** Add retry logic or alternative protection method

### 11. MEDIUM: Daily PnL Calculation Inaccurate
**Location:** `bot/worker.py:_register_trade`
**Problem:** Uses local `_pnl_day` which may drift from real account
**Impact:** Daily limits may not trigger correctly
**Fix:** Use `AccountState.realized_pnl_today` as source of truth

## Implementation Priority

1. **Fix #1** (crash) - Immediate
2. **Fix #2, #3, #4** (money safety) - Before any live trading
3. **Fix #5** (position safety) - Before any live trading
4. **Fix #6, #7, #8** (risk management) - High priority
5. **Fix #9, #10, #11** (reliability) - Medium priority

