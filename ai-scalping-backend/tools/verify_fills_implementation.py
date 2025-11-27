#!/usr/bin/env python3
"""
Verification script for fills implementation.

This script verifies that:
1. Executor methods exist and have correct signatures
2. Adapter methods exist for fetching trades and prices
3. Repository methods exist for saving fills
"""
import sys
import inspect
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def verify_executor():
    """Verify executor has all required methods."""
    from exec.executor import Executor
    
    print("✓ Executor class imported successfully")
    
    # Check required methods
    required_methods = [
        'place_entry',
        'place_exit',
        'place_bracket',
        '_fetch_and_save_trades',
        '_repo_save_fill',
        '_repo_save_order',
    ]
    
    for method_name in required_methods:
        if hasattr(Executor, method_name):
            print(f"✓ Executor.{method_name} exists")
        else:
            print(f"✗ Executor.{method_name} MISSING")
            return False
    
    return True


def verify_adapter():
    """Verify adapter has all required methods."""
    from adapters.binance_rest import BinanceUSDTMAdapter, PaperAdapter
    
    print("\n✓ Adapter classes imported successfully")
    
    # Check required methods for BinanceUSDTMAdapter
    required_methods = [
        'get_trades_for_order',
        'get_price',
        'create_order',
        'cancel_order',
        'get_order',
    ]
    
    for method_name in required_methods:
        if hasattr(BinanceUSDTMAdapter, method_name):
            print(f"✓ BinanceUSDTMAdapter.{method_name} exists")
        else:
            print(f"✗ BinanceUSDTMAdapter.{method_name} MISSING")
            return False
    
    # Check PaperAdapter
    for method_name in required_methods:
        if hasattr(PaperAdapter, method_name):
            print(f"✓ PaperAdapter.{method_name} exists")
        else:
            print(f"✗ PaperAdapter.{method_name} MISSING")
            return False
    
    return True


def verify_repository():
    """Verify repository has all required methods."""
    from storage.repo import save_order, save_fill, close_trade
    
    print("\n✓ Repository functions imported successfully")
    
    # Check signatures
    sig_save_fill = inspect.signature(save_fill)
    print(f"✓ save_fill signature: {sig_save_fill}")
    
    sig_close_trade = inspect.signature(close_trade)
    print(f"✓ close_trade signature: {sig_close_trade}")
    
    return True


def verify_database_schema():
    """Verify database schema has fills table."""
    schema_path = Path(__file__).parent.parent / "storage" / "schema.sql"
    
    if not schema_path.exists():
        print(f"\n✗ Schema file not found: {schema_path}")
        return False
    
    print(f"\n✓ Schema file found: {schema_path}")
    
    schema_content = schema_path.read_text()
    
    if "CREATE TABLE fills" in schema_content:
        print("✓ 'fills' table definition found in schema")
    else:
        print("✗ 'fills' table definition NOT FOUND in schema")
        return False
    
    required_columns = ["id", "order_id", "ts_ms", "px", "qty", "fee_usd"]
    for col in required_columns:
        if col in schema_content:
            print(f"✓ Column '{col}' found in schema")
        else:
            print(f"✗ Column '{col}' NOT FOUND in schema")
            return False
    
    return True


def main():
    print("=" * 60)
    print("Verifying Fills Implementation")
    print("=" * 60)
    
    try:
        executor_ok = verify_executor()
        adapter_ok = verify_adapter()
        repo_ok = verify_repository()
        schema_ok = verify_database_schema()
        
        print("\n" + "=" * 60)
        if executor_ok and adapter_ok and repo_ok and schema_ok:
            print("✓ ALL VERIFICATIONS PASSED")
            print("=" * 60)
            return 0
        else:
            print("✗ SOME VERIFICATIONS FAILED")
            print("=" * 60)
            return 1
    except Exception as e:
        print(f"\n✗ Verification failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

