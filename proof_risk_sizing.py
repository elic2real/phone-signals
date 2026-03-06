#!/usr/bin/env python3
"""Proof script for 2% NAV risk-based sizing integration."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    extract_nav_from_account,
    get_instrument_meta_cached,
    INSTR_META,
    emit_trade_kind,
    normalize_pair,
    pip_size,
    to_pips,
    get_pip_value_usd,
    get_split_ratios,
    _ENABLE_MARGIN_SIZING
)
from typing import Dict, Tuple, Optional
import math


def test_calc_units_integration():
    """Test that calc_units properly integrates with risk-based sizing."""
    print("\n=== Testing calc_units Integration ===")
    
    # Mock broker metadata
    INSTR_META["EUR_USD"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,
        "pipLocation": -4,
        "displayPrecision": 5
    }
    
    # Mock price map
    price_map = {"EUR_USD": (1.1000, 1.1001)}
    
    # Test calc_units with required parameters
    units, reason, debug = calc_units(
        pair="EUR_USD",
        side="LONG",
        price=1.1000,
        margin_avail=5000.0,  # This is ignored in risk sizing
        util=0.1,  # This is ignored in risk sizing
        speed_class="MED",
        spread_pips=1.0,
        disp_atr=0.5,
        size_mult=1.0,
        sl_price=1.0900,  # Required for risk sizing
        nav_usd=10000.0,  # Required for risk sizing
        price_map=price_map
    )
    
    print(f"calc_units result:")
    print(f"  Units: {units}")
    print(f"  Reason: {reason}")
    print(f"  Debug keys: {list(debug.keys()) if debug else 'None'}")
    
    assert units > 0, "Should have positive units"
    assert reason == "success", f"Should succeed, got reason: {reason}"
    assert debug is not None, "Should have debug info"
    assert "risk_usd_actual" in debug, "Debug should contain risk info"
    
    # Verify risk is approximately 2% of NAV
    risk_actual = debug.get("risk_usd_actual", 0)
    expected_risk = 10000.0 * 0.02 * 0.625  # 2% * confidence multiplier
    assert abs(risk_actual - expected_risk) / expected_risk < 0.1, "Risk should be close to 2% of NAV"
    
    print("✓ calc_units integration test passed")


def test_margin_sizing_disabled():
    """Test that old margin-based sizing is disabled."""
    print("\n=== Testing Margin Sizing Disabled ===")
    
    assert not _ENABLE_MARGIN_SIZING, "Margin sizing should be disabled"
    
    # Try to use the old function directly
    from phone_bot import compute_units_recycling
    
    units_main, units_runner, debug = compute_units_recycling(
        pair="EUR_USD",
        direction="LONG",
        price=1.1000,
        margin_available=5000.0,
        margin_rate=0.0333,
        confidence=0.5,
        spread_mult=1.0,
        base_deploy_frac=0.1
    )
    
    print(f"compute_units_recycling result:")
    print(f"  Units main: {units_main}")
    print(f"  Units runner: {units_runner}")
    print(f"  Debug: {debug}")
    
    assert units_main == 0, "Should return 0 units when disabled"
    assert units_runner == 0, "Should return 0 units when disabled"
    assert debug.get("reason") == "margin_sizing_disabled", "Should indicate disabled"
    
    print("✓ Margin sizing is properly disabled")


def test_spread_gating():
    """Test that spread gating works in calc_units."""
    print("\n=== Testing Spread Gating ===")
    
    # Test with very wide spread
    units, reason, debug = calc_units(
        pair="EUR_USD",
        side="LONG",
        price=1.1000,
        margin_avail=5000.0,
        util=0.1,
        speed_class="MED",
        spread_pips=6.0,  # Very wide spread
        disp_atr=0.5,
        size_mult=1.0,
        sl_price=1.0900,
        nav_usd=10000.0,
        price_map={"EUR_USD": (1.1000, 1.1001)}
    )
    
    print(f"Wide spread result:")
    print(f"  Units: {units}")
    print(f"  Reason: {reason}")
    
    assert units == 0, "Should block on wide spread"
    assert reason == "spread_too_high", f"Should have spread reason, got: {reason}"
    
    print("✓ Spread gating works correctly")


def test_missing_sl_price():
    """Test that missing SL price is handled properly."""
    print("\n=== Testing Missing SL Price ===")
    
    # Test without SL price
    units, reason, debug = calc_units(
        pair="EUR_USD",
        side="LONG",
        price=1.1000,
        margin_avail=5000.0,
        util=0.1,
        speed_class="MED",
        spread_pips=1.0,
        disp_atr=0.5,
        size_mult=1.0,
        # sl_price omitted
        nav_usd=10000.0,
        price_map={"EUR_USD": (1.1000, 1.1001)}
    )
    
    print(f"Missing SL result:")
    print(f"  Units: {units}")
    print(f"  Reason: {reason}")
    
    assert units == 0, "Should block without SL price"
    assert reason == "missing_sl_price", f"Should have SL reason, got: {reason}"
    
    print("✓ Missing SL price handled correctly")


def test_size_calc_event_emitted():
    """Test that SIZE_CALC event is emitted."""
    print("\n=== Testing SIZE_CALC Event Emission ===")
    
    # Capture events
    events = []
    original_log = emit_trade_kind
    
    def capture_events(kind, payload=None):
        events.append((kind, payload))
        return original_log(kind, payload)
    
    # Temporarily replace emit_trade_kind
    import phone_bot
    phone_bot.emit_trade_kind = capture_events
    
    try:
        units, reason, debug = calc_units(
            pair="EUR_USD",
            side="LONG",
            price=1.1000,
            margin_avail=5000.0,
            util=0.1,
            speed_class="MED",
            spread_pips=1.0,
            disp_atr=0.5,
            size_mult=1.0,
            sl_price=1.0900,
            nav_usd=10000.0,
            price_map={"EUR_USD": (1.1000, 1.1001)}
        )
        
        # Check for SIZE_CALC event
        size_calc_events = [e for e in events if e[0] == "SIZE_CALC"]
        assert len(size_calc_events) > 0, "Should emit SIZE_CALC event"
        
        event_kind, event_payload = size_calc_events[0]
        print(f"SIZE_CALC event captured:")
        print(f"  Pair: {event_payload.get('pair')}")
        print(f"  Risk target: ${event_payload.get('risk_usd_target'):.2f}")
        print(f"  Risk actual: ${event_payload.get('risk_usd_actual'):.2f}")
        print(f"  Units total: {event_payload.get('units_total')}")
        
        assert event_payload.get("pair") == "EUR_USD", "Event should include pair"
        assert event_payload.get("risk_pct") == 0.02, "Event should include risk percentage"
        
    finally:
        # Restore original function
        phone_bot.emit_trade_kind = original_log
    
    print("✓ SIZE_CALC event emitted correctly")


def test_nav_extraction():
    """Test NAV extraction from account summary."""
    print("\n=== Testing NAV Extraction ===")
    
    # Mock account summary with nested structure
    acct_sum_nested = {
        "account": {
            "netAssetValue": "10500.50",
            "balance": "10500.50",
            "marginAvailable": "5000.00"
        },
        "lastTransactionID": "1234"
    }
    
    nav, source = extract_nav_from_account(acct_sum_nested)
    print(f"NAV from nested structure: ${nav}")
    print(f"Source: {source}")
    
    assert nav == 10500.50, f"Should extract NAV correctly, got {nav}"
    assert source == "nested_account_nav", f"Should identify source correctly, got {source}"
    
    # Mock account summary with top-level structure
    acct_sum_top = {
        "netAssetValue": "11000.00",
        "balance": "11000.00",
        "marginAvailable": "6000.00"
    }
    
    nav, source = extract_nav_from_account(acct_sum_top)
    print(f"NAV from top-level structure: ${nav}")
    print(f"Source: {source}")
    
    assert nav == 11000.00, f"Should extract NAV correctly, got {nav}"
    assert source == "top_level_nav", f"Should identify source correctly, got {source}"
    
    # Test fallback to balance
    acct_sum_balance = {
        "account": {
            "balance": "10750.25"
        }
    }
    
    nav, source = extract_nav_from_account(acct_sum_balance)
    print(f"NAV from balance fallback: ${nav}")
    print(f"Source: {source}")
    
    assert nav == 10750.25, f"Should fallback to balance, got {nav}"
    assert source == "nested_account_balance", f"Should use balance source, got {source}"
    
    print("✓ NAV extraction works correctly")


def main():
    """Run all proof tests."""
    print("=" * 60)
    print("RISK-BASED SIZING INTEGRATION PROOF")
    print("=" * 60)
    
    try:
        test_calc_units_integration()
        test_margin_sizing_disabled()
        test_spread_gating()
        test_missing_sl_price()
        test_size_calc_event_emitted()
        test_nav_extraction()
        
        print("\n" + "=" * 60)
        print("✅ ALL INTEGRATION TESTS PASSED")
        print("=" * 60)
        print("\nKey validations:")
        print("• calc_units uses risk-based sizing (2% NAV)")
        print("• Legacy margin-based sizing is disabled")
        print("• Spread gating works correctly")
        print("• SL price is required for risk sizing")
        print("• SIZE_CALC events are emitted for audit")
        print("• NAV extraction handles various account structures")
        
    except AssertionError as e:
        print(f"\n❌ INTEGRATION TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
