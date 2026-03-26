#!/usr/bin/env python3
"""Test script to verify JPY pip value calculations work correctly."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    compute_units_risk_2pct,
    get_pip_value_usd,
    pip_size,
    normalize_pair,
    INSTR_META,
    to_pips
)
from typing import Dict, Tuple, Optional
import math


def test_jpy_pip_value():
    """Test pip value calculation for JPY pairs."""
    print("\n=== Testing JPY Pip Value Calculations ===")
    
    # Mock broker metadata for JPY pairs
    INSTR_META["USD_JPY"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,
        "pipLocation": -2,  # For JPY pairs, 0.01 = 10^-2
        "displayPrecision": 3
    }
    
    INSTR_META["AUD_JPY"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,
        "pipLocation": -2,  # For JPY pairs, 0.01 = 10^-2
        "displayPrecision": 3
    }
    
    # Test 1: USD/JPY direct calculation
    print("\n1. USD/JPY Pip Value Test:")
    print("   Given: USD/JPY = 150.00")
    print("   Expected: 1 pip (0.01 JPY) = $0.00006667")
    
    price_map = {"USD_JPY": (150.00, 150.01)}
    pip_value = get_pip_value_usd("USD_JPY", price_map)
    expected = 0.01 / 150.00  # 0.01 JPY divided by JPY per USD
    
    print(f"   Calculated pip value: ${pip_value:.8f}")
    print(f"   Expected pip value:  ${expected:.8f}")
    print(f"   Difference: {abs(pip_value - expected):.10f}")
    
    assert abs(pip_value - expected) < 1e-8, f"USD/JPY pip value incorrect: {pip_value} != {expected}"
    print("   ✓ PASS")
    
    # Test 2: AUD/JPY cross pair calculation
    print("\n2. AUD/JPY Pip Value Test:")
    print("   Given: AUD/JPY = 85.00, USD/JPY = 150.00")
    print("   Expected: 1 pip (0.01 JPY) = $0.00006667")
    
    price_map = {
        "AUD_JPY": (85.00, 85.01),
        "USD_JPY": (150.00, 150.01)
    }
    pip_value = get_pip_value_usd("AUD_JPY", price_map)
    expected = 0.01 / 150.00  # Convert JPY to USD via USD/JPY
    
    print(f"   Calculated pip value: ${pip_value:.8f}")
    print(f"   Expected pip value:  ${expected:.8f}")
    print(f"   Difference: {abs(pip_value - expected):.10f}")
    
    assert abs(pip_value - expected) < 1e-8, f"AUD/JPY pip value incorrect: {pip_value} != {expected}"
    print("   ✓ PASS")
    
    # Test 3: Verify pip size for JPY pairs
    print("\n3. JPY Pip Size Test:")
    usd_jpy_pip = pip_size("USD_JPY")
    aud_jpy_pip = pip_size("AUD_JPY")
    
    print(f"   USD/JPY pip size: {usd_jpy_pip}")
    print(f"   AUD/JPY pip size: {aud_jpy_pip}")
    print(f"   Expected: 0.01")
    
    assert float(usd_jpy_pip) == 0.01, f"USD/JPY pip size wrong: {usd_jpy_pip}"
    assert float(aud_jpy_pip) == 0.01, f"AUD/JPY pip size wrong: {aud_jpy_pip}"
    print("   ✓ PASS")


def test_jpy_risk_sizing():
    """Test complete risk sizing calculation for JPY pairs."""
    print("\n=== Testing JPY Risk Sizing Calculations ===")
    
    # Test Case 1: USD/JPY
    print("\nTest Case 1: USD/JPY LONG")
    print("  NAV: $50,000")
    print("  Entry: 150.00")
    print("  SL: 148.50 (150 pips)")
    print("  Expected risk: $1,000 (2% of NAV)")
    
    nav = 50000.0
    result = compute_units_risk_2pct(
        pair="USD_JPY",
        side="LONG",
        entry_price=150.00,
        sl_price=148.50,
        nav_usd=nav,
        spread_pips=0.5,
        spread_mult=1.0,
        speed_class="MED",
        confidence=1.0,  # No confidence reduction
        price_map={"USD_JPY": (150.00, 150.01)}
    )
    
    print(f"\n  Results:")
    print(f"    Stop distance: {result['stop_dist_pips']} pips")
    print(f"    Pip value: ${result['pip_value_usd']:.8f}")
    print(f"    Units total: {result['units_total']}")
    print(f"    Risk actual: ${result['risk_usd_actual']:.2f}")
    print(f"    Risk %: {(result['risk_usd_actual']/nav)*100:.3f}%")
    
    # Verify calculations
    expected_stop_dist = 150.0  # 150 pips
    expected_pip_value = 0.01 / 150.00  # $0.00006667
    expected_units = (nav * 0.02) / (expected_stop_dist * expected_pip_value)
    expected_risk = expected_units * expected_stop_dist * expected_pip_value
    
    print(f"\n  Verification:")
    print(f"    Expected stop distance: {expected_stop_dist} pips")
    print(f"    Expected pip value: ${expected_pip_value:.8f}")
    print(f"    Expected units: {expected_units:.0f}")
    print(f"    Expected risk: ${expected_risk:.2f}")
    
    assert result['stop_dist_pips'] == expected_stop_dist, f"Stop distance wrong: {result['stop_dist_pips']}"
    assert abs(result['pip_value_usd'] - expected_pip_value) < 1e-8, f"Pip value wrong: {result['pip_value_usd']}"
    assert abs(result['risk_usd_actual'] - expected_risk) < 1.0, f"Risk wrong: {result['risk_usd_actual']}"
    print("  ✓ PASS")
    
    # Test Case 2: AUD/JPY with confidence
    print("\nTest Case 2: AUD/JPY SHORT with Confidence=0.5")
    print("  NAV: $25,000")
    print("  Entry: 85.00")
    print("  SL: 86.50 (150 pips)")
    print("  Confidence: 0.5 (multiplier = 0.625)")
    print("  Spread multiplier: 0.95")
    print("  Expected risk: $296.87 (2% × 0.625 × 0.95)")
    
    nav = 25000.0
    result = compute_units_risk_2pct(
        pair="AUD_JPY",
        side="SHORT",
        entry_price=85.00,
        sl_price=86.50,
        nav_usd=nav,
        spread_pips=1.0,
        spread_mult=0.95,
        speed_class="FAST",
        confidence=0.5,
        price_map={
            "AUD_JPY": (85.00, 85.01),
            "USD_JPY": (150.00, 150.01)
        }
    )
    
    print(f"\n  Results:")
    print(f"    Stop distance: {result['stop_dist_pips']} pips")
    print(f"    Pip value: ${result['pip_value_usd']:.8f}")
    print(f"    Units total: {result['units_total']}")
    print(f"    Units main: {result['units_main']}")
    print(f"    Units runner: {result['units_runner']}")
    print(f"    Risk actual: ${result['risk_usd_actual']:.2f}")
    print(f"    Risk %: {(result['risk_usd_actual']/nav)*100:.3f}%")
    
    # Verify confidence reduction
    base_risk = nav * 0.02  # $500
    confidence_mult = 0.25 + 0.75 * 0.5  # 0.625
    spread_mult = 0.95
    expected_risk = base_risk * confidence_mult * spread_mult  # $500 × 0.625 × 0.95 = $296.87
    
    print(f"\n  Verification:")
    print(f"    Base risk (2%): ${base_risk:.2f}")
    print(f"    Confidence multiplier: 0.625")
    print(f"    Expected risk: ${expected_risk:.2f}")
    
    assert abs(result['risk_usd_actual'] - expected_risk) < 5.0, f"Risk wrong: {result['risk_usd_actual']}"
    assert result['units_main'] == int(result['units_total'] * 0.85), f"Main split wrong for FAST"
    assert result['units_runner'] == result['units_total'] - result['units_main'], f"Runner split wrong"
    print("  ✓ PASS")


def test_manual_calculation():
    """Manually verify the JPY calculation step by step."""
    print("\n=== Manual Calculation Verification ===")
    
    print("\nScenario: USD/JPY")
    print("  Account: $100,000 NAV")
    print("  Trade: Long at 150.00, SL at 147.00")
    print("  Stop distance: 300 pips")
    
    # Step 1: Calculate risk target
    nav = 100000.0
    risk_target = nav * 0.02
    print(f"\n  Step 1 - Risk Target:")
    print(f"    2% of ${nav:,.0f} = ${risk_target:,.2f}")
    
    # Step 2: Get pip value
    usd_jpy_rate = 150.00
    pip_value_jpy = 0.01  # JPY pip size
    pip_value_usd = pip_value_jpy / usd_jpy_rate
    print(f"\n  Step 2 - Pip Value:")
    print(f"    1 pip = 0.01 JPY")
    print(f"    USD/JPY = {usd_jpy_rate}")
    print(f"    Pip value in USD = 0.01 / {usd_jpy_rate} = ${pip_value_usd:.8f}")
    
    # Step 3: Calculate risk per unit
    stop_pips = 300.0
    risk_per_unit = stop_pips * pip_value_usd
    print(f"\n  Step 3 - Risk per Unit:")
    print(f"    {stop_pips} pips × ${pip_value_usd:.8f} = ${risk_per_unit:.6f} per unit")
    
    # Step 4: Calculate units
    units = risk_target / risk_per_unit
    print(f"\n  Step 4 - Calculate Units:")
    print(f"    ${risk_target:,.2f} / ${risk_per_unit:.6f} = {units:,.0f} units")
    
    # Step 5: Verify
    actual_risk = units * stop_pips * pip_value_usd
    print(f"\n  Step 5 - Verification:")
    print(f"    {units:,.0f} units × {stop_pips} pips × ${pip_value_usd:.8f} = ${actual_risk:,.2f}")
    print(f"    Risk % = ${(actual_risk/nav)*100:.3f}%")
    
    # Compare with function
    result = compute_units_risk_2pct(
        pair="USD_JPY",
        side="LONG",
        entry_price=150.00,
        sl_price=147.00,
        nav_usd=nav,
        spread_pips=0.5,
        spread_mult=1.0,
        speed_class="MED",
        confidence=1.0,
        price_map={"USD_JPY": (150.00, 150.01)}
    )
    
    print(f"\n  Function Result:")
    print(f"    Units: {result['units_total']}")
    print(f"    Risk: ${result['risk_usd_actual']:.2f}")
    
    assert abs(units - result['units_total']) < 10, f"Units mismatch: {units} != {result['units_total']}"
    assert abs(actual_risk - result['risk_usd_actual']) < 1.0, f"Risk mismatch: {actual_risk} != {result['risk_usd_actual']}"
    print("\n  ✓ Manual calculation matches function result!")


def main():
    """Run all JPY calculation tests."""
    print("=" * 60)
    print("JPY CALCULATION VERIFICATION")
    print("=" * 60)
    
    try:
        test_jpy_pip_value()
        test_jpy_risk_sizing()
        test_manual_calculation()
        
        print("\n" + "=" * 60)
        print("✅ ALL JPY CALCULATION TESTS PASSED")
        print("=" * 60)
        print("\nKey findings:")
        print("• JPY pip size correctly set to 0.01")
        print("• USD/JPY pip value: $0.00006667 at 150.00 rate")
        print("• AUD/JPY pip value: Same as USD/JPY (uses USD/JPY for conversion)")
        print("• Risk sizing works correctly for all JPY pairs")
        print("• Confidence and spread multipliers applied correctly")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
