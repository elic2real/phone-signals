#!/usr/bin/env python3
"""Test script to trigger size tracing for JPY pairs."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    compute_units_risk_2pct,
    INSTR_META,
    emit_size_trace
)
from typing import Dict, Tuple, Optional


def test_jpy_size_trace():
    """Test size tracing with the same parameters from the logs."""
    print("\n=== Testing JPY Size Tracing ===")
    
    # Mock broker metadata for USD_JPY
    INSTR_META["USD_JPY"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,  # This is the fallback that's causing issues
        "pipLocation": -2,
        "displayPrecision": 3
    }
    
    # Parameters from the actual log
    params = {
        "pair": "USD_JPY",
        "side": "LONG",
        "entry_price": 157.883,
        "sl_price": 153.425,  # 445.8 pips away
        "nav_usd": 32271.0353,
        "spread_pips": 1.8,
        "spread_mult": 0.84,
        "speed_class": "SLOW",
        "confidence": 0.15,  # util=0.15 from the logs!
        "price_map": {"USD_JPY": (157.946, 157.966)}
    }
    
    print(f"\nTest parameters:")
    for k, v in params.items():
        print(f"  {k}: {v}")
    
    # Call the function
    result = compute_units_risk_2pct(**params)
    
    print(f"\nResults:")
    print(f"  Units total: {result['units_total']}")
    print(f"  Units main: {result['units_main']}")
    print(f"  Units runner: {result['units_runner']}")
    print(f"  Risk actual: ${result['risk_usd_actual']:.2f}")
    print(f"  Risk %: {(result['risk_usd_actual']/params['nav_usd'])*100:.3f}%")
    print(f"  Blocked: {result['blocked']}")
    if result['blocked']:
        print(f"  Block reason: {result['block_reason']}")
    
    # Check debug info
    debug = result.get('debug', {})
    if debug.get('meta_fallback'):
        print(f"  ⚠️ Used fallback metadata")
    if debug.get('margin_downscaled'):
        print(f"  ⚠️ Margin downscaled - max affordable: {debug.get('max_affordable_units')}")
    
    # Expected values
    expected_risk_target = params['nav_usd'] * 0.02
    print(f"\nExpected:")
    print(f"  Risk target (2%): ${expected_risk_target:.2f}")
    print(f"  Stop distance: ~445.8 pips")
    print(f"  Pip value: ~$0.00006334")
    print(f"  Expected units: ~22,857 (raw)")
    print(f"  Expected after margin: ~2,877")
    
    print(f"\n🔍 Check the logs for SIZE_TRACE events to see where the collapse occurs!")


def test_with_correct_margin_rate():
    """Test with correct 50:1 leverage margin rate."""
    print("\n=== Testing with Correct 50:1 Margin Rate ===")
    
    # Update mock metadata with correct margin rate for USD_JPY
    INSTR_META["USD_JPY"]["marginRate"] = 0.02  # 50:1 leverage
    
    params = {
        "pair": "USD_JPY",
        "side": "LONG",
        "entry_price": 157.883,
        "sl_price": 153.425,
        "nav_usd": 32271.0353,
        "spread_pips": 1.8,
        "spread_mult": 0.84,
        "speed_class": "SLOW",
        "confidence": None,
        "price_map": {"USD_JPY": (157.946, 157.966)}
    }
    
    result = compute_units_risk_2pct(**params)
    
    print(f"\nResults with 2% margin rate:")
    print(f"  Units total: {result['units_total']}")
    print(f"  Units main: {result['units_main']}")
    print(f"  Units runner: {result['units_runner']}")
    print(f"  Risk actual: ${result['risk_usd_actual']:.2f}")
    print(f"  Risk %: {(result['risk_usd_actual']/params['nav_usd'])*100:.3f}%")


def main():
    """Run size tracing tests."""
    print("=" * 60)
    print("JPY SIZE TRACING TEST")
    print("=" * 60)
    
    test_jpy_size_trace()
    test_with_correct_margin_rate()
    
    print("\n" + "=" * 60)
    print("✅ Size tracing tests complete")
    print("Check the logs for SIZE_TRACE events to analyze the pipeline")
    print("=" * 60)


if __name__ == "__main__":
    main()
