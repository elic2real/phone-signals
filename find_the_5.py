#!/usr/bin/env python3
"""Find where the number 5 is coming from in JPY sizing."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    INSTR_META
)

# Monkey-patch to add logging
orig_calc_units = calc_units
def traced_calc_units(*args, **kwargs):
    print(f"\n[TRACE] calc_units called with:")
    print(f"  pair: {kwargs.get('pair', args[0] if args else 'N/A')}")
    print(f"  util: {kwargs.get('util', args[4] if len(args) > 4 else 'N/A')}")
    print(f"  nav_usd: {kwargs.get('nav_usd', args[8] if len(args) > 8 else 'N/A')}")
    
    result = orig_calc_units(*args, **kwargs)
    
    print(f"[TRACE] calc_units returning: {result[0]} units")
    if result[0] <= 10:
        print(f"[TRACE] *** UNITS IS {result[0]} - THIS IS THE PROBLEM! ***")
    
    return result

# Monkey-patch
import phone_bot
phone_bot.calc_units = traced_calc_units

# Setup metadata
INSTR_META["USD_JPY"] = {
    "minimumTradeSize": 1,
    "tradeUnitsPrecision": 0,
    "marginRate": 0.0333,
    "pipLocation": -2,
    "displayPrecision": 3
}

print("=" * 60)
print("FINDING THE SOURCE OF '5' UNITS")
print("=" * 60)

# Test with exact parameters
print("\nTesting with exact log parameters...")
units_total, units_reason, units_debug = calc_units(
    pair="USD_JPY",
    side="LONG",
    price=157.883,
    margin_avail=15126.764,
    util=0.15,
    speed_class="SLOW",
    spread_pips=1.8000000000000682,
    disp_atr=0.00010230769230767799,
    size_mult=1.0,
    sl_price=153.425,
    nav_usd=32271.0353,
    price_map={"USD_JPY": (157.946, 157.966)}
)

print(f"\nFinal result: {units_total} units")
print(f"Reason: {units_reason}")
print(f"Debug info: {units_debug}")

# Check if util as confidence is the issue
print("\n" + "=" * 60)
print("TESTING HYPOTHESIS: util=0.15 is being used as confidence")
print("=" * 60)

# Test compute_units_risk_2pct with confidence=0.15
result = compute_units_risk_2pct(
    pair="USD_JPY",
    side="LONG",
    entry_price=157.883,
    sl_price=153.425,
    nav_usd=32271.0353,
    spread_pips=1.8000000000000682,
    spread_mult=0.84,
    speed_class="SLOW",
    confidence=0.15,  # Using util as confidence
    price_map={"USD_JPY": (157.946, 157.966)}
)

print(f"\nWith confidence=0.15: {result['units_total']} units")

# Test with confidence=0.5 (hardcoded in calc_units)
result = compute_units_risk_2pct(
    pair="USD_JPY",
    side="LONG",
    entry_price=157.883,
    sl_price=153.425,
    nav_usd=32271.0353,
    spread_pips=1.8000000000000682,
    spread_mult=0.84,
    speed_class="SLOW",
    confidence=0.5,  # Hardcoded in calc_units
    price_map={"USD_JPY": (157.946, 157.966)}
)

print(f"With confidence=0.5: {result['units_total']} units")

print("\n" + "=" * 60)
print("LOOKING FOR OTHER SOURCES OF 5")
print("=" * 60)

# Check if there's a specific minimum somewhere
print("\nChecking for hardcoded minimums...")
print("- MIN_ATR_PIPS_EXEC_JPY = 0.05 (not 5)")
print("- minimumTradeSize = 1 (not 5)")
print("- tradeUnitsPrecision = 0 (not 5)")

# Check if it's related to setup_id
print("\nChecking setup_id=5 connection...")
print("setup_id=5 is for SWEEP_POP, but that shouldn't affect sizing")

# Check if it's a division issue
print("\nChecking division issues...")
print("If something is being divided by 1000 (lot size):")
print("12006 / 1000 = 12 (not 5)")
print("If something is being divided by 2400 (micro lots):")
print("12006 / 2400 = 5 (BINGO!)")

print("\n*** HYPOTHESIS: Something is dividing by 2400! ***")
print("12006 units / 2400 = 5.0025 ≈ 5")
print("\nWhere could 2400 come from?")
print("- Micro lot size (0.01 lot = 1000 units)")
print("- Nano lot size (0.001 lot = 100 units)")
print("- Some other lot conversion?")
