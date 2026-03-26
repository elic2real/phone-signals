#!/usr/bin/env python3
"""Test hypothesis: units are being divided by 2400 somewhere."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    INSTR_META
)

print("=" * 60)
print("TESTING HYPOTHESIS: DIVISION BY 2400")
print("=" * 60)

# Setup metadata
INSTR_META["USD_JPY"] = {
    "minimumTradeSize": 1,
    "tradeUnitsPrecision": 0,
    "marginRate": 0.0333,
    "pipLocation": -2,
    "displayPrecision": 3
}

# Test 1: Direct division by 2400
print("\n1. Testing direct division by 2400:")
expected_units = 12006
divided = expected_units / 2400
print(f"   {expected_units} / 2400 = {divided:.2f}")

# Test 2: Check if it's related to seconds
print("\n2. Time-based divisions:")
print(f"   Seconds in 40 minutes: {40 * 60}")
print(f"   Expected units / (40*60): {expected_units / (40*60):.2f}")
print(f"   Seconds in 1 hour: {3600}")
print(f"   Expected units / 3600: {expected_units / 3600:.2f}")

# Test 3: Check if it's related to pips
print("\n3. Pip-based divisions:")
print(f"   JPY pip size: 0.01")
print(f"   Expected units * 0.01: {expected_units * 0.01:.2f}")
print(f"   Expected units / 100: {expected_units / 100:.2f}")

# Test 4: Check if it's related to lot sizes
print("\n4. Lot size divisions:")
print(f"   Standard lot: 100,000 units")
print(f"   Expected units / 100000: {expected_units / 100000:.6f}")
print(f"   Mini lot: 10,000 units")
print(f"   Expected units / 10000: {expected_units / 10000:.2f}")
print(f"   Micro lot: 1,000 units")
print(f"   Expected units / 1000: {expected_units / 1000:.2f}")

# Test 5: Check if it's related to price
print("\n5. Price-based divisions:")
price = 157.883
print(f"   USD/JPY price: {price}")
print(f"   Expected units / price: {expected_units / price:.2f}")
print(f"   Expected units / (price*10): {expected_units / (price*10):.2f}")
print(f"   Expected units / (price*15): {expected_units / (price*15):.2f}")

# Test 6: Check if it's related to ATR
print("\n6. ATR-based divisions:")
atr_exec = 557.1428571428173
print(f"   ATR exec: {atr_exec}")
print(f"   Expected units / ATR: {expected_units / atr_exec:.2f}")
print(f"   Expected units / (ATR/100): {expected_units / (atr_exec/100):.2f}")

# Test 7: Check if it's related to spread
print("\n7. Spread-based divisions:")
spread_pips = 1.8000000000000682
print(f"   Spread pips: {spread_pips}")
print(f"   Expected units / spread: {expected_units / spread_pips:.2f}")

# Test 8: Check if it's related to margin
print("\n8. Margin-based divisions:")
margin_avail = 15126.764
print(f"   Margin available: {margin_avail}")
print(f"   Expected units / margin: {expected_units / margin_avail:.6f}")
print(f"   Margin / 1000: {margin_avail / 1000:.2f}")

# Test 9: Check specific combinations
print("\n9. Specific combinations:")
print(f"   (Expected units / 100) / 24: {(expected_units / 100) / 24:.2f}")
print(f"   (Expected units / 24) / 100: {(expected_units / 24) / 100:.2f}")
print(f"   Expected units / (24 * 100): {expected_units / (24 * 100):.2f}")

# Test 10: The actual calculation
print("\n10. Reverse engineering:")
target = 5
print(f"    To get {target} units from {expected_units}:")
print(f"    Divisor needed: {expected_units / target:.0f}")
print(f"    Closest round numbers: 2400, 2500, 2000")

# Test 11: Check if it's integer division
print("\n11. Integer division tests:")
print(f"    {expected_units} // 2400 = {expected_units // 2400}")
print(f"    {expected_units} // 2500 = {expected_units // 2500}")
print(f"    {expected_units} // 2000 = {expected_units // 2000}")

# Test 12: Check if it's modulo operation
print("\n12. Modulo operations:")
print(f"    {expected_units} % 2400 = {expected_units % 2400}")
print(f"    {expected_units} % 1000 = {expected_units % 1000}")

print("\n" + "=" * 60)
print("CONCLUSION")
print("=" * 60)
print("The most likely explanation is a division by 2400.")
print("This could be from:")
print("1. A time-based calculation (40 minutes * 60 seconds)")
print("2. A buffer size (PathBuffer uses 2400)")
print("3. Some other constant that equals 2400")
print("\nTo find this in production:")
print("1. Search for any code dividing by 2400")
print("2. Add logging before/after any division operations")
print("3. Check if there's a time window or buffer affecting sizing")
