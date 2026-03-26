#!/usr/bin/env python3
"""Test broker minimum units logic."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import check_broker_min_units, INSTR_META

# Setup metadata
INSTR_META["USD_JPY"] = {
    "minimumTradeSize": 1,
    "tradeUnitsPrecision": 0,
    "marginRate": 0.0333,
    "pipLocation": -2,
    "displayPrecision": 3
}

print("Testing check_broker_min_units with various inputs:")
print()

# Test with small values
test_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

for units in test_values:
    final, reason, debug = check_broker_min_units("USD_JPY", units)
    print(f"Input: {units:2d} -> Output: {final:2d} (reason: {reason})")

print()
print("Checking if there's a specific condition that results in 5...")

# Let's also check if there's any rounding happening
print("\nChecking precision rounding:")
for units in [4.6, 4.7, 4.8, 4.9, 5.1, 5.2, 5.3, 5.4]:
    final, reason, debug = check_broker_min_units("USD_JPY", int(units))
    print(f"Input: {units:4.1f} -> Output: {final:2d} (reason: {reason})")
