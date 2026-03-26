#!/usr/bin/env python3
"""
Debug test to check why EUR_USD thursday london isn't loading
"""

import logging
logging.basicConfig(level=logging.ERROR)  # Only show errors

from compiled_trading_map import CompiledTradingMap
from state_key import compute_dow, compute_session, compute_quarter

# Create map
map = CompiledTradingMap()

# Check what we have
print(f"Total nodes loaded: {len(map._map)}")

# Check for EUR_USD thursday london specifically
for quarter in ["Q1", "Q2", "Q3", "Q4"]:
    key = ("EUR_USD", "thursday", "london", quarter)
    if key in map._map:
        print(f"✅ Found {key}")
        config = map._map[key]
        print(f"   Config type: {type(config)}")
        print(f"   Config keys: {list(config.keys())[:5]}")
    else:
        print(f"❌ Missing {key}")

# Check what days we have for EUR_USD london
print("\nEUR_USD London nodes:")
for key in sorted(map._map.keys()):
    if key[0] == "EUR_USD" and key[2] == "london":
        print(f"   {key}")

# Test state_key functions
from datetime import datetime, timezone
dt = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday
print(f"\nTest time: {dt}")
print(f"DOW: {compute_dow(dt)}")
print(f"Session: {compute_session(dt)}")
print(f"Quarter: {compute_quarter(dt)}")
