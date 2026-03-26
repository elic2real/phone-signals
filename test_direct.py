#!/usr/bin/env python3
"""
Direct test of the mapping
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from compiled_trading_map import CompiledTradingMap
from state_key import compute_dow, compute_session, compute_quarter
from datetime import datetime, timezone

# Create map
cal_map = CompiledTradingMap()

# Test the mapping directly
dt = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday 11:00 UTC
ts = dt.timestamp()

print(f"Time: {dt}")
print(f"compute_dow: {compute_dow(ts)}")
print(f"compute_session: {compute_session(ts)}")
print(f"compute_quarter: {compute_quarter(ts)}")

# Test our mapping
mapped_dow = cal_map._map_weekday(compute_dow(ts))
mapped_session = cal_map._map_session(compute_session(ts))
print(f"Mapped DOW: {mapped_dow}")
print(f"Mapped Session: {mapped_session}")

# Check if key exists
key = ("EUR_USD", mapped_dow, mapped_session, compute_quarter(ts, compute_session(ts)))
print(f"Looking for key: {key}")
print(f"Key exists: {key in cal_map._map}")

# Test get_config
config = cal_map.get_config("EUR_USD", ts)
print(f"Config found: {config is not None}")
if config:
    print(f"Source: {config.get('source', 'no source')}")
    print(f"Type: {type(config)}")
