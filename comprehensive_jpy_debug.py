#!/usr/bin/env python3
"""Comprehensive debug to find where JPY units become 5."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Enable all debugging
os.environ["DEBUG"] = "1"

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    INSTR_META,
    get_instrument_meta_cached
)

# First, let's check if there's something in the cached metadata
print("=" * 60)
print("CHECKING INSTRUMENT METADATA")
print("=" * 60)

# Clear any existing cache
INSTR_META.clear()

# Setup metadata as it would be in production
INSTR_META["USD_JPY"] = {
    "minimumTradeSize": 1,
    "tradeUnitsPrecision": 0,
    "marginRate": 0.0333,
    "pipLocation": -2,
    "displayPrecision": 3,
    "unitsPerTrade": 1  # Check if this exists
}

print("Initial metadata:")
print(INSTR_META.get("USD_JPY", {}))

# Check what get_instrument_meta_cached returns
meta = get_instrument_meta_cached("USD_JPY")
print(f"\nCached metadata: {meta}")

# Check if there's a default metadata fallback
print("\n" + "=" * 60)
print("CHECKING FOR DEFAULT METADATA")
print("=" * 60)

from phone_bot import _fallback_instrument_meta
fallback_meta = _fallback_instrument_meta("USD_JPY")
print(f"Fallback metadata: {fallback_meta}")

# Now let's trace through the actual calculation
print("\n" + "=" * 60)
print("TRACING UNIT CALCULATION")
print("=" * 60)

# Monkey-patch compute_units_risk_2pct to add tracing
orig_compute_units = compute_units_risk_2pct

def traced_compute_units(*args, **kwargs):
    print(f"\n[TRACE] compute_units_risk_2pct called")
    print(f"  pair: {kwargs.get('pair')}")
    print(f"  nav_usd: {kwargs.get('nav_usd')}")
    print(f"  spread_pips: {kwargs.get('spread_pips')}")
    print(f"  spread_mult: {kwargs.get('spread_mult')}")
    print(f"  confidence: {kwargs.get('confidence')}")
    
    result = orig_compute_units(*args, **kwargs)
    
    print(f"[TRACE] compute_units_risk_2pct result:")
    print(f"  units_total: {result.get('units_total')}")
    print(f"  units_main: {result.get('units_main')}")
    print(f"  units_runner: {result.get('units_runner')}")
    print(f"  blocked: {result.get('blocked')}")
    
    return result

import phone_bot
phone_bot.compute_units_risk_2pct = traced_compute_units

# Also patch calc_units
orig_calc_units = calc_units

def traced_calc_units(*args, **kwargs):
    print(f"\n[TRACE] calc_units called")
    print(f"  pair: {kwargs.get('pair', args[0] if args else 'N/A')}")
    print(f"  margin_avail: {kwargs.get('margin_avail', args[3] if len(args) > 3 else 'N/A')}")
    print(f"  util: {kwargs.get('util', args[4] if len(args) > 4 else 'N/A')}")
    
    result = orig_calc_units(*args, **kwargs)
    
    print(f"[TRACE] calc_units result: {result[0]} units")
    
    return result

phone_bot.calc_units = traced_calc_units

# Run the test
print("\nRunning test with exact parameters...")
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

print(f"\nFINAL RESULT: {units_total} units")
print(f"Reason: {units_reason}")
print(f"Debug: {units_debug}")

# Check if there's something specific about the configuration
print("\n" + "=" * 60)
print("CHECKING CONFIGURATION")
print("=" * 60)

# Check for any size-related configuration
config_vars = [
    "MIN_TRADE_SIZE",
    "MAX_UNITS_PER_TRADE", 
    "MIN_POSITION_SIZE_USD",
    "JPY_MIN_UNITS",
    "UNITS_PER_LOT",
    "LOT_SIZE"
]

for var in config_vars:
    value = globals().get(var, "NOT FOUND")
    print(f"{var}: {value}")

# Check environment variables
print("\nEnvironment variables affecting sizing:")
env_vars = [
    "SIZE_TRACE_ENABLED",
    "MIN_UNITS",
    "MAX_UNITS",
    "LOT_SIZE",
    "UNITS_PRECISION"
]

for var in env_vars:
    value = os.environ.get(var, "NOT SET")
    print(f"{var}: {value}")
