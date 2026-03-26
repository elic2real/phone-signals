#!/usr/bin/env python3
"""Enable production debugging for JPY sizing collapse."""

import os
import sys

print("JPY SIZING PRODUCTION DEBUG GUIDE")
print("=" * 60)

print("\n1. ADD SIZE_TRACE ENVIRONMENT VARIABLE")
print("   Add this to your environment before running the bot:")
print("   export SIZE_TRACE_ENABLED=1")
print("   Or add to phone_bot.py after imports:")
print("   os.environ['SIZE_TRACE_ENABLED'] = '1'")

print("\n2. ADD DEBUG LOGGING TO FIND THE COLLAPSE POINT")
print("""
Add these log lines after line 15589 in phone_bot.py (after units_raw = units_total):

log(f"{EMOJI_DEBUG} UNITS_AFTER_CALC {pair_tag(pair, sig.direction)}", {
    "units_total": units_total,
    "units_raw": units_raw,
    "units_reason": units_reason,
    "margin_avail": margin_avail,
    "util": util,
    "price_for_units": price_for_units,
    "DEBUG_CHECK": "IF_UNITS_ARE_5_HERE_PROBLEM_IS_IN_CALC_UNITS"
})

# Then after spread adjustment (around line 15727):
log(f"{EMOJI_DEBUG} UNITS_AFTER_SPREAD {pair_tag(pair, sig.direction)}", {
    "units_before_spread": units_total,
    "units_after_spread": units_final,
    "spread_mult": mult,
    "DEBUG_CHECK": "IF_UNITS_ARE_5_BEFORE_SPREAD_PROBLEM_IS_EARLIER"
})
""")

print("\n3. CHECK PRODUCTION INSTRUMENT METADATA")
print("""
Add this debug code to see what metadata production is actually using:

meta = get_instrument_meta_cached(pair)
log(f"{EMOJI_DEBUG} INSTRUMENT_META_DEBUG {pair}", {
    "meta_source": "cached" if meta else "none",
    "metadata": meta,
    "minimumTradeSize": meta.get("minimumTradeSize") if meta else "NONE",
    "tradeUnitsPrecision": meta.get("tradeUnitsPrecision") if meta else "NONE",
    "marginRate": meta.get("marginRate") if meta else "NONE"
})
""")

print("\n4. LOOK FOR HIDDEN DIVISIONS")
print("""
Search for any code that might divide units:
- grep -n "units.*//" phone_bot.py
- grep -n "units.*/" phone_bot.py
- grep -n "/.*units" phone_bot.py

Also check for:
- Lot size conversions (1000, 10000, 100000)
- Time-based divisions (86400 seconds/day, 3600 seconds/hour)
- Currency pair specific conversions
""")

print("\n5. HYPOTHESIS: 2400 DIVISION")
print("""
If units are being divided by 2400:
- 12006 / 2400 = 5.0025 ≈ 5
- Check for any code using 2400 as a divisor
- Check if it's related to:
  * Minutes in 40 hours (40 * 60)
  * Some time window calculation
  * Buffer size (PathBuffer uses 2400)
""")

print("\n6. QUICK FIX FOR MARGIN RATE")
print("""
Add this to fix the margin rate issue:
if pair.endswith("_JPY"):
    # Use correct 2% margin rate for JPY pairs (50:1 leverage)
    if meta and meta.get("marginRate", 0.0333) > 0.025:
        meta["marginRate"] = 0.02
        log_runtime("warning", "JPY_MARGIN_RATE_FIXED", 
                   pair=pair, old_rate=0.0333, new_rate=0.02)
""")

print("\n7. RUN WITH DEBUG ENABLED")
print("""
1. Stop the bot
2. Add the environment variable: export SIZE_TRACE_ENABLED=1
3. Add the debug log lines to phone_bot.py
4. Restart the bot
5. Wait for a JPY trade
6. Check logs for:
   - UNITS_AFTER_CALC
   - UNITS_AFTER_SPREAD
   - SIZE_TRACE events
   - INSTRUMENT_META_DEBUG
""")

print("\n8. ANALYZE THE RESULTS")
print("""
If UNITS_AFTER_CALC shows 5 units:
- Problem is inside calc_units
- Check margin_available value
- Check util value
- Check if compute_units_risk_2pct is being called

If UNITS_AFTER_CALC shows 12006 but UNITS_AFTER_SPREAD shows 5:
- Problem is in spread adjustment
- Check spread multiplier calculation
- Check for division in spread_size_mult
""")

print("\nEXPECTED OUTCOME")
print("""
With SIZE_TRACE enabled, you should see events like:
- SIZE_TRACE: raw_risk_sizing (units_risk_raw: 22869)
- SIZE_TRACE: after_spread (units_after_spread: 19210)
- SIZE_TRACE: after_confidence (units_after_confidence: 12006)
- SIZE_TRACE: precision_round (units_after_round: 12006)
- SIZE_TRACE: margin_check (required_margin: ~63000)

This will show exactly where the collapse occurs.
""")
