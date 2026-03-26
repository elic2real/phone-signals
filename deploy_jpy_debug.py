#!/usr/bin/env python3
"""Deploy JPY sizing debug to production."""

import os
import sys

print("JPY SIZING DEBUG DEPLOYMENT")
print("=" * 50)

# Check if we're in the right directory
if not os.path.exists("phone_bot.py"):
    print("ERROR: Must run from phone signals directory")
    sys.exit(1)

# Enable SIZE_TRACE
os.environ["SIZE_TRACE_ENABLED"] = "1"
print("✓ SIZE_TRACE_ENABLED = 1")

# Create a patch for phone_bot.py
patch_content = '''
# JPY SIZING DEBUG - Add after line 15589 (after units_raw = units_total)
log(f"{EMOJI_DEBUG} UNITS_DEBUG {pair_tag(pair, sig.direction)}", {
    "units_from_calc": units_total,
    "units_raw": units_raw,
    "units_reason": units_reason,
    "margin_avail": margin_avail,
    "util": util,
    "price_for_units": price_for_units,
    "DEBUG_CHECK": "DIVISION_BY_2400_HAPPENED_HERE_IF_UNITS=5"
})

# JPY SIZING DEBUG - Add after line 15727 (after spread adjustment)
log(f"{EMOJI_DEBUG} SPREAD_DEBUG {pair_tag(pair, sig.direction)}", {
    "units_before_spread": units_total,
    "units_after_spread": units_final,
    "spread_mult": mult,
    "DEBUG_CHECK": "IF_UNITS_ARE_5_BEFORE_SPREAD_PROBLEM_IS_EARLIER"
})

# JPY SIZING DEBUG - Check instrument metadata
meta = get_instrument_meta_cached(pair)
log(f"{EMOJI_DEBUG} META_DEBUG {pair}", {
    "meta_source": "cached" if meta else "none",
    "minimumTradeSize": meta.get("minimumTradeSize") if meta else "NONE",
    "tradeUnitsPrecision": meta.get("tradeUnitsPrecision") if meta else "NONE",
    "marginRate": meta.get("marginRate") if meta else "NONE",
    "pair": pair
})
'''

# Write patch to file
with open("jpy_debug_patch.txt", "w") as f:
    f.write(patch_content)

print("\n✓ Created jpy_debug_patch.txt")
print("\nTo apply the patch:")
print("1. Open phone_bot.py")
print("2. Find line 15589 (after 'units_raw = units_total')")
print("3. Add the debug code from jpy_debug_patch.txt")
print("4. Restart the bot with: export SIZE_TRACE_ENABLED=1")
print("5. Monitor logs for:")
print("   - UNITS_DEBUG entries")
print("   - SPREAD_DEBUG entries")
print("   - META_DEBUG entries")
print("   - SIZE_TRACE events")

print("\nExpected results:")
print("- If UNITS_DEBUG shows 5 units → Problem is in calc_units")
print("- If UNITS_DEBUG shows 12006 but SPREAD_DEBUG shows 5 → Problem is in spread adjustment")
print("- META_DEBUG will show the actual metadata being used")

print("\nAfter finding the division point:")
print("1. Search for any division by 2400, 24, or 100")
print("2. Check for time-based calculations")
print("3. Look for buffer size operations")
print("4. Verify no lot size conversions")

print("\nQuick fix for margin rate (add after metadata retrieval):")
print("""
if pair.endswith("_JPY"):
    if meta and meta.get("marginRate", 0.0333) > 0.025:
        meta["marginRate"] = 0.02
        log_runtime("warning", "JPY_MARGIN_RATE_FIXED", 
                   pair=pair, old_rate=0.0333, new_rate=0.02)
""")
