#!/usr/bin/env python3
"""Debug the JPY sizing collapse by matching exact log conditions."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    INSTR_META,
    pip_size,
    to_pips,
    get_pip_value_usd
)


def debug_exact_log_conditions():
    """Debug with exact parameters from the logs."""
    print("\n=== Debugging Exact Log Conditions ===")
    
    # Setup metadata exactly as it would be
    INSTR_META["USD_JPY"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,  # Using fallback
        "pipLocation": -2,
        "displayPrecision": 3
    }
    
    # Exact parameters from SIZING_ATTEMPT log
    params = {
        "pair": "USD_JPY",
        "side": "LONG",
        "price": 157.883,  # entry_price from log
        "margin_avail": 15126.764,
        "util": 0.15,  # util from log
        "speed_class": "SLOW",
        "spread_pips": 1.8000000000000682,
        "disp_atr": 0.00010230769230767799,
        "size_mult": 1.0,  # default
        "sl_price": None,  # This is the key - will be calculated
        "nav_usd": 32271.0353,
        "price_map": {"USD_JPY": (157.946, 157.966)}
    }
    
    print(f"Parameters:")
    for k, v in params.items():
        print(f"  {k}: {v}")
    
    # First, let's see what sl_price would be calculated as
    # From the log: stop_dist_pips = 445.7999999999998
    # And entry_price = 157.883
    # So sl_price = entry_price - (stop_dist_pips * pip_size)
    pip_sz = float(pip_size("USD_JPY"))
    sl_price_calc = 157.883 - (445.7999999999998 * pip_sz)
    print(f"\nCalculated SL price: {sl_price_calc}")
    
    # Update params with calculated SL
    params["sl_price"] = sl_price_calc
    
    # Call calc_units (which calls compute_units_risk_2pct)
    print(f"\n=== Calling calc_units ===")
    units_total, units_reason, units_debug = calc_units(**params)
    
    print(f"\nResults from calc_units:")
    print(f"  Units total: {units_total}")
    print(f"  Reason: {units_reason}")
    print(f"  Debug: {units_debug}")
    
    # Now call compute_units_risk_2pct directly with confidence=0.5 (as calc_units does)
    print(f"\n=== Calling compute_units_risk_2pct directly ===")
    result = compute_units_risk_2pct(
        pair="USD_JPY",
        side="LONG",
        entry_price=157.883,
        sl_price=sl_price_calc,
        nav_usd=32271.0353,
        spread_pips=1.8000000000000682,
        spread_mult=0.84,  # From spread calculation
        speed_class="SLOW",
        confidence=0.5,  # Hardcoded in calc_units!
        price_map={"USD_JPY": (157.946, 157.966)}
    )
    
    print(f"\nResults from compute_units_risk_2pct:")
    print(f"  Units total: {result['units_total']}")
    print(f"  Units main: {result['units_main']}")
    print(f"  Units runner: {result['units_runner']}")
    print(f"  Risk actual: ${result['risk_usd_actual']:.2f}")
    print(f"  Blocked: {result['blocked']}")
    if result['blocked']:
        print(f"  Block reason: {result['block_reason']}")
    
    # Check the intermediate values
    print(f"\n=== Intermediate Values ===")
    stop_dist_price = abs(157.883 - sl_price_calc)
    stop_dist_pips = to_pips("USD_JPY", stop_dist_price)
    pip_value = get_pip_value_usd("USD_JPY", {"USD_JPY": (157.946, 157.966)})
    
    print(f"  Stop distance (price): {stop_dist_price}")
    print(f"  Stop distance (pips): {stop_dist_pips}")
    print(f"  Pip value: ${pip_value:.8f}")
    print(f"  Risk per unit: ${stop_dist_pips * pip_value:.6f}")
    print(f"  Risk target: ${32271.0353 * 0.02:.2f}")
    print(f"  Expected units: {(32271.0353 * 0.02) / (stop_dist_pips * pip_value):.0f}")


def check_spread_multiplier():
    """Check how spread multiplier is calculated."""
    print("\n=== Checking Spread Multiplier ===")
    
    spread_pips = 1.8000000000000682
    spread_mult = 1.0
    
    if spread_pips > 0:
        if spread_pips > 5.0:
            spread_mult = max(0.5, 1.0 - (spread_pips - 2.0) * 0.1)
        elif spread_pips > 2.0:
            spread_mult = max(0.5, 1.0 - (spread_pips - 2.0) * 0.1)
        elif spread_pips > 1.0:
            spread_mult = max(0.8, 1.0 - (spread_pips - 1.0) * 0.2)
    
    print(f"  Spread pips: {spread_pips}")
    print(f"  Spread multiplier: {spread_mult}")
    
    # But the log shows spread_mult: 0.8399999999999863
    # Let's check if there's another calculation
    print(f"\n  Log shows spread_mult: 0.8399999999999863")
    print(f"  This suggests a different calculation might be used!")


def main():
    """Run debugging tests."""
    print("=" * 60)
    print("JPY SIZING COLLAPSE DEBUG")
    print("=" * 60)
    
    debug_exact_log_conditions()
    check_spread_multiplier()
    
    print("\n" + "=" * 60)
    print("Debug complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
