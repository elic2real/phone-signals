#!/usr/bin/env python3
"""Trace the exact point where JPY units collapse to 5."""

import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from phone_bot import (
    calc_units,
    compute_units_risk_2pct,
    INSTR_META,
    pip_size,
    to_pips,
    get_pip_value_usd,
    spread_size_mult,
    spread_atr,
    atr_pips
)

def trace_jpy_collapse():
    """Trace step by step to find where units become 5."""
    print("\n=== Tracing JPY Unit Collapse ===")
    
    # Setup metadata
    INSTR_META["USD_JPY"] = {
        "minimumTradeSize": 1,
        "tradeUnitsPrecision": 0,
        "marginRate": 0.0333,
        "pipLocation": -2,
        "displayPrecision": 3
    }
    
    # Exact parameters from logs
    pair = "USD_JPY"
    side = "LONG"
    entry_price = 157.883
    sl_price = 153.425
    nav_usd = 32271.0353
    spread_pips = 1.8000000000000682
    speed_class = "SLOW"
    confidence = 0.5  # Hardcoded in calc_units
    
    # Step 1: Check basic risk sizing
    print("\n--- Step 1: Basic Risk Sizing ---")
    risk_pct = 0.02
    risk_usd_target = nav_usd * risk_pct
    sl_dist_price = abs(entry_price - sl_price)
    stop_dist_pips = to_pips(pair, sl_dist_price)
    pip_value_usd = get_pip_value_usd(pair, {"USD_JPY": (157.946, 157.966)})
    risk_per_unit_usd = stop_dist_pips * pip_value_usd
    units_raw = risk_usd_target / risk_per_unit_usd
    
    print(f"Risk target: ${risk_usd_target:.2f}")
    print(f"Stop distance: {stop_dist_pips:.1f} pips")
    print(f"Pip value: ${pip_value_usd:.8f}")
    print(f"Risk per unit: ${risk_per_unit_usd:.6f}")
    print(f"Raw units: {units_raw:.0f}")
    
    # Step 2: Spread multiplier
    print("\n--- Step 2: Spread Multiplier ---")
    # From logs: spread_mult: 0.8399999999999863
    spread_mult = 0.84  # Approximate
    units_after_spread = units_raw * spread_mult
    print(f"Spread multiplier: {spread_mult}")
    print(f"Units after spread: {units_after_spread:.0f}")
    
    # Step 3: Confidence multiplier
    print("\n--- Step 3: Confidence Multiplier ---")
    conf_mult = 0.25 + 0.75 * confidence
    units_after_conf = units_after_spread * conf_mult
    print(f"Confidence: {confidence}")
    print(f"Confidence multiplier: {conf_mult}")
    print(f"Units after confidence: {units_after_conf:.0f}")
    
    # Step 4: Precision rounding
    print("\n--- Step 4: Precision Rounding ---")
    precision = 0  # tradeUnitsPrecision for JPY
    units_rounded = int(round(units_after_conf, precision))
    print(f"Precision: {precision}")
    print(f"Units after rounding: {units_rounded}")
    
    # Step 5: Main/Runner split
    print("\n--- Step 5: Main/Runner Split ---")
    main_ratio, runner_ratio = 0.80, 0.20  # SLOW speed class
    units_main = int(units_rounded * main_ratio)
    units_runner = units_rounded - units_main
    print(f"Main ratio: {main_ratio}")
    print(f"Units main: {units_main}")
    print(f"Units runner: {units_runner}")
    print(f"Total units: {units_main + units_runner}")
    
    # Step 6: Check actual compute_units_risk_2pct
    print("\n--- Step 6: Actual Function Call ---")
    result = compute_units_risk_2pct(
        pair=pair,
        side=side,
        entry_price=entry_price,
        sl_price=sl_price,
        nav_usd=nav_usd,
        spread_pips=spread_pips,
        spread_mult=spread_mult,
        speed_class=speed_class,
        confidence=confidence,
        price_map={"USD_JPY": (157.946, 157.966)}
    )
    
    print(f"Function result - Total: {result['units_total']}")
    print(f"Function result - Main: {result['units_main']}")
    print(f"Function result - Runner: {result['units_runner']}")
    print(f"Blocked: {result['blocked']}")
    if result['blocked']:
        print(f"Block reason: {result['block_reason']}")
    
    # Step 7: Check calc_units
    print("\n--- Step 7: Check calc_units ---")
    units_total, units_reason, units_debug = calc_units(
        pair=pair,
        side=side,
        price=entry_price,
        margin_avail=15126.764,
        util=0.15,
        speed_class=speed_class,
        spread_pips=spread_pips,
        disp_atr=0.00010230769230767799,
        size_mult=1.0,
        sl_price=sl_price,
        nav_usd=nav_usd,
        price_map={"USD_JPY": (157.946, 157.966)}
    )
    
    print(f"calc_units result: {units_total}")
    print(f"Reason: {units_reason}")
    print(f"Debug: {units_debug}")
    
    # Step 8: Check spread adjustment calculation
    print("\n--- Step 8: Spread Adjustment Calculation ---")
    # From logs: atr_exec = 557.1428571428173
    atr_exec = 557.1428571428173
    s_atr = spread_atr(pair, spread_pips, atr_exec)
    spread_mult_calc = spread_size_mult(speed_class, s_atr)
    
    print(f"ATR exec: {atr_exec}")
    print(f"Spread ATR: {s_atr}")
    print(f"Calculated spread mult: {spread_mult_calc}")
    
    # Step 9: What if units are already small before spread adjustment?
    print("\n--- Step 9: Hypothetical ---")
    print("If units somehow became 5 before spread adjustment...")
    hypothetical_units = 5
    adjusted_units = int(hypothetical_units * spread_mult_calc)
    print(f"5 units * {spread_mult_calc} = {adjusted_units}")
    
    # Check if there's a specific condition that produces 5
    print("\n--- Looking for 5-unit sources ---")
    print("Is there a minimum position size somewhere?")
    print("Is there a lot size conversion (e.g., dividing by 1000)?")
    print("Is there a specific JPY minimum?")

if __name__ == "__main__":
    trace_jpy_collapse()
