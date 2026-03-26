#!/usr/bin/env python3
"""
SIZING LOGIC REPORT
Current state and integration plan
"""

print("=" * 70)
print("SIZING LOGIC AUDIT REPORT")
print("=" * 70)

print("\n1. CURRENT SIZING IMPLEMENTATION:")
print("-" * 70)

print("\nA. Main Sizing Function:")
print("   File: phone_bot.py")
print("   Function: compute_units_risk_2pct() (line 18221)")
print("   Risk: Fixed at 2% of NAV (line 18277: risk_pct = 0.02)")

print("\nB. Sizing Flow:")
print("   1. calc_units() calls compute_units_risk_2pct()")
print("   2. compute_units_risk_2pct() uses HARD-CODED 2% risk")
print("   3. Result goes to calculate_spread_aware_size() for adjustment")
print("   4. Final units returned to trade execution")

print("\nC. Key Code Locations:")
print("   - Line 6905: compute_units_risk_2pct() called")
print("   - Line 6914: confidence fixed at 0.5")
print("   - Line 6928: risk_pct logged as 0.02 (2%)")
print("   - Line 18277: risk_pct = 0.02 (HARD-CODED)")

print("\n2. PROBLEM IDENTIFIED:")
print("-" * 70)
print("\n❌ CRITICAL ISSUE:")
print("   Risk percentage is HARD-CODED at 2% in compute_units_risk_2pct()")
print("   Runtime calibration max_risk_percent is IGNORED!")
print("   Emergency fallback 0.5% is IGNORED!")

print("\n3. INTEGRATION REQUIRED:")
print("-" * 70)

print("\nA. Changes Needed:")
print("   1. Modify compute_units_risk_2pct() to accept risk_pct parameter")
print("   2. Pass runtime calibration risk_percent to sizing function")
print("   3. Ensure fallback risk levels are respected")

print("\nB. Integration Points:")
print("   - calc_units() (line ~6900): Get risk from runtime calibration")
print("   - compute_units_risk_2pct() (line 18221): Accept risk_pct parameter")
print("   - Runtime calibration: Provide risk_percent for current quarter")

print("\n4. IMPLEMENTATION PLAN:")
print("-" * 70)

print("\nStep 1: Modify compute_units_risk_2pct signature:")
print("   Add risk_pct: float = 0.02 parameter")

print("\nStep 2: Update calc_units to get risk from runtime:")
print("   config = _RUNTIME_CALIBRATION.get_current_config(pair, ts)")
print("   risk_pct = config.get('position_sizing', {}).get('max_risk_percent', 0.02) / 100")

print("\nStep 3: Pass risk_pct through the chain:")
print("   calc_units() → compute_units_risk_2pct() → position calculation")

print("\nStep 4: Test scenarios:")
print("   - Normal: Use compiled research risk")
print("   - Adaptive: Use adaptive risk (0.8-1.1x base)")
print("   - Emergency: Use 0.5% risk")

print("\n5. EXACT CODE CHANGES:")
print("-" * 70)

print("\nA. phone_bot.py line 18221:")
print("   FROM:")
print("   def compute_units_risk_2pct(*, pair: str, side: str, ...):")
print("   TO:")
print("   def compute_units_risk_2pct(*, pair: str, side: str, ..., risk_pct: float = 0.02):")

print("\nB. phone_bot.py line 18277:")
print("   FROM:")
print("   risk_pct = 0.02  # Hard-coded")
print("   TO:")
print("   # Use provided risk_pct (from runtime calibration)")

print("\nC. phone_bot.py line ~6905:")
print("   ADD before compute_units_risk_2pct call:")
print("   # Get risk from runtime calibration")
print("   config = _RUNTIME_CALIBRATION.get_current_config(pair, now_ts())")
print("   risk_from_cal = config.get('position_sizing', {}).get('max_risk_percent', 2.0)")
print("   risk_pct = risk_from_cal / 100.0  # Convert percent to decimal")

print("\n6. VERIFICATION CHECKLIST:")
print("-" * 70)
print("   [ ] compute_units_risk_2pct accepts risk_pct parameter")
print("   [ ] calc_units gets risk from runtime calibration")
print("   [ ] Risk properly converted from percent to decimal")
print("   [ ] Emergency fallback uses 0.5% risk")
print("   [ ] Adaptive risk uses bounded values")
print("   [ ] Compiled research risk is preserved")
print("   [ ] Test with all fallback scenarios")

print("\n" + "=" * 70)
print("PRIORITY: HIGH")
print("The sizing system is currently IGNORING runtime calibration!")
print("This must be fixed for the calibration system to be effective.")
print("=" * 70)
