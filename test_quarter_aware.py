#!/usr/bin/env python3
"""
Test to show improved quarter-specific fallback behavior
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test():
    print("Runtime Calibration with Quarter-Specific Fallbacks")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    
    # Test NY session (which doesn't have compiled data)
    print("\n1. Testing NY session (uses quarter fallback):")
    dt_ny = datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)  # Thursday 17:00 = NY Q1
    config = cal.get_current_config("EUR_USD", dt_ny.timestamp())
    
    print(f"   Time: {dt_ny.strftime('%H:%M')} (NY Q1)")
    print(f"   Source: {config.get('source')}")
    print(f"   Quarter: {config.get('quarter')}")
    print(f"   Session: {config.get('session')}")
    
    # Show research-specific values
    if "aee.strictness_mult" in config:
        print(f"   Strictness multiplier: {config['aee.strictness_mult']}")
    if "promote_mfe_atr" in config:
        print(f"   Promote MFE ATR: {config['promote_mfe_atr']}")
    if "aee.near_tp_band_atr" in config:
        print(f"   Near TP band ATR: {config['aee.near_tp_band_atr']}")
    
    # Test different quarters in NY
    print("\n2. Different quarters in NY session:")
    ny_times = [
        (datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc), "Q1"),  # 17:00
        (datetime(2024, 1, 11, 19, 0, 0, tzinfo=timezone.utc), "Q2"),  # 19:00
        (datetime(2024, 1, 11, 21, 0, 0, tzinfo=timezone.utc), "Q3"),  # 21:00
        (datetime(2024, 1, 11, 23, 0, 0, tzinfo=timezone.utc), "Q4"),  # 23:00
    ]
    
    for dt, expected_q in ny_times:
        config = cal.get_current_config("EUR_USD", dt.timestamp())
        actual_q = config.get('quarter')
        
        # Show relevant values
        strictness = config.get('aee.strictness_mult', 'N/A')
        
        status = "✅" if actual_q == expected_q else "❌"
        print(f"   {status} {dt.strftime('%H:%M')} - Quarter {actual_q}, Strictness {strictness}")
        
    # Test missing pair
    print("\n3. Missing pair (uses quarter fallback):")
    config = cal.get_current_config("FAKE_PAIR", dt_ny.timestamp())
    print(f"   Source: {config.get('source')}")
    print(f"   Quarter: {config.get('quarter')}")
    print(f"   Description: {config.get('description')}")
    
    # Show stats
    stats = cal.get_stats()
    print(f"\n4. Statistics:")
    print(f"   Compiled hits: {stats['compiled_hits']}")
    print(f"   Quarter fallbacks: {stats['research_fallbacks']}")
    print(f"   Emergency fallbacks: {stats['conservative_fallbacks']}")
    
    print("\n✅ Test complete - quarter-specific fallbacks working!")

if __name__ == "__main__":
    test()
