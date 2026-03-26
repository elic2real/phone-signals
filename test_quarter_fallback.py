#!/usr/bin/env python3
"""
Test quarter-specific fallback configurations
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration
from fallback_templates import FallbackTemplates

def test_quarter_fallbacks():
    print("Testing Quarter-Specific Fallback Configurations")
    print("=" * 60)
    
    # Test direct quarter fallback
    print("\n1. Direct quarter fallback tests:")
    test_times = [
        (datetime(2024, 1, 11, 9, 0, 0, tzinfo=timezone.utc), "Q1"),  # London 9:00 = Q1
        (datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc), "Q2"), # London 11:00 = Q2
        (datetime(2024, 1, 11, 13, 0, 0, tzinfo=timezone.utc), "Q3"), # London 13:00 = Q3
        (datetime(2024, 1, 11, 15, 0, 0, tzinfo=timezone.utc), "Q4"), # London 15:00 = Q4
    ]
    
    for dt, expected_quarter in test_times:
        config = FallbackTemplates.get_quarter_fallback(dt.timestamp())
        actual_quarter = config.get("quarter")
        source = config.get("source")
        
        status = "✅" if actual_quarter == expected_quarter else "❌"
        print(f"   {status} {dt.strftime('%H:%M')} - Expected {expected_quarter}, got {actual_quarter}")
        print(f"      Source: {source}")
        print(f"      Panic multiplier: {config['management']['panic_multiplier']}")
        print(f"      Target ATR: {config['targets']['default_target_atr']}")
        
    # Test through RuntimeCalibration with missing pair
    print("\n2. Through RuntimeCalibration (missing pair):")
    cal = RuntimeCalibration()
    
    for dt, expected_quarter in test_times:
        config = cal.get_current_config("MISSING_PAIR", dt.timestamp())
        actual_quarter = config.get("quarter")
        source = config.get("source")
        
        status = "✅" if "quarter_fallback" in source else "❌"
        print(f"   {status} {dt.strftime('%H:%M')} - {source}")
        print(f"      Quarter: {actual_quarter}")
        
    # Test session adjustments
    print("\n3. Session-specific adjustments:")
    sessions = [
        (datetime(2024, 1, 11, 5, 0, 0, tzinfo=timezone.utc), "ASIA"),
        (datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc), "LONDON"),
        (datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc), "NY"),
    ]
    
    for dt, session_name in sessions:
        config = FallbackTemplates.get_quarter_fallback(dt.timestamp())
        max_spread = config["entry_filters"]["max_spread_pips"]
        stall_timeout = config["management"]["stall_timeout_minutes"]
        
        print(f"   {session_name}:")
        print(f"      Max spread: {max_spread:.1f} pips")
        print(f"      Stall timeout: {stall_timeout} minutes")
        
    # Test quarter differences
    print("\n4. Quarter behavior differences:")
    print("   Quarter   | Panic | Target | Trailing | Partial")
    print("   ----------|-------|--------|----------|--------")
    
    for quarter in ["Q1", "Q2", "Q3", "Q4"]:
        config = FallbackTemplates.QUARTER_CONFIGS[quarter]
        panic = config["management"]["panic_multiplier"]
        target = config["targets"]["default_target_atr"]
        trailing = config["management"]["trailing_stop_enabled"]
        partial = config["targets"]["partial_targets_enabled"]
        
        print(f"   {quarter:9} | {panic:5.1f} | {target:6.1f} | {str(trailing):8} | {str(partial):7}")
        
    print("\n✅ Quarter-specific fallback test complete")

if __name__ == "__main__":
    test_quarter_fallbacks()
