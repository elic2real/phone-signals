#!/usr/bin/env python3
"""
Test Runtime Calibration System
Verifies that compiled configs load and quarter handoffs work
"""

import sys
from datetime import datetime, timezone, timedelta
from runtime_calibration import RuntimeCalibration

def test_basic_functionality():
    """Test basic config loading and resolution"""
    print("=" * 60)
    print("Testing Runtime Calibration System")
    print("=" * 60)
    
    # Initialize the system
    cal = RuntimeCalibration()
    
    # Test current time
    now = datetime.now(timezone.utc).timestamp()
    print(f"\nCurrent time: {datetime.fromtimestamp(now, timezone.utc)}")
    
    # Test a few major pairs
    test_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "EUR_JPY"]
    
    for pair in test_pairs:
        print(f"\n--- Testing {pair} ---")
        
        # Check if supported
        supported = cal.is_pair_supported(pair, now)
        print(f"Supported: {supported}")
        
        # Get current config
        config = cal.get_current_config(pair, now)
        source = config.get("source", "unknown")
        print(f"Config source: {source}")
        
        # Get entry filters
        filters = cal.get_entry_filters(pair, now)
        if filters:
            print(f"Entry filters: {filters}")
            
        # Get management rules
        mgmt = cal.get_management_rules(pair, now)
        if mgmt:
            print(f"Management rules: {mgmt}")
            
    # Show stats
    print("\n--- Statistics ---")
    stats = cal.get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")

def test_quarter_handoff():
    """Test quarter change detection"""
    print("\n" + "=" * 60)
    print("Testing Quarter Handoff Detection")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    pair = "EUR_USD"
    
    # Simulate time progression through London session
    base_time = datetime(2024, 1, 8, 8, 0, 0, tzinfo=timezone.utc)  # Monday 8:00 UTC
    
    print(f"\nSimulating London session for {pair}:")
    
    # Test each quarter
    for hour in [8, 10, 12, 14, 16]:  # London session hours
        test_time = base_time.replace(hour=hour).timestamp()
        
        config = cal.get_current_config(pair, test_time)
        source = config.get("source", "unknown")
        
        dt = datetime.fromtimestamp(test_time, timezone.utc)
        quarter = (hour - 8) // 2 + 1
        print(f"  {dt.strftime('%H:%M')} (Q{quarter}): {source}")

def test_fallback_behavior():
    """Test fallback when compiled data missing"""
    print("\n" + "=" * 60)
    print("Testing Fallback Behavior")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    
    # Test with a pair that likely doesn't exist
    fake_pair = "FAKE_FAKE"
    now = datetime.now(timezone.utc).timestamp()
    
    print(f"\nTesting with non-existent pair {fake_pair}:")
    
    config = cal.get_current_config(fake_pair, now)
    source = config.get("source", "unknown")
    print(f"Config source: {source}")
    print(f"Config keys: {list(config.keys())}")

def main():
    """Run all tests"""
    try:
        test_basic_functionality()
        test_quarter_handoff()
        test_fallback_behavior()
        
        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
