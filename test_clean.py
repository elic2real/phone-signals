#!/usr/bin/env python3
"""
Clean test to verify runtime calibration works
"""

import logging
# Suppress all logging
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger().setLevel(logging.CRITICAL)

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test():
    print("Testing Runtime Calibration")
    print("=" * 60)
    
    # Initialize
    cal = RuntimeCalibration()
    
    # Test Thursday London time
    dt = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday 11:00 UTC
    ts = dt.timestamp()
    
    print(f"Test time: {dt} (London Q2)")
    
    # Test EUR_USD
    config = cal.get_current_config("EUR_USD", ts)
    source = config.get("source", "unknown")
    print(f"Config source: {source}")
    
    if source == "compiled_map":
        print("✅ SUCCESS: Using compiled calibration!")
        
        # Show some config details
        if "direction" in config:
            print(f"Direction: {config['direction']}")
        if "target_distance" in config:
            print(f"Target distance: {config['target_distance']}")
        if "path_class_name" in config:
            print(f"Path class: {config['path_class_name']}")
    else:
        print("❌ Using fallback")
        
    # Show stats
    stats = cal.get_stats()
    print(f"\nCompiled hits: {stats['compiled_hits']}")
    print(f"Fallbacks: {stats['research_fallbacks'] + stats['conservative_fallbacks']}")
    
    return stats['compiled_hits'] > 0

if __name__ == "__main__":
    success = test()
    print(f"\n{'✅ PASS' if success else '❌ FAIL'}")
