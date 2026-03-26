#!/usr/bin/env python3
"""
Simple test to verify runtime calibration works with compiled nodes
"""

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test_compiled_loading():
    """Test that compiled nodes load correctly"""
    print("Testing Runtime Calibration with Compiled Nodes")
    print("=" * 60)
    
    # Initialize
    cal = RuntimeCalibration()
    
    # Test specific time: Thursday 11:00 UTC = London Q2
    dt = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday
    ts = dt.timestamp()
    
    print(f"\nTest time: {dt}")
    print(f"Expected: Thursday, London, Q2")
    
    # Test EUR_USD
    print(f"\n--- EUR_USD ---")
    supported = cal.is_pair_supported("EUR_USD", ts)
    print(f"Supported: {supported}")
    
    config = cal.get_current_config("EUR_USD", ts)
    source = config.get("source", "unknown")
    print(f"Config source: {source}")
    
    if source == "compiled_map":
        print("✅ SUCCESS: Using compiled calibration!")
        print(f"Config has {len(config)} fields")
    else:
        print("❌ Using fallback instead of compiled")
        
    # Show stats
    stats = cal.get_stats()
    print(f"\n--- Statistics ---")
    print(f"Compiled hits: {stats['compiled_hits']}")
    print(f"Research fallbacks: {stats['research_fallbacks']}")
    print(f"Conservative fallbacks: {stats['conservative_fallbacks']}")
    
    # Test quarter handoff
    print(f"\n--- Quarter Handoff Test ---")
    # Test at 10:00 (Q1) and 12:00 (Q2)
    dt_q1 = datetime(2024, 1, 11, 10, 0, 0, tzinfo=timezone.utc)
    dt_q2 = datetime(2024, 1, 11, 12, 0, 0, tzinfo=timezone.utc)
    
    config_q1 = cal.get_current_config("EUR_USD", dt_q1.timestamp())
    config_q2 = cal.get_current_config("EUR_USD", dt_q2.timestamp())
    
    print(f"Q1 config source: {config_q1.get('source', 'unknown')}")
    print(f"Q2 config source: {config_q2.get('source', 'unknown')}")
    
    # Check if handoff was detected
    handoffs = stats['handoffs_detected']
    print(f"Handoffs detected: {handoffs}")
    
    return stats['compiled_hits'] > 0

if __name__ == "__main__":
    success = test_compiled_loading()
    print(f"\n{'✅ PASS' if success else '❌ FAIL'}: Compiled calibration working")
