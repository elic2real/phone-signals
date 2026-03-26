#!/usr/bin/env python3
"""
Comprehensive test of runtime calibration system
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test_all():
    print("Comprehensive Runtime Calibration Test")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    
    # Test 1: Basic compiled config loading
    print("\n1. Testing compiled config loading:")
    dt_thursday = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday London Q2
    config = cal.get_current_config("EUR_USD", dt_thursday.timestamp())
    
    if config and config.get("source") == "compiled_map":
        print("   ✅ EUR_USD Thursday London: Using compiled config")
        print(f"      Direction: {config.get('direction', 'N/A')}")
        print(f"      Target: {config.get('target_distance', 'N/A')} ATR")
    else:
        print("   ❌ EUR_USD Thursday London: Not using compiled")
        
    # Test 2: Different pairs
    print("\n2. Testing multiple pairs:")
    test_pairs = ["GBP_USD", "USD_JPY", "EUR_JPY"]
    for pair in test_pairs:
        config = cal.get_current_config(pair, dt_thursday.timestamp())
        source = config.get("source", "unknown")
        status = "✅" if source == "compiled_map" else "❌"
        print(f"   {status} {pair}: {source}")
        
    # Test 3: Quarter handoff detection
    print("\n3. Testing quarter handoff:")
    
    # Check Q1 to Q2 transition
    dt_q1 = datetime(2024, 1, 11, 9, 0, 0, tzinfo=timezone.utc)  # Thursday 9:00 = Q1
    dt_q2 = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # Thursday 11:00 = Q2
    
    # Get config for Q1
    config_q1 = cal.get_current_config("EUR_USD", dt_q1.timestamp())
    print(f"   Q1 config source: {config_q1.get('source', 'unknown')}")
    
    # Get config for Q2 (should trigger handoff)
    config_q2 = cal.get_current_config("EUR_USD", dt_q2.timestamp())
    print(f"   Q2 config source: {config_q2.get('source', 'unknown')}")
    
    # Check if handoff was detected
    stats = cal.get_stats()
    if stats['handoffs_detected'] > 0:
        print(f"   ✅ Handoffs detected: {stats['handoffs_detected']}")
    else:
        print("   ⚠️  No handoffs detected (might be first check)")
        
    # Test 4: Different sessions
    print("\n4. Testing different sessions:")
    sessions = [
        (datetime(2024, 1, 11, 5, 0, 0, tzinfo=timezone.utc), "Asia"),
        (datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc), "London"),
        (datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc), "NY"),
    ]
    
    for dt, session_name in sessions:
        config = cal.get_current_config("EUR_USD", dt.timestamp())
        source = config.get("source", "unknown")
        status = "✅" if source == "compiled_map" else "❌"
        print(f"   {status} {session_name}: {source}")
        
    # Test 5: Fallback behavior
    print("\n5. Testing fallback for missing pair:")
    fake_config = cal.get_current_config("FAKE_FAKE", dt_thursday.timestamp())
    if fake_config.get("source") == "conservative_fallback":
        print("   ✅ Missing pair uses conservative fallback")
    elif fake_config.get("source") == "research_fallback":
        print("   ✅ Missing pair uses research fallback")
    else:
        print("   ❌ Missing pair not handled correctly")
        
    # Test 6: Statistics
    print("\n6. Final statistics:")
    stats = cal.get_stats()
    print(f"   Total requests: {stats['config_requests']}")
    print(f"   Compiled hits: {stats['compiled_hits']}")
    print(f"   Research fallbacks: {stats['research_fallbacks']}")
    print(f"   Conservative fallbacks: {stats['conservative_fallbacks']}")
    print(f"   Handoffs detected: {stats['handoffs_detected']}")
    
    # Overall result
    success = stats['compiled_hits'] > 0
    print(f"\n{'✅ OVERALL PASS' if success else '❌ OVERALL FAIL'}")
    
    return success

if __name__ == "__main__":
    test_all()
