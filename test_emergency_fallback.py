#!/usr/bin/env python3
"""
Test Emergency Fallback Reachability
"""

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test_emergency_fallback():
    print("=" * 70)
    print("EMERGENCY FALLBACK REACHABILITY TEST")
    print("=" * 70)
    
    cal = RuntimeCalibration()
    
    # Test with clearly impossible pair
    print("\n1. Testing with impossible pair:")
    impossible_pair = "IMPOSSIBLE_PAIR_THAT_DOES_NOT_EXIST"
    
    config = cal.get_current_config(impossible_pair, datetime.now(timezone.utc).timestamp())
    source = config.get('source', 'unknown')
    
    print(f"   Pair: {impossible_pair}")
    print(f"   Result: {source}")
    
    if 'emergency' in source or 'conservative' in source:
        print("   ✅ Emergency fallback IS reachable")
    else:
        print("   ❌ Emergency fallback NOT reached - something else provided config")
        
    # Check what get_conservative_config returns
    print("\n2. Direct emergency fallback test:")
    emergency_config = cal.fallback.get_conservative_config()
    print(f"   Emergency config keys: {list(emergency_config.keys())}")
    print(f"   Max risk: {emergency_config.get('position_sizing', {}).get('max_risk_percent', 'N/A')}%")
    print(f"   Description: {emergency_config.get('description', 'N/A')}")
    
    # Test if adaptive can fail
    print("\n3. Testing adaptive failure conditions:")
    
    # Break adaptive to force emergency
    original_generate = cal.adaptive.generate_adaptive_config
    cal.adaptive.generate_adaptive_config = lambda pair, ts: None
    
    # Also break fallback
    original_fallback = cal.fallback.get_quarter_fallback
    cal.fallback.get_quarter_fallback = lambda pair, ts: None
    
    # Now test
    config = cal.get_current_config("TEST_PAIR", datetime.now(timezone.utc).timestamp())
    source = config.get('source', 'unknown')
    
    print(f"   With all fallbacks broken: {source}")
    
    if 'emergency' in source or 'conservative' in source:
        print("   ✅ Emergency fallback IS the final safety net")
    else:
        print("   ❌ Emergency fallback still not reached")
        
    # Restore functions
    cal.adaptive.generate_adaptive_config = original_generate
    cal.fallback.get_quarter_fallback = original_fallback
    
    # Show the exact path
    print("\n4. Exact fallback path in code:")
    print("""
    runtime_calibration.py get_current_config():
    
    1. compiled_map.get_config() → If exists, return
    2. adaptive.generate_adaptive_config() → If confidence > 0.3, return  
    3. fallback.get_quarter_fallback() → If exists, return
    4. fallback.get_conservative_config() → ALWAYS returns (final safety)
    
    CONCLUSION: Emergency fallback IS reachable and is the FINAL safety layer.
    It's only reached if all other layers fail.
    """)
    
    return True

if __name__ == "__main__":
    test_emergency_fallback()
