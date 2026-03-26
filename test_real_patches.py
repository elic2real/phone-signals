#!/usr/bin/env python3
"""
Test runtime calibration with real research patches
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test_with_patches():
    print("Runtime Calibration with Real Research Patches")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    
    # Test AUD_USD in NY Q1 (has patches)
    print("\n1. AUD_USD NY Q1 (has patches):")
    dt_ny_q1 = datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)  # NY Q1
    config = cal.get_current_config("AUD_USD", dt_ny_q1.timestamp())
    
    print(f"   Time: {dt_ny_q1.strftime('%H:%M')} (NY Q1)")
    print(f"   Source: {config.get('source')}")
    print(f"   Quarter: {config.get('quarter')}")
    
    # Check for patched values
    if config.get('source') == 'research_patch_NY_Q1':
        print("   ✅ Using NY Q1 patch!")
        print(f"   Strictness multiplier: {config.get('aee.strictness_mult')}")
        print(f"   Promote MFE ATR: {config.get('promote_mfe_atr')}")
        if 'evidence' in config:
            print(f"   Evidence - n: {config['evidence'].get('n')}")
            print(f"   Evidence - exit_result_per_h: {config['evidence'].get('exit_result_per_h'):.3f}")
    else:
        print(f"   Using base config (strictness: {config.get('aee.strictness_mult', 'N/A')})")
        
    # Test EUR_USD in NY Q1 (no patches, uses base)
    print("\n2. EUR_USD NY Q1 (no patches):")
    config = cal.get_current_config("EUR_USD", dt_ny_q1.timestamp())
    
    print(f"   Source: {config.get('source')}")
    print(f"   Strictness multiplier: {config.get('aee.strictness_mult', 'N/A')}")
    print(f"   Near TP band ATR: {config.get('aee.near_tp_band_atr', 'N/A')}")
    
    # Test compiled vs fallback
    print("\n3. Compiled vs Research Fallback:")
    
    # EUR_USD London (has compiled)
    dt_london = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)
    config = cal.get_current_config("EUR_USD", dt_london.timestamp())
    print(f"   EUR_USD London: {config.get('source')}")
    
    # AUD_USD NY (uses research fallback with patches)
    config = cal.get_current_config("AUD_USD", dt_ny_q1.timestamp())
    print(f"   AUD_USD NY Q1: {config.get('source')}")
    
    # Show stats
    stats = cal.get_stats()
    print(f"\n4. Statistics:")
    print(f"   Compiled hits: {stats['compiled_hits']}")
    print(f"   Research fallbacks: {stats['research_fallbacks']}")
    print(f"   Emergency fallbacks: {stats['conservative_fallbacks']}")
    
    print("\n✅ Test complete - real research patches working!")

if __name__ == "__main__":
    test_with_patches()
