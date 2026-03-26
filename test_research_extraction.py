#!/usr/bin/env python3
"""
Test research mapping extraction with real quarter data
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

from datetime import datetime, timezone
from quarter_mapping_extractor import QuarterMappingExtractor

def test_research_extraction():
    print("Testing Research Mapping Extraction")
    print("=" * 60)
    
    extractor = QuarterMappingExtractor()
    
    # Test NY Q1 which has patches
    print("\n1. Testing NY Q1 with patches:")
    dt_ny_q1 = datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)  # NY Q1
    config = extractor.get_quarter_config("AUD_USD", dt_ny_q1.timestamp())
    
    if config:
        print(f"   ✅ Found config for AUD_USD NY Q1")
        print(f"   Source: {config.get('source')}")
        print(f"   Session: {config.get('session')}")
        print(f"   Quarter: {config.get('quarter')}")
        
        # Check for patched values
        if "aee.strictness_mult" in config:
            print(f"   Strictness multiplier: {config['aee.strictness_mult']}")
        if "promote_mfe_atr" in config:
            print(f"   Promote MFE ATR: {config['promote_mfe_atr']}")
            
        if "evidence" in config:
            evidence = config["evidence"]
            print(f"   Evidence - n: {evidence.get('n')}")
            print(f"   Evidence - exit_result_per_h: {evidence.get('exit_result_per_h'):.3f}")
    else:
        print("   ❌ No config found")
        
    # Test base config for pairs without patches
    print("\n2. Testing base config (no patches):")
    dt_london = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)  # London Q2
    config = extractor.get_quarter_config("EUR_USD", dt_london.timestamp())
    
    if config:
        print(f"   ✅ Found base config for EUR_USD London")
        print(f"   Source: {config.get('source')}")
        print(f"   Session: {config.get('session')}")
        print(f"   Quarter: {config.get('quarter')}")
        
        # Check for base config values
        if "aee.near_tp_band_atr" in config:
            print(f"   Near TP band ATR: {config['aee.near_tp_band_atr']}")
        if "entry.tick.base_max_dist_atr" in config:
            print(f"   Base max dist ATR: {config['entry.tick.base_max_dist_atr']}")
    else:
        print("   ❌ No base config found")
        
    # Test available quarters
    print("\n3. Available quarter patches:")
    available = extractor.get_available_quarters("AUD_USD")
    for session, quarters in available.items():
        if quarters:
            print(f"   {session}: {quarters}")
            
    print("\n✅ Research mapping extraction test complete")

if __name__ == "__main__":
    test_research_extraction()
