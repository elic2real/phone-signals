#!/usr/bin/env python3
"""
Debug research mapping extraction
"""

import logging
logging.basicConfig(level=logging.INFO)

from datetime import datetime, timezone
from quarter_mapping_extractor import QuarterMappingExtractor

def debug_extraction():
    print("Debug Research Mapping Extraction")
    print("=" * 60)
    
    extractor = QuarterMappingExtractor()
    
    # Force load
    extractor._load_tune_map_data()
    
    print(f"\nLoaded {len(extractor._quarter_cache)} patches")
    print(f"Base config loaded: {bool(extractor._base_config)}")
    
    # Show cached patches
    print("\nCached patches:")
    for key, data in extractor._quarter_cache.items():
        print(f"   {key}: {data['source']}")
        
    # Test NY Q1
    dt_ny_q1 = datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)
    session = "ny"
    quarter = "Q1"
    
    print(f"\nLooking for AUD_USD_{session}_{quarter}")
    cache_key = f"AUD_USD_{session}_{quarter}"
    
    if cache_key in extractor._quarter_cache:
        print("✅ Found in cache!")
        patch_data = extractor._quarter_cache[cache_key]
        print(f"   Source: {patch_data['source']}")
        print(f"   AEE patch: {patch_data.get('aee_patch', {})}")
        
        config = extractor._prepare_config(patch_data)
        print(f"\nPrepared config:")
        print(f"   Strictness: {config.get('aee.strictness_mult', 'not found')}")
        print(f"   Source: {config.get('source')}")
    else:
        print("❌ Not found in cache")
        
    # Check base config
    print("\nChecking base config for AUD_USD NY:")
    base = extractor._get_base_config("AUD_USD", "ny", "Q1")
    if base:
        print(f"   Base strictness: {base.get('aee.strictness_mult', 'not found')}")

if __name__ == "__main__":
    debug_extraction()
