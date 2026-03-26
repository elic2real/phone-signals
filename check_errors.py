#!/usr/bin/env python3
"""
Show only errors and warnings from system readiness
"""

import logging
import sys
from datetime import datetime, timezone

# Suppress all logging except errors
logging.basicConfig(level=logging.ERROR)

from compiled_trading_map import CompiledTradingMap
from quarter_mapping_extractor import QuarterMappingExtractor

def check_errors():
    print("CHECKING FOR ERRORS AND WARNINGS")
    print("=" * 60)
    
    error_count = 0
    warning_count = 0
    
    # 1. Check compiled nodes loading
    print("\n1. Compiled Nodes Loading Issues:")
    try:
        import logging
        # Capture warnings
        log_capture = []
        
        class WarningCapture(logging.Handler):
            def emit(self, record):
                if record.levelno == logging.WARNING:
                    log_capture.append(record.getMessage())
                    
        handler = WarningCapture()
        logging.getLogger('compiled_trading_map').addHandler(handler)
        logging.getLogger('compiled_trading_map').setLevel(logging.WARNING)
        
        cal_map = CompiledTradingMap()
        
        if log_capture:
            print(f"   ⚠️  Found {len(log_capture)} warnings during loading:")
            # Show unique warnings
            unique_warnings = set()
            for warning in log_capture:
                if "Failed to load config for" in warning:
                    pair_session = warning.split("Failed to load config for ")[1].split(" ")[0]
                    unique_warnings.add(pair_session)
                    
            for item in sorted(unique_warnings)[:10]:  # Show first 10
                print(f"      - {item}")
            if len(unique_warnings) > 10:
                print(f"      ... and {len(unique_warnings) - 10} more")
            warning_count += len(unique_warnings)
        else:
            print("   ✅ No warnings during compiled nodes loading")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        error_count += 1
        
    # 2. Check which pairs/sessions are missing compiled data
    print("\n2. Missing Compiled Data Analysis:")
    if 'cal_map' in locals():
        all_pairs = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'EUR_JPY', 'AUD_USD', 'USD_CHF']
        sessions = ['london', 'new_york', 'asia', 'sydney']
        dt = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)
        
        missing_data = []
        for pair in all_pairs:
            for session in sessions:
                # Try different days
                for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
                    # Create a test timestamp for this session
                    test_dt = dt.replace(hour=11)  # Use a time that should be in session
                    if not cal_map.is_node_available(pair, test_dt.timestamp()):
                        missing_data.append(f"{pair} {session}")
                        break
                        
        if missing_data:
            unique_missing = set(missing_data)
            print(f"   ⚠️  Pairs/sessions without compiled data:")
            for item in sorted(unique_missing)[:20]:
                print(f"      - {item}")
            if len(unique_missing) > 20:
                print(f"      ... and {len(unique_missing) - 20} more")
            warning_count += len(unique_missing)
        else:
            print("   ✅ All major pairs have compiled data")
            
    # 3. Check research mapping issues
    print("\n3. Research Mapping Issues:")
    try:
        extractor = QuarterMappingExtractor()
        extractor._load_tune_map_data()
        
        if not extractor._base_config:
            print("   ❌ No base tune_map_seed.json loaded")
            error_count += 1
        else:
            print("   ✅ Base tune_map_seed.json loaded")
            
        patch_count = len(extractor._quarter_cache)
        if patch_count == 0:
            print("   ⚠️  No quarter-specific patches found")
            warning_count += 1
        else:
            print(f"   ✅ Found {patch_count} quarter-specific patches")
            
    except Exception as e:
        print(f"   ❌ Error loading research mapping: {e}")
        error_count += 1
        
    # 4. Summary
    print("\n" + "=" * 60)
    print("ERROR/WARNING SUMMARY")
    print("=" * 60)
    print(f"\nErrors: {error_count}")
    print(f"Warnings: {warning_count}")
    
    if error_count == 0:
        if warning_count == 0:
            print("\n✅ No issues found - system is clean")
        else:
            print(f"\n⚠️  {warning_count} warnings found - system should still work")
            print("\nRecommendations:")
            print("   - Warnings are expected for missing compiled data")
            print("   - System will fall back to research mapping for these cases")
            print("   - Consider running rebuild for missing pairs if needed")
    else:
        print(f"\n❌ {error_count} errors found - must fix before trading")
        
    return error_count == 0

if __name__ == "__main__":
    success = check_errors()
    sys.exit(0 if success else 1)
