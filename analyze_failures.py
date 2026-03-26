#!/usr/bin/env python3
"""
Clear view of failure points and fallback usage
"""

import logging
logging.basicConfig(level=logging.ERROR)

from datetime import datetime, timezone
from compiled_trading_map import CompiledTradingMap
from runtime_calibration import RuntimeCalibration

def analyze_failure_points():
    print("FAILURE POINT ANALYSIS")
    print("=" * 60)
    print("Each fallback usage = A failure in the data pipeline")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    cal_map = CompiledTradingMap()
    
    # Analyze each major pair/session combination
    all_pairs = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'EUR_JPY', 'AUD_USD', 'USD_CHF', 'AUD_CAD', 'CHF_JPY']
    sessions = [
        ('London', datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)),
        ('NY', datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)),
        ('Asia', datetime(2024, 1, 11, 5, 0, 0, tzinfo=timezone.utc))
    ]
    
    print("\nPAIR BY PAIR ANALYSIS:")
    print("-" * 60)
    
    total_failures = 0
    total_checks = 0
    
    for pair in all_pairs:
        print(f"\n{pair}:")
        pair_failures = 0
        
        for session_name, dt in sessions:
            total_checks += 1
            
            # Check if compiled data exists
            has_compiled = cal_map.is_node_available(pair, dt.timestamp())
            
            # Get actual config used
            config = cal.get_current_config(pair, dt.timestamp())
            source = config.get('source', 'unknown')
            
            if has_compiled:
                print(f"   {session_name:8}: ✅ COMPILED (primary success)")
            else:
                total_failures += 1
                pair_failures += 1
                print(f"   {session_name:8}: ❌ COMPILED FAILED")
                
                if 'research' in source:
                    print(f"            → Using RESEARCH FALLBACK")
                    if 'patch' in source:
                        print(f"            → With quarter-specific patches")
                elif 'emergency' in source:
                    print(f"            → Using EMERGENCY FALLBACK (double failure!)")
                    
        if pair_failures > 0:
            print(f"   → {pair_failures}/3 sessions FAILED compiled data")
            
    print("\n" + "=" * 60)
    print("FAILURE SUMMARY")
    print("=" * 60)
    print(f"Total checks: {total_checks}")
    print(f"Compiled data failures: {total_failures}")
    print(f"Success rate: {((total_checks - total_failures) / total_checks * 100):.1f}%")
    
    if total_failures > 0:
        print(f"\n🚨 FAILURE POINTS IDENTIFIED:")
        print(f"   - {total_failures} instances where compiled data is missing")
        print(f"   - System falls back but these are DATA PIPELINE FAILURES")
        print(f"\n📋 TO FIX THESE FAILURES:")
        print(f"   Run rebuild for missing pairs/sessions:")
        for pair in all_pairs:
            missing_sessions = []
            for session_name, dt in sessions:
                if not cal_map.is_node_available(pair, dt.timestamp()):
                    missing_sessions.append(session_name)
            if missing_sessions:
                print(f"   - {pair}: {', '.join(missing_sessions)}")
                
    print("\n" + "=" * 60)
    if total_failures == 0:
        print("✅ NO FAILURE POINTS - Perfect data coverage")
    else:
        print(f"⚠️  {total_failures} FAILURE POINTS - System works but data incomplete")
    print("=" * 60)

if __name__ == "__main__":
    analyze_failure_points()
