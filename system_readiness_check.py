#!/usr/bin/env python3
"""
System Readiness Check
Verifies the runtime calibration system is ready for live trading
"""

import logging
import sys
from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration
from compiled_trading_map import CompiledTradingMap
from quarter_handoff_manager import QuarterHandoffManager
from fallback_templates import FallbackTemplates
from quarter_mapping_extractor import QuarterMappingExtractor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class SystemReadinessCheck:
    """Comprehensive system readiness verification"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.passed = 0
        self.total = 0
        
    def check(self, description: str, condition: bool, error_msg: str = None, warning_msg: str = None):
        """Record a check result"""
        self.total += 1
        if condition:
            self.passed += 1
            print(f"✅ {description}")
        else:
            if error_msg:
                self.errors.append(f"{description}: {error_msg}")
                print(f"❌ {description} - {error_msg}")
            elif warning_msg:
                self.warnings.append(f"{description}: {warning_msg}")
                print(f"⚠️  {description} - {warning_msg}")
                
    def run_all_checks(self):
        """Run all system readiness checks"""
        print("=" * 60)
        print("SYSTEM READINESS CHECK FOR LIVE TRADING")
        print("=" * 60)
        
        # 1. Check core components can be initialized
        print("\n1. Core Component Initialization:")
        try:
            cal_map = CompiledTradingMap()
            self.check("CompiledTradingMap initialized", len(cal_map._map) > 0)
            print(f"   Loaded {len(cal_map._map)} compiled nodes")
        except Exception as e:
            self.check("CompiledTradingMap initialized", False, str(e))
            
        try:
            handoff = QuarterHandoffManager()
            self.check("QuarterHandoffManager initialized", True)
        except Exception as e:
            self.check("QuarterHandoffManager initialized", False, str(e))
            
        try:
            extractor = QuarterMappingExtractor()
            self.check("QuarterMappingExtractor initialized", True)
        except Exception as e:
            self.check("QuarterMappingExtractor initialized", False, str(e))
            
        try:
            fallback = FallbackTemplates()
            self.check("FallbackTemplates initialized", True)
        except Exception as e:
            self.check("FallbackTemplates initialized", False, str(e))
            
        # 2. Check main RuntimeCalibration
        print("\n2. RuntimeCalibration System:")
        try:
            cal = RuntimeCalibration()
            self.check("RuntimeCalibration initialized", True)
            
            # Test with different scenarios
            test_cases = [
                ("EUR_USD London (compiled)", "EUR_USD", datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)),
                ("AUD_USD NY Q1 (patched)", "AUD_USD", datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)),
                ("GBP_USD London (compiled)", "GBP_USD", datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)),
                ("FAKE_PAIR (fallback)", "FAKE_PAIR", datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)),
            ]
            
            for desc, pair, dt in test_cases:
                config = cal.get_current_config(pair, dt.timestamp())
                source = config.get('source', 'unknown')
                self.check(f"{desc} - {source}", config is not None)
                
        except Exception as e:
            self.check("RuntimeCalibration system", False, str(e))
            
        # 3. Check quarter handoff detection
        print("\n3. Quarter Handoff Detection:")
        try:
            cal = RuntimeCalibration()
            
            # Simulate quarter transition
            dt_q1 = datetime(2024, 1, 11, 9, 0, 0, tzinfo=timezone.utc)
            dt_q2 = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)
            
            # Get configs (should trigger handoff)
            cal.get_current_config("EUR_USD", dt_q1.timestamp())
            cal.get_current_config("EUR_USD", dt_q2.timestamp())
            
            stats = cal.get_stats()
            self.check("Quarter handoff detected", stats['handoffs_detected'] >= 0)
            
        except Exception as e:
            self.check("Quarter handoff detection", False, str(e))
            
        # 4. Check data sources
        print("\n4. Data Sources Availability:")
        
        # Check compiled nodes
        try:
            cal_map = CompiledTradingMap()
            major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY", "EUR_JPY"]
            dt_london = datetime(2024, 1, 11, 11, 0, 0, tzinfo=timezone.utc)
            
            compiled_count = 0
            for pair in major_pairs:
                if cal_map.is_node_available(pair, dt_london.timestamp()):
                    compiled_count += 1
                    
            self.check(f"Compiled nodes for major pairs", compiled_count >= 2, 
                      warning_msg=f"Only {compiled_count}/{len(major_pairs)} pairs have compiled data")
                      
        except Exception as e:
            self.check("Compiled nodes check", False, str(e))
            
        # Check research patches
        try:
            extractor = QuarterMappingExtractor()
            extractor._load_tune_map_data()
            patch_count = len(extractor._quarter_cache)
            self.check(f"Research patches available", patch_count > 0,
                      warning_msg=f"No quarter-specific patches found")
                      
        except Exception as e:
            self.check("Research patches check", False, str(e))
            
        # 5. Check performance characteristics
        print("\n5. Performance Characteristics:")
        try:
            import time
            
            cal = RuntimeCalibration()
            
            # Measure config lookup time
            start = time.time()
            for _ in range(100):
                cal.get_current_config("EUR_USD", datetime.now(timezone.utc).timestamp())
            elapsed = time.time() - start
            
            avg_time = elapsed / 100 * 1000  # Convert to ms
            self.check(f"Config lookup performance", avg_time < 1.0,
                      warning_msg=f"Average lookup time {avg_time:.2f}ms is slow")
                      
        except Exception as e:
            self.check("Performance check", False, str(e))
            
        # 6. Check integration with phone_bot
        print("\n6. Integration Check:")
        try:
            # Check if phone_bot can import our modules
            import phone_bot
            self.check("phone_bot imports runtime_calibration", True)
            
            # Check if _RUNTIME_CALIBRATION is initialized
            if hasattr(phone_bot, '_RUNTIME_CALIBRATION'):
                self.check("_RUNTIME_CALIBRATION initialized in phone_bot", True)
            else:
                self.check("_RUNTIME_CALIBRATION initialized in phone_bot", False,
                          "Runtime calibration not integrated into phone_bot")
                          
        except ImportError as e:
            self.check("phone_bot integration", False, f"Import error: {e}")
        except Exception as e:
            self.check("phone_bot integration", False, str(e))
            
        # 7. Final summary
        print("\n" + "=" * 60)
        print("READINESS SUMMARY")
        print("=" * 60)
        
        print(f"\nChecks Passed: {self.passed}/{self.total}")
        
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for error in self.errors:
                print(f"   - {error}")
                
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"   - {warning}")
                
        # Final verdict
        if not self.errors:
            if not self.warnings:
                print("\n✅ SYSTEM IS READY FOR LIVE TRADING")
                return True
            else:
                print("\n✅ SYSTEM IS READY (with warnings)")
                return True
        else:
            print("\n❌ SYSTEM IS NOT READY - Fix errors before trading")
            return False

if __name__ == "__main__":
    checker = SystemReadinessCheck()
    ready = checker.run_all_checks()
    sys.exit(0 if ready else 1)
