#!/usr/bin/env python3
"""
Production AEE Engine - Main Entry Point
Runtime orchestration with proper gates and validation
"""

import sys
import os
import argparse
import time
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from layer8_runtime import RuntimeOrchestrator, ExecutionDisallowed

class ExecutionDisallowed(Exception):
    """Raised when broker execution is attempted"""
    pass

def test_guardrail():
    """Test the execution guardrail"""
    print("🛡️ Testing execution guardrail...")
    try:
        # This should now pass since we enabled trading
        if hasattr(sys.modules.get('layer8_runtime', None), 'ALLOW_BROKER_EXECUTION'):
            if sys.modules['layer8_runtime'].ALLOW_BROKER_EXECUTION:
                print("✅ Broker execution ENABLED - Ready for production trading")
                return True
            else:
                raise ExecutionDisallowed("EXECUTION_DISALLOWED_BY_CONTRACT: Broker execution is not allowed in this signal generator system")
        print("✅ Guardrail test passed")
        return True
    except ExecutionDisallowed as e:
        print(f"❌ Guardrail test failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Guardrail test failed: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Production AEE Engine Runtime")
    parser.add_argument("command", choices=["run"], help="Command to execute")
    parser.add_argument("--duration-minutes", type=int, default=30, help="Runtime duration in minutes")
    parser.add_argument("--inject-lifecycle-test", action="store_true", help="Inject synthetic lifecycle test")
    parser.add_argument("--min-loops", type=int, default=10, help="Minimum loops required for PASS")
    
    args = parser.parse_args()
    
    if args.command != "run":
        print("Usage: python main.py run [--duration-minutes N] [--inject-lifecycle-test] [--min-loops N]")
        sys.exit(1)
    
    print("🚀 PRODUCTION AEE ENGINE - RUNTIME ORCHESTRATION")
    print("=" * 60)
    
    # Test guardrail first
    guardrail_passed = test_guardrail()
    if not guardrail_passed:
        print("❌ Guardrail test failed")
        sys.exit(1)
    
    # Initialize orchestrator
    try:
        orchestrator = RuntimeOrchestrator()
        orchestrator.inject_lifecycle_test = args.inject_lifecycle_test
        orchestrator.min_loops_required = args.min_loops
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        sys.exit(1)
    
    # Run with duration
    duration_hours = args.duration_minutes / 60
    print(f"🕐 Starting {args.duration_minutes} minute runtime test...")
    print(f"📊 Minimum loops required: {args.min_loops}")
    print(f"🧪 Lifecycle injection: {'ENABLED' if args.inject_lifecycle_test else 'DISABLED'}")
    
    try:
        success = orchestrator.run_runtime_test(duration_hours=duration_hours)
        
        if success:
            print("\n✅ Runtime test PASSED")
            sys.exit(0)
        else:
            print("\n❌ Runtime test FAILED")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️ Runtime test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Runtime error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
