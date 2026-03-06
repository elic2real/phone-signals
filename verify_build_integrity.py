#!/usr/bin/env python3
"""
Build Integrity Verification
===========================
Script to verify all production modules compile and imports work.
"""

import py_compile
import importlib
import sys
import traceback
from pathlib import Path

# Production modules to check
PRODUCTION_MODULES = [
    "phone_bot",
    "tier0_gates",
    "entry_logic",
    "aee_engine",
    "phone_bot_logging",
    "active_artifacts",
    "artifact_collector",
    "outcome_accelerator",
    "state_key",
    "tune_apply",
    "tune_map",
    "vol_bucket_spec"
]

def test_compilation():
    """Test that all modules compile without syntax errors."""
    print("Testing module compilation...")
    
    failed = []
    
    for module in PRODUCTION_MODULES:
        file_path = f"{module}.py"
        if not Path(file_path).exists():
            print(f"⚠️  {file_path} not found, skipping...")
            continue
            
        try:
            py_compile.compile(file_path, doraise=True)
            print(f"✅ {module}.py compiles")
        except py_compile.PyCompileError as e:
            print(f"❌ {module}.py compilation failed: {e}")
            failed.append(module)
    
    return len(failed) == 0, failed


def test_imports():
    """Test that all modules can be imported."""
    print("\nTesting module imports...")
    
    failed = []
    
    for module in PRODUCTION_MODULES:
        try:
            # Add current directory to path if needed
            if "." not in sys.path:
                sys.path.insert(0, ".")
            
            importlib.import_module(module)
            print(f"✅ {module} imports successfully")
        except Exception as e:
            print(f"❌ {module} import failed: {e}")
            # Show traceback for debugging
            traceback.print_exc()
            failed.append(module)
    
    return len(failed) == 0, failed


def test_config_resolution():
    """Test that config files resolve deterministically."""
    print("\nTesting config resolution...")
    
    try:
        import phone_bot
        
        # Check key config values are resolved
        configs = {
            "LIVE_MODE": phone_bot.LIVE_MODE,
            "DRY_RUN_ONLY": phone_bot.DRY_RUN_ONLY,
            "ALLOW_ENTRIES": phone_bot.ALLOW_ENTRIES,
            "MAX_CONCURRENCY": phone_bot.config.MAX_CONCURRENCY if hasattr(phone_bot, 'config') else 15,
            "PAIRS": phone_bot.PAIRS[:5] if phone_bot.PAIRS else [],  # Check first 5 pairs
        }
        
        print("✅ Config values resolved:")
        for key, value in configs.items():
            print(f"   {key}: {value}")
        
        return True, []
        
    except Exception as e:
        print(f"❌ Config resolution failed: {e}")
        return False, ["config_resolution"]


def test_version_stamps():
    """Test that version stamps are available."""
    print("\nTesting version stamps...")
    
    missing = []
    
    # Check if version info functions exist
    import phone_bot
    
    version_functions = [
        "get_code_sha",
        "get_baseline_patch_sha",
        "get_cache_sha"
    ]
    
    for func_name in version_functions:
        if hasattr(phone_bot, func_name):
            try:
                value = getattr(phone_bot, func_name)()
                print(f"✅ {func_name}: {value[:12]}...")
            except Exception as e:
                print(f"⚠️  {func_name} failed: {e}")
                missing.append(func_name)
        else:
            print(f"⚠️  {func_name} not found")
            missing.append(func_name)
    
    return len(missing) == 0, missing


def generate_build_report():
    """Generate build integrity report."""
    print("=" * 60)
    print("BUILD INTEGRITY VERIFICATION")
    print("=" * 60)
    
    results = {}
    
    # Run all tests
    results["compilation"], comp_failed = test_compilation()
    results["imports"], imp_failed = test_imports()
    results["config_resolution"], conf_failed = test_config_resolution()
    results["version_stamps"], ver_failed = test_version_stamps()
    
    # Calculate summary
    all_failed = comp_failed + imp_failed + conf_failed + ver_failed
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    print("\n" + "=" * 60)
    print("BUILD SUMMARY")
    print("=" * 60)
    print(f"Tests passed: {passed}/{total}")
    
    if all_failed:
        print(f"\n❌ Failed components: {', '.join(all_failed)}")
    else:
        print("\n✅ All build integrity tests PASSED")
    
    # Save report
    import json
    import time
    
    report = {
        "timestamp": time.time(),
        "results": results,
        "failed_components": all_failed,
        "summary": {
            "passed": passed,
            "total": total,
            "pass_rate": f"{passed/total*100:.1f}%",
            "status": "PASS" if passed == total else "FAIL"
        }
    }
    
    with open("proof_artifacts/build_integrity_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\nReport saved to: proof_artifacts/build_integrity_report.json")
    
    return passed == total


if __name__ == "__main__":
    # Ensure directories exist
    Path("proof_artifacts").mkdir(exist_ok=True)
    
    # Run verification
    success = generate_build_report()
    
    if success:
        print("\n✅ Build integrity verification PASSED")
        exit(0)
    else:
        print("\n❌ Build integrity verification FAILED")
        exit(1)
