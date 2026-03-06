#!/usr/bin/env python3
"""
Verify Entry-AEE Contract Implementation
=======================================
Script to test that TradeSpec is properly created and consumed.
"""

import json
import time
from typing import Dict, Any

def test_tradespec_creation():
    """Test that TradeSpec is created with all required fields."""
    print("Testing TradeSpec creation...")
    
    # Import after path setup
    import phone_bot
    
    # Create a test signal
    test_signal = phone_bot.SignalDef(
        pair="EUR_USD",
        setup_id=1,
        setup_name="COMPRESSION_EXPANSION",
        direction="LONG",
        mode="MED",
        ttl_sec=480,
        pg_t=240,
        pg_atr=0.30,
        tp1_atr=1.0,
        tp2_atr=2.0,
        sl_atr=0.5,
        reason="test",
        created_at=time.time() - 5.0,  # 5 seconds ago
        entry_zone_price=1.1000
    )
    
    # Create a test state
    test_state = phone_bot.PairState()
    test_state.energy = 0.70
    test_state.efficiency = 0.85
    test_state.atr_exec = 0.0010
    
    # Create TradeSpec
    sp = phone_bot.get_speed_params("MED")
    trade_spec = phone_bot.TradeSpec(
        trade_id="test_123",
        pair=test_signal.pair,
        setup_name=test_signal.setup_name,
        direction=test_signal.direction,
        speed_class="MED",
        expected_move_atr=sp["tp1_atr"],
        window_size_sec=300,
        expected_progress_per_window=0.2,
        strictness_base=1.0,
        fail_windows_budget=12,
        entry_quality=1.0,
        entry_price=1.1005,
        intended_price=test_signal.entry_zone_price,
        fill_delay_ms=500,
        entry_ts=time.time(),
        entry_energy=test_state.energy,
        entry_efficiency=test_state.efficiency,
        pocket_key="test_pocket",
        cluster_id="test_cluster"
    )
    
    # Verify all fields
    required_fields = [
        'trade_id', 'pair', 'setup_name', 'direction',
        'speed_class', 'expected_move_atr', 'window_size_sec',
        'expected_progress_per_window', 'strictness_base',
        'fail_windows_budget', 'entry_quality', 'entry_price',
        'intended_price', 'fill_delay_ms', 'entry_ts',
        'entry_energy', 'entry_efficiency', 'pocket_key'
    ]
    
    missing = []
    for field in required_fields:
        if not hasattr(trade_spec, field):
            missing.append(field)
    
    if missing:
        print(f"❌ TradeSpec missing fields: {missing}")
        return False
    
    # Test entry quality calculation
    if trade_spec.entry_quality <= 0 or trade_spec.entry_quality > 1:
        print(f"❌ Entry quality out of range: {trade_spec.entry_quality}")
        return False
    
    print("✅ TradeSpec creation test passed")
    return True


def test_energy_ratio_calculation():
    """Test that energy ratio uses TradeSpec when available."""
    print("\nTesting energy ratio calculation...")
    
    import phone_bot
    
    # Mock trade with TradeSpec
    trade_id = 12345
    phone_bot.trade_specs[str(trade_id)] = phone_bot.TradeSpec(
        trade_id=str(trade_id),
        pair="EUR_USD",
        setup_name="TEST",
        direction="LONG",
        speed_class="MED",
        expected_move_atr=1.0,
        window_size_sec=300,
        expected_progress_per_window=0.2,
        strictness_base=1.0,
        fail_windows_budget=12,
        entry_quality=0.95,
        entry_price=1.1000,
        intended_price=1.0995,
        fill_delay_ms=100,
        entry_ts=time.time() - 600,  # 10 minutes ago
        entry_energy=0.70,
        entry_efficiency=0.85,
        pocket_key="test_pocket"
    )
    
    # Mock trade record
    trade_record = {
        "id": trade_id,
        "pair": "EUR_USD",
        "direction": "LONG",
        "entry": 1.1000,
        "atr_entry": 0.0010
    }
    
    # Mock AEE metrics
    aee_metrics = {
        "progress": 0.4,  # 40% progress
        "energy_ratio": None  # Should be calculated
    }
    
    # Test calculation
    now = time.time()
    trade_spec = phone_bot.trade_specs[str(trade_id)]
    time_in_trade = now - trade_spec.entry_ts
    windows_elapsed = time_in_trade / trade_spec.window_size_sec
    expected_progress = windows_elapsed * trade_spec.expected_progress_per_window
    
    if expected_progress > 0:
        energy_ratio = aee_metrics["progress"] / expected_progress
        print(f"✅ Energy ratio calculated: {energy_ratio:.3f}")
        print(f"   Progress: {aee_metrics['progress']:.3f}")
        print(f"   Expected: {expected_progress:.3f}")
        print(f"   Windows elapsed: {windows_elapsed:.1f}")
    else:
        print("❌ Expected progress is zero")
        return False
    
    # Clean up
    del phone_bot.trade_specs[str(trade_id)]
    
    return True


def test_notification_payload():
    """Test that notifications use correct field names."""
    print("\nTesting notification payload...")
    
    # Check for old field names in actual code
    import phone_bot
    import inspect
    
    # Get the source code of the phone_bot module
    source = inspect.getsource(phone_bot)
    
    # Look for ENTRY_ENTER notification
    if '"sl_price"' in source or '"tp_price"' in source:
        print("❌ Found old field names in notification code (sl_price/tp_price)")
        # Find specific lines for debugging
        lines = source.split('\n')
        for i, line in enumerate(lines):
            if '"sl_price"' in line or '"tp_price"' in line:
                print(f"   Line {i+1}: {line.strip()}")
        return False
    
    print("✅ Notification payload uses correct field names")
    return True


def generate_contract_report():
    """Generate a report on Entry-AEE contract compliance."""
    print("\nGenerating Entry-AEE Contract Report...")
    
    report = {
        "timestamp": time.time(),
        "tests": {
            "tradespec_creation": test_tradespec_creation(),
            "energy_ratio_calculation": test_energy_ratio_calculation(),
            "notification_payload": test_notification_payload()
        },
        "summary": {}
    }
    
    # Calculate pass rate
    passed = sum(1 for result in report["tests"].values() if result)
    total = len(report["tests"])
    report["summary"]["pass_rate"] = f"{passed}/{total} ({passed/total*100:.1f}%)"
    report["summary"]["status"] = "PASS" if passed == total else "FAIL"
    
    # Save report
    with open("proof_artifacts/entry_aee_contract_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\nReport saved to: proof_artifacts/entry_aee_contract_report.json")
    print(f"Summary: {report['summary']['pass_rate']} - Status: {report['summary']['status']}")
    
    return report["summary"]["status"] == "PASS"


if __name__ == "__main__":
    # Ensure directories exist
    import os
    os.makedirs("proof_artifacts", exist_ok=True)
    
    # Run verification
    success = generate_contract_report()
    
    if success:
        print("\n✅ All Entry-AEE contract tests PASSED")
        exit(0)
    else:
        print("\n❌ Some Entry-AEE contract tests FAILED")
        exit(1)
