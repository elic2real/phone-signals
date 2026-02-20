import pytest
import csv
import os
import hashlib
from tick_generator import (
    SCENARIO_REGISTRY, 
    SCENARIO_VERSION, 
    run_hostile_auditor_generation
)

GOLDEN_DIR = f"scenarios/golden/{SCENARIO_VERSION}"

def test_golden_artifacts_existence():
    """Ensure all scenarios have a corresponding golden CSV."""
    assert os.path.isdir(GOLDEN_DIR), f"Golden directory {GOLDEN_DIR} missing"
    for name in SCENARIO_REGISTRY:
        csv_path = os.path.join(GOLDEN_DIR, f"{name}.csv")
        assert os.path.exists(csv_path), f"Missing golden file: {csv_path}"

def test_scenario_deterministic_reproducibility():
    """Ensure running the generator again produces identical output (byte-for-byte)."""
    # Create a temp dir for new generation
    temp_dir = f"scenarios/temp_test_{SCENARIO_VERSION}"
    if os.path.exists(temp_dir):
        import shutil
        shutil.rmtree(temp_dir)
    
    # Run generation to temp dir
    run_hostile_auditor_generation(output_dir=temp_dir)
    
    # Compare files
    for name in SCENARIO_REGISTRY:
        golden_path = os.path.join(GOLDEN_DIR, f"{name}.csv")
        temp_path = os.path.join(temp_dir, f"{name}.csv")
        
        with open(golden_path, "rb") as f1, open(temp_path, "rb") as f2:
            assert f1.read() == f2.read(), f"Scenario {name} is not deterministic!"
            
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)

def read_ticks(name):
    csv_path = os.path.join(GOLDEN_DIR, f"{name}.csv")
    ticks = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert to appropriate types
            row["ts"] = float(row["ts"])
            row["bid"] = float(row["bid"])
            row["ask"] = float(row["ask"])
            row["mid"] = float(row["mid"])
            ticks.append(row)
    return ticks

@pytest.mark.parametrize("scenario_name", list(SCENARIO_REGISTRY.keys()))
def test_scenario_invariants_generic(scenario_name):
    """Check universal invariants for all scenarios."""
    ticks = read_ticks(scenario_name)
    
    # 1. Wall time >= 600s
    duration = ticks[-1]["ts"] - ticks[0]["ts"]
    assert duration >= 600, f"{scenario_name}: Duration {duration} < 600s"
    
    # 2. Monotonic time
    timestamps = [t["ts"] for t in ticks]
    assert all(x < y for x, y in zip(timestamps, timestamps[1:])), f"{scenario_name}: Non-monotonic timestamps"

    # 3. Spread sanity
    # Determine pip size context from instrument name in first tick
    instr = ticks[0]["instrument"]
    is_jpy = "JPY" in instr
    max_spread = 0.50 if is_jpy else 0.0050
    
    for t in ticks:
        # Note: spread in CSV is ask-bid
        spread = float(t["ask"]) - float(t["bid"])
        assert spread > 0, f"{scenario_name}: Negative spread at {t['ts']}"
        # Max spread cap
        assert spread < max_spread, f"{scenario_name}: Spread {spread} > {max_spread} limit at {t['ts']}"


def test_whipsaw_structural_invariant():
    """Verify 'whipsaw_spike_fade' actually contains the required shape."""
    ticks = read_ticks("whipsaw_spike_fade")
    mids = [t["mid"] for t in ticks]
    
    # Find max peak
    peak_val = max(mids)
    peak_idx = mids.index(peak_val)
    
    # Assert peak is somewhat late (after accumulation) but not at very end
    assert 200 < peak_idx < len(mids) - 50, "Peak not in expected window"
    
    # Check for sharp rise (spike)
    # Look at 20 ticks before peak
    pre_peak = mids[peak_idx - 20]
    spike_amp = peak_val - pre_peak
    # Should be at least ~20 pips for a "spike"
    assert spike_amp > 0.0020, f"Spike amplitude {spike_amp} too small"
    
    # Check for giveback (fade)
    # Look at 30 ticks after peak
    post_peak = mids[peak_idx + 30]
    giveback = peak_val - post_peak
    assert giveback > 0.0010, f"Giveback {giveback} too small"

def test_panic_reversal_invariant():
    """Verify 'panic_reversal' contains extreme volatility."""
    ticks = read_ticks("panic_reversal")
    # Identify the 'panic' phase by looking for max range in a window
    # The scenario has UP then DOWN.
    mids = [t["mid"] for t in ticks]
    total_range = max(mids) - min(mids)
    
    # For USD_JPY (0.01 pip), 110.50 -> ...
    # Drift 0.0002.
    assert total_range > 0.50, f"Panic reversal range {total_range} too small for major event"

if __name__ == "__main__":
    # Allow running directly
    sys.exit(pytest.main(["-v", __file__]))
