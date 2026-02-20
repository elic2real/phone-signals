#!/usr/bin/env python3
"""
Whipsaw Verification Benchmark
Run targeted simulation on whipsaw scenarios to verify:
1. Overshoot frequency (WHIPSAW_SPIKE_OVERSHOOT_CAPTURE)
2. Stall rejection frequency (WHIPSAW_REJECTION_CAPTURE)
"""
import sys
import tick_generator
from sim_harness import SimEnvironment, create_default_trade, Tick
from collections import Counter

def run_whipsaw_verification():
    # Only target scenarios likely to produce whipsaws
    targets = [
        "whipsaw_spike_fade",
        "whipsaw_chop_sl", # Might trigger rejection
        "pulse_blow_off",  # Might trigger overshoot
        "high_energy_trend", # Control
        "news_spike_reversal" # Strong candidate for overshoot
    ]
    
    print(f"{'SCENARIO':<30} | {'EXIT REASON':<35} | {'PIPS':<6} | {'SPEED':<5} | {'PEAK':<5}")
    print("-" * 95)
    
    counts = Counter()
    total_pips = {}
    
    for scenario in targets:
        if scenario not in tick_generator.SCENARIO_REGISTRY:
            continue
            
        # Run 3 iterations per scenario
        for i in range(3):
            raw_ticks = tick_generator.SCENARIO_REGISTRY[scenario]()
            if not raw_ticks: continue
            
            raw_ticks.sort(key=lambda x: x["ts"])
            ticks_by_inst = {}
            for row in raw_ticks:
                inst = str(row.get("instrument", "EUR_USD"))
                tick = Tick(inst, float(row["ts"]), float(row["bid"]), float(row["ask"]))
                if inst not in ticks_by_inst: ticks_by_inst[inst] = []
                ticks_by_inst[inst].append(tick)
            
            primary = list(ticks_by_inst.keys())[0]
            
            # Setup trade with tight TP to force AEE logic to work
            trade = create_default_trade(primary, "LONG", ticks_by_inst, atr_pips=10, tp_atr=1.5)
            # Override TP to be small to test "virtual TP" logic
            # SimHarness creates default trade with entry logic, we want to simulate the AEE behavior
            
            env = SimEnvironment([primary], ticks_by_inst, bucket_sec=1.0)
            
            out = env.run_aee_replay(trade, speed_class="FAST")
            
            res = out.get("exit", {})
            reason = res.get("reason", "NONE")
            metrics = res.get("metrics", {})
            
            pips = out.get("total_weighted_pips", 0.0)
            speed = metrics.get("speed", 0.0)
            peak = metrics.get("overshoot_peak_atr", 0.0)
            
            counts[reason] += 1
            if reason not in total_pips: total_pips[reason] = []
            total_pips[reason].append(pips)
            
            if "WHIPSAW" in reason or i == 0:
                print(f"{scenario:<30} | {reason:<35} | {pips:6.1f} | {speed:5.2f} | {peak:5.2f}")

    print("\n=== SUMMARY ===")
    for reason, count in counts.items():
        avg_pips = sum(total_pips[reason]) / len(total_pips[reason])
        print(f"{reason:<35}: {count:3d} hits (Avg Pips: {avg_pips:.1f})")

if __name__ == "__main__":
    run_whipsaw_verification()
