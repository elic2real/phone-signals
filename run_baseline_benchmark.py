#!/usr/bin/env python3
"""Baseline Benchmark: Run current phone_bot config against all scenarios."""

import json
import random
import statistics
import sys
from typing import Dict, List, Any

# Ensure we can import local modules
sys.path.append(".")

import tick_generator

# Import simulator components directly
from sim_harness import SimEnvironment, create_default_trade, Tick
import phone_bot

def run_baseline_benchmark():
    print("Collecting scenarios...")
    scenarios = list(tick_generator.SCENARIO_REGISTRY.keys())
    # Sort scenarios alphabetically for consistent output
    scenarios.sort()
    print(f"Benchmarking {len(scenarios)} scenarios...")
    
    results = {}
    detailed_results = {}
    
    # Run each scenario 5 times to get a stable average
    N_RUNS = 5
    
    try:
        for scenario_name in scenarios:
            print(f"  > {scenario_name:<30}", end="", flush=True)
            scenario_pph_values = []
            
            for i in range(N_RUNS):
                # Seed for deterministic tick generation per run index
                seed = 1000 + i
                random.seed(seed)
                
                # Generate ticks
                if scenario_name in tick_generator.SCENARIO_REGISTRY:
                    raw_ticks = tick_generator.SCENARIO_REGISTRY[scenario_name]()
                else:
                    print(f" [SKIP] ", end="")
                    continue
                    
                # Convert to Sim Tick format
                if not raw_ticks:
                    print(f" [EMPTY] ", end="")
                    continue
                    
                raw_ticks.sort(key=lambda x: x["ts"])
                
                ticks_by_inst: Dict[str, List[Tick]] = {}
                for row in raw_ticks:
                    instrument = str(row.get("instrument", "EUR_USD"))
                    tick_obj = Tick(
                        instrument=instrument,
                        ts=float(row["ts"]),
                        bid=float(row["bid"]),
                        ask=float(row["ask"])
                    )
                    if instrument not in ticks_by_inst:
                        ticks_by_inst[instrument] = []
                    ticks_by_inst[instrument].append(tick_obj)
                
                # Determine primary instrument
                # Most scenarios are single instrument
                if not ticks_by_inst:
                    print(f" [NO INST] ", end="")
                    continue
                    
                primary_pair = list(ticks_by_inst.keys())[0]

                # Create Default Trade
                trade = create_default_trade(
                    pair=primary_pair,
                    direction="LONG",
                    ticks_by_inst=ticks_by_inst,
                    atr_pips=10.0,
                    tp_atr=2.0 
                )
                
                # Init Env
                env = SimEnvironment(
                    instruments=[primary_pair],
                    ticks_by_inst=ticks_by_inst,
                    bucket_sec=5.0
                )
                
                pph = 0.0
                try:
                    # Run Replay
                    out = env.run_aee_replay(trade=trade, speed_class="FAST")
                    
                    # Extract Score
                    pph_dict = out.get("pips_per_hour", {})
                    if isinstance(pph_dict, dict):
                        pph = float(pph_dict.get("weighted", 0.0))
                    else:
                        pph = 0.0
                        
                    scenario_pph_values.append(pph)
                    
                except Exception as e:
                    # print(f"[Err: {e}]", end="")
                    pass
                finally:
                    env.restore_live_wiring()
            
            # Compute Average
            if scenario_pph_values:
                avg_pph = statistics.mean(scenario_pph_values)
                results[scenario_name] = avg_pph
                detailed_results[scenario_name] = scenario_pph_values
                print(f" {avg_pph:10.0f} pips/hr")
            else:
                results[scenario_name] = 0.0
                detailed_results[scenario_name] = []
                print(f" {'Failed':>10}")

    except KeyboardInterrupt:
        print("\nAborted.")
        return

    print("\n=== BASELINE PERFORMANCE REPORT ===")
    print(f"{'Scenario':<30} | {'Avg Pips/Hr':<15}")
    print("-" * 50)
    
    total_score = 0
    valid_count = 0
    
    for name in scenarios:
        score = results.get(name, 0.0)
        print(f"{name:<30} | {score:<15.0f}")
        if score != 0:
            total_score += score
            valid_count += 1
            
    avg_total = total_score / valid_count if valid_count > 0 else 0
    print("-" * 50)
    print(f"{'GLOBAL AVERAGE':<30} | {avg_total:<15.0f}")
    
    with open("baseline_performance.json", "w") as f:
        json.dump({
            "summary": results, 
            "details": detailed_results,
            "global_avg": avg_total
        }, f, indent=2)
        
    print(f"\nSaved report to baseline_performance.json")

if __name__ == "__main__":
    run_baseline_benchmark()
