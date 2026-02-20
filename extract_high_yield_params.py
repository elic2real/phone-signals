#!/usr/bin/env python3
"""Extract high-yield parameters from optimizer artifacts."""

import argparse
import json
import os
from pathlib import Path
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="scenario_optimizations")
    parser.add_argument("--threshold", type=float, default=0.1)
    args = parser.parse_args()
    
    base_dir = Path(args.dir)
    all_runs = []

    print(f"Scanning {base_dir} for runs > {args.threshold} pips/hr...")

    for root, dirs, files in os.walk(base_dir):
        # Scan all subdirectories if --dir is provided
        
        for file in files:
            if file.startswith("sim_results_") and file.endswith(".json"):
                path = Path(root) / file
                # Skip files > 5MB to be safe, though 3.5MB should be fine.
                if path.stat().st_size > 10 * 1024 * 1024:
                    print(f"Skipping large file: {path}")
                    continue
                    
                try:
                    with open(path, "r") as f:
                        data = json.load(f)
                    
                    # Extract key metrics
                    # Some files might have different structures depending on if they are single run results or summaries
                    # The optimizer outputs 'sim_results_best_XX.json' which are usually lists of runs or a single run dict
                    
                    # Handle if it's a list of runs (from collect_runs=True) or single run
                    runs_to_process = []
                    if isinstance(data, list):
                        runs_to_process = data
                    elif isinstance(data, dict):
                        # It might be a summary or a single run
                        if "runs" in data:
                            runs_to_process = data["runs"]
                        else:
                            runs_to_process = [data]

                    for r in runs_to_process:
                        pph = float(r.get("weighted_pips_per_hour", 0.0))
                        
                        # Fallback: Check if pips_per_hour is a dict (sim artifacts)
                        if pph == 0.0:
                            pph_dict = r.get("pips_per_hour")
                            if isinstance(pph_dict, dict):
                                pph = float(pph_dict.get("weighted", 0.0))

                        # Fallback: Calculate PPH manually if missing
                        if pph == 0.0 and "weighted_pips" in r and "hold_sec" in r:
                            weighted_pips = float(r["weighted_pips"])
                            hold_sec = float(r["hold_sec"])
                            if hold_sec > 0:
                                pph = (weighted_pips / hold_sec) * 3600.0

                        scenario = r.get("scenario", "unknown")
                        params = r.get("params", {})
                        
                        # We only care about the big winners
                        if pph > args.threshold: 
                            all_runs.append({
                                "pph": pph,
                                "scenario": scenario,
                                "file": str(path),
                                "params": params,
                                "exit_reason": r.get("exit_reason"),
                                "hold_sec": r.get("hold_sec")
                            })

                except Exception as e:
                    pass
                    # print(f"Skipping {path}: {e}")

    # Sort by PPH descending
    all_runs.sort(key=lambda x: x["pph"], reverse=True)

    print(f"\nFound {len(all_runs)} runs (> {args.threshold} pips/hr).")
    
    if not all_runs:
        print("No suitable runs found.")
        return

    print("\n=== TOP 5 GLOBAL CONFIGURATIONS ===")
    
    # Deduplicate by params hash to avoid listing the same config multiple times
    seen_configs = set()
    top_unique = []
    
    for r in all_runs:
        # Create a hashable representation of params
        p_str = json.dumps(r["params"], sort_keys=True)
        if p_str not in seen_configs:
            seen_configs.add(p_str)
            top_unique.append(r)
            if len(top_unique) >= 5:
                break
    
    for i, r in enumerate(top_unique, 1):
        hold_sec = r.get('hold_sec') or 0.0
        print(f"\nRank {i}: {r['pph']:.0f} pips/hr | Scenario: {r['scenario']}")
        print(f"Exit: {r['exit_reason']} | Hold: {hold_sec:.1f}s")
        print("Parameters:")
        for k, v in r["params"].items():
            print(f"  {k}: {v}")
            
    # Save the best to a file for easy application
    if top_unique:
        best = top_unique[0]
        with open("best_high_yield_config.json", "w") as f:
            json.dump(best["params"], f, indent=2)
        print("\n\n(Saved best parameters to best_high_yield_config.json)")

if __name__ == "__main__":
    main()
