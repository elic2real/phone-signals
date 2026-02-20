#!/usr/bin/env python3
"""Run AEE optimizer for each scenario individually."""

import sys
import subprocess
import json
import tick_generator
from pathlib import Path


def _pick_final_candidate(report: dict) -> tuple[dict | None, str]:
    best = report.get("best")
    if isinstance(best, dict) and isinstance(best.get("params"), dict):
        return best, "best_stable"

    leaderboard = report.get("leaderboard") or []
    if isinstance(leaderboard, list):
        for row in leaderboard:
            if isinstance(row, dict) and isinstance(row.get("params"), dict):
                return row, "leaderboard_top"

    pareto = report.get("pareto_front") or []
    if isinstance(pareto, list):
        for row in pareto:
            if isinstance(row, dict) and isinstance(row.get("params"), dict):
                return row, "pareto_top"

    baseline = report.get("baseline")
    if isinstance(baseline, dict) and isinstance(baseline.get("params"), dict):
        return baseline, "baseline_fallback"

    return None, "none"

def main():
    scenarios = list(tick_generator.SCENARIO_REGISTRY.keys())
    print(f"Found {len(scenarios)} scenarios: {scenarios}")

    results = {}
    final_configs = {}
    global_candidate = None
    global_candidate_scenario = None
    
    # Create artifacts dir
    base_dir = Path("scenario_optimizations")
    base_dir.mkdir(exist_ok=True)

    for scenario in scenarios:
        print(f"\n=== Optimizing for Scenario: {scenario} ===")
        
        # Define output paths
        scenario_dir = base_dir / scenario
        scenario_dir.mkdir(exist_ok=True)
        report_file = scenario_dir / "optimizer_report.json"
        
        cmd = [
            "python3", "aee_optimizer.py",
            "--scenario", scenario,
            "--trials", "10",           # Expanded search budget
            "--runs-per-trial", "50",   # Valid comparison
            "--stability-runs", "100",  # Minimal stability check
            "--distribution-runs", "50", # Quick gating
            "--artifact-dir", str(scenario_dir),
            "--output", str(report_file)
        ]
        
        print(f"Running: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            
            # Load result
            if report_file.exists():
                with open(report_file, "r") as f:
                    data = json.load(f)
                    
                baseline = data.get("baseline")
                selected, selected_source = _pick_final_candidate(data)

                selected_pph = float(selected.get("median_weighted_pips_per_hour", 0.0) if selected else 0.0)
                base_pph = float(baseline.get("median_weighted_pips_per_hour", 0.0) if baseline else 0.0)
                selected_params = selected.get("params") if selected else None

                if isinstance(selected_params, dict):
                    final_configs[scenario] = selected_params
                    if global_candidate is None or selected_pph > float(global_candidate.get("median_weighted_pips_per_hour", 0.0)):
                        global_candidate = selected
                        global_candidate_scenario = scenario
                
                results[scenario] = {
                    "baseline_pph": base_pph,
                    "selected_pph": selected_pph,
                    "improvement": (selected_pph - base_pph),
                    "selected_source": selected_source,
                    "best_params": selected_params,
                }
                print(f"  -> Baseline: {base_pph:.1f}, Selected({selected_source}): {selected_pph:.1f}")
            else:
                print(f"  -> No report generated for {scenario}")
                
        except subprocess.CalledProcessError as e:
            print(f"  -> Failed: {e}")
        except Exception as e:
            print(f"  -> Error: {e}")

    # Summarize
    print("\n\n=== OVERALL SUMMARY ===")
    print(f"{'Scenario':<30} | {'Baseline PPH':<12} | {'Selected PPH':<12} | {'Improvement':<12}")
    print("-" * 75)
    for name, res in results.items():
        print(f"{name:<30} | {res['baseline_pph']:<12.1f} | {res['selected_pph']:<12.1f} | {res['improvement']:<12.1f}")

    # Dump all results
    with open("scenario_optimization_summary.json", "w") as f:
        json.dump(results, f, indent=2)

    with open("scenario_final_configs.json", "w") as f:
        json.dump(final_configs, f, indent=2)

    if isinstance(global_candidate, dict) and isinstance(global_candidate.get("params"), dict):
        with open("best_high_yield_config.json", "w") as f:
            json.dump(global_candidate["params"], f, indent=2)
        with open("best_high_yield_config.meta.json", "w") as f:
            json.dump(
                {
                    "source_scenario": global_candidate_scenario,
                    "source_type": "max_selected_median_weighted_pips_per_hour",
                    "selected_pph": float(global_candidate.get("median_weighted_pips_per_hour", 0.0)),
                },
                f,
                indent=2,
            )
        print(f"\nWrote global config from scenario '{global_candidate_scenario}' -> best_high_yield_config.json")
    else:
        print("\nNo global config written (no valid candidate params found).")

if __name__ == "__main__":
    main()
