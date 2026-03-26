#!/usr/bin/env python3
"""
Phase 1 AEE testing framework with reporting and analysis.

Provides comprehensive testing of AEE logic on synthetic paths
including attribution matrices, MFE/MAE analysis, and parameter sweeps.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from synthetic_path_generator import (
    PathClass,
    SyntheticPath,
    generate_weighted_paths,
    get_path_class_weights,
)
from aee_synthetic_evaluator import AEEEvaluator, AEEKnobs, run_static_baseline


@dataclass
class TestResult:
    """Results from a single test run."""
    # Configuration
    knobs: AEEKnobs
    path_count: int
    
    # Per-path results
    path_results: List[Dict] = field(default_factory=list)
    
    # Aggregated metrics
    avg_r_static: float = 0.0
    avg_r_aee: float = 0.0
    delta_r: float = 0.0
    
    # Exit counts
    exit_counts: Dict[str, int] = field(default_factory=dict)
    class_exit_counts: Dict[PathClass, Dict[str, int]] = field(default_factory=dict)
    
    # Performance metrics
    sl_hit_rate_static: float = 0.0
    sl_hit_rate_aee: float = 0.0
    tp_hit_rate_static: float = 0.0
    tp_hit_rate_aee: float = 0.0
    
    premature_clip_rate: float = 0.0  # AEE closed before TP but static would hit TP
    loss_reduction_rate: float = 0.0  # AEE closed before SL when static would hit SL
    
    # MFE/MAE metrics
    mfe_capture_rate: float = 0.0  # How much of MFE was captured
    avg_giveback: float = 0.0


class Phase1Tester:
    """Main tester for Phase 1 AEE evaluation."""
    
    def __init__(self):
        self.results: List[TestResult] = []
    
    def run_test(
        self,
        knobs: AEEKnobs,
        path_count: int = 300,
        random_seed: Optional[int] = None,
    ) -> TestResult:
        """Run a single test with given AEE knobs."""
        
        print(f"\n{'='*60}")
        print(f"Running test with {path_count} paths")
        print(f"Knobs: profit_capture={knobs.profit_capture_min_atr:.2f}, "
              f"giveback={knobs.allowed_giveback_atr_mult:.2f}")
        print(f"{'='*60}")
        
        # Generate paths
        paths = generate_weighted_paths(
            total_paths=path_count,
            atr_pips=15.0,
            spread_pips=1.5,
            random_seed=random_seed,
        )
        
        # Initialize evaluator
        evaluator = AEEEvaluator(knobs)
        
        # Track results
        result = TestResult(knobs=knobs, path_count=path_count)
        
        # Evaluate each path
        for i, path in enumerate(paths):
            if (i + 1) % 50 == 0:
                print(f"  Processed {i + 1}/{len(paths)} paths...")
            
            # Run static baseline
            static_result = run_static_baseline(path)
            
            # Run AEE
            aee_result = evaluator.evaluate_path(path, f"trade_{i}")
            
            # Store combined result
            combined = {
                "path_class": path.path_class.value,
                "direction": path.direction,
                "static": static_result,
                "aee": aee_result,
                "delta_r": aee_result["actual_r"] - static_result["actual_r"],
            }
            result.path_results.append(combined)
        
        # Calculate aggregated metrics
        self._calculate_metrics(result)
        
        # Store result
        self.results.append(result)
        
        # Print summary
        self._print_summary(result)
        
        return result
    
    def _calculate_metrics(self, result: TestResult) -> None:
        """Calculate aggregated metrics from path results."""
        
        # Basic averages
        static_r_values = [r["static"]["actual_r"] for r in result.path_results]
        aee_r_values = [r["aee"]["actual_r"] for r in result.path_results]
        
        result.avg_r_static = statistics.mean(static_r_values)
        result.avg_r_aee = statistics.mean(aee_r_values)
        result.delta_r = result.avg_r_aee - result.avg_r_static
        
        # Exit reason counts
        for r in result.path_results:
            static_exit = r["static"]["exit_reason"]
            aee_exit = r["aee"]["exit_reason"]
            path_class = PathClass(r["path_class"])
            
            result.exit_counts[static_exit] = result.exit_counts.get(static_exit, 0) + 1
            result.exit_counts[aee_exit] = result.exit_counts.get(aee_exit, 0) + 1
            
            if path_class not in result.class_exit_counts:
                result.class_exit_counts[path_class] = {}
            
            result.class_exit_counts[path_class][static_exit] = \
                result.class_exit_counts[path_class].get(static_exit, 0) + 1
            result.class_exit_counts[path_class][aee_exit] = \
                result.class_exit_counts[path_class].get(aee_exit, 0) + 1
        
        # Hit rates
        result.sl_hit_rate_static = sum(1 for r in result.path_results 
                                       if r["static"]["exit_reason"] == "HIT_SL") / len(result.path_results)
        result.sl_hit_rate_aee = sum(1 for r in result.path_results 
                                    if r["aee"]["exit_reason"] == "SL_HIT") / len(result.path_results)
        
        result.tp_hit_rate_static = sum(1 for r in result.path_results 
                                       if r["static"]["exit_reason"] == "HIT_TP") / len(result.path_results)
        result.tp_hit_rate_aee = sum(1 for r in result.path_results 
                                    if r["aee"]["exit_reason"] in ["POST_TP", "PROFIT_CAPTURE"]) / len(result.path_results)
        
        # Special metrics
        premature_clips = 0
        loss_reductions = 0
        
        for r in result.path_results:
            # Premature clip: AEE closed before TP but static would have hit TP
            if (r["aee"]["closed_before_tp"] and 
                r["static"]["exit_reason"] == "HIT_TP" and
                r["aee"]["actual_r"] < r["static"]["actual_r"]):
                premature_clips += 1
            
            # Loss reduction: AEE closed before SL when static would hit SL
            if (r["aee"]["closed_before_sl"] and 
                r["static"]["exit_reason"] == "HIT_SL" and
                r["aee"]["actual_r"] > r["static"]["actual_r"]):
                loss_reductions += 1
        
        result.premature_clip_rate = premature_clips / len(result.path_results)
        result.loss_reduction_rate = loss_reductions / len(result.path_results)
        
        # MFE/MAE metrics
        mfe_ratios = []
        givebacks = []
        
        for r in result.path_results:
            if r["aee"]["mfe_r"] > 0:
                capture_ratio = r["aee"]["actual_r"] / r["aee"]["mfe_r"]
                mfe_ratios.append(min(1.0, capture_ratio))
            
            if r["aee"]["giveback_r"] > 0:
                givebacks.append(r["aee"]["giveback_r"])
        
        result.mfe_capture_rate = statistics.mean(mfe_ratios) if mfe_ratios else 0.0
        result.avg_giveback = statistics.mean(givebacks) if givebacks else 0.0
    
    def _print_summary(self, result: TestResult) -> None:
        """Print test summary."""
        
        print(f"\n{'='*60}")
        print(f"TEST SUMMARY")
        print(f"{'='*60}")
        
        # Core metrics
        print(f"\nCore Performance:")
        print(f"  Static avg R:    {result.avg_r_static:+.3f}")
        print(f"  AEE avg R:       {result.avg_r_aee:+.3f}")
        print(f"  Delta R:         {result.delta_r:+.3f}")
        
        # Hit rates
        print(f"\nHit Rates:")
        print(f"  SL hit - Static: {result.sl_hit_rate_static:.1%}")
        print(f"  SL hit - AEE:    {result.sl_hit_rate_aee:.1%}")
        print(f"  TP hit - Static: {result.tp_hit_rate_static:.1%}")
        print(f"  TP hit - AEE:    {result.tp_hit_rate_aee:.1%}")
        
        # Special metrics
        print(f"\nAEE Behavior:")
        print(f"  Premature clips: {result.premature_clip_rate:.1%}")
        print(f"  Loss reductions: {result.loss_reduction_rate:.1%}")
        print(f"  MFE capture:     {result.mfe_capture_rate:.1%}")
        print(f"  Avg giveback:    {result.avg_giveback:.3f} R")
        
        # Exit attribution matrix
        print(f"\nExit Attribution Matrix:")
        self._print_exit_matrix(result)
        
        # Class-level breakdown
        print(f"\nClass-Level Performance:")
        self._print_class_breakdown(result)
    
    def _print_exit_matrix(self, result: TestResult) -> None:
        """Print exit attribution matrix."""
        
        # Get all exit reasons
        all_reasons = set()
        for counts in result.class_exit_counts.values():
            all_reasons.update(counts.keys())
        
        # Print header
        header = "Class".ljust(20)
        for reason in sorted(all_reasons):
            header += f" {reason[:8].ljust(8)}"
        print(header)
        
        # Print each class
        for path_class, counts in result.class_exit_counts.items():
            row = path_class.value[:20].ljust(20)
            for reason in sorted(all_reasons):
                count = counts.get(reason, 0)
                pct = count / sum(counts.values()) * 100 if counts else 0
                row += f" {pct:6.1f}%".ljust(9)
            print(row)
    
    def _print_class_breakdown(self, result: TestResult) -> None:
        """Print performance breakdown by path class."""
        
        # Group results by class
        class_results = {}
        for r in result.path_results:
            path_class = PathClass(r["path_class"])
            if path_class not in class_results:
                class_results[path_class] = []
            class_results[path_class].append(r)
        
        # Print each class
        for path_class, results in class_results.items():
            static_r = statistics.mean([r["static"]["actual_r"] for r in results])
            aee_r = statistics.mean([r["aee"]["actual_r"] for r in results])
            delta = aee_r - static_r
            
            mfe = statistics.mean([r["aee"]["mfe_r"] for r in results])
            mae = statistics.mean([r["aee"]["mae_r"] for r in results])
            
            print(f"\n  {path_class.value}:")
            print(f"    Static R: {static_r:+.3f}")
            print(f"    AEE R:    {aee_r:+.3f}")
            print(f"    Delta:    {delta:+.3f}")
            print(f"    MFE:      {mfe:+.3f}")
            print(f"    MAE:      {mae:+.3f}")
    
    def run_parameter_sweep(
        self,
        base_knobs: AEEKnobs,
        sweep_params: Dict[str, List[float]],
        path_count: int = 300,
    ) -> List[TestResult]:
        """Run parameter sweep across given dimensions."""
        
        print(f"\n{'='*60}")
        print(f"RUNNING PARAMETER SWEEP")
        print(f"Base knobs: {base_knobs}")
        print(f"Sweep params: {sweep_params}")
        print(f"{'='*60}")
        
        sweep_results = []
        
        # For now, implement simple 1D sweeps
        for param_name, values in sweep_params.items():
            print(f"\nSweeping {param_name}...")
            
            for value in values:
                # Create new knobs with swept value
                knobs = AEEKnobs(**{**base_knobs.__dict__, param_name: value})
                
                # Run test
                result = self.run_test(knobs, path_count)
                sweep_results.append(result)
                
                # Store parameter info
                result.sweep_param = param_name
                result.sweep_value = value
        
        return sweep_results
    
    def find_best_configuration(self, sweep_results: List[TestResult]) -> Tuple[TestResult, str]:
        """Find best configuration from sweep results."""
        
        best = None
        best_metric = -float('inf')
        best_reason = ""
        
        for result in sweep_results:
            # Simple scoring: prioritize positive delta with low premature clips
            score = result.delta_r - (result.premature_clip_rate * 2)
            
            if score > best_metric:
                best = result
                best_metric = score
                best_reason = f"Delta R: {result.delta_r:+.3f}, Clips: {result.premature_clip_rate:.1%}"
        
        return best, best_reason
    
    def save_results(self, filename: str) -> None:
        """Save all test results to file."""
        
        data = {
            "test_results": [],
        }
        
        for result in self.results:
            result_data = {
                "knobs": result.knobs.__dict__,
                "path_count": result.path_count,
                "avg_r_static": result.avg_r_static,
                "avg_r_aee": result.avg_r_aee,
                "delta_r": result.delta_r,
                "sl_hit_rate_static": result.sl_hit_rate_static,
                "sl_hit_rate_aee": result.sl_hit_rate_aee,
                "tp_hit_rate_static": result.tp_hit_rate_static,
                "tp_hit_rate_aee": result.tp_hit_rate_aee,
                "premature_clip_rate": result.premature_clip_rate,
                "loss_reduction_rate": result.loss_reduction_rate,
                "mfe_capture_rate": result.mfe_capture_rate,
                "avg_giveback": result.avg_giveback,
                "exit_counts": result.exit_counts,
                "class_exit_counts": {
                    cls.value: counts for cls, counts in result.class_exit_counts.items()
                },
            }
            
            # Add sweep info if present
            if hasattr(result, 'sweep_param'):
                result_data["sweep_param"] = result.sweep_param
                result_data["sweep_value"] = result.sweep_value
            
            data["test_results"].append(result_data)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nResults saved to {filename}")


def main():
    """Main entry point for Phase 1 testing."""
    
    tester = Phase1Tester()
    
    # Run 0 - Kill test with 40 paths
    print("\n" + "="*60)
    print("RUN 0 - KILL TEST (40 paths)")
    print("="*60)
    
    base_knobs = AEEKnobs(profit_capture_min_atr=0.45)
    kill_result = tester.run_test(base_knobs, path_count=40, random_seed=42)
    
    # Check for obvious failure
    if kill_result.delta_r < -0.1:
        print("\n❌ KILL TEST FAILED - AEE is clearly destructive")
        print("Stopping further testing...")
        return
    
    print("\n✅ KILL TEST PASSED - Proceeding with full testing")
    
    # Run 1 - Baseline with 300 paths
    print("\n" + "="*60)
    print("RUN 1 - BASELINE TEST (300 paths)")
    print("="*60)
    
    baseline_result = tester.run_test(base_knobs, path_count=300, random_seed=123)
    
    # Run 2 - Profit capture sweep
    print("\n" + "="*60)
    print("RUN 2 - PROFIT CAPTURE SWEEP")
    print("="*60)
    
    profit_sweep = tester.run_parameter_sweep(
        base_knobs,
        {"profit_capture_min_atr": [0.25, 0.35, 0.45, 0.55]},
        path_count=300,
    )
    
    # Find best profit capture
    best_profit, reason = tester.find_best_configuration(profit_sweep)
    print(f"\nBest profit capture: {best_profit.knobs.profit_capture_min_atr:.2f}")
    print(f"Reason: {reason}")
    
    # Update base knobs with best value
    base_knobs.profit_capture_min_atr = best_profit.knobs.profit_capture_min_atr
    
    # Run 3 - Giveback sweep
    print("\n" + "="*60)
    print("RUN 3 - GIVEBACK SWEEP")
    print("="*60)
    
    giveback_sweep = tester.run_parameter_sweep(
        base_knobs,
        {"allowed_giveback_atr_mult": [0.25, 0.35, 0.45]},
        path_count=300,
    )
    
    # Find best giveback
    best_giveback, reason = tester.find_best_configuration(giveback_sweep)
    print(f"\nBest giveback: {best_giveback.knobs.allowed_giveback_atr_mult:.2f}")
    print(f"Reason: {reason}")
    
    # Final validation
    print("\n" + "="*60)
    print("RUN 9 - FINAL VALIDATION (2000 paths)")
    print("="*60)
    
    final_knobs = AEEKnobs(
        profit_capture_min_atr=best_profit.knobs.profit_capture_min_atr,
        allowed_giveback_atr_mult=best_giveback.knobs.allowed_giveback_atr_mult,
    )
    
    final_result = tester.run_test(final_knobs, path_count=2000, random_seed=999)
    
    # Save all results
    tester.save_results("phase1_aee_test_results.json")
    
    # Final decision
    print("\n" + "="*60)
    print("PHASE 1 FINAL DECISION")
    print("="*60)
    
    if final_result.delta_r > 0.05 and final_result.premature_clip_rate < 0.15:
        print("✅ GO - AEE adds value and meets criteria")
        print(f"   Delta R: {final_result.delta_r:+.3f}")
        print(f"   Clip rate: {final_result.premature_clip_rate:.1%}")
    else:
        print("❌ NO-GO - AEE does not meet criteria")
        print(f"   Delta R: {final_result.delta_r:+.3f}")
        print(f"   Clip rate: {final_result.premature_clip_rate:.1%}")
    
    print("\nBest configuration:")
    print(f"  profit_capture_min_atr: {final_knobs.profit_capture_min_atr:.2f}")
    print(f"  allowed_giveback_atr_mult: {final_knobs.allowed_giveback_atr_mult:.2f}")


if __name__ == "__main__":
    main()
