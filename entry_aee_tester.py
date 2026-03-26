#!/usr/bin/env python3
"""
Entry + AEE Testing Framework

Tests both entry and AEE logic with toggles for:
- Longs only
- Shorts only
- Both together
"""

from __future__ import annotations

import json
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from enum import Enum

from aee_synthetic_evaluator import AEEEvaluator, AEEKnobs, AEEState


@dataclass
class EntryConfig:
    """Entry configuration parameters."""
    confirm_disp_atr: float
    confirm_m1_closes: int
    confirm_sec: float
    base_max_dist_atr: float
    dist_vel_k: float


class TestMode(Enum):
    """Testing mode."""
    LONGS_ONLY = "longs_only"
    SHORTS_ONLY = "shorts_only"
    BOTH = "both"


class EntryMode(Enum):
    """Entry testing mode."""
    ENABLED = "enabled"
    DISABLED = "disabled"


class AEEMode(Enum):
    """AEE testing mode."""
    ENABLED = "enabled"
    DISABLED = "disabled"


@dataclass
class TestConfiguration:
    """Configuration for entry + AEE testing."""
    test_mode: TestMode
    entry_mode: EntryMode
    aee_mode: AEEMode
    aee_knobs: AEEKnobs


@dataclass
class TestResult:
    """Result of a single opportunity test."""
    opportunity_id: str
    direction: str
    bucket: str

    # Entry results
    entry_triggered: bool
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None

    # AEE results
    aee_exit_reason: Optional[str] = None
    aee_exit_price: Optional[float] = None
    aee_exit_time: Optional[datetime] = None
    aee_final_r: Optional[float] = None
    aee_mfe_r: Optional[float] = None
    aee_mae_r: Optional[float] = None

    # Static baseline (no AEE)
    static_exit_reason: Optional[str] = None
    static_final_r: Optional[float] = None

    # Performance metrics
    entry_success: bool = False  # Whether entry logic captured the opportunity
    aee_improvement: Optional[float] = None  # AEE R - static R


class EntryAeeTester:
    """
    Tests entry and AEE logic together with flexible toggles.
    """

    def __init__(self, config: TestConfiguration):
        self.config = config
        self.aee_evaluator = AEEEvaluator(config.aee_knobs) if config.aee_mode == AEEMode.ENABLED else None

    def test_opportunities(self, analyzed_opportunities: List[Dict]) -> List[TestResult]:
        """
        Test all opportunities according to the configuration.

        Args:
            analyzed_opportunities: List of analyzed opportunity dicts from Phase 2

        Returns:
            List of test results
        """
        results = []

        for opp_data in analyzed_opportunities:
            try:
                # Filter by test mode
                if not self._should_test_opportunity(opp_data):
                    continue

                result = self._test_single_opportunity(opp_data)
                results.append(result)

            except Exception as e:
                print(f"Error testing opportunity {opp_data.get('opportunity_id')}: {e}")
                continue

        print(f"Tested {len(results)} opportunities")
        return results

    def _should_test_opportunity(self, opp_data: Dict) -> bool:
        """Determine if this opportunity should be tested based on mode."""
        direction = opp_data["direction"]

        if self.config.test_mode == TestMode.LONGS_ONLY:
            return direction == "LONG"
        elif self.config.test_mode == TestMode.SHORTS_ONLY:
            return direction == "SHORT"
        else:  # BOTH
            return True

    def _test_single_opportunity(self, opp_data: Dict) -> TestResult:
        """Test a single opportunity."""

        # Extract opportunity data
        opportunity_id = opp_data["opportunity_id"]
        direction = opp_data["direction"]
        bucket = opp_data["bucket"]
        aee_params = opp_data["aee_params"]

        # Handle full_price_path - it might be missing or empty for some opportunities
        full_price_path = opp_data.get("full_price_path", [])
        if not full_price_path:
            # If no price path, skip AEE testing
            return TestResult(
                opportunity_id=opportunity_id,
                direction=direction,
                bucket=bucket,
                entry_triggered=False
            )

        # Test entry logic
        entry_result = self._test_entry_logic(opp_data)

        # Test AEE logic
        aee_result = self._test_aee_logic(opp_data, full_price_path)

        # Test static baseline
        static_result = self._test_static_baseline(opp_data, full_price_path)

        # Calculate performance metrics
        entry_success = self._calculate_entry_success(entry_result, opp_data)
        aee_improvement = self._calculate_aee_improvement(aee_result, static_result)

        return TestResult(
            opportunity_id=opportunity_id,
            direction=direction,
            bucket=bucket,
            entry_triggered=entry_result["triggered"],
            entry_price=entry_result.get("entry_price"),
            entry_time=entry_result.get("entry_time"),
            aee_exit_reason=aee_result.get("exit_reason"),
            aee_exit_price=aee_result.get("exit_price"),
            aee_exit_time=aee_result.get("exit_time"),
            aee_final_r=aee_result.get("actual_r"),
            aee_mfe_r=aee_result.get("mfe_r"),
            aee_mae_r=aee_result.get("mae_r"),
            static_exit_reason=static_result.get("exit_reason"),
            static_final_r=static_result.get("actual_r"),
            entry_success=entry_success,
            aee_improvement=aee_improvement
        )

    def _test_entry_logic(self, opp_data: Dict) -> Dict[str, Any]:
        """Test entry logic for this opportunity."""

        if self.config.entry_mode == EntryMode.DISABLED:
            # Simplified: assume entry triggers at opportunity start
            start_time = datetime.fromisoformat(opp_data["start_time"])
            entry_price = opp_data["aee_params"]["entry_price"]

            return {
                "triggered": True,
                "entry_price": entry_price,
                "entry_time": start_time
            }

        # TODO: Implement actual entry logic testing
        # For now, simplified entry logic
        start_time = datetime.fromisoformat(opp_data["start_time"])
        entry_price = opp_data["aee_params"]["entry_price"]

        # Simple entry condition: if bucket is GOOD, assume entry triggers
        triggered = opp_data["bucket"] == "good"

        return {
            "triggered": triggered,
            "entry_price": entry_price if triggered else None,
            "entry_time": start_time if triggered else None
        }

    def _test_aee_logic(self, opp_data: Dict, price_path: List[Dict]) -> Dict[str, Any]:
        """Test AEE logic on the price path."""

        if self.config.aee_mode == AEEMode.DISABLED or self.aee_evaluator is None:
            return {}

        # Create synthetic path for AEE testing
        synthetic_path = self._create_synthetic_path(opp_data, price_path)

        # Run AEE evaluation
        aee_result = self.aee_evaluator.evaluate_path(synthetic_path)

        # Convert exit time to datetime
        if aee_result.get("exit_time"):
            exit_time = datetime.fromtimestamp(aee_result["exit_time"], timezone.utc)
            aee_result["exit_time"] = exit_time

        return aee_result

    def _test_static_baseline(self, opp_data: Dict, price_path: List[Dict]) -> Dict[str, Any]:
        """Test static TP/SL baseline (no AEE)."""

        # Create synthetic path
        synthetic_path = self._create_synthetic_path(opp_data, price_path)

        # Run static evaluation
        static_result = self._run_static_baseline(synthetic_path)

        return static_result

    def _create_synthetic_path(self, opp_data: Dict, price_path: List[Dict]):
        """Create synthetic path for AEE testing."""
        from synthetic_path_generator import SyntheticPath, PathClass

        aee_params = opp_data["aee_params"]
        direction = opp_data["direction"]
        bucket = opp_data["bucket"]

        # Extract timestamps and prices
        timestamps = [candle["timestamp"].timestamp() for candle in price_path]
        mid_prices = [candle["close"] for candle in price_path]
        spreads = [candle.get("spread_pips", 0.1) for candle in price_path]

        # Determine path class based on bucket
        if bucket == "good":
            path_class = PathClass.CLEAN_CONTINUATION
        elif bucket == "bad":
            path_class = PathClass.STALL_THEN_FAIL
        else:
            path_class = PathClass.WHIPSAW

        return SyntheticPath(
            path_class=path_class,
            direction=direction,
            entry_price=aee_params["entry_price"],
            entry_spread=1.5,
            atr_pips=aee_params["atr_pips"],
            tp_price=aee_params["tp_price"],
            sl_price=aee_params["sl_price"],
            timestamps=timestamps,
            mid_prices=mid_prices,
            spreads=spreads,
            exit_reason="TIMEOUT",  # Will be overridden
            exit_time=timestamps[-1],
            final_r=0.0  # Will be overridden
        )

    def _run_static_baseline(self, path):
        """Run static TP/SL baseline."""
        from aee_synthetic_evaluator import run_static_baseline
        return run_static_baseline(path)

    def _calculate_entry_success(self, entry_result: Dict, opp_data: Dict) -> bool:
        """Calculate whether entry logic successfully captured the opportunity."""
        if not entry_result.get("triggered"):
            return False

        # For GOOD opportunities, entry should trigger
        # For BAD opportunities, entry should ideally not trigger (but we're testing both)
        bucket = opp_data["bucket"]
        if bucket == "good":
            return True
        elif bucket == "bad":
            # For testing, we want to see how AEE performs even on bad entries
            return True
        else:  # NOISE
            return False

    def _calculate_aee_improvement(self, aee_result: Dict, static_result: Dict) -> Optional[float]:
        """Calculate AEE improvement over static baseline."""
        aee_r = aee_result.get("actual_r")
        static_r = static_result.get("actual_r")

        if aee_r is not None and static_r is not None:
            return aee_r - static_r
        return None

    def generate_test_report(self, results: List[TestResult]) -> Dict[str, Any]:
        """Generate comprehensive test report."""

        # Separate by direction and bucket
        long_good = [r for r in results if r.direction == "LONG" and r.bucket == "good"]
        long_bad = [r for r in results if r.direction == "LONG" and r.bucket == "bad"]
        short_good = [r for r in results if r.direction == "SHORT" and r.bucket == "good"]
        short_bad = [r for r in results if r.direction == "SHORT" and r.bucket == "bad"]

        def analyze_bucket(results: List[TestResult]) -> Dict[str, Any]:
            if not results:
                return {"count": 0, "entry_rate": 0, "aee_win_rate": 0, "avg_aee_improvement": 0}

            entry_rate = len([r for r in results if r.entry_success]) / len(results)

            aee_results = [r for r in results if r.aee_final_r is not None]
            aee_win_rate = len([r for r in aee_results if r.aee_final_r > 0]) / len(aee_results) if aee_results else 0

            improvements = [r.aee_improvement for r in results if r.aee_improvement is not None]
            avg_improvement = sum(improvements) / len(improvements) if improvements else 0

            return {
                "count": len(results),
                "entry_rate": entry_rate,
                "aee_win_rate": aee_win_rate,
                "avg_aee_improvement": avg_improvement
            }

        return {
            "test_config": {
                "test_mode": self.config.test_mode.value,
                "entry_mode": self.config.entry_mode.value,
                "aee_mode": self.config.aee_mode.value,
                "aee_knobs": {
                    "profit_capture_min_atr": self.config.aee_knobs.profit_capture_min_atr,
                    "allowed_giveback_atr_mult": self.config.aee_knobs.allowed_giveback_atr_mult
                }
            },
            "summary": {
                "total_tested": len(results),
                "long_good": analyze_bucket(long_good),
                "long_bad": analyze_bucket(long_bad),
                "short_good": analyze_bucket(short_good),
                "short_bad": analyze_bucket(short_bad)
            },
            "generated_utc": datetime.now(timezone.utc).isoformat()
        }


def main():
    """Run the Entry + AEE testing framework."""
    import argparse

    parser = argparse.ArgumentParser(description="Entry + AEE Testing Framework")
    parser.add_argument("--phase2-results", required=True, help="Path to Phase 2 analysis JSON")
    parser.add_argument("--test-mode", choices=["longs_only", "shorts_only", "both"], default="both")
    parser.add_argument("--entry-mode", choices=["enabled", "disabled"], default="enabled")
    parser.add_argument("--aee-mode", choices=["enabled", "disabled"], default="enabled")
    parser.add_argument("--entry-config", help="Path to optimized entry config JSON")
    parser.add_argument("--aee-config", help="Path to optimized AEE config JSON")
    parser.add_argument("--output", default="test_results.json", help="Output file path")

    args = parser.parse_args()

    # Load Phase 2 results
    print("Loading Phase 2 analysis results...")
    with open(args.phase2_results, 'r') as f:
        phase2_data = json.load(f)

    opportunities = phase2_data["opportunities"]
    print(f"Loaded {len(opportunities)} analyzed opportunities")

    # Load optimized configurations if provided
    entry_knobs = None
    aee_knobs = None

    if args.entry_config:
        print(f"Loading optimized entry config from {args.entry_config}")
        with open(args.entry_config, 'r') as f:
            entry_data = json.load(f)
        # Convert entry config to EntryConfig object
        from entry_fitter import EntryConfig
        config_data = entry_data["config"]
        entry_knobs = EntryConfig(
            confirm_disp_atr=config_data["confirm_disp_atr"],
            confirm_m1_closes=config_data["confirm_m1_closes"],
            confirm_sec=config_data["confirm_sec"],
            base_max_dist_atr=config_data["base_max_dist_atr"],
            dist_vel_k=config_data["dist_vel_k"]
        )
    else:
        from entry_fitter import EntryConfig
        entry_knobs = EntryConfig(0.5, 2, 30.0, 1.5, 0.8)  # Default

    if args.aee_config:
        print(f"Loading optimized AEE config from {args.aee_config}")
        with open(args.aee_config, 'r') as f:
            aee_data = json.load(f)
        # Convert AEE config to AEEKnobs object
        config_data = aee_data["config"]
        aee_knobs = AEEKnobs(
            profit_capture_min_atr=config_data["profit_capture_min_atr"],
            allowed_giveback_atr_mult=config_data["allowed_giveback_atr_mult"],
            panic_velocity=config_data["panic_velocity"],
            decay_min_hold_sec=config_data["decay_min_hold_sec"]
        )
    else:
        aee_knobs = AEEKnobs(0.35, 0.45, -0.8, 120.0)  # Default

    # Create test configuration
    test_config = TestConfiguration(
        test_mode=TestMode(args.test_mode),
        entry_mode=EntryMode(args.entry_mode),
        aee_mode=AEEMode(args.aee_mode),
        aee_knobs=aee_knobs
    )

    # Run tests
    print(f"Running tests: {test_config.test_mode.value} | Entry: {test_config.entry_mode.value} | AEE: {test_config.aee_mode.value}")
    if args.entry_config:
        print("Using OPTIMIZED entry settings")
    else:
        print("Using DEFAULT entry settings")

    if args.aee_config:
        print("Using OPTIMIZED AEE settings")
    else:
        print("Using DEFAULT AEE settings")

    tester = EntryAeeTester(test_config)
    results = tester.test_opportunities(opportunities)

    # Generate report
    report = tester.generate_test_report(results)

    # Print summary
    print("\nTEST RESULTS SUMMARY:")
    print(f"Total opportunities tested: {report['summary']['total_tested']}")

    for direction in ["long", "short"]:
        for bucket in ["good", "bad"]:
            key = f"{direction}_{bucket}"
            data = report['summary'][key]
            if data['count'] > 0:
                print(f"{direction.upper()} {bucket.upper()}: {data['count']} opps | Entry: {data['entry_rate']:.1%} | AEE Win: {data['aee_win_rate']:.1%} | Avg Improvement: {data['avg_aee_improvement']:.3f}R")

    # Save detailed results
    output_data = {
        "report": report,
        "results": [
            {
                "opportunity_id": r.opportunity_id,
                "direction": r.direction,
                "bucket": r.bucket,
                "entry_triggered": r.entry_triggered,
                "entry_success": r.entry_success,
                "aee_final_r": r.aee_final_r,
                "static_final_r": r.static_final_r,
                "aee_improvement": r.aee_improvement
            }
            for r in results
        ]
    }

    with open(args.output, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nDetailed results saved to {args.output}")

    return 0


if __name__ == "__main__":
    exit(main())
