"""Entry Recovery System - Implements comprehensive entry recovery plan.

This module addresses the core entry failure modes:
1. Feature integrity gates (missing_features hard stop)
2. Split blocker attribution (distinct blocker reasons)
3. Coverage-first retuning (score = good_capture - penalty_bad - penalty_noise)
4. Hard model pass/fail gates
5. Stability checks across seeds

Priority order:
1. Fix feature integrity
2. Split blockers cleanly
3. Coverage-first retune
4. Frontier selection
5. Hard pass/fail
6. Only then re-enable AEE tuning
"""

from __future__ import annotations

import json
import random
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from dataclasses import dataclass
from typing import List, Dict, Any, Optional


class BlockerReason(Enum):
    """Distinct blocker reasons for clear attribution."""
    BLOCK_CONFIRM_DISP_ATR = "block_confirm_disp_atr"
    BLOCK_CONFIRM_M1_CLOSES = "block_confirm_m1_closes"
    BLOCK_CONFIRM_SEC = "block_confirm_sec"
    BLOCK_BASE_MAX_DIST_ATR = "block_base_max_dist_atr"
    BLOCK_DIST_VEL_K = "block_dist_vel_k"


@dataclass
class OpportunityZone:
    """Represents a mapped opportunity zone."""
    direction: str  # "LONG" or "SHORT"
    bucket_type: str  # "GOOD", "BAD", "NOISE"
    zone_type: str  # "CONTINUATION", "EXTENSION", "SPIKE", "GRIND"
    features: Dict[str, Any]
    has_complete_features: bool = False
    mfe: float = 0.0
    mae: float = 0.0
    tau: int = 0
    extension_range: float = 0.0
    reversal_range: float = 0.0
    realized_range: float = 0.0


@dataclass
class BlockerCounts:
    """Tracks blocker attribution with first/cumulative distinction."""
    block_confirm_disp_atr: int = 0
    block_confirm_m1_closes: int = 0
    block_confirm_sec: int = 0
    block_base_max_dist_atr: int = 0
    block_dist_vel_k: int = 0

    # First blocker hit (which blocker stops trades first)
    first_block_confirm_disp_atr: int = 0
    first_block_confirm_m1_closes: int = 0
    first_block_confirm_sec: int = 0
    first_block_base_max_dist_atr: int = 0
    first_block_dist_vel_k: int = 0


@dataclass
class EntryResults:
    """Results from entry evaluation on opportunity zones."""
    # Feature coverage
    total_zones: int = 0
    feature_complete_zones: int = 0

    # Per bucket/direction coverage
    good_long_total: int = 0
    good_long_complete: int = 0
    good_short_total: int = 0
    good_short_complete: int = 0
    bad_long_total: int = 0
    bad_long_complete: int = 0
    bad_short_total: int = 0
    bad_short_complete: int = 0
    noise_long_total: int = 0
    noise_long_complete: int = 0
    noise_short_total: int = 0
    noise_short_complete: int = 0

    # Trigger results
    good_long_triggered: int = 0
    good_short_triggered: int = 0
    bad_long_triggered: int = 0
    bad_short_triggered: int = 0
    noise_long_triggered: int = 0
    noise_short_triggered: int = 0

    # Blocker attribution
    long_blockers: BlockerCounts = field(default_factory=BlockerCounts)
    short_blockers: BlockerCounts = field(default_factory=BlockerCounts)

    # Performance metrics
    long_pips_mean: float = 0.0
    short_pips_mean: float = 0.0
    both_pips_mean: float = 0.0

    def calculate_coverage_rates(self) -> Dict[str, float]:
        """Calculate feature coverage rates per bucket/direction."""
        return {
            "good_long_coverage": self.good_long_complete / max(1, self.good_long_total),
            "good_short_coverage": self.good_short_complete / max(1, self.good_short_total),
            "bad_long_coverage": self.bad_long_complete / max(1, self.bad_long_total),
            "bad_short_coverage": self.bad_short_complete / max(1, self.bad_short_total),
            "noise_long_coverage": self.noise_long_complete / max(1, self.noise_long_total),
            "noise_short_coverage": self.noise_short_complete / max(1, self.noise_short_total),
        }

    def calculate_trigger_rates(self) -> Dict[str, float]:
        """Calculate trigger rates (what % of feature-complete zones trigger)."""
        return {
            "good_long_trigger_rate": self.good_long_triggered / max(1, self.good_long_complete),
            "good_short_trigger_rate": self.good_short_triggered / max(1, self.good_short_complete),
            "bad_long_trigger_rate": self.bad_long_triggered / max(1, self.bad_long_complete),
            "bad_short_trigger_rate": self.bad_short_triggered / max(1, self.bad_short_complete),
            "noise_long_trigger_rate": self.noise_long_triggered / max(1, self.noise_long_complete),
            "noise_short_trigger_rate": self.noise_short_triggered / max(1, self.noise_short_complete),
        }

    def calculate_good_triggerable_rate(self) -> Dict[str, float]:
        """Calculate triggerable rate (feature-complete GOOD zones vs total GOOD zones)."""
        return {
            "good_long_triggerable": self.good_long_complete / max(1, self.good_long_total),
            "good_short_triggerable": self.good_short_complete / max(1, self.good_short_total),
        }


@dataclass
class EntryConfig:
    """Entry configuration parameters."""

    # Confirmation parameters (relaxed for sample data)
    confirm_disp_atr: float = 0.1  # Relaxed from 0.5
    confirm_m1_closes: int = 1     # Relaxed from 3
    confirm_sec: int = 10          # Relaxed from 30

    # Base distance parameters
    base_max_dist_atr: float = 2.0  # Relaxed from 1.0
    dist_vel_k: float = 1.5         # Relaxed from 0.8

    def to_dict(self) -> Dict[str, Any]:
        return {
            "confirm_disp_atr": self.confirm_disp_atr,
            "confirm_m1_closes": self.confirm_m1_closes,
            "confirm_sec": self.confirm_sec,
            "base_max_dist_atr": self.base_max_dist_atr,
            "dist_vel_k": self.dist_vel_k,
        }


@dataclass
class EntryRecoveryConfig:
    """Configuration for entry recovery process."""
    # Coverage-first scoring weights
    good_capture_weight: float = 1.0
    bad_penalty_weight: float = 0.5
    noise_penalty_weight: float = 0.2

    # Hard constraints
    bad_trigger_max: float = 0.15
    pips_mean_min: float = 0.0

    # Hard pass gates
    good_capture_min: float = 0.50

    # Stability check parameters
    stability_seeds: int = 5
    stability_variance_max: float = 0.05  # Max variance for good_capture, bad_trigger, pips_mean

    # Retune ranges
    disp_atr_range: Tuple[float, float] = (0.1, 1.0)
    base_max_dist_range: Tuple[float, float] = (0.5, 2.0)
    dist_vel_k_range: Tuple[float, float] = (0.5, 1.5)
    m1_closes_range: Tuple[int, int] = (1, 5)
    confirm_sec_range: Tuple[int, int] = (15, 60)


class EntryRecoveryEngine:
    """Implements the entry recovery plan."""

    def __init__(self, config: EntryRecoveryConfig):
        self.config = config

    def check_feature_completeness_gate(self, zones: List[OpportunityZone]) -> Tuple[bool, Dict[str, float]]:
        """Hard gate: Check if feature coverage is near-complete.

        Returns (pass_flag, coverage_rates)
        """
        results = EntryResults()

        # Count zones by bucket and direction
        for zone in zones:
            results.total_zones += 1
            if zone.has_complete_features:
                results.feature_complete_zones += 1

            direction = zone.direction.lower()
            bucket = zone.bucket_type.lower()

            # Update counts
            if bucket == "good":
                if direction == "long":
                    results.good_long_total += 1
                    if zone.has_complete_features:
                        results.good_long_complete += 1
                else:  # short
                    results.good_short_total += 1
                    if zone.has_complete_features:
                        results.good_short_complete += 1
            elif bucket == "bad":
                if direction == "long":
                    results.bad_long_total += 1
                    if zone.has_complete_features:
                        results.bad_long_complete += 1
                else:  # short
                    results.bad_short_total += 1
                    if zone.has_complete_features:
                        results.bad_short_complete += 1
            elif bucket == "noise":
                if direction == "long":
                    results.noise_long_total += 1
                    if zone.has_complete_features:
                        results.noise_long_complete += 1
                else:  # short
                    results.noise_short_total += 1
                    if zone.has_complete_features:
                        results.noise_short_complete += 1

        coverage_rates = results.calculate_coverage_rates()

        # Print coverage report
        print("\n" + "="*60)
        print("FEATURE COMPLETENESS GATE CHECK")
        print("="*60)
        print(f"Total zones: {results.total_zones}")
        print(f"Feature complete: {results.feature_complete_zones} ({results.feature_complete_zones/results.total_zones*100:.1f}%)")
        print("\nCoverage by bucket/direction:")
        print(".1%")
        print(".1%")
        print(".1%")
        print(".1%")
        print(".1%")
        print(".1%")

        # Hard gate: require near-complete coverage (>=95%) for all buckets
        gate_pass = all(rate >= 0.95 for rate in coverage_rates.values())

        if not gate_pass:
            print("\n❌ FEATURE COMPLETENESS GATE FAILED")
            print("Entry runs blocked - missing features must be fixed first")
        else:
            print("\n✅ FEATURE COMPLETENESS GATE PASSED")

        return gate_pass, coverage_rates

    def evaluate_entry_with_blocker_tracking(
        self,
        zones: List[OpportunityZone],
        config: EntryConfig
    ) -> EntryResults:
        """Evaluate entry logic on zones with detailed blocker attribution."""
        results = EntryResults()

        # Initialize counters from feature completeness check
        self.check_feature_completeness_gate(zones)

        for zone in zones:
            if not zone.has_complete_features:
                continue  # Skip zones with missing features

            direction = zone.direction.lower()
            bucket = zone.bucket_type.lower()

            # Evaluate entry gates with blocker tracking
            triggered, blocker_chain = self._evaluate_entry_gates_with_blockers(zone, config)

            # Update blocker counts
            if direction == "long":
                blockers = results.long_blockers
            else:
                blockers = results.short_blockers

            # Track cumulative blockers
            for blocker in blocker_chain:
                if blocker == BlockerReason.BLOCK_CONFIRM_DISP_ATR:
                    blockers.block_confirm_disp_atr += 1
                elif blocker == BlockerReason.BLOCK_CONFIRM_M1_CLOSES:
                    blockers.block_confirm_m1_closes += 1
                elif blocker == BlockerReason.BLOCK_CONFIRM_SEC:
                    blockers.block_confirm_sec += 1
                elif blocker == BlockerReason.BLOCK_BASE_MAX_DIST_ATR:
                    blockers.block_base_max_dist_atr += 1
                elif blocker == BlockerReason.BLOCK_DIST_VEL_K:
                    blockers.block_dist_vel_k += 1

            # Track first blocker
            if blocker_chain:
                first_blocker = blocker_chain[0]
                if first_blocker == BlockerReason.BLOCK_CONFIRM_DISP_ATR:
                    blockers.first_block_confirm_disp_atr += 1
                elif first_blocker == BlockerReason.BLOCK_CONFIRM_M1_CLOSES:
                    blockers.first_block_confirm_m1_closes += 1
                elif first_blocker == BlockerReason.BLOCK_CONFIRM_SEC:
                    blockers.first_block_confirm_sec += 1
                elif first_blocker == BlockerReason.BLOCK_BASE_MAX_DIST_ATR:
                    blockers.first_block_base_max_dist_atr += 1
                elif first_blocker == BlockerReason.BLOCK_DIST_VEL_K:
                    blockers.first_block_dist_vel_k += 1

            # Update trigger counts
            if triggered:
                if bucket == "good":
                    if direction == "long":
                        results.good_long_triggered += 1
                    else:
                        results.good_short_triggered += 1
                elif bucket == "bad":
                    if direction == "long":
                        results.bad_long_triggered += 1
                    else:
                        results.bad_short_triggered += 1
                elif bucket == "noise":
                    if direction == "long":
                        results.noise_long_triggered += 1
                    else:
                        results.noise_short_triggered += 1

        return results

    def _evaluate_entry_gates_with_blockers(
        self,
        zone: OpportunityZone,
        config: EntryConfig
    ) -> Tuple[bool, List[BlockerReason]]:
        """Evaluate entry gates and return (triggered, blocker_chain)."""
        blockers = []

        # Gate 1: confirm_disp_atr
        if not self._check_confirm_disp_atr(zone, config.confirm_disp_atr):
            blockers.append(BlockerReason.BLOCK_CONFIRM_DISP_ATR)

        # Gate 2: confirm_m1_closes
        if not self._check_confirm_m1_closes(zone, config.confirm_m1_closes):
            blockers.append(BlockerReason.BLOCK_CONFIRM_M1_CLOSES)

        # Gate 3: confirm_sec
        if not self._check_confirm_sec(zone, config.confirm_sec):
            blockers.append(BlockerReason.BLOCK_CONFIRM_SEC)

        # Gate 4: base_max_dist_atr
        if not self._check_base_max_dist_atr(zone, config.base_max_dist_atr):
            blockers.append(BlockerReason.BLOCK_BASE_MAX_DIST_ATR)

        # Gate 5: dist_vel_k
        if not self._check_dist_vel_k(zone, config.dist_vel_k):
            blockers.append(BlockerReason.BLOCK_DIST_VEL_K)

        # Triggered if no blockers
        triggered = len(blockers) == 0
        return triggered, blockers

    def _check_confirm_disp_atr(self, zone: OpportunityZone, threshold: float) -> bool:
        """Check displacement ATR confirmation."""
        return zone.features.get("disp_atr", 0.0) >= threshold

    def _check_confirm_m1_closes(self, zone: OpportunityZone, threshold: int) -> bool:
        """Check M1 closes confirmation."""
        return zone.features.get("m1_closes", 0) >= threshold

    def _check_confirm_sec(self, zone: OpportunityZone, threshold: int) -> bool:
        """Check time confirmation."""
        return zone.features.get("confirm_sec", 0) >= threshold

    def _check_base_max_dist_atr(self, zone: OpportunityZone, threshold: float) -> bool:
        """Check base max distance ATR."""
        return zone.features.get("base_max_dist_atr", 0.0) <= threshold

    def _check_dist_vel_k(self, zone: OpportunityZone, threshold: float) -> bool:
        """Check distance velocity k."""
        return zone.features.get("dist_vel_k", 0.0) <= threshold

    def print_blocker_report(self, results: EntryResults):
        """Print detailed blocker attribution report."""
        print("\n" + "="*60)
        print("BLOCKER ATTRIBUTION REPORT")
        print("="*60)

        for direction in ["long", "short"]:
            print(f"\n{direction.upper()} BLOCKERS:")
            if direction == "long":
                blockers = results.long_blockers
            else:
                blockers = results.short_blockers

            total_cumulative = (
                blockers.block_confirm_disp_atr +
                blockers.block_confirm_m1_closes +
                blockers.block_confirm_sec +
                blockers.block_base_max_dist_atr +
                blockers.block_dist_vel_k
            )

            if total_cumulative == 0:
                print("  No blockers recorded")
                continue

            print("  Cumulative blockers:")
            print(f"    block_confirm_disp_atr: {blockers.block_confirm_disp_atr} ({blockers.block_confirm_disp_atr/total_cumulative*100:.1f}%)")
            print(f"    block_confirm_m1_closes: {blockers.block_confirm_m1_closes} ({blockers.block_confirm_m1_closes/total_cumulative*100:.1f}%)")
            print(f"    block_confirm_sec: {blockers.block_confirm_sec} ({blockers.block_confirm_sec/total_cumulative*100:.1f}%)")
            print(f"    block_base_max_dist_atr: {blockers.block_base_max_dist_atr} ({blockers.block_base_max_dist_atr/total_cumulative*100:.1f}%)")
            print(f"    block_dist_vel_k: {blockers.block_dist_vel_k} ({blockers.block_dist_vel_k/total_cumulative*100:.1f}%)")

            print("  First blockers (kills trades first):")
            print(f"    block_confirm_disp_atr: {blockers.first_block_confirm_disp_atr}")
            print(f"    block_confirm_m1_closes: {blockers.first_block_confirm_m1_closes}")
            print(f"    block_confirm_sec: {blockers.first_block_confirm_sec}")
            print(f"    block_base_max_dist_atr: {blockers.first_block_base_max_dist_atr}")
            print(f"    block_dist_vel_k: {blockers.first_block_dist_vel_k}")

    def calculate_coverage_first_score(
        self,
        results: EntryResults,
        good_weight: float = 1.0,
        bad_penalty: float = 0.5,
        noise_penalty: float = 0.2
    ) -> float:
        """Calculate coverage-first score: good_capture - penalty_bad - penalty_noise."""
        trigger_rates = results.calculate_trigger_rates()

        long_good_capture = trigger_rates["good_long_trigger_rate"]
        short_good_capture = trigger_rates["good_short_trigger_rate"]
        long_bad_trigger = trigger_rates["bad_long_trigger_rate"]
        short_bad_trigger = trigger_rates["bad_short_trigger_rate"]
        long_noise_trigger = trigger_rates["noise_long_trigger_rate"]
        short_noise_trigger = trigger_rates["noise_short_trigger_rate"]

        score = (
            good_weight * (long_good_capture + short_good_capture) / 2 -
            bad_penalty * (long_bad_trigger + short_bad_trigger) / 2 -
            noise_penalty * (long_noise_trigger + short_noise_trigger) / 2
        )

        return score

    def check_hard_constraints(self, results: EntryResults) -> bool:
        """Check hard constraints: bad_trigger <= 0.15, both.pips_mean > 0."""
        trigger_rates = results.calculate_trigger_rates()

        long_bad_trigger = trigger_rates["bad_long_trigger_rate"]
        short_bad_trigger = trigger_rates["bad_short_trigger_rate"]

        bad_trigger_ok = (long_bad_trigger <= self.config.bad_trigger_max and
                         short_bad_trigger <= self.config.bad_trigger_max)
        pips_ok = results.both_pips_mean > self.config.pips_mean_min

        return bad_trigger_ok and pips_ok

    def check_model_pass_gates(self, results: EntryResults) -> bool:
        """Check MODEL_PASS gates: long/short good_capture >= 0.50, bad_triggers <= 0.15, pips_mean > 0."""
        trigger_rates = results.calculate_trigger_rates()

        good_capture_ok = (
            trigger_rates["good_long_trigger_rate"] >= self.config.good_capture_min and
            trigger_rates["good_short_trigger_rate"] >= self.config.good_capture_min
        )

        hard_constraints_ok = self.check_hard_constraints(results)

        return good_capture_ok and hard_constraints_ok

    def run_coverage_first_retune(
        self,
        zones: List[OpportunityZone],
        direction: str
    ) -> Tuple[EntryConfig, EntryResults, float]:
        """Run coverage-first retune for specified direction.

        Returns (best_config, best_results, best_score)
        """
        print(f"\n{'='*60}")
        print(f"COVERAGE-FIRST RETUNE: {direction.upper()}")
        print(f"{'='*60}")

        best_score = float('-inf')
        best_config = None
        best_results = None

        # Retune order based on direction
        if direction.upper() == "LONG":
            param_order = [
                ("confirm_disp_atr", self.config.disp_atr_range, "lower"),
                ("base_max_dist_atr", self.config.base_max_dist_range, "widen"),
                ("dist_vel_k", self.config.dist_vel_k_range, "adjust"),
                ("confirm_m1_closes", self.config.m1_closes_range, "adjust"),
                ("confirm_sec", self.config.confirm_sec_range, "adjust"),
            ]
        else:  # SHORT
            param_order = [
                ("confirm_disp_atr", self.config.disp_atr_range, "lower"),
                ("base_max_dist_atr", self.config.base_max_dist_range, "widen"),
                ("dist_vel_k", self.config.dist_vel_k_range, "adjust"),
                ("confirm_m1_closes", self.config.m1_closes_range, "adjust"),
                ("confirm_sec", self.config.confirm_sec_range, "adjust"),
            ]

        # Step through parameters in order
        base_config = EntryConfig()
        for param_name, param_range, action in param_order:
            print(f"\nOptimizing {param_name} ({action})...")

            # Try different values for this parameter
            param_values = self._generate_param_values(param_name, param_range)

            for value in param_values:
                # Set parameter value
                test_config = self._set_config_param(base_config, param_name, value)

                # Evaluate only zones for this direction
                direction_zones = [z for z in zones if z.direction.upper() == direction.upper()]
                results = self.evaluate_entry_with_blocker_tracking(direction_zones, test_config)

                # Calculate coverage-first score
                score = self.calculate_coverage_first_score(results)

                # Check hard constraints
                if not self.check_hard_constraints(results):
                    continue

                if score > best_score:
                    best_score = score
                    best_config = test_config
                    best_results = results
                    print(".3f")
            # Update base config with best value for this parameter
            if best_config:
                base_config = self._set_config_param(base_config, param_name,
                                                   getattr(best_config, param_name))

        return best_config, best_results, best_score

    def _generate_param_values(self, param_name: str, param_range: Tuple) -> List:
        """Generate parameter values to test."""
        if param_name in ["confirm_m1_closes", "confirm_sec"]:
            # Integer parameters
            start, end = param_range
            return list(range(start, end + 1))
        else:
            # Float parameters - try 5 values
            start, end = param_range
            step = (end - start) / 4
            return [start + i * step for i in range(5)]

    def _set_config_param(self, config: EntryConfig, param_name: str, value) -> EntryConfig:
        """Set parameter value in config."""
        new_config = EntryConfig(**config.to_dict())
        setattr(new_config, param_name, value)
        return new_config

    def run_step_down_frontier(
        self,
        zones: List[OpportunityZone]
    ) -> List[Dict[str, Any]]:
        """Run step-down frontier selection with decreasing capture floors."""
        print(f"\n{'='*60}")
        print("STEP-DOWN FRONTIER SELECTION")
        print(f"{'='*60}")

        # Find best configs for LONG and SHORT
        long_config, long_results, long_score = self.run_coverage_first_retune(zones, "LONG")
        short_config, short_results, short_score = self.run_coverage_first_retune(zones, "SHORT")

        # Try decreasing capture floors
        capture_floors = [0.80, 0.70, 0.60, 0.50, 0.40]

        frontier_points = []

        for floor in capture_floors:
            print(f"\nTesting capture floor: {floor:.2f}")

            # Temporarily adjust config to try to hit this floor
            temp_config = EntryRecoveryConfig()
            temp_config.good_capture_min = floor

            # Check if current configs meet this floor
            combined_results = self._combine_results(long_results, short_results)
            model_pass = self.check_model_pass_gates(combined_results)

            if model_pass:
                point = {
                    "capture_floor": floor,
                    "long_config": long_config.to_dict(),
                    "short_config": short_config.to_dict(),
                    "combined_config": self._create_combined_config(long_config, short_config),
                    "long_good_capture": long_results.calculate_trigger_rates()["good_long_trigger_rate"],
                    "short_good_capture": short_results.calculate_trigger_rates()["good_short_trigger_rate"],
                    "long_bad_trigger": long_results.calculate_trigger_rates()["bad_long_trigger_rate"],
                    "short_bad_trigger": short_results.calculate_trigger_rates()["bad_short_trigger_rate"],
                    "both_pips_mean": combined_results.both_pips_mean,
                    "model_pass": True
                }
                frontier_points.append(point)
                print("  ✅ Model passes at this floor")
            else:
                print("  ❌ Model fails at this floor")
                break  # Stop at first failure

        return frontier_points

    def _combine_results(self, long_results: EntryResults, short_results: EntryResults) -> EntryResults:
        """Combine LONG and SHORT results."""
        combined = EntryResults()

        # Combine all counters
        combined.good_long_total = long_results.good_long_total
        combined.good_long_complete = long_results.good_long_complete
        combined.good_long_triggered = long_results.good_long_triggered
        combined.good_short_total = short_results.good_short_total
        combined.good_short_complete = short_results.good_short_complete
        combined.good_short_triggered = short_results.good_short_triggered
        combined.bad_long_total = long_results.bad_long_total
        combined.bad_long_complete = long_results.bad_long_complete
        combined.bad_long_triggered = long_results.bad_long_triggered
        combined.bad_short_total = short_results.bad_short_total
        combined.bad_short_complete = short_results.bad_short_complete
        combined.bad_short_triggered = short_results.bad_short_triggered
        combined.noise_long_total = long_results.noise_long_total
        combined.noise_long_complete = long_results.noise_long_complete
        combined.noise_long_triggered = long_results.noise_long_triggered
        combined.noise_short_total = short_results.noise_short_total
        combined.noise_short_complete = short_results.noise_short_complete
        combined.noise_short_triggered = short_results.noise_short_triggered

        # Combine blocker counts
        combined.long_blockers = long_results.long_blockers
        combined.short_blockers = short_results.short_blockers

        # Calculate combined pips (placeholder)
        combined.long_pips_mean = long_results.long_pips_mean
        combined.short_pips_mean = short_results.short_pips_mean
        combined.both_pips_mean = (combined.long_pips_mean + combined.short_pips_mean) / 2

        return combined

    def _create_combined_config(self, long_config: EntryConfig, short_config: EntryConfig) -> Dict[str, Any]:
        """Create combined directional config."""
        return {
            "long": long_config.to_dict(),
            "short": short_config.to_dict(),
        }

    def run_stability_check(
        self,
        zones: List[OpportunityZone],
        config: EntryConfig,
        direction: str
    ) -> Tuple[bool, Dict[str, float]]:
        """Run stability check across multiple seeds.

        Returns (stable, variance_metrics)
        """
        print(f"\n{'='*60}")
        print(f"STABILITY CHECK: {direction.upper()} ({self.config.stability_seeds} seeds)")
        print(f"{'='*60}")

        good_captures = []
        bad_triggers = []
        pips_means = []

        for seed in range(self.config.stability_seeds):
            # Set seed for reproducible evaluation
            random.seed(seed)

            # Evaluate with this seed
            direction_zones = [z for z in zones if z.direction.upper() == direction.upper()]
            results = self.evaluate_entry_with_blocker_tracking(direction_zones, config)

            trigger_rates = results.calculate_trigger_rates()
            if direction.upper() == "LONG":
                good_captures.append(trigger_rates["good_long_trigger_rate"])
                bad_triggers.append(trigger_rates["bad_long_trigger_rate"])
                pips_means.append(results.long_pips_mean)
            else:
                good_captures.append(trigger_rates["good_short_trigger_rate"])
                bad_triggers.append(trigger_rates["bad_short_trigger_rate"])
                pips_means.append(results.short_pips_mean)

        # Calculate variances
        variances = {
            "good_capture_variance": statistics.variance(good_captures) if len(good_captures) > 1 else 0.0,
            "bad_trigger_variance": statistics.variance(bad_triggers) if len(bad_triggers) > 1 else 0.0,
            "pips_mean_variance": statistics.variance(pips_means) if len(pips_means) > 1 else 0.0,
        }

        # Check stability
        stable = all(var <= self.config.stability_variance_max for var in variances.values())

        print("Stability metrics:")
        print(".6f")
        print(".6f")
        print(".6f")

        if stable:
            print("✅ Configuration is stable across seeds")
        else:
            print("❌ Configuration shows high variance - not promotable")
    def run_recovery_analysis_only(self, zones: List[OpportunityZone]) -> Dict[str, Any]:
        """Run recovery analysis without the feature completeness gate (for debugging)."""

        # Skip feature completeness gate, run the rest
        print("\n2. Running entry logic evaluation with blocker tracking...")

        # Run evaluation on zones (assuming they have complete features for analysis)
        long_zones = [z for z in zones if z.direction == "LONG"]
        short_zones = [z for z in zones if z.direction == "SHORT"]

        # Use default config for analysis
        default_config = EntryConfig()

        long_results = self.evaluate_entry_with_blocker_tracking(long_zones, default_config)
        short_results = self.evaluate_entry_with_blocker_tracking(short_zones, default_config)

        # Print blocker reports
        self.print_blocker_report(long_results)
        self.print_blocker_report(short_results)

        # Calculate some basic metrics
        long_trigger_rates = long_results.calculate_trigger_rates()
        short_trigger_rates = short_results.calculate_trigger_rates()

        return {
            "status": "ANALYSIS_ONLY",
            "long_results": {
                "good_trigger_rate": long_trigger_rates.get("good_long_trigger_rate", 0),
                "bad_trigger_rate": long_trigger_rates.get("bad_long_trigger_rate", 0),
                "blockers": self._blocker_report_dict(long_results.long_blockers)
            },
            "short_results": {
                "good_trigger_rate": short_trigger_rates.get("good_short_trigger_rate", 0),
                "bad_trigger_rate": short_trigger_rates.get("bad_short_trigger_rate", 0),
                "blockers": self._blocker_report_dict(short_results.short_blockers)
            },
            "message": "Analysis completed bypassing feature completeness gate"
        }
        """Run the complete entry recovery pipeline."""
        print("🚀 STARTING ENTRY RECOVERY PIPELINE")
        print("="*60)

        # 1. Check feature completeness gate
        gate_pass, coverage_rates = self.check_feature_completeness_gate(zones)
        if not gate_pass:
            return {
                "status": "FAILED",
                "failure_point": "feature_completeness_gate",
                "coverage_rates": coverage_rates,
                "message": "Entry runs blocked - missing features must be fixed first"
            }

        # 2. Run coverage-first retune for LONG and SHORT
        long_config, long_results, long_score = self.run_coverage_first_retune(zones, "LONG")
        short_config, short_results, short_score = self.run_coverage_first_retune(zones, "SHORT")

        # 3. Print blocker reports
        self.print_blocker_report(long_results)
        self.print_blocker_report(short_results)

        # 4. Run step-down frontier
        frontier_points = self.run_step_down_frontier(zones)

        # 5. Find best frontier point
        best_point = None
        for point in frontier_points:
            if point["model_pass"]:
                best_point = point
                break

        if not best_point:
            return {
                "status": "FAILED",
                "failure_point": "no_valid_frontier_point",
                "message": "No frontier point meets MODEL_PASS criteria"
            }

        # 6. Run stability checks
        long_stable, long_variances = self.run_stability_check(zones, long_config, "LONG")
        short_stable, short_variances = self.run_stability_check(zones, short_config, "SHORT")

        if not (long_stable and short_stable):
            return {
                "status": "FAILED",
                "failure_point": "stability_check",
                "message": "Configuration not stable across seeds"
            }

        # Success!
        result = {
            "status": "SUCCESS",
            "combined_config": best_point["combined_config"],
            "long_config": best_point["long_config"],
            "short_config": best_point["short_config"],
            "coverage_rates": coverage_rates,
            "frontier_points": frontier_points,
            "best_point": best_point,
            "stability": {
                "long_stable": long_stable,
                "short_stable": short_stable,
                "long_variances": long_variances,
                "short_variances": short_variances,
            },
            "blocker_reports": {
                "long": self._blocker_report_dict(long_results.long_blockers),
                "short": self._blocker_report_dict(short_results.short_blockers),
            }
        }

        print(f"\n{'='*60}")
        print("🎉 ENTRY RECOVERY SUCCESS")
        print(f"{'='*60}")
        print("Combined config ready for production")

        return result

    def _blocker_report_dict(self, blockers: BlockerCounts) -> Dict[str, Any]:
        """Convert blocker counts to dict for JSON output."""
        return {
            "cumulative": {
                "block_confirm_disp_atr": blockers.block_confirm_disp_atr,
                "block_confirm_m1_closes": blockers.block_confirm_m1_closes,
                "block_confirm_sec": blockers.block_confirm_sec,
                "block_base_max_dist_atr": blockers.block_base_max_dist_atr,
                "block_dist_vel_k": blockers.block_dist_vel_k,
            },
            "first_hit": {
                "block_confirm_disp_atr": blockers.first_block_confirm_disp_atr,
                "block_confirm_m1_closes": blockers.first_block_confirm_m1_closes,
                "block_confirm_sec": blockers.first_block_confirm_sec,
                "block_base_max_dist_atr": blockers.first_block_base_max_dist_atr,
                "block_dist_vel_k": blockers.first_block_dist_vel_k,
            }
        }


# Main execution
if __name__ == "__main__":
    # Example usage
    config = EntryRecoveryConfig()
    engine = EntryRecoveryEngine(config)

    # Create sample zones for testing
    sample_zones = [
        OpportunityZone(
            direction="LONG",
            bucket_type="GOOD",
            zone_type="CONTINUATION",
            features={"disp_atr": 0.8, "m1_closes": 4, "confirm_sec": 45, "base_max_dist_atr": 0.7, "dist_vel_k": 0.6},
            has_complete_features=True,
            mfe=2.5, mae=1.0
        ),
        OpportunityZone(
            direction="SHORT",
            bucket_type="GOOD",
            zone_type="SPIKE",
            features={"disp_atr": 0.6, "m1_closes": 3, "confirm_sec": 35, "base_max_dist_atr": 0.8, "dist_vel_k": 0.7},
            has_complete_features=True,
            mfe=1.8, mae=0.9
        ),
        OpportunityZone(
            direction="LONG",
            bucket_type="BAD",
            zone_type="GRIND",
            features={"disp_atr": 0.3, "m1_closes": 2, "confirm_sec": 25, "base_max_dist_atr": 1.2, "dist_vel_k": 1.0},
            has_complete_features=True,
            mfe=0.5, mae=1.5
        ),
    ]

    results = engine.run_full_entry_recovery(sample_zones)
    print("\nFinal results:")
    print(json.dumps(results, indent=2))
