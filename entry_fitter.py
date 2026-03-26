#!/usr/bin/env python3
"""
Entry Fitter

Purpose: Learn entry thresholds that separate GOOD from BAD/NOISE.

Entry Features (as specified in research architecture):

confirm_disp_atr: Measures displacement relative to ATR
confirm_m1_closes: Number of consecutive M1 closes confirming direction
confirm_sec: Time delay before entry
base_max_dist_atr: Maximum distance from reference price allowed for entry
dist_vel_k: Penalty based on velocity of distance expansion
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from enum import Enum
import statistics


class BucketType(Enum):
    """Opportunity bucket classification."""
    GOOD = "good"
    BAD = "bad"
    NOISE = "noise"


@dataclass
class EntryConfig:
    """Entry configuration parameters."""
    confirm_disp_atr: float  # Displacement relative to ATR
    confirm_m1_closes: int   # Consecutive M1 closes confirming direction
    confirm_sec: float       # Time delay before entry (seconds)
    base_max_dist_atr: float # Maximum distance from reference price (ATR multiples)
    dist_vel_k: float        # Penalty based on velocity of distance expansion


@dataclass
class EntryFeatures:
    """Calculated entry features for an opportunity."""
    confirm_disp_atr: float
    confirm_m1_closes: int
    confirm_sec: float
    base_max_dist_atr: float
    dist_vel_k: float


@dataclass
class EntryFitResult:
    """Result of entry fitting."""
    config: EntryConfig
    good_capture_rate: float
    bad_trigger_rate: float
    noise_trigger_rate: float
    overall_score: float


class EntryFitter:
    """
    Learns entry thresholds that separate GOOD from BAD/NOISE opportunities.

    Goal: maximize GOOD capture, minimize BAD triggers, minimize NOISE triggers
    """

    def __init__(self):
        self.baseline_config = EntryConfig(
            confirm_disp_atr=0.5,
            confirm_m1_closes=2,
            confirm_sec=30.0,
            base_max_dist_atr=1.5,
            dist_vel_k=0.8
        )

    def fit_entry_from_opportunities(self, opportunities: List[Dict]) -> EntryFitResult:
        """
        Fit entry parameters from analyzed opportunities.

        Args:
            opportunities: List of analyzed opportunity dicts from Phase 2

        Returns:
            Best entry configuration with performance metrics
        """
        print("Fitting entry logic from opportunities...")

        # Separate by bucket
        good_opps = [o for o in opportunities if o["bucket"] == "good"]
        bad_opps = [o for o in opportunities if o["bucket"] == "bad"]
        noise_opps = [o for o in opportunities if o["bucket"] == "noise"]

        print(f"  GOOD opportunities: {len(good_opps)}")
        print(f"  BAD opportunities: {len(bad_opps)}")
        print(f"  NOISE opportunities: {len(noise_opps)}")

        # Calculate features for all opportunities
        all_features = []
        for opp in opportunities:
            features = self._calculate_entry_features(opp)
            all_features.append((opp, features))

        # Grid search over parameter combinations
        best_result = None
        best_score = float('-inf')

        # Parameter ranges to test
        param_ranges = {
            "confirm_disp_atr": [0.2, 0.5, 0.8, 1.0],
            "confirm_m1_closes": [1, 2, 3],
            "confirm_sec": [10.0, 30.0, 60.0],
            "base_max_dist_atr": [1.0, 1.5, 2.0],
            "dist_vel_k": [0.5, 0.8, 1.0]
        }

        # Simple grid search (in production, use more sophisticated optimization)
        for disp_atr in param_ranges["confirm_disp_atr"]:
            for m1_closes in param_ranges["confirm_m1_closes"]:
                for sec in param_ranges["confirm_sec"]:
                    for max_dist in param_ranges["base_max_dist_atr"]:
                        for vel_k in param_ranges["dist_vel_k"]:

                            config = EntryConfig(
                                confirm_disp_atr=disp_atr,
                                confirm_m1_closes=m1_closes,
                                confirm_sec=sec,
                                base_max_dist_atr=max_dist,
                                dist_vel_k=vel_k
                            )

                            # Evaluate this configuration
                            result = self._evaluate_config(config, all_features, good_opps, bad_opps, noise_opps)

                            if result.overall_score > best_score:
                                best_score = result.overall_score
                                best_result = result

        print("""
Best entry configuration found:""")
        print(f"  confirm_disp_atr: {best_result.config.confirm_disp_atr}")
        print(f"  confirm_m1_closes: {best_result.config.confirm_m1_closes}")
        print(f"  confirm_sec: {best_result.config.confirm_sec}")
        print(f"  base_max_dist_atr: {best_result.config.base_max_dist_atr}")
        print(f"  dist_vel_k: {best_result.config.dist_vel_k}")
        print("""
Performance:""")
        print(f"  Good capture rate: {best_result.good_capture_rate:.1%}")
        print(f"  Bad trigger rate: {best_result.bad_trigger_rate:.1%}")
        print(f"  Noise trigger rate: {best_result.noise_trigger_rate:.1%}")
        print(f"  Overall score: {best_result.overall_score:.3f}")

        return best_result

    def _calculate_entry_features(self, opportunity: Dict) -> EntryFeatures:
        """
        Calculate entry features for an opportunity.

        confirm_disp_atr: Measures displacement relative to ATR
        confirm_m1_closes: Number of consecutive M1 closes confirming direction
        confirm_sec: Time delay before entry
        base_max_dist_atr: Maximum distance from reference price allowed for entry
        dist_vel_k: Penalty based on velocity of distance expansion
        """
        price_path = opportunity.get("full_price_path", [])
        start_time = datetime.fromisoformat(opportunity["start_time"])
        direction = opportunity["direction"]
        atr_pips = opportunity["aee_params"]["atr_pips"]

        if not price_path:
            return EntryFeatures(0.0, 0, 0.0, 0.0, 0.0)

        # Find entry candle (first candle at/after start_time)
        entry_candle = None
        for candle in price_path:
            if candle["timestamp"] >= start_time:
                entry_candle = candle
                break

        if not entry_candle:
            return EntryFeatures(0.0, 0, 0.0, 0.0, 0.0)

        # confirm_disp_atr: displacement relative to ATR
        # For now, simplified - distance from entry to some reference
        reference_price = price_path[0]["close"] if price_path else entry_candle["close"]
        displacement = abs(entry_candle["close"] - reference_price)
        confirm_disp_atr = (displacement * 10000) / atr_pips if atr_pips > 0 else 0.0

        # confirm_m1_closes: consecutive closes in direction
        confirm_m1_closes = self._calculate_consecutive_closes(price_path, start_time, direction)

        # confirm_sec: time delay before entry (simplified)
        confirm_sec = (entry_candle["timestamp"] - start_time).total_seconds()

        # base_max_dist_atr: distance from reference in ATR terms
        base_max_dist_atr = confirm_disp_atr

        # dist_vel_k: penalty based on velocity (simplified)
        # Higher values mean faster expansion (more penalty)
        dist_vel_k = min(2.0, confirm_disp_atr / max(0.1, confirm_sec / 60.0))  # Distance per minute

        return EntryFeatures(
            confirm_disp_atr=confirm_disp_atr,
            confirm_m1_closes=confirm_m1_closes,
            confirm_sec=confirm_sec,
            base_max_dist_atr=base_max_dist_atr,
            dist_vel_k=dist_vel_k
        )

    def _calculate_consecutive_closes(self, price_path: List[Dict], start_time: datetime,
                                    direction: str) -> int:
        """Calculate number of consecutive M1 closes confirming direction."""
        consecutive = 0

        for candle in price_path:
            if candle["timestamp"] >= start_time:
                if direction == "LONG":
                    if candle["close"] > candle["open"]:  # Bullish close
                        consecutive += 1
                    else:
                        break
                else:  # SHORT
                    if candle["close"] < candle["open"]:  # Bearish close
                        consecutive += 1
                    else:
                        break

        return consecutive

    def _evaluate_config(self, config: EntryConfig, all_features: List[tuple],
                        good_opps: List[Dict], bad_opps: List[Dict],
                        noise_opps: List[Dict]) -> EntryFitResult:
        """
        Evaluate an entry configuration.

        entry_score = good_capture - bad_trigger - noise_trigger
        """

        # Count triggers for each bucket
        good_triggers = 0
        bad_triggers = 0
        noise_triggers = 0

        for opp, features in all_features:
            if self._would_trigger_entry(config, features):
                bucket = opp["bucket"]
                if bucket == "good":
                    good_triggers += 1
                elif bucket == "bad":
                    bad_triggers += 1
                elif bucket == "noise":
                    noise_triggers += 1

        # Calculate rates
        good_capture_rate = good_triggers / len(good_opps) if good_opps else 0
        bad_trigger_rate = bad_triggers / len(bad_opps) if bad_opps else 0
        noise_trigger_rate = noise_triggers / len(noise_opps) if noise_opps else 0

        # Calculate overall score
        overall_score = good_capture_rate - bad_trigger_rate - noise_trigger_rate

        return EntryFitResult(
            config=config,
            good_capture_rate=good_capture_rate,
            bad_trigger_rate=bad_trigger_rate,
            noise_trigger_rate=noise_trigger_rate,
            overall_score=overall_score
        )

    def _would_trigger_entry(self, config: EntryConfig, features: EntryFeatures) -> bool:
        """
        Determine if entry would trigger based on features and config.
        """
        return (
            features.confirm_disp_atr >= config.confirm_disp_atr and
            features.confirm_m1_closes >= config.confirm_m1_closes and
            features.confirm_sec >= config.confirm_sec and
            features.base_max_dist_atr <= config.base_max_dist_atr and
            features.dist_vel_k <= config.dist_vel_k
        )

    def save_entry_config(self, result: EntryFitResult, output_path: str):
        """Save entry configuration to JSON file."""
        data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "config": {
                "confirm_disp_atr": result.config.confirm_disp_atr,
                "confirm_m1_closes": result.config.confirm_m1_closes,
                "confirm_sec": result.config.confirm_sec,
                "base_max_dist_atr": result.config.base_max_dist_atr,
                "dist_vel_k": result.config.dist_vel_k
            },
            "performance": {
                "good_capture_rate": result.good_capture_rate,
                "bad_trigger_rate": result.bad_trigger_rate,
                "noise_trigger_rate": result.noise_trigger_rate,
                "overall_score": result.overall_score
            }
        }

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved entry configuration to {output_path}")


def main():
    """Run the Entry Fitter on Phase 2 results."""
    import argparse

    parser = argparse.ArgumentParser(description="Entry Fitter")
    parser.add_argument("--phase2-results", required=True, help="Path to Phase 2 analysis JSON")
    parser.add_argument("--output", default="entry_config.json", help="Output file path")

    args = parser.parse_args()

    # Load Phase 2 results
    print("Loading Phase 2 analysis results...")
    with open(args.phase2_results, 'r') as f:
        phase2_data = json.load(f)

    opportunities = phase2_data["opportunities"]
    print(f"Loaded {len(opportunities)} analyzed opportunities")

    # Fit entry logic
    fitter = EntryFitter()
    result = fitter.fit_entry_from_opportunities(opportunities)

    # Save results
    fitter.save_entry_config(result, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
