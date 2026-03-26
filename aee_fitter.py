#!/usr/bin/env python3
"""
AEE Fitter

Purpose: Learn exit management that extends winners and cuts losers early.

AEE Path Features (as specified in research architecture):

current_profit: current_price - entry_price
MFE: max profit reached
giveback: MFE - current_profit
velocity_decay: previous_velocity - current_velocity
bars_since_new_high: time since last new high
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from enum import Enum
import statistics

from aee_synthetic_evaluator import AEEKnobs


class BucketType(Enum):
    """Opportunity bucket classification."""
    GOOD = "good"
    BAD = "bad"
    NOISE = "noise"


@dataclass
class AEEPathFeatures:
    """AEE path features calculated during trade."""
    current_profit: float      # current_price - entry_price (in price units)
    mfe: float                 # max profit reached (in price units)
    giveback: float            # MFE - current_profit (in price units)
    velocity_decay: float      # previous_velocity - current_velocity
    bars_since_new_high: int   # bars since last new high


@dataclass
class AEEAction:
    """AEE action at a point in time."""
    timestamp: datetime
    features: AEEPathFeatures
    action: str  # "HOLD", "PROFIT_HARVEST", "PANIC_EXIT", "EXTENSION"


@dataclass
class AEESimulationResult:
    """Result of simulating AEE on an opportunity."""
    opportunity_id: str
    direction: str
    bucket: str
    actions: List[AEEAction]
    final_r: float
    mfe_r: float
    bars_held: int
    exit_reason: str


@dataclass
class AEEFitResult:
    """Result of AEE fitting."""
    knobs: AEEKnobs
    good_extension_rate: float    # % of GOOD trades extended beyond static TP
    bad_cutoff_rate: float        # % of BAD trades cut early
    overall_score: float          # extension_rate + cutoff_rate


class AEEFitter:
    """
    Learns AEE parameters that extend winners and cut losers early.

    Focus: extend winners, cut losers early, harvest stalled profits
    """

    def __init__(self):
        self.baseline_knobs = AEEKnobs(
            profit_capture_min_atr=0.35,
            allowed_giveback_atr_mult=0.45,
            stall_min_profit_atr=0.2,
            decay_exit_min_profit=0.1,
            decay_min_hold_sec=120.0,
            panic_velocity=-0.8,
            panic_pullback=0.6,
            max_hold_sec=3600.0  # 1 hour max
        )

    def fit_aee_from_opportunities(self, opportunities: List[Dict]) -> AEEFitResult:
        """
        Fit AEE parameters from analyzed opportunities.

        Args:
            opportunities: List of analyzed opportunity dicts from Phase 2

        Returns:
            Best AEE configuration with performance metrics
        """
        print("Fitting AEE logic from opportunities...")

        # Separate by bucket
        good_opps = [o for o in opportunities if o["bucket"] == "good"]
        bad_opps = [o for o in opportunities if o["bucket"] == "bad"]

        print(f"  GOOD opportunities: {len(good_opps)}")
        print(f"  BAD opportunities: {len(bad_opps)}")

        # Grid search over AEE parameter combinations
        best_result = None
        best_score = float('-inf')

        # Parameter ranges to test (focused around baseline)
        param_ranges = {
            "profit_capture_min_atr": [0.25, 0.35, 0.45],
            "allowed_giveback_atr_mult": [0.35, 0.45, 0.55],
            "panic_velocity": [-1.0, -0.8, -0.6],
            "decay_min_hold_sec": [60.0, 120.0, 180.0]
        }

        # Grid search
        for profit_cap in param_ranges["profit_capture_min_atr"]:
            for giveback in param_ranges["allowed_giveback_atr_mult"]:
                for panic_vel in param_ranges["panic_velocity"]:
                    for decay_hold in param_ranges["decay_min_hold_sec"]:

                        knobs = AEEKnobs(
                            profit_capture_min_atr=profit_cap,
                            allowed_giveback_atr_mult=giveback,
                            panic_velocity=panic_vel,
                            decay_min_hold_sec=decay_hold
                        )

                        # Evaluate on opportunities
                        result = self._evaluate_knobs(knobs, good_opps, bad_opps)

                        if result.overall_score > best_score:
                            best_score = result.overall_score
                            best_result = result

        print("""
Best AEE configuration found:""")
        print(f"  profit_capture_min_atr: {best_result.knobs.profit_capture_min_atr}")
        print(f"  allowed_giveback_atr_mult: {best_result.knobs.allowed_giveback_atr_mult}")
        print(f"  panic_velocity: {best_result.knobs.panic_velocity}")
        print(f"  decay_min_hold_sec: {best_result.knobs.decay_min_hold_sec}")
        print("""
Performance:""")
        print(f"  Good extension rate: {best_result.good_extension_rate:.1%}")
        print(f"  Bad cutoff rate: {best_result.bad_cutoff_rate:.1%}")
        print(f"  Overall score: {best_result.overall_score:.3f}")

        return best_result

    def _evaluate_knobs(self, knobs: AEEKnobs, good_opps: List[Dict],
                       bad_opps: List[Dict]) -> AEEFitResult:
        """
        Evaluate AEE knobs on opportunities.

        Reward: extra profit captured + loss avoided
        Penalty: premature winner exits + holding losers
        """

        # Simulate AEE on GOOD opportunities (should extend winners)
        good_extension_count = 0
        for opp in good_opps:
            sim_result = self._simulate_aee_on_opportunity(knobs, opp)
            # Calculate static R for comparison (simple TP/SL at 2.5 ATR)
            static_r = self._calculate_static_r(opp)
            if sim_result.final_r > static_r:
                good_extension_count += 1

        good_extension_rate = good_extension_count / len(good_opps) if good_opps else 0

        # Simulate AEE on BAD opportunities (should cut losses early)
        bad_cutoff_count = 0
        for opp in bad_opps:
            sim_result = self._simulate_aee_on_opportunity(knobs, opp)
            static_r = self._calculate_static_r(opp)
            if sim_result.final_r > static_r:  # Cut losses better than static
                bad_cutoff_count += 1

        bad_cutoff_rate = bad_cutoff_count / len(bad_opps) if bad_opps else 0

        # Overall score
        overall_score = good_extension_rate + bad_cutoff_rate

        return AEEFitResult(
            knobs=knobs,
            good_extension_rate=good_extension_rate,
            bad_cutoff_rate=bad_cutoff_rate,
            overall_score=overall_score
        )

    def _calculate_static_r(self, opportunity: Dict) -> float:
        """
        Calculate static R from simple TP/SL strategy (2.5 ATR targets).

        This simulates what a basic static exit strategy would achieve.
        """
        params = opportunity["aee_params"]
        direction = opportunity["direction"]

        # Get TP/SL prices (already calculated in phase 2)
        tp_price = params["tp_price"]
        sl_price = params["sl_price"]
        entry_price = params["entry_price"]
        atr_pips = params["atr_pips"]

        # Simulate hitting TP or SL
        # For simplicity, assume we hit TP (optimistic baseline)
        if direction == "LONG":
            profit_pips = (tp_price - entry_price) * 10000
        else:  # SHORT
            profit_pips = (entry_price - tp_price) * 10000

        # Convert to R (profit in ATR units)
        static_r = profit_pips / atr_pips if atr_pips > 0 else 0.0

        return static_r

    def _simulate_aee_on_opportunity(self, knobs: AEEKnobs, opportunity: Dict) -> AEESimulationResult:
        """
        Simulate AEE behavior on a single opportunity.

        This simulates the AEE path features and decision logic.
        """
        price_path = opportunity.get("full_price_path", [])
        if not price_path:
            return AEESimulationResult(
                opportunity_id=opportunity["opportunity_id"],
                direction=opportunity["direction"],
                bucket=opportunity["bucket"],
                actions=[],
                final_r=0.0,
                mfe_r=0.0,
                bars_held=0,
                exit_reason="NO_DATA"
            )

        # Extract trade parameters
        entry_price = opportunity["aee_params"]["entry_price"]
        direction = opportunity["direction"]
        atr_pips = opportunity["aee_params"]["atr_pips"]

        # Initialize AEE state
        actions = []
        max_profit = 0.0
        bars_since_new_high = 0
        last_velocity = 0.0

        # Simulate through price path
        for i, candle in enumerate(price_path):
            current_price = candle["close"]
            timestamp = candle["timestamp"]

            # Calculate current profit
            if direction == "LONG":
                current_profit = current_price - entry_price
            else:
                current_profit = entry_price - current_price

            # Update MFE
            max_profit = max(max_profit, current_profit)

            # Calculate giveback
            giveback = max_profit - current_profit

            # Calculate velocity (simplified)
            if i > 0:
                prev_price = price_path[i-1]["close"]
                price_change = current_price - prev_price
                time_change = 60.0  # Assume 1-minute bars
                current_velocity = price_change / time_change
            else:
                current_velocity = 0.0

            # Calculate velocity decay
            velocity_decay = last_velocity - current_velocity
            last_velocity = current_velocity

            # Update bars since new high
            if current_profit >= max_profit - 0.00001:  # Essentially equal
                bars_since_new_high = 0
            else:
                bars_since_new_high += 1

            # Create path features
            features = AEEPathFeatures(
                current_profit=current_profit,
                mfe=max_profit,
                giveback=giveback,
                velocity_decay=velocity_decay,
                bars_since_new_high=bars_since_new_high
            )

            # Determine AEE action based on features and knobs
            action = self._determine_aee_action(features, knobs, i, atr_pips, direction)

            actions.append(AEEAction(
                timestamp=timestamp,
                features=features,
                action=action
            ))

            # Check for exit conditions
            if action in ["PROFIT_HARVEST", "PANIC_EXIT"]:
                break

        # Calculate final results
        final_profit = actions[-1].features.current_profit if actions else 0.0
        final_r = (final_profit / (atr_pips / 10000)) if atr_pips > 0 else 0.0
        mfe_r = (max_profit / (atr_pips / 10000)) if atr_pips > 0 else 0.0
        bars_held = len(actions)

        exit_reason = actions[-1].action if actions else "NO_ACTIONS"

        return AEESimulationResult(
            opportunity_id=opportunity["opportunity_id"],
            direction=direction,
            bucket=opportunity["bucket"],
            actions=actions,
            final_r=final_r,
            mfe_r=mfe_r,
            bars_held=bars_held,
            exit_reason=exit_reason
        )

    def _determine_aee_action(self, features: AEEPathFeatures, knobs: AEEKnobs,
                            bars_held: int, atr_pips: float, direction: str) -> str:
        """
        Determine AEE action based on path features and knobs.

        Actions: HOLD, PROFIT_HARVEST, PANIC_EXIT, EXTENSION
        """

        # Convert features to ATR-normalized units
        atr_unit = atr_pips / 10000
        profit_atr = features.current_profit / atr_unit if atr_unit > 0 else 0
        giveback_atr = features.giveback / atr_unit if atr_unit > 0 else 0

        # PANIC EXIT: Strong reversal detected
        if features.velocity_decay > abs(knobs.panic_velocity) or giveback_atr > knobs.panic_pullback:
            return "PANIC_EXIT"

        # PROFIT HARVEST: Trade stalls but still profitable
        min_profit_threshold = knobs.profit_capture_min_atr
        if (profit_atr >= min_profit_threshold and
            features.bars_since_new_high >= 5 and  # Stalled for 5+ bars
            giveback_atr > knobs.allowed_giveback_atr_mult):
            return "PROFIT_HARVEST"

        # EXTENSION: Allow trade to run beyond target
        if profit_atr > min_profit_threshold * 2:  # Well beyond target
            return "EXTENSION"

        # HOLD: Continue trade
        return "HOLD"

    def save_aee_config(self, result: AEEFitResult, output_path: str):
        """Save AEE configuration to JSON file."""
        data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "config": {
                "profit_capture_min_atr": result.knobs.profit_capture_min_atr,
                "allowed_giveback_atr_mult": result.knobs.allowed_giveback_atr_mult,
                "stall_min_profit_atr": result.knobs.stall_min_profit_atr,
                "decay_exit_min_profit": result.knobs.decay_exit_min_profit,
                "decay_min_hold_sec": result.knobs.decay_min_hold_sec,
                "panic_velocity": result.knobs.panic_velocity,
                "panic_pullback": result.knobs.panic_pullback,
                "max_hold_sec": result.knobs.max_hold_sec
            },
            "performance": {
                "good_extension_rate": result.good_extension_rate,
                "bad_cutoff_rate": result.bad_cutoff_rate,
                "overall_score": result.overall_score
            }
        }

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved AEE configuration to {output_path}")


def main():
    """Run the AEE Fitter on Phase 2 results."""
    import argparse

    parser = argparse.ArgumentParser(description="AEE Fitter")
    parser.add_argument("--phase2-results", required=True, help="Path to Phase 2 analysis JSON")
    parser.add_argument("--output", default="aee_config.json", help="Output file path")

    args = parser.parse_args()

    # Load Phase 2 results
    print("Loading Phase 2 analysis results...")
    with open(args.phase2_results, 'r') as f:
        phase2_data = json.load(f)

    opportunities = phase2_data["opportunities"]
    print(f"Loaded {len(opportunities)} analyzed opportunities")

    # Fit AEE logic
    fitter = AEEFitter()
    result = fitter.fit_aee_from_opportunities(opportunities)

    # Save results
    fitter.save_aee_config(result, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
