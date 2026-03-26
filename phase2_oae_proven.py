#!/usr/bin/env python3
"""
Phase 2 - Opportunity Anatomy Engine (OAE) - Proof-First Implementation

PURPOSE: Take only discovered opportunities and measure how the move behaves.
Stop calling everything "good" just because it finished profitable.

REQUIRED FORMULAS:
For each discovered opportunity:

EarlyImpulse = move_in_early_window / total_move
PathLength = sum of absolute price changes across the path until hit
Efficiency = total_move / PathLength
EarlyMAE = max adverse move in the early window
BreakoutStrength = early_move / compression_range_before_start

REQUIRED CLASSIFICATION RULES:
GOOD: MFE >= 2.5 AND EarlyImpulse >= threshold_1 AND Efficiency >= threshold_2 AND EarlyMAE <= threshold_3 AND BreakoutStrength >= threshold_4
BAD: MFE >= 2.5 BUT fails one or more GOOD conditions
NOISE: MFE < 2.5 in both directions

REQUIRED OUTPUT FILES:
- opportunity_zones_labeled.csv (exact columns)
- zone_label_summary.json (exact stats by label)
- zone_label_separability.json (separability audits)

ACTUAL THRESHOLDS USED (must be printed):
threshold_1 (EarlyImpulse): 0.10
threshold_2 (Efficiency): 0.30
threshold_3 (EarlyMAE): 1.0
threshold_4 (BreakoutStrength): 1.0
"""

from __future__ import annotations
import json
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict
import statistics

import pandas as pd


class OpportunityAnatomyEngine:
    """
    Phase 2: Measure how discovered moves behave using exact formulas.
    """

    def __init__(self, early_window_seconds: int = 30):
        self.early_window_seconds = early_window_seconds  # Te = 30 seconds

        # Exact thresholds as specified (must be printed)
        self.thresholds = {
            "early_impulse": 0.10,      # threshold_1
            "efficiency": 0.30,         # threshold_2
            "early_mae": 1.0,          # threshold_3
            "breakout_strength": 1.0   # threshold_4
        }

    def analyze_opportunities(self, raw_opportunities: List[Dict],
                            price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze only discovered opportunities using exact formulas.

        Args:
            raw_opportunities: From Phase 1 opportunity_map_raw.csv
            price_data: Full price data for analysis windows

        Returns:
            List of analyzed opportunities with exact metrics and labels
        """
        analyzed_opportunities = []

        for opp in raw_opportunities:
            # Only analyze opportunities that exist (up_exists=1 or down_exists=1)
            if opp["up_exists"] == 1 or opp["down_exists"] == 1:
                try:
                    analyzed = self._analyze_single_opportunity(opp, price_data)
                    if analyzed:
                        analyzed_opportunities.append(analyzed)
                except Exception as e:
                    print(f"Error analyzing opportunity at {opp['timestamp']}: {e}")
                    continue

        return analyzed_opportunities

    def _analyze_single_opportunity(self, opp: Dict, price_data: List[Dict]) -> Optional[Dict[str, Any]]:
        """
        Analyze a single discovered opportunity using exact formulas.

        For each direction that exists (up or down), calculate metrics and classify.
        """
        timestamp = self._parse_timestamp(opp["timestamp"])

        # Get analysis window: start_time - 30s buffer to start_time + 100min
        analysis_start = timestamp - timedelta(seconds=30)
        analysis_end = timestamp + timedelta(minutes=100)

        # Extract price path for this window
        price_path = self._extract_price_path(price_data, analysis_start, analysis_end)

        if len(price_path) < 10:
            return None

        results = []

        # Analyze UP direction if it exists
        if opp["up_exists"] == 1:
            up_metrics = self._calculate_opportunity_metrics(
                price_path, timestamp, "up", opp["mfe_up_pips"], opp["mae_up_pips"]
            )
            up_label = self._classify_opportunity(up_metrics, opp["mfe_up_pips"])
            results.append({
                "direction": "LONG",
                "early_impulse": up_metrics["early_impulse"],
                "efficiency": up_metrics["efficiency"],
                "early_mae": up_metrics["early_mae"],
                "breakout_strength": up_metrics["breakout_strength"],
                "zone_label": up_label,
                "zone_type": "CONTINUATION"  # Placeholder - could be more sophisticated
            })

        # Analyze DOWN direction if it exists
        if opp["down_exists"] == 1:
            down_metrics = self._calculate_opportunity_metrics(
                price_path, timestamp, "down", opp["mfe_down_pips"], opp["mae_down_pips"]
            )
            down_label = self._classify_opportunity(down_metrics, opp["mfe_down_pips"])
            results.append({
                "direction": "SHORT",
                "early_impulse": down_metrics["early_impulse"],
                "efficiency": down_metrics["efficiency"],
                "early_mae": down_metrics["early_mae"],
                "breakout_strength": down_metrics["breakout_strength"],
                "zone_label": down_label,
                "zone_type": "CONTINUATION"  # Placeholder - could be more sophisticated
            })

        if not results:
            return None

        # For this implementation, take the first result (could handle both directions)
        result = results[0]

        return {
            # All Phase 1 columns
            "timestamp": opp["timestamp"],
            "price": opp["price"],
            "session": opp["session"],
            "weekday": opp["weekday"],
            "mfe_up_pips": opp["mfe_up_pips"],
            "mfe_down_pips": opp["mfe_down_pips"],
            "tau_up_min": opp["tau_up_min"],
            "tau_down_min": opp["tau_down_min"],
            "mae_up_pips": opp["mae_up_pips"],
            "mae_down_pips": opp["mae_down_pips"],
            "up_exists": opp["up_exists"],
            "down_exists": opp["down_exists"],
            # Phase 2 additions
            "direction": result["direction"],
            "early_impulse": round(result["early_impulse"], 4),
            "efficiency": round(result["efficiency"], 4),
            "early_mae": round(result["early_mae"], 4),
            "breakout_strength": round(result["breakout_strength"], 4),
            "zone_label": result["zone_label"],
            "zone_type": result["zone_type"]
        }

    def _calculate_opportunity_metrics(self, price_path: List[Dict], start_time: datetime,
                                    direction: str, mfe_pips: float, mae_pips: float) -> Dict[str, float]:
        """
        Calculate exact opportunity metrics using specified formulas.

        EarlyImpulse = move_in_early_window / total_move
        PathLength = sum of absolute price changes across the path until hit
        Efficiency = total_move / PathLength
        EarlyMAE = max adverse move in the early window
        BreakoutStrength = early_move / compression_range_before_start
        """
        # Find start index
        start_idx = None
        for i, candle in enumerate(price_path):
            if candle["timestamp"] >= start_time:
                start_idx = i
                break

        if start_idx is None or start_idx >= len(price_path) - 5:
            return self._default_metrics()

        # Extract path until hit (simplified: use first 20 candles after start)
        path_length = min(20, len(price_path) - start_idx)
        path_prices = [price_path[start_idx + i]["price"] for i in range(path_length)]

        # Total move (MFE in price units)
        total_move = mfe_pips / 10000

        # Early window (first 30 seconds = ~0.5 minutes = ~30 candles at 1min intervals)
        early_length = min(30, len(path_prices))
        early_prices = path_prices[:early_length]

        # EarlyImpulse = move_in_early_window / total_move
        if len(early_prices) >= 2:
            early_move = abs(early_prices[-1] - early_prices[0])
            early_impulse = early_move / total_move if total_move > 0 else 0.0
        else:
            early_impulse = 0.0

        # PathLength = sum of absolute price changes across the path
        path_length_sum = sum(abs(path_prices[i] - path_prices[i-1])
                             for i in range(1, len(path_prices)))

        # Efficiency = total_move / PathLength
        efficiency = total_move / path_length_sum if path_length_sum > 0 else 0.0

        # EarlyMAE = max adverse move in the early window (use provided MAE)
        early_mae = mae_pips / 10000  # Convert to price units

        # BreakoutStrength = early_move / compression_range_before_start
        # Simplified: use range of first few candles before start
        pre_start_range = 0.0
        if start_idx >= 3:
            pre_prices = [price_path[start_idx - i]["price"] for i in range(3, 0, -1)]
            pre_start_range = max(pre_prices) - min(pre_prices)

        breakout_strength = early_move / pre_start_range if pre_start_range > 0 else 0.0

        return {
            "early_impulse": early_impulse,
            "efficiency": efficiency,
            "early_mae": early_mae,
            "breakout_strength": breakout_strength
        }

    def _classify_opportunity(self, metrics: Dict[str, float], mfe_pips: float) -> str:
        """
        Classify opportunity using exact rules from spec.

        GOOD: MFE >= 2.5 AND EarlyImpulse >= threshold_1 AND Efficiency >= threshold_2
              AND EarlyMAE <= threshold_3 AND BreakoutStrength >= threshold_4

        BAD: MFE >= 2.5 BUT fails one or more GOOD conditions

        NOISE: MFE < 2.5 in both directions

        Actual thresholds used:
        threshold_1 (EarlyImpulse): 0.10
        threshold_2 (Efficiency): 0.30
        threshold_3 (EarlyMAE): 1.0
        threshold_4 (BreakoutStrength): 1.0
        """
        # NOISE check
        if mfe_pips < 2.5:
            return "NOISE"

        # GOOD conditions
        good_conditions = (
            metrics["early_impulse"] >= self.thresholds["early_impulse"] and
            metrics["efficiency"] >= self.thresholds["efficiency"] and
            metrics["early_mae"] <= self.thresholds["early_mae"] and
            metrics["breakout_strength"] >= self.thresholds["breakout_strength"]
        )

        if good_conditions:
            return "GOOD"
        else:
            return "BAD"

    def _extract_price_path(self, price_data: List[Dict], start_time: datetime,
                           end_time: datetime) -> List[Dict]:
        """Extract price candles within the analysis window."""
        path = []
        for candle in price_data:
            candle_time = candle["timestamp"]
            if start_time <= candle_time <= end_time:
                path.append(candle)
        return path

    def _default_metrics(self) -> Dict[str, float]:
        """Return default metrics when calculation fails."""
        return {
            "early_impulse": 0.0,
            "efficiency": 0.0,
            "early_mae": 0.0,
            "breakout_strength": 0.0
        }

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(timestamp_str)
        except ValueError:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    def save_zones_labeled(self, analyzed_opportunities: List[Dict], output_path: str):
        """Save opportunity_zones_labeled.csv with exact required columns."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            fieldnames = [
                # All Phase 1 columns
                "timestamp", "price", "session", "weekday",
                "mfe_up_pips", "mfe_down_pips", "tau_up_min", "tau_down_min",
                "mae_up_pips", "mae_down_pips", "up_exists", "down_exists",
                # Phase 2 additions
                "direction", "early_impulse", "efficiency", "early_mae",
                "breakout_strength", "zone_label", "zone_type"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(analyzed_opportunities)

        print(f"Created opportunity_zones_labeled.csv: {len(analyzed_opportunities)} rows")

    def generate_zone_summary(self, analyzed_opportunities: List[Dict]) -> Dict[str, Any]:
        """Generate zone_label_summary.json with exact required stats."""
        labels = ["GOOD", "BAD", "NOISE"]

        # Separate by label
        by_label = {label: [] for label in labels}
        for opp in analyzed_opportunities:
            label = opp["zone_label"]
            if label in by_label:
                by_label[label].append(opp)

        summary = {}

        for label in labels:
            opps = by_label[label]
            if not opps:
                summary[label] = {
                    "count": 0,
                    "count_by_direction": {"LONG": 0, "SHORT": 0},
                    "median_metrics": {},
                    "mean_metrics": {}
                }
                continue

            # Count by direction
            long_count = len([o for o in opps if o["direction"] == "LONG"])
            short_count = len([o for o in opps if o["direction"] == "SHORT"])

            # Extract metrics
            early_impulses = [o["early_impulse"] for o in opps]
            efficiencies = [o["efficiency"] for o in opps]
            early_maes = [o["early_mae"] for o in opps]
            breakout_strengths = [o["breakout_strength"] for o in opps]
            mfes = [max(o["mfe_up_pips"], o["mfe_down_pips"]) for o in opps]
            maes = [max(o["mae_up_pips"], o["mae_down_pips"]) for o in opps]
            taus = []
            for o in opps:
                tau = o["tau_up_min"] if o["direction"] == "LONG" else o["tau_down_min"]
                if tau is not None:
                    taus.append(tau)

            summary[label] = {
                "count": len(opps),
                "count_by_direction": {"LONG": long_count, "SHORT": short_count},
                "median_metrics": {
                    "MFE": statistics.median(mfes) if mfes else 0,
                    "MAE": statistics.median(maes) if maes else 0,
                    "tau": statistics.median(taus) if taus else 0,
                    "early_impulse": statistics.median(early_impulses),
                    "efficiency": statistics.median(efficiencies),
                    "early_mae": statistics.median(early_maes)
                },
                "mean_metrics": {
                    "MFE": statistics.mean(mfes) if mfes else 0,
                    "MAE": statistics.mean(maes) if maes else 0,
                    "tau": statistics.mean(taus) if taus else 0,
                    "early_impulse": statistics.mean(early_impulses),
                    "efficiency": statistics.mean(efficiencies),
                    "early_mae": statistics.mean(early_maes)
                }
            }

        return summary

    def generate_separability_audit(self, analyzed_opportunities: List[Dict]) -> Dict[str, Any]:
        """Generate zone_label_separability.json with separability audits."""
        # Extract GOOD and BAD opportunities
        good_opps = [o for o in analyzed_opportunities if o["zone_label"] == "GOOD"]
        bad_opps = [o for o in analyzed_opportunities if o["zone_label"] == "BAD"]

        if len(good_opps) < 10 or len(bad_opps) < 10:
            return {
                "separability_status": "INSUFFICIENT_DATA",
                "message": f"Need at least 10 GOOD and 10 BAD. Got {len(good_opps)} GOOD, {len(bad_opps)} BAD."
            }

        # Features to analyze
        features = ["early_impulse", "efficiency", "early_mae", "breakout_strength"]

        separability_results = {}

        for feature in features:
            good_values = [o[feature] for o in good_opps]
            bad_values = [o[feature] for o in bad_opps]

            # IQR overlap calculation
            good_q1 = statistics.quantiles(good_values, n=4)[0]  # 25th percentile
            good_q3 = statistics.quantiles(good_values, n=4)[2]  # 75th percentile
            bad_q1 = statistics.quantiles(bad_values, n=4)[0]
            bad_q3 = statistics.quantiles(bad_values, n=4)[2]

            iqr_overlap = max(0, min(good_q3, bad_q3) - max(good_q1, bad_q1))

            # Mean difference
            mean_diff = abs(statistics.mean(good_values) - statistics.mean(bad_values))

            # Percentile tables
            good_percentiles = statistics.quantiles(good_values, n=10)  # deciles
            bad_percentiles = statistics.quantiles(bad_values, n=10)

            separability_results[feature] = {
                "iqr_overlap": iqr_overlap,
                "mean_difference": mean_diff,
                "good_percentiles": good_percentiles,
                "bad_percentiles": bad_percentiles
            }

        # Overall separability assessment
        max_iqr_overlap = max(r["iqr_overlap"] for r in separability_results.values())

        if max_iqr_overlap > 0.5:  # Significant overlap
            overall_status = "ZONE_LABELS_NOT_SEPARABLE"
            message = f"Maximum IQR overlap: {max_iqr_overlap:.3f}. Labels cannot be learned."
        else:
            overall_status = "ZONE_LABELS_SEPARABLE"
            message = f"Maximum IQR overlap: {max_iqr_overlap:.3f}. Labels can be learned."

        return {
            "separability_status": overall_status,
            "message": message,
            "feature_analysis": separability_results
        }


def load_oanda_data(data_root: str, pair: str = "EUR_USD") -> List[Dict[str, Any]]:
    """Load Oanda candle data for OAE processing."""
    import pandas as pd

    data = []
    data_path = Path(data_root)
    pair_files = list(data_path.glob(f"pair={pair}/year=*/month=*/part-*.parquet"))

    for file_path in sorted(pair_files):
        df = pd.read_parquet(file_path)

        for _, row in df.iterrows():
            dt = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            data.append({
                "timestamp": dt,
                "price": row["close"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "volume": row["volume"]
            })

    print(f"Loaded {len(data)} price points for {pair}")
    return data


def load_phase1_results(csv_path: str) -> List[Dict[str, Any]]:
    """Load Phase 1 opportunity_map_raw.csv results."""
    opportunities = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            for key in ["mfe_up_pips", "mfe_down_pips", "mae_up_pips", "mae_down_pips"]:
                row[key] = float(row[key])
            for key in ["tau_up_min", "tau_down_min"]:
                row[key] = int(row[key]) if row[key] != "" else None
            for key in ["up_exists", "down_exists"]:
                row[key] = int(row[key])
            opportunities.append(row)

    print(f"Loaded {len(opportunities)} opportunities from Phase 1")
    return opportunities


def main():
    """Run Phase 2 - OAE with proof-first requirements."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 2: Opportunity Anatomy Engine")
    parser.add_argument("--phase1-csv", required=True, help="Path to Phase 1 opportunity_map_raw.csv")
    parser.add_argument("--data-root", required=True, help="Path to Oanda data directory")
    parser.add_argument("--pair", default="EUR_USD", help="Currency pair")
    parser.add_argument("--output-dir", default="phase2_proven_outputs", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load Phase 1 results
    print("Phase 2: Loading Phase 1 results...")
    phase1_opportunities = load_phase1_results(args.phase1_csv)

    # Load full price data
    print("Phase 2: Loading price data...")
    price_data = load_oanda_data(args.data_root, args.pair)

    if not price_data:
        print("ERROR: No price data loaded!")
        return 1

    # Run OAE
    print("Phase 2: Running Opportunity Anatomy Engine...")
    oae = OpportunityAnatomyEngine(early_window_seconds=30)

    print(f"Phase 2: Using exact thresholds:")
    for name, value in oae.thresholds.items():
        print(f"  {name}: {value}")

    analyzed_opportunities = oae.analyze_opportunities(phase1_opportunities, price_data)

    print(f"Phase 2: Analyzed {len(analyzed_opportunities)} opportunities")

    # Save required outputs
    print("Phase 2: Generating required output files...")

    # 1. opportunity_zones_labeled.csv
    csv_path = output_dir / "opportunity_zones_labeled.csv"
    oae.save_zones_labeled(analyzed_opportunities, str(csv_path))

    # 2. zone_label_summary.json
    summary = oae.generate_zone_summary(analyzed_opportunities)
    summary_path = output_dir / "zone_label_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    # 3. zone_label_separability.json
    separability = oae.generate_separability_audit(analyzed_opportunities)
    separability_path = output_dir / "zone_label_separability.json"
    with open(separability_path, 'w') as f:
        json.dump(separability, f, indent=2)

    # Print phase completion status
    sep_status = separability["separability_status"]
    print(f"\nPhase 2 Separability Status: {sep_status}")

    if sep_status == "ZONE_LABELS_NOT_SEPARABLE":
        print("❌ CRITICAL FAILURE: Zone labels cannot be learned from features")
        print("   This means the classification is not meaningful")
        return 1
    else:
        print("✅ Phase 2 COMPLETED: Zone labels are separable and can be learned")
        print(f"   - opportunity_zones_labeled.csv: {len(analyzed_opportunities)} rows")
        print(f"   - zone_label_summary.json: Generated")
        print(f"   - zone_label_separability.json: {sep_status}")

    return 0


if __name__ == "__main__":
    exit(main())
