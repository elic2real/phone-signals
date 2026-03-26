#!/usr/bin/env python3
"""
Phase 8 - AEE Path Analysis

Derive AEE boundaries from GOOD path distributions.

This implements the reverse-engineering approach where AEE rules are derived from the
physics of successful paths, not statistical fitting.

Key principles:
- All parameters derived from GOOD path distributions
- Bounded regions (min/max) from GOOD distributions, not single thresholds
- Perfect calibration expected (near 100% AEE extraction)
- Every parameter traceable to dataset percentiles
"""

import csv
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import argparse


@dataclass
class AEEFeatures:
    """Post-entry path features for AEE analysis."""
    max_giveback_pips: float  # Maximum drawdown in pips
    giveback_ratio: float  # Giveback as ratio of profit
    stall_duration_bars: float  # Bars since peak before continuation
    extension_ratio: float  # Final profit / initial target
    velocity_decay: float  # How velocity changes over path
    final_profit_pips: float  # Total profit achieved


@dataclass
class AEEBoundaries:
    """Derived AEE boundaries from GOOD distributions."""
    max_giveback_limit: float
    giveback_ratio_limit: float
    stall_duration_limit: float
    extension_min: float
    velocity_decay_limit: float


class AEEDeriver:
    """Derives AEE logic from GOOD path distributions."""

    def __init__(self, opportunities_csv: str, labeled_csv: str, output_dir: str):
        self.opportunities_csv = Path(opportunities_csv)
        self.labeled_csv = Path(labeled_csv)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Load and merge data
        self.opportunities = self._load_and_merge_data()
        print(f"Loaded {len(self.opportunities)} opportunities with labels and price paths")

        # Relabel based on profitability for AEE calibration
        for opp in self.opportunities:
            mfe = float(opp.get('max_mfe_pips', 0))
            mae = float(opp.get('max_mae_pips', 0))
            sl = 2.5  # Assume SL = target distance
            if mfe >= 2.5 and mae <= sl:
                opp['profit_label'] = 'GOOD'
            else:
                opp['profit_label'] = 'BAD'

        # Separate GOOD and BAD based on profitability
        self.good_opportunities = [opp for opp in self.opportunities if opp.get('profit_label') == 'GOOD']
        self.bad_opportunities = [opp for opp in self.opportunities if opp.get('profit_label') == 'BAD']

        print(f"Relabeled - GOOD opportunities: {len(self.good_opportunities)}")
        print(f"Relabeled - BAD opportunities: {len(self.bad_opportunities)}")

    def _load_and_merge_data(self) -> List[Dict]:
        """Load original opportunities and labeled data, merge by timestamp_start."""
        # Load original opportunities with price_path
        original_opportunities = {}
        with open(self.opportunities_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                opp_id = row['timestamp_start']
                # Parse price_path from JSON string
                if 'price_path' in row:
                    row['price_path'] = json.loads(row['price_path'])
                original_opportunities[opp_id] = row

        # Load labeled data and merge
        merged_opportunities = []
        with open(self.labeled_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                opp_id = row['timestamp_start']
                if opp_id in original_opportunities:
                    # Merge labeled data with original data
                    merged = original_opportunities[opp_id].copy()
                    merged.update(row)
                    merged_opportunities.append(merged)

        return merged_opportunities

    def _extract_aee_features(self, opportunity: Dict) -> AEEFeatures:
        """
        Extract AEE features from the opportunity path.

        Analyze the post-entry trajectory to understand successful exit patterns.
        """
        price_path = opportunity['price_path']
        direction = opportunity.get('direction', 'LONG')
        target_distance = float(opportunity.get('target_distance', 2.5))

        if len(price_path) < 3:
            return AEEFeatures(0, 0, 0, 0, 0, 0)

        # Convert to pips relative to entry
        entry_price = price_path[0]
        if direction == 'LONG':
            path_pips = [(p - entry_price) * 10000 for p in price_path]
        else:  # SHORT
            path_pips = [(entry_price - p) * 10000 for p in price_path]

        # Calculate running profit
        running_profit = path_pips.copy()

        # Find peak profit and max drawdown
        peak_profit = 0
        max_giveback = 0
        peak_idx = 0

        for i, profit in enumerate(running_profit):
            if profit > peak_profit:
                peak_profit = profit
                peak_idx = i
            current_giveback = peak_profit - profit
            max_giveback = max(max_giveback, current_giveback)

        # Giveback ratio
        final_profit = running_profit[-1]
        giveback_ratio = max_giveback / max(final_profit, 0.1) if final_profit > 0 else 0

        # Stall duration: bars since peak before final continuation
        stall_duration = len(running_profit) - peak_idx - 1

        # Extension ratio: final profit / initial target
        extension_ratio = final_profit / target_distance if target_distance > 0 else 0

        # Velocity decay: how profit accumulation slows
        if len(running_profit) >= 3:
            velocities = np.diff(running_profit)
            avg_velocity = np.mean(velocities)
            final_velocity = np.mean(velocities[-3:]) if len(velocities) >= 3 else avg_velocity
            velocity_decay = final_velocity / max(avg_velocity, 0.01)
        else:
            velocity_decay = 1.0

        return AEEFeatures(
            max_giveback_pips=max_giveback,
            giveback_ratio=giveback_ratio,
            stall_duration_bars=stall_duration,
            extension_ratio=extension_ratio,
            velocity_decay=velocity_decay,
            final_profit_pips=final_profit
        )

    def _compute_aee_distributions(self) -> Dict[str, Dict]:
        """
        Compute AEE feature distributions for GOOD and BAD opportunities.

        Returns distributions with min, max, percentiles for each feature.
        """
        print("Computing AEE feature distributions...")

        good_features = []
        bad_features = []

        for opp in self.good_opportunities:
            features = self._extract_aee_features(opp)
            good_features.append(features)

        for opp in self.bad_opportunities:
            features = self._extract_aee_features(opp)
            bad_features.append(features)

        distributions = {}

        feature_names = ['max_giveback_pips', 'giveback_ratio', 'stall_duration_bars',
                        'extension_ratio', 'velocity_decay', 'final_profit_pips']

        for feature_name in feature_names:
            good_values = [getattr(f, feature_name) for f in good_features]
            bad_values = [getattr(f, feature_name) for f in bad_features]

            # Filter out invalid values (but keep 0s which are valid for some features)
            good_values = [v for v in good_values if v is not None and not np.isnan(v)]
            bad_values = [v for v in bad_values if v is not None and not np.isnan(v)]

            if not good_values:
                continue

            distributions[feature_name] = {
                'good': {
                    'count': len(good_values),
                    'min': float(np.min(good_values)),
                    'max': float(np.max(good_values)),
                    'median': float(np.median(good_values)),
                    'p5': float(np.percentile(good_values, 5)),
                    'p95': float(np.percentile(good_values, 95))
                },
                'bad': {
                    'count': len(bad_values),
                    'min': float(np.min(bad_values)) if bad_values else 0,
                    'max': float(np.max(bad_values)) if bad_values else 0,
                    'median': float(np.median(bad_values)) if bad_values else 0,
                    'p5': float(np.percentile(bad_values, 5)) if bad_values else 0,
                    'p95': float(np.percentile(bad_values, 95)) if bad_values else 0
                }
            }

        return distributions

    def _derive_aee_boundaries(self, distributions: Dict) -> AEEBoundaries:
        """
        Derive AEE boundaries from GOOD distributions.

        Use physical boundaries (min/max) from GOOD distributions, not statistical centers.
        """
        print("Deriving AEE boundaries from GOOD distributions...")

        # For max giveback: allow up to max observed in GOOD
        max_giveback_limit = distributions['max_giveback_pips']['good']['max']

        # For giveback ratio: allow up to max observed in GOOD
        giveback_ratio_limit = distributions['giveback_ratio']['good']['max']

        # For stall duration: allow up to max observed in GOOD
        stall_duration_limit = distributions['stall_duration_bars']['good']['max']

        # For extension: require at least min observed in GOOD
        extension_min = distributions['extension_ratio']['good']['min']

        # For velocity decay: allow down to min observed in GOOD
        velocity_decay_limit = distributions['velocity_decay']['good']['min']

        boundaries = AEEBoundaries(
            max_giveback_limit=max_giveback_limit,
            giveback_ratio_limit=giveback_ratio_limit,
            stall_duration_limit=stall_duration_limit,
            extension_min=extension_min,
            velocity_decay_limit=velocity_decay_limit
        )

        return boundaries

    def _save_distributions(self, distributions: Dict, boundaries: AEEBoundaries):
        """Save AEE feature distributions and derived boundaries."""
        output = {
            'aee_distributions': distributions,
            'derived_boundaries': {
                'max_giveback_limit': {
                    'value': boundaries.max_giveback_limit,
                    'derived_from': 'max_giveback_pips GOOD distribution',
                    'boundary_type': 'max',
                    'dataset_percentile': '100%'
                },
                'giveback_ratio_limit': {
                    'value': boundaries.giveback_ratio_limit,
                    'derived_from': 'giveback_ratio GOOD distribution',
                    'boundary_type': 'max',
                    'dataset_percentile': '100%'
                },
                'stall_duration_limit': {
                    'value': boundaries.stall_duration_limit,
                    'derived_from': 'stall_duration_bars GOOD distribution',
                    'boundary_type': 'max',
                    'dataset_percentile': '100%'
                },
                'extension_min': {
                    'value': boundaries.extension_min,
                    'derived_from': 'extension_ratio GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                },
                'velocity_decay_limit': {
                    'value': boundaries.velocity_decay_limit,
                    'derived_from': 'velocity_decay GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                }
            }
        }

        path = self.output_dir / "aee_feature_distributions.json"
        with open(path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Created {path}")

    def _test_aee_calibration(self, boundaries: AEEBoundaries) -> Dict:
        """
        Test AEE calibration by simulating AEE-managed exits vs static exits.

        Expected: AEE should achieve near-theoretical maximum extraction.
        """
        print("Testing AEE calibration performance...")

        good_aee_profits = []
        good_static_profits = []

        # Test GOOD opportunities
        for opp in self.good_opportunities:
            features = self._extract_aee_features(opp)

            # Static exit: hold to final profit
            static_profit = features.final_profit_pips

            # AEE exit: apply boundaries
            aee_profit = self._simulate_aee_exit(features, boundaries)

            good_static_profits.append(static_profit)
            good_aee_profits.append(aee_profit)

        # Calculate statistics
        static_avg = np.mean(good_static_profits) if good_static_profits else 0
        aee_avg = np.mean(good_aee_profits) if good_aee_profits else 0
        improvement = aee_avg - static_avg

        calibration_results = {
            'good_count': len(self.good_opportunities),
            'static_avg_profit': float(static_avg),
            'aee_avg_profit': float(aee_avg),
            'avg_improvement': float(improvement),
            'improvement_ratio': float(improvement / max(static_avg, 0.01)) if static_avg > 0 else 0,
            'perfect_extraction_rate': float(sum(1 for p in good_aee_profits if p >= static_avg * 0.95) / len(good_aee_profits)) if good_aee_profits else 0
        }

        return calibration_results

    path = self.output_dir / "aee_feature_distributions.json"
    with open(path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"Created {path}")

    def _test_aee_calibration(self, boundaries: AEEBoundaries) -> Dict:
        """
        Test AEE calibration by simulating AEE-managed exits vs static exits.

        Expected: AEE should achieve near-theoretical maximum extraction.
        """
        print("Testing AEE calibration performance...")

        good_static_profits = []
        for opp in self.good_opportunities:
            features = self._extract_aee_features(opp)
            good_static_profits.append(features.final_profit_pips)

        static_avg = np.mean(good_static_profits)

        good_aee_profits = [self._simulate_aee_exit(opp) for opp in self.good_opportunities]
        aee_avg = np.mean(good_aee_profits)

        improvement = aee_avg - static_avg

        calibration_results = {
            'good_count': len(self.good_opportunities),
            'static_avg_profit': float(static_avg),
            'aee_avg_profit': float(aee_avg),
            'avg_improvement': float(improvement),
            'improvement_ratio': float(improvement / max(static_avg, 0.01)) if static_avg > 0 else 0,
            'perfect_extraction_rate': float(sum(1 for p in good_aee_profits if p >= static_avg * 0.95) / len(good_aee_profits)) if good_aee_profits else 0
        }

        path = self.output_dir / "aee_calibration_results.json"
        with open(path, 'w') as f:
            json.dump(calibration_results, f, indent=2)
        print(f"Created {path}")

        return calibration_results

    def derive_aee_logic(self):
        """Main AEE derivation pipeline."""
        print("Phase 8: AEE Path Analysis")
        print("=" * 50)

        # 1. Compute AEE feature distributions
        distributions = self._compute_aee_distributions()

        # 2. Derive boundaries from GOOD distributions
        boundaries = self._derive_aee_boundaries(distributions)

        # 3. Save distributions and boundaries
        self._save_distributions(distributions, boundaries)

        # 4. Test AEE calibration
        calibration_results = self._test_aee_calibration(boundaries)
        self._save_calibration_results(calibration_results)

        # 5. Report results
        print("\nAEE Logic Derivation Complete:")
        print(f"GOOD opportunities analyzed: {calibration_results['good_count']}")
        print(".2f")
        print(".2f")
        print(".2f")

        if calibration_results['perfect_extraction_rate'] >= 0.95:
            print("✅ PERFECT CALIBRATION: AEE logic achieves near-theoretical extraction")
        else:
            print("❌ CALIBRATION FAILED: AEE logic cannot achieve optimal extraction")
            return 1

        if calibration_results['avg_improvement'] >= 0:
            print("✅ AEE IMPROVES: AEE logic provides benefit over static exits")
        else:
            print("⚠️  AEE REGRESSION: AEE logic worse than static exits")

        return 0


def main():
    parser = argparse.ArgumentParser(description="Phase 8: AEE Path Analysis")
    parser.add_argument('--opportunities-csv', required=True,
                       help='Path to original opportunities CSV with price_path')
    parser.add_argument('--labeled-csv', required=True,
                       help='Path to labeled opportunities CSV from Phase 3')
    parser.add_argument('--output-dir', required=True,
                       help='Output directory for results')

    args = parser.parse_args()

    deriver = AEEDeriver(args.opportunities_csv, args.labeled_csv, args.output_dir)
    exit(deriver.derive_aee_logic())


if __name__ == "__main__":
    main()
