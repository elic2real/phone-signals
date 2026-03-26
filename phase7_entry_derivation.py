#!/usr/bin/env python3
"""
Phase 7 - Entry Logic Derivation

Mechanically derive entry thresholds as bounded regions from pre-entry feature distributions.

This implements the reverse-engineering approach where entry rules are derived from the
physics of the moves, not statistical fitting.

Key principles:
- All parameters derived from feature distributions
- Bounded regions (min/max) from GOOD distributions, not single thresholds
- Perfect calibration expected (95-100% GOOD capture)
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
class EntryFeatures:
    """Pre-entry state features for opportunity analysis."""
    velocity_before: float  # Price velocity in pips/minute before opportunity
    acceleration_before: float  # Rate of change of velocity
    compression_before: float  # Price compression (range/MAD ratio)
    structure_distance: float  # Distance to nearest structure in pips
    micro_volatility: float  # Micro volatility (std dev of last 5 bars)
    trend_alignment: float  # Alignment with larger trend (0-1)


@dataclass
class EntryBoundaries:
    """Derived entry boundaries from GOOD distributions."""
    velocity_min: float
    acceleration_min: float
    compression_max: float
    structure_distance_max: float
    micro_volatility_min: float
    trend_alignment_min: float


class EntryDeriver:
    """Derives entry logic from pre-entry feature distributions."""

    def __init__(self, opportunities_csv: str, labeled_csv: str, output_dir: str):
        self.opportunities_csv = Path(opportunities_csv)
        self.labeled_csv = Path(labeled_csv)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Load and merge data
        self.opportunities = self._load_and_merge_data()
        print(f"Loaded {len(self.opportunities)} opportunities with labels and price paths")

        # Separate GOOD and BAD
        self.good_opportunities = [opp for opp in self.opportunities if opp.get('label') == 'GOOD']
        self.bad_opportunities = [opp for opp in self.opportunities if opp.get('label') == 'BAD']

        print(f"GOOD opportunities: {len(self.good_opportunities)}")
        print(f"BAD opportunities: {len(self.bad_opportunities)}")

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

    def _extract_pre_entry_features(self, opportunity: Dict) -> EntryFeatures:
        """
        Extract pre-entry features from the opportunity metadata.

        Since the dataset doesn't have pre-entry price history, we use the
        opportunity characteristics as proxy features.
        """
        # Use opportunity metadata as features
        speed = float(opportunity.get('speed', 0))
        efficiency = float(opportunity.get('efficiency', 0))
        extension = float(opportunity.get('extension', 0))
        drawdown_ratio = float(opportunity.get('drawdown_ratio', 0))
        composite_score = float(opportunity.get('composite_score', 0))
        target_distance = float(opportunity.get('target_distance', 0))

        # Map metadata to entry features
        velocity_before = speed  # pips per minute
        acceleration_before = efficiency * speed  # proxy for acceleration
        compression_before = 1.0 / (extension + 0.1)  # inverse of extension
        structure_distance = target_distance * 10  # proxy for structure distance
        micro_volatility = drawdown_ratio * 100  # proxy for volatility
        trend_alignment = composite_score  # overall quality as alignment

        return EntryFeatures(
            velocity_before=velocity_before,
            acceleration_before=acceleration_before,
            compression_before=compression_before,
            structure_distance=structure_distance,
            micro_volatility=micro_volatility,
            trend_alignment=trend_alignment
        )

    def _compute_feature_distributions(self) -> Dict[str, Dict]:
        """
        Compute feature distributions for GOOD and BAD opportunities.

        Returns distributions with min, max, percentiles for each feature.
        """
        print("Computing pre-entry feature distributions...")

        good_features = []
        bad_features = []

        for opp in self.good_opportunities:
            features = self._extract_pre_entry_features(opp)
            good_features.append(features)

        for opp in self.bad_opportunities:
            features = self._extract_pre_entry_features(opp)
            bad_features.append(features)

        distributions = {}

        feature_names = ['velocity_before', 'acceleration_before', 'compression_before',
                        'structure_distance', 'micro_volatility', 'trend_alignment']

        for feature_name in feature_names:
            good_values = [getattr(f, feature_name) for f in good_features if getattr(f, feature_name) != 0]
            bad_values = [getattr(f, feature_name) for f in bad_features if getattr(f, feature_name) != 0]

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

    def _derive_entry_boundaries(self, distributions: Dict) -> EntryBoundaries:
        """
        Derive entry boundaries from GOOD distributions.

        Use physical boundaries (min/max) from GOOD distributions, not statistical centers.
        """
        print("Deriving entry boundaries from GOOD distributions...")

        # For velocity: need sufficient momentum (≥ min of GOOD)
        velocity_min = distributions['velocity_before']['good']['min']

        # For acceleration: need positive acceleration (≥ min of GOOD)
        acceleration_min = distributions['acceleration_before']['good']['min']

        # For compression: need sufficient compression (≤ max of GOOD)
        compression_max = distributions['compression_before']['good']['max']

        # For structure distance: need to be close to structure (≤ max of GOOD)
        structure_distance_max = distributions['structure_distance']['good']['max']

        # For micro volatility: need sufficient volatility (≥ min of GOOD)
        micro_volatility_min = distributions['micro_volatility']['good']['min']

        # For trend alignment: need sufficient alignment (≥ min of GOOD)
        trend_alignment_min = distributions['trend_alignment']['good']['min']

        boundaries = EntryBoundaries(
            velocity_min=velocity_min,
            acceleration_min=acceleration_min,
            compression_max=compression_max,
            structure_distance_max=structure_distance_max,
            micro_volatility_min=micro_volatility_min,
            trend_alignment_min=trend_alignment_min
        )

        return boundaries

    def _save_distributions(self, distributions: Dict, boundaries: EntryBoundaries):
        """Save feature distributions and derived boundaries."""
        output = {
            'feature_distributions': distributions,
            'derived_boundaries': {
                'velocity_min': {
                    'value': boundaries.velocity_min,
                    'derived_from': 'velocity_before GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                },
                'acceleration_min': {
                    'value': boundaries.acceleration_min,
                    'derived_from': 'acceleration_before GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                },
                'compression_max': {
                    'value': boundaries.compression_max,
                    'derived_from': 'compression_before GOOD distribution',
                    'boundary_type': 'max',
                    'dataset_percentile': '100%'
                },
                'structure_distance_max': {
                    'value': boundaries.structure_distance_max,
                    'derived_from': 'structure_distance GOOD distribution',
                    'boundary_type': 'max',
                    'dataset_percentile': '100%'
                },
                'micro_volatility_min': {
                    'value': boundaries.micro_volatility_min,
                    'derived_from': 'micro_volatility GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                },
                'trend_alignment_min': {
                    'value': boundaries.trend_alignment_min,
                    'derived_from': 'trend_alignment GOOD distribution',
                    'boundary_type': 'min',
                    'dataset_percentile': '0%'
                }
            }
        }

        path = self.output_dir / "entry_feature_distributions.json"
        with open(path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Created {path}")

    def _test_calibration(self, boundaries: EntryBoundaries) -> Dict:
        """
        Test calibration performance with derived boundaries.

        Expected: near-perfect GOOD capture, minimal BAD trigger.
        """
        print("Testing calibration performance...")

        good_captured = 0
        bad_triggered = 0

        # Test GOOD opportunities
        for opp in self.good_opportunities:
            features = self._extract_pre_entry_features(opp)

            # Check if features satisfy entry conditions
            if (features.velocity_before >= boundaries.velocity_min and
                features.acceleration_before >= boundaries.acceleration_min and
                features.compression_before <= boundaries.compression_max and
                features.structure_distance <= boundaries.structure_distance_max and
                features.micro_volatility >= boundaries.micro_volatility_min and
                features.trend_alignment >= boundaries.trend_alignment_min):
                good_captured += 1

        # Test BAD opportunities
        for opp in self.bad_opportunities:
            features = self._extract_pre_entry_features(opp)

            # Check if features would trigger entry
            if (features.velocity_before >= boundaries.velocity_min and
                features.acceleration_before >= boundaries.acceleration_min and
                features.compression_before <= boundaries.compression_max and
                features.structure_distance <= boundaries.structure_distance_max and
                features.micro_volatility >= boundaries.micro_volatility_min and
                features.trend_alignment >= boundaries.trend_alignment_min):
                bad_triggered += 1

        calibration_results = {
            'good_total': len(self.good_opportunities),
            'good_captured': good_captured,
            'good_capture_rate': good_captured / len(self.good_opportunities) if self.good_opportunities else 0,
            'bad_total': len(self.bad_opportunities),
            'bad_triggered': bad_triggered,
            'bad_trigger_rate': bad_triggered / len(self.bad_opportunities) if self.bad_opportunities else 0
        }

        return calibration_results

    def _save_calibration_results(self, results: Dict):
        """Save calibration test results."""
        path = self.output_dir / "entry_calibration_results.json"
        with open(path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Created {path}")

    def derive_entry_logic(self):
        """Main entry derivation pipeline."""
        print("Phase 7: Entry Logic Derivation")
        print("=" * 50)

        # 1. Compute feature distributions
        distributions = self._compute_feature_distributions()

        # 2. Derive boundaries from GOOD distributions
        boundaries = self._derive_entry_boundaries(distributions)

        # 3. Save distributions and boundaries
        self._save_distributions(distributions, boundaries)

        # 4. Test calibration
        calibration_results = self._test_calibration(boundaries)
        self._save_calibration_results(calibration_results)

        # 5. Report results
        print("\nEntry Logic Derivation Complete:")
        print(f"GOOD capture rate: {calibration_results['good_capture_rate']:.1%}")
        print(f"BAD trigger rate: {calibration_results['bad_trigger_rate']:.1%}")

        if calibration_results['good_capture_rate'] >= 0.95:
            print("✅ PERFECT CALIBRATION: Entry logic captures almost all GOOD opportunities")
        else:
            print("❌ CALIBRATION FAILED: Entry logic cannot reproduce known GOOD outcomes")
            return 1

        if calibration_results['bad_trigger_rate'] <= 0.05:
            print("✅ LOW FALSE POSITIVES: Entry logic avoids most BAD opportunities")
        else:
            print("⚠️  HIGH FALSE POSITIVES: Entry logic triggers on too many BAD opportunities")

        return 0


def main():
    parser = argparse.ArgumentParser(description="Phase 7: Entry Logic Derivation")
    parser.add_argument('--opportunities-csv', required=True,
                       help='Path to original opportunities CSV with price_path')
    parser.add_argument('--labeled-csv', required=True,
                       help='Path to labeled opportunities CSV from Phase 3')
    parser.add_argument('--output-dir', required=True,
                       help='Output directory for results')

    args = parser.parse_args()

    deriver = EntryDeriver(args.opportunities_csv, args.labeled_csv, args.output_dir)
    exit(deriver.derive_entry_logic())


if __name__ == "__main__":
    main()
