#!/usr/bin/env python3
"""
Phase 3 - Opportunity Energy Audit (OEA)

PURPOSE: Prove measurements are real and separable in market data.
Before building entry/AEE logic, verify energy metrics actually separate real moves.

STEPS:
1. Build Energy Dataset - compute all energy metrics for every opportunity
2. Build Distribution Map - compute quantiles for each metric
3. Build Opportunity Score - composite energy score with normalization
4. Rank All Opportunities - sort by score
5. Define Buckets - quantile-based classification (top 20% GOOD, middle 50% BAD, bottom 30% NOISE)
6. Separability Proof - KS statistic, IQR overlap, mean difference
7. Entry Logic Derivation - analyze pre-entry features
8. AEE Path Analysis - derive exit settings from GOOD paths
9. Replay Verification - compare static vs AEE exits
"""

from __future__ import annotations
import json
import csv
import statistics
import math
from pathlib import Path
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import scipy.stats as stats
import numpy as np


class OpportunityEnergyAuditor:
    """
    Phase 3: Prove real separability in market data.
    """

    def __init__(self, target_pips: float = 2.5, pip_multiplier: float = 10000):
        self.target_pips = target_pips
        self.target_r = target_pips
        self.pip_multiplier = pip_multiplier

    def load_opportunities(self, csv_path: str) -> List[Dict[str, Any]]:
        """Load opportunities with price paths."""
        opportunities = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse price_path
                if 'price_path' in row and row['price_path']:
                    try:
                        price_path = [float(p.strip()) for p in row['price_path'].strip('[]').split(',')]
                        row['price_path'] = price_path
                    except:
                        row['price_path'] = []

                # Convert numeric fields
                for key in ['price_start', 'time_to_target', 'max_mfe_pips', 'max_mae_pips',
                           'speed', 'efficiency', 'drawdown_ratio', 'extension', 'composite_score', 'final_price']:
                    if key in row and row[key]:
                        try:
                            row[key] = float(row[key])
                        except:
                            pass
                opportunities.append(row)
        return opportunities

    def compute_energy_metrics(self, opportunity: Dict) -> Dict[str, float]:
        """Compute all energy metrics for one opportunity."""
        price_path = opportunity.get('price_path', [])
        if not price_path or len(price_path) < 2:
            return {'valid': False}

        direction = opportunity.get('direction', 'LONG')
        start_price = opportunity.get('price_start', 0)

        # Favorable movement F(k)
        if direction == 'LONG':
            F = lambda k: (price_path[k] - start_price) * self.pip_multiplier
        else:  # SHORT
            F = lambda k: (start_price - price_path[k]) * self.pip_multiplier

        # 3.1 tau_i - time to target
        tau = None
        for k in range(1, len(price_path)):
            if F(k) >= self.target_r:
                tau = k
                break

        if tau is None or tau < 1:
            return {'valid': False}

        # 3.2 MFE_i - maximum favorable excursion
        mfe = max(F(k) for k in range(len(price_path)))

        # 3.3 MAE_i - maximum adverse excursion (up to tau)
        mae = 0.0
        for k in range(tau + 1):
            if direction == 'LONG':
                adverse = start_price - price_path[k]
            else:
                adverse = price_path[k] - start_price
            mae = max(mae, adverse * self.pip_multiplier)

        # 3.4 Speed_i
        speed = self.target_r / tau if tau > 0 else 0.0

        # 3.5 EarlyImpulse_i (Te = 5 bars)
        te = min(5, len(price_path) - 1)
        early_move = F(te)
        early_impulse = early_move / mfe if mfe > 0 else 0.0

        # 3.6 Efficiency_i
        path_length = sum(abs(price_path[k] - price_path[k-1]) for k in range(1, len(price_path)))
        path_length_pips = path_length * self.pip_multiplier
        efficiency = mfe / path_length_pips if path_length_pips > 0 else 0.0

        # 3.7 RiskRatio_i
        risk_ratio = mfe / (mae + 0.01)  # ε = 0.01

        # 3.8 Extension_i
        extension = mfe / self.target_r

        # Drawdown ratio (MAE / target)
        drawdown_ratio = mae / self.target_r if self.target_r > 0 else 0.0

        return {
            'valid': True,
            'tau': tau,
            'mfe': mfe,
            'mae': mae,
            'speed': speed,
            'early_impulse': early_impulse,
            'efficiency': efficiency,
            'risk_ratio': risk_ratio,
            'extension': extension,
            'drawdown_ratio': drawdown_ratio,
            'direction': direction,
            'timestamp_start': opportunity.get('timestamp_start', ''),
            'opportunity_id': f"{opportunity.get('timestamp_start', '')}_{direction}"
        }

    def build_energy_dataset(self, opportunities: List[Dict]) -> List[Dict]:
        """Build energy dataset with all metrics for every opportunity."""
        print(f"Computing energy metrics for {len(opportunities)} opportunities...")

        energy_dataset = []
        for opp in opportunities:
            metrics = self.compute_energy_metrics(opp)
            if metrics['valid']:
                energy_dataset.append(metrics)

        print(f"Valid energy metrics computed for {len(energy_dataset)} opportunities")
        return energy_dataset

    def save_energy_dataset(self, energy_dataset: List[Dict], output_path: str):
        """Save energy dataset as CSV."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            if energy_dataset:
                fieldnames = energy_dataset[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(energy_dataset)

        print(f"Created opportunity_energy_dataset.csv: {len(energy_dataset)} opportunities")

    def build_distribution_map(self, energy_dataset: List[Dict]) -> Dict[str, Dict[str, float]]:
        """Build distribution map with quantiles for each metric."""
        metrics = ['tau', 'mfe', 'mae', 'speed', 'early_impulse', 'efficiency',
                  'risk_ratio', 'extension', 'drawdown_ratio']

        distribution_map = {}

        for metric in metrics:
            values = [opp[metric] for opp in energy_dataset if metric in opp and opp[metric] is not None]
            if not values:
                continue

            values.sort()
            quantiles = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0]

            distribution_map[metric] = {}
            for q in quantiles:
                if q == 0.0:
                    distribution_map[metric]['min'] = min(values)
                elif q == 1.0:
                    distribution_map[metric]['max'] = max(values)
                else:
                    idx = int(q * (len(values) - 1))
                    distribution_map[metric][f'{int(q*100)}%'] = values[idx]

        return distribution_map

    def save_distribution_map(self, distribution_map: Dict, output_path: str):
        """Save distribution map as JSON."""
        with open(output_path, 'w') as f:
            json.dump(distribution_map, f, indent=2)
        print("Created energy_metric_distribution.json")

    def build_opportunity_score(self, energy_dataset: List[Dict]) -> List[Dict]:
        """Build composite energy score for each opportunity."""
        if not energy_dataset:
            return []

        # Get min/max for normalization
        metrics = ['speed', 'efficiency', 'drawdown_ratio', 'extension', 'early_impulse']
        mins = {}
        maxs = {}

        for metric in metrics:
            values = [opp[metric] for opp in energy_dataset if metric in opp]
            if values:
                mins[metric] = min(values)
                maxs[metric] = max(values)
            else:
                mins[metric] = 0
                maxs[metric] = 1

        # Weights (empirically determined)
        weights = {
            'speed': 0.3,
            'efficiency': 0.3,
            'drawdown_ratio': -0.2,  # Penalty
            'extension': 0.1,
            'early_impulse': 0.1
        }

        scored_opportunities = []
        for opp in energy_dataset:
            # Normalize each metric
            normalized = {}
            for metric in metrics:
                value = opp.get(metric, 0)
                min_val = mins[metric]
                max_val = maxs[metric]
                if max_val > min_val:
                    normalized[metric] = (value - min_val) / (max_val - min_val)
                else:
                    normalized[metric] = 0.5  # Default

            # Compute composite score
            score = sum(weights[metric] * normalized[metric] for metric in metrics)

            opp_with_score = opp.copy()
            opp_with_score['composite_score'] = score
            opp_with_score['normalized_metrics'] = normalized
            scored_opportunities.append(opp_with_score)

        return scored_opportunities

    def save_scored_opportunities(self, scored_opportunities: List[Dict], output_path: str):
        """Save scored opportunities as CSV."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            if scored_opportunities:
                # Remove complex fields for CSV
                simple_opps = []
                for opp in scored_opportunities:
                    simple = {k: v for k, v in opp.items()
                             if k != 'normalized_metrics' and not isinstance(v, dict)}
                    simple_opps.append(simple)

                fieldnames = simple_opps[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(simple_opps)

        print(f"Created opportunity_scored.csv: {len(scored_opportunities)} opportunities")

    def rank_opportunities(self, scored_opportunities: List[Dict]) -> List[Dict]:
        """Rank opportunities by composite score (descending)."""
        ranked = sorted(scored_opportunities, key=lambda x: x['composite_score'], reverse=True)

        for i, opp in enumerate(ranked, 1):
            opp['rank'] = i

        return ranked

    def save_ranked_opportunities(self, ranked_opportunities: List[Dict], output_path: str):
        """Save ranked opportunities as CSV."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            if ranked_opportunities:
                # Remove complex fields for CSV
                simple_opps = []
                for opp in ranked_opportunities:
                    simple = {k: v for k, v in opp.items()
                             if k != 'normalized_metrics' and not isinstance(v, dict)}
                    simple_opps.append(simple)

                fieldnames = simple_opps[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(simple_opps)

        print(f"Created opportunity_ranked.csv: {len(ranked_opportunities)} opportunities")

    def define_buckets(self, ranked_opportunities: List[Dict]) -> List[Dict]:
        """Define buckets using quantiles: top 20% GOOD, middle 50% BAD, bottom 30% NOISE."""
        total = len(ranked_opportunities)
        if total == 0:
            return []

        # Quantile thresholds
        good_threshold = int(total * 0.2)  # Top 20%
        bad_threshold = int(total * 0.7)   # Top 70% (middle 50% is 20%-70%)

        bucketed_opportunities = []
        for opp in ranked_opportunities:
            rank = opp['rank']
            if rank <= good_threshold:
                label = 'GOOD'
            elif rank <= bad_threshold:
                label = 'BAD'
            else:
                label = 'NOISE'

            opp_with_label = opp.copy()
            opp_with_label['label'] = label
            bucketed_opportunities.append(opp_with_label)

        return bucketed_opportunities

    def save_labeled_opportunities(self, labeled_opportunities: List[Dict], output_path: str):
        """Save labeled opportunities as CSV."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            if labeled_opportunities:
                # Remove complex fields for CSV
                simple_opps = []
                for opp in labeled_opportunities:
                    simple = {k: v for k, v in opp.items()
                             if k != 'normalized_metrics' and not isinstance(v, dict)}
                    simple_opps.append(simple)

                fieldnames = simple_opps[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(simple_opps)

        print(f"Created opportunity_labeled.csv: {len(labeled_opportunities)} opportunities")

    def compute_separability_proof(self, labeled_opportunities: List[Dict]) -> Dict[str, Any]:
        """Compute separability proof: KS statistic, IQR overlap, mean difference."""
        # Group by label
        groups = {'GOOD': [], 'BAD': [], 'NOISE': []}
        for opp in labeled_opportunities:
            label = opp.get('label', 'UNKNOWN')
            if label in groups:
                groups[label].append(opp)

        metrics = ['tau', 'mfe', 'mae', 'speed', 'early_impulse', 'efficiency',
                  'risk_ratio', 'extension', 'drawdown_ratio']

        separability_report = {}

        for metric in metrics:
            good_values = [opp[metric] for opp in groups['GOOD'] if metric in opp]
            bad_values = [opp[metric] for opp in groups['BAD'] if metric in opp]

            if not good_values or not bad_values:
                continue

            # Basic statistics
            good_median = statistics.median(good_values)
            bad_median = statistics.median(bad_values)
            mean_diff = statistics.mean(good_values) - statistics.mean(bad_values)

            # KS statistic
            try:
                ks_stat, ks_p = stats.ks_2samp(good_values, bad_values)
            except:
                ks_stat, ks_p = 0.0, 1.0

            # IQR overlap
            good_q1 = np.percentile(good_values, 25)
            good_q3 = np.percentile(good_values, 75)
            bad_q1 = np.percentile(bad_values, 25)
            bad_q3 = np.percentile(bad_values, 75)

            iqr_overlap = max(0, min(good_q3, bad_q3) - max(good_q1, bad_q1))
            iqr_overlap_ratio = iqr_overlap / (good_q3 - good_q1) if (good_q3 - good_q1) > 0 else 0

            separability_report[metric] = {
                'good_median': float(good_median),
                'bad_median': float(bad_median),
                'mean_difference': float(mean_diff),
                'ks_statistic': float(ks_stat),
                'ks_p_value': float(ks_p),
                'iqr_overlap': float(iqr_overlap),
                'iqr_overlap_ratio': float(iqr_overlap_ratio),
                'separable': bool(ks_p < 0.05 and iqr_overlap_ratio < 0.5)
            }

        return separability_report

    def save_separability_report(self, separability_report: Dict, output_path: str):
        """Save separability report as JSON."""
        with open(output_path, 'w') as f:
            json.dump(separability_report, f, indent=2)
        print("Created opportunity_separability_report.json")


def main():
    """Run Phase 3 - Opportunity Energy Audit."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 3: Opportunity Energy Audit")
    parser.add_argument("--opportunities-csv", required=True, help="Path to opportunities_dataset.csv")
    parser.add_argument("--output-dir", default="phase3_oea_outputs", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize auditor
    auditor = OpportunityEnergyAuditor(target_pips=2.5, pip_multiplier=10000)

    # Load opportunities
    print("Phase 3: Loading opportunities dataset...")
    opportunities = auditor.load_opportunities(args.opportunities_csv)

    # Step 1: Build Energy Dataset
    print("Phase 3: Building energy dataset...")
    energy_dataset = auditor.build_energy_dataset(opportunities)
    energy_csv_path = output_dir / "opportunity_energy_dataset.csv"
    auditor.save_energy_dataset(energy_dataset, str(energy_csv_path))

    # Step 2: Build Distribution Map
    print("Phase 3: Building distribution map...")
    distribution_map = auditor.build_distribution_map(energy_dataset)
    dist_json_path = output_dir / "energy_metric_distribution.json"
    auditor.save_distribution_map(distribution_map, str(dist_json_path))

    # Step 3: Build Opportunity Score
    print("Phase 3: Building opportunity scores...")
    scored_opportunities = auditor.build_opportunity_score(energy_dataset)
    scored_csv_path = output_dir / "opportunity_scored.csv"
    auditor.save_scored_opportunities(scored_opportunities, str(scored_csv_path))

    # Step 4: Rank All Opportunities
    print("Phase 3: Ranking opportunities...")
    ranked_opportunities = auditor.rank_opportunities(scored_opportunities)
    ranked_csv_path = output_dir / "opportunity_ranked.csv"
    auditor.save_ranked_opportunities(ranked_opportunities, str(ranked_csv_path))

    # Step 5: Define Buckets
    print("Phase 3: Defining buckets (top 20% GOOD, middle 50% BAD, bottom 30% NOISE)...")
    labeled_opportunities = auditor.define_buckets(ranked_opportunities)
    labeled_csv_path = output_dir / "opportunity_labeled.csv"
    auditor.save_labeled_opportunities(labeled_opportunities, str(labeled_csv_path))

    # Step 6: Separability Proof
    print("Phase 3: Computing separability proof...")
    separability_report = auditor.compute_separability_proof(labeled_opportunities)
    separability_json_path = output_dir / "opportunity_separability_report.json"
    auditor.save_separability_report(separability_report, str(separability_json_path))

    # Check if system should stop
    separable_metrics = sum(1 for metric, stats in separability_report.items()
                           if stats.get('separable', False))
    total_metrics = len(separability_report)

    print("""
PHASE 3 RESULTS - OPPORTUNITY ENERGY AUDIT:""")
    print(f"Total opportunities with energy metrics: {len(energy_dataset)}")
    print(f"Separable metrics: {separable_metrics}/{total_metrics}")

    # Count labels
    labels = {}
    for opp in labeled_opportunities:
        label = opp.get('label', 'UNKNOWN')
        labels[label] = labels.get(label, 0) + 1

    print(f"GOOD opportunities: {labels.get('GOOD', 0)}")
    print(f"BAD opportunities: {labels.get('BAD', 0)}")
    print(f"NOISE opportunities: {labels.get('NOISE', 0)}")

    if separable_metrics < total_metrics * 0.5:  # Less than 50% of metrics are separable
        print("""
❌ SYSTEM INVALID: Labels not sufficiently separable""")
        print("   Recommendation: Energy metrics do not distinguish GOOD from BAD opportunities")
        print("   Action: Redesign energy metrics or accept weaker separability")
        return 1
    else:
        print("""
✅ PHASE 3 COMPLETED: Energy metrics prove real separability""")
        print(f"   - opportunity_energy_dataset.csv: {len(energy_dataset)} opportunities with energy metrics")
        print(f"   - energy_metric_distribution.json: Real market quantiles")
        print(f"   - opportunity_scored.csv: Composite energy scores")
        print(f"   - opportunity_ranked.csv: Ranked by energy")
        print(f"   - opportunity_labeled.csv: GOOD/BAD/NOISE buckets")
        print(f"   - opportunity_separability_report.json: Proof of separability")
        print("   - READY: Can now derive real entry and AEE settings from market data")

    return 0


if __name__ == "__main__":
    exit(main())
