#!/usr/bin/env python3
"""
Phase 3 - Entry Fitter: Compare Pre-Entry Conditions of GOOD vs BAD/NOISE

PURPOSE: Analyze market conditions immediately before opportunity discovery
to find patterns that predict GOOD opportunities vs BAD/NOISE traps.

APPROACH:
1. For each opportunity, extract pre-entry features from price data (30 min before start)
2. Compare feature distributions across GOOD/BAD/NOISE groups
3. Identify statistically significant differences
4. Optimize entry thresholds using these distinguishing features
5. Output optimized entry settings for capturing GOOD opportunities

PRE-ENTRY FEATURES:
- pre_volatility: Price range in previous 30 minutes (pips)
- pre_trend: Price direction in previous 30 minutes (pips)  
- pre_range_ratio: Pre-entry range vs opportunity target (2.5 pips)
- time_features: hour_of_day, weekday_encoded, session
- momentum_features: recent velocity, acceleration
"""

from __future__ import annotations
import json
import csv
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict
import statistics
import scipy.stats as stats


class EntryFitter:
    """
    Phase 3: Analyze pre-entry conditions to derive entry settings.
    """

    def __init__(self, pre_entry_window_minutes: int = 30):
        self.pre_entry_window_minutes = pre_entry_window_minutes  # Look back 30 minutes
        self.pip_multiplier = 10000  # EURUSD

    def load_price_data(self, data_root: str, pair: str = "EUR_USD") -> Dict[str, pd.DataFrame]:
        """
        Load OANDA price data organized by date for efficient pre-entry feature extraction.
        """
        print("Loading OANDA price data for pre-entry analysis...")

        data_by_date = {}
        data_path = Path(data_root)
        pair_files = list(data_path.glob(f"pair={pair}/year=*/month=*/part-*.parquet"))

        for file_path in sorted(pair_files):
            df = pd.read_parquet(file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Group by date for faster lookups
            for date, date_df in df.groupby(df['timestamp'].dt.date):
                if date not in data_by_date:
                    data_by_date[date] = []
                data_by_date[date].extend(date_df.to_dict('records'))

        print(f"Loaded price data for {len(data_by_date)} dates")
        return data_by_date

    def extract_pre_entry_features(self, opportunity: Dict, price_data: Dict[str, List[Dict]]) -> Dict[str, float]:
        """
        Extract pre-entry features from the 30 minutes before opportunity start.
        """
        start_time = datetime.fromisoformat(opportunity['timestamp_start'].replace('Z', '+00:00'))

        # Get price data for the relevant date
        date = start_time.date()
        if date not in price_data:
            return self._default_features()

        date_prices = sorted(price_data[date], key=lambda x: x['timestamp'])

        # Find pre-entry window: 30 minutes before start
        pre_start = start_time - timedelta(minutes=self.pre_entry_window_minutes)
        pre_prices = []

        for point in date_prices:
            point_time = point['timestamp']
            if pre_start <= point_time < start_time:
                pre_prices.append(point['close'])

        if len(pre_prices) < 5:  # Need minimum data
            return self._default_features()

        # Calculate features
        features = {}

        # Basic price metrics
        features['pre_start_price'] = pre_prices[0]
        features['pre_end_price'] = pre_prices[-1]
        features['pre_range_pips'] = (max(pre_prices) - min(pre_prices)) * self.pip_multiplier
        features['pre_trend_pips'] = (pre_prices[-1] - pre_prices[0]) * self.pip_multiplier

        # Volatility: average true range proxy
        if len(pre_prices) > 1:
            ranges = [abs(pre_prices[i] - pre_prices[i-1]) for i in range(1, len(pre_prices))]
            features['pre_volatility'] = statistics.mean(ranges) * self.pip_multiplier
        else:
            features['pre_volatility'] = 0.0

        # Range ratio: pre-entry range vs opportunity target
        features['pre_range_ratio'] = features['pre_range_pips'] / 2.5

        # Time features
        features['hour_of_day'] = start_time.hour
        features['weekday_encoded'] = start_time.weekday()  # 0=Monday, 6=Sunday
        features['is_london_session'] = 1 if 8 <= start_time.hour < 16 else 0

        # Momentum features
        if len(pre_prices) >= 3:
            # Recent velocity (last 5 prices)
            recent_prices = pre_prices[-5:]
            if len(recent_prices) >= 2:
                velocities = [recent_prices[i] - recent_prices[i-1] for i in range(1, len(recent_prices))]
                features['recent_velocity'] = statistics.mean(velocities) * self.pip_multiplier
                if len(velocities) >= 2:
                    accelerations = [velocities[i] - velocities[i-1] for i in range(1, len(velocities))]
                    features['recent_acceleration'] = statistics.mean(accelerations) * self.pip_multiplier
                else:
                    features['recent_acceleration'] = 0.0
            else:
                features['recent_velocity'] = 0.0
                features['recent_acceleration'] = 0.0
        else:
            features['recent_velocity'] = 0.0
            features['recent_acceleration'] = 0.0

        return features

    def _default_features(self) -> Dict[str, float]:
        """Return default features when data is insufficient."""
        return {
            'pre_start_price': 0.0,
            'pre_end_price': 0.0,
            'pre_range_pips': 0.0,
            'pre_trend_pips': 0.0,
            'pre_volatility': 0.0,
            'pre_range_ratio': 0.0,
            'hour_of_day': 0,
            'weekday_encoded': 0,
            'is_london_session': 0,
            'recent_velocity': 0.0,
            'recent_acceleration': 0.0
        }

    def analyze_pre_entry_conditions(self, opportunities: List[Dict], price_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Analyze pre-entry conditions across GOOD/BAD/NOISE groups.
        """
        print(f"Analyzing pre-entry conditions for {len(opportunities)} opportunities...")

        # Group opportunities by label
        groups = {'GOOD': [], 'BAD': [], 'NOISE': []}
        for opp in opportunities:
            label = opp['label']
            if label in groups:
                groups[label].append(opp)

        print(f"GOOD: {len(groups['GOOD'])}, BAD: {len(groups['BAD'])}, NOISE: {len(groups['NOISE'])}")

        # Extract features for each group
        group_features = {}
        for label, opps in groups.items():
            features_list = []
            for opp in opps:
                features = self.extract_pre_entry_features(opp, price_data)
                features_list.append(features)
            group_features[label] = features_list

        # Perform statistical analysis
        analysis_results = self._statistical_analysis(group_features)

        return {
            'group_counts': {label: len(opps) for label, opps in groups.items()},
            'statistical_analysis': analysis_results,
            'group_features': group_features  # For optimization
        }

    def _statistical_analysis(self, group_features: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Perform statistical tests to find significant differences between groups.
        """
        feature_names = list(group_features['GOOD'][0].keys()) if group_features['GOOD'] else []

        results = {}

        for feature in feature_names:
            good_values = [f[feature] for f in group_features['GOOD']]
            bad_values = [f[feature] for f in group_features['BAD']]
            noise_values = [f[feature] for f in group_features['NOISE']]

            # Skip if insufficient data
            if len(good_values) < 3 or len(bad_values) < 3 or len(noise_values) < 3:
                continue

            # One-way ANOVA to test if groups have different means
            try:
                f_stat, p_value = stats.f_oneway(good_values, bad_values, noise_values)
                significant = p_value < 0.05

                # Effect size (eta squared)
                all_values = good_values + bad_values + noise_values
                ss_between = sum(len(group) * (statistics.mean(group) - statistics.mean(all_values))**2
                                for group in [good_values, bad_values, noise_values])
                ss_total = sum((x - statistics.mean(all_values))**2 for x in all_values)
                eta_squared = ss_between / ss_total if ss_total > 0 else 0

                results[feature] = {
                    'anova_f_stat': f_stat,
                    'anova_p_value': p_value,
                    'significant': significant,
                    'eta_squared': eta_squared,
                    'means': {
                        'GOOD': statistics.mean(good_values),
                        'BAD': statistics.mean(bad_values),
                        'NOISE': statistics.mean(noise_values)
                    },
                    'medians': {
                        'GOOD': statistics.median(good_values),
                        'BAD': statistics.median(bad_values),
                        'NOISE': statistics.median(noise_values)
                    }
                }
            except Exception as e:
                results[feature] = {'error': str(e)}

        return results

    def optimize_entry_thresholds(self, analysis_results: Dict, group_features: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Optimize entry thresholds based on statistically significant features.
        """
        print("Optimizing entry thresholds...")

        # Find significant features
        significant_features = [
            feature for feature, stats in analysis_results['statistical_analysis'].items()
            if isinstance(stats, dict) and stats.get('significant', False)
        ]

        print(f"Found {len(significant_features)} statistically significant features")

        # For each significant feature, find optimal threshold to maximize GOOD capture while minimizing BAD/NOISE
        optimized_thresholds = {}

        for feature in significant_features:
            threshold = self._optimize_single_feature(feature, group_features)
            if threshold:
                optimized_thresholds[feature] = threshold

        # Calculate overall entry score
        entry_score = self._calculate_entry_score(optimized_thresholds, group_features)

        return {
            'significant_features': significant_features,
            'optimized_thresholds': optimized_thresholds,
            'entry_score': entry_score,
            'recommendations': self._generate_recommendations(optimized_thresholds, analysis_results)
        }

    def _optimize_single_feature(self, feature: str, group_features: Dict[str, List[Dict]]) -> Optional[Dict]:
        """
        Find optimal threshold for a single feature to distinguish GOOD from BAD/NOISE.
        """
        good_values = [f[feature] for f in group_features['GOOD']]
        bad_values = [f[feature] for f in group_features['BAD']]
        noise_values = [f[feature] for f in group_features['NOISE']]

        if not good_values or not bad_values or not noise_values:
            return None

        # Combine BAD and NOISE as "avoid"
        avoid_values = bad_values + noise_values

        # Try different thresholds
        all_values = sorted(set(good_values + avoid_values))
        best_threshold = None
        best_score = 0

        for threshold in all_values:
            # Calculate capture rates
            good_captured = sum(1 for v in good_values if v >= threshold)
            avoid_captured = sum(1 for v in avoid_values if v >= threshold)

            if good_captured + avoid_captured == 0:
                continue

            # Score: prioritize GOOD capture, penalize BAD/NOISE capture
            score = (good_captured / len(good_values)) - (avoid_captured / len(avoid_values))

            if score > best_score:
                best_score = score
                best_threshold = threshold

        if best_threshold is not None:
            return {
                'threshold': best_threshold,
                'direction': 'above' if best_threshold > statistics.median(good_values) else 'below',
                'good_capture_rate': sum(1 for v in good_values if v >= best_threshold) / len(good_values),
                'avoid_trigger_rate': sum(1 for v in avoid_values if v >= best_threshold) / len(avoid_values),
                'score': best_score
            }

        return None

    def _calculate_entry_score(self, thresholds: Dict, group_features: Dict[str, List[Dict]]) -> Dict[str, float]:
        """
        Calculate overall entry performance score.
        """
        if not thresholds:
            return {'overall_score': 0.0}

        # Simulate entry decisions
        good_decisions = []
        bad_decisions = []
        noise_decisions = []

        for opp, features in zip(group_features['GOOD'], group_features['GOOD']):
            decision = self._simulate_entry_decision(features, thresholds)
            good_decisions.append(decision)

        for opp, features in zip(group_features['BAD'], group_features['BAD']):
            decision = self._simulate_entry_decision(features, thresholds)
            bad_decisions.append(decision)

        for opp, features in zip(group_features['NOISE'], group_features['NOISE']):
            decision = self._simulate_entry_decision(features, thresholds)
            noise_decisions.append(decision)

        good_capture = sum(good_decisions) / len(good_decisions) if good_decisions else 0
        bad_trigger = sum(bad_decisions) / len(bad_decisions) if bad_decisions else 0
        noise_trigger = sum(noise_decisions) / len(noise_decisions) if noise_decisions else 0

        # Overall score: high GOOD capture, low BAD/NOISE trigger
        overall_score = good_capture - bad_trigger - noise_trigger

        return {
            'good_capture_rate': good_capture,
            'bad_trigger_rate': bad_trigger,
            'noise_trigger_rate': noise_trigger,
            'overall_score': overall_score
        }

    def _simulate_entry_decision(self, features: Dict, thresholds: Dict) -> bool:
        """
        Simulate whether entry would trigger based on thresholds.
        """
        # For now, simple AND logic: all thresholds must be met
        for feature, threshold_data in thresholds.items():
            if feature not in features:
                continue

            value = features[feature]
            threshold = threshold_data['threshold']
            direction = threshold_data['direction']

            if direction == 'above' and value < threshold:
                return False
            elif direction == 'below' and value > threshold:
                return False

        return True

    def _generate_recommendations(self, thresholds: Dict, analysis_results: Dict) -> List[str]:
        """
        Generate human-readable recommendations based on the analysis.
        """
        recommendations = []

        if thresholds:
            recommendations.append(f"Found {len(thresholds)} optimized entry thresholds")
            recommendations.append("Entry logic: AND combination of all thresholds")

            for feature, data in thresholds.items():
                direction = "above" if data['direction'] == 'above' else "below"
                recommendations.append(f"{feature}: {direction} {data['threshold']:.3f}")
        else:
            recommendations.append("No significant features found - entry logic needs more data")

        return recommendations


def main():
    """Run Phase 3 - Entry Fitter."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 3: Entry Fitter")
    parser.add_argument("--opportunities-csv", required=True, help="Path to opportunity_ranked.csv")
    parser.add_argument("--data-root", required=True, help="Path to OANDA data directory")
    parser.add_argument("--pair", default="EUR_USD", help="Currency pair")
    parser.add_argument("--output-dir", default="phase3_entry_fit_outputs", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load opportunities
    print("Phase 3: Loading opportunity data...")
    opportunities = []
    with open(args.opportunities_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            opportunities.append(row)

    # Load price data
    fitter = EntryFitter(pre_entry_window_minutes=30)
    price_data = fitter.load_price_data(args.data_root, args.pair)

    # Analyze pre-entry conditions
    analysis = fitter.analyze_pre_entry_conditions(opportunities, price_data)

    # Optimize entry thresholds
    optimization = fitter.optimize_entry_thresholds(analysis, analysis['group_features'])

    # Combine results
    results = {
        'phase': 'entry_fitter',
        'timestamp': datetime.now().isoformat(),
        'input_files': {
            'opportunities_csv': args.opportunities_csv,
            'data_root': args.data_root,
            'pair': args.pair
        },
        'analysis_summary': {
            'total_opportunities': len(opportunities),
            'group_counts': analysis['group_counts']
        },
        'statistical_analysis': analysis['statistical_analysis'],
        'optimization_results': optimization
    }

    # Save results
    results_path = output_dir / "entry_fit_results.json"
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print("""
PHASE 3 RESULTS:""")
    print(f"Total opportunities analyzed: {len(opportunities)}")
    print(f"GOOD: {analysis['group_counts']['GOOD']}, BAD: {analysis['group_counts']['BAD']}, NOISE: {analysis['group_counts']['NOISE']}")
    print(f"Significant features found: {len(optimization['significant_features'])}")
    print(f"Optimized thresholds: {len(optimization['optimized_thresholds'])}")
    print(f"Entry score: {optimization['entry_score']['overall_score']:.3f}")

    print("""
✅ Phase 3 COMPLETED: Entry settings derived from pre-entry analysis""")
    print(f"   - entry_fit_results.json: Complete statistical analysis and optimized thresholds")
    print("   - READY: Entry logic can now capture GOOD opportunities")

    if optimization['entry_score']['overall_score'] > 0:
        print("   - SUCCESS: Positive entry score - thresholds effectively distinguish GOOD from BAD/NOISE")
    else:
        print("   - WARNING: Negative entry score - thresholds need refinement or more data")

    return 0


if __name__ == "__main__":
    exit(main())
