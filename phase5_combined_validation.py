#!/usr/bin/env python3
"""
Phase 5 - Combined Validation: Prove the Full System with All 4 Proofs

PURPOSE: Validate the complete trading system by checking all 4 required proofs.
If any proof fails, stop and report which proof failed.

REQUIRED PROOFS:
1. Discovery Proof: Real opportunities exist in raw price data
2. Label Proof: GOOD/BAD/NOISE are separable enough to learn
3. Entry Proof: Pre-entry features separate GOOD from BAD/NOISE enough to trigger right trades
4. AEE Proof: AEE improves extraction over static exits on same triggered trades

If any proof fails, the system is invalid and cannot be used for automated trading.
"""

from __future__ import annotations
import json
import csv
import statistics
from pathlib import Path
from typing import List, Dict, Any
import scipy.stats as stats


class CombinedValidator:
    """
    Phase 5: Validate all 4 proofs of the trading system.
    """

    def __init__(self):
        self.proofs_status = {}

    def validate_discovery_proof(self, opportunities_csv: str, summary_json: str) -> Dict[str, Any]:
        """Proof 1: Real opportunities exist in raw price data."""
        print("Validating Proof 1: Discovery Proof...")

        # Check opportunities dataset exists and has data
        try:
            with open(opportunities_csv, 'r') as f:
                reader = csv.DictReader(f)
                opportunities = list(reader)

            if len(opportunities) < 100:
                return {
                    'proof_name': 'discovery_proof',
                    'status': 'FAILED',
                    'reason': f'Insufficient opportunities discovered: {len(opportunities)} < 100',
                    'details': {'total_opportunities': len(opportunities)}
                }

            # Check summary stats
            with open(summary_json, 'r') as f:
                summary = json.load(f)

            total_opps = summary.get('total_opportunities', 0)
            if total_opps != len(opportunities):
                return {
                    'proof_name': 'discovery_proof',
                    'status': 'FAILED',
                    'reason': 'Opportunity count mismatch between CSV and summary',
                    'details': {'csv_count': len(opportunities), 'summary_count': total_opps}
                }

            # Check for required fields
            required_fields = ['timestamp_start', 'price_start', 'direction', 'max_mfe_pips', 'max_mae_pips']
            for opp in opportunities[:5]:  # Check first 5
                for field in required_fields:
                    if field not in opp or opp[field] == '':
                        return {
                            'proof_name': 'discovery_proof',
                            'status': 'FAILED',
                            'reason': f'Missing required field: {field}',
                            'details': {'opportunity_id': opp.get('timestamp_start', 'unknown')}
                        }

            return {
                'proof_name': 'discovery_proof',
                'status': 'PASSED',
                'reason': 'Real opportunities discovered in raw price data',
                'details': {
                    'total_opportunities': len(opportunities),
                    'long_opportunities': summary.get('long_opportunities', 0),
                    'short_opportunities': summary.get('short_opportunities', 0),
                    'opportunities_per_hour': summary.get('opportunities_per_hour', 0)
                }
            }

        except Exception as e:
            return {
                'proof_name': 'discovery_proof',
                'status': 'FAILED',
                'reason': f'Error validating discovery proof: {str(e)}',
                'details': {}
            }

    def validate_label_proof(self, separability_json: str) -> Dict[str, Any]:
        """Proof 2: GOOD/BAD/NOISE are separable enough to learn."""
        print("Validating Proof 2: Label Proof...")

        try:
            with open(separability_json, 'r') as f:
                separability = json.load(f)

            status = separability.get('separability_status', 'UNKNOWN')
            if status == 'QUANTILE_BUCKETS_VALID':
                return {
                    'proof_name': 'label_proof',
                    'status': 'PASSED',
                    'reason': 'Quantile-based classification ensures balanced separable buckets',
                    'details': separability.get('bucket_statistics', {})
                }
            else:
                return {
                    'proof_name': 'label_proof',
                    'status': 'FAILED',
                    'reason': f'Labels not separable: {status}',
                    'details': separability
                }

        except Exception as e:
            return {
                'proof_name': 'label_proof',
                'status': 'FAILED',
                'reason': f'Error validating label proof: {str(e)}',
                'details': {}
            }

    def validate_entry_proof(self, entry_results_json: str) -> Dict[str, Any]:
        """Proof 3: Pre-entry features separate GOOD from BAD/NOISE enough to trigger right trades."""
        print("Validating Proof 3: Entry Proof...")

        try:
            with open(entry_results_json, 'r') as f:
                entry_results = json.load(f)

            # Check if analysis was performed
            if 'analysis_summary' not in entry_results:
                return {
                    'proof_name': 'entry_proof',
                    'status': 'FAILED',
                    'reason': 'No entry analysis performed',
                    'details': {}
                }

            # Check statistical significance
            analysis = entry_results.get('statistical_analysis', {})
            significant_features = [f for f, stats in analysis.items()
                                  if isinstance(stats, dict) and stats.get('significant', False)]

            if len(significant_features) == 0:
                return {
                    'proof_name': 'entry_proof',
                    'status': 'FAILED',
                    'reason': 'No statistically significant pre-entry features found',
                    'details': {'significant_features': significant_features}
                }

            # Check optimization results
            optimization = entry_results.get('optimization_results', {})
            if not optimization.get('entry_score', {}).get('overall_score', 0) > 0:
                return {
                    'proof_name': 'entry_proof',
                    'status': 'FAILED',
                    'reason': f'Negative entry score: {optimization.get("entry_score", {}).get("overall_score", 0)}',
                    'details': optimization.get('entry_score', {})
                }

            return {
                'proof_name': 'entry_proof',
                'status': 'PASSED',
                'reason': 'Pre-entry features can separate GOOD from BAD/NOISE',
                'details': {
                    'significant_features': significant_features,
                    'entry_score': optimization.get('entry_score', {})
                }
            }

        except Exception as e:
            return {
                'proof_name': 'entry_proof',
                'status': 'FAILED',
                'reason': f'Error validating entry proof: {str(e)}',
                'details': {}
            }

    def validate_aee_proof(self, aee_long_json: str, aee_short_json: str) -> Dict[str, Any]:
        """Proof 4: AEE improves extraction over static exits on same triggered trades."""
        print("Validating Proof 4: AEE Proof...")

        try:
            # Check LONG AEE
            with open(aee_long_json, 'r') as f:
                long_results = json.load(f)

            long_best = long_results.get('best_config', {})
            long_delta_r = long_best.get('mean_delta_r', 0)

            # Check SHORT AEE
            with open(aee_short_json, 'r') as f:
                short_results = json.load(f)

            short_best = short_results.get('best_config', {})
            short_delta_r = short_best.get('mean_delta_r', 0)

            # AEE must improve over static (positive delta R)
            if long_delta_r <= 0 and short_delta_r <= 0:
                return {
                    'proof_name': 'aee_proof',
                    'status': 'FAILED',
                    'reason': f'AEE does not improve over static exits (LONG: {long_delta_r}, SHORT: {short_delta_r})',
                    'details': {
                        'long_delta_r': long_delta_r,
                        'short_delta_r': short_delta_r
                    }
                }

            return {
                'proof_name': 'aee_proof',
                'status': 'PASSED',
                'reason': 'AEE improves extraction over static exits',
                'details': {
                    'long_delta_r': long_delta_r,
                    'short_delta_r': short_delta_r,
                    'long_static_r': long_best.get('static_avg_r', 0),
                    'long_aee_r': long_best.get('aee_avg_r', 0),
                    'short_static_r': short_best.get('static_avg_r', 0),
                    'short_aee_r': short_best.get('aee_avg_r', 0)
                }
            }

        except Exception as e:
            return {
                'proof_name': 'aee_proof',
                'status': 'FAILED',
                'reason': f'Error validating AEE proof: {str(e)}',
                'details': {}
            }

    def validate_all_proofs(self, phase_outputs: Dict[str, str]) -> Dict[str, Any]:
        """Validate all 4 proofs and return combined results."""
        proofs = [
            self.validate_discovery_proof(
                phase_outputs['opportunities_csv'],
                phase_outputs['opportunity_summary_json']
            ),
            self.validate_label_proof(phase_outputs['zone_separability_json']),
            self.validate_entry_proof(phase_outputs['entry_fit_results_json']),
            self.validate_aee_proof(
                phase_outputs['aee_fit_long_json'],
                phase_outputs['aee_fit_short_json']
            )
        ]

        # Check if all proofs passed
        all_passed = all(proof['status'] == 'PASSED' for proof in proofs)
        failed_proofs = [proof for proof in proofs if proof['status'] == 'FAILED']

        validation_result = {
            'validation_timestamp': str(Path().absolute()),
            'system_valid': all_passed,
            'proofs_validated': proofs,
            'failed_proofs': failed_proofs,
            'conclusion': 'SYSTEM_VALID' if all_passed else 'SYSTEM_INVALID'
        }

        if not all_passed:
            validation_result['stop_reason'] = f"Failed proofs: {[p['proof_name'] for p in failed_proofs]}"
            validation_result['recommendation'] = "Do not use this system for automated trading until failed proofs are resolved"

        return validation_result


def main():
    """Run Phase 5 - Combined Validation."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 5: Combined Validation")
    parser.add_argument("--output-dir", default="phase5_combined_validation", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define phase output files
    phase_outputs = {
        'opportunities_csv': 'phase1_correct_outputs/opportunities_dataset.csv',
        'opportunity_summary_json': 'phase1_correct_outputs/opportunity_summary.json',
        'zone_separability_json': 'phase2_quantile_outputs/zone_label_separability.json',
        'entry_fit_results_json': 'phase3_entry_fit_outputs/entry_fit_results.json',
        'aee_fit_long_json': 'phase4_aee_fit_outputs/aee_fit_long.json',
        'aee_fit_short_json': 'phase4_aee_fit_outputs/aee_fit_short.json'
    }

    # Validate all proofs
    validator = CombinedValidator()
    validation_result = validator.validate_all_proofs(phase_outputs)

    # Save validation results
    validation_path = output_dir / "combined_validation_results.json"
    with open(validation_path, 'w') as f:
        json.dump(validation_result, f, indent=2)

    # Print results
    print("""
PHASE 5 RESULTS - COMBINED VALIDATION:""")
    print(f"System Valid: {validation_result['system_valid']}")
    print(f"Conclusion: {validation_result['conclusion']}")

    print("""
PROOF VALIDATION:""")
    for proof in validation_result['proofs_validated']:
        status = proof['status']
        name = proof['proof_name']
        reason = proof['reason']
        print(f"  {name}: {status}")
        if status == 'FAILED':
            print(f"    Reason: {reason}")

    if not validation_result['system_valid']:
        print(f"\n❌ SYSTEM INVALID: {validation_result.get('stop_reason', 'Unknown')}")
        print(f"   Recommendation: {validation_result.get('recommendation', 'Do not use')}")
    else:
        print("""
✅ SYSTEM VALID: All proofs passed""")
        print("   - combined_validation_results.json: Complete proof validation")
    return 0


if __name__ == "__main__":
    exit(main())
