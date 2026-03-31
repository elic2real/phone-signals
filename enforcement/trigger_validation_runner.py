#!/usr/bin/env python3
"""
Enforcement: Integrated Trigger Validation Runner
===================================================
Runs full trigger validation pipeline including sibling distinctness gate.

Pipeline:
  1. Load trigger_truth.json
  2. Validate schema compliance
  3. Check trigger siblings for distinctness (prevent fake variants)
  4. Generate detailed report

Usage:
    python trigger_validation_runner.py \
      --trigger-dir PC2/discovery/stage_a \
      --schema-dir enforcement/schemas \
      --output-dir control/trigger_validation_reports
"""
from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


def run_trigger_validation(
    trigger_dir: Path,
    schema_dir: Path,
    output_dir: Path,
) -> Dict[str, Any]:
    """
    Run comprehensive trigger validation including distinctness gate.
    Returns overall result and writes detailed reports.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        from enforcement.trigger_validator import validate_triggers
    except ImportError:
        from trigger_validator import validate_triggers
    
    # Run validation
    result = validate_triggers(trigger_dir, schema_dir)
    
    # Build comprehensive report
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "validation_status": "PASS" if result.passed else "FAIL",
        "trigger_validation": {
            "status": "PASS" if result.passed else "FAIL",
            "total_triggers": result.total_triggers,
            "sibling_groups_count": len(result.sibling_groups),
            "fake_variant_groups_count": len(result.fake_variant_groups),
        },
        "sibling_distinctness": {
            "status": "DISTINCT" if len(result.fake_variant_groups) == 0 else "FAKE_VARIANTS_BLOCKED",
            "fake_groups": [g.distinctness_report() for g in result.fake_variant_groups],
        },
        "all_sibling_groups": [g.distinctness_report() for g in result.sibling_groups],
        "errors": result.errors,
    }
    
    # Write main report
    report_file = output_dir / "trigger_validation_report.json"
    with report_file.open("w") as f:
        json.dump(report, f, indent=2)
    print(f"Validation report: {report_file}")
    
    # Write distinctness detail
    distinctness_file = output_dir / "sibling_distinctness_report.json"
    with distinctness_file.open("w") as f:
        json.dump(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "sibling_groups": [g.distinctness_report() for g in result.sibling_groups],
                "pass_groups": [g.distinctness_report() for g in result.sibling_groups if g.is_distinct()],
                "fail_groups": [g.distinctness_report() for g in result.fake_variant_groups],
            },
            f,
            indent=2,
        )
    print(f"Distinctness report: {distinctness_file}")
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Run integrated trigger validation with sibling distinctness gate"
    )
    parser.add_argument(
        "--trigger-dir",
        type=Path,
        required=True,
        help="Directory containing trigger_truth.json",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        required=True,
        help="Directory containing schema files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory for validation reports",
    )
    
    args = parser.parse_args()
    
    report = run_trigger_validation(args.trigger_dir, args.schema_dir, args.output_dir)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"TRIGGER VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Status: {report['validation_status']}")
    print(f"Total triggers: {report['trigger_validation']['total_triggers']}")
    print(f"Sibling groups: {report['trigger_validation']['sibling_groups_count']}")
    print(f"Fake variant groups: {report['trigger_validation']['fake_variant_groups_count']}")
    print(f"Distinctness: {report['sibling_distinctness']['status']}")
    
    if report['errors']:
        print(f"\nErrors:")
        for error in report['errors']:
            print(f"  - {error}")
    
    if report['sibling_distinctness']['fake_groups']:
        print(f"\nFake variant groups (BLOCKED):")
        for group in report['sibling_distinctness']['fake_groups']:
            print(f"  - {group['sibling_key']}: {group['trigger_count']} triggers")
    
    sys.exit(0 if report['validation_status'] == "PASS" else 1)


if __name__ == "__main__":
    main()
