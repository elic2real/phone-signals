#!/usr/bin/env python3
"""
Batch Extension & Validation Runner

Takes existing Stage A artifacts and demonstrates batch scaling by:
1. Creating a batch manifest with planned new candidates
2. Synthesizing realistic batch artifacts (aligned with existing patterns)
3. Running full validation pipeline
4. Generating comparative reports

This is a DEMONSTRATION of the scaling pipeline.
For PRODUCTION, replace synthetic generation with real Phase 0-2 discovery.

Usage:
  python control/batch_validator_runner.py \
    --base-batch PC2/discovery/stage_a \
    --batch-name eurusd_london_short_extended \
    --output-dir control/batches/eurusd_london_short_extended \
    --extension-buckets 8 \
    --extension-samples 100
"""

from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
import copy


def load_artifact(path: Path) -> Dict[str, Any]:
    """Safe artifact loader with error handling."""
    if not path.exists():
        return {}
    try:
        with path.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load {path}: {e}")
        return {}


def create_batch_manifest(
    base_pair: str,
    base_session: str,
    base_direction: str,
    extension_buckets: List[int],
) -> Dict[str, Any]:
    """Create manifest documenting what the batch will contain."""
    return {
        "batch_type": "extension",
        "base_source": "stage_a",
        "pair": base_pair,
        "session": base_session,
        "direction": base_direction,
        "base_buckets": [2, 3, 5, 10],
        "extension_buckets": extension_buckets,
        "planned_outputs": [
            "business_viability_report.json",
            "path_family_report.json",
            "structure_truth.json",
            "setup_truth.json",
            "trigger_truth.json",
            "ceiling_report.json",
        ],
        "strategy": "Extend existing family with additional bucket coverage and sample depth",
    }


def synthesize_batch_artifacts(
    base_batch_dir: Path,
    output_dir: Path,
    pair: str,
    session: str,
    direction: str,
    extension_buckets: List[int],
) -> bool:
    """
    Synthesize batch artifacts by extending base artifacts.
    
    For DEMONSTRATION only. Production version would use real discovered candidates.
    """
    print(f"\nSynthesizing batch artifacts for {pair} {session} {direction}...")
    
    # Load base artifacts
    base_vr = load_artifact(base_batch_dir / "business_viability_report.json")
    base_sr = load_artifact(base_batch_dir / "setup_truth.json")
    base_tr = load_artifact(base_batch_dir / "trigger_truth.json")
    base_pfr = load_artifact(base_batch_dir / "path_family_report.json")
    base_str = load_artifact(base_batch_dir / "structure_truth.json")
    base_cr = load_artifact(base_batch_dir / "ceiling_report.json")
    
    if not base_vr or not base_sr or not base_tr:
        print("ERROR: Base artifacts missing or invalid")
        return False
    
    # Filter base artifacts to target (pair, session, direction)
    base_viability = [r for r in base_vr.get("records", [])
                      if r["pair"] == pair and r["direction"] == direction]
    base_setups = [s for s in base_sr.get("records", [])
                   if s["pair"] == pair and s["direction"] == direction]
    base_triggers = [t for t in base_tr.get("records", [])
                     if t["pair"] == pair and t["direction"] == direction]
    
    print(f"  Base artifacts: {len(base_viability)} viability, {len(base_setups)} setups, {len(base_triggers)} triggers")
    
    # Create extended artifacts
    # NOTE: This is SYNTHETIC for demo. Real discovery would compute these.
    
    extended_viability = copy.deepcopy(base_viability)
    for ext_bucket in extension_buckets:
        # Create synthetic viability record for extension bucket
        if not any(v["target_bucket_pips"] == ext_bucket for v in extended_viability):
            template = base_viability[0] if base_viability else {}
            new_record = copy.deepcopy(template)
            new_record["target_bucket_pips"] = ext_bucket
            # Adjust metrics slightly for realism
            if "avg_mfe_pips" in new_record and new_record["avg_mfe_pips"]:
                new_record["avg_mfe_pips"] *= (ext_bucket / base_viability[0].get("target_bucket_pips", 1))
            new_record["viable"] = True  # Assume viable for demo
            extended_viability.append(new_record)
    
    # Synthesize extended setups
    extended_setups = copy.deepcopy(base_setups)
    for ext_bucket in extension_buckets:
        if not any(s["target_bucket"] == ext_bucket for s in extended_setups):
            if base_setups:
                template_setup = base_setups[0]
                new_setup = copy.deepcopy(template_setup)
                new_setup["target_bucket"] = ext_bucket
                new_setup["setup_label"] = f"{pair}_{session}_{direction}_{ext_bucket}pip_{template_setup.get('path_family', 'sweep')}_{template_setup.get('structure_label', 'retest_level')}"
                extended_setups.append(new_setup)
    
    # Synthesize extended triggers
    extended_triggers = copy.deepcopy(base_triggers)
    for ext_bucket in extension_buckets:
        if not any(t["target_bucket"] == ext_bucket for t in extended_triggers):
            if base_triggers:
                template_trigger = base_triggers[0]
                new_trigger = copy.deepcopy(template_trigger)
                new_trigger["target_bucket"] = ext_bucket
                new_trigger["trigger_label"] = f"trigger::{pair}_{session}_{direction}_{ext_bucket}pip_{template_trigger.get('path_family', 'sweep')}_{template_trigger.get('structure_label', 'retest_level')}::REASSERTION"
                extended_triggers.append(new_trigger)
    
    # Write extended artifacts
    artifacts = [
           ("path_family_report.json", {
               "$artifact": "path_family_report",
               "produced_by": "SCALED_DISCOVERY_DEMO",
               "run_ts_utc": datetime.now(timezone.utc).isoformat(),
               "batch": f"{pair}_{session}_{direction}",
               "records": copy.deepcopy(base_pfr.get("records", [])) if base_pfr else [],
           }),
           ("structure_truth.json", {
               "$artifact": "structure_truth",
               "produced_by": "SCALED_DISCOVERY_DEMO",
               "run_ts_utc": datetime.now(timezone.utc).isoformat(),
               "batch": f"{pair}_{session}_{direction}",
               "records": copy.deepcopy(base_str.get("records", [])) if base_str else [],
           }),
           ("ceiling_report.json", {
               "$artifact": "ceiling_report",
               "produced_by": "SCALED_DISCOVERY_DEMO",
               "run_ts_utc": datetime.now(timezone.utc).isoformat(),
               "batch": f"{pair}_{session}_{direction}",
               "records": copy.deepcopy(base_cr.get("records", [])) if base_cr else [],
           }),
        ("business_viability_report.json", {
            "$artifact": "business_viability_report",
            "produced_by": "SCALED_DISCOVERY_DEMO",
            "run_ts_utc": datetime.now(timezone.utc).isoformat(),
            "batch": f"{pair}_{session}_{direction}",
            "summary": {
                "total_evaluated": len(extended_viability),
                "viable_count": sum(1 for r in extended_viability if r.get("viable")),
            },
            "records": extended_viability,
        }),
        ("setup_truth.json", {
            "$artifact": "setup_truth",
            "produced_by": "SCALED_DISCOVERY_DEMO",
            "run_ts_utc": datetime.now(timezone.utc).isoformat(),
            "batch": f"{pair}_{session}_{direction}",
            "summary": {
                "total_setups": len(extended_setups),
                "valid_setups": len([s for s in extended_setups if s.get("locked")]),
            },
            "records": extended_setups,
        }),
        ("trigger_truth.json", {
            "$artifact": "trigger_truth",
            "produced_by": "SCALED_DISCOVERY_DEMO",
            "run_ts_utc": datetime.now(timezone.utc).isoformat(),
            "batch": f"{pair}_{session}_{direction}",
            "summary": {
                "candidate_setups": len(extended_setups),
                "valid_triggers": len([t for t in extended_triggers if t.get("locked")]),
            },
            "records": extended_triggers,
        }),
    ]
    
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, data in artifacts:
        output_file = output_dir / filename
        with output_file.open("w") as f:
            json.dump(data, f, indent=2)
        print(f"  ✓ {filename}: {len(data.get('records', []))} records")
    
    return True


def run_batch_validation(
    batch_dir: Path,
    batch_name: str,
) -> Dict[str, Any]:
    """
    Run standard validation pipeline on batch artifacts.
    Returns summary of validation results.
    """
    print(f"\nValidating batch {batch_name}...")
    
    results = {
        "batch_name": batch_name,
        "batch_dir": str(batch_dir),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_status": "UNKNOWN",
        "details": {},
    }
    
    # Load artifacts
    setup_file = batch_dir / "setup_truth.json"
    trigger_file = batch_dir / "trigger_truth.json"
    
    if setup_file.exists():
        with setup_file.open() as f:
            setup_data = json.load(f)
        results["details"]["setup_count"] = len(setup_data.get("records", []))
    
    if trigger_file.exists():
        with trigger_file.open() as f:
            trigger_data = json.load(f)
        results["details"]["trigger_count"] = len(trigger_data.get("records", []))
        
        # Check sibling distinctness info
        siblings = {}
        for t in trigger_data.get("records", []):
            key = (t.get("pair"), t.get("direction"), t.get("structure_label"), t.get("path_family"))
            if key not in siblings:
                siblings[key] = []
            siblings[key].append(t.get("target_bucket"))
        results["details"]["sibling_groups"] = len(siblings)
        results["details"]["sibling_details"] = [
            {"family": f"{k[2]}_{k[3]}", "buckets": sorted(v)}
            for k, v in sorted(siblings.items())
        ]
    
    results["validation_status"] = "READY_FOR_FULL_PIPELINE"
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Batch extension and validation runner"
    )
    parser.add_argument(
        "--base-batch", type=Path, default=Path("PC2/discovery/stage_a"),
        help="Base batch directory to extend from"
    )
    parser.add_argument(
        "--batch-name", required=True,
        help="Name for this batch (e.g., eurusd_london_short_extended)"
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True,
        help="Output directory for batch artifacts"
    )
    parser.add_argument(
        "--pair", default="EUR_USD",
        help="Pair to focus on (e.g., EUR_USD)"
    )
    parser.add_argument(
        "--session", default="London",
        help="Session (e.g., London)"
    )
    parser.add_argument(
        "--direction", default="SHORT", choices=["LONG", "SHORT"],
        help="Direction"
    )
    parser.add_argument(
        "--extension-buckets", type=int, nargs="+", default=[8],
        help="Additional buckets to add (e.g., 8)"
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"BATCH VALIDATOR — {args.batch_name}")
    print(f"{'='*70}")
    
    # Create manifest
    manifest = create_batch_manifest(
        args.pair, args.session, args.direction, args.extension_buckets
    )
    
    # Synthesize artifacts
    success = synthesize_batch_artifacts(
        args.base_batch,
        args.output_dir,
        args.pair,
        args.session,
        args.direction,
        args.extension_buckets,
    )
    
    if not success:
        print("ERROR: Artifact synthesis failed")
        sys.exit(1)
    
    # Validate
    validation_results = run_batch_validation(args.output_dir, args.batch_name)
    
    # Write results
    results_file = args.output_dir / "batch_validation_summary.json"
    output_report = {
        "manifest": manifest,
        "validation": validation_results,
        "note": "This is a DEMONSTRATION batch. For production, replace synthetic generation with real Phase 0-2 discovery.",
    }
    
    with results_file.open("w") as f:
        json.dump(output_report, f, indent=2)
    
    print(f"\n{'='*70}")
    print(f"BATCH SUMMARY")
    print(f"{'='*70}")
    print(f"Batch: {args.batch_name}")
    print(f"Pair/Session/Direction: {args.pair}/{args.session}/{args.direction}")
    print(f"Extension buckets: {args.extension_buckets}")
    print(f"Setups: {validation_results['details'].get('setup_count', 0)}")
    print(f"Triggers: {validation_results['details'].get('trigger_count', 0)}")
    print(f"Sibling groups: {validation_results['details'].get('sibling_groups', 0)}")
    print(f"\nResults written to: {results_file}")


if __name__ == "__main__":
    main()
