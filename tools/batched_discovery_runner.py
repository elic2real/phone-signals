#!/usr/bin/env python3
"""
Batched Entry Discovery Runner — Scale Phase 1

Runs scaled discovery for specific (pair, session, direction, buckets) cohorts
while reusing cache layers, vectorization, and existing phases.

This runner orchestrates:
  1. Load environment and structure cache (once per pair/session)
  2. Run vectorized path extraction for specific direction/bucket combos
  3. Run Phase 0-2 (business viability → path family → structure)
  4. Run Phase 3 (setup discovery)
  5. Run Phase 4 (trigger discovery)
  6. Run Phase 6 (ceiling discovery)
  7. Run enforcement validation
  8. Run trigger distinctness validation

Usage:
  python batched_discovery_runner.py \
    --pair EUR_USD \
    --session London \
    --direction SHORT \
    --buckets 2 3 5 8 10 \
    --output-dir control/batches/eurusd_london_short \
    --sample-size 50

This allows parallel execution of independent (pair, session, direction) cohorts.
"""
from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import subprocess


def run_batch_discovery(
    pair: str,
    session: str,
    direction: str,
    buckets: List[int],
    output_dir: Path,
    workspace_root: Path,
    sample_size: int = 50,
) -> Dict[str, Any]:
    """
    Run complete discovery pipeline for a single (pair, session, direction, buckets) batch.
    
    Returns summary of outputs and validation results.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_key = f"{pair}_{session}_{direction}"
    batch_buckets_str = "_".join(str(b) for b in buckets)
    
    print(f"\n{'='*70}")
    print(f"BATCH DISCOVERY: {batch_key} / buckets {buckets}")
    print(f"{'='*70}")
    
    # For now, create a summary structure that tracks what would be discoverable
    # In the actual implementation, this would call the PC2 discovery phases
    
    results = {
        "batch_key": batch_key,
        "pair": pair,
        "session": session,
        "direction": direction,
        "buckets": buckets,
        "sample_size": sample_size,
        "output_dir": str(output_dir),
        "timestamp": datetime.utcnow().isoformat(),
        "status": "READY_FOR_DISCOVERY",
        "discovery_phases": {
            "phase_0_viability": None,
            "phase_1_family": None,
            "phase_2_structure": None,
            "phase_3_setup": None,
            "phase_4_trigger": None,
            "phase_6_ceiling": None,
        },
        "validation_status": {
            "enforcement_validation": None,
            "setup_phase_discovery": None,
            "setup_phase_promotion": None,
            "trigger_distinctness": None,
        },
        "summary": {
            "viable_count": 0,
            "families_discovered": [],
            "structures_discovered": [],
            "setup_count": 0,
            "trigger_count": 0,
            "max_expectancy": None,
            "max_quality_score": None,
        },
    }
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run batched discovery for a single (pair, session, direction) cohort"
    )
    parser.add_argument("--pair", required=True, help="Currency pair (e.g., EUR_USD)")
    parser.add_argument("--session", required=True, help="Trading session (e.g., London)")
    parser.add_argument("--direction", required=True, choices=["LONG", "SHORT"], help="Direction")
    parser.add_argument(
        "--buckets", type=int, nargs="+", required=True, help="Target bucket pips (e.g., 2 3 5 10)"
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="Output directory for batch artifacts"
    )
    parser.add_argument(
        "--sample-size", type=int, default=50, help="Max samples per slice (default 50)"
    )
    parser.add_argument(
        "--workspace-root", type=Path, default=Path.cwd(), help="Workspace root (for imports)"
    )
    
    args = parser.parse_args()
    
    results = run_batch_discovery(
        pair=args.pair,
        session=args.session,
        direction=args.direction,
        buckets=args.buckets,
        output_dir=args.output_dir,
        workspace_root=args.workspace_root,
        sample_size=args.sample_size,
    )
    
    # Write batch summary
    results_file = args.output_dir / "batch_discovery_summary.json"
    with results_file.open("w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nBatch summary: {results_file}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
