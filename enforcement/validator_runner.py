#!/usr/bin/env python3
"""
Enforcement: Validator Runner
==============================
Top-level CLI that runs ALL enforcement checks in sequence on a directory
of PC2 discovery artifacts. Intended to be the single entry point for the
enforcement side after a PC2 run.

Exit codes:
    0 — all checks passed
    1 — one or more checks failed

Usage:
    python validator_runner.py --artifacts path/to/artifacts/
    python validator_runner.py --artifacts path/to/ --skip-analyzer
"""
from __future__ import annotations

import sys
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from artifact_validator import validate_batch, print_results
from ownership_validator import validate_directory as ownership_check
from dependency_validator import validate_directory as dependency_check
from universal_analyzer import build_bundles_from_directory, UniversalAnalyzerRunner


def run_all(artifacts_dir: Path, skip_analyzer: bool = False) -> int:
    print("\n" + "=" * 70)
    print(f"Enforcement Validator Runner")
    print(f"Artifacts directory: {artifacts_dir}")
    print("=" * 70)

    overall_exit = 0

    # ---- Step 1: Schema + domain constraint validation ----
    print("\n[1/4] Schema & Domain Constraint Validation")
    print("-" * 50)
    batch_results = validate_batch(artifacts_dir)
    exit_code = print_results(batch_results, verbose=False)
    if exit_code != 0:
        overall_exit = 1

    # ---- Step 2: Ownership validation ----
    print("\n[2/4] Ownership Validation")
    print("-" * 50)
    ownership_result = ownership_check(artifacts_dir)
    print(ownership_result.summary())
    if not ownership_result.passed:
        overall_exit = 1

    # ---- Step 3: Dependency chain validation ----
    print("\n[3/4] Dependency Chain Validation")
    print("-" * 50)
    dep_result = dependency_check(artifacts_dir)
    print(dep_result.summary())
    if not dep_result.passed:
        overall_exit = 1

    # ---- Step 4: Universal analyzer routing (scaffolding only) ----
    if not skip_analyzer:
        print("\n[4/4] Universal Analyzer (routing confirmation)")
        print("-" * 50)
        bundles = build_bundles_from_directory(artifacts_dir)
        if bundles:
            runner = UniversalAnalyzerRunner()
            results = runner.run(bundles)
            runner.print_routing_report(results)
        else:
            print("  No artifact bundles found — skipping analyzer routing.")
    else:
        print("\n[4/4] Universal Analyzer — SKIPPED")

    print("=" * 70)
    status = "ALL CHECKS PASSED" if overall_exit == 0 else "ONE OR MORE CHECKS FAILED"
    print(f"Result: {status}")
    print("=" * 70 + "\n")
    return overall_exit


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all enforcement checks on a directory of PC2 artifacts."
    )
    parser.add_argument(
        "--artifacts", type=Path, required=True,
        help="Directory containing PC2 discovery artifact .json files."
    )
    parser.add_argument(
        "--skip-analyzer", action="store_true",
        help="Skip the universal analyzer routing step."
    )
    args = parser.parse_args()

    if not args.artifacts.is_dir():
        print(f"ERROR: {args.artifacts} is not a directory.", file=sys.stderr)
        return 2

    return run_all(args.artifacts, skip_analyzer=args.skip_analyzer)


if __name__ == "__main__":
    sys.exit(main())
