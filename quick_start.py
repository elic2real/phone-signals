#!/usr/bin/env python3
"""Quick-start runner for synthetic-data AEE simulation and validation.

Includes both:
- single-run smoke test (`test`)
- uncapped batch replay (`batch`) so analysis is not limited to 50 runs
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
from pathlib import Path

from aee_validator import AEEValidator
from tick_generator import export_ticks_csv, sample_scenario_mix


def _generate_ticks(rng: random.Random, scenario_weights: dict[str, float] | None = None) -> tuple[str, list[dict]]:
    scenario, ticks = sample_scenario_mix(rng=rng, weights=scenario_weights)
    ticks.sort(key=lambda x: x["ts"])
    return scenario, ticks


def _run_sim(csv_path: Path, out_path: Path, bucket_sec: float, pair: str) -> int:
    cmd = [
        sys.executable,
        "sim_harness.py",
        "--ticks",
        str(csv_path),
        "--pair",
        str(pair),
        "--direction",
        "LONG",
        "--atr-pips",
        "10",
        "--tp-atr",
        "1.2",
        "--bucket-sec",
        str(bucket_sec),
        "--out",
        str(out_path),
    ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)


def run_test_mode(bucket_sec: float, seed: int) -> int:
    rng = random.Random(seed)
    scenario, ticks = _generate_ticks(rng)
    pair = str(ticks[0]["instrument"]) if ticks else "EUR_USD"
    csv_path = Path("test_data.csv")
    out_path = Path("sim_results.json")
    export_ticks_csv(ticks, str(csv_path))

    rc = _run_sim(csv_path=csv_path, out_path=out_path, bucket_sec=bucket_sec, pair=pair)
    if rc != 0:
        return rc

    with out_path.open("r", encoding="utf-8") as f:
        results = json.load(f)
    results["scenario"] = scenario
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    validator = AEEValidator(results)
    report = validator.run_all_validations()
    with open("aee_validation_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("Validation complete -> aee_validation_report.json")
    return 0


def run_batch_mode(runs: int, output_dir: Path, audit_file: Path, bucket_sec: float, seed: int) -> int:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    for i in range(1, runs + 1):
        scenario, ticks = _generate_ticks(rng)
        pair = str(ticks[0]["instrument"]) if ticks else "EUR_USD"
        csv_path = output_dir / f"test_data_{i:04d}__{scenario}.csv"
        out_path = output_dir / f"sim_results_{i:04d}__{scenario}.json"
        export_ticks_csv(ticks, str(csv_path))
        rc = _run_sim(csv_path=csv_path, out_path=out_path, bucket_sec=bucket_sec, pair=pair)
        if rc != 0:
            return rc
        with out_path.open("r", encoding="utf-8") as f:
            res = json.load(f)
        res["scenario"] = scenario
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(res, f, indent=2)

    audit_cmd = [
        sys.executable,
        "sim_extraction_audit.py",
        "--input_dir",
        str(output_dir),
        "--output_file",
        str(audit_file),
    ]
    print("Running:", " ".join(audit_cmd))
    rc = subprocess.call(audit_cmd)
    if rc != 0:
        return rc

    with audit_file.open("r", encoding="utf-8") as f:
        report = json.load(f)
    summary = report.get("summary", {}) if isinstance(report, dict) else {}
    scorecard = report.get("AEE_SCORECARD", {}) if isinstance(report, dict) else {}
    print(
        "BATCH_SUMMARY",
        json.dumps(
            {
                "runs_requested": runs,
                "runs_analyzed": summary.get("total_trades", 0),
                "avg_pips_per_trade": summary.get("avg_pips_per_trade", 0.0),
                "pips_per_hour": summary.get("pips_per_hour", 0.0),
                "scorecard": scorecard,
            },
            indent=2,
        ),
    )
    print(f"Batch complete -> {audit_file}")
    return 0


def print_usage() -> None:
    print(
        "Usage:\n"
        "  python quick_start.py test [--bucket-sec 5]\n"
        "  python quick_start.py batch --runs 500 --output-dir sim_outputs_500 --audit-file extraction_audit_report_500.json [--bucket-sec 5]"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run single or batch synthetic AEE simulation replays")
    sub = parser.add_subparsers(dest="mode")

    p_test = sub.add_parser("test", help="single smoke run")
    p_test.add_argument("--bucket-sec", type=float, default=5.0)
    p_test.add_argument("--seed", type=int, default=123)

    p_batch = sub.add_parser("batch", help="uncapped batch runs + aggregate audit")
    p_batch.add_argument("--runs", type=int, required=True, help="number of runs (not capped)")
    p_batch.add_argument("--output-dir", default="sim_outputs", help="directory for per-run sim_results files")
    p_batch.add_argument("--audit-file", default="extraction_audit_report.json", help="aggregate output file")
    p_batch.add_argument("--bucket-sec", type=float, default=5.0)
    p_batch.add_argument("--seed", type=int, default=123)

    args = parser.parse_args()
    if args.mode == "test" or args.mode is None:
        raise SystemExit(run_test_mode(bucket_sec=getattr(args, "bucket_sec", 5.0), seed=getattr(args, "seed", 123)))
    if args.mode == "batch":
        raise SystemExit(
            run_batch_mode(
                runs=args.runs,
                output_dir=Path(args.output_dir),
                audit_file=Path(args.audit_file),
                bucket_sec=args.bucket_sec,
                seed=args.seed,
            )
        )

    print_usage()
    raise SystemExit(1)
