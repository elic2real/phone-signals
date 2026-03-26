#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from pathlib import Path

from optimize_target_entry_classes_pph_static_cached import load_csv, rule_applies, summarize


def best_class_key(row: dict) -> tuple:
    return (
        1 if row.get("timeouts", 0) == 0 else 0,
        float(row.get("objective_score", 0.0)),
        float(row.get("total_pips", 0.0)),
        float(row.get("pips_per_hour", 0.0)),
        float(row.get("capture_rate", 0.0)),
        int(row.get("trade_count", 0)),
        float(row.get("expectancy", 0.0)),
        float(row.get("tp_hit_rate", 0.0)),
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def build_inputs_hash(
    base_rules: Path,
    targeted_rules: Path,
    truth_csv: Path,
    historical_fast: bool,
    priority_mode: str,
    frozen_rules: Path | None = None,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "base_rules_hash": sha256_file(base_rules),
                "targeted_rules_hash": sha256_file(targeted_rules),
                "truth_csv_hash": sha256_file(truth_csv),
                "optimizer_hash": sha256_file(Path(__file__).resolve().parent / "optimize_target_entry_classes_no_timeouts.py"),
                "historical_fast": historical_fast,
                "priority_mode": priority_mode,
                "frozen_rules_hash": sha256_file(frozen_rules) if frozen_rules and frozen_rules.exists() else None,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def write_frozen_outputs(base_rules: Path, truth_csv: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    base_payload = json.loads(base_rules.read_text())
    entry_classes = base_payload.get("entry_classes", [])
    (output_dir / "target_entry_classes.json").write_text(json.dumps({"entry_classes": entry_classes}, indent=2))
    rows = load_csv(truth_csv)
    selected_rows = []
    summary = []
    for direction in sorted({row["direction_assumed"] for row in rows}):
        direction_rows = [row for row in rows if row["direction_assumed"] == direction]
        for target in sorted({float(row["target_distance"]) for row in direction_rows}):
            target_rows = [row for row in direction_rows if float(row["target_distance"]) == target]
            rules = [
                rule
                for rule in entry_classes
                if rule.get("direction") == direction and float(rule.get("target_distance", 0.0)) == target
            ]
            chosen_rows = []
            for row in target_rows:
                for rule in rules:
                    if rule_applies(row, rule):
                        chosen_rows.append(row)
                        break
            selected_rows.extend(chosen_rows)
            replay = summarize(chosen_rows, target)
            trade_count = int(replay.get("trade_count", 0))
            opp_count = len(target_rows)
            summary.append(
                {
                    "direction": direction,
                    "target_distance": target,
                    "rule_count": len(rules),
                    "opportunity_count": opp_count,
                    "opportunity_density_per_hour": opp_count / 88.0 if opp_count else 0.0,
                    "selected_density_per_hour": trade_count / 88.0 if trade_count else 0.0,
                    "capture_rate": (trade_count / opp_count) if opp_count else 0.0,
                    "objective_score": replay.get("total_pips", 0.0),
                    "trade_count": trade_count,
                    "tp_hits": int(replay.get("tp_hits", 0)),
                    "sl_hits": int(replay.get("sl_hits", 0)),
                    "timeouts": int(replay.get("timeouts", 0)),
                    "tp_hit_rate": float(replay.get("tp_hit_rate", 0.0)),
                    "avg_pips": float(replay.get("avg_pips", 0.0)),
                    "avg_R": float(replay.get("avg_R", 0.0)),
                    "expectancy": float(replay.get("expectancy", 0.0)),
                    "total_pips": float(replay.get("total_pips", 0.0)),
                    "pips_per_hour": float(replay.get("pips_per_hour", 0.0)),
                    "equity_per_hour_at_2pct_risk": float(replay.get("equity_per_hour_at_2pct_risk", 0.0)),
                }
            )

    population_path = output_dir / "target_entry_population.csv"
    fieldnames = list(selected_rows[0].keys()) if selected_rows else (list(rows[0].keys()) if rows else ["timestamp"])
    with population_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if selected_rows:
            writer.writerows(selected_rows)

    summary_payload = {
        "summary": summary,
        "class_reports": {},
        "status": "FROZEN_RULES_FAST_MODE",
        "mode": "historical_fast",
        "empty_population": len(selected_rows) == 0,
        "class_count": len(entry_classes),
        "selected_trade_count": len(selected_rows),
    }
    (output_dir / "target_entry_class_report.json").write_text(json.dumps(summary_payload, indent=2))
    with (output_dir / "target_entry_class_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "direction",
                "target_distance",
                "rule_count",
                "opportunity_count",
                "opportunity_density_per_hour",
                "selected_density_per_hour",
                "capture_rate",
                "objective_score",
                "trade_count",
                "tp_hits",
                "sl_hits",
                "timeouts",
                "tp_hit_rate",
                "avg_pips",
                "avg_R",
                "expectancy",
                "total_pips",
                "pips_per_hour",
                "equity_per_hour_at_2pct_risk",
            ],
        )
        writer.writeheader()
        writer.writerows(summary)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--base-rules",
        type=Path,
        required=True,
    )
    ap.add_argument(
        "--targeted-rules",
        type=Path,
        required=True,
    )
    ap.add_argument(
        "--truth-csv",
        type=Path,
        required=True,
    )
    ap.add_argument(
        "--output-dir",
        default="compiled_target_entry_classes_no_timeouts_11_sessions",
        type=Path,
    )
    ap.add_argument("--historical-fast", action="store_true", help="Use frozen base rules and skip greedy no-timeout optimization.")
    ap.add_argument("--priority-mode", choices=["balanced", "winrate_first", "expand_quality_entries"], default="balanced")
    ap.add_argument(
        "--frozen-rules",
        type=Path,
        help="Existing no-timeout rules to preserve for buckets outside the scoped optimize set.",
    )
    ap.add_argument(
        "--optimize-group",
        action="append",
        default=[],
        help="Restrict optimization to specific class buckets formatted as DIRECTION:TARGET_DISTANCE (for example LONG:2.5).",
    )
    ap.add_argument(
        "--freeze-unlisted-groups",
        action="store_true",
        help="Keep every class bucket not listed in --optimize-group frozen to the merged baseline rules.",
    )
    args = ap.parse_args()

    optimize_groups = sorted(str(spec).strip().upper() for spec in (args.optimize_group or []) if str(spec).strip())
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "base_hash": build_inputs_hash(
                    args.base_rules,
                    args.targeted_rules,
                    args.truth_csv,
                    args.historical_fast,
                    args.priority_mode,
                    args.frozen_rules,
                ),
                "optimize_groups": optimize_groups,
                "freeze_unlisted_groups": bool(args.freeze_unlisted_groups),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest_path = args.output_dir / "runner_manifest.json"
    if has_files(
        args.output_dir / "target_entry_class_report.json",
        args.output_dir / "target_entry_classes.json",
        args.output_dir / "target_entry_population.csv",
        manifest_path,
    ):
        try:
            existing_manifest = json.loads(manifest_path.read_text())
        except Exception:
            existing_manifest = {}
        if existing_manifest.get("inputs_hash") == inputs_hash:
            print(json.dumps({"status": "SKIP", "output_dir": str(args.output_dir), "reason": "no_timeout_artifacts_current"}, indent=2))
            return

    if args.historical_fast:
        write_frozen_outputs(args.base_rules, args.truth_csv, args.output_dir)
    else:
        cmd = [
            "python3",
            "optimize_target_entry_classes_no_timeouts.py",
            "--base-rules",
            str(args.base_rules),
            "--targeted-rules",
            str(args.targeted_rules),
            "--truth-csv",
            str(args.truth_csv),
            "--output-dir",
            str(args.output_dir),
            "--priority-mode",
            args.priority_mode,
        ]
        if args.frozen_rules:
            cmd.extend(["--frozen-rules", str(args.frozen_rules)])
        for spec in optimize_groups:
            cmd.extend(["--optimize-group", spec])
        if args.freeze_unlisted_groups:
            cmd.append("--freeze-unlisted-groups")
        subprocess.run(cmd, check=True)

    report_path = args.output_dir / "target_entry_class_report.json"
    report = json.loads(report_path.read_text())
    summary = report["summary"]
    best = max(summary, key=best_class_key) if summary else None

    manifest = {
        "runner": "run_target_entry_no_timeout.py",
        "source_optimizer": "optimize_target_entry_classes_no_timeouts.py",
        "base_rules": str(args.base_rules),
        "targeted_rules": str(args.targeted_rules),
        "truth_csv": str(args.truth_csv),
        "historical_fast": args.historical_fast,
        "priority_mode": args.priority_mode,
        "frozen_rules": str(args.frozen_rules) if args.frozen_rules else None,
        "optimize_groups": optimize_groups,
        "freeze_unlisted_groups": bool(args.freeze_unlisted_groups),
        "inputs_hash": inputs_hash,
        "report": str(report_path),
        "best_class": (
            {
                "direction": best["direction"],
                "target_distance": best["target_distance"],
                "trade_count": best["trade_count"],
                "tp_hits": best["tp_hits"],
                "sl_hits": best["sl_hits"],
                "timeouts": best["timeouts"],
                "tp_hit_rate": best["tp_hit_rate"],
                "objective_score": best.get("objective_score", 0.0),
                "capture_rate": best.get("capture_rate", 0.0),
                "opportunity_count": best.get("opportunity_count", 0),
                "total_pips": best["total_pips"],
                "pips_per_hour": best["pips_per_hour"],
                "equity_per_hour_at_2pct_risk": best["equity_per_hour_at_2pct_risk"],
            }
            if best
            else None
        ),
    }
    (args.output_dir / "runner_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
