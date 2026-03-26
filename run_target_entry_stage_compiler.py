#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import shutil
from pathlib import Path
from typing import Sequence

from optimize_target_entry_classes import TARGETS
import optimize_target_entry_classes_contextual_v2 as contextual_v2
from optimize_target_entry_classes_pph_static_cached import load_csv, rule_applies, summarize


ROOT = Path(__file__).resolve().parent
MIN_ZONE_OPPORTUNITIES = 10
MAX_ZONE_OPPORTUNITIES = 10000
MIN_TOTAL_TRUTH_ROWS = 100
MAX_TOTAL_TRUTH_ROWS = 200000


def run(cmd: Sequence[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def jload(path: Path) -> dict:
    return json.loads(path.read_text())


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_truth_opportunity_sanity(truth_csv: Path) -> None:
    total_rows = 0
    zone_counts: dict[tuple[str, str, float], int] = {}
    with truth_csv.open() as f:
        for row in csv.DictReader(f):
            total_rows += 1
            try:
                key = (
                    str(row.get("quarter", "")),
                    str(row.get("direction_assumed") or row.get("direction") or ""),
                    float(row.get("target_distance") or 0.0),
                )
            except Exception:
                continue
            zone_counts[key] = zone_counts.get(key, 0) + 1

    anomalies: list[dict[str, object]] = []
    if total_rows < MIN_TOTAL_TRUTH_ROWS or total_rows > MAX_TOTAL_TRUTH_ROWS:
        anomalies.append(
            {
                "type": "total_truth_rows_out_of_bounds",
                "total_rows": total_rows,
                "min_allowed": MIN_TOTAL_TRUTH_ROWS,
                "max_allowed": MAX_TOTAL_TRUTH_ROWS,
            }
        )

    for (quarter, direction, target), count in sorted(zone_counts.items()):
        if count < MIN_ZONE_OPPORTUNITIES or count > MAX_ZONE_OPPORTUNITIES:
            anomalies.append(
                {
                    "type": "zone_opportunity_count_out_of_bounds",
                    "quarter": quarter,
                    "direction": direction,
                    "target_distance": target,
                    "count": count,
                    "min_allowed": MIN_ZONE_OPPORTUNITIES,
                    "max_allowed": MAX_ZONE_OPPORTUNITIES,
                }
            )

    if anomalies:
        raise RuntimeError(json.dumps({"status": "INVALID_OPPORTUNITY_SANITY", "truth_csv": str(truth_csv), "anomalies": anomalies[:50]}, indent=2))


def truth_matches_lock_dates(truth_csv: Path, dataset_lock: Path, sample_limit: int = 200) -> bool:
    try:
        lock = jload(dataset_lock)
    except Exception:
        return False
    valid_dates = {str(d) for d in lock.get("dates", [])}
    if not valid_dates:
        return True
    checked = 0
    try:
        with truth_csv.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session_id = str(row.get("session_id") or "").strip()
                ts = str(row.get("timestamp") or "").strip()
                ts_date = ts[:10] if len(ts) >= 10 else ""
                if session_id and session_id not in valid_dates:
                    return False
                if ts_date and ts_date not in valid_dates:
                    return False
                checked += 1
                if checked >= sample_limit:
                    break
    except Exception:
        return False
    return True


def optional_sha256_file(path: Path) -> str | None:
    return sha256_file(path) if path.exists() else None


def build_stage_inputs_hash(args: argparse.Namespace) -> str:
    state_machine = ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json"
    payload = {
        "dataset_lock_hash": sha256_file(args.dataset_lock),
        "data_root": str(args.data_root.resolve()),
        "historical_fast": args.historical_fast,
        "research_lite": args.research_lite,
        "research_max_sessions": args.research_max_sessions,
        "research_row_stride": args.research_row_stride,
        "research_max_rows_per_session": args.research_max_rows_per_session,
        "script_hashes": {
            "run_target_entry_stage_compiler.py": sha256_file(ROOT / "run_target_entry_stage_compiler.py"),
            "stage1_5_deterministic_compiler.py": sha256_file(ROOT / "stage1_5_deterministic_compiler.py"),
            "build_session_state_stream_v2.py": sha256_file(ROOT / "build_session_state_stream_v2.py"),
            "build_energy_context_engine.py": sha256_file(ROOT / "build_energy_context_engine.py"),
            "build_point_energy_trajectory.py": sha256_file(ROOT / "build_point_energy_trajectory.py"),
            "optimize_target_entry_classes_contextual_v2.py": sha256_file(ROOT / "optimize_target_entry_classes_contextual_v2.py"),
            "optimize_target_entry_classes_no_timeouts.py": sha256_file(ROOT / "optimize_target_entry_classes_no_timeouts.py"),
        },
        "config_hashes": {
            "entry_trigger_state_machine.json": optional_sha256_file(state_machine),
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def clear_stage_outputs(out_dir: Path) -> None:
    for name in [
        "stage1_6",
        "target_contextual_v2",
        "target_contextual_v2_targeted",
        "stream_seed",
        "context_seed",
        "trajectory_seed",
        "target_stage_report.json",
        "target_stage_manifest.json",
        "session_calibration_report.json",
        "session_calibration_manifest.json",
    ]:
        path = out_dir / name
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)


def copy_seed_cache(template_seed_root: Path, out_dir: Path) -> None:
    if not template_seed_root.exists():
        return
    for name in ["stream_seed", "context_seed", "trajectory_seed"]:
        src = template_seed_root / name
        dst = out_dir / name
        if src.exists() and not dst.exists():
            shutil.copytree(src, dst)


def copy_contextual_cache(template_context_root: Path, out_dir: Path) -> None:
    if not template_context_root.exists():
        return
    base_src = template_context_root / "target_contextual_v2"
    base_dst = out_dir / "target_contextual_v2"
    if base_src.exists() and not base_dst.exists():
        shutil.copytree(base_src, base_dst)
    targeted_src = template_context_root / "target_contextual_v2_targeted"
    targeted_dst = out_dir / "target_contextual_v2_targeted"
    if not targeted_src.exists():
        targeted_src = base_src
    if targeted_src.exists() and not targeted_dst.exists():
        shutil.copytree(targeted_src, targeted_dst)


def copy_stage1_6_cache(template_stage1_root: Path, out_dir: Path) -> None:
    if not template_stage1_root.exists():
        return
    dst = out_dir / "stage1_6"
    if template_stage1_root.exists() and not dst.exists():
        shutil.copytree(template_stage1_root, dst)


def write_template_apply_manifest(
    out_dir: Path,
    stage_inputs_hash: str,
    args: argparse.Namespace,
    stage1_6_dir: Path,
    stream_seed_dir: Path,
    context_seed_dir: Path,
    trajectory_seed_dir: Path,
    target_context_dir: Path,
    target_targeted_dir: Path,
) -> None:
    final_report = jload(target_context_dir / "target_entry_class_report.json")
    summary = final_report["summary"]
    best = max(summary, key=best_class_key) if summary else None
    manifest = {
        "runner": "run_target_entry_stage_compiler.py",
        "stage_inputs_hash": stage_inputs_hash,
        "historical_fast": args.historical_fast,
        "research_lite": args.research_lite,
        "research_config": {
            "max_sessions": args.research_max_sessions,
            "row_stride": args.research_row_stride,
            "max_rows_per_session": args.research_max_rows_per_session,
        },
        "dataset_lock": str(args.dataset_lock),
        "data_root": str(args.data_root),
        "stage1_6_output_dir": str(stage1_6_dir),
        "stream_seed_dir": str(stream_seed_dir),
        "context_seed_dir": str(context_seed_dir),
        "trajectory_seed_dir": str(trajectory_seed_dir),
        "target_contextual_v2_output_dir": str(target_context_dir),
        "target_contextual_v2_targeted_output_dir": str(target_targeted_dir),
        "target_no_timeout_output_dir": None,
        "final_report": str(target_context_dir / "target_entry_class_report.json"),
        "template_apply": True,
        "best_class": (
            {
                "direction": best["direction"],
                "target_distance": best["target_distance"],
                "trade_count": best["trade_count"],
                "tp_hits": best.get("tp_hits", best.get("wins", 0)),
                "sl_hits": best.get("sl_hits", best.get("losses", 0)),
                "timeouts": best.get("timeouts", 0),
                "tp_hit_rate": best.get("tp_hit_rate", best.get("win_rate", 0.0)),
                "objective_score": best.get("objective_score", 0.0),
                "capture_rate": best.get("capture_rate", 0.0),
                "opportunity_count": best.get("opportunity_count", 0),
                "total_pips": best.get("total_pips", best.get("expectancy", 0.0) * best.get("trade_count", 0)),
                "pips_per_hour": best.get("pips_per_hour", 0.0),
                "equity_per_hour_at_2pct_risk": best.get("equity_per_hour_at_2pct_risk", 0.0),
            }
            if best
            else None
        ),
    }
    (out_dir / "target_stage_manifest.json").write_text(json.dumps(manifest, indent=2))
    (out_dir / "target_stage_report.json").write_text(json.dumps(final_report, indent=2))


def write_frozen_no_timeout_outputs(base_rules_path: Path, truth_csv_path: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    base_payload = jload(base_rules_path)
    entry_classes = base_payload.get("entry_classes", [])
    (output_dir / "target_entry_classes.json").write_text(json.dumps({"entry_classes": entry_classes}, indent=2))

    rows = load_csv(truth_csv_path)
    selected_rows: list[dict] = []
    summary_rows: list[dict] = []

    grouped_rules: dict[tuple[str, float], list[dict]] = {}
    for rule in entry_classes:
        grouped_rules.setdefault((str(rule["direction"]).upper(), float(rule["target_distance"])), []).append(rule)

    grouped_rows: dict[tuple[str, float], list[dict]] = {}
    for row in rows:
        grouped_rows.setdefault((str(row["direction_assumed"]).upper(), float(row["target_distance"])), []).append(row)

    for (direction, target), truth_group in sorted(grouped_rows.items()):
        rules = grouped_rules.get((direction, target), [])
        chosen_rows: list[dict] = []
        for row in truth_group:
            for rule in rules:
                if rule_applies(row, rule):
                    chosen_rows.append(row)
                    break
        replay = summarize(chosen_rows, target)
        replay.update(
            {
                "direction": direction,
                "target_distance": target,
                "rule_count": len(rules),
                "trade_count": len(chosen_rows),
                "opportunity_count": len(truth_group),
                "capture_rate": (len(chosen_rows) / len(truth_group)) if truth_group else 0.0,
                "objective_score": replay["total_pips"],
            }
        )
        summary_rows.append(replay)
        for row in chosen_rows:
            selected_rows.append({"direction": direction, "target_distance": target, **row})

    fieldnames = list(selected_rows[0].keys()) if selected_rows else (list(rows[0].keys()) if rows else ["timestamp"])
    with (output_dir / "target_entry_population.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if selected_rows:
            writer.writerows(selected_rows)

    (output_dir / "target_entry_class_report.json").write_text(
        json.dumps(
            {
                "summary": summary_rows,
                "class_reports": {},
                "status": "FROZEN_RULES_FAST_MODE",
                "mode": "historical_fast",
                "empty_population": len(selected_rows) == 0,
                "class_count": len(entry_classes),
                "selected_trade_count": len(selected_rows),
            },
            indent=2,
        )
    )
    (output_dir / "target_entry_class_summary.csv").write_text(
        "direction,target_distance,rule_count,opportunity_count,capture_rate,objective_score,trade_count,tp_hits,sl_hits,timeouts,tp_hit_rate,avg_pips,avg_R,expectancy,total_pips,pips_per_hour,equity_per_hour_at_2pct_risk\n"
    )
    with (output_dir / "target_entry_class_summary.csv").open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "direction",
                "target_distance",
                "rule_count",
                "opportunity_count",
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
        writer.writerows({k: row.get(k, "") for k in writer.fieldnames} for row in summary_rows)


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


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Deterministic compiler from locked dataset through target-specific no-timeout entry classes."
    )
    ap.add_argument(
        "--dataset-lock",
        type=Path,
        default=ROOT / "dataset_lock_11_sessions.json",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "compiled_target_entry_stage_11_sessions",
    )
    ap.add_argument(
        "--data-root",
        type=Path,
        default=ROOT / "london_session_data_11",
    )
    ap.add_argument("--historical-fast", action="store_true", help="Skip no-timeout greedy optimization and use frozen target_contextual_v2 rules.")
    ap.add_argument("--research-lite", action="store_true", help="Use scoped contextual fitting for research instead of full-node truth construction.")
    ap.add_argument("--research-max-sessions", type=int, default=3)
    ap.add_argument("--research-row-stride", type=int, default=3)
    ap.add_argument("--research-max-rows-per-session", type=int, default=180)
    ap.add_argument("--template-stage1-root", type=Path)
    ap.add_argument("--template-seed-root", type=Path)
    ap.add_argument("--template-context-root", type=Path)
    args = ap.parse_args()
    lock = jload(args.dataset_lock)
    pair = str(lock.get("pair") or "EUR_USD")

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    stage1_6_dir = out_dir / "stage1_6"
    target_context_dir = out_dir / "target_contextual_v2"
    target_targeted_dir = out_dir / "target_contextual_v2_targeted"
    target_no_timeout_dir = out_dir / "target_no_timeouts"
    stream_seed_dir = out_dir / "stream_seed"
    context_seed_dir = out_dir / "context_seed"
    trajectory_seed_dir = out_dir / "trajectory_seed"
    if args.template_stage1_root:
        copy_stage1_6_cache(args.template_stage1_root, out_dir)
    if args.template_seed_root:
        copy_seed_cache(args.template_seed_root, out_dir)
    if args.template_context_root:
        copy_contextual_cache(args.template_context_root, out_dir)
    stage_inputs_hash = build_stage_inputs_hash(args)
    manifest_path = out_dir / "target_stage_manifest.json"
    if has_files(
        target_context_dir / "target_entry_classes.json",
        target_context_dir / "target_entry_truth_table.csv",
        target_targeted_dir / "target_entry_classes.json",
        stream_seed_dir / "session_energy_state_stream.csv",
        context_seed_dir / "session_energy_context_stream.csv",
        trajectory_seed_dir / "point_energy_trajectory.csv",
    ):
        if not (
            truth_matches_lock_dates(target_context_dir / "target_entry_truth_table.csv", args.dataset_lock)
            and truth_matches_lock_dates(target_targeted_dir / "target_entry_truth_table.csv", args.dataset_lock)
        ):
            clear_stage_outputs(out_dir)
        else:
            current = False
            if manifest_path.exists():
                try:
                    existing_manifest = jload(manifest_path)
                except Exception:
                    existing_manifest = {}
                current = existing_manifest.get("stage_inputs_hash") == stage_inputs_hash
            else:
                contextual_manifest = target_context_dir / "contextual_v2_manifest.json"
                targeted_manifest = target_targeted_dir / "contextual_v2_manifest.json"
                if contextual_manifest.exists() and targeted_manifest.exists():
                    try:
                        contextual = jload(contextual_manifest)
                        targeted = jload(targeted_manifest)
                    except Exception:
                        contextual = {}
                        targeted = {}
                    dataset_hash = sha256_file(args.dataset_lock)
                    current = (
                        contextual.get("dataset_hash") == dataset_hash
                        and targeted.get("dataset_hash") == dataset_hash
                    )
            if current:
                print(json.dumps({"status": "SKIP", "output_dir": str(out_dir), "reason": "target_stage_artifacts_current"}, indent=2))
                return
            clear_stage_outputs(out_dir)

    if has_files(
        stage1_6_dir / "compiler_report.json",
        stream_seed_dir / "session_energy_state_stream.csv",
        context_seed_dir / "session_energy_context_stream.csv",
        trajectory_seed_dir / "point_energy_trajectory.csv",
        target_context_dir / "target_entry_classes.json",
        target_context_dir / "target_entry_truth_table.csv",
        target_context_dir / "target_entry_class_report.json",
        target_targeted_dir / "target_entry_classes.json",
        target_targeted_dir / "target_entry_truth_table.csv",
        target_targeted_dir / "target_entry_class_report.json",
    ):
        if not (
            truth_matches_lock_dates(target_context_dir / "target_entry_truth_table.csv", args.dataset_lock)
            and truth_matches_lock_dates(target_targeted_dir / "target_entry_truth_table.csv", args.dataset_lock)
        ):
            clear_stage_outputs(out_dir)
        else:
            validate_truth_opportunity_sanity(target_context_dir / "target_entry_truth_table.csv")
            validate_truth_opportunity_sanity(target_targeted_dir / "target_entry_truth_table.csv")
            write_template_apply_manifest(
                out_dir,
                stage_inputs_hash,
                args,
                stage1_6_dir,
                stream_seed_dir,
                context_seed_dir,
                trajectory_seed_dir,
                target_context_dir,
                target_targeted_dir,
            )
            print(json.dumps({"status": "PASS", "output_dir": str(out_dir), "reason": "template_apply_cache_hit"}, indent=2))
            return

    if not has_files(
        stage1_6_dir / "compiler_report.json",
        stage1_6_dir / "phase1" / "opportunity_map_summary.json",
        stage1_6_dir / "phase2" / "opportunity_clusters.csv",
        stage1_6_dir / "phase3" / "entry_window_states.csv",
        stage1_6_dir / "phase4" / "opportunity_zones_labeled.csv",
        stage1_6_dir / "phase5" / "zone_label_separability.json",
        stage1_6_dir / "phase6" / "odm_ceiling_report.json",
    ):
        run(
            [
                "python3",
                str(ROOT / "stage1_5_deterministic_compiler.py"),
                "--dataset-lock",
                str(args.dataset_lock),
                "--data-root",
                str(args.data_root),
                "--pair",
                pair,
                "--output-root",
                str(stage1_6_dir),
            ]
        )

    # Build node-local stream/context/trajectory artifacts so contextual target
    # fitting cannot accidentally reuse shared global files from another node.
    if not has_files(
        stream_seed_dir / "session_energy_state_stream.csv",
        stream_seed_dir / "state_action_truth_table.csv",
    ):
        weekday = str(lock.get("weekday") or "thursday")
        session = str(lock.get("session") or "sydney")
        run(
            [
                "python3",
                str(ROOT / "build_session_state_stream_v2.py"),
                "--data-root",
                str(args.data_root),
                "--output-dir",
                str(stream_seed_dir),
                "--pair",
                pair,
                "--weekday",
                weekday,
                "--session",
                session,
            ]
        )

    if not has_files(
        context_seed_dir / "session_energy_context_stream.csv",
        context_seed_dir / "energy_context_report.json",
    ):
        run(
            [
                "python3",
                str(ROOT / "build_energy_context_engine.py"),
                "--stream-csv",
                str(stream_seed_dir / "session_energy_state_stream.csv"),
                "--rules-json",
                str(ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json"),
                "--output-dir",
                str(context_seed_dir),
            ]
        )

    if not has_files(
        trajectory_seed_dir / "point_energy_trajectory.csv",
        trajectory_seed_dir / "point_energy_transition_report.json",
    ):
        run(
            [
                "python3",
                str(ROOT / "build_point_energy_trajectory.py"),
                "--context-stream-csv",
                str(context_seed_dir / "session_energy_context_stream.csv"),
                "--truth-csv",
                str(stream_seed_dir / "state_action_truth_table.csv"),
                "--output-dir",
                str(trajectory_seed_dir),
            ]
        )

    # Full target contextual pass on the fixed 11-session truth sample.
    if not has_files(
        target_context_dir / "target_entry_classes.json",
        target_context_dir / "target_entry_truth_table.csv",
        target_context_dir / "target_entry_class_report.json",
    ):
        contextual_v2.run_contextual_v2(
            data_root=args.data_root,
            targets=TARGETS,
            context_csv=context_seed_dir / "session_energy_context_stream.csv",
            trajectory_csv=trajectory_seed_dir / "point_energy_trajectory.csv",
            out_dir=target_context_dir,
            research_mode=args.research_lite,
            research_max_sessions=args.research_max_sessions,
            research_row_stride=args.research_row_stride,
            research_max_rows_per_session=args.research_max_rows_per_session,
        )
    validate_truth_opportunity_sanity(target_context_dir / "target_entry_truth_table.csv")

    # Targeted rescue pass for the historically quarter-choked classes.
    if not has_files(
        target_targeted_dir / "target_entry_classes.json",
        target_targeted_dir / "target_entry_truth_table.csv",
        target_targeted_dir / "target_entry_class_report.json",
    ):
        contextual_v2.run_contextual_v2(
            data_root=args.data_root,
            targets=TARGETS,
            context_csv=context_seed_dir / "session_energy_context_stream.csv",
            trajectory_csv=trajectory_seed_dir / "point_energy_trajectory.csv",
            out_dir=target_targeted_dir,
            research_mode=args.research_lite,
            research_max_sessions=args.research_max_sessions,
            research_row_stride=args.research_row_stride,
            research_max_rows_per_session=args.research_max_rows_per_session,
        )
    validate_truth_opportunity_sanity(target_targeted_dir / "target_entry_truth_table.csv")

    summary = jload(target_context_dir / "target_entry_class_report.json")["summary"]
    best = max(summary, key=best_class_key) if summary else None

    manifest = {
        "runner": "run_target_entry_stage_compiler.py",
        "stage_inputs_hash": stage_inputs_hash,
        "historical_fast": args.historical_fast,
        "research_lite": args.research_lite,
        "research_config": {
            "max_sessions": args.research_max_sessions,
            "row_stride": args.research_row_stride,
            "max_rows_per_session": args.research_max_rows_per_session,
        },
        "dataset_lock": str(args.dataset_lock),
        "data_root": str(args.data_root),
        "stage1_6_output_dir": str(stage1_6_dir),
        "stream_seed_dir": str(stream_seed_dir),
        "context_seed_dir": str(context_seed_dir),
        "trajectory_seed_dir": str(trajectory_seed_dir),
        "target_contextual_v2_output_dir": str(target_context_dir),
        "target_contextual_v2_targeted_output_dir": str(target_targeted_dir),
        "target_no_timeout_output_dir": None,
        "final_report": str(target_context_dir / "target_entry_class_report.json"),
        "best_class": (
            {
                "direction": best["direction"],
                "target_distance": best["target_distance"],
                "trade_count": best["trade_count"],
                "tp_hits": best.get("tp_hits", best.get("wins", 0)),
                "sl_hits": best.get("sl_hits", best.get("losses", 0)),
                "timeouts": best.get("timeouts", 0),
                "tp_hit_rate": best.get("tp_hit_rate", best.get("win_rate", 0.0)),
                "objective_score": best.get("objective_score", 0.0),
                "capture_rate": best.get("capture_rate", 0.0),
                "opportunity_count": best.get("opportunity_count", 0),
                "total_pips": best.get("total_pips", best.get("expectancy", 0.0) * best.get("trade_count", 0)),
                "pips_per_hour": best.get("pips_per_hour", 0.0),
                "equity_per_hour_at_2pct_risk": best.get("equity_per_hour_at_2pct_risk", 0.0),
            }
            if best
            else None
        ),
    }

    (out_dir / "target_stage_manifest.json").write_text(json.dumps(manifest, indent=2))
    (out_dir / "target_stage_report.json").write_text(json.dumps(jload(target_context_dir / "target_entry_class_report.json"), indent=2))
    print(json.dumps({"status": "PASS", "output_dir": str(out_dir), "best_class": manifest["best_class"]}, indent=2))


if __name__ == "__main__":
    main()
