#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


STOP_LIMIT_PIPS = 2.5
SESSION_HOURS = 8.0


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def cluster_label(good: int, bad: int, noise: int) -> str:
    if good > 0:
        return "GOOD"
    if bad > 0:
        return "BAD"
    return "NOISE"


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 6 ODM/ceiling compiler for the 11-session deterministic pipeline")
    parser.add_argument("--clusters-csv", default="compiled_stage1_5_11_sessions/phase2/opportunity_clusters.csv")
    parser.add_argument("--entry-windows-csv", default="compiled_stage1_5_11_sessions/phase3/entry_window_states.csv")
    parser.add_argument("--labeled-csv", default="compiled_stage1_5_11_sessions/phase4/opportunity_zones_labeled.csv")
    parser.add_argument("--dataset-lock", default="dataset_lock_11_sessions.json")
    parser.add_argument("--output-dir", default="compiled_stage1_5_11_sessions/phase6")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    clusters = load_csv(Path(args.clusters_csv))
    entry_rows = load_csv(Path(args.entry_windows_csv))
    labeled = load_csv(Path(args.labeled_csv))
    dataset_lock = json.loads(Path(args.dataset_lock).read_text())

    label_index = {(r["timestamp_start"], r["direction"]): r["zone_label"] for r in labeled}
    windows_by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in entry_rows:
        windows_by_cluster[row["cluster_id"]].append(row)

    cluster_rows: list[dict[str, Any]] = []
    for cluster in clusters:
        members = [ts for ts in cluster["member_start_times"].split("|") if ts]
        counts = Counter(label_index.get((ts, cluster["direction"]), "NOISE") for ts in members)
        c_label = cluster_label(counts.get("GOOD", 0), counts.get("BAD", 0), counts.get("NOISE", 0))
        valid_rows = windows_by_cluster.get(cluster["cluster_id"], [])
        cluster_mfe = float(cluster["cluster_MFE_pips"])
        entry_ceiling_ratio = mean(
            min(1.0, float(r["future_mfe_pips"]) / cluster_mfe) if cluster_mfe > 0 else 0.0
            for r in valid_rows
        ) if valid_rows else 0.0
        cluster_rows.append(
            {
                **cluster,
                "cluster_label": c_label,
                "good_members": counts.get("GOOD", 0),
                "bad_members": counts.get("BAD", 0),
                "noise_members": counts.get("NOISE", 0),
                "valid_entry_count": len(valid_rows),
                "entry_ceiling_ratio": entry_ceiling_ratio,
            }
        )

    session_count = int(dataset_lock.get("session_count", 1))
    total_hours = session_count * SESSION_HOURS
    good_clusters = [r for r in cluster_rows if r["cluster_label"] == "GOOD"]
    bad_clusters = [r for r in cluster_rows if r["cluster_label"] == "BAD"]

    c_good = mean(1.0 if int(r["valid_entry_count"]) > 0 else 0.0 for r in good_clusters) if good_clusters else 0.0
    b_bad = mean(1.0 if int(r["valid_entry_count"]) > 0 else 0.0 for r in bad_clusters) if bad_clusters else 0.0
    x_good = mean(float(r["entry_ceiling_ratio"]) for r in good_clusters) if good_clusters else 0.0
    mfe_good = mean(float(r["cluster_MFE_pips"]) for r in good_clusters) if good_clusters else 0.0
    l_bad = mean(min(float(r["cluster_MAE_pips"]), STOP_LIMIT_PIPS) for r in bad_clusters) if bad_clusters else STOP_LIMIT_PIPS
    raw_supply = len(good_clusters) / total_hours if total_hours > 0 else 0.0
    theoretical_pips_per_hour = ((len(good_clusters) * c_good * x_good * mfe_good) - (len(bad_clusters) * b_bad * l_bad)) / total_hours if total_hours > 0 else 0.0

    by_pair_session: dict[str, dict[str, int]] = {}
    grouped = defaultdict(list)
    for r in cluster_rows:
        grouped[r["pair"]].append(r)
    for pair, rows in grouped.items():
        by_pair_session[pair] = dict(Counter(r["session"] for r in rows))

    report = {
        "executable_clusters_per_pair_session": by_pair_session,
        "avg_good_mfe_pips": mfe_good,
        "raw_movement_supply_per_hour": raw_supply,
        "theoretical_pips_per_hour_ceiling": theoretical_pips_per_hour,
        "theoretical_equity_per_hour_ceiling": theoretical_pips_per_hour,
        "cluster_resolved_totals_only": True,
        "entry_window_based_capture_proxy": True,
        "formula_inputs": {
            "n_good": len(good_clusters),
            "c_good": c_good,
            "x_good": x_good,
            "mfe_good": mfe_good,
            "n_bad": len(bad_clusters),
            "b_bad": b_bad,
            "l_bad": l_bad,
            "session_count": session_count,
            "hours_per_session": SESSION_HOURS,
        },
    }
    audit = {
        "good_clusters_present": len(good_clusters) > 0,
        "bad_clusters_present": len(bad_clusters) > 0,
        "cluster_rows_compiled": len(cluster_rows),
        "entry_rows_compiled": len(entry_rows),
        "labeled_rows_compiled": len(labeled),
        "overall_phase6_status": "PHASE6_PASS" if len(good_clusters) > 0 and len(cluster_rows) > 0 else "PHASE6_FAIL",
    }

    (output_dir / "odm_ceiling_report.json").write_text(json.dumps(report, indent=2))
    (output_dir / "odm_audit.json").write_text(json.dumps(audit, indent=2))
    fieldnames = list(cluster_rows[0].keys()) if cluster_rows else [
        "cluster_id",
        "pair",
        "direction",
        "date",
        "session",
        "weekday",
        "timestamp_start",
        "timestamp_end",
        "duration_min",
        "cluster_MFE_pips",
        "cluster_MAE_pips",
        "member_count",
        "member_start_times",
        "cluster_label",
        "good_members",
        "bad_members",
        "noise_members",
        "valid_entry_count",
        "entry_ceiling_ratio",
    ]
    with (output_dir / "cluster_resolved_labels.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cluster_rows)

    print(json.dumps({"report": report, "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
