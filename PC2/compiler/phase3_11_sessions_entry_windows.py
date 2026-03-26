#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any


STOP_LIMIT_PIPS = 2.5


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_phase1_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["dt"] = parse_ts(row["timestamp"])
            rows.append(row)
    return rows


def load_clusters(path: Path) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["start_dt"] = parse_ts(row["cluster_start"])
            row["end_dt"] = parse_ts(row["cluster_end"])
            clusters.append(row)
    return clusters


def build_entry_rows(phase1_rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_session_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in phase1_rows:
        by_session_date[row["dt"].date().isoformat()].append(row)
    for rows in by_session_date.values():
        rows.sort(key=lambda r: r["dt"])

    out: list[dict[str, Any]] = []
    for cluster in clusters:
        session_rows = by_session_date.get(cluster["session_date"], [])
        cluster_rows = [r for r in session_rows if cluster["start_dt"] <= r["dt"] <= cluster["end_dt"]]
        if not cluster_rows:
            continue
        valid_rows: list[dict[str, Any]] = []
        direction = cluster["direction"]
        for row in cluster_rows:
            if direction == "LONG":
                valid = row["up_exists"] == "1" and float(row["mae_up_pips"]) <= STOP_LIMIT_PIPS
                future_mfe = float(row["mfe_up_pips"])
                future_mae = float(row["mae_up_pips"])
                time_to_target = int(float(row["tau_up_min"])) if row["up_exists"] == "1" else None
            else:
                valid = row["down_exists"] == "1" and float(row["mae_down_pips"]) <= STOP_LIMIT_PIPS
                future_mfe = float(row["mfe_down_pips"])
                future_mae = float(row["mae_down_pips"])
                time_to_target = int(float(row["tau_down_min"])) if row["down_exists"] == "1" else None
            if not valid:
                continue
            valid_rows.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "pair": cluster["pair"],
                    "direction": direction,
                    "session_date": cluster["session_date"],
                    "session": cluster["session"],
                    "weekday": cluster["weekday"],
                    "entry_window_start": cluster["cluster_start"],
                    "entry_window_end": cluster["cluster_end"],
                    "timestamp": row["timestamp"],
                    "future_mfe_pips": future_mfe,
                    "future_mae_pips": future_mae,
                    "time_to_target_min": time_to_target,
                    "valid_entry": 1,
                }
            )
        out.extend(valid_rows)
    return out


def build_summary(rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> dict[str, Any]:
    by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_cluster[row["cluster_id"]].append(row)
    window_lengths = []
    for cluster_id, cluster_rows in by_cluster.items():
        sorted_rows = sorted(cluster_rows, key=lambda r: parse_ts(r["timestamp"]))
        start = parse_ts(sorted_rows[0]["timestamp"])
        end = parse_ts(sorted_rows[-1]["timestamp"])
        window_lengths.append(int((end - start).total_seconds() // 60))

    by_pair = Counter(r["pair"] for r in rows)
    by_session = Counter(r["session"] for r in rows)
    return {
        "avg_valid_entry_points_per_cluster": mean(len(v) for v in by_cluster.values()) if by_cluster else 0.0,
        "avg_window_length_minutes": mean(window_lengths) if window_lengths else 0.0,
        "max_window_length_minutes": max(window_lengths) if window_lengths else 0,
        "valid_entries_by_pair": dict(by_pair),
        "valid_entries_by_session": dict(by_session),
        "cluster_count": len(clusters),
        "clusters_with_valid_entries": len(by_cluster),
        "stop_limit_pips": STOP_LIMIT_PIPS,
    }


def build_audit(rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> dict[str, Any]:
    cluster_ids = {c["cluster_id"] for c in clusters}
    row_cluster_ids = {r["cluster_id"] for r in rows}
    missing_clusters = sorted(cluster_ids - row_cluster_ids)
    orphan_rows = sorted(row_cluster_ids - cluster_ids)
    return {
        "every_entry_maps_to_valid_cluster": {"passed": len(orphan_rows) == 0, "orphan_cluster_ids": orphan_rows},
        "no_entry_without_cluster": {"passed": len(orphan_rows) == 0},
        "clusters_silently_skipped": {
            "passed": len(missing_clusters) == 0,
            "missing_cluster_ids": missing_clusters,
        },
        "overall_phase3_status": "PHASE3_PASS" if len(orphan_rows) == 0 and len(missing_clusters) == 0 else "PHASE3_PARTIAL",
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "cluster_id",
        "pair",
        "direction",
        "session_date",
        "session",
        "weekday",
        "entry_window_start",
        "entry_window_end",
        "timestamp",
        "future_mfe_pips",
        "future_mae_pips",
        "time_to_target_min",
        "valid_entry",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract entry-window states for the 11-session EUR/USD London Monday dataset")
    parser.add_argument("--phase1-csv", default="phase1_11_sessions_outputs/opportunity_map_raw.csv")
    parser.add_argument("--clusters-csv", default="phase2_11_sessions_outputs/opportunity_clusters.csv")
    parser.add_argument("--output-dir", default="phase3_11_sessions_outputs")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    phase1_rows = load_phase1_rows(Path(args.phase1_csv))
    clusters = load_clusters(Path(args.clusters_csv))
    entry_rows = build_entry_rows(phase1_rows, clusters)

    write_csv(output_dir / "entry_window_states.csv", entry_rows)
    (output_dir / "entry_window_summary.json").write_text(json.dumps(build_summary(entry_rows, clusters), indent=2))
    (output_dir / "entry_window_audit.json").write_text(json.dumps(build_audit(entry_rows, clusters), indent=2))

    print(f"Compiled {len(entry_rows)} valid entry states across {len(clusters)} clusters")
    print(f"Wrote: {output_dir / 'entry_window_states.csv'}")
    print(f"Wrote: {output_dir / 'entry_window_summary.json'}")
    print(f"Wrote: {output_dir / 'entry_window_audit.json'}")


if __name__ == "__main__":
    main()
