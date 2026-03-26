#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


SESSION_HOURS = 88.0
MIN_ZONE_OPPORTUNITIES = 10
MAX_ZONE_OPPORTUNITIES = 10000


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def jload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_truth(truth: pd.DataFrame) -> pd.DataFrame:
    df = truth.copy()
    df["dir"] = df["direction_assumed"] if "direction_assumed" in df.columns else df["direction"]
    df["target"] = df["target_distance"].astype(float)
    if "session_id" not in df.columns:
        df["session_id"] = "unknown"
    df["timestamp"] = df["timestamp"].astype(str)
    return df


def compute_zone_metrics(zone: pd.DataFrame, target: float) -> dict[str, Any]:
    tp_feasible = (zone["future_mfe_pips"].astype(float) >= target).astype(int)
    tp_hit = zone.loc[zone["tp_hit_min"].astype(float) > 0, "tp_hit_min"].astype(float)
    return {
        "opportunity_count": int(len(zone)),
        "opportunity_density_per_hour": float(len(zone) / SESSION_HOURS),
        "distinct_timestamps": int(zone["timestamp"].nunique()),
        "distinct_session_ids": int(zone["session_id"].astype(str).nunique()),
        "tp_feasible_count": int(tp_feasible.sum()),
        "tp_feasible_density_per_hour": float(tp_feasible.sum() / SESSION_HOURS),
        "median_tp_hit_min": float(tp_hit.median()) if not tp_hit.empty else None,
    }


def run(dataset_lock: Path, truth_csv: Path, output_dir: Path) -> dict[str, Any]:
    lock = jload(dataset_lock)
    truth = normalize_truth(pd.read_csv(truth_csv))

    zones: list[dict[str, Any]] = []
    sanity_anomalies: list[dict[str, Any]] = []
    for (quarter, direction, target), zone in truth.groupby(["quarter", "dir", "target"]):
        metrics = compute_zone_metrics(zone, float(target))
        if metrics["opportunity_count"] < MIN_ZONE_OPPORTUNITIES or metrics["opportunity_count"] > MAX_ZONE_OPPORTUNITIES:
            sanity_anomalies.append(
                {
                    "quarter": quarter,
                    "direction": direction,
                    "target_distance": float(target),
                    "opportunity_count": metrics["opportunity_count"],
                    "min_allowed": MIN_ZONE_OPPORTUNITIES,
                    "max_allowed": MAX_ZONE_OPPORTUNITIES,
                }
            )
        zones.append(
            {
                "quarter": quarter,
                "direction": direction,
                "target_distance": float(target),
                **metrics,
            }
        )

    pair_rollup: dict[str, Any] = {
        "total_opportunities": int(len(truth)),
        "total_opportunity_density_per_hour": float(len(truth) / SESSION_HOURS),
        "distinct_timestamps": int(truth["timestamp"].nunique()),
        "distinct_session_ids": int(truth["session_id"].astype(str).nunique()),
    }
    for direction in ["LONG", "SHORT"]:
        dir_zones = [z for z in zones if z["direction"] == direction]
        prefix = direction.lower()
        pair_rollup[f"{prefix}_opportunity_count"] = int(sum(int(z["opportunity_count"]) for z in dir_zones))
        pair_rollup[f"{prefix}_opportunity_density_per_hour"] = round(
            sum(float(z["opportunity_density_per_hour"]) for z in dir_zones), 6
        )
        pair_rollup[f"{prefix}_tp_feasible_count"] = int(sum(int(z["tp_feasible_count"]) for z in dir_zones))
        pair_rollup[f"{prefix}_tp_feasible_density_per_hour"] = round(
            sum(float(z["tp_feasible_density_per_hour"]) for z in dir_zones), 6
        )

    report = {
        "status": "INVALID_OPPORTUNITY_SANITY" if sanity_anomalies else "PASS",
        "mode": "session_opportunity_map",
        "timestamp": iso_now(),
        "node": {
            "pair": lock.get("pair"),
            "weekday": lock.get("weekday"),
            "session": lock.get("session"),
        },
        "pair_rollup": pair_rollup,
        "zones": zones,
        "sanity_anomalies": sanity_anomalies,
    }
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "dataset_lock_hash": sha256_file(dataset_lock),
                "truth_csv_hash": sha256_file(truth_csv),
                "script_hash": sha256_file(Path(__file__)),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "runner": "session_opportunity_map.py",
        "inputs_hash": inputs_hash,
        "dataset_lock": str(dataset_lock),
        "truth_csv": str(truth_csv),
        "report": str(output_dir / "session_opportunity_map_report.json"),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "session_opportunity_map_report.json").write_text(json.dumps(report, indent=2))
    (output_dir / "session_opportunity_map_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"status": report["status"], "node": report["node"], "output_dir": str(output_dir)}, indent=2))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--truth-csv", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    run(
        dataset_lock=args.dataset_lock,
        truth_csv=args.truth_csv,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
