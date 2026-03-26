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
ROOT = Path(__file__).resolve().parent
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


def signed_value(series: pd.Series, direction: str) -> pd.Series:
    if direction == "LONG":
        return series.astype(float)
    return -series.astype(float)


def normalize(series: pd.Series) -> pd.Series:
    s = series.astype(float).fillna(0.0)
    lo = s.quantile(0.10)
    hi = s.quantile(0.90)
    if hi <= lo:
        return pd.Series([0.5] * len(s), index=s.index)
    return ((s - lo) / (hi - lo)).clip(0.0, 1.0)


def compute_zone_potential(zone: pd.DataFrame, direction: str, target: float) -> dict[str, Any]:
    df = zone.copy()
    if df.empty:
        return {
            "opportunity_count": 0,
            "opportunity_density_per_hour": 0.0,
            "potential_count": 0,
            "expected_opportunities_per_hour": 0.0,
            "strong_potential_count": 0,
            "expected_strong_opportunities_per_hour": 0.0,
            "expected_recyclable_opportunities_per_hour": 0.0,
            "median_time_to_tp": None,
        }

    feasible = (df["future_mfe_pips"].astype(float) >= target).astype(float)
    signed_vel = normalize(signed_value(df["velocity_now"], direction))
    signed_pressure = normalize(signed_value(df["pressure_5"], direction))
    signed_bias = normalize(signed_value(df["quarter_relative_bias"], direction))
    vol_score = normalize(df["recent_vol_10"])
    range_score = normalize(df["recent_range_20"])
    breakout_score = normalize(df["breakout_distance"])
    compression_score = 1.0 - normalize(df["compression"])
    tp_speed = 1.0 - normalize(df["tp_hit_min"].fillna(9999).clip(upper=9999))
    recyclable = ((df["tp_hit_min"].astype(float) > 0) & (df["tp_hit_min"].astype(float) <= 20)).astype(float)

    potential_score = (
        0.22 * feasible
        + 0.16 * signed_vel
        + 0.16 * signed_pressure
        + 0.12 * signed_bias
        + 0.12 * vol_score
        + 0.08 * range_score
        + 0.08 * breakout_score
        + 0.06 * compression_score
    )
    strong_potential_score = 0.75 * potential_score + 0.25 * tp_speed

    potential_mask = (feasible > 0) & (potential_score >= 0.45)
    strong_mask = (feasible > 0) & (strong_potential_score >= 0.60)
    recyclable_mask = potential_mask & (recyclable > 0)

    tp_hit = df.loc[df["tp_hit_min"].astype(float) > 0, "tp_hit_min"].astype(float)
    median_time_to_tp = float(tp_hit.median()) if not tp_hit.empty else None

    return {
        "opportunity_count": int(len(df)),
        "opportunity_density_per_hour": float(len(df) / SESSION_HOURS),
        "potential_count": int(potential_mask.sum()),
        "expected_opportunities_per_hour": float(potential_mask.sum() / SESSION_HOURS),
        "strong_potential_count": int(strong_mask.sum()),
        "expected_strong_opportunities_per_hour": float(strong_mask.sum() / SESSION_HOURS),
        "recyclable_potential_count": int(recyclable_mask.sum()),
        "expected_recyclable_opportunities_per_hour": float(recyclable_mask.sum() / SESSION_HOURS),
        "median_time_to_tp": median_time_to_tp,
    }


def run(
    dataset_lock: Path,
    truth_csv: Path,
    entry_population_csv: Path,
    output_dir: Path,
) -> dict[str, Any]:
    lock = jload(dataset_lock)
    truth = pd.read_csv(truth_csv)
    population = pd.read_csv(entry_population_csv)

    truth["dir"] = truth["direction_assumed"] if "direction_assumed" in truth.columns else truth["direction"]
    truth["target"] = truth["target_distance"].astype(float)
    population["dir"] = population["direction_assumed"] if "direction_assumed" in population.columns else population["direction"]
    population["target"] = population["target_distance"].astype(float)

    selected = (
        population.groupby(["quarter", "dir", "target"])
        .size()
        .rename("selected_count")
        .reset_index()
    )
    entry = (
        population.groupby(["quarter", "dir", "target"])
        .agg(
            actual_trades=("timestamp", "count"),
            entry_win_rate=("static_pips", lambda s: float((pd.Series(s) > 0).mean())),
            entry_total_pips=("static_pips", "sum"),
        )
        .reset_index()
    )

    zones: list[dict[str, Any]] = []
    sanity_anomalies: list[dict[str, Any]] = []
    for (quarter, direction, target), zone in truth.groupby(["quarter", "dir", "target"]):
        metrics = compute_zone_potential(zone, direction, float(target))
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
        selected_row = selected[
            (selected["quarter"] == quarter) & (selected["dir"] == direction) & (selected["target"] == float(target))
        ]
        entry_row = entry[
            (entry["quarter"] == quarter) & (entry["dir"] == direction) & (entry["target"] == float(target))
        ]
        selected_count = int(selected_row["selected_count"].iloc[0]) if not selected_row.empty else 0
        actual_tph = float(selected_count / SESSION_HOURS)
        expected_tph = float(metrics["expected_opportunities_per_hour"])
        expected_recyclable = float(metrics["expected_recyclable_opportunities_per_hour"])
        zones.append(
            {
                "quarter": quarter,
                "direction": direction,
                "target_distance": float(target),
                **metrics,
                "actual_selected_count": selected_count,
                "actual_trades_per_hour": actual_tph,
                "utilization_ratio": float(actual_tph / expected_tph) if expected_tph > 0 else 0.0,
                "recycling_utilization_ratio": float(actual_tph / expected_recyclable) if expected_recyclable > 0 else 0.0,
                "entry_win_rate": float(entry_row["entry_win_rate"].iloc[0]) if not entry_row.empty else 0.0,
                "entry_total_pips": float(entry_row["entry_total_pips"].iloc[0]) if not entry_row.empty else 0.0,
            }
        )

    pair_rollup: dict[str, Any] = {}
    for direction in ["LONG", "SHORT"]:
        dir_zones = [z for z in zones if z["direction"] == direction]
        key = direction.lower()
        pair_rollup[f"expected_{key}_opportunities_per_hour"] = round(
            sum(z["expected_opportunities_per_hour"] for z in dir_zones), 6
        )
        pair_rollup[f"expected_{key}_recyclable_opportunities_per_hour"] = round(
            sum(z["expected_recyclable_opportunities_per_hour"] for z in dir_zones), 6
        )
        pair_rollup[f"actual_{key}_trades_per_hour"] = round(
            sum(z["actual_trades_per_hour"] for z in dir_zones), 6
        )
        exp = pair_rollup[f"expected_{key}_opportunities_per_hour"]
        rec = pair_rollup[f"expected_{key}_recyclable_opportunities_per_hour"]
        act = pair_rollup[f"actual_{key}_trades_per_hour"]
        pair_rollup[f"{key}_utilization_ratio"] = round((act / exp), 6) if exp > 0 else 0.0
        pair_rollup[f"{key}_recycling_utilization_ratio"] = round((act / rec), 6) if rec > 0 else 0.0

    report = {
        "status": "INVALID_OPPORTUNITY_SANITY" if sanity_anomalies else "PASS",
        "mode": "session_potential",
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
                "entry_population_hash": sha256_file(entry_population_csv),
                "script_hash": sha256_file(Path(__file__)),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "runner": "session_potential.py",
        "inputs_hash": inputs_hash,
        "dataset_lock": str(dataset_lock),
        "truth_csv": str(truth_csv),
        "entry_population_csv": str(entry_population_csv),
        "report": str(output_dir / "session_potential_report.json"),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "session_potential_report.json").write_text(json.dumps(report, indent=2))
    (output_dir / "session_potential_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"status": "PASS", "node": report["node"], "output_dir": str(output_dir)}, indent=2))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--truth-csv", type=Path, required=True)
    ap.add_argument("--entry-population-csv", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    args = ap.parse_args()
    run(
        dataset_lock=args.dataset_lock,
        truth_csv=args.truth_csv,
        entry_population_csv=args.entry_population_csv,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
