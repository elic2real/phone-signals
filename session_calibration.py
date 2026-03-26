#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
SESSION_HOURS = 88.0
MIN_ZONE_OPPORTUNITIES = 10
MAX_ZONE_OPPORTUNITIES = 10000
SYMMETRIC_BREAK_EVEN = 0.505
WEAK_EDGE_WARNING = 0.51


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


def classify_zone(
    opp: int,
    selected: int,
    capture: float,
    win_rate: float,
    avg_pips: float,
    downstream_wr: float | None,
    downstream_avg_pips: float | None,
    downstream_trades: int,
    symmetric_break_even: float,
    weak_edge_warning: float,
) -> tuple[str, str]:
    effective_wr = downstream_wr if downstream_wr is not None and downstream_trades > 0 else win_rate
    effective_pips = downstream_avg_pips if downstream_avg_pips is not None and downstream_trades > 0 else avg_pips

    if opp <= 0:
        return "stabilize", "no_active_coverage"
    if selected <= 0 and opp >= 25:
        return "repair", "no_selected_entries_high_opportunity"
    if selected <= 0:
        return "stabilize", "no_selected_entries"
    if selected < 8 and opp >= 25 and capture <= 0.01:
        return "repair", "ultra_thin_high_opportunity"
    if selected < 8:
        return "stabilize", "thin_target_regime"
    if effective_wr < symmetric_break_even:
        return "repair", "below_symmetric_break_even"
    if effective_wr < weak_edge_warning:
        return "refine", "weak_edge_warning"
    if selected >= 80 and effective_wr >= 0.52:
        return "refine", "active_and_positive"
    if selected >= 40 and effective_wr >= 0.58 and capture < 0.06 and effective_pips > 0:
        return "expand", "good_quality_under_captured"
    return "refine", "usable_but_needs_quality_lift"


def run(
    dataset_lock: Path,
    truth_csv: Path,
    entry_population_csv: Path,
    output_dir: Path,
    trade_rows_json: Path | None = None,
    symmetric_break_even: float = SYMMETRIC_BREAK_EVEN,
    weak_edge_warning: float = WEAK_EDGE_WARNING,
) -> dict[str, Any]:
    lock = jload(dataset_lock)
    truth = pd.read_csv(truth_csv)
    population = pd.read_csv(entry_population_csv)
    trades = pd.read_json(trade_rows_json) if trade_rows_json and trade_rows_json.exists() else None

    if "direction_assumed" in truth.columns:
        truth["dir"] = truth["direction_assumed"]
    else:
        truth["dir"] = truth["direction"]
    if "direction_assumed" in population.columns:
        population["dir"] = population["direction_assumed"]
    else:
        population["dir"] = population["direction"]
    truth["target"] = truth["target_distance"].astype(float)
    population["target"] = population["target_distance"].astype(float)

    opp = truth.groupby(["quarter", "dir", "target"]).size().rename("opp").reset_index()
    selected = population.groupby(["quarter", "dir", "target"]).size().rename("selected").reset_index()
    entry = (
        population.groupby(["quarter", "dir", "target"])
        .agg(
            entry_win_rate=("static_pips", lambda s: float((pd.Series(s) > 0).mean())),
            entry_avg_pips=("static_pips", "mean"),
            entry_total_pips=("static_pips", "sum"),
        )
        .reset_index()
    )
    merged = opp.merge(selected, how="outer").merge(entry, how="outer").fillna(0)
    merged["capture_rate"] = merged["selected"] / merged["opp"].where(merged["opp"] > 0, 1)
    merged["opportunity_density_per_hour"] = merged["opp"] / SESSION_HOURS
    merged["selected_density_per_hour"] = merged["selected"] / SESSION_HOURS

    if trades is not None and not trades.empty:
        trades["dir"] = trades["direction"]
        trades["target"] = trades["target_distance"].astype(float)
        downstream = (
            trades.groupby(["quarter", "dir", "target"])
            .agg(
                downstream_trade_count=("trade_id", "count"),
                downstream_win_rate=("aee_pips", lambda s: float((pd.Series(s) > 0).mean())),
                downstream_avg_pips=("aee_pips", "mean"),
                downstream_total_pips=("aee_pips", "sum"),
            )
            .reset_index()
        )
        merged = merged.merge(downstream, how="left").fillna(0)
        merged["downstream_density_per_hour"] = merged["downstream_trade_count"] / SESSION_HOURS
    else:
        merged["downstream_trade_count"] = 0
        merged["downstream_win_rate"] = 0.0
        merged["downstream_avg_pips"] = 0.0
        merged["downstream_total_pips"] = 0.0
        merged["downstream_density_per_hour"] = 0.0

    zones: list[dict[str, Any]] = []
    sanity_anomalies: list[dict[str, Any]] = []
    for _, row in merged.sort_values(["dir", "target", "quarter"]).iterrows():
        opp_count = int(row["opp"])
        if opp_count < MIN_ZONE_OPPORTUNITIES or opp_count > MAX_ZONE_OPPORTUNITIES:
            sanity_anomalies.append(
                {
                    "quarter": row["quarter"],
                    "direction": row["dir"],
                    "target_distance": float(row["target"]),
                    "opportunity_count": opp_count,
                    "min_allowed": MIN_ZONE_OPPORTUNITIES,
                    "max_allowed": MAX_ZONE_OPPORTUNITIES,
                }
            )
        action, reason = classify_zone(
            opp=opp_count,
            selected=int(row["selected"]),
            capture=float(row["capture_rate"]),
            win_rate=float(row["entry_win_rate"]),
            avg_pips=float(row["entry_avg_pips"]),
            downstream_wr=float(row["downstream_win_rate"]) if int(row["downstream_trade_count"]) > 0 else None,
            downstream_avg_pips=float(row["downstream_avg_pips"]) if int(row["downstream_trade_count"]) > 0 else None,
            downstream_trades=int(row["downstream_trade_count"]),
            symmetric_break_even=symmetric_break_even,
            weak_edge_warning=weak_edge_warning,
        )
        zones.append(
            {
                "quarter": row["quarter"],
                "direction": row["dir"],
                "target_distance": float(row["target"]),
                "opportunity_count": int(row["opp"]),
                "opportunity_density_per_hour": float(row["opportunity_density_per_hour"]),
                "selected_count": int(row["selected"]),
                "selected_density_per_hour": float(row["selected_density_per_hour"]),
                "capture_rate": float(row["capture_rate"]),
                "entry_win_rate": float(row["entry_win_rate"]),
                "entry_avg_pips": float(row["entry_avg_pips"]),
                "entry_total_pips": float(row["entry_total_pips"]),
                "downstream_trade_count": int(row["downstream_trade_count"]),
                "downstream_density_per_hour": float(row["downstream_density_per_hour"]),
                "downstream_win_rate": float(row["downstream_win_rate"]),
                "downstream_avg_pips": float(row["downstream_avg_pips"]),
                "downstream_total_pips": float(row["downstream_total_pips"]),
                "action": action,
                "reason": reason,
            }
        )

    pair_summary = (
        population.groupby("dir")
        .agg(
            selected_count=("timestamp", "count"),
            entry_win_rate=("static_pips", lambda s: float((pd.Series(s) > 0).mean())),
            entry_total_pips=("static_pips", "sum"),
        )
        .reset_index()
        .to_dict(orient="records")
    )
    for row in pair_summary:
        row["selected_density_per_hour"] = float(row["selected_count"]) / SESSION_HOURS
        row["trades_per_hour"] = row["selected_density_per_hour"]

    directional_tph = {
        row["dir"]: float(row["selected_count"]) / SESSION_HOURS
        for row in pair_summary
    }
    action_counts: dict[str, int] = {}
    for zone in zones:
        action_counts[zone["action"]] = action_counts.get(zone["action"], 0) + 1

    report = {
        "status": "INVALID_OPPORTUNITY_SANITY" if sanity_anomalies else "PASS",
        "mode": "session_calibration",
        "timestamp": iso_now(),
        "node": {
            "pair": lock.get("pair"),
            "weekday": lock.get("weekday"),
            "session": lock.get("session"),
        },
        "symmetric_break_even": symmetric_break_even,
        "weak_edge_warning": weak_edge_warning,
        "long_trades_per_hour": directional_tph.get("LONG", 0.0),
        "short_trades_per_hour": directional_tph.get("SHORT", 0.0),
        "pair_summary": pair_summary,
        "action_counts": action_counts,
        "zones": zones,
        "sanity_anomalies": sanity_anomalies,
    }
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "dataset_lock_hash": sha256_file(dataset_lock),
                "truth_csv_hash": sha256_file(truth_csv),
                "entry_population_hash": sha256_file(entry_population_csv),
                "trade_rows_hash": sha256_file(trade_rows_json) if trade_rows_json and trade_rows_json.exists() else None,
                "script_hash": sha256_file(Path(__file__)),
                "symmetric_break_even": symmetric_break_even,
                "weak_edge_warning": weak_edge_warning,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "runner": "session_calibration.py",
        "inputs_hash": inputs_hash,
        "dataset_lock": str(dataset_lock),
        "truth_csv": str(truth_csv),
        "entry_population_csv": str(entry_population_csv),
        "trade_rows_json": str(trade_rows_json) if trade_rows_json else None,
        "report": str(output_dir / "session_calibration_report.json"),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "session_calibration_report.json").write_text(json.dumps(report, indent=2))
    (output_dir / "session_calibration_manifest.json").write_text(json.dumps(manifest, indent=2))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--truth-csv", type=Path, required=True)
    ap.add_argument("--entry-population-csv", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--trade-rows-json", type=Path)
    ap.add_argument("--symmetric-break-even", type=float, default=SYMMETRIC_BREAK_EVEN)
    ap.add_argument("--weak-edge-warning", type=float, default=WEAK_EDGE_WARNING)
    args = ap.parse_args()
    report = run(
        dataset_lock=args.dataset_lock,
        truth_csv=args.truth_csv,
        entry_population_csv=args.entry_population_csv,
        output_dir=args.output_dir,
        trade_rows_json=args.trade_rows_json,
        symmetric_break_even=args.symmetric_break_even,
        weak_edge_warning=args.weak_edge_warning,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "node": report["node"],
                "action_counts": report["action_counts"],
                "output_dir": str(args.output_dir),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
