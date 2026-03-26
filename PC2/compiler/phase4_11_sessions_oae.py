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


PIP_THRESHOLD = 2.5


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def quantile_rank(value: float, sorted_values: list[float]) -> float:
    if not sorted_values:
        return 0.0
    lo = 0
    hi = len(sorted_values)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_values[mid] < value:
            lo = mid + 1
        else:
            hi = mid
    if len(sorted_values) == 1:
        return 1.0
    return lo / (len(sorted_values) - 1)


def load_phase1_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = parse_ts(row["timestamp"])
            common = {
                "timestamp_start": row["timestamp"],
                "price_start": float(row["price"]),
                "pair": row.get("pair", "EUR_USD"),
                "session": row["session"],
                "weekday": row["weekday"],
                "session_date": dt.date().isoformat(),
                "dt": dt,
                "target_distance": PIP_THRESHOLD,
            }
            if row["up_exists"] == "1":
                mfe = float(row["mfe_up_pips"])
                mae = float(row["mae_up_pips"])
                tau = int(float(row["tau_up_min"]))
                extension = max(0.0, mfe - PIP_THRESHOLD)
                efficiency = PIP_THRESHOLD / max(PIP_THRESHOLD, mfe + mae)
                speed = PIP_THRESHOLD / max(1, tau)
                risk_ratio = mae / PIP_THRESHOLD
                score = speed * 2.0 + efficiency * 2.0 + extension * 0.5 - risk_ratio
                rows.append(
                    {
                        **common,
                        "direction": "LONG",
                        "time_to_target": tau,
                        "max_mfe_pips": mfe,
                        "max_mae_pips": mae,
                        "speed": speed,
                        "efficiency": efficiency,
                        "extension": extension,
                        "risk_ratio": risk_ratio,
                        "composite_score": score,
                    }
                )
            if row["down_exists"] == "1":
                mfe = float(row["mfe_down_pips"])
                mae = float(row["mae_down_pips"])
                tau = int(float(row["tau_down_min"]))
                extension = max(0.0, mfe - PIP_THRESHOLD)
                efficiency = PIP_THRESHOLD / max(PIP_THRESHOLD, mfe + mae)
                speed = PIP_THRESHOLD / max(1, tau)
                risk_ratio = mae / PIP_THRESHOLD
                score = speed * 2.0 + efficiency * 2.0 + extension * 0.5 - risk_ratio
                rows.append(
                    {
                        **common,
                        "direction": "SHORT",
                        "time_to_target": tau,
                        "max_mfe_pips": mfe,
                        "max_mae_pips": mae,
                        "speed": speed,
                        "efficiency": efficiency,
                        "extension": extension,
                        "risk_ratio": risk_ratio,
                        "composite_score": score,
                    }
                )
    return rows


def assign_labels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scores = sorted(r["composite_score"] for r in rows)
    out = []
    for row in rows:
        q = quantile_rank(row["composite_score"], scores)
        if q >= 0.75:
            label = "GOOD"
        elif q < 0.35:
            label = "BAD"
        else:
            label = "NOISE"
        out.append({**row, "zone_label": label})
    return out


def summarize_feature(rows: list[dict[str, Any]], key: str) -> dict[str, float]:
    vals = [float(r[key]) for r in rows]
    return {
        "min": min(vals),
        "p10": percentile(vals, 0.10),
        "p25": percentile(vals, 0.25),
        "p50": percentile(vals, 0.50),
        "p75": percentile(vals, 0.75),
        "p90": percentile(vals, 0.90),
        "max": max(vals),
        "mean": mean(vals),
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label in ("GOOD", "BAD", "NOISE"):
        subset = [r for r in rows if r["zone_label"] == label]
        by_direction = Counter(r["direction"] for r in subset)
        by_pair = Counter(r["pair"] for r in subset)
        by_session = Counter(r["session"] for r in subset)
        out[label] = {
            "count": len(subset),
            "count_by_direction": {"LONG": by_direction.get("LONG", 0), "SHORT": by_direction.get("SHORT", 0)},
            "counts_by_pair": dict(by_pair),
            "counts_by_session": dict(by_session),
            "feature_quantiles": {
                "tau": summarize_feature(subset, "time_to_target"),
                "MFE": summarize_feature(subset, "max_mfe_pips"),
                "MAE": summarize_feature(subset, "max_mae_pips"),
                "efficiency": summarize_feature(subset, "efficiency"),
                "speed": summarize_feature(subset, "speed"),
                "extension": summarize_feature(subset, "extension"),
            } if subset else {},
        }
    return out


def build_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(r["zone_label"] for r in rows)
    return {
        "classification_method": "quantile_based_composite_score",
        "good_present": counts.get("GOOD", 0) > 0,
        "bad_present": counts.get("BAD", 0) > 0,
        "noise_present": counts.get("NOISE", 0) > 0,
        "label_counts": dict(counts),
        "overall_phase4_status": "PHASE4_PASS" if all(counts.get(x, 0) > 0 for x in ("GOOD", "BAD", "NOISE")) else "PHASE4_FAIL",
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    serializable = []
    for row in rows:
        clean = dict(row)
        clean["dt"] = clean["dt"].isoformat()
        serializable.append(clean)
    with path.open("w", newline="") as f:
        if serializable:
            writer = csv.DictWriter(f, fieldnames=list(serializable[0].keys()))
            writer.writeheader()
            writer.writerows(serializable)
        else:
            fieldnames = [
                "timestamp_start",
                "price_start",
                "pair",
                "session",
                "weekday",
                "session_date",
                "dt",
                "target_distance",
                "direction",
                "time_to_target",
                "max_mfe_pips",
                "max_mae_pips",
                "speed",
                "efficiency",
                "extension",
                "risk_ratio",
                "composite_score",
                "zone_label",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 4 OAE on the 11-session EUR/USD Monday London dataset")
    parser.add_argument("--phase1-csv", default="phase1_11_sessions_outputs/opportunity_map_raw.csv")
    parser.add_argument("--output-dir", default="phase4_11_sessions_outputs")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_phase1_rows(Path(args.phase1_csv))
    labeled = assign_labels(rows)

    write_csv(output_dir / "opportunity_zones_labeled.csv", labeled)
    (output_dir / "zone_label_summary.json").write_text(json.dumps(build_summary(labeled), indent=2))
    (output_dir / "zone_label_audit.json").write_text(json.dumps(build_audit(labeled), indent=2))

    print(f"Classified {len(labeled)} directional opportunities")
    print(f"Wrote: {output_dir / 'opportunity_zones_labeled.csv'}")
    print(f"Wrote: {output_dir / 'zone_label_summary.json'}")
    print(f"Wrote: {output_dir / 'zone_label_audit.json'}")


if __name__ == "__main__":
    main()
