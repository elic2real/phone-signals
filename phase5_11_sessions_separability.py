#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean, median
from typing import Any


FEATURES = [
    ("time_to_target", "tau"),
    ("max_mfe_pips", "MFE"),
    ("max_mae_pips", "MAE"),
    ("efficiency", "efficiency"),
    ("speed", "speed"),
    ("extension", "extension"),
]


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


def ks_statistic(a: list[float], b: list[float]) -> float:
    a = sorted(a)
    b = sorted(b)
    vals = sorted(set(a + b))
    i = j = 0
    best = 0.0
    for v in vals:
        while i < len(a) and a[i] <= v:
            i += 1
        while j < len(b) and b[j] <= v:
            j += 1
        da = i / len(a) if a else 0.0
        db = j / len(b) if b else 0.0
        best = max(best, abs(da - db))
    return best


def iqr_range(values: list[float]) -> tuple[float, float]:
    return percentile(values, 0.25), percentile(values, 0.75)


def iqr_overlap(a: list[float], b: list[float]) -> float:
    a_lo, a_hi = iqr_range(a)
    b_lo, b_hi = iqr_range(b)
    lo = max(a_lo, b_lo)
    hi = min(a_hi, b_hi)
    return max(0.0, hi - lo)


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["zone_label"] not in {"GOOD", "BAD"}:
                continue
            rows.append(row)
    return rows


def summarize_feature(good: list[float], bad: list[float]) -> dict[str, Any]:
    if not good or not bad:
        return {
            "good_count": len(good),
            "bad_count": len(bad),
            "ks_statistic": 0.0,
            "iqr_overlap": 0.0,
            "median_difference": None,
            "mean_difference": None,
            "good_iqr": None,
            "bad_iqr": None,
            "good_median": None,
            "bad_median": None,
            "status": "INSUFFICIENT_DATA",
        }
    return {
        "good_count": len(good),
        "bad_count": len(bad),
        "ks_statistic": ks_statistic(good, bad),
        "iqr_overlap": iqr_overlap(good, bad),
        "median_difference": median(good) - median(bad),
        "mean_difference": mean(good) - mean(bad),
        "good_iqr": list(iqr_range(good)),
        "bad_iqr": list(iqr_range(bad)),
        "good_median": median(good),
        "bad_median": median(bad),
        "status": "OK",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 5 separability proof on the 11-session EUR/USD Monday London dataset")
    parser.add_argument("--labeled-csv", default="phase4_11_sessions_outputs/opportunity_zones_labeled.csv")
    parser.add_argument("--output-dir", default="phase5_11_sessions_outputs")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(Path(args.labeled_csv))
    feature_report: dict[str, Any] = {}
    strong_features = 0
    for source_name, report_name in FEATURES:
        good = [float(r[source_name]) for r in rows if r["zone_label"] == "GOOD"]
        bad = [float(r[source_name]) for r in rows if r["zone_label"] == "BAD"]
        feature_report[report_name] = summarize_feature(good, bad)
        if feature_report[report_name]["ks_statistic"] >= 0.35:
            strong_features += 1

    good_count = sum(1 for r in rows if r["zone_label"] == "GOOD")
    bad_count = sum(1 for r in rows if r["zone_label"] == "BAD")
    if good_count == 0 or bad_count == 0:
        status = "INSUFFICIENT_DATA"
    else:
        status = "PASS" if strong_features >= 3 else "FAIL"
    out = {
        "separability_status": status,
        "good_count": good_count,
        "bad_count": bad_count,
        "feature_by_feature_separation": feature_report,
        "strong_feature_count": strong_features,
        "threshold_for_strong_feature": 0.35,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "zone_label_separability.json").write_text(json.dumps(out, indent=2))

    print(f"Computed separability on {out['good_count']} GOOD vs {out['bad_count']} BAD rows")
    print(f"Strong features: {strong_features}")
    print(f"Wrote: {output_dir / 'zone_label_separability.json'}")


if __name__ == "__main__":
    main()
