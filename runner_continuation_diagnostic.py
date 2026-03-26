#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(".")
PARTIAL_TP = 1.5
BINS = [
    ("<=0", None, 0.0),
    ("0-1", 0.0, 1.0),
    ("1-2", 1.0, 2.0),
    ("2-4", 2.0, 4.0),
    ("4-6", 4.0, 6.0),
    ("6+", 6.0, None),
]


def bucketize(series: pd.Series) -> dict[str, int]:
    out: dict[str, int] = {}
    for label, low, high in BINS:
        if low is None:
            mask = series <= high
        elif high is None:
            mask = series > low
        else:
            mask = (series > low) & (series <= high)
        out[label] = int(mask.sum())
    return out


def summarize(df: pd.DataFrame) -> dict:
    continuation = (df["future_mfe"] - PARTIAL_TP).clip(lower=0.0)
    positive = continuation[continuation > 0]
    return {
        "row_count": int(len(df)),
        "partial_tp_pips": PARTIAL_TP,
        "continuation_exists_rate": float((continuation > 0).mean()) if len(df) else 0.0,
        "mean_continuation_pips": float(continuation.mean()) if len(df) else 0.0,
        "median_continuation_pips": float(continuation.median()) if len(df) else 0.0,
        "p75_continuation_pips": float(continuation.quantile(0.75)) if len(df) else 0.0,
        "p90_continuation_pips": float(continuation.quantile(0.90)) if len(df) else 0.0,
        "max_continuation_pips": float(continuation.max()) if len(df) else 0.0,
        "mean_positive_continuation_pips": float(positive.mean()) if len(positive) else 0.0,
        "histogram": bucketize(continuation),
        "future_mfe_mean": float(df["future_mfe"].mean()) if len(df) else 0.0,
        "future_mfe_median": float(df["future_mfe"].median()) if len(df) else 0.0,
        "future_mfe_p90": float(df["future_mfe"].quantile(0.90)) if len(df) else 0.0,
        "future_mfe_max": float(df["future_mfe"].max()) if len(df) else 0.0,
    }


def main() -> None:
    df = pd.read_csv(ROOT / "entry_outcomes.csv")
    report = {
        "global": summarize(df),
        "by_direction": {},
        "by_distance": {},
        "by_direction_distance": {},
    }
    for direction, g in df.groupby("direction"):
        report["by_direction"][direction] = summarize(g)
    for distance, g in df.groupby("distance"):
        report["by_distance"][f"{distance:g}"] = summarize(g)
    for (direction, distance), g in df.groupby(["direction", "distance"]):
        report["by_direction_distance"][f"{direction}_{distance:g}"] = summarize(g)

    (ROOT / "runner_continuation_histogram.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
