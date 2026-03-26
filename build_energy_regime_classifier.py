#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import build_entry_trigger_state_machine as trig


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def classify_regime(row: dict[str, Any]) -> str:
    macro_dir = float(row["macro_dir_score"])
    micro_dir = float(row["micro_dir_score"])
    compression = float(row["compression_score"])
    release = float(row["release_quality_score"])
    exhaustion = float(row["exhaustion_score"])
    noise = float(row["noise_score"])
    budget = float(row["remaining_budget_score"])

    if noise >= 0.62:
        return "CHOP"
    if exhaustion >= 0.58 and budget < 0.42:
        return "EXHAUSTION"
    if release >= 0.62 and compression >= 0.52 and budget >= 0.55:
        return "FRESH_RELEASE"
    if macro_dir >= 0.56 and micro_dir >= 0.56 and budget >= 0.52 and exhaustion < 0.56:
        return "TREND_CONTINUATION"
    if budget >= 0.45 and exhaustion < 0.62:
        return "LATE_CONTINUATION"
    return "TRANSITION"


def summarize(rows: list[dict[str, Any]], population: list[dict[str, Any]]) -> dict[str, Any]:
    return trig.summarize_replay(rows, population)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--context-stream-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_csv(args.context_stream_csv)
    rules = json.loads(args.rules_json.read_text())["path_classes"]

    enriched: list[dict[str, Any]] = []
    for row in rows:
        regime = classify_regime(row)
        enriched.append({**row, "energy_regime": regime})

    regime_counts = Counter(r["energy_regime"] for r in enriched)
    selected_rows: list[dict[str, Any]] = []
    for row in enriched:
        for rule in rules:
            if trig.match_rule(row, rule):
                selected_rows.append(
                    {
                        **row,
                        "rule_key": f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}",
                    }
                )
                break

    matrix: list[dict[str, Any]] = []
    by_rule_regime: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in selected_rows:
        by_rule_regime[(row["rule_key"], row["energy_regime"])].append(row)

    for rule in rules:
        key = f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"
        for regime in ("FRESH_RELEASE", "TREND_CONTINUATION", "LATE_CONTINUATION", "TRANSITION", "EXHAUSTION", "CHOP"):
            bucket = by_rule_regime.get((key, regime), [])
            if not bucket:
                continue
            replay = summarize(bucket, enriched)
            matrix.append(
                {
                    "rule_key": key,
                    "direction": rule["direction"],
                    "quarter": rule["quarter"],
                    "path_class_name": rule["path_class_name"],
                    "energy_regime": regime,
                    "trade_count": replay["trade_count"],
                    "win_rate": replay["win_rate"],
                    "expectancy": replay["expectancy"],
                    "avg_R": replay["avg_R"],
                    "pips_per_hour": replay["pips_per_hour"],
                    "good_capture": replay["good_capture"],
                    "bad_trigger": replay["bad_trigger"],
                    "noise_trigger": replay["noise_trigger"],
                }
            )

    matrix.sort(key=lambda r: (r["rule_key"], -r["pips_per_hour"]))

    allowed_states: dict[str, list[str]] = {}
    for rule in rules:
        key = f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"
        candidates = [r for r in matrix if r["rule_key"] == key]
        keep = []
        for row in sorted(candidates, key=lambda r: (r["expectancy"], r["pips_per_hour"]), reverse=True):
            if (
                row["expectancy"] > 0.10
                and row["good_capture"] > row["bad_trigger"]
                and row["noise_trigger"] <= 0.10
            ):
                keep.append(row["energy_regime"])
        allowed_states[key] = keep

    report = {
        "regime_counts_full_stream": dict(regime_counts),
        "selected_trade_count": len(selected_rows),
        "rule_allowed_regimes": allowed_states,
        "top_rule_regime_rows": sorted(matrix, key=lambda r: r["pips_per_hour"], reverse=True)[:25],
    }

    write_csv(out_dir / "full_stream_regimes.csv", enriched, list(enriched[0].keys()) if enriched else ["timestamp"])
    write_csv(out_dir / "selected_trade_regimes.csv", selected_rows, list(selected_rows[0].keys()) if selected_rows else ["timestamp"])
    write_csv(out_dir / "island_regime_matrix.csv", matrix, list(matrix[0].keys()) if matrix else ["rule_key"])
    (out_dir / "energy_regime_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({
        "regime_counts": report["regime_counts_full_stream"],
        "selected_trade_count": len(selected_rows),
        "rule_count": len(rules),
    }, indent=2))


if __name__ == "__main__":
    main()
