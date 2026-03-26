#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from optimize_target_entry_classes_pph_static_cached import rule_applies, load_csv, load_json


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth-csv", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_truth_table.csv", type=Path)
    ap.add_argument("--rules-json", default="compiled_target_entry_classes_pph_static_cached_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--output-dir", default="compiled_target_timeout_populations_11_sessions", type=Path)
    args = ap.parse_args()

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    rows = load_csv(args.truth_csv)
    rules = load_json(args.rules_json)["entry_classes"]

    grouped_rules: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for rule in rules:
        grouped_rules[(rule["direction"], float(rule["target_distance"]))].append(rule)

    grouped_rows: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped_rows[(row["direction_assumed"], float(row["target_distance"]))].append(row)

    summary = []
    all_timeout_rows: list[dict[str, Any]] = []
    for key, class_rows in sorted(grouped_rows.items()):
        direction, target = key
        class_rules = grouped_rules.get(key, [])
        selected = []
        for row in class_rows:
            for rule in class_rules:
                if rule_applies(row, rule):
                    selected.append(row)
                    break
        tp = sl = to = 0
        timeout_rows = []
        for row in selected:
            pips = float(row["static_pips"])
            if abs(pips - target) < 1e-9:
                tp += 1
            elif abs(pips + target) < 1e-9:
                sl += 1
            else:
                to += 1
                timeout_row = dict(row)
                timeout_row["class_direction"] = direction
                timeout_row["class_target_distance"] = target
                timeout_rows.append(timeout_row)
                all_timeout_rows.append(timeout_row)
        summary.append(
            {
                "direction": direction,
                "target_distance": target,
                "selected_trades": len(selected),
                "tp_hits": tp,
                "sl_hits": sl,
                "timeouts": to,
            }
        )
        with (out / f"{direction.lower()}_{str(target).replace('.0','')}_timeouts.csv").open("w", newline="") as f:
            if timeout_rows:
                writer = csv.DictWriter(f, fieldnames=list(timeout_rows[0].keys()))
                writer.writeheader()
                writer.writerows(timeout_rows)
            else:
                writer = csv.writer(f)
                writer.writerow(["no_timeouts"])

    with (out / "all_timeouts.csv").open("w", newline="") as f:
        if all_timeout_rows:
            writer = csv.DictWriter(f, fieldnames=list(all_timeout_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_timeout_rows)
        else:
            writer = csv.writer(f)
            writer.writerow(["no_timeouts"])

    report = {
        "summary": summary,
        "total_timeouts": sum(r["timeouts"] for r in summary),
    }
    (out / "timeout_population_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
