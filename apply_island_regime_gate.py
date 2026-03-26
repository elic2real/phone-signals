#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
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


def rule_key(rule: dict[str, Any]) -> str:
    return f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"


def replay(rows: list[dict[str, Any]], rules: list[dict[str, Any]], allowed: dict[str, list[str]] | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        for rule in rules:
            if not trig.match_rule(row, rule):
                continue
            if allowed is not None:
                key = rule_key(rule)
                regimes = allowed.get(key, [])
                if regimes and row["energy_regime"] not in regimes:
                    continue
            selected.append(row)
            break
    return selected, trig.summarize_replay(selected, rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--context-stream-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--regime-report-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    rows = load_csv(args.context_stream_csv)
    rules = json.loads(args.rules_json.read_text())["path_classes"]
    regime_report = json.loads(args.regime_report_json.read_text())
    allowed = regime_report["rule_allowed_regimes"]
    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    base_rows, base_replay = replay(rows, rules, None)
    gated_rows, gated_replay = replay(rows, rules, allowed)

    report = {
        "before": base_replay,
        "after": gated_replay,
        "delta": {
            "trade_count": gated_replay["trade_count"] - base_replay["trade_count"],
            "win_rate": gated_replay["win_rate"] - base_replay["win_rate"],
            "expectancy": gated_replay["expectancy"] - base_replay["expectancy"],
            "avg_R": gated_replay["avg_R"] - base_replay["avg_R"],
            "pips_per_hour": gated_replay["pips_per_hour"] - base_replay["pips_per_hour"],
            "good_capture": gated_replay["good_capture"] - base_replay["good_capture"],
            "bad_trigger": gated_replay["bad_trigger"] - base_replay["bad_trigger"],
            "noise_trigger": gated_replay["noise_trigger"] - base_replay["noise_trigger"],
        },
        "allowed_regimes": allowed,
    }

    write_csv(out_dir / "before_island_regime_gate_population.csv", base_rows, list(base_rows[0].keys()) if base_rows else ["timestamp"])
    write_csv(out_dir / "after_island_regime_gate_population.csv", gated_rows, list(gated_rows[0].keys()) if gated_rows else ["timestamp"])
    (out_dir / "island_regime_gate_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
