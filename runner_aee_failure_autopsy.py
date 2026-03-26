#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean

import run_aee_stage_compiler as aee


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "compiled_aee_stage_11_sessions_canonical"
OUT_DIR = ROOT / "compiled_aee_runner_failure_autopsy_11_sessions"
RUNNER_TARGETS = {"4.5", "6.0", "7.0", "8.0", "9.0", "11.0", "13.0", "15.0"}


def load_json(path: Path):
    return json.loads(path.read_text())


def filter_runner_trade_rows(rows: list[dict]) -> list[dict]:
    return [r for r in rows if str(float(r["target_distance"])) in RUNNER_TARGETS]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    replay = load_json(INPUT_DIR / "aee_replay" / "target_selective_aee.json")
    runner_rows = filter_runner_trade_rows(replay["trade_rows"])
    failures = [r for r in runner_rows if r["underperformed_static"]]

    by_label = Counter()
    by_action = Counter()
    state_regions = defaultdict(list)
    by_target = defaultdict(int)
    by_direction = defaultdict(int)

    for row in failures:
        label = aee.derive_failure_label(row["first_aee_action"], float(row["aee_pips"]), float(row["static_pips"]))
        row["failure_label"] = label
        by_label[label] += 1
        by_action[row["first_aee_action"]] += 1
        by_target[str(float(row["target_distance"]))] += 1
        by_direction[row["direction"]] += 1
        state_regions[f"{row['first_aee_action']}|profit"].append(float(row["profit_at_action"]))
        state_regions[f"{row['first_aee_action']}|missed"].append(float(row["missed_extension_pips"]))
        state_regions[f"{row['first_aee_action']}|avoidable"].append(float(row["avoidable_loss_pips"]))

    summary = {
        "trade_count": len(runner_rows),
        "failure_count": len(failures),
        "failure_rate": len(failures) / len(runner_rows) if runner_rows else 0.0,
        "by_label": dict(by_label),
        "by_action": dict(by_action),
        "by_target_distance": dict(by_target),
        "by_direction": dict(by_direction),
        "state_regions": {
            k: {"mean": mean(v) if v else 0.0, "min": min(v) if v else 0.0, "max": max(v) if v else 0.0}
            for k, v in state_regions.items()
        },
    }
    (OUT_DIR / "runner_aee_failure_autopsy.json").write_text(json.dumps(failures, indent=2))
    (OUT_DIR / "runner_aee_failure_autopsy_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
