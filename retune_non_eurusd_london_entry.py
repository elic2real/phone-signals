#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"
REQUIRED_TARGET_STAGE = [
    Path("target_entry_stage/target_contextual_v2/target_entry_classes.json"),
    Path("target_entry_stage/target_contextual_v2/target_entry_truth_table.csv"),
    Path("target_entry_stage/target_contextual_v2_targeted/target_entry_classes.json"),
]


def main() -> None:
    attempted = []
    for node_dir in sorted(OUTPUT_ROOT.glob("*__*__london")):
        parts = node_dir.name.split("__")
        if len(parts) != 3:
            continue
        pair, weekday, session = parts
        if pair == "EUR_USD":
            continue
        if not all((node_dir / rel).exists() for rel in REQUIRED_TARGET_STAGE):
            attempted.append({"node": node_dir.name, "status": "SKIP_MISSING_TARGET_STAGE"})
            continue
        lock_path = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
        if not lock_path.exists():
            attempted.append({"node": node_dir.name, "status": "SKIP_MISSING_LOCK"})
            continue
        cmd = [
            "python3",
            "rebuild_entry_and_downstream_fast.py",
            "--dataset-lock",
            str(lock_path),
            "--output-root",
            str(OUTPUT_ROOT),
            "--priority-mode",
            "winrate_first",
            "--entry-only",
        ]
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        attempted.append(
            {
                "node": node_dir.name,
                "status": "PASS" if proc.returncode == 0 else "FAIL",
                "returncode": proc.returncode,
                "stderr": (proc.stderr or "")[-2000:],
            }
        )

    report = {"mode": "winrate_first_non_eurusd_london", "attempted": attempted}
    report_path = ROOT / "retune_non_eurusd_london_entry_report.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
