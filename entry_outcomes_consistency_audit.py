from __future__ import annotations

import csv
import json
import random
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def load_selected_rows() -> dict[tuple[str, str], dict]:
    selected = {}
    report = json.loads((ROOT / "entry_metric_ceiling_report_unified.json").read_text())
    for side in ("long", "short"):
        for mode in ("harvester", "runner"):
            for _dist, payload in report["results"][side][mode].items():
                profit = payload.get("profit_ceiling")
                if not profit:
                    continue
                for row in profit.get("rows", []):
                    selected[(mode, row["cluster_id"], row["timestamp_start"])] = row
    return selected


def main() -> None:
    selected = load_selected_rows()
    mismatches = []
    matches = 0
    seen = 0
    samples = []
    with (ROOT / "entry_outcomes.csv").open() as f:
        for row in csv.DictReader(f):
            for mode, profit_col, reason_col in (
                ("harvester", "harvester_profit", "harvester_reason"),
                ("runner", "runner_static_profit", "runner_static_reason"),
            ):
                key = (mode, row["cluster_id"], row["timestamp"])
                if key not in selected:
                    continue
                seen += 1
                sel = selected[key]
                outcome_pips = float(row[profit_col])
                selected_pips = float(sel["pips"])
                outcome_reason = row.get(reason_col, "")
                selected_reason = str(sel.get("reason", ""))
                if abs(outcome_pips - selected_pips) > 1e-9 or outcome_reason != selected_reason:
                    mismatches.append(
                        {
                            "mode": mode,
                            "cluster_id": row["cluster_id"],
                            "timestamp": row["timestamp"],
                            "outcome_pips": outcome_pips,
                            "selected_pips": selected_pips,
                            "outcome_reason": outcome_reason,
                            "selected_reason": selected_reason,
                        }
                    )
                else:
                    matches += 1
                    if len(samples) < 20 and random.random() < 0.2:
                        samples.append(
                            {
                                "mode": mode,
                                "cluster_id": row["cluster_id"],
                                "timestamp": row["timestamp"],
                                "pips": outcome_pips,
                                "reason": outcome_reason,
                            }
                        )
    verdict = "OUTCOME_COMPILATION_INVALID" if mismatches else "PASS"
    audit = {
        "verdict": verdict,
        "matched_rows_checked": seen,
        "exact_matches": matches,
        "mismatch_count": len(mismatches),
        "sample_exact_matches": samples,
        "sample_mismatches": mismatches[:20],
    }
    (ROOT / "entry_outcomes_consistency_audit.json").write_text(json.dumps(audit, indent=2))
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
