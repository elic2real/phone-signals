from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_JSON = ROOT / "market_grid_tracker.json"
OUTPUT_CSV = ROOT / "market_grid_tracker.csv"

SESSIONS = ["ASIA", "LONDON", "NY"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday"]


def load_pair_universe() -> list[str]:
    manifests = [
        ROOT / "data_tape_oanda_m5_15_stitched" / "_manifest.json",
        ROOT / "data_tape_stitched_15_full" / "_manifest.json",
        ROOT / "data_tape_stitched_15" / "_manifest.json",
    ]
    for manifest in manifests:
        if manifest.exists():
            obj = json.loads(manifest.read_text())
            pairs = []
            for file_name in obj.get("files", []):
                if file_name.startswith("pair="):
                    pairs.append(file_name.split("/", 1)[0].split("=", 1)[1])
            if pairs:
                return sorted(set(pairs))
    return []


def load_current_base_node() -> dict[str, str]:
    lock_path = ROOT / "dataset_lock.json"
    if not lock_path.exists():
        return {}
    obj = json.loads(lock_path.read_text())
    return {
        "pair": str(obj.get("pair", "")),
        "session": str(obj.get("session", "")).upper(),
        "weekday": str(obj.get("weekday", "")).lower(),
    }


def load_solved_quarter_nodes() -> set[tuple[str, str, str, str]]:
    quarter_report = ROOT / "compiled_ceiling_quarters.json"
    if not quarter_report.exists():
        return set()
    obj = json.loads(quarter_report.read_text())
    node = obj.get("node", {})
    pair = str(node.get("pair", ""))
    session = str(node.get("session", "")).upper()
    weekday = str(node.get("weekday", "")).lower()
    solved = set()
    for quarter, payload in obj.get("combined", {}).items():
        if payload.get("total_trades", 0) > 0:
            solved.add((pair, weekday, session, quarter))
    return solved


def scan_surrogate_nodes() -> dict[tuple[str, str, str], list[Path]]:
    root = ROOT / "calibration" / "candidates" / "surrogate"
    pattern = re.compile(
        r"^(?P<pair>[A-Z_]+)_(?P<session>ASIA|LONDON|NY)_(?P<quarter>Q[1-4])_trial_\d+\.json$"
    )
    found: dict[tuple[str, str, str], list[Path]] = defaultdict(list)
    if not root.exists():
        return found
    for path in root.glob("*.json"):
        match = pattern.match(path.name)
        if not match:
            continue
        key = (match["pair"], match["session"], match["quarter"])
        found[key].append(path)
    return found


def main() -> None:
    pairs = load_pair_universe()
    base_node = load_current_base_node()
    solved_quarter_nodes = load_solved_quarter_nodes()
    surrogate_nodes = scan_surrogate_nodes()

    rows = []
    pair_data_present = {
        pair: (ROOT / "data_tape_oanda_m5_15_stitched" / f"pair={pair}" / "stitched.parquet").exists()
        for pair in pairs
    }

    for pair in pairs:
        for weekday in WEEKDAYS:
            for session in SESSIONS:
                for quarter in QUARTERS:
                    surrogate_key = (pair, session, quarter)
                    trials = len(surrogate_nodes.get(surrogate_key, []))
                    is_base = (
                        pair == base_node.get("pair")
                        and session == base_node.get("session")
                        and weekday == base_node.get("weekday")
                    )
                    is_solved_quarter = (pair, weekday, session, quarter) in solved_quarter_nodes
                    if is_solved_quarter:
                        status = "SOLVED_BASE"
                    elif is_base:
                        status = "BASE_SESSION_ONLY"
                    elif trials > 0:
                        status = "PARTIAL_SURROGATE"
                    elif pair_data_present.get(pair):
                        status = "DATA_ONLY"
                    else:
                        status = "MISSING_DATA"
                    rows.append(
                        {
                            "pair": pair,
                            "weekday": weekday,
                            "session": session,
                            "quarter": quarter,
                            "data_present": pair_data_present.get(pair, False),
                            "surrogate_trial_count": trials,
                            "status": status,
                        }
                    )

    counts = defaultdict(int)
    for row in rows:
        counts[row["status"]] += 1

    by_pair = {}
    for pair in pairs:
        pair_rows = [row for row in rows if row["pair"] == pair]
        by_pair[pair] = {
            "total_nodes": len(pair_rows),
            "solved_base_nodes": sum(1 for row in pair_rows if row["status"] == "SOLVED_BASE"),
            "base_session_only_nodes": sum(1 for row in pair_rows if row["status"] == "BASE_SESSION_ONLY"),
            "partial_surrogate_nodes": sum(1 for row in pair_rows if row["status"] == "PARTIAL_SURROGATE"),
            "data_only_nodes": sum(1 for row in pair_rows if row["status"] == "DATA_ONLY"),
            "missing_data_nodes": sum(1 for row in pair_rows if row["status"] == "MISSING_DATA"),
        }

    summary = {
        "expected_grid": {
            "pair_count": len(pairs),
            "weekday_count": len(WEEKDAYS),
            "session_count": len(SESSIONS),
            "quarter_count": len(QUARTERS),
            "total_nodes": len(rows),
        },
        "status_counts": dict(sorted(counts.items())),
        "current_base_node": base_node,
        "pairs": pairs,
        "pair_summary": by_pair,
        "completion": {
            "solved_base_nodes": counts["SOLVED_BASE"],
            "base_session_only_nodes": counts["BASE_SESSION_ONLY"],
            "partial_surrogate_nodes": counts["PARTIAL_SURROGATE"],
            "covered_nodes_any_form": counts["SOLVED_BASE"] + counts["BASE_SESSION_ONLY"] + counts["PARTIAL_SURROGATE"],
            "coverage_ratio_any_form": (
                (counts["SOLVED_BASE"] + counts["BASE_SESSION_ONLY"] + counts["PARTIAL_SURROGATE"]) / len(rows) if rows else 0.0
            ),
        },
    }

    OUTPUT_JSON.write_text(json.dumps(summary, indent=2))
    with OUTPUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["pair", "weekday", "session", "quarter", "data_present", "surrogate_trial_count", "status"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
