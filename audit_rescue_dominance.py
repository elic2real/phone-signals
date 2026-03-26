#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def classify_rescue_signature(best_row: dict[str, Any], rescue_rule_ids: list[str]) -> str | None:
    direction = str(best_row.get("direction", "")).upper()
    try:
        target = float(best_row.get("target_distance", 0.0) or 0.0)
    except Exception:
        target = 0.0
    trade_count = int(best_row.get("trade_count", 0) or 0)
    if direction == "LONG" and target >= 11.0 and rescue_rule_ids:
        return "toxic_long_high_rescue"
    if direction == "SHORT" and target <= 1.5 and rescue_rule_ids and trade_count >= 100:
        return "short_scalp_rescue_dominant"
    return None


def best_class_row(summary: list[dict[str, Any]]) -> dict[str, Any] | None:
    best = None
    best_score = None
    for row in summary:
        if not isinstance(row, dict):
            continue
        score = (
            int(row.get("trade_count", 0) or 0),
            float(row.get("tp_hit_rate", 0.0) or 0.0),
            float(row.get("total_pips", 0.0) or 0.0),
        )
        if best_score is None or score > best_score:
            best_score = score
            best = row
    return best


def audit(compiled_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for perf_path in sorted(compiled_root.glob("*__*__*/session_performance_check/session_performance_check_report.json")):
        try:
            perf = load_json(perf_path)
        except Exception:
            continue
        node_dir = perf_path.parents[1]
        class_report_path = node_dir / "target_entry_no_timeouts" / "target_entry_class_report.json"
        if not class_report_path.exists():
            continue
        try:
            class_report = load_json(class_report_path)
        except Exception:
            continue
        best_row = best_class_row(class_report.get("summary", []) or [])
        if not best_row:
            continue
        rules = best_row.get("rules", []) or []
        rescue_rule_ids = [
            str(rule.get("path_class_id", ""))
            for rule in rules
            if isinstance(rule, dict) and "rescue" in str(rule.get("path_class_id", "")).lower()
        ]
        signature = classify_rescue_signature(best_row, rescue_rule_ids)
        if not signature:
            continue
        sides = perf.get("sides", {}) or {}
        rows.append(
            {
                "node": node_dir.name,
                "signature": signature,
                "status": perf.get("status"),
                "issue_count": int(perf.get("issue_count", 0) or 0),
                "selected_opportunity_count": int(perf.get("selected_opportunity_count", 0) or 0),
                "best_direction": str(best_row.get("direction", "")),
                "best_target_distance": float(best_row.get("target_distance", 0.0) or 0.0),
                "best_trade_count": int(best_row.get("trade_count", 0) or 0),
                "best_tp_hit_rate": float(best_row.get("tp_hit_rate", 0.0) or 0.0),
                "rescue_rule_count": len(rescue_rule_ids),
                "rescue_rule_ids": rescue_rule_ids,
                "long_wr": sides.get("LONG", {}).get("effective_win_rate"),
                "short_wr": sides.get("SHORT", {}).get("effective_win_rate"),
            }
        )
    rows.sort(key=lambda row: (row["signature"], row["status"] != "REPAIR_REQUIRED", -row["best_trade_count"], row["node"]))
    return rows


def write_csv_report(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "node",
        "signature",
        "status",
        "issue_count",
        "selected_opportunity_count",
        "best_direction",
        "best_target_distance",
        "best_trade_count",
        "best_tp_hit_rate",
        "rescue_rule_count",
        "rescue_rule_ids",
        "long_wr",
        "short_wr",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            payload["rescue_rule_ids"] = "|".join(row.get("rescue_rule_ids", []))
            writer.writerow(payload)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--compiled-root", type=Path, default=Path("compiled_market_nodes"))
    ap.add_argument("--json-out", type=Path, default=Path("artifacts/rescue_dominance_audit.json"))
    ap.add_argument("--csv-out", type=Path, default=Path("artifacts/rescue_dominance_audit.csv"))
    args = ap.parse_args()

    rows = audit(args.compiled_root)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(rows, indent=2))
    write_csv_report(args.csv_out, rows)
    print(
        json.dumps(
            {
                "status": "PASS",
                "compiled_root": str(args.compiled_root),
                "json_out": str(args.json_out),
                "csv_out": str(args.csv_out),
                "count": len(rows),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
