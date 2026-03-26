#!/usr/bin/env python3
from __future__ import annotations

import json
import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LOCAL = ROOT / "compiled_aee_target_local_11_sessions" / "target_local_aee_report.json"
HOT = ROOT / "compiled_aee_hotspot_11_sessions" / "aee_hotspot_report.json"
OUT = ROOT / "compiled_aee_target_local_hotspot_merged_11_sessions"
LOCAL_ROWS = ROOT / "compiled_aee_target_local_11_sessions" / "target_local_aee_trade_rows.json"
HOT_ROWS = ROOT / "compiled_aee_hotspot_11_sessions" / "aee_hotspot_trade_rows.json"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-report", type=Path, default=LOCAL)
    parser.add_argument("--hotspot-report", type=Path, default=HOT)
    parser.add_argument("--local-trade-rows", type=Path, default=LOCAL_ROWS)
    parser.add_argument("--hotspot-trade-rows", type=Path, default=HOT_ROWS)
    parser.add_argument("--output-dir", type=Path, default=OUT)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    local = json.loads(args.local_report.read_text())
    hot = json.loads(args.hotspot_report.read_text())
    local_rows = json.loads(args.local_trade_rows.read_text())
    hot_rows = json.loads(args.hotspot_trade_rows.read_text())
    local_rows_by_class = {}
    hot_rows_by_class = {}
    for row in local_rows:
        local_rows_by_class.setdefault(f"{row['direction']}_{float(row['target_distance'])}", []).append(row)
    for row in hot_rows:
        hot_rows_by_class.setdefault(f"{row['direction']}_{float(row['target_distance'])}", []).append(row)

    merged = {}
    improved = []
    merged_rows = []
    total_aee = 0.0
    total_static = 0.0
    total_trades = 0
    total_aee_r = 0.0
    total_static_r = 0.0
    total_tp = 0
    total_sl = 0
    total_to = 0

    for key, local_payload in local["class_reports"].items():
        candidate = hot["class_reports"].get(key, {}).get("optimized_metrics")
        chosen_metrics = local_payload["metrics"]
        chosen_source = "target_local"
        if candidate and candidate["pips_per_hour"] > chosen_metrics["pips_per_hour"]:
            chosen_metrics = candidate
            chosen_source = "hotspot"
            improved.append(key)
        merged[key] = {
            "source": chosen_source,
            "metrics": chosen_metrics,
            "baseline_local_metrics": local_payload["metrics"],
            "hotspot_metrics": hot["class_reports"].get(key, {}).get("optimized_metrics"),
        }
        chosen_rows = local_rows_by_class.get(key, [])
        if chosen_source == "hotspot":
            chosen_rows = hot_rows_by_class.get(key, chosen_rows)
        merged_rows.extend(chosen_rows)
        total_trades += chosen_metrics["trade_count"]
        total_tp += chosen_metrics["tp_hits"]
        total_sl += chosen_metrics["sl_hits"]
        total_to += chosen_metrics["timeouts"]
        total_aee += chosen_metrics["avg_aee_pips"] * chosen_metrics["trade_count"]
        total_static += chosen_metrics["avg_static_pips"] * chosen_metrics["trade_count"]
        total_aee_r += chosen_metrics["avg_aee_R"] * chosen_metrics["trade_count"]
        total_static_r += chosen_metrics["avg_static_R"] * chosen_metrics["trade_count"]

    aggregate = {
        "trade_count": total_trades,
        "tp_hits": total_tp,
        "sl_hits": total_sl,
        "timeouts": total_to,
        "avg_static_pips": round(total_static / total_trades, 6) if total_trades else 0.0,
        "avg_aee_pips": round(total_aee / total_trades, 6) if total_trades else 0.0,
        "avg_static_R": round(total_static_r / total_trades, 6) if total_trades else 0.0,
        "avg_aee_R": round(total_aee_r / total_trades, 6) if total_trades else 0.0,
        "pips_per_hour": round(total_aee / 88.0, 6),
        "estimated_equity_per_hour": round((total_aee / 2.5) * 2.0 / 88.0, 6),
        "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        "delta_avg_R": round((total_aee_r - total_static_r) / total_trades, 6) if total_trades else 0.0,
    }

    payload = {
        "aggregate_metrics": aggregate,
        "improved_classes": improved,
        "class_reports": merged,
    }
    (args.output_dir / "aee_target_local_hotspot_merged_report.json").write_text(json.dumps(payload, indent=2))
    (args.output_dir / "aee_target_local_hotspot_merged_trade_rows.json").write_text(json.dumps(merged_rows, indent=2))
    print(json.dumps({"status": "PASS", "aggregate_pips_per_hour": aggregate["pips_per_hour"], "improved_classes": improved}, indent=2))


if __name__ == "__main__":
    main()
