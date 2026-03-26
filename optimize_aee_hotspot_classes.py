#!/usr/bin/env python3
from __future__ import annotations

import copy
import csv
import json
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import run_aee_stage_compiler as aee


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "compiled_aee_stage_11_sessions_canonical"
OUTPUT_DIR = ROOT / "compiled_aee_hotspot_11_sessions"
HOTSPOTS = {("LONG", "1.5"), ("SHORT", "1.5"), ("LONG", "2.5"), ("SHORT", "2.5")}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    total_static = sum(float(r["static_pips"]) for r in rows)
    total_aee = sum(float(r["aee_pips"]) for r in rows)
    tp_hits = sum(1 for r in rows if r["aee_reason"] == "TP_HIT")
    sl_hits = sum(1 for r in rows if r["aee_reason"] in {"SL_HIT", "PANIC", "DECAY_EXIT"} and float(r["aee_pips"]) < 0)
    return {
        "trade_count": total,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "timeouts": sum(1 for r in rows if r["aee_reason"] == "TIMEOUT"),
        "avg_static_pips": round(total_static / total, 6) if total else 0.0,
        "avg_aee_pips": round(total_aee / total, 6) if total else 0.0,
        "avg_static_R": round(aee.mean0([float(r["static_R"]) for r in rows]), 6),
        "avg_aee_R": round(aee.mean0([float(r["aee_R"]) for r in rows]), 6),
        "pips_per_hour": round(total_aee / 88.0, 6),
        "estimated_equity_per_hour": round((total_aee / 2.5) * 2.0 / 88.0, 6),
        "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        "delta_avg_R": round(aee.mean0([float(r["aee_R"]) - float(r["static_R"]) for r in rows]), 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    replay = load_json(args.input_dir / "aee_replay" / "target_selective_aee.json")
    trade_rows = replay["trade_rows"]
    state_rows = load_csv(args.input_dir / "aee_state_stream" / "aee_state_stream.csv")
    rules = load_json(args.input_dir / "aee_rules" / "aee_rule_derivation_report.json")

    by_trade_states: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_trade_states[row["trade_id"]].append(row)
    for rows in by_trade_states.values():
        rows.sort(key=lambda r: int(r["bar_index"]))

    by_class_trades: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in trade_rows:
        by_class_trades[(row["direction"], str(float(row["target_distance"])))].append(row)

    merged_rows: list[dict[str, Any]] = []
    class_report: dict[str, Any] = {}

    for key, subset_trades in sorted(by_class_trades.items()):
        direction, target = key
        subset_states = []
        for tr in subset_trades:
            subset_states.extend(by_trade_states[tr["trade_id"]])

        base_result = aee.replay_variant(
            [
                {
                    "trade_id": tr["trade_id"],
                    "entry_time": tr["entry_time"],
                    "direction": tr["direction"],
                    "target_distance": float(tr["target_distance"]),
                    "quarter": tr["quarter"],
                    "session_id": tr["session_id"],
                    "static_pips": float(tr["static_pips"]),
                    "static_R": float(tr["static_R"]),
                    "static_reason": tr["static_reason"],
                }
                for tr in subset_trades
            ],
            subset_states,
            rules,
            "bias_plus_context_aee",
        )
        best_result = base_result
        best_rules = copy.deepcopy(rules)
        if key in HOTSPOTS:
            base_hf = float(rules["direction_modifiers"][direction]["harvest_profit_floor"])
            base_hg = float(rules["target_modifiers"].get(target, {}).get("harvest_giveback_tolerance", 0.0))
            base_dt = float(rules["target_modifiers"].get(target, {}).get("decay_time_since_peak", 0.0))
            base_eb = float(rules["target_modifiers"].get(target, {}).get("extension_budget_floor", 0.0))
            for hf_add in [0.0, 1.0]:
                for hg_add in [0.0, 0.5]:
                    for dt_add in [0.0, 8.0]:
                        for eb_add in [0.0, 0.06]:
                            cand = copy.deepcopy(rules)
                            cand["direction_modifiers"][direction]["harvest_profit_floor"] = round(base_hf + hf_add, 6)
                            cand["target_modifiers"].setdefault(target, {})
                            cand["target_modifiers"][target]["harvest_giveback_tolerance"] = round(base_hg + hg_add, 6)
                            cand["target_modifiers"][target]["decay_time_since_peak"] = round(base_dt + dt_add, 6)
                            cand["target_modifiers"][target]["extension_budget_floor"] = round(base_eb + eb_add, 6)
                            result = aee.replay_variant(
                                [
                                    {
                                        "trade_id": tr["trade_id"],
                                        "entry_time": tr["entry_time"],
                                        "direction": tr["direction"],
                                        "target_distance": float(tr["target_distance"]),
                                        "quarter": tr["quarter"],
                                        "session_id": tr["session_id"],
                                        "static_pips": float(tr["static_pips"]),
                                        "static_R": float(tr["static_R"]),
                                        "static_reason": tr["static_reason"],
                                    }
                                    for tr in subset_trades
                                ],
                                subset_states,
                                cand,
                                "bias_plus_context_aee",
                            )
                            if (
                                result.metrics["pips_per_hour"] > best_result.metrics["pips_per_hour"]
                                or (
                                    result.metrics["pips_per_hour"] == best_result.metrics["pips_per_hour"]
                                    and result.metrics["avg_aee_R"] > best_result.metrics["avg_aee_R"]
                                )
                            ):
                                best_result = result
                                best_rules = cand

        merged_rows.extend(best_result.trade_rows)
        class_report[f"{direction}_{target}"] = {
            "direction": direction,
            "target_distance": target,
            "baseline_metrics": base_result.metrics,
            "optimized_metrics": best_result.metrics,
            "rules": {
                "direction_modifiers": best_rules["direction_modifiers"][direction],
                "target_modifiers": best_rules["target_modifiers"].get(target, {}),
            },
        }

    aggregate = compute_metrics(merged_rows)
    (args.output_dir / "aee_hotspot_report.json").write_text(json.dumps({"aggregate_metrics": aggregate, "class_reports": class_report}, indent=2))
    (args.output_dir / "aee_hotspot_trade_rows.json").write_text(json.dumps(merged_rows, indent=2))
    print(json.dumps({"status": "PASS", "aggregate_pips_per_hour": aggregate["pips_per_hour"]}, indent=2))


if __name__ == "__main__":
    main()
