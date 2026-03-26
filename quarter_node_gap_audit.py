from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent
WEAK_NODES = [("LONG", "Q1"), ("LONG", "Q3"), ("SHORT", "Q2"), ("SHORT", "Q4")]


def load_json(name: str) -> Dict[str, Any]:
    return json.loads((ROOT / name).read_text())


def summarize_session_bucket(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    static_total = sum(float(t["static_pips"]) for t in trades)
    aee_total = sum(float(t["aee_pips"]) for t in trades)
    static_wins = sum(1 for t in trades if float(t["static_pips"]) > 0)
    aee_wins = sum(1 for t in trades if float(t["aee_pips"]) > 0)
    return {
        "trade_count": n,
        "static_win_rate": static_wins / n if n else 0.0,
        "aee_win_rate": aee_wins / n if n else 0.0,
        "static_pph": static_total / 2.0 if n else 0.0,
        "aee_pph": aee_total / 2.0 if n else 0.0,
    }


def summarize_quarter_local_static(payload: Dict[str, Any]) -> Dict[str, Any]:
    m = payload["entry_metrics"]
    return {
        "trade_count": int(m["trade_count"]),
        "win_rate": float(m["win_rate"]),
        "static_pph": float(m["pips_per_hour"]),
    }


def summarize_quarter_local_aee(payload: Dict[str, Any]) -> Dict[str, Any]:
    r = payload["aee_report"]
    return {
        "trade_count": int(r["total_trades"]),
        "win_rate": None,
        "aee_pph": float(r["aee_pips_per_hour"]),
        "delta_pph": float(r["delta_pips_per_hour"]),
    }


def main() -> None:
    combined = load_json("aee_state_machine_replay_combined.json")
    quarter_local = load_json("quarter_side_ceiling_replay.json")

    by_node: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for trade in combined.get("trades", []):
        key = (trade["direction"], compute_quarter(trade["timestamp_start"], "LONDON"))
        by_node[key].append(trade)

    report: Dict[str, Any] = {}
    for direction, quarter in WEAK_NODES:
        label = f"{direction}_{quarter}"
        session_bucket = summarize_session_bucket(by_node[(direction, quarter)])
        local_payload = quarter_local["nodes"][label]
        local_static = summarize_quarter_local_static(local_payload)
        local_aee = summarize_quarter_local_aee(local_payload)
        selection_gap = local_static["static_pph"] - session_bucket["static_pph"]
        aee_gap = local_aee["aee_pph"] - local_static["static_pph"]
        total_gap = local_aee["aee_pph"] - session_bucket["aee_pph"]
        if selection_gap < 0:
            first_failing_layer = "selection"
        elif aee_gap < 0:
            first_failing_layer = "aee"
        else:
            first_failing_layer = "none"
        report[label] = {
            "session_bucket_benchmark": session_bucket,
            "quarter_local_static": local_static,
            "quarter_local_aee": local_aee,
            "selection_gap": selection_gap,
            "aee_gap": aee_gap,
            "total_gap": total_gap,
            "first_failing_layer": first_failing_layer,
        }

    (ROOT / "quarter_node_gap_audit.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
