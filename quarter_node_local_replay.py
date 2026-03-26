from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, summarize as summarize_aee


ROOT = Path(__file__).resolve().parent
TARGETS = ["LONG_Q1", "LONG_Q3", "SHORT_Q2", "SHORT_Q4"]


def load_rows(label: str) -> List[Dict[str, Any]]:
    payload = json.loads((ROOT / f"quarter_side_{label.lower()}.json").read_text())
    rows = payload["selected_rows"]
    out = []
    for row in rows:
        clean = dict(row)
        clean["entry_mode"] = "harvester"
        sim = simulate_harvester_trade(clean, float(clean["distance"]))
        clean["pips"] = sim["pips"]
        clean["reason"] = sim["reason"]
        out.append(clean)
    return out


def node_param_grid(label: str) -> List[Dict[str, float]]:
    if label.startswith("LONG"):
        return [
            {},
            {
                "panic_time_open_mult": 1.3,
                "panic_time_since_peak_mult": 1.3,
                "long_panic_opp_mult": 1.2,
                "long_distance_floor_mult": 0.8,
                "panic_giveback_mult": 0.9,
                "decay_time_open_mult": 0.9,
                "decay_time_since_peak_mult": 0.9,
                "harvest_time_mult": 0.22,
                "harvest_under_profit_mult": 0.22,
                "harvest_giveback_mult": 0.12,
            },
            {
                "panic_time_open_mult": 1.6,
                "panic_time_since_peak_mult": 1.6,
                "long_panic_opp_mult": 1.5,
                "long_distance_floor_mult": 1.0,
                "panic_giveback_mult": 1.0,
                "decay_time_open_mult": 1.0,
                "decay_time_since_peak_mult": 1.0,
                "harvest_time_mult": 0.18,
                "harvest_under_profit_mult": 0.18,
                "harvest_giveback_mult": 0.10,
            },
            {
                "panic_time_open_mult": 2.0,
                "panic_time_since_peak_mult": 2.0,
                "long_panic_opp_mult": 1.8,
                "long_distance_floor_mult": 1.2,
                "panic_giveback_mult": 1.1,
                "decay_time_open_mult": 1.1,
                "decay_time_since_peak_mult": 1.1,
                "harvest_time_mult": 0.16,
                "harvest_under_profit_mult": 0.16,
                "harvest_giveback_mult": 0.08,
            },
        ]
    return [
        {},
        {
            "panic_time_open_mult": 0.8,
            "panic_time_since_peak_mult": 0.8,
            "short_panic_opp_mult": 1.0,
            "bad_trade_opp_mult": 1.0,
            "panic_giveback_mult": 0.9,
            "decay_time_open_mult": 0.9,
            "decay_time_since_peak_mult": 0.9,
            "harvest_time_mult": 0.22,
            "harvest_under_profit_mult": 0.22,
            "harvest_giveback_mult": 0.12,
        },
        {
            "panic_time_open_mult": 1.0,
            "panic_time_since_peak_mult": 1.0,
            "short_panic_opp_mult": 1.2,
            "bad_trade_opp_mult": 1.2,
            "panic_giveback_mult": 1.0,
            "decay_time_open_mult": 1.0,
            "decay_time_since_peak_mult": 1.0,
            "harvest_time_mult": 0.18,
            "harvest_under_profit_mult": 0.18,
            "harvest_giveback_mult": 0.10,
        },
        {
            "panic_time_open_mult": 1.2,
            "panic_time_since_peak_mult": 1.2,
            "short_panic_opp_mult": 1.4,
            "bad_trade_opp_mult": 1.4,
            "panic_giveback_mult": 1.1,
            "decay_time_open_mult": 1.1,
            "decay_time_since_peak_mult": 1.1,
            "harvest_time_mult": 0.16,
            "harvest_under_profit_mult": 0.16,
            "harvest_giveback_mult": 0.08,
        },
    ]


def main() -> None:
    results: Dict[str, Any] = {}
    for label in TARGETS:
        rows = load_rows(label)
        thresholds = json.loads((ROOT / f"quarter_node_{label.lower()}_thresholds.json").read_text())
        best = None
        best_payload = None
        for override in node_param_grid(label):
            params = dict(BASE)
            params.update(override)
            report = summarize_aee(rows, label, thresholds, params)
            score = (report["aee_pips_per_hour"], report["delta_pips_per_hour"], report["avg_aee_R"])
            if best is None or score > best:
                best = score
                best_payload = {"params": override, "report": report}
        results[label] = best_payload
        (ROOT / f"quarter_node_local_{label.lower()}_replay.json").write_text(json.dumps(best_payload, indent=2))
    (ROOT / "quarter_node_local_replay.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
