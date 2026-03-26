from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, summarize as summarize_aee


ROOT = Path(__file__).resolve().parent


NODE_RULES = {
    "LONG_Q1": [
        {
            "name": "harvest_delay",
            "params": {
                "panic_time_open_mult": 1.8,
                "panic_time_since_peak_mult": 1.8,
                "long_panic_opp_mult": 1.6,
                "long_distance_floor_mult": 1.1,
                "harvest_time_mult": 0.45,
                "harvest_under_profit_mult": 0.45,
                "harvest_giveback_mult": 0.55,
                "harvest_velocity_add": 0.02,
                "decay_time_open_mult": 1.1,
                "decay_time_since_peak_mult": 1.1,
            },
        },
        {
            "name": "harvest_off_patient",
            "params": {
                "panic_time_open_mult": 2.0,
                "panic_time_since_peak_mult": 2.0,
                "long_panic_opp_mult": 1.8,
                "long_distance_floor_mult": 1.25,
                "harvest_time_mult": 0.65,
                "harvest_under_profit_mult": 0.65,
                "harvest_giveback_mult": 0.75,
                "harvest_velocity_add": 0.0,
                "decay_time_open_mult": 1.2,
                "decay_time_since_peak_mult": 1.2,
            },
        },
    ],
    "LONG_Q3": [
        {
            "name": "decay_harder",
            "params": {
                "panic_time_open_mult": 1.4,
                "panic_time_since_peak_mult": 1.4,
                "long_panic_opp_mult": 1.4,
                "long_distance_floor_mult": 1.0,
                "decay_time_open_mult": 2.0,
                "decay_time_since_peak_mult": 2.0,
                "decay_giveback_mult": 1.8,
                "harvest_time_mult": 0.32,
                "harvest_under_profit_mult": 0.32,
                "harvest_giveback_mult": 0.22,
            },
        },
        {
            "name": "decay_very_hard",
            "params": {
                "panic_time_open_mult": 1.6,
                "panic_time_since_peak_mult": 1.6,
                "long_panic_opp_mult": 1.6,
                "long_distance_floor_mult": 1.1,
                "decay_time_open_mult": 2.4,
                "decay_time_since_peak_mult": 2.4,
                "decay_giveback_mult": 2.2,
                "harvest_time_mult": 0.35,
                "harvest_under_profit_mult": 0.35,
                "harvest_giveback_mult": 0.25,
            },
        },
    ],
    "SHORT_Q2": [
        {
            "name": "panic_decay_split",
            "params": {
                "panic_time_open_mult": 1.8,
                "panic_time_since_peak_mult": 1.8,
                "short_panic_opp_mult": 1.8,
                "bad_trade_opp_mult": 1.8,
                "panic_giveback_mult": 1.5,
                "decay_time_open_mult": 1.7,
                "decay_time_since_peak_mult": 1.6,
                "decay_giveback_mult": 1.6,
                "harvest_time_mult": 0.40,
                "harvest_under_profit_mult": 0.40,
                "harvest_giveback_mult": 0.35,
                "harvest_velocity_add": 0.02,
            },
        },
        {
            "name": "panic_decay_more_patient",
            "params": {
                "panic_time_open_mult": 2.0,
                "panic_time_since_peak_mult": 2.0,
                "short_panic_opp_mult": 2.0,
                "bad_trade_opp_mult": 2.0,
                "panic_giveback_mult": 1.7,
                "decay_time_open_mult": 2.0,
                "decay_time_since_peak_mult": 1.8,
                "decay_giveback_mult": 1.8,
                "harvest_time_mult": 0.45,
                "harvest_under_profit_mult": 0.45,
                "harvest_giveback_mult": 0.40,
                "harvest_velocity_add": 0.0,
            },
        },
    ],
    "SHORT_Q4": [
        {
            "name": "panic_raise",
            "params": {
                "panic_time_open_mult": 1.8,
                "panic_time_since_peak_mult": 1.8,
                "short_panic_opp_mult": 1.8,
                "bad_trade_opp_mult": 1.6,
                "panic_giveback_mult": 1.5,
                "decay_time_open_mult": 1.1,
                "decay_time_since_peak_mult": 1.1,
                "harvest_time_mult": 0.28,
                "harvest_under_profit_mult": 0.28,
                "harvest_giveback_mult": 0.18,
            },
        },
        {
            "name": "panic_raise_more",
            "params": {
                "panic_time_open_mult": 2.0,
                "panic_time_since_peak_mult": 2.0,
                "short_panic_opp_mult": 2.0,
                "bad_trade_opp_mult": 1.8,
                "panic_giveback_mult": 1.7,
                "decay_time_open_mult": 1.2,
                "decay_time_since_peak_mult": 1.2,
                "harvest_time_mult": 0.30,
                "harvest_under_profit_mult": 0.30,
                "harvest_giveback_mult": 0.20,
            },
        },
    ],
}


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


def main() -> None:
    results: Dict[str, Any] = {}
    for label, candidates in NODE_RULES.items():
        thresholds = json.loads((ROOT / f"quarter_node_{label.lower()}_thresholds.json").read_text())
        rows = load_rows(label)
        best = None
        best_payload = None
        for candidate in candidates:
            params = dict(BASE)
            params.update(candidate["params"])
            report = summarize_aee(rows, label, thresholds, params)
            score = (report["aee_pips_per_hour"], report["delta_pips_per_hour"], report["avg_aee_R"])
            if best is None or score > best:
                best = score
                best_payload = {
                    "rule_name": candidate["name"],
                    "params": candidate["params"],
                    "report": report,
                }
        results[label] = best_payload
        (ROOT / f"quarter_action_rescue_{label.lower()}.json").write_text(json.dumps(best_payload, indent=2))
    (ROOT / "quarter_action_rescue.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
