from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from aee_state_machine import load_thresholds
from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, summarize as summarize_aee


ROOT = Path(__file__).resolve().parent
TARGETS = ["LONG_Q1", "LONG_Q3", "SHORT_Q2", "SHORT_Q4"]


def load_rows(label: str) -> List[Dict[str, Any]]:
    payload = json.loads((ROOT / f"quarter_side_{label.lower()}.json").read_text())
    rows = payload["selected_rows"]
    prepared = []
    for row in rows:
        clean = dict(row)
        clean["entry_mode"] = "harvester"
        sim = simulate_harvester_trade(clean, float(clean["distance"]))
        clean["pips"] = sim["pips"]
        clean["reason"] = sim["reason"]
        prepared.append(clean)
    return prepared


def param_grid(label: str) -> List[Dict[str, float]]:
    out = []
    if label.startswith("LONG"):
        for panic_open in [1.8, 2.2]:
            for panic_peak in [1.6, 2.0]:
                for long_opp in [2.2, 2.8]:
                    for long_floor in [1.5, 2.0]:
                        for decay_open in [1.0, 1.4]:
                            for decay_peak in [1.0, 1.3]:
                                for harvest_time in [0.18, 0.26]:
                                    for harvest_under in [0.18, 0.26]:
                                        for harvest_giveback in [0.10, 0.18]:
                                            out.append(
                                                {
                                                    "panic_time_open_mult": panic_open,
                                                    "panic_time_since_peak_mult": panic_peak,
                                                    "long_panic_opp_mult": long_opp,
                                                    "long_distance_floor_mult": long_floor,
                                                    "decay_time_open_mult": decay_open,
                                                    "decay_time_since_peak_mult": decay_peak,
                                                    "harvest_time_mult": harvest_time,
                                                    "harvest_under_profit_mult": harvest_under,
                                                    "harvest_giveback_mult": harvest_giveback,
                                                }
                                            )
    else:
        for panic_open in [1.4, 1.8]:
            for panic_peak in [1.2, 1.6]:
                for short_opp in [1.8, 2.2]:
                    for bad_opp in [1.5, 1.9]:
                        for decay_open in [1.0, 1.4]:
                            for decay_peak in [1.0, 1.3]:
                                for harvest_time in [0.18, 0.26]:
                                    for harvest_under in [0.18, 0.26]:
                                        for harvest_giveback in [0.10, 0.18]:
                                            out.append(
                                                {
                                                    "panic_time_open_mult": panic_open,
                                                    "panic_time_since_peak_mult": panic_peak,
                                                    "short_panic_opp_mult": short_opp,
                                                    "bad_trade_opp_mult": bad_opp,
                                                    "decay_time_open_mult": decay_open,
                                                    "decay_time_since_peak_mult": decay_peak,
                                                    "harvest_time_mult": harvest_time,
                                                    "harvest_under_profit_mult": harvest_under,
                                                    "harvest_giveback_mult": harvest_giveback,
                                                }
                                            )
    return out


def main() -> None:
    thresholds = load_thresholds()
    results: Dict[str, Any] = {}
    for label in TARGETS:
        rows = load_rows(label)
        best_score = None
        best_payload = None
        for override in param_grid(label):
            params = dict(BASE)
            params.update(override)
            report = summarize_aee(rows, label, thresholds, params)
            score = (report["aee_pips_per_hour"], report["delta_pips_per_hour"], report["avg_aee_R"])
            if best_score is None or score > best_score:
                best_score = score
                best_payload = {
                    "params": override,
                    "report": report,
                    "trade_count": len(rows),
                }
        results[label] = best_payload
        (ROOT / f"quarter_specific_{label.lower()}_aee.json").write_text(json.dumps(best_payload, indent=2))
    (ROOT / "quarter_specific_aee_tune.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
