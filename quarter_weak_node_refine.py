from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from aee_state_machine import load_thresholds
from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, PRESETS, summarize as summarize_aee
from quarter_side_ceiling_replay import build_state_rows, build_subset, score_rows
from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent
WEAK_NODES = ["LONG_Q1", "LONG_Q3", "SHORT_Q2", "SHORT_Q4"]
PRESET_POOL = ["baseline", "faster_harvest", "patient_panic", "patient_trend", "panic_light", "harvest_heavy", "salvage_weak", "ultra_patient"]


def prepare_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        clean = dict(row)
        clean["entry_mode"] = "harvester"
        sim = simulate_harvester_trade(clean, float(clean["distance"]))
        clean["pips"] = sim["pips"]
        clean["reason"] = sim["reason"]
        out.append(clean)
    return out


def refine_node(rows: List[Dict[str, Any]], label: str, base_cfg: Dict[str, float], base_preset: str, thresholds: Dict[str, Any]) -> Dict[str, Any]:
    best = None
    best_payload = None
    base_rows = prepare_rows(rows)
    node_presets = PRESET_POOL if label in {"LONG_Q1", "SHORT_Q2"} else [base_preset, "faster_harvest", "panic_light", "patient_panic"]
    pgmins = [0.0, base_cfg["cluster_progress_min"]]
    if label in {"LONG_Q3", "SHORT_Q4"}:
        speed_mults = [0.95, 1.0, 1.05]
        eff_mults = [0.95, 1.0, 1.05]
        vol_mults = [0.95, 1.0, 1.05]
        range_mults = [0.95, 1.0, 1.05]
        pgmax_mults = [0.95, 1.0, 1.05]
    else:
        speed_mults = [0.85, 1.0, 1.15]
        eff_mults = [0.85, 1.0, 1.15]
        vol_mults = [0.85, 1.0, 1.15]
        range_mults = [0.85, 1.0, 1.15]
        pgmax_mults = [0.85, 1.0, 1.15]
    for speed_mult in speed_mults:
        for eff_mult in eff_mults:
            for vol_mult in vol_mults:
                for range_mult in range_mults:
                    for pgmin in pgmins:
                        for pgmax_mult in pgmax_mults:
                            cfg = {
                                "pre_speed_min": max(0.0, base_cfg["pre_speed_min"] * speed_mult),
                                "pre_efficiency_min": max(0.0, base_cfg["pre_efficiency_min"] * eff_mult),
                                "pre_volatility_min": max(0.0, base_cfg["pre_volatility_min"] * vol_mult),
                                "pre_range_min": max(0.0, base_cfg["pre_range_min"] * range_mult),
                                "cluster_progress_min": max(0.0, pgmin),
                                "cluster_progress_max": min(1.0, base_cfg["cluster_progress_max"] * pgmax_mult),
                            }
                            if cfg["cluster_progress_max"] <= cfg["cluster_progress_min"]:
                                continue
                            subset = build_subset(rows, cfg)
                            if len(subset) < 8:
                                continue
                            prepared = prepare_rows(subset)
                            entry_metrics = score_rows(subset)
                            for preset_name in node_presets:
                                params = dict(BASE)
                                params.update(PRESETS[preset_name])
                                report = summarize_aee(prepared, label, thresholds, params)
                                score = (
                                    report["aee_pips_per_hour"],
                                    report["delta_pips_per_hour"],
                                    report["avg_aee_R"],
                                    entry_metrics["win_rate"],
                                )
                                if best is None or score > best:
                                    best = score
                                    best_payload = {
                                        "entry_config": cfg,
                                        "entry_metrics": entry_metrics,
                                        "best_aee_preset": preset_name,
                                        "aee_report": report,
                                        "selected_trade_rows": len(subset),
                                    }
    return best_payload


def main() -> None:
    base = json.loads((ROOT / "quarter_side_ceiling_replay.json").read_text())
    thresholds = load_thresholds()
    state_rows = build_state_rows()
    by_node: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_node[(row["direction"], compute_quarter(row["timestamp_start"], "LONDON"))].append(row)

    labels = [sys.argv[1]] if len(sys.argv) > 1 else WEAK_NODES
    updates = {}
    for label in labels:
        direction, quarter = label.split("_")
        payload = base["nodes"][label]
        update = refine_node(by_node[(direction, quarter)], label, payload["entry_config"], payload["best_aee_preset"], thresholds)
        updates[label] = update
        base["nodes"][label] = {
            "raw_population": payload["raw_population"],
            **update,
        }
        (ROOT / f"quarter_weak_node_{label.lower()}_refine.json").write_text(json.dumps(update, indent=2))

    (ROOT / "quarter_weak_node_refine.json").write_text(json.dumps(updates, indent=2))
    if len(labels) == len(WEAK_NODES):
        (ROOT / "quarter_side_ceiling_max.json").write_text(json.dumps(base, indent=2))
    print(json.dumps(updates, indent=2))


if __name__ == "__main__":
    main()
