from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from aee_state_machine import load_thresholds
from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, PRESETS, summarize as summarize_aee
from quarter_side_ceiling_replay import build_state_rows, build_subset, percentile, score_rows, tune_aee
from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent
WEAK_NODES = [("LONG", "Q1"), ("LONG", "Q3"), ("SHORT", "Q2"), ("SHORT", "Q4")]
PRESET_POOL = [
    "baseline",
    "faster_harvest",
    "patient_panic",
    "patient_trend",
    "panic_light",
    "harvest_heavy",
    "salvage_weak",
    "ultra_patient",
]


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


def joint_search(rows: List[Dict[str, Any]], label: str, thresholds: Dict[str, Any]) -> Dict[str, Any]:
    if not rows:
        return {"entry_config": None, "entry_metrics": score_rows([]), "best_aee_preset": None, "aee_report": None, "selected_rows": []}
    speeds = [r["pre_speed"] for r in rows]
    effs = [r["pre_efficiency"] for r in rows]
    vols = [r["pre_volatility"] for r in rows]
    ranges = [r["pre_range_pips"] for r in rows]
    progresses = [r["cluster_progress"] for r in rows]
    best = None
    best_payload = None
    for sq in [0.1, 0.2, 0.3, 0.4]:
        for eq in [0.1, 0.2, 0.3, 0.4]:
            for vq in [0.1, 0.2, 0.3, 0.4]:
                for rq in [0.0, 0.1, 0.2, 0.3]:
                    for pgmin in [0.0, 0.05, 0.1]:
                        for pgmax in [0.33, 0.5, 0.66, 0.8]:
                            if pgmax <= pgmin:
                                continue
                            cfg = {
                                "pre_speed_min": percentile(speeds, sq),
                                "pre_efficiency_min": percentile(effs, eq),
                                "pre_volatility_min": percentile(vols, vq),
                                "pre_range_min": percentile(ranges, rq),
                                "cluster_progress_min": percentile(progresses, pgmin),
                                "cluster_progress_max": percentile(progresses, pgmax),
                            }
                            subset = build_subset(rows, cfg)
                            if len(subset) < 8:
                                continue
                            prepared = prepare_rows(subset)
                            entry_metrics = score_rows(subset)
                            for preset_name in PRESET_POOL:
                                overrides = PRESETS[preset_name]
                                params = dict(BASE)
                                params.update(overrides)
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
                                        "selected_rows": subset,
                                    }
    return best_payload or {"entry_config": None, "entry_metrics": score_rows([]), "best_aee_preset": None, "aee_report": None, "selected_rows": []}


def serializable_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        clean = dict(row)
        if "dt" in clean:
            clean["dt"] = clean["dt"].isoformat()
        out.append(clean)
    return out


def main() -> None:
    base = json.loads((ROOT / "quarter_side_ceiling_replay.json").read_text())
    thresholds = load_thresholds()
    state_rows = build_state_rows()
    by_node: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_node[(row["direction"], compute_quarter(row["timestamp_start"], "LONDON"))].append(row)

    updates = {}
    for direction, quarter in WEAK_NODES:
        label = f"{direction}_{quarter}"
        result = joint_search(by_node[(direction, quarter)], label, thresholds)
        updates[label] = {
            "raw_population": score_rows(by_node[(direction, quarter)]),
            "entry_config": result["entry_config"],
            "entry_metrics": result["entry_metrics"],
            "selected_trade_rows": len(result["selected_rows"]),
            "best_aee_preset": result["best_aee_preset"],
            "aee_report": result["aee_report"],
        }
        (ROOT / f"quarter_side_{label.lower()}_max.json").write_text(
            json.dumps(
                {
                    "label": label,
                    **updates[label],
                    "selected_rows": serializable_rows(result["selected_rows"]),
                },
                indent=2,
            )
        )

    for label, payload in updates.items():
        base["nodes"][label] = payload

    (ROOT / "quarter_side_ceiling_max.json").write_text(json.dumps(base, indent=2))
    print(json.dumps(updates, indent=2))


if __name__ == "__main__":
    main()
