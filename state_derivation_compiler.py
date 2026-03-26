from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent


def percentile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def load_json(name: str) -> Dict[str, Any]:
    return json.loads((ROOT / name).read_text())


def load_csv(name: str) -> List[Dict[str, Any]]:
    with (ROOT / name).open() as f:
        return list(csv.DictReader(f))


def collect_selected_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(ROOT.glob("quarter_side_*_q*.json")):
        payload = load_json(path.name)
        label = payload["label"]
        for row in payload.get("selected_rows", []):
            clean = dict(row)
            clean["node_label"] = label
            clean["harvester_profit"] = float(clean["harvester_profit"])
            clean["future_mfe"] = float(clean["future_mfe"])
            clean["future_mae"] = float(clean["future_mae"])
            clean["pre_speed"] = float(clean["pre_speed"])
            clean["pre_efficiency"] = float(clean["pre_efficiency"])
            clean["pre_volatility"] = float(clean["pre_volatility"])
            clean["pre_range_pips"] = float(clean["pre_range_pips"])
            clean["cluster_progress"] = float(clean["cluster_progress"])
            rows.append(clean)
    return rows


def summarize_numeric(rows: List[Dict[str, Any]], fields: List[str]) -> Dict[str, Any]:
    out = {"count": len(rows)}
    for field in fields:
        vals = [float(r[field]) for r in rows]
        out[field] = {
            "p10": percentile(vals, 0.10),
            "p25": percentile(vals, 0.25),
            "p50": percentile(vals, 0.50),
            "p75": percentile(vals, 0.75),
            "p90": percentile(vals, 0.90),
        }
    return out


def ks_like(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    points = sorted(set(a + b))
    best = 0.0
    for p in points:
        fa = sum(1 for x in a if x <= p) / len(a)
        fb = sum(1 for x in b if x <= p) / len(b)
        best = max(best, abs(fa - fb))
    return best


def derive_entry(rows: List[Dict[str, Any]]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    winners = [r for r in rows if r["harvester_profit"] > 0]
    losers = [r for r in rows if r["harvester_profit"] <= 0]
    fields = ["pre_speed", "pre_efficiency", "pre_volatility", "pre_range_pips", "cluster_progress"]
    dists = {
        "all": summarize_numeric(rows, fields),
        "winners": summarize_numeric(winners, fields),
        "losers": summarize_numeric(losers, fields),
    }
    boundaries = {
        "pre_speed_min": percentile([r["pre_speed"] for r in winners], 0.25),
        "pre_efficiency_min": percentile([r["pre_efficiency"] for r in winners], 0.25),
        "pre_volatility_min": percentile([r["pre_volatility"] for r in winners], 0.20),
        "pre_range_min": percentile([r["pre_range_pips"] for r in winners], 0.20),
        "cluster_progress_max": percentile([r["cluster_progress"] for r in winners], 0.75),
        "derivation": {
            f: {
                "boundary_type": "winner_lower_quantile" if f != "cluster_progress" else "winner_upper_quantile",
                "ks_like": ks_like([r[f] for r in winners], [r[f] for r in losers]),
                "winner_p25": percentile([r[f] for r in winners], 0.25),
                "winner_p75": percentile([r[f] for r in winners], 0.75),
                "loser_p50": percentile([r[f] for r in losers], 0.50),
            }
            for f in fields
        },
    }
    return dists, boundaries


def load_global_scenarios() -> List[Dict[str, Any]]:
    rows = load_csv("aee_energy_scenarios.csv")
    numeric = [
        "profit_now",
        "mfe_so_far",
        "giveback_now",
        "velocity_now",
        "velocity_change",
        "time_open",
        "time_since_peak",
        "time_under_profit_threshold",
        "cluster_progress",
        "distance_to_recent_extreme",
        "opposite_direction_strength",
    ]
    for row in rows:
        for field in numeric:
            row[field] = float(row[field])
    return rows


def derive_scenario_boundaries(rows: List[Dict[str, Any]], scenario_type: str) -> Dict[str, Any]:
    subset = [r for r in rows if r["scenario_type"] == scenario_type]
    if not subset:
        return {"count": 0}
    return {
        "count": len(subset),
        "velocity_now": {"p25": percentile([r["velocity_now"] for r in subset], 0.25), "p50": percentile([r["velocity_now"] for r in subset], 0.50), "p75": percentile([r["velocity_now"] for r in subset], 0.75)},
        "giveback_now": {"p25": percentile([r["giveback_now"] for r in subset], 0.25), "p50": percentile([r["giveback_now"] for r in subset], 0.50), "p75": percentile([r["giveback_now"] for r in subset], 0.75)},
        "time_open": {"p25": percentile([r["time_open"] for r in subset], 0.25), "p50": percentile([r["time_open"] for r in subset], 0.50), "p75": percentile([r["time_open"] for r in subset], 0.75)},
        "time_since_peak": {"p25": percentile([r["time_since_peak"] for r in subset], 0.25), "p50": percentile([r["time_since_peak"] for r in subset], 0.50), "p75": percentile([r["time_since_peak"] for r in subset], 0.75)},
        "time_under_profit_threshold": {"p25": percentile([r["time_under_profit_threshold"] for r in subset], 0.25), "p50": percentile([r["time_under_profit_threshold"] for r in subset], 0.50), "p75": percentile([r["time_under_profit_threshold"] for r in subset], 0.75)},
        "opposite_direction_strength": {"p25": percentile([r["opposite_direction_strength"] for r in subset], 0.25), "p50": percentile([r["opposite_direction_strength"] for r in subset], 0.50), "p75": percentile([r["opposite_direction_strength"] for r in subset], 0.75)},
    }


def main() -> None:
    selected_rows = collect_selected_rows()
    entry_dists, entry_bounds = derive_entry(selected_rows)
    global_scenarios = load_global_scenarios()
    continuation_dists = derive_scenario_boundaries(global_scenarios, "continuation")
    panic_dists = derive_scenario_boundaries(global_scenarios, "panic")
    decay_dists = derive_scenario_boundaries(global_scenarios, "decay")
    bad_rows = [r for r in global_scenarios if r["recommended_action"] in {"PANIC", "DO_NOT_ENTER", "DECAY_EXIT"}]
    bad_trade_dists = {
        "count": len(bad_rows),
        "opposite_direction_strength": {
            "p50": percentile([r["opposite_direction_strength"] for r in bad_rows], 0.50),
            "p75": percentile([r["opposite_direction_strength"] for r in bad_rows], 0.75),
        },
        "giveback_now": {
            "p50": percentile([r["giveback_now"] for r in bad_rows], 0.50),
            "p75": percentile([r["giveback_now"] for r in bad_rows], 0.75),
        },
        "time_open": {
            "p50": percentile([r["time_open"] for r in bad_rows], 0.50),
        },
    }
    partial_bounds = {
        "partial_profit_band_pips": continuation_dists["giveback_now"]["p25"] if continuation_dists.get("count") else 0.0,
        "harvest_velocity_ceiling": derive_scenario_boundaries(global_scenarios, "stall")["velocity_now"]["p75"] if derive_scenario_boundaries(global_scenarios, "stall").get("count") else 0.0,
        "harvest_time_open_floor": derive_scenario_boundaries(global_scenarios, "stall")["time_open"]["p50"] if derive_scenario_boundaries(global_scenarios, "stall").get("count") else 0.0,
        "boundary_type": "stall_distribution",
    }
    runner_bounds = {
        "continuation_profit_floor": continuation_dists["giveback_now"]["p25"] if continuation_dists.get("count") else 0.0,
        "continuation_opp_pressure_ceiling": continuation_dists["opposite_direction_strength"]["p50"] if continuation_dists.get("count") else 0.0,
        "boundary_type": "continuation_distribution",
    }
    panic_bounds = {
        "velocity_floor": panic_dists["velocity_now"]["p50"] if panic_dists.get("count") else 0.0,
        "giveback_floor": panic_dists["giveback_now"]["p50"] if panic_dists.get("count") else 0.0,
        "time_open_floor": panic_dists["time_open"]["p50"] if panic_dists.get("count") else 0.0,
        "opp_strength_floor": panic_dists["opposite_direction_strength"]["p50"] if panic_dists.get("count") else 0.0,
        "boundary_type": "panic_distribution",
    }
    decay_bounds = {
        "giveback_floor": decay_dists["giveback_now"]["p50"] if decay_dists.get("count") else 0.0,
        "time_open_floor": decay_dists["time_open"]["p50"] if decay_dists.get("count") else 0.0,
        "time_since_peak_floor": decay_dists["time_since_peak"]["p50"] if decay_dists.get("count") else 0.0,
        "boundary_type": "decay_distribution",
    }
    bad_trade_bounds = {
        "opp_strength_floor": bad_trade_dists["opposite_direction_strength"]["p75"] if bad_trade_dists.get("count") else 0.0,
        "giveback_floor": bad_trade_dists["giveback_now"]["p75"] if bad_trade_dists.get("count") else 0.0,
        "time_open_floor": bad_trade_dists["time_open"]["p50"] if bad_trade_dists.get("count") else 0.0,
        "boundary_type": "bad_trade_distribution",
    }

    (ROOT / "entry_state_distributions.json").write_text(json.dumps(entry_dists, indent=2))
    (ROOT / "continuation_state_distributions.json").write_text(json.dumps(continuation_dists, indent=2))
    (ROOT / "panic_state_distributions.json").write_text(json.dumps(panic_dists, indent=2))
    (ROOT / "decay_state_distributions.json").write_text(json.dumps(decay_dists, indent=2))
    (ROOT / "bad_trade_state_distributions.json").write_text(json.dumps(bad_trade_dists, indent=2))

    (ROOT / "derived_entry_boundaries.json").write_text(json.dumps(entry_bounds, indent=2))
    (ROOT / "derived_partial_boundaries.json").write_text(json.dumps(partial_bounds, indent=2))
    (ROOT / "derived_runner_boundaries.json").write_text(json.dumps(runner_bounds, indent=2))
    (ROOT / "derived_panic_boundaries.json").write_text(json.dumps(panic_bounds, indent=2))
    (ROOT / "derived_decay_boundaries.json").write_text(json.dumps(decay_bounds, indent=2))
    (ROOT / "derived_bad_trade_boundaries.json").write_text(json.dumps(bad_trade_bounds, indent=2))

    report = {
        "selected_trade_count": len(selected_rows),
        "global_scenario_count": len(global_scenarios),
        "artifacts": [
            "entry_state_distributions.json",
            "continuation_state_distributions.json",
            "panic_state_distributions.json",
            "decay_state_distributions.json",
            "bad_trade_state_distributions.json",
            "derived_entry_boundaries.json",
            "derived_partial_boundaries.json",
            "derived_runner_boundaries.json",
            "derived_panic_boundaries.json",
            "derived_decay_boundaries.json",
            "derived_bad_trade_boundaries.json",
        ],
    }
    (ROOT / "state_derivation_compiler_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
