#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts"
ALLOWLIST_PATH = ARTIFACTS / "final_entry_allowlist_v1.json"

PROTOTYPE_NODES: list[dict[str, str]] = [
    {
        "node": "NZD_USD__friday__sydney",
        "behavior_type": "fast_sydney_survivor",
        "reason": "High-volume repaired Friday survivor with fast Sydney behavior and strong opportunity density.",
    },
    {
        "node": "EUR_USD__friday__new_york",
        "behavior_type": "liquid_new_york_benchmark",
        "reason": "Liquid major-pair New York benchmark for checking whether a universal AEE core behaves sanely on a clean mainstream node.",
    },
    {
        "node": "CHF_JPY__monday__london",
        "behavior_type": "noisy_cross_london",
        "reason": "Large-sample London cross chosen to test whether minimal AEE can reduce damage on noisier path shapes.",
    },
    {
        "node": "EUR_CHF__monday__london",
        "behavior_type": "slow_grind_cross",
        "reason": "Slower CHF cross included to represent grindier sessions where weak-trade kills matter more than runner logic.",
    },
    {
        "node": "GBP_JPY__monday__asia",
        "behavior_type": "asia_impulse_cross",
        "reason": "JPY cross with impulse potential added to make sure the prototype is not tuned only to slower London profiles.",
    },
]

STAGE_B_NODES: list[dict[str, str]] = [
    *PROTOTYPE_NODES,
    {
        "node": "AUD_USD__monday__london",
        "behavior_type": "london_major_grinder",
        "reason": "London major survivor added to widen the sample with a slower, higher-throughput trend candidate.",
    },
    {
        "node": "EUR_JPY__friday__asia",
        "behavior_type": "asia_jpy_major",
        "reason": "Asia JPY major survivor added to widen session coverage beyond Monday and London-heavy profiles.",
    },
    {
        "node": "USD_JPY__monday__asia",
        "behavior_type": "asia_liquid_jpy",
        "reason": "Liquid JPY survivor included to test whether minimal AEE stays additive on a faster, cleaner Asia path.",
    },
    {
        "node": "GBP_USD__monday__london",
        "behavior_type": "london_cable",
        "reason": "Cable survivor used to check whether the same controls behave on a liquid directional London pair.",
    },
    {
        "node": "USD_CHF__monday__london",
        "behavior_type": "london_defensive_major",
        "reason": "Defensive major survivor included to widen the cohort with a different volatility profile.",
    },
    {
        "node": "AUD_CAD__thursday__london",
        "behavior_type": "thursday_cross_london",
        "reason": "Thursday London cross added so the widened cohort is not overly concentrated in Monday behavior.",
    },
    {
        "node": "NZD_JPY__monday__london",
        "behavior_type": "london_jpy_cross",
        "reason": "JPY cross survivor added for another noisy mixed-regime profile in the expanded cohort.",
    },
]

MINIMAL_SPEC: dict[str, Any] = {
    "version": "aee_minimal_v1_2026_03_19",
    "design_principle": "Universal outcome control on frozen entry survivors only.",
    "decision_order": [
        "early_profit_lock",
        "pre_sl_protection",
        "weak_trade_kill",
        "static_fallback",
    ],
    "controls": {
        "early_profit_lock": {
            "enabled": True,
            "arm_mfe_r": 0.60,
            "giveback_r": 0.35,
            "minimum_exit_r": 0.15,
            "description": "Lock a modest winner before it round-trips after enough open profit has been proven.",
        },
        "pre_sl_protection": {
            "enabled": True,
            "loss_r": -0.70,
            "min_bars_open": 4,
            "min_bars_without_progress": 3,
            "min_opposite_pressure": 1.10,
            "max_energy_ratio": 0.05,
            "description": "Exit a slow bleed before full structural loss when pressure and weak energy align against the trade.",
        },
        "weak_trade_kill": {
            "enabled": True,
            "min_bars_open": 8,
            "max_mfe_r": 0.20,
            "max_profit_r": 0.05,
            "max_energy_ratio": 0.10,
            "description": "Close dead-money trades that have failed to prove themselves after a short evaluation window.",
        },
    },
    "metric_definitions": {
        "win_count": "realized_R > 0",
        "loss_count": "realized_R < 0",
        "small_win_count": "0 < realized_R < 1.0",
        "full_loss_count": "realized_R <= -0.99",
        "expectancy_R": "mean realized_R over the same frozen trade set",
        "avg_exit_bar_capped": "mean exit bar, capped to the observed state-stream horizon when static exit lies beyond that horizon",
    },
}


@dataclass
class TradeResult:
    trade_id: str
    static_pips: float
    static_r: float
    static_exit_bar_capped: int
    aee_pips: float
    aee_r: float
    aee_reason: str
    exit_bar_capped: int


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_allowlist() -> dict[str, Any]:
    return json.loads(ALLOWLIST_PATH.read_text())


def node_dir(node: str) -> Path:
    return ROOT / "compiled_market_nodes" / node


def selected_population_path(node: str) -> Path:
    return node_dir(node) / "aee_stage" / "aee_state_stream" / "selected_entry_population.csv"


def state_stream_path(node: str) -> Path:
    return node_dir(node) / "aee_stage" / "aee_state_stream" / "aee_state_stream.csv"


def iter_csv(path: Path) -> Iterator[dict[str, str]]:
    with path.open() as f:
        yield from csv.DictReader(f)


def iter_trade_groups(path: Path) -> Iterator[list[dict[str, str]]]:
    current_trade_id: str | None = None
    bucket: list[dict[str, str]] = []
    for row in iter_csv(path):
        trade_id = row["trade_id"]
        if current_trade_id is None:
            current_trade_id = trade_id
        if trade_id != current_trade_id:
            yield bucket
            bucket = []
            current_trade_id = trade_id
        bucket.append(row)
    if bucket:
        yield bucket


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def summarize_node_profile(node: str, behavior_type: str, reason: str) -> dict[str, Any]:
    rows = list(iter_csv(selected_population_path(node)))
    direction_mix = Counter(str(r.get("direction") or r.get("direction_assumed") or "UNKNOWN") for r in rows)
    target_mix = Counter(str(r["target_distance"]) for r in rows)
    static_reasons = Counter(str(r["static_reason"]) for r in rows)
    static_rs = [float(r["static_R"]) for r in rows]
    tp_minutes = [float(r["tp_hit_min"]) for r in rows]
    sl_minutes = [float(r["sl_hit_min"]) for r in rows]
    return {
        "node": node,
        "behavior_type": behavior_type,
        "reason": reason,
        "trade_count": len(rows),
        "static_win_rate": round(sum(1 for r in static_rs if r > 0) / len(static_rs), 6) if static_rs else 0.0,
        "avg_static_R": round(avg(static_rs), 6),
        "avg_static_pips": round(avg([float(r["static_pips"]) for r in rows]), 6) if rows else 0.0,
        "avg_tp_minutes": round(avg(tp_minutes), 3),
        "avg_sl_minutes": round(avg(sl_minutes), 3),
        "direction_mix": dict(direction_mix),
        "target_mix_top": target_mix.most_common(5),
        "static_reason_mix": dict(static_reasons),
        "state_stream_path": str(state_stream_path(node)),
        "selected_entry_population_path": str(selected_population_path(node)),
    }


def decide_minimal_action(row: dict[str, str], spec: dict[str, Any]) -> str:
    target = float(row["target_distance"])
    profit_now = float(row["profit_now"])
    mfe_so_far = float(row["mfe_so_far"])
    giveback_now = float(row["giveback_now"])
    time_open = int(row["time_open"])
    time_since_last_progress = int(row["time_since_last_progress"])
    energy_ratio = float(row["energy_ratio"])
    opposite_pressure = float(row["opposite_direction_strength"])

    profit_lock = spec["controls"]["early_profit_lock"]
    if (
        profit_lock["enabled"]
        and mfe_so_far >= profit_lock["arm_mfe_r"] * target
        and giveback_now >= profit_lock["giveback_r"] * target
        and profit_now >= profit_lock["minimum_exit_r"] * target
    ):
        return "EARLY_PROFIT_LOCK"

    pre_sl = spec["controls"]["pre_sl_protection"]
    if (
        pre_sl["enabled"]
        and profit_now <= pre_sl["loss_r"] * target
        and time_open >= pre_sl["min_bars_open"]
        and time_since_last_progress >= pre_sl["min_bars_without_progress"]
        and opposite_pressure >= pre_sl["min_opposite_pressure"]
        and energy_ratio <= pre_sl["max_energy_ratio"]
    ):
        return "PRE_SL_PROTECT"

    weak = spec["controls"]["weak_trade_kill"]
    if (
        weak["enabled"]
        and time_open >= weak["min_bars_open"]
        and mfe_so_far <= weak["max_mfe_r"] * target
        and profit_now <= weak["max_profit_r"] * target
        and energy_ratio <= weak["max_energy_ratio"]
    ):
        return "WEAK_TRADE_KILL"

    return "HOLD"


def replay_trade(rows: list[dict[str, str]], spec: dict[str, Any]) -> TradeResult:
    first = rows[0]
    target = float(first["target_distance"])
    static_pips = float(first["static_pips"])
    static_r = float(first["static_R"])
    total_bars = int(first["total_bars"])
    static_exit_bar_capped = min(int(first["static_exit_bar"]), total_bars)

    for row in rows:
        action = decide_minimal_action(row, spec)
        if action == "HOLD":
            continue
        realized = max(float(row["profit_now"]), -target)
        return TradeResult(
            trade_id=first["trade_id"],
            static_pips=static_pips,
            static_r=static_r,
            static_exit_bar_capped=static_exit_bar_capped,
            aee_pips=realized,
            aee_r=realized / target,
            aee_reason=action,
            exit_bar_capped=min(int(row["bar_index"]), total_bars),
        )

    return TradeResult(
        trade_id=first["trade_id"],
        static_pips=static_pips,
        static_r=static_r,
        static_exit_bar_capped=static_exit_bar_capped,
        aee_pips=static_pips,
        aee_r=static_r,
        aee_reason="STATIC_FALLBACK",
        exit_bar_capped=static_exit_bar_capped,
    )


def metrics_from_results(results: list[TradeResult], mode: str) -> dict[str, Any]:
    if mode == "static":
        realized_pips = [r.static_pips for r in results]
        realized_r = [r.static_r for r in results]
        exit_bars = [r.static_exit_bar_capped for r in results]
    else:
        realized_pips = [r.aee_pips for r in results]
        realized_r = [r.aee_r for r in results]
        exit_bars = [r.exit_bar_capped for r in results]
    wins = sum(1 for r in realized_r if r > 0)
    losses = sum(1 for r in realized_r if r < 0)
    small_wins = sum(1 for r in realized_r if 0 < r < 1.0)
    full_losses = sum(1 for r in realized_r if r <= -0.99)
    return {
        "trade_count": len(results),
        "win_count": wins,
        "loss_count": losses,
        "small_win_count": small_wins,
        "full_loss_count": full_losses,
        "win_rate": round(wins / len(results), 6) if results else 0.0,
        "avg_realized_pips": round(avg(realized_pips), 6),
        "expectancy_R": round(avg(realized_r), 6),
        "avg_exit_bar_capped": round(avg([float(x) for x in exit_bars]), 6),
    }


def action_frequency(results: list[TradeResult]) -> dict[str, int]:
    return dict(Counter(r.aee_reason for r in results))


def compare_metrics(static_metrics: dict[str, Any], aee_metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "delta_win_count": aee_metrics["win_count"] - static_metrics["win_count"],
        "delta_loss_count": aee_metrics["loss_count"] - static_metrics["loss_count"],
        "delta_small_win_count": aee_metrics["small_win_count"] - static_metrics["small_win_count"],
        "delta_full_loss_count": aee_metrics["full_loss_count"] - static_metrics["full_loss_count"],
        "delta_avg_realized_pips": round(aee_metrics["avg_realized_pips"] - static_metrics["avg_realized_pips"], 6),
        "delta_expectancy_R": round(aee_metrics["expectancy_R"] - static_metrics["expectancy_R"], 6),
        "delta_avg_exit_bar_capped": round(aee_metrics["avg_exit_bar_capped"] - static_metrics["avg_exit_bar_capped"], 6),
    }


def node_comparison(node: str) -> dict[str, Any]:
    results = [replay_trade(rows, MINIMAL_SPEC) for rows in iter_trade_groups(state_stream_path(node))]
    static_metrics = metrics_from_results(results, "static")
    aee_metrics = metrics_from_results(results, "aee")
    return {
        "node": node,
        "static": static_metrics,
        "aee_minimal_v1": aee_metrics,
        "delta": compare_metrics(static_metrics, aee_metrics),
        "aee_action_frequency": action_frequency(results),
    }


def aggregate_comparisons(per_node: list[dict[str, Any]]) -> dict[str, Any]:
    total_static = {
        "trade_count": sum(n["static"]["trade_count"] for n in per_node),
        "win_count": sum(n["static"]["win_count"] for n in per_node),
        "loss_count": sum(n["static"]["loss_count"] for n in per_node),
        "small_win_count": sum(n["static"]["small_win_count"] for n in per_node),
        "full_loss_count": sum(n["static"]["full_loss_count"] for n in per_node),
    }
    total_aee = {
        "trade_count": sum(n["aee_minimal_v1"]["trade_count"] for n in per_node),
        "win_count": sum(n["aee_minimal_v1"]["win_count"] for n in per_node),
        "loss_count": sum(n["aee_minimal_v1"]["loss_count"] for n in per_node),
        "small_win_count": sum(n["aee_minimal_v1"]["small_win_count"] for n in per_node),
        "full_loss_count": sum(n["aee_minimal_v1"]["full_loss_count"] for n in per_node),
    }
    total_trade_count = total_static["trade_count"]
    weighted_static_pips = (
        sum(n["static"]["avg_realized_pips"] * n["static"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )
    weighted_aee_pips = (
        sum(n["aee_minimal_v1"]["avg_realized_pips"] * n["aee_minimal_v1"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )
    weighted_static_r = (
        sum(n["static"]["expectancy_R"] * n["static"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )
    weighted_aee_r = (
        sum(n["aee_minimal_v1"]["expectancy_R"] * n["aee_minimal_v1"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )
    weighted_static_exit = (
        sum(n["static"]["avg_exit_bar_capped"] * n["static"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )
    weighted_aee_exit = (
        sum(n["aee_minimal_v1"]["avg_exit_bar_capped"] * n["aee_minimal_v1"]["trade_count"] for n in per_node) / total_trade_count
        if total_trade_count
        else 0.0
    )

    static_metrics = {
        **total_static,
        "win_rate": round(total_static["win_count"] / total_trade_count, 6) if total_trade_count else 0.0,
        "avg_realized_pips": round(weighted_static_pips, 6),
        "expectancy_R": round(weighted_static_r, 6),
        "avg_exit_bar_capped": round(weighted_static_exit, 6),
    }
    aee_metrics = {
        **total_aee,
        "win_rate": round(total_aee["win_count"] / total_trade_count, 6) if total_trade_count else 0.0,
        "avg_realized_pips": round(weighted_aee_pips, 6),
        "expectancy_R": round(weighted_aee_r, 6),
        "avg_exit_bar_capped": round(weighted_aee_exit, 6),
    }
    return {
        "static": static_metrics,
        "aee_minimal_v1": aee_metrics,
        "delta": compare_metrics(static_metrics, aee_metrics),
    }


def decide_expand(per_node: list[dict[str, Any]], aggregate: dict[str, Any]) -> dict[str, Any]:
    nodes_with_expectancy_uplift = [n["node"] for n in per_node if n["delta"]["delta_expectancy_R"] > 0]
    nodes_with_full_loss_reduction = [n["node"] for n in per_node if n["delta"]["delta_full_loss_count"] < 0]
    nodes_with_worse_expectancy = [n["node"] for n in per_node if n["delta"]["delta_expectancy_R"] < 0]
    agg_delta = aggregate["delta"]
    should_expand = (
        agg_delta["delta_full_loss_count"] < 0
        and agg_delta["delta_expectancy_R"] > 0
        and len(nodes_with_expectancy_uplift) >= 3
        and len(nodes_with_full_loss_reduction) >= 3
    )
    decision = "expand" if should_expand else "refine"
    return {
        "generated_at": now_iso(),
        "decision": decision,
        "basis": {
            "aggregate_delta_expectancy_R": agg_delta["delta_expectancy_R"],
            "aggregate_delta_full_loss_count": agg_delta["delta_full_loss_count"],
            "aggregate_delta_avg_realized_pips": agg_delta["delta_avg_realized_pips"],
            "nodes_with_expectancy_uplift": nodes_with_expectancy_uplift,
            "nodes_with_full_loss_reduction": nodes_with_full_loss_reduction,
            "nodes_with_worse_expectancy": nodes_with_worse_expectancy,
        },
        "rule": "expand only if full losses fall, expectancy improves, and uplift appears across multiple node types",
        "next_step": (
            "Widen to a 10-15 node cohort with the same frozen-entry baseline."
            if should_expand
            else "Refine one minimal control at a time on the same prototype set before widening."
        ),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def stage_c_nodes(allowlist_nodes: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for node in allowlist_nodes:
        if not state_stream_path(node).exists() or not selected_population_path(node).exists():
            continue
        out.append(
            {
                "node": node,
                "behavior_type": "full_survivor_inventory",
                "reason": "Stage C full frozen survivor inventory coverage using existing node-level AEE state streams.",
            }
        )
    return out


def cohort_nodes(preset: str, allowlist_nodes: list[str]) -> list[dict[str, str]]:
    if preset == "prototype":
        return PROTOTYPE_NODES
    if preset == "stage_b":
        return STAGE_B_NODES
    if preset == "stage_c":
        return stage_c_nodes(allowlist_nodes)
    raise ValueError(f"Unknown preset: {preset}")


def artifact_names(preset: str) -> dict[str, str]:
    if preset == "prototype":
        return {
            "node_set": "aee_prototype_node_set.json",
            "spec": "aee_minimal_spec_v1.json",
            "comparison": "aee_replay_comparison_v1.json",
            "decision": "aee_expand_decision.json",
        }
    if preset == "stage_b":
        return {
            "node_set": "aee_stage_b_node_set.json",
            "spec": "aee_minimal_spec_v1_stage_b.json",
            "comparison": "aee_replay_comparison_v2.json",
            "decision": "aee_expand_decision_v2.json",
        }
    if preset == "stage_c":
        return {
            "node_set": "aee_stage_c_node_set.json",
            "spec": "aee_minimal_spec_v1_stage_c.json",
            "comparison": "aee_replay_comparison_v3.json",
            "decision": "aee_expand_decision_v3.json",
        }
    raise ValueError(f"Unknown preset: {preset}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build minimal universal AEE comparisons on frozen entry survivors.")
    ap.add_argument("--preset", choices=["prototype", "stage_b", "stage_c"], default="prototype")
    args = ap.parse_args()

    allowlist = load_allowlist()
    allow_nodes = list(allowlist["allow_nodes"])
    chosen_nodes = cohort_nodes(args.preset, allow_nodes)
    names = artifact_names(args.preset)
    allowed_nodes = set(allowlist["allow_nodes"])
    missing = [n["node"] for n in chosen_nodes if n["node"] not in allowed_nodes]
    if missing:
        raise SystemExit(f"Prototype nodes not in frozen allowlist: {missing}")
    uncovered_allowlist_nodes = sorted(
        node for node in allow_nodes if node not in {item["node"] for item in chosen_nodes}
    )

    prototype_payload = {
        "generated_at": now_iso(),
        "preset": args.preset,
        "source_allowlist": str(ALLOWLIST_PATH),
        "node_count": len(chosen_nodes),
        "allowlist_count": len(allow_nodes),
        "covered_allowlist_count": len(chosen_nodes),
        "uncovered_allowlist_nodes": uncovered_allowlist_nodes,
        "nodes": [
            summarize_node_profile(item["node"], item["behavior_type"], item["reason"])
            for item in chosen_nodes
        ],
    }
    comparison_per_node = [node_comparison(item["node"]) for item in chosen_nodes]
    comparison_payload = {
        "generated_at": now_iso(),
        "preset": args.preset,
        "source_allowlist": str(ALLOWLIST_PATH),
        "spec_version": MINIMAL_SPEC["version"],
        "prototype_nodes": [item["node"] for item in chosen_nodes],
        "aggregate": aggregate_comparisons(comparison_per_node),
        "per_node": comparison_per_node,
    }
    expand_payload = decide_expand(comparison_per_node, comparison_payload["aggregate"])
    expand_payload["preset"] = args.preset

    write_json(ARTIFACTS / names["node_set"], prototype_payload)
    write_json(ARTIFACTS / names["spec"], MINIMAL_SPEC)
    write_json(ARTIFACTS / names["comparison"], comparison_payload)
    write_json(ARTIFACTS / names["decision"], expand_payload)

    print(json.dumps(
        {
            "status": "ok",
            "preset": args.preset,
            "prototype_nodes": [item["node"] for item in chosen_nodes],
            "decision": expand_payload["decision"],
            "aggregate_delta_expectancy_R": comparison_payload["aggregate"]["delta"]["delta_expectancy_R"],
            "aggregate_delta_full_loss_count": comparison_payload["aggregate"]["delta"]["delta_full_loss_count"],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
