#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aee_historical_system_scoreboard as hs


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _pair_from_stream(path: Path) -> str:
    parts = path.parts
    if "compiled_market_nodes" in parts:
        i = parts.index("compiled_market_nodes")
        if i + 1 < len(parts):
            node = parts[i + 1]
            return node.split("__", 1)[0]
    return "GLOBAL"


def _load_seed_configs(sweep_path: Path, top_n: int) -> list[tuple[str, Path]]:
    obj = json.loads(sweep_path.read_text(encoding="utf-8"))
    seeds: list[tuple[str, Path]] = []
    for row in obj.get("eligible_ranked_configs", [])[:top_n]:
        name = str(row.get("name", "")).strip()
        p = Path(str(row.get("config_path", "")).strip())
        if name and p.exists():
            seeds.append((name, p))
    return seeds


def _set_rule_value(cfg: dict[str, Any], rule_id: str, key: str, factor: float, min_value: float = 0.0) -> None:
    for rule in cfg.get("base_rules", []):
        if str(rule.get("rule_id", "")) == rule_id:
            cond = rule.setdefault("conditions", {})
            base = hs._safe_float(cond.get(key, 0.0))
            cond[key] = round(max(min_value, base * factor), 6)
            return


def _set_direction_value(cfg: dict[str, Any], direction: str, key: str, factor: float, min_value: float = 0.0) -> None:
    dmods = cfg.setdefault("direction_modifiers", {})
    if direction not in dmods or not isinstance(dmods.get(direction), dict):
        return
    base = hs._safe_float(dmods[direction].get(key, 0.0))
    dmods[direction][key] = round(max(min_value, base * factor), 6)


def _mutate_config(base_cfg: dict[str, Any], mutation: dict[str, float]) -> dict[str, Any]:
    cfg = copy.deepcopy(base_cfg)

    proving_factor = mutation.get("proving_window_factor", 1.0)
    decay_factor = mutation.get("decay_tsp_factor", 1.0)
    harvest_gb_factor = mutation.get("harvest_giveback_factor", 1.0)
    panic_time_open_factor = mutation.get("panic_time_open_factor", 1.0)
    base_decay_tsp_factor = mutation.get("base_decay_tsp_factor", 1.0)
    harvest_profit_floor_factor = mutation.get("harvest_profit_floor_factor", 1.0)

    _set_rule_value(cfg, "base_panic", "time_open_min", panic_time_open_factor, min_value=3.0)
    _set_rule_value(cfg, "base_decay", "time_since_peak_min", base_decay_tsp_factor, min_value=2.0)
    _set_direction_value(cfg, "LONG", "harvest_profit_floor", harvest_profit_floor_factor, min_value=0.5)
    _set_direction_value(cfg, "SHORT", "harvest_profit_floor", harvest_profit_floor_factor, min_value=0.5)

    for target_mod in (cfg.get("target_modifiers", {}) or {}).values():
        pv = hs._safe_int(target_mod.get("proving_window", 2), 2)
        target_mod["proving_window"] = max(1, min(6, int(round(pv * proving_factor))))

        dt = hs._safe_float(target_mod.get("decay_time_since_peak", 0.0), 0.0)
        target_mod["decay_time_since_peak"] = round(max(1.0, dt * decay_factor), 6)

        gb = hs._safe_float(target_mod.get("harvest_giveback_tolerance", 0.0), 0.0)
        target_mod["harvest_giveback_tolerance"] = round(max(0.1, gb * harvest_gb_factor), 6)

    return cfg


def _mutation_library(family: str) -> list[tuple[str, dict[str, float]]]:
    if family == "decay_cap":
        return [
            ("baseline", {}),
            ("ultra_early_decay", {"proving_window_factor": 0.65, "decay_tsp_factor": 0.65, "base_decay_tsp_factor": 0.7}),
            ("fastest_safe", {"proving_window_factor": 0.6, "decay_tsp_factor": 0.6, "harvest_giveback_factor": 0.75, "panic_time_open_factor": 0.8, "base_decay_tsp_factor": 0.7}),
            ("cap_decay_extreme", {"proving_window_factor": 0.55, "decay_tsp_factor": 0.55, "base_decay_tsp_factor": 0.65, "harvest_giveback_factor": 0.8}),
        ]
    if family == "early_harvest":
        return [
            ("harvest_hard_1", {"proving_window_factor": 0.65, "harvest_giveback_factor": 0.65, "harvest_profit_floor_factor": 0.75, "decay_tsp_factor": 0.7}),
            ("harvest_hard_2", {"proving_window_factor": 0.6, "harvest_giveback_factor": 0.6, "harvest_profit_floor_factor": 0.7, "decay_tsp_factor": 0.7, "base_decay_tsp_factor": 0.75}),
            ("forced_partial_proxy_1", {"proving_window_factor": 0.55, "harvest_giveback_factor": 0.55, "harvest_profit_floor_factor": 0.65, "decay_tsp_factor": 0.65, "base_decay_tsp_factor": 0.7}),
            ("forced_partial_proxy_2", {"proving_window_factor": 0.5, "harvest_giveback_factor": 0.5, "harvest_profit_floor_factor": 0.6, "decay_tsp_factor": 0.6, "base_decay_tsp_factor": 0.65, "panic_time_open_factor": 0.8}),
        ]
    return [
        ("baseline", {}),
        ("faster_decay", {"decay_tsp_factor": 0.85, "base_decay_tsp_factor": 0.9}),
        ("faster_decay_tight_gb", {"decay_tsp_factor": 0.8, "base_decay_tsp_factor": 0.85, "harvest_giveback_factor": 0.9}),
        ("early_proving", {"proving_window_factor": 0.8}),
        ("early_proving_decay", {"proving_window_factor": 0.8, "decay_tsp_factor": 0.85}),
        ("tight_harvest_gb", {"harvest_giveback_factor": 0.85}),
    ]


def _prepare_stream_contexts(stream_paths: list[Path], usd_per_pip: float) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for sp in stream_paths:
        rows = hs._load_state_rows(sp)
        if not rows:
            continue

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            by_trade[str(row.get("trade_id", "")).strip()].append(row)
        for t_rows in by_trade.values():
            t_rows.sort(key=lambda r: hs._safe_int(r.get("bar_index", 0), 0))

        duration_hr = hs._window_duration_hours(rows)
        static_outcomes = [hs._evaluate_static_trade(t_rows) for t_rows in by_trade.values() if t_rows]
        static_metrics = hs._compute_metrics(static_outcomes, duration_hr, usd_per_pip)
        static_result = {
            "name": "static_baseline",
            "metrics": static_metrics,
            "rejected": False,
            "rejection_reasons": [],
        }
        contexts.append(
            {
                "path": sp,
                "pair": _pair_from_stream(sp),
                "by_trade": by_trade,
                "duration_hr": duration_hr,
                "static_result": static_result,
                "static_metrics": static_metrics,
            }
        )
    return contexts


def _score_candidate(
    cfg_name: str,
    cfg_path: Path,
    cfg_obj: dict[str, Any],
    mutation_name: str,
    mutation_payload: dict[str, float],
    contexts: list[dict[str, Any]],
    usd_per_pip: float,
    min_delta_pph: float,
    max_hold_ratio: float,
    min_stream_wins: int,
    min_stream_win_rate: float,
    min_win_pair_count: int,
) -> dict[str, Any]:
    stream_rows: list[dict[str, Any]] = []
    win_pairs: set[str] = set()
    sum_metrics = defaultdict(float)
    sum_delta = defaultdict(float)

    for ctx in contexts:
        outcomes = [hs._evaluate_aee_trade(t_rows, cfg_obj) for t_rows in ctx["by_trade"].values() if t_rows]
        metrics = hs._compute_metrics(outcomes, ctx["duration_hr"], usd_per_pip)
        result = {"name": cfg_name, "metrics": metrics}
        rejected, reject_reasons = hs._apply_rejection_rules(result, ctx["static_result"])
        delta_pph = metrics.get("realized_pips_per_hour", 0.0) - ctx["static_metrics"].get("realized_pips_per_hour", 0.0)
        hold_ratio = (
            metrics.get("avg_hold_sec", 0.0) / max(1.0, ctx["static_metrics"].get("avg_hold_sec", 0.0))
            if ctx["static_metrics"].get("avg_hold_sec", 0.0) > 0
            else 1.0
        )
        stream_win = (
            (not rejected)
            and delta_pph >= min_delta_pph
            and metrics.get("avg_realized_r", 0.0) > 0.0
            and hold_ratio <= max_hold_ratio
        )
        if stream_win:
            win_pairs.add(ctx["pair"])

        row = {
            "stream_path": str(ctx["path"]),
            "pair": ctx["pair"],
            "stream_win": stream_win,
            "rejected": rejected,
            "rejection_reasons": reject_reasons,
            "metrics": metrics,
            "delta_vs_static": {
                "realized_pips_per_hour": delta_pph,
                "realized_usd_per_hour": metrics.get("realized_usd_per_hour", 0.0) - ctx["static_metrics"].get("realized_usd_per_hour", 0.0),
                "avg_realized_r": metrics.get("avg_realized_r", 0.0) - ctx["static_metrics"].get("avg_realized_r", 0.0),
                "avg_hold_sec": metrics.get("avg_hold_sec", 0.0) - ctx["static_metrics"].get("avg_hold_sec", 0.0),
                "hold_ratio": hold_ratio,
            },
        }
        stream_rows.append(row)

        for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "capital_recycling_rate"):
            sum_metrics[k] += metrics.get(k, 0.0)
        for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "hold_ratio"):
            sum_delta[k] += row["delta_vs_static"].get(k, 0.0)

    n = max(1, len(stream_rows))
    stream_win_count = sum(1 for r in stream_rows if r.get("stream_win", False))
    stream_win_rate = stream_win_count / n

    promoted = (
        stream_win_count >= min_stream_wins
        and stream_win_rate >= min_stream_win_rate
        and (sum_metrics["avg_realized_r"] / n) > 0.0
        and (sum_delta["realized_pips_per_hour"] / n) >= min_delta_pph
        and (sum_delta["hold_ratio"] / n) <= max_hold_ratio
        and len(win_pairs - {"GLOBAL"}) >= min_win_pair_count
    )

    return {
        "seed_name": cfg_name,
        "seed_config_path": str(cfg_path),
        "mutation_name": mutation_name,
        "mutation": mutation_payload,
        "stream_count": n,
        "stream_win_count": stream_win_count,
        "stream_win_rate": stream_win_rate,
        "win_pair_count": len(win_pairs - {"GLOBAL"}),
        "avg_metrics": {k: (sum_metrics[k] / n) for k in sum_metrics},
        "avg_delta_vs_static": {k: (sum_delta[k] / n) for k in sum_delta},
        "promoted": promoted,
        "stream_rows": stream_rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Narrow timing-only hold-efficiency optimizer with strict robustness gates.")
    ap.add_argument("--seed-sweep", default="aee_historical_system_scoreboard_sweep.json", help="Sweep JSON used to source top seed configs.")
    ap.add_argument("--top-seeds", type=int, default=5, help="Number of top seed configs from sweep file.")
    ap.add_argument("--state-stream-glob", action="append", default=[], help="State stream globs for robustness scoring.")
    ap.add_argument("--max-streams", type=int, default=40, help="Cap number of streams to score for runtime control.")
    ap.add_argument("--usd-per-pip", type=float, default=0.8)
    ap.add_argument("--min-delta-pph", type=float, default=0.03)
    ap.add_argument("--max-hold-ratio", type=float, default=1.5)
    ap.add_argument("--min-stream-wins", type=int, default=4)
    ap.add_argument("--min-stream-win-rate", type=float, default=0.6)
    ap.add_argument("--min-win-pairs", type=int, default=2)
    ap.add_argument(
        "--mutation-family",
        action="append",
        default=[],
        help="Mutation family selector (repeatable): decay_cap, early_harvest. If omitted, legacy mixed set is used.",
    )
    ap.add_argument("--out", default="aee_hold_efficiency_optimizer_report.json")
    ap.add_argument("--write-best-config", default="", help="Optional path to write best promoted config JSON.")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    sweep_path = Path(args.seed_sweep)
    if not sweep_path.is_absolute():
        sweep_path = (root / sweep_path).resolve()
    if not sweep_path.exists():
        raise SystemExit(f"seed sweep json not found: {sweep_path}")

    seeds = _load_seed_configs(sweep_path, int(args.top_seeds))
    if not seeds:
        raise SystemExit("no seed configs resolved from sweep file")

    if args.state_stream_glob:
        stream_paths: list[Path] = []
        for g in args.state_stream_glob:
            stream_paths.extend(hs._resolve_streams_from_globs([g], root))
        stream_paths = hs._dedupe_paths(stream_paths)
    else:
        stream_paths = hs._resolve_streams_from_globs(["compiled_aee_stage_11_sessions*/aee_state_stream/aee_state_stream.csv"], root)

    if not stream_paths:
        raise SystemExit("no state streams resolved")
    stream_paths = stream_paths[: max(1, int(args.max_streams))]

    contexts = _prepare_stream_contexts(stream_paths, float(args.usd_per_pip))
    if not contexts:
        raise SystemExit("all selected state streams are empty")

    candidates: list[dict[str, Any]] = []
    if args.mutation_family:
        mutation_lib: list[tuple[str, dict[str, float]]] = []
        for fam in args.mutation_family:
            mutation_lib.extend(_mutation_library(str(fam).strip()))
    else:
        mutation_lib = _mutation_library("legacy")

    for seed_name, seed_path in seeds:
        seed_cfg = hs._read_json(seed_path)
        for mut_name, mut_payload in mutation_lib:
            cfg_obj = _mutate_config(seed_cfg, mut_payload)
            scored = _score_candidate(
                cfg_name=seed_name,
                cfg_path=seed_path,
                cfg_obj=cfg_obj,
                mutation_name=mut_name,
                mutation_payload=mut_payload,
                contexts=contexts,
                usd_per_pip=float(args.usd_per_pip),
                min_delta_pph=float(args.min_delta_pph),
                max_hold_ratio=float(args.max_hold_ratio),
                min_stream_wins=int(args.min_stream_wins),
                min_stream_win_rate=float(args.min_stream_win_rate),
                min_win_pair_count=int(args.min_win_pairs),
            )
            scored["_config_obj"] = cfg_obj
            candidates.append(scored)

    ranked = sorted(
        candidates,
        key=lambda r: (
            1 if r.get("promoted", False) else 0,
            r.get("stream_win_rate", 0.0),
            r.get("avg_delta_vs_static", {}).get("realized_pips_per_hour", 0.0),
            r.get("avg_metrics", {}).get("avg_realized_r", 0.0),
            -r.get("avg_delta_vs_static", {}).get("hold_ratio", 999.0),
        ),
        reverse=True,
    )

    promoted = [r for r in ranked if r.get("promoted", False)]

    out_payload = {
        "generated_at": _iso_now(),
        "seed_sweep": str(sweep_path),
        "seed_count": len(seeds),
        "mutation_count_per_seed": len(mutation_lib),
        "candidate_count": len(ranked),
        "stream_count": len(contexts),
        "streams": [str(c["path"]) for c in contexts],
        "gates": {
            "min_delta_pph": float(args.min_delta_pph),
            "max_hold_ratio": float(args.max_hold_ratio),
            "min_stream_wins": int(args.min_stream_wins),
            "min_stream_win_rate": float(args.min_stream_win_rate),
            "min_win_pairs": int(args.min_win_pairs),
        },
        "ranked_candidates": [
            {
                k: v
                for k, v in row.items()
                if k != "_config_obj"
            }
            for row in ranked
        ],
        "promoted_candidates": [
            {
                k: v
                for k, v in row.items()
                if k != "_config_obj"
            }
            for row in promoted
        ],
    }

    if args.mutation_family:
        out_payload["timing_only_family_status"] = (
            "structurally_non_promotable_under_current_path_logic"
            if not promoted
            else "timing_family_promotable"
        )
        out_payload["timing_only_recommendation"] = (
            "escalate_to_path_shape_or_band_trigger_logic_changes"
            if not promoted
            else "candidate_ready_for_next_validation_stage"
        )

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()
    out_path.write_text(json.dumps(out_payload, indent=2) + "\n", encoding="utf-8")

    if args.write_best_config and promoted:
        best = promoted[0]
        best_cfg = best.get("_config_obj")
        if isinstance(best_cfg, dict):
            best_path = Path(args.write_best_config)
            if not best_path.is_absolute():
                best_path = (root / best_path).resolve()
            best_path.write_text(json.dumps(best_cfg, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "out": str(out_path),
                "candidate_count": len(ranked),
                "stream_count": len(contexts),
                "promoted_count": len(promoted),
                "best_promoted": promoted[0]["seed_name"] + "::" + promoted[0]["mutation_name"] if promoted else None,
                "best_candidate": ranked[0]["seed_name"] + "::" + ranked[0]["mutation_name"] if ranked else None,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
