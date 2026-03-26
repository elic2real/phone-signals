#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import pickle
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import run_aee_stage_compiler as aee
from entry_contract import build_selected_entries_from_population, validate_canonical_entry_rows


ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET_LOCK = ROOT / "dataset_lock_11_sessions.json"
DEFAULT_ENTRY_POPULATION = ROOT / "compiled_target_entry_classes_no_timeouts_11_sessions" / "target_entry_population.csv"
DEFAULT_SEED_AEE_DIR = ROOT / "compiled_aee_stage_11_sessions_canonical"
DEFAULT_OUTPUT_DIR = ROOT / "compiled_aee_target_local_fixedpop_11_sessions"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def write_pickle(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_inputs_hash(dataset_lock_path: Path, entry_population_path: Path, seed_aee_dir: Path, output_dir: Path) -> str:
    state_stream_path = resolve_state_stream_path(output_dir, seed_aee_dir)
    return hashlib.sha256(
        json.dumps(
            {
                "dataset_lock_hash": sha256_file(dataset_lock_path),
                "entry_population_hash": sha256_file(entry_population_path),
                "seed_rules_hash": sha256_file(seed_aee_dir / "aee_rules" / "aee_rule_derivation_report.json"),
                "seed_explain_hash": sha256_file(seed_aee_dir / "aee_rules" / "aee_rules.json"),
                "state_stream_path": str(state_stream_path),
                "state_stream_hash": sha256_file(state_stream_path),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def resolve_state_stream_path(output_dir: Path, seed_aee_dir: Path) -> Path:
    local_state_stream = output_dir.parent / "aee_stage" / "aee_state_stream" / "aee_state_stream.csv"
    if local_state_stream.exists():
        return local_state_stream
    return seed_aee_dir / "aee_state_stream" / "aee_state_stream.csv"


def qvals(values: list[float]) -> list[float]:
    vals = sorted(values)
    if not vals:
        return []
    picks = [0.25, 0.5, 0.75]
    out = []
    for p in picks:
        idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * p))))
        out.append(float(vals[idx]))
    return out


def narrowed(vals: list[float], fallback: float) -> list[float]:
    if not vals:
        return [float(fallback)]
    uniq: list[float] = []
    for v in vals[1:3]:
        fv = round(float(v), 6)
        if fv not in uniq:
            uniq.append(fv)
    if not uniq:
        uniq = [round(float(vals[-1]), 6)]
    return uniq


def class_signatures(state_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_trade: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_trade[row["trade_id"]].append(row)
    for rows in by_trade.values():
        rows.sort(key=lambda r: int(r["bar_index"]))

    before_keys = [
        "pre_build_slope",
        "pre_build_accel",
        "pre_compression_release_delta",
        "pre_macro_micro_alignment",
        "pre_budget_slope",
        "pre_noise_slope",
        "pre_exhaustion_slope",
        "macro_dir_score",
        "micro_dir_score",
        "compression",
        "release_quality",
        "remaining_budget",
    ]
    during_keys = [
        "profit_now",
        "mfe_so_far",
        "giveback_now",
        "velocity_now",
        "velocity_change",
        "progress_ratio",
        "energy_ratio",
        "noise",
        "exhaustion",
        "opposite_direction_strength",
    ]
    after_keys = [
        "time_since_peak",
        "time_since_last_progress",
        "remaining_budget",
        "noise",
        "exhaustion",
    ]
    before_acc = defaultdict(list)
    during_acc = defaultdict(list)
    after_acc = defaultdict(list)
    for rows in by_trade.values():
        if not rows:
            continue
        first = rows[0]
        middle = rows[: min(3, len(rows))]
        tail = rows[max(0, len(rows) - 3) :]
        for k in before_keys:
            before_acc[k].append(float(first[k]))
        for k in during_keys:
            during_acc[k].append(mean(float(r[k]) for r in middle))
        for k in after_keys:
            after_acc[k].append(mean(float(r[k]) for r in tail))
    return {
        "before": {k: round(mean(v), 6) for k, v in before_acc.items() if v},
        "during": {k: round(mean(v), 6) for k, v in during_acc.items() if v},
        "after": {k: round(mean(v), 6) for k, v in after_acc.items() if v},
    }


def compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    total_static = sum(float(r["static_pips"]) for r in rows)
    total_aee = sum(float(r["aee_pips"]) for r in rows)
    tp_hits = sum(1 for r in rows if r["aee_reason"] == "TP_HIT")
    sl_hits = sum(1 for r in rows if r["aee_reason"] in {"SL_HIT", "PANIC", "DECAY_EXIT"} and float(r["aee_pips"]) < 0)
    timeouts = sum(1 for r in rows if r["aee_reason"] == "TIMEOUT")
    return {
        "trade_count": total,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "timeouts": timeouts,
        "tp_hit_rate": round(tp_hits / total, 6) if total else 0.0,
        "avg_static_pips": round(total_static / total, 6) if total else 0.0,
        "avg_aee_pips": round(total_aee / total, 6) if total else 0.0,
        "avg_static_R": round(aee.mean0([float(r["static_R"]) for r in rows]), 6),
        "avg_aee_R": round(aee.mean0([float(r["aee_R"]) for r in rows]), 6),
        "pips_per_hour": round(total_aee / 88.0, 6),
        "estimated_equity_per_hour": round((total_aee / 2.5) * 2.0 / 88.0, 6),
        "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        "delta_avg_R": round(aee.mean0([float(r["aee_R"]) - float(r["static_R"]) for r in rows]), 6),
    }


def build_candidate_rules(base_rules: dict[str, Any], direction: str, target: str, state_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    harvest_rows = [r for r in state_rows if r["action_truth"] == "HARVEST"]
    hold_rows = [r for r in state_rows if r["action_truth"] in {"HOLD", "EXTEND"}]
    decay_rows = [r for r in state_rows if r["action_truth"] == "DECAY_EXIT"]

    harvest_profit = narrowed(
        qvals([float(r["profit_now"]) for r in harvest_rows]),
        base_rules["direction_modifiers"][direction]["harvest_profit_floor"],
    )
    harvest_gb = narrowed(
        qvals([float(r["giveback_now"]) for r in harvest_rows]),
        base_rules["target_modifiers"].get(target, {}).get("harvest_giveback_tolerance", 0.0),
    )
    ext_budget = narrowed(
        qvals([float(r["remaining_budget"]) for r in hold_rows]),
        base_rules["target_modifiers"].get(target, {}).get("extension_budget_floor", 0.0),
    )
    decay_tsp = narrowed(
        qvals([float(r["time_since_peak"]) for r in decay_rows]),
        base_rules["target_modifiers"].get(target, {}).get("decay_time_since_peak", 0.0),
    )

    candidates = []
    for hp in harvest_profit:
        for hgb in harvest_gb:
            for eb in ext_budget:
                for dt in decay_tsp:
                    rules = copy.deepcopy(base_rules)
                    rules["direction_modifiers"][direction]["harvest_profit_floor"] = round(float(hp), 6)
                    tmod = rules["target_modifiers"].setdefault(target, {})
                    tmod["harvest_giveback_tolerance"] = round(float(hgb), 6)
                    tmod["extension_budget_floor"] = round(float(eb), 6)
                    tmod["decay_time_since_peak"] = round(float(dt), 6)
                    candidates.append(rules)
    return candidates


def run(dataset_lock_path: Path, entry_population_path: Path, seed_aee_dir: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "target_local_fixedpop_aee_report.json"
    summary_path = output_dir / "target_local_fixedpop_aee_summary.csv"
    classes_path = output_dir / "target_local_fixedpop_aee_classes.json"
    trade_rows_path = output_dir / "target_local_fixedpop_aee_trade_rows.json"
    state_stream_path = output_dir / "aee_state_stream.csv"
    manifest_path = output_dir / "local_fixedpop_manifest.json"
    entry_population_cache_path = output_dir / "target_entry_population.pkl"
    state_stream_cache_path = output_dir / "aee_state_stream.pkl"
    inputs_hash = build_inputs_hash(dataset_lock_path, entry_population_path, seed_aee_dir, output_dir)

    if (
        report_path.exists()
        and summary_path.exists()
        and classes_path.exists()
        and trade_rows_path.exists()
        and state_stream_path.exists()
        and manifest_path.exists()
    ):
        try:
            manifest = load_json(manifest_path)
        except Exception:
            manifest = {}
        if manifest.get("inputs_hash") == inputs_hash:
            return {
                "status": "SKIP",
                "reason": "local_fixedpop_artifacts_current",
                "inputs_hash": inputs_hash,
            }

    seed_rules = load_json(seed_aee_dir / "aee_rules" / "aee_rule_derivation_report.json")
    explain_rules = load_json(seed_aee_dir / "aee_rules" / "aee_rules.json")
    if entry_population_cache_path.exists():
        entry_rows = load_pickle(entry_population_cache_path)
    else:
        entry_rows = load_csv(entry_population_path)
        write_pickle(entry_population_cache_path, entry_rows)
    selected_entries = build_selected_entries_from_population(entry_rows)
    validate_canonical_entry_rows(selected_entries)
    
    # Prefer the node-local AEE state stream so trade_ids align with the selected entry population.
    canonical_state_stream_path = resolve_state_stream_path(output_dir, seed_aee_dir)
    if not canonical_state_stream_path.exists():
        raise FileNotFoundError(f"Canonical AEE state stream not found at {canonical_state_stream_path}. "
                              "Ensure run_aee_stage_compiler.py has been run first.")
    if state_stream_cache_path.exists():
        state_rows = load_pickle(state_stream_cache_path)
    else:
        state_rows = load_csv(canonical_state_stream_path)
        write_pickle(state_stream_cache_path, state_rows)
    
    # Copy to output directory for reproducibility
    aee.write_csv(state_stream_path, state_rows, list(state_rows[0].keys()) if state_rows else ["trade_id"])
    
    # Build trades from selected entries (not state rows)
    trades: list[dict[str, Any]] = []
    for entry in selected_entries:
        trades.append(dict(entry))

    by_trade_states: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_trade_states[row["trade_id"]].append(row)
    for rows in by_trade_states.values():
        rows.sort(key=lambda r: int(r["bar_index"]))
    trade_meta = {trade["trade_id"]: trade for trade in trades}

    by_class_trades: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        by_class_trades[(trade["direction"], str(float(trade["target_distance"])))].append(trade)

    class_reports: dict[str, Any] = {}
    merged_rows: list[dict[str, Any]] = []

    for (direction, target), subset_trades in sorted(by_class_trades.items()):
        subset_states = []
        for tr in subset_trades:
            subset_states.extend(by_trade_states[tr["trade_id"]])
        subset_context = aee.ReplayContext(
            trades=subset_trades,
            by_trade={tr["trade_id"]: by_trade_states.get(tr["trade_id"], []) for tr in subset_trades},
            trade_meta={tr["trade_id"]: trade_meta[tr["trade_id"]] for tr in subset_trades},
        )

        signatures = class_signatures(subset_states)
        baseline = aee.replay_variant_with_context(subset_context, seed_rules, "bias_plus_context_aee")
        best_result = baseline
        best_rules = copy.deepcopy(seed_rules)
        seen = set()
        for rules in build_candidate_rules(seed_rules, direction, target, subset_states):
            sig = json.dumps(rules["direction_modifiers"][direction], sort_keys=True) + json.dumps(rules["target_modifiers"].get(target, {}), sort_keys=True)
            if sig in seen:
                continue
            seen.add(sig)
            result = aee.replay_variant_with_context(subset_context, rules, "bias_plus_context_aee")
            if (
                result.metrics["pips_per_hour"] > best_result.metrics["pips_per_hour"]
                or (
                    result.metrics["pips_per_hour"] == best_result.metrics["pips_per_hour"]
                    and result.metrics["avg_aee_R"] > best_result.metrics["avg_aee_R"]
                )
            ):
                best_result = result
                best_rules = rules

        class_key = f"{direction}_{target}"
        class_reports[class_key] = {
            "direction": direction,
            "target_distance": target,
            "signatures": signatures,
            "metrics": best_result.metrics,
            "rules": {
                "direction_modifiers": best_rules["direction_modifiers"][direction],
                "target_modifiers": best_rules["target_modifiers"].get(target, {}),
                "base_decay_conditions": explain_rules["base_rules"][1]["conditions"],
            },
        }
        merged_rows.extend(best_result.trade_rows)

    aggregate = compute_metrics(merged_rows)
    report = {"aggregate_metrics": aggregate, "class_reports": class_reports}
    report_path.write_text(json.dumps(report, indent=2))
    trade_rows_path.write_text(json.dumps(merged_rows, indent=2))
    classes_path.write_text(json.dumps({k: v["rules"] for k, v in class_reports.items()}, indent=2))

    with summary_path.open("w", newline="") as f:
        fieldnames = [
            "direction", "target_distance", "trade_count", "tp_hits", "sl_hits", "timeouts",
            "tp_hit_rate", "avg_aee_pips", "avg_aee_R", "pips_per_hour", "estimated_equity_per_hour", "delta_pips_per_hour",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for payload in class_reports.values():
            row = {"direction": payload["direction"], "target_distance": payload["target_distance"], **payload["metrics"]}
            w.writerow({k: row.get(k, "") for k in fieldnames})

    manifest_path.write_text(
        json.dumps(
            {
                "runner": Path(__file__).name,
                "inputs_hash": inputs_hash,
                "dataset_lock": str(dataset_lock_path),
                "entry_population": str(entry_population_path),
                "seed_aee_dir": str(seed_aee_dir),
                "report": str(report_path),
            },
            indent=2,
        )
    )
    return {"status": "PASS", "aggregate_pips_per_hour": aggregate["pips_per_hour"], "class_count": len(class_reports)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, default=DEFAULT_DATASET_LOCK)
    ap.add_argument("--entry-population", type=Path, default=DEFAULT_ENTRY_POPULATION)
    ap.add_argument("--seed-aee-dir", type=Path, default=DEFAULT_SEED_AEE_DIR)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = ap.parse_args()
    print(json.dumps(run(args.dataset_lock, args.entry_population, args.seed_aee_dir, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
