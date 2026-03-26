#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from run_aee_band_floor_baseline import (  # Reuse existing evaluator/helpers without changing AEE logic.
    _context_from_stream,
    _entry_filter_reason,
    _eval_trade_baseline,
    _max_profit_within_window,
    _parse_ts,
    _row_at_or_after_bar,
    _safe_float,
    _safe_int,
    _stream_duration_hours,
)

ENTRY_TYPES = [
    "EXPANSION_BREAKOUT",
    "RECLAIM_CONTINUATION",
    "PULLBACK_CONTINUATION",
    "RANGE_ESCAPE",
    "OTHER",
]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    idx = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * q))))
    return float(vals[idx])


def _dominant(counter: Counter, k: int = 3) -> list[dict[str, Any]]:
    total = sum(counter.values())
    out: list[dict[str, Any]] = []
    for key, cnt in counter.most_common(k):
        out.append(
            {
                "name": str(key),
                "count": int(cnt),
                "share": (float(cnt) / total) if total > 0 else 0.0,
            }
        )
    return out


def _friction_per_trade(cfg: dict[str, Any], spread: float, slip: float, commission: float, latency: float) -> float:
    extraction = cfg.get("extraction") or {}
    spread_override = extraction.get("open_spread_pips", spread)
    return (
        max(0.0, float(spread_override))
        + (2.0 * max(0.0, float(slip)))
        + max(0.0, float(commission))
        + max(0.0, float(latency))
    )


def _infer_label(
    trows: list[dict[str, str]],
    td: float,
) -> tuple[str, str, list[str], dict[str, Any]]:
    first = trows[0]
    bar2 = _row_at_or_after_bar(trows, 2)
    bar3 = _row_at_or_after_bar(trows, 3)

    compression = _safe_float(first.get("compression", 0.0), 0.0)
    release_quality = _safe_float(first.get("release_quality", 0.0), 0.0)
    noise = _safe_float(first.get("noise", 1.0), 1.0)
    pre_align = _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)
    pre_rel = _safe_float(first.get("pre_compression_release_delta", 0.0), 0.0)
    pre_slope = _safe_float(first.get("pre_build_slope", 0.0), 0.0)
    pre_accel = _safe_float(first.get("pre_build_accel", 0.0), 0.0)
    pre_noise_slope = _safe_float(first.get("pre_noise_slope", 0.0), 0.0)
    pre_budget_slope = _safe_float(first.get("pre_budget_slope", 0.0), 0.0)
    macro_dir = _safe_float(first.get("macro_dir_score", 0.0), 0.0)
    micro_dir = _safe_float(first.get("micro_dir_score", 0.0), 0.0)
    progress2 = _safe_float(bar2.get("progress_ratio", 0.0), 0.0)
    progress3 = _safe_float(bar3.get("progress_ratio", 0.0), 0.0)
    pmax_2m = _max_profit_within_window(trows, 120.0)
    pmax_3m = _max_profit_within_window(trows, 180.0)
    life = str(first.get("lifecycle_label", "")).upper().strip()

    early = trows[: min(4, len(trows))]
    early_pips = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in early]
    early_range = (max(early_pips) - min(early_pips)) if early_pips else 0.0
    monotonic_push = sum(1 for i in range(1, len(early_pips)) if early_pips[i] >= early_pips[i - 1] - 0.05) >= max(1, len(early_pips) - 1)

    reclaim_score = 0
    pullback_score = 0
    breakout_score = 0
    range_score = 0

    reclaim_reasons: list[str] = []
    pullback_reasons: list[str] = []
    breakout_reasons: list[str] = []
    range_reasons: list[str] = []

    if pre_align < -0.08:
        reclaim_score += 2
        reclaim_reasons.append("pre_alignment_negative_then_recovery_context")
    if pre_rel > 0.06:
        reclaim_score += 1
        reclaim_reasons.append("pre_release_delta_positive")
    if progress2 > 0.03:
        reclaim_score += 1
        reclaim_reasons.append("progress_recovered_by_bar2")
    if progress3 > progress2 + 0.08:
        reclaim_score += 1
        reclaim_reasons.append("late_recovery_after_early_drag")
    if abs(macro_dir) < 0.12 and abs(micro_dir) > 0.12:
        reclaim_score += 1
        reclaim_reasons.append("micro_reclaims_macro_flat_context")
    # Mechanical reclaim proxy: weak early progress followed by decisive recross-like recovery.
    if progress2 < 0.10 and progress3 > 0.30 and pre_align < -0.02:
        reclaim_score += 2
        reclaim_reasons.append("early_drag_then_recross_recovery")

    if abs(pre_slope) > 0.03:
        pullback_score += 1
        pullback_reasons.append("pre_build_slope_directional")
    if pre_accel < -0.01:
        pullback_score += 1
        pullback_reasons.append("pre_build_accel_negative_pullback")
    if pre_noise_slope < 0.0:
        pullback_score += 1
        pullback_reasons.append("noise_falling_into_entry")
    if 0.06 <= progress2 <= 0.55:
        pullback_score += 1
        pullback_reasons.append("moderate_early_progress")
    if progress3 > progress2 + 0.06:
        pullback_score += 1
        pullback_reasons.append("post_pullback_resume_progress")
    if life in {"FRAGILE", "PROVING"}:
        pullback_score += 1
        pullback_reasons.append("fragile_or_proving_lifecycle")
    # Mechanical pullback proxy from observed shape frequency.
    if progress2 < 0.12 and progress3 > 0.25:
        pullback_score += 2
        pullback_reasons.append("late_resume_after_shallow_early_progress")

    if release_quality >= 0.12:
        breakout_score += 1
        breakout_reasons.append("release_quality_high")
    if pre_rel >= 0.08:
        breakout_score += 1
        breakout_reasons.append("compression_release_delta_high")
    if compression <= 0.58:
        breakout_score += 1
        breakout_reasons.append("compression_not_high")
    if progress2 >= 0.05:
        breakout_score += 1
        breakout_reasons.append("early_progress_present")
    if monotonic_push:
        breakout_score += 1
        breakout_reasons.append("early_monotonic_push")
    if pre_budget_slope >= 0.0:
        breakout_score += 1
        breakout_reasons.append("budget_not_degrading_into_entry")

    if compression >= 0.60:
        range_score += 2
        range_reasons.append("compression_high_local_box")
    if noise <= 0.78:
        range_score += 1
        range_reasons.append("noise_not_extreme")
    if early_range <= max(0.8, 0.35 * td):
        range_score += 1
        range_reasons.append("early_range_tight")
    if pmax_2m >= 0.15 and pmax_3m >= pmax_2m:
        range_score += 1
        range_reasons.append("post_box_escape_followthrough")

    if reclaim_score >= 4:
        return (
            "RECLAIM_CONTINUATION",
            "high" if reclaim_score >= 4 else "medium",
            reclaim_reasons,
            {
                "final_blocker_before_entry": "reclaim_pending_resolved",
                "require_break_cross": True,
                "require_reclaim": True,
                "require_pullback": False,
            },
        )

    if pullback_score >= 4 and reclaim_score < 4:
        return (
            "PULLBACK_CONTINUATION",
            "high" if pullback_score >= 4 else "medium",
            pullback_reasons,
            {
                "final_blocker_before_entry": "pullback_pending_resolved",
                "require_break_cross": True,
                "require_reclaim": False,
                "require_pullback": True,
            },
        )

    if breakout_score >= 5 and reclaim_score < 4 and pullback_score < 5:
        return (
            "EXPANSION_BREAKOUT",
            "high" if breakout_score >= 5 else "medium",
            breakout_reasons,
            {
                "final_blocker_before_entry": "break_not_crossed_resolved",
                "require_break_cross": True,
                "require_reclaim": False,
                "require_pullback": False,
            },
        )

    if range_score >= 3 and breakout_score < 5:
        return (
            "RANGE_ESCAPE",
            "medium",
            range_reasons,
            {
                "final_blocker_before_entry": "range_edge_wait_resolved",
                "require_break_cross": False,
                "require_reclaim": False,
                "require_pullback": False,
            },
        )

    reasons = [
        f"scores: reclaim={reclaim_score}, pullback={pullback_score}, breakout={breakout_score}, range={range_score}",
        "insufficient_family_separation",
    ]
    return (
        "OTHER",
        "low",
        reasons,
        {
            "final_blocker_before_entry": "unknown_or_mixed",
            "require_break_cross": False,
            "require_reclaim": False,
            "require_pullback": False,
        },
    )


def _init_bucket() -> dict[str, Any]:
    return {
        "count": 0,
        "mfe_pips": [],
        "mae_pips": [],
        "realized_pips": [],
        "realized_r": [],
        "time_to_be": [],
        "time_to_mfe": [],
        "time_to_first_green": [],
        "mfe_above_friction": 0,
        "mfe_above_005r": 0,
        "mfe_above_010r": 0,
        "mfe_above_020r": 0,
        "exit_reasons": Counter(),
    }


def _update_bucket(bucket: dict[str, Any], row: dict[str, Any]) -> None:
    bucket["count"] += 1
    bucket["mfe_pips"].append(float(row["mfe_pips"]))
    bucket["mae_pips"].append(float(row["mae_pips"]))
    bucket["realized_pips"].append(float(row["realized_pips"]))
    bucket["realized_r"].append(float(row["realized_r"]))
    if row["time_to_break_even_sec"] is not None:
        bucket["time_to_be"].append(float(row["time_to_break_even_sec"]))
    if row["time_to_mfe_sec"] is not None:
        bucket["time_to_mfe"].append(float(row["time_to_mfe_sec"]))
    if row["time_to_first_green_sec"] is not None:
        bucket["time_to_first_green"].append(float(row["time_to_first_green_sec"]))
    if bool(row["mfe_above_friction"]):
        bucket["mfe_above_friction"] += 1
    if bool(row["mfe_above_0_05r"]):
        bucket["mfe_above_005r"] += 1
    if bool(row["mfe_above_0_10r"]):
        bucket["mfe_above_010r"] += 1
    if bool(row["mfe_above_0_20r"]):
        bucket["mfe_above_020r"] += 1
    bucket["exit_reasons"][str(row["exit_reason"])] += 1


def _bucket_report(bucket: dict[str, Any], total_hours: float, friction_per_trade: float) -> dict[str, Any]:
    n = max(1, int(bucket["count"]))
    trades_per_hour = bucket["count"] / max(total_hours, 1e-9)
    avg_realized = _mean(bucket["realized_pips"])
    gross_pph = sum(bucket["realized_pips"]) / max(total_hours, 1e-9)
    net_pph = gross_pph - (friction_per_trade * trades_per_hour)
    return {
        "trade_count": int(bucket["count"]),
        "trades_per_hour": trades_per_hour,
        "gross_extraction_per_hour": gross_pph,
        "friction_pips_per_hour": friction_per_trade * trades_per_hour,
        "avg_mfe_pips": _mean(bucket["mfe_pips"]),
        "median_mfe_pips": median(bucket["mfe_pips"]) if bucket["mfe_pips"] else 0.0,
        "p80_mfe_pips": _percentile(bucket["mfe_pips"], 0.80),
        "p90_mfe_pips": _percentile(bucket["mfe_pips"], 0.90),
        "avg_mae_pips": _mean(bucket["mae_pips"]),
        "avg_realized_pips": avg_realized,
        "avg_realized_r": _mean(bucket["realized_r"]),
        "pct_mfe_above_friction": bucket["mfe_above_friction"] / n,
        "pct_mfe_above_0_05r": bucket["mfe_above_005r"] / n,
        "pct_mfe_above_0_10r": bucket["mfe_above_010r"] / n,
        "pct_mfe_above_0_20r": bucket["mfe_above_020r"] / n,
        "avg_time_to_break_even_sec": _mean(bucket["time_to_be"]),
        "avg_time_to_mfe_sec": _mean(bucket["time_to_mfe"]),
        "avg_time_to_first_green_sec": _mean(bucket["time_to_first_green"]),
        "dominant_exit_reasons": _dominant(bucket["exit_reasons"], 4),
        "net_extraction_per_hour": net_pph,
    }


def _verdict(rep: dict[str, Any]) -> str:
    if rep["trade_count"] < 80:
        return "NEEDS_MORE_ATTRIBUTION"
    if rep["net_extraction_per_hour"] > 0 and rep["pct_mfe_above_friction"] >= 0.58 and rep["pct_mfe_above_0_10r"] >= 0.45:
        return "KEEP"
    if rep["net_extraction_per_hour"] <= 0 and rep["pct_mfe_above_friction"] < 0.50:
        return "KILL"
    if rep["pct_mfe_above_0_20r"] > 0.38 and rep["avg_realized_pips"] <= 0:
        return "SPLIT_FURTHER"
    return "NEEDS_MORE_ATTRIBUTION"


def _family_from_strategy_name(strategy: str) -> str:
    s = (strategy or "").upper()
    if "FAILED_BREAKOUT_FADE" in s:
        return "RECLAIM_CONTINUATION"
    if "INTENTIONAL_RUNNER" in s:
        return "PULLBACK_CONTINUATION"
    if "COMPRESSION_EXPANSION_RUN" in s or "VOL_REIGNITE" in s:
        return "EXPANSION_BREAKOUT"
    if "RANGE" in s:
        return "RANGE_ESCAPE"
    return "OTHER"


def _family_from_trace_attempt(event: dict[str, Any]) -> str:
    trigger_mode = str(event.get("trigger_mode", "")).upper()
    entry_trigger = str(event.get("entry_trigger", "")).lower()
    reason = str(event.get("reason", "")).lower()
    setup = str(event.get("setup", ""))

    if trigger_mode == "RECLAIM" or "reclaim" in entry_trigger or "reclaim" in reason:
        return "RECLAIM_CONTINUATION"
    if trigger_mode == "RESUME" or "pullback" in entry_trigger or "resume" in entry_trigger:
        return "PULLBACK_CONTINUATION"
    if trigger_mode == "BREAK":
        if "range" in setup.lower() or "range" in reason:
            return "RANGE_ESCAPE"
        return "EXPANSION_BREAKOUT"
    return _family_from_strategy_name(setup)


def _aggregate_runtime_trace_evidence(root: Path, runtime_trace_glob: str) -> dict[str, Any]:
    files = sorted(root.glob(runtime_trace_glob))

    attempt_counts = Counter()
    attempt_decision_counts: dict[str, Counter] = defaultdict(Counter)
    trigger_mode_counts = Counter()
    entry_trigger_counts = Counter()
    block_reason_counts = Counter()
    pair_family_counts = Counter()
    setup_family_counts = Counter()
    filled_transition_counts = Counter()
    signal_strategy_counts = Counter()

    parse_errors = 0
    line_count = 0
    event_count = 0

    for fp in files:
        try:
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line_count += 1
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        ev = json.loads(s)
                    except Exception:
                        parse_errors += 1
                        continue

                    event_count += 1
                    ev_type = str(ev.get("event", ""))

                    if ev_type == "TRADE_ATTEMPT":
                        fam = _family_from_trace_attempt(ev)
                        decision = str(ev.get("decision", "UNKNOWN")).upper()
                        pair = str(ev.get("pair", "UNKNOWN"))
                        setup = str(ev.get("setup", "UNKNOWN"))

                        attempt_counts[fam] += 1
                        attempt_decision_counts[fam][decision] += 1
                        pair_family_counts[f"{pair}::{fam}"] += 1
                        setup_family_counts[f"{setup}::{fam}"] += 1

                        trig_mode = str(ev.get("trigger_mode", "UNKNOWN")).upper()
                        entry_trig = str(ev.get("entry_trigger", "UNKNOWN"))
                        block_reason = str(ev.get("tick_entry_block_reason", "UNKNOWN"))
                        trigger_mode_counts[trig_mode] += 1
                        entry_trigger_counts[entry_trig] += 1
                        block_reason_counts[block_reason] += 1

                    elif ev_type == "STATE_TRANSITION":
                        reason = str(ev.get("reason", ""))
                        to_state = str(ev.get("to", ""))
                        strategy = str(ev.get("strategy", ""))
                        if reason == "trade_filled" or to_state == "MANAGING":
                            fam = _family_from_strategy_name(strategy)
                            filled_transition_counts[fam] += 1

                    elif ev_type == "SIGNAL_GENERATED":
                        signal_strategy_counts[_family_from_strategy_name(str(ev.get("reason_code", "")))] += 1
        except FileNotFoundError:
            continue

    attempts_total = sum(attempt_counts.values())
    filled_total = sum(filled_transition_counts.values())

    family_rows = []
    for fam in ENTRY_TYPES:
        n_attempt = int(attempt_counts.get(fam, 0))
        n_filled = int(filled_transition_counts.get(fam, 0))
        d = attempt_decision_counts.get(fam, Counter())
        n_arm = int(d.get("ARM", 0))
        n_reject = int(d.get("REJECT", 0))
        n_other = max(0, n_attempt - n_arm - n_reject)
        family_rows.append(
            {
                "entry_type": fam,
                "attempt_count": n_attempt,
                "attempt_share": (n_attempt / attempts_total) if attempts_total else 0.0,
                "decision_arm_count": n_arm,
                "decision_reject_count": n_reject,
                "decision_other_count": n_other,
                "filled_transition_count": n_filled,
                "filled_transition_share": (n_filled / filled_total) if filled_total else 0.0,
            }
        )

    family_rows.sort(
        key=lambda r: (
            r["filled_transition_count"],
            r["attempt_count"],
            r["decision_arm_count"],
        ),
        reverse=True,
    )

    return {
        "generated_at": _iso_now(),
        "runtime_trace_glob": runtime_trace_glob,
        "files_scanned": len(files),
        "lines_scanned": line_count,
        "events_parsed": event_count,
        "json_parse_errors": parse_errors,
        "attempts_total": attempts_total,
        "filled_transitions_total": filled_total,
        "family_evidence": family_rows,
        "trigger_modes_top": _dominant(trigger_mode_counts, 8),
        "entry_triggers_top": _dominant(entry_trigger_counts, 12),
        "tick_block_reasons_top": _dominant(block_reason_counts, 10),
        "pair_family_top": _dominant(pair_family_counts, 15),
        "setup_family_top": _dominant(setup_family_counts, 15),
        "signal_strategy_family_top": _dominant(signal_strategy_counts, 8),
        "notes": {
            "domain_alignment": "runtime_trace_domain_not_trade_id_aligned_with_replay_stream_domain",
            "intended_use": "aggregate_family_incidence_and_transition_evidence",
            "not_for": "per_trade_join_or_direct_outcome_substitution",
        },
    }


def _confidence_reconciliation(
    ranked: list[dict[str, Any]],
    runtime_trace_evidence: dict[str, Any],
) -> dict[str, Any]:
    runtime_by_family: dict[str, dict[str, Any]] = {}
    for row in runtime_trace_evidence.get("family_evidence", []):
        fam = str(row.get("entry_type", "OTHER"))
        runtime_by_family[fam] = row

    rows: list[dict[str, Any]] = []
    for rr in ranked:
        fam = str(rr.get("entry_type", "OTHER"))
        replay_count = int(rr.get("trade_count", 0))
        replay_net_pph = float(rr.get("net_extraction_per_hour", 0.0))
        replay_verdict = str(rr.get("verdict", "NEEDS_MORE_ATTRIBUTION"))

        rt = runtime_by_family.get(fam, {})
        runtime_attempts = int(rt.get("attempt_count", 0))
        runtime_filled = int(rt.get("filled_transition_count", 0))

        if replay_count >= 500 and runtime_attempts >= 500:
            confidence = "HIGH"
        elif replay_count >= 80 and runtime_attempts >= 100:
            confidence = "MEDIUM"
        elif replay_count >= 80 or runtime_attempts >= 100:
            confidence = "LOW"
        else:
            confidence = "VERY_LOW"

        if replay_verdict == "KEEP" and confidence in {"HIGH", "MEDIUM"}:
            action = "KEEP_ACTIVE"
        elif replay_verdict == "KEEP":
            action = "KEEP_GUARDED"
        elif replay_verdict == "SPLIT_FURTHER":
            action = "SPLIT_BY_CONTEXT"
        elif runtime_attempts >= 100:
            action = "ATTRIBUTION_PRIORITY_HIGH"
        else:
            action = "DEFER"

        rows.append(
            {
                "entry_type": fam,
                "replay_trade_count": replay_count,
                "replay_net_extraction_per_hour": replay_net_pph,
                "replay_verdict": replay_verdict,
                "runtime_attempt_count": runtime_attempts,
                "runtime_filled_transition_count": runtime_filled,
                "confidence_grade": confidence,
                "recommended_action": action,
            }
        )

    rows.sort(
        key=lambda r: (
            ["HIGH", "MEDIUM", "LOW", "VERY_LOW"].index(r["confidence_grade"]),
            -float(r["replay_net_extraction_per_hour"]),
        )
    )

    confidence_counts = Counter(r["confidence_grade"] for r in rows)
    action_counts = Counter(r["recommended_action"] for r in rows)

    return {
        "generated_at": _iso_now(),
        "objective": "maximize_net_realized_extraction_per_hour",
        "families": rows,
        "summary": {
            "confidence_grade_counts": dict(confidence_counts),
            "recommended_action_counts": dict(action_counts),
            "notes": [
                "replay-domain realized net extraction per hour is primary ranking source",
                "runtime-domain attempts/fills are corroboration, not a per-trade substitution",
                "low replay count with meaningful runtime attempts should be prioritized for attribution passes",
            ],
        },
    }


def _build_promotion_policy(
    confidence_reconciliation: dict[str, Any],
    ranked: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = list(confidence_reconciliation.get("families", []))
    by_type = {str(r.get("entry_type", "")): r for r in rows}

    keep_active = [str(r.get("entry_type", "")) for r in rows if str(r.get("recommended_action", "")) == "KEEP_ACTIVE"]
    keep_guarded = [str(r.get("entry_type", "")) for r in rows if str(r.get("recommended_action", "")) == "KEEP_GUARDED"]
    attribution_high = [
        str(r.get("entry_type", ""))
        for r in rows
        if str(r.get("recommended_action", "")) == "ATTRIBUTION_PRIORITY_HIGH"
    ]
    split_by_context = [str(r.get("entry_type", "")) for r in rows if str(r.get("recommended_action", "")) == "SPLIT_BY_CONTEXT"]

    strict_active = sorted(keep_active)
    guarded_active = sorted(keep_active + keep_guarded)
    exploratory_only = sorted(attribution_high + split_by_context)

    objective = "maximize_net_realized_extraction_per_hour"

    policy_notes = [
        "strict policy activates only KEEP_ACTIVE families",
        "guarded policy activates KEEP_ACTIVE + KEEP_GUARDED families with monitoring gates",
        "ATTRIBUTION_PRIORITY_HIGH families stay out of activation until attribution depth improves",
    ]

    observability_gates = {
        "replay_primary_gate": {
            "minimum_net_extraction_per_hour": 0.0,
            "minimum_replay_trade_count": 80,
            "note": "replay-domain net extraction per hour remains primary gate",
        },
        "runtime_secondary_gate": {
            "minimum_runtime_attempt_count_for_confidence": 100,
            "minimum_runtime_filled_transition_count_for_visibility": 1,
            "note": "runtime-domain is corroborative and must not replace replay ranking",
        },
    }

    family_status = []
    for r in ranked:
        et = str(r.get("entry_type", "OTHER"))
        c = by_type.get(et, {})
        family_status.append(
            {
                "entry_type": et,
                "replay_net_extraction_per_hour": float(r.get("net_extraction_per_hour", 0.0)),
                "replay_trade_count": int(r.get("trade_count", 0)),
                "confidence_grade": str(c.get("confidence_grade", "VERY_LOW")),
                "recommended_action": str(c.get("recommended_action", "DEFER")),
                "strict_policy_state": "ACTIVE" if et in strict_active else "INACTIVE",
                "guarded_policy_state": (
                    "ACTIVE_MONITORED" if et in keep_guarded else ("ACTIVE" if et in keep_active else "INACTIVE")
                ),
            }
        )

    return {
        "generated_at": _iso_now(),
        "objective": objective,
        "selected_default_policy": "GUARDED_KEEP_LOW_CONFIDENCE",
        "candidate_policies": {
            "STRICT_KEEP_ACTIVE_ONLY": {
                "active_entry_types": strict_active,
                "inactive_entry_types": sorted([et for et in ENTRY_TYPES if et not in strict_active]),
                "why": "maximize confidence discipline and reduce low-visibility exposure",
            },
            "GUARDED_KEEP_LOW_CONFIDENCE": {
                "active_entry_types": guarded_active,
                "guarded_entry_types": sorted(keep_guarded),
                "exploratory_only_entry_types": exploratory_only,
                "why": "preserve replay-proven families while enforcing stronger monitoring on lower-confidence families",
            },
        },
        "family_status": family_status,
        "activation_summary": {
            "keep_active": strict_active,
            "keep_guarded": sorted(keep_guarded),
            "attribution_priority_high": sorted(attribution_high),
            "split_by_context": sorted(split_by_context),
        },
        "observability_gates": observability_gates,
        "notes": policy_notes,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Entry family audit + rebuild recommendation generator.")
    ap.add_argument("--config", default="entry_v23_strict_plus_six_contexts.json")
    ap.add_argument("--runbook", default="entry_v23_recheck_newblock_runbook.json")
    ap.add_argument("--stream-glob", action="append", default=[])
    ap.add_argument("--max-streams", type=int, default=31)
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--entry-trade-audit-out", default="entry_trade_audit.jsonl")
    ap.add_argument("--entry-type-audit-out", default="entry_type_audit.json")
    ap.add_argument("--entry-context-type-matrix-out", default="entry_context_type_matrix.json")
    ap.add_argument("--entry-family-ranked-report-out", default="entry_family_ranked_report.json")
    ap.add_argument("--entry-blocker-report-out", default="entry_blocker_report.json")
    ap.add_argument("--entry-rebuild-recommendation-out", default="entry_rebuild_recommendation.json")
    ap.add_argument("--entry-runtime-trace-audit-out", default="entry_runtime_trace_audit.json")
    ap.add_argument("--entry-confidence-reconciliation-out", default="entry_confidence_reconciliation.json")
    ap.add_argument("--entry-promotion-policy-out", default="entry_promotion_policy.json")
    ap.add_argument("--entry-type-labeling-notes-out", default="entry_type_labeling_notes.md")
    ap.add_argument("--entry-unknown-examples-out", default="entry_unknown_examples.json")
    ap.add_argument("--runtime-trace-glob", default="runs/**/trades.jsonl")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    cfg_path = (root / args.config).resolve() if not Path(args.config).is_absolute() else Path(args.config)
    runbook_path = (root / args.runbook).resolve() if not Path(args.runbook).is_absolute() else Path(args.runbook)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    runbook = json.loads(runbook_path.read_text(encoding="utf-8")) if runbook_path.exists() else {}
    manifest = runbook.get("reproducibility", {}).get("stream_manifest", [])

    streams: list[Path] = []
    if manifest:
        for rel in manifest:
            p = (root / str(rel)).resolve()
            if p.is_file():
                streams.append(p)
    else:
        globs = args.stream_glob or [
            "compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
            "compiled_market_nodes/EUR_CHF__*/aee_stage/aee_state_stream/aee_state_stream.csv",
            "compiled_market_nodes/USD_CAD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
            "compiled_market_nodes/EUR_GBP__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        ]
        for g in globs:
            streams.extend([p.resolve() for p in root.glob(g) if p.is_file()])
    streams = sorted(set(streams))[: max(1, int(args.max_streams))]
    if not streams:
        raise SystemExit("No streams found for entry audit")

    entry_filters = cfg.get("entry_filters") or {}
    exclude_contexts_lc = {str(x).lower().strip() for x in entry_filters.get("exclude_contexts", [])}
    min_profit_now_pips_by_bar = list(entry_filters.get("min_profit_now_pips_by_bar", []))
    min_progress_ratio_by_bar = list(entry_filters.get("min_progress_ratio_by_bar", []))
    min_release_quality_by_bar = list(entry_filters.get("min_release_quality_by_bar", []))
    max_noise_by_bar = list(entry_filters.get("max_noise_by_bar", []))
    micro_confirm = dict(entry_filters.get("micro_confirm", {}))
    family_specific_filters = dict(entry_filters.get("family_specific_filters", {}))

    friction_per_trade = _friction_per_trade(
        cfg,
        spread=float(args.spread_pips),
        slip=float(args.slippage_pips_per_side),
        commission=float(args.commission_pips_roundtrip),
        latency=float(args.latency_penalty_pips),
    )
    break_even_pips = friction_per_trade

    total_hours = 0.0
    context_hours: dict[str, float] = defaultdict(float)
    pair_hours: dict[str, float] = defaultdict(float)
    session_hours: dict[str, float] = defaultdict(float)

    audit_rows: list[dict[str, Any]] = []
    unknown_examples: list[dict[str, Any]] = []

    prevented_blockers = Counter()
    resolved_blockers_strong = Counter()
    resolved_blockers_weak = Counter()

    by_type: dict[str, dict[str, Any]] = {t: _init_bucket() for t in ENTRY_TYPES}
    by_context_type: dict[tuple[str, str], dict[str, Any]] = defaultdict(_init_bucket)
    by_pair_type: dict[tuple[str, str], dict[str, Any]] = defaultdict(_init_bucket)
    by_session_type: dict[tuple[str, str], dict[str, Any]] = defaultdict(_init_bucket)

    gate_pass_counts = Counter()
    gate_low_excursion_counts = Counter()

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue

        pair, day, session, context = _context_from_stream(root, sp)
        context_lc = context.lower()
        hours = _stream_duration_hours(rows)
        total_hours += hours
        context_hours[context] += hours
        pair_hours[pair] += hours
        session_hours[session] += hours

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for trade_id, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue

            filter_reason = _entry_filter_reason(
                trows,
                pair,
                context_lc,
                set(),
                set(),
                exclude_contexts_lc,
                min_profit_now_pips_by_bar,
                min_progress_ratio_by_bar,
                min_release_quality_by_bar,
                max_noise_by_bar,
                micro_confirm,
                family_specific_filters,
            )
            if filter_reason is not None:
                prevented_blockers[str(filter_reason)] += 1
                continue

            first = trows[0]
            direction = str(first.get("direction", "")).upper() or "UNKNOWN"
            td = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))

            label, confidence, label_reasons, gate_info = _infer_label(trows, td)

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=friction_per_trade,
                economic_value_margin_mult=1.1,
            )

            profit_series = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows]
            ts_series = [_parse_ts(str(r.get("timestamp", ""))) for r in trows]

            mfe_pips = max(profit_series) if profit_series else 0.0
            mae_pips = min(profit_series) if profit_series else 0.0
            mfe_r = mfe_pips / td
            mae_r = mae_pips / td

            t0 = ts_series[0] if ts_series else None
            t_green = None
            t_be = None
            t_mfe = None
            if t0 is not None:
                for i, pips in enumerate(profit_series):
                    ts = ts_series[i]
                    if ts is None:
                        continue
                    if t_green is None and pips > 0.0:
                        t_green = ts - t0
                    if t_be is None and pips >= break_even_pips:
                        t_be = ts - t0
                if profit_series:
                    idx_mfe = max(range(len(profit_series)), key=lambda i: profit_series[i])
                    ts_mfe = ts_series[idx_mfe]
                    if ts_mfe is not None:
                        t_mfe = ts_mfe - t0

            realized_pips = float(aee.get("pips", 0.0))
            gross_realized_pips = float(aee.get("gross_pips", realized_pips))
            spread_adjusted_realized_pips = float(aee.get("net_spread_pips", realized_pips))
            net_realized_pips = gross_realized_pips - friction_per_trade
            realized_r = float(aee.get("r", 0.0))
            exit_reason = str(aee.get("reason", "UNKNOWN"))

            setup_ts = None
            armed_ts = None
            entry_ts = str(first.get("timestamp", "") or first.get("entry_time", "")) or None
            setup_to_entry_sec = None

            bar2 = _row_at_or_after_bar(trows, 2)
            bar3 = _row_at_or_after_bar(trows, 3)

            row = {
                "trade_id": str(trade_id),
                "pair": pair,
                "direction": direction,
                "session": session,
                "weekday": day,
                "context_node": context,
                "entry_type": label,
                "label_confidence": confidence,
                "label_reasons": label_reasons,
                "setup_ts": setup_ts,
                "armed_ts": armed_ts,
                "entry_ts": entry_ts,
                "setup_to_entry_sec": setup_to_entry_sec,
                "entry_price": None,
                "trigger_price": None,
                "spread_at_entry_pips": _safe_float((cfg.get("extraction") or {}).get("open_spread_pips", args.spread_pips), float(args.spread_pips)),
                "final_blocker_before_arm": None,
                "final_blocker_before_entry": gate_info["final_blocker_before_entry"],
                "require_break_cross": gate_info["require_break_cross"],
                "require_reclaim": gate_info["require_reclaim"],
                "require_pullback": gate_info["require_pullback"],
                "confirm_disp_value": _safe_float(bar2.get("profit_now", 0.0), 0.0),
                "max_dist_value": _safe_float(first.get("compression", 0.0), 0.0),
                "reclaim_tolerance_value": abs(_safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)),
                "pullback_value": abs(_safe_float(first.get("pre_build_slope", 0.0), 0.0)),
                "progress_ratio_bar2": _safe_float(bar2.get("progress_ratio", 0.0), 0.0),
                "progress_ratio_bar3": _safe_float(bar3.get("progress_ratio", 0.0), 0.0),
                "release_quality": _safe_float(first.get("release_quality", 0.0), 0.0),
                "noise": _safe_float(first.get("noise", 1.0), 1.0),
                "compression": _safe_float(first.get("compression", 0.0), 0.0),
                "mfe_pips": mfe_pips,
                "mfe_r": mfe_r,
                "mae_pips": mae_pips,
                "mae_r": mae_r,
                "time_to_first_green_sec": t_green,
                "time_to_break_even_sec": t_be,
                "time_to_mfe_sec": t_mfe,
                "realized_pips": gross_realized_pips,
                "gross_realized_pips": gross_realized_pips,
                "spread_adjusted_realized_pips": spread_adjusted_realized_pips,
                "net_realized_pips": net_realized_pips,
                "realized_r": realized_r,
                "exit_reason": exit_reason,
                "friction_pips": friction_per_trade,
                "mfe_above_friction": mfe_pips > friction_per_trade,
                "mfe_above_0_05r": mfe_r > 0.05,
                "mfe_above_0_10r": mfe_r > 0.10,
                "mfe_above_0_20r": mfe_r > 0.20,
            }
            audit_rows.append(row)

            strong_trade = bool(row["mfe_above_0_20r"]) and row["realized_pips"] > 0.0
            weak_trade = (not bool(row["mfe_above_friction"])) or row["realized_pips"] <= 0.0
            if strong_trade:
                resolved_blockers_strong[str(row["final_blocker_before_entry"])] += 1
            if weak_trade:
                resolved_blockers_weak[str(row["final_blocker_before_entry"])] += 1

            _update_bucket(by_type[label], row)
            _update_bucket(by_context_type[(context, label)], row)
            _update_bucket(by_pair_type[(pair, label)], row)
            _update_bucket(by_session_type[(session, label)], row)

            for gate_name in ["require_break_cross", "require_reclaim", "require_pullback"]:
                if bool(row[gate_name]):
                    gate_pass_counts[gate_name] += 1
                    if not bool(row["mfe_above_friction"]):
                        gate_low_excursion_counts[gate_name] += 1

            if label == "OTHER" and len(unknown_examples) < 50:
                unknown_examples.append(
                    {
                        "trade_id": str(trade_id),
                        "pair": pair,
                        "context_node": context,
                        "direction": direction,
                        "label_confidence": confidence,
                        "label_reasons": label_reasons,
                        "features": {
                            "compression": _safe_float(first.get("compression", 0.0), 0.0),
                            "release_quality": _safe_float(first.get("release_quality", 0.0), 0.0),
                            "noise": _safe_float(first.get("noise", 1.0), 1.0),
                            "pre_macro_micro_alignment": _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0),
                            "pre_compression_release_delta": _safe_float(first.get("pre_compression_release_delta", 0.0), 0.0),
                            "progress_ratio_bar2": _safe_float(bar2.get("progress_ratio", 0.0), 0.0),
                        },
                    }
                )

    total_hours = max(total_hours, 1.0 / 60.0)

    entry_type_audit: dict[str, Any] = {
        "generated_at": _iso_now(),
        "objective": "maximize_net_realized_extraction_per_hour",
        "config": str(cfg_path),
        "streams_used": len(streams),
        "total_hours": total_hours,
        "entry_types": {},
        "pair_x_entry_type": {},
        "session_x_entry_type": {},
        "notes": {
            "setup_ts_unavailable": True,
            "armed_ts_unavailable": True,
            "entry_and_trigger_price_unavailable": True,
            "cost_accounting_model": "gross_per_trade_minus_friction_once_at_aggregate",
            "fallback_used": "pre_entry_features_from_state_stream_and_deterministic_rules",
        },
    }

    for t in ENTRY_TYPES:
        rep = _bucket_report(by_type[t], total_hours=total_hours, friction_per_trade=friction_per_trade)
        rep["verdict"] = _verdict(rep)
        entry_type_audit["entry_types"][t] = rep

    for (pair, t), bucket in by_pair_type.items():
        key = f"{pair}::{t}"
        entry_type_audit["pair_x_entry_type"][key] = _bucket_report(
            bucket,
            total_hours=max(pair_hours[pair], 1.0 / 60.0),
            friction_per_trade=friction_per_trade,
        )

    for (session, t), bucket in by_session_type.items():
        key = f"{session}::{t}"
        entry_type_audit["session_x_entry_type"][key] = _bucket_report(
            bucket,
            total_hours=max(session_hours[session], 1.0 / 60.0),
            friction_per_trade=friction_per_trade,
        )

    matrix = {
        "generated_at": _iso_now(),
        "objective": "maximize_net_realized_extraction_per_hour",
        "cells": {},
        "matrix": {},
    }
    for (context, t), bucket in by_context_type.items():
        rep = _bucket_report(bucket, total_hours=max(context_hours[context], 1.0 / 60.0), friction_per_trade=friction_per_trade)
        matrix["cells"][f"{context}::{t}"] = {
            "trade_count": rep["trade_count"],
            "avg_mfe_pips": rep["avg_mfe_pips"],
            "avg_realized_pips": rep["avg_realized_pips"],
            "net_extraction_per_hour": rep["net_extraction_per_hour"],
            "pct_mfe_above_friction": rep["pct_mfe_above_friction"],
        }
        if context not in matrix["matrix"]:
            matrix["matrix"][context] = {}
        matrix["matrix"][context][t] = {
            "trade_count": rep["trade_count"],
            "avg_mfe_pips": rep["avg_mfe_pips"],
            "avg_realized_pips": rep["avg_realized_pips"],
            "net_extraction_per_hour": rep["net_extraction_per_hour"],
            "pct_mfe_above_friction": rep["pct_mfe_above_friction"],
        }

    ranked = []
    for t in ENTRY_TYPES:
        rep = entry_type_audit["entry_types"][t]
        ranked.append(
            {
                "entry_type": t,
                "net_extraction_per_hour": rep["net_extraction_per_hour"],
                "avg_realized_pips": rep["avg_realized_pips"],
                "pct_mfe_above_friction": rep["pct_mfe_above_friction"],
                "trades_per_hour": rep["trades_per_hour"],
                "trade_count": rep["trade_count"],
                "verdict": rep["verdict"],
            }
        )
    ranked.sort(
        key=lambda r: (
            r["net_extraction_per_hour"],
            r["avg_realized_pips"],
            r["pct_mfe_above_friction"],
            r["trades_per_hour"],
        ),
        reverse=True,
    )

    gate_redundancy = {}
    executed_count = max(1, len(audit_rows))
    for gate_name, cnt in gate_pass_counts.items():
        pass_rate = cnt / executed_count
        gate_redundancy[gate_name] = {
            "pass_count": int(cnt),
            "pass_rate": pass_rate,
            "redundant": bool(pass_rate >= 0.98 or pass_rate <= 0.02),
        }

    low_excursion_by_gate = {}
    for gate_name, cnt in gate_pass_counts.items():
        low = gate_low_excursion_counts.get(gate_name, 0)
        low_excursion_by_gate[gate_name] = {
            "pass_count": int(cnt),
            "low_excursion_count": int(low),
            "low_excursion_share": (low / cnt) if cnt > 0 else 0.0,
        }

    blocker_report = {
        "generated_at": _iso_now(),
        "objective": "maximize_net_realized_extraction_per_hour",
        "blockers_preventing_entry": dict(prevented_blockers),
        "blockers_resolved_before_strong_trades": dict(resolved_blockers_strong),
        "blockers_resolved_before_weak_trades": dict(resolved_blockers_weak),
        "gate_redundancy": gate_redundancy,
        "gates_passing_low_excursion_trades": low_excursion_by_gate,
    }

    best = ranked[0] if ranked else None
    worst = ranked[-1] if ranked else None
    strongest_excursion = max(
        [{"entry_type": t, "p90_mfe_pips": entry_type_audit["entry_types"][t]["p90_mfe_pips"]} for t in ENTRY_TYPES],
        key=lambda x: x["p90_mfe_pips"],
    )
    fake_weak = min(
        [{"entry_type": t, "pct_mfe_above_friction": entry_type_audit["entry_types"][t]["pct_mfe_above_friction"]} for t in ENTRY_TYPES],
        key=lambda x: x["pct_mfe_above_friction"],
    )

    other_share = (entry_type_audit["entry_types"].get("OTHER", {}).get("trade_count", 0) / max(1, len(audit_rows)))

    hurt_by = []
    if any(r["net_extraction_per_hour"] <= 0 for r in ranked[1:]):
        hurt_by.append("bad_family_design")
    if any(v["net_extraction_per_hour"] <= 0 for v in matrix["cells"].values()):
        hurt_by.append("bad_contexts")
    if any(r["avg_time_to_break_even_sec"] > 180 for r in entry_type_audit["entry_types"].values()):
        hurt_by.append("bad_timing")
    if not hurt_by:
        hurt_by = ["mixed_but_mostly_family_selection"]

    rebuild = {
        "generated_at": _iso_now(),
        "objective": "maximize_net_realized_extraction_per_hour",
        "how_many_real_entry_families_exist": sum(1 for t in ENTRY_TYPES if entry_type_audit["entry_types"][t]["trade_count"] >= 80),
        "best_family": best,
        "worst_family": worst,
        "strongest_post_entry_excursion_family": strongest_excursion,
        "fake_or_weak_trade_family": fake_weak,
        "results_mainly_hurt_by": hurt_by,
        "family_to_keep": [r for r in ranked if r["verdict"] == "KEEP"],
        "family_to_kill": [r for r in ranked if r["verdict"] == "KILL"],
        "family_to_split_further": [r for r in ranked if r["verdict"] == "SPLIT_FURTHER"],
        "keep": [r for r in ranked if r["verdict"] == "KEEP"],
        "kill": [r for r in ranked if r["verdict"] == "KILL"],
        "split_further": [r for r in ranked if r["verdict"] == "SPLIT_FURTHER"],
        "other_bucket_share": other_share,
        "next_minimal_rebuild": {
            "step_1": "route entry by deterministic family classifier",
            "step_2": "keep top net_extraction_per_hour family active globally",
            "step_3": "disable KILL families or context-family cells with negative net_extraction_per_hour",
            "step_4": "for SPLIT_FURTHER family, split by context and rerun full-31",
            "step_5": "retain independent per-family scorecards as release gate",
        },
    }

    runtime_trace_evidence = _aggregate_runtime_trace_evidence(root, args.runtime_trace_glob)
    confidence_reconciliation = _confidence_reconciliation(ranked, runtime_trace_evidence)
    promotion_policy = _build_promotion_policy(confidence_reconciliation, ranked)
    rebuild["runtime_trigger_evidence"] = {
        "attempts_total": runtime_trace_evidence["attempts_total"],
        "filled_transitions_total": runtime_trace_evidence["filled_transitions_total"],
        "family_evidence": runtime_trace_evidence["family_evidence"],
        "reconciliation_notes": [
            "runtime traces confirm reclaim/pullback trigger families exist in live trigger domain",
            "replay stream domain remains primary source for realized net extraction/hour ranking",
            "use runtime evidence to prevent false zero-incidence conclusions for underrepresented replay families",
        ],
    }
    rebuild["family_confidence_reconciliation"] = confidence_reconciliation["families"]
    rebuild["promotion_policy"] = {
        "selected_default_policy": promotion_policy["selected_default_policy"],
        "activation_summary": promotion_policy["activation_summary"],
    }

    notes_md = (
        "# Entry Type Labeling Notes\n\n"
        "Important:\n"
        "Treat the current entry system as a generic negative selector until proven otherwise.\n"
        "The purpose of this task is to expose the real offensive entry families mechanically, not to add more generic filtering.\n\n"
        "## Deterministic precedence\n"
        "1. RECLAIM_CONTINUATION\n"
        "2. PULLBACK_CONTINUATION\n"
        "3. EXPANSION_BREAKOUT\n"
        "4. RANGE_ESCAPE\n"
        "5. OTHER\n\n"
        "## Data sources used\n"
        "- Executed trades from aee_state_stream.csv grouped by trade_id\n"
        "- Existing gate/filter logic from run_aee_band_floor_baseline.py\n"
        "- Pre-entry proxy features in stream columns (pre_*, compression, release_quality, noise, progress_ratio)\n"
        "- Existing AEE evaluator function for realized outcome fields\n"
    )

    out_trade = (root / args.entry_trade_audit_out).resolve()
    out_type = (root / args.entry_type_audit_out).resolve()
    out_matrix = (root / args.entry_context_type_matrix_out).resolve()
    out_rank = (root / args.entry_family_ranked_report_out).resolve()
    out_blocker = (root / args.entry_blocker_report_out).resolve()
    out_rebuild = (root / args.entry_rebuild_recommendation_out).resolve()
    out_runtime_trace = (root / args.entry_runtime_trace_audit_out).resolve()
    out_confidence = (root / args.entry_confidence_reconciliation_out).resolve()
    out_promotion = (root / args.entry_promotion_policy_out).resolve()
    out_notes = (root / args.entry_type_labeling_notes_out).resolve()
    out_unknown = (root / args.entry_unknown_examples_out).resolve()

    out_trade.write_text("\n".join(json.dumps(r, ensure_ascii=True) for r in audit_rows) + "\n", encoding="utf-8")
    out_type.write_text(json.dumps(entry_type_audit, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_matrix.write_text(json.dumps(matrix, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_rank.write_text(json.dumps({"generated_at": _iso_now(), "objective": "maximize_net_realized_extraction_per_hour", "ranking": ranked}, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_blocker.write_text(json.dumps(blocker_report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_rebuild.write_text(json.dumps(rebuild, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_runtime_trace.write_text(json.dumps(runtime_trace_evidence, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_confidence.write_text(json.dumps(confidence_reconciliation, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_promotion.write_text(json.dumps(promotion_policy, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    out_notes.write_text(notes_md, encoding="utf-8")
    out_unknown.write_text(json.dumps({"generated_at": _iso_now(), "examples": unknown_examples}, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "entry_trade_audit": str(out_trade),
                "entry_type_audit": str(out_type),
                "entry_context_type_matrix": str(out_matrix),
                "entry_family_ranked_report": str(out_rank),
                "entry_blocker_report": str(out_blocker),
                "entry_rebuild_recommendation": str(out_rebuild),
                "entry_runtime_trace_audit": str(out_runtime_trace),
                "entry_confidence_reconciliation": str(out_confidence),
                "entry_promotion_policy": str(out_promotion),
                "entry_type_labeling_notes": str(out_notes),
                "entry_unknown_examples": str(out_unknown),
                "trades_audited": len(audit_rows),
                "streams_used": len(streams),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
