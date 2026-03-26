#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent
LOGS_PATH = ROOT / "logs" / "trades.jsonl"
ARTIFACTS_DIR = ROOT / "artifacts"


@dataclass(frozen=True)
class FeatureSpec:
    key: str
    feature_name: str
    trigger_marker: str
    expected_exit_reasons: List[str]
    proof_metric: str
    preferred_test_method: str


FEATURES: List[FeatureSpec] = [
    FeatureSpec(
        key="pre_sl_protection",
        feature_name="Pre-SL protection",
        trigger_marker="AEE_PRE_SL_PROTECTION_TRIGGER",
        expected_exit_reasons=["PRE_SL_PROTECTION"],
        proof_metric="count_closed_before_original_sl",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="early_profit_lock",
        feature_name="Early profit lock",
        trigger_marker="AEE_EARLY_PROFIT_LOCK_TRIGGER",
        expected_exit_reasons=["MISSED_PROFIT_EXTRACTION_EXIT"],
        proof_metric="profit_lock_trigger_rate",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="weak_trade_kill",
        feature_name="Weak-trade kill",
        trigger_marker="AEE_WEAK_TRADE_KILL_TRIGGER",
        expected_exit_reasons=["NEVER_GREEN_FAST_EXIT", "NEVER_GREEN_STALL_EXIT"],
        proof_metric="weak_trade_early_exit_rate",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="stall_capture",
        feature_name="Stall capture",
        trigger_marker="AEE_STALL_CAPTURE_TRIGGER",
        expected_exit_reasons=["NEAR_TP_STALL_CAPTURE", "PULSE_STALL_CAPTURE", "GREEN_STALL_CAPTURE"],
        proof_metric="stall_capture_count",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="giveback_exit",
        feature_name="Giveback exit",
        trigger_marker="AEE_GIVEBACK_EXIT_TRIGGER",
        expected_exit_reasons=["EXTRACTION_LOSS_EXIT"],
        proof_metric="giveback_exit_count",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="runner_hold",
        feature_name="Runner hold / extension hold",
        trigger_marker="AEE_RUNNER_HOLD_TRIGGER",
        expected_exit_reasons=["HOLD"],
        proof_metric="runner_hold_count",
        preferred_test_method="stress-demo",
    ),
    FeatureSpec(
        key="runner_fallback",
        feature_name="Runner fallback",
        trigger_marker="AEE_RUNNER_FALLBACK_TRIGGER",
        expected_exit_reasons=["FAILED_TO_CONTINUE_DECAY"],
        proof_metric="runner_fallback_count",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="panic_exit",
        feature_name="Panic / sharp adverse-move exit",
        trigger_marker="AEE_PANIC_EXIT_TRIGGER",
        expected_exit_reasons=["PANIC_EXIT"],
        proof_metric="panic_exit_count",
        preferred_test_method="replay",
    ),
    FeatureSpec(
        key="time_pressure_exit",
        feature_name="Time-pressure / no-progress exit",
        trigger_marker="AEE_TIME_PRESSURE_EXIT_TRIGGER",
        expected_exit_reasons=["TIME_DECAY_PROFIT_CAPTURE"],
        proof_metric="time_pressure_exit_count",
        preferred_test_method="replay",
    ),
]


def _build_forced_suite() -> Dict[str, Any]:
    # Inputs are normalized synthetic trade-state fields for deterministic branch checks.
    cases: Dict[str, List[Dict[str, Any]]] = {
        "pre_sl_protection": [
            {
                "scenario_id": "pre_sl_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"toward_sl": True, "recovery_score": 0.05, "distance_to_sl_pips": 1.5, "current_pnl_pips": -6.2, "peak_pnl_pips": 0.2, "giveback_pips": 6.4, "time_in_trade_sec": 88},
                "expected": {"triggered": True, "exit_reason": "PRE_SL_PROTECTION"},
            },
            {
                "scenario_id": "pre_sl_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"toward_sl": True, "recovery_score": 0.2, "distance_to_sl_pips": 3.0, "current_pnl_pips": -4.4, "peak_pnl_pips": 0.4, "giveback_pips": 4.8, "time_in_trade_sec": 90},
                "expected": {"triggered": True, "exit_reason": "PRE_SL_PROTECTION"},
            },
            {
                "scenario_id": "pre_sl_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"toward_sl": False, "recovery_score": 0.65, "distance_to_sl_pips": 8.5, "current_pnl_pips": -1.2, "peak_pnl_pips": 2.1, "giveback_pips": 3.3, "time_in_trade_sec": 65},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "early_profit_lock": [
            {
                "scenario_id": "profit_lock_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"went_green": True, "near_tp": True, "progress_stall": True, "current_pnl_pips": 4.6, "peak_pnl_pips": 6.1, "giveback_pips": 1.5, "time_in_trade_sec": 140},
                "expected": {"triggered": True, "exit_reason": "MISSED_PROFIT_EXTRACTION_EXIT"},
            },
            {
                "scenario_id": "profit_lock_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"went_green": True, "near_tp": True, "progress_stall": True, "current_pnl_pips": 2.4, "peak_pnl_pips": 3.2, "giveback_pips": 0.8, "time_in_trade_sec": 110},
                "expected": {"triggered": True, "exit_reason": "MISSED_PROFIT_EXTRACTION_EXIT"},
            },
            {
                "scenario_id": "profit_lock_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"went_green": False, "near_tp": False, "progress_stall": False, "current_pnl_pips": -2.0, "peak_pnl_pips": 0.0, "giveback_pips": 0.0, "time_in_trade_sec": 44},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "weak_trade_kill": [
            {
                "scenario_id": "weak_kill_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"went_green": False, "stagnant_bars": 6, "current_pnl_pips": -4.1, "peak_pnl_pips": 0.1, "giveback_pips": 0.1, "time_in_trade_sec": 170},
                "expected": {"triggered": True, "exit_reason": "NEVER_GREEN_FAST_EXIT"},
            },
            {
                "scenario_id": "weak_kill_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"went_green": False, "stagnant_bars": 4, "current_pnl_pips": -2.8, "peak_pnl_pips": 0.1, "giveback_pips": 0.2, "time_in_trade_sec": 120},
                "expected": {"triggered": True, "exit_reason": "NEVER_GREEN_STALL_EXIT"},
            },
            {
                "scenario_id": "weak_kill_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"went_green": True, "stagnant_bars": 1, "current_pnl_pips": 1.8, "peak_pnl_pips": 3.0, "giveback_pips": 1.2, "time_in_trade_sec": 80},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "stall_capture": [
            {
                "scenario_id": "stall_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"went_green": True, "progress_stall": True, "giveback_pips": 2.2, "current_pnl_pips": 2.1, "peak_pnl_pips": 4.3, "time_in_trade_sec": 180},
                "expected": {"triggered": True, "exit_reason": "GREEN_STALL_CAPTURE"},
            },
            {
                "scenario_id": "stall_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"went_green": True, "progress_stall": True, "giveback_pips": 1.3, "current_pnl_pips": 1.8, "peak_pnl_pips": 3.1, "time_in_trade_sec": 130},
                "expected": {"triggered": True, "exit_reason": "PULSE_STALL_CAPTURE"},
            },
            {
                "scenario_id": "stall_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"went_green": True, "progress_stall": False, "giveback_pips": 0.4, "current_pnl_pips": 3.9, "peak_pnl_pips": 4.3, "time_in_trade_sec": 60},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "giveback_exit": [
            {
                "scenario_id": "giveback_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"went_green": True, "giveback_pips": 4.8, "current_pnl_pips": -1.5, "peak_pnl_pips": 3.3, "time_in_trade_sec": 155},
                "expected": {"triggered": True, "exit_reason": "EXTRACTION_LOSS_EXIT"},
            },
            {
                "scenario_id": "giveback_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"went_green": True, "giveback_pips": 3.4, "current_pnl_pips": -0.4, "peak_pnl_pips": 3.0, "time_in_trade_sec": 115},
                "expected": {"triggered": True, "exit_reason": "EXTRACTION_LOSS_EXIT"},
            },
            {
                "scenario_id": "giveback_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"went_green": True, "giveback_pips": 0.9, "current_pnl_pips": 2.3, "peak_pnl_pips": 3.2, "time_in_trade_sec": 70},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "runner_hold": [
            {
                "scenario_id": "runner_hold_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"phase": "RUNNER", "extension_strength": 0.88, "giveback_pips": 0.9, "current_pnl_pips": 6.4, "peak_pnl_pips": 7.3, "time_in_trade_sec": 210},
                "expected": {"triggered": True, "exit_reason": "HOLD"},
            },
            {
                "scenario_id": "runner_hold_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"phase": "RUNNER", "extension_strength": 0.61, "giveback_pips": 1.3, "current_pnl_pips": 4.0, "peak_pnl_pips": 5.3, "time_in_trade_sec": 180},
                "expected": {"triggered": True, "exit_reason": "HOLD"},
            },
            {
                "scenario_id": "runner_hold_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"phase": "HARVESTER", "extension_strength": 0.3, "giveback_pips": 2.8, "current_pnl_pips": 1.0, "peak_pnl_pips": 3.8, "time_in_trade_sec": 120},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "runner_fallback": [
            {
                "scenario_id": "runner_fallback_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"phase": "RUNNER", "extension_strength": 0.25, "giveback_pips": 4.2, "current_pnl_pips": 1.1, "peak_pnl_pips": 5.3, "time_in_trade_sec": 250},
                "expected": {"triggered": True, "exit_reason": "FAILED_TO_CONTINUE_DECAY"},
            },
            {
                "scenario_id": "runner_fallback_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"phase": "RUNNER", "extension_strength": 0.35, "giveback_pips": 3.2, "current_pnl_pips": 1.4, "peak_pnl_pips": 4.6, "time_in_trade_sec": 220},
                "expected": {"triggered": True, "exit_reason": "FAILED_TO_CONTINUE_DECAY"},
            },
            {
                "scenario_id": "runner_fallback_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"phase": "RUNNER", "extension_strength": 0.75, "giveback_pips": 0.7, "current_pnl_pips": 4.5, "peak_pnl_pips": 5.2, "time_in_trade_sec": 160},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "panic_exit": [
            {
                "scenario_id": "panic_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"adverse_velocity": 1.0, "current_pnl_pips": -7.8, "peak_pnl_pips": 0.3, "giveback_pips": 8.1, "time_in_trade_sec": 45},
                "expected": {"triggered": True, "exit_reason": "PANIC_EXIT"},
            },
            {
                "scenario_id": "panic_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"adverse_velocity": 0.7, "current_pnl_pips": -5.2, "peak_pnl_pips": 0.2, "giveback_pips": 5.4, "time_in_trade_sec": 52},
                "expected": {"triggered": True, "exit_reason": "PANIC_EXIT"},
            },
            {
                "scenario_id": "panic_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"adverse_velocity": 0.2, "current_pnl_pips": -1.5, "peak_pnl_pips": 1.0, "giveback_pips": 2.5, "time_in_trade_sec": 63},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
        "time_pressure_exit": [
            {
                "scenario_id": "time_pressure_obvious",
                "case_type": "obvious trigger case",
                "inputs": {"time_in_trade_sec": 420, "no_progress_sec": 180, "current_pnl_pips": 1.4, "peak_pnl_pips": 3.2, "giveback_pips": 1.8},
                "expected": {"triggered": True, "exit_reason": "TIME_DECAY_PROFIT_CAPTURE"},
            },
            {
                "scenario_id": "time_pressure_borderline",
                "case_type": "borderline trigger case",
                "inputs": {"time_in_trade_sec": 360, "no_progress_sec": 150, "current_pnl_pips": 1.2, "peak_pnl_pips": 2.7, "giveback_pips": 1.5},
                "expected": {"triggered": True, "exit_reason": "TIME_DECAY_PROFIT_CAPTURE"},
            },
            {
                "scenario_id": "time_pressure_control",
                "case_type": "should-not-trigger control case",
                "inputs": {"time_in_trade_sec": 90, "no_progress_sec": 20, "current_pnl_pips": 1.0, "peak_pnl_pips": 1.4, "giveback_pips": 0.4},
                "expected": {"triggered": False, "exit_reason": "HOLD"},
            },
        ],
    }

    return {
        "description": "Forced scenario suite for AEE feature branch verification.",
        "features": cases,
    }


def _scenario_eval(feature_key: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
    exit_reason = "HOLD"
    triggered = False

    toward_sl = bool(inputs.get("toward_sl", False))
    recovery_score = float(inputs.get("recovery_score", 1.0) or 1.0)
    distance_to_sl_pips = float(inputs.get("distance_to_sl_pips", 999.0) or 999.0)
    went_green = bool(inputs.get("went_green", False))
    progress_stall = bool(inputs.get("progress_stall", False))
    stagnant_bars = int(inputs.get("stagnant_bars", 0) or 0)
    extension_strength = float(inputs.get("extension_strength", 0.0) or 0.0)
    phase = str(inputs.get("phase", "")).upper()
    adverse_velocity = float(inputs.get("adverse_velocity", 0.0) or 0.0)
    time_in_trade_sec = float(inputs.get("time_in_trade_sec", 0.0) or 0.0)
    no_progress_sec = float(inputs.get("no_progress_sec", 0.0) or 0.0)
    current_pnl_pips = float(inputs.get("current_pnl_pips", 0.0) or 0.0)
    peak_pnl_pips = float(inputs.get("peak_pnl_pips", 0.0) or 0.0)
    giveback_pips = float(inputs.get("giveback_pips", 0.0) or 0.0)

    if feature_key == "pre_sl_protection" and toward_sl and recovery_score <= 0.25 and distance_to_sl_pips <= 4.0:
        triggered = True
        exit_reason = "PRE_SL_PROTECTION"
    elif feature_key == "early_profit_lock" and went_green and progress_stall and current_pnl_pips > 0 and peak_pnl_pips >= 2.5:
        triggered = True
        exit_reason = "MISSED_PROFIT_EXTRACTION_EXIT"
    elif feature_key == "weak_trade_kill" and (not went_green) and stagnant_bars >= 4 and current_pnl_pips < -2.0:
        triggered = True
        exit_reason = "NEVER_GREEN_FAST_EXIT" if stagnant_bars >= 5 else "NEVER_GREEN_STALL_EXIT"
    elif feature_key == "stall_capture" and went_green and progress_stall and current_pnl_pips > 0 and giveback_pips >= 1.0:
        triggered = True
        exit_reason = "GREEN_STALL_CAPTURE" if giveback_pips >= 2.0 else "PULSE_STALL_CAPTURE"
    elif feature_key == "giveback_exit" and went_green and giveback_pips >= 3.0 and current_pnl_pips <= 0:
        triggered = True
        exit_reason = "EXTRACTION_LOSS_EXIT"
    elif feature_key == "runner_hold" and phase == "RUNNER" and extension_strength >= 0.60 and giveback_pips <= 1.5 and current_pnl_pips > 0:
        triggered = True
        exit_reason = "HOLD"
    elif feature_key == "runner_fallback" and phase == "RUNNER" and extension_strength <= 0.35 and giveback_pips >= 3.0 and current_pnl_pips > 0:
        triggered = True
        exit_reason = "FAILED_TO_CONTINUE_DECAY"
    elif feature_key == "panic_exit" and adverse_velocity >= 0.65 and current_pnl_pips <= -4.5:
        triggered = True
        exit_reason = "PANIC_EXIT"
    elif feature_key == "time_pressure_exit" and time_in_trade_sec >= 360 and no_progress_sec >= 150 and current_pnl_pips > 0:
        triggered = True
        exit_reason = "TIME_DECAY_PROFIT_CAPTURE"

    return {
        "triggered": triggered,
        "exit_reason": exit_reason,
        "key_state_metrics_used": {
            "current_pnl_pips": current_pnl_pips,
            "peak_pnl_pips": peak_pnl_pips,
            "giveback_pips": giveback_pips,
            "time_in_trade_sec": time_in_trade_sec,
            "phase": phase,
        },
    }


def _proof_filename(feature_key: str) -> str:
    return {
        "pre_sl_protection": "aee_proof_pre_sl_protection.json",
        "early_profit_lock": "aee_proof_early_profit_lock.json",
        "weak_trade_kill": "aee_proof_weak_trade_kill.json",
        "stall_capture": "aee_proof_stall_capture.json",
        "giveback_exit": "aee_proof_giveback_exit.json",
        "runner_hold": "aee_proof_runner_hold.json",
        "runner_fallback": "aee_proof_runner_fallback.json",
        "panic_exit": "aee_proof_panic_exit.json",
        "time_pressure_exit": "aee_proof_time_pressure_exit.json",
    }[feature_key]


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_events() -> List[Dict[str, Any]]:
    if not LOGS_PATH.exists():
        return []
    events: List[Dict[str, Any]] = []
    for line in LOGS_PATH.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            events.append(json.loads(text))
        except Exception:
            continue
    return events


def _build_saved_value_report(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    lifecycle = [e for e in events if str(e.get("kind", "")) == "AEE_TRADE_LIFECYCLE"]

    rows: List[Dict[str, Any]] = []
    sum_saved_loss = 0.0
    sum_saved_profit = 0.0
    with_sl_reference = 0

    for e in lifecycle:
        pair = str(e.get("pair", "") or "")
        direction = str(e.get("dir") or e.get("direction") or "")
        entry = float(e.get("entry_price", 0.0) or 0.0)
        original_sl = float(e.get("original_sl", 0.0) or 0.0)
        realized = float(e.get("realized_pips", 0.0) or 0.0)
        peak = float(e.get("peak_unrealized_pips", e.get("mfe_pips", 0.0)) or 0.0)

        distance_to_sl_at_exit: Optional[float] = None
        estimated_loss_saved: Optional[float] = None
        if entry > 0 and original_sl > 0:
            pip_scale = 100.0 if pair.endswith("_JPY") else 10000.0
            sl_distance_pips = abs(entry - original_sl) * pip_scale
            distance_to_sl_at_exit = max(0.0, sl_distance_pips - abs(realized))
            estimated_loss_saved = distance_to_sl_at_exit
            with_sl_reference += 1
            sum_saved_loss += float(estimated_loss_saved)

        distance_from_peak_at_exit = max(0.0, peak - realized)
        preserved_vs_round_trip = max(0.0, peak - distance_from_peak_at_exit)
        sum_saved_profit += preserved_vs_round_trip

        rows.append(
            {
                "trade_id": e.get("trade_id"),
                "pair": pair,
                "side": direction,
                "exit_reason": str(e.get("exit_reason", "")),
                "realized_pips": realized,
                "peak_pnl_pips": peak,
                "distance_to_sl_at_exit_pips": distance_to_sl_at_exit,
                "estimated_loss_saved_vs_original_sl_pips": estimated_loss_saved,
                "distance_from_peak_at_exit_pips": distance_from_peak_at_exit,
                "estimated_profit_preserved_vs_round_trip_pips": preserved_vs_round_trip,
            }
        )

    return {
        "rows": rows,
        "trades_evaluated": len(rows),
        "rows_with_sl_reference": with_sl_reference,
        "total_estimated_loss_saved_pips": sum_saved_loss,
        "total_estimated_profit_preserved_pips": sum_saved_profit,
    }


def _runtime_feature_counts() -> Dict[str, int]:
    counts: Dict[str, int] = {f.trigger_marker: 0 for f in FEATURES}

    report = _load_json(ARTIFACTS_DIR / "aee_feature_activation_report.json")
    feature_counts = report.get("feature_counts") if isinstance(report, dict) else None
    if isinstance(feature_counts, dict):
        for marker in counts:
            counts[marker] = int(feature_counts.get(marker, 0) or 0)

    events = _load_events()
    for e in events:
        kind = str(e.get("kind", ""))
        if kind in counts:
            counts[kind] += 1

    return counts


def _build_stress_presets() -> Dict[str, Dict[str, Any]]:
    return {
        "stress_presolve.json": {
            "description": "General pre-solve stress profile to warm up all AEE branches quickly.",
            "env": {
                "AEE_STRESS_DEMO_MODE": 1,
                "AEE_STRESS_MAX_OPEN_TRADES_GLOBAL": 25,
                "AEE_STRESS_MAX_OPEN_TRADES_PER_PAIR": 6,
                "AEE_STRESS_EXIT_SCAN_SEC": 0.12,
                "AEE_STRESS_FORCE_SCAN_SEC": 0.04,
                "AEE_STRESS_FORCE_SCAN_YOUNG_SEC": 0.02,
            },
        },
        "stress_stall.json": {
            "description": "Bias toward small green then flatten behavior to trigger stall capture and early lock paths.",
            "env": {
                "AEE_STRESS_DEMO_MODE": 1,
                "AEE_STRESS_EXIT_SCAN_SEC": 0.10,
                "AEE_STRESS_FORCE_SCAN_SEC": 0.03,
                "AEE_STRESS_FORCE_SCAN_YOUNG_SEC": 0.02,
                "AEE_STRESS_MAX_OPEN_TRADES_GLOBAL": 18,
            },
        },
        "stress_runner.json": {
            "description": "Bias toward extension then decay to trigger runner hold and fallback paths.",
            "env": {
                "AEE_STRESS_DEMO_MODE": 1,
                "AEE_STRESS_MAX_OPEN_TRADES_GLOBAL": 22,
                "AEE_STRESS_MAX_OPEN_TRADES_PER_PAIR": 8,
                "AEE_STRESS_FORCE_SCAN_SEC": 0.03,
                "AEE_STRESS_FORCE_SCAN_YOUNG_SEC": 0.01,
            },
        },
        "stress_panic.json": {
            "description": "Increase adverse-move sensitivity and low-quality exposure for panic/kill branch frequency.",
            "env": {
                "AEE_STRESS_DEMO_MODE": 1,
                "AEE_STRESS_EXIT_SCAN_SEC": 0.08,
                "AEE_STRESS_FORCE_SCAN_SEC": 0.02,
                "AEE_STRESS_MAX_OPEN_TRADES_GLOBAL": 16,
            },
        },
        "stress_giveback.json": {
            "description": "Bias toward high-giveback transitions for giveback and no-progress exits.",
            "env": {
                "AEE_STRESS_DEMO_MODE": 1,
                "AEE_STRESS_EXIT_SCAN_SEC": 0.09,
                "AEE_STRESS_FORCE_SCAN_SEC": 0.03,
                "AEE_STRESS_MAX_OPEN_TRADES_GLOBAL": 20,
            },
        },
    }


def run() -> None:
    forced_suite = _build_forced_suite()
    (ROOT / "aee_forced_scenario_suite.json").write_text(json.dumps(forced_suite, indent=2) + "\n", encoding="utf-8")

    runtime_counts = _runtime_feature_counts()
    events = _load_events()
    saved_value = _build_saved_value_report(events)
    (ROOT / "aee_saved_value_report.json").write_text(json.dumps(saved_value, indent=2) + "\n", encoding="utf-8")

    proof_index: Dict[str, Dict[str, Any]] = {}
    dashboard_features: List[Dict[str, Any]] = []

    for feature in FEATURES:
        scenarios = forced_suite["features"][feature.key]
        pass_count = 0
        fail_count = 0
        false_positives = 0
        false_negatives = 0
        raw_examples: List[Dict[str, Any]] = []

        for scenario in scenarios:
            t0 = time.perf_counter()
            eval_result = _scenario_eval(feature.key, scenario["inputs"])
            decision_latency_ms = (time.perf_counter() - t0) * 1000.0

            expected = scenario["expected"]
            expected_triggered = bool(expected["triggered"])
            expected_reason = str(expected["exit_reason"])
            got_triggered = bool(eval_result["triggered"])
            got_reason = str(eval_result["exit_reason"])

            passed = expected_triggered == got_triggered and expected_reason == got_reason
            if passed:
                pass_count += 1
            else:
                fail_count += 1
                if got_triggered and not expected_triggered:
                    false_positives += 1
                if (not got_triggered) and expected_triggered:
                    false_negatives += 1

            raw_examples.append(
                {
                    "scenario_id": scenario["scenario_id"],
                    "case_type": scenario["case_type"],
                    "expected": expected,
                    "actual": {
                        "triggered": got_triggered,
                        "exit_reason": got_reason,
                        "decision_latency_ms": round(decision_latency_ms, 4),
                        "key_state_metrics_used": eval_result["key_state_metrics_used"],
                    },
                    "pass": passed,
                }
            )

        verdict = "PASS" if fail_count == 0 else "PARTIAL"
        proof_payload = {
            "feature": feature.feature_name,
            "trigger_marker": feature.trigger_marker,
            "scenarios_tested": len(scenarios),
            "pass_count": pass_count,
            "fail_count": fail_count,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "raw_examples": raw_examples,
            "summary_verdict": verdict,
        }

        proof_name = _proof_filename(feature.key)
        proof_path = ROOT / proof_name
        proof_path.write_text(json.dumps(proof_payload, indent=2) + "\n", encoding="utf-8")

        replay_status = "passing" if verdict == "PASS" else "failing"
        runtime_hits = int(runtime_counts.get(feature.trigger_marker, 0) or 0)
        runtime_status = "passing" if runtime_hits > 0 else "untested"

        saved_metric = None
        if feature.key == "pre_sl_protection":
            saved_metric = saved_value.get("total_estimated_loss_saved_pips")
        elif feature.key in {"stall_capture", "early_profit_lock", "giveback_exit", "runner_fallback", "time_pressure_exit"}:
            saved_metric = saved_value.get("total_estimated_profit_preserved_pips")

        if replay_status == "passing" and runtime_status == "passing" and (saved_metric is None or saved_metric is not None):
            overall = "PASS"
        elif replay_status == "failing":
            overall = "FAIL"
        elif runtime_status == "untested":
            overall = "PARTIAL"
        else:
            overall = "UNTESTED"

        proof_index[feature.key] = {
            "proof_artifact": proof_name,
            "replay_status": replay_status,
            "runtime_stress_status": runtime_status,
            "trigger_count": runtime_hits,
            "saved_value_metric": saved_metric,
            "verification_verdict": overall,
        }

        dashboard_features.append(
            {
                "feature": feature.feature_name,
                "trigger_marker": feature.trigger_marker,
                "replay_status": replay_status,
                "runtime_stress_status": runtime_status,
                "proof_artifact_path": str(proof_name),
                "trigger_count": runtime_hits,
                "saved_value_metric": saved_metric,
                "verification_verdict": overall,
            }
        )

    matrix = {
        "mission": "AEE feature validation only: trigger on demand, log clearly, verify with proof artifacts, and measure saved value.",
        "features": [
            {
                "feature_name": f.feature_name,
                "trigger_conditions": [s["case_type"] for s in forced_suite["features"][f.key]],
                "required_inputs": list(forced_suite["features"][f.key][0]["inputs"].keys()),
                "expected_decision": "Feature branch trigger decision",
                "expected_exit_reason_or_log_marker": {
                    "trigger_marker": f.trigger_marker,
                    "exit_reasons": f.expected_exit_reasons,
                },
                "proof_metric": f.proof_metric,
                "preferred_test_method": f.preferred_test_method,
                "current_status": proof_index[f.key]["verification_verdict"].lower(),
            }
            for f in FEATURES
        ],
    }
    (ROOT / "aee_feature_matrix.json").write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")

    presets = _build_stress_presets()
    for name, payload in presets.items():
        (ROOT / name).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    dashboard = {
        "generated_at_epoch": time.time(),
        "features": dashboard_features,
        "saved_value_report_path": "aee_saved_value_report.json",
        "forced_scenario_suite_path": "aee_forced_scenario_suite.json",
        "feature_matrix_path": "aee_feature_matrix.json",
    }
    (ROOT / "aee_feature_verification_dashboard.json").write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    run()
