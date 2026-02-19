#!/usr/bin/env python3
"""AEE validation report generator based on sim_harness telemetry."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


PRIORITY_ORDER = ["PANIC_EXIT", "NEAR_TP_STALL_CAPTURE", "PULSE_STALL_CAPTURE", "FAILED_TO_CONTINUE_DECAY"]


class AEEValidator:
    def __init__(self, sim_results: Dict[str, Any]):
        self.results = sim_results
        self.logs = sim_results.get("logs", [])
        self.tick_records = sim_results.get("tick_records", [])
        self.trace_events = [e for e in self.logs if e.get("event") == "AEE_EVAL_TRACE"]

    def run_all_validations(self) -> dict:
        # Placeholder: run all available validations and return a summary dict
        results = {}
        # Example: add cadence switching validation
        results['cadence_switching'] = self.validate_cadence_switching()
        # Add more validations as needed
        return results
    def validate_cadence_switching(self) -> Dict[str, Any]:
        if len(self.trace_events) < 3:
            return {"error": "not_enough_trace_events"}
        for ev in self.trace_events:
            payload = ev.get("payload", {})
            pair_eval_idx = int(payload.get("pair_eval_idx", -1))
            metrics = payload.get("metrics", {}) or {}
            dist = float(metrics.get("dist_to_tp", 9e9) or 9e9)
            band = float(metrics.get("near_tp_band", 0.25) or 0.25)
        near_gaps = [near_idx[i + 1] - near_idx[i] for i in range(len(near_idx) - 1)]
        avg_far = (sum(far_gaps) / len(far_gaps)) if far_gaps else 0.0
        avg_near = (sum(near_gaps) / len(near_gaps)) if near_gaps else 0.0

        switched = bool(avg_far > 0 and avg_near > 0 and avg_near < avg_far)
        return {
            "far_eval_gaps": far_gaps,
            "near_eval_gaps": near_gaps,
            "avg_gap_far_tp": avg_far,
            "avg_gap_near_tp": avg_near,
            "switched": switched,
            "interpretation": "TICK_MODE_ENGAGED" if switched else "NO_CLEAR_SWITCH",
            "samples": {"far": len(far_idx), "near": len(near_idx)},
        }

    def validate_priority_override(self) -> Dict[str, Any]:
        violations: List[Dict[str, Any]] = []
        checked: List[Dict[str, Any]] = []

        for ev in self.trace_events:
            payload = ev.get("payload", {})
            chosen = payload.get("aee_chosen")
            truths = list(payload.get("aee_true", []) or [])
            if not chosen:
                continue

            top_true: Optional[str] = None
            for name in PRIORITY_ORDER:
                if name in truths:
                    top_true = name
                    break

            checked.append(
                {
                    "idx": payload.get("idx"),
                    "pair_eval_idx": payload.get("pair_eval_idx"),
                    "instrument": payload.get("instrument"),
                    "chosen": chosen,
                    "aee_true": truths,
                    "top_true": top_true,
                }
            )
            if top_true and chosen != top_true:
                violations.append(
                    {
                        "idx": payload.get("idx"),
                        "pair_eval_idx": payload.get("pair_eval_idx"),
                        "chosen": chosen,
                        "top_true": top_true,
                        "aee_true": truths,
                    }
                )

        return {"checked": checked, "violations": violations, "summary": "PASS" if not violations else "FAIL"}

    def validate_exit_triggers(self) -> Dict[str, Any]:
        exit_record = self.results.get("exit")
        if not exit_record:
            return {"error": "no_exit_detected"}

        exit_idx = int(exit_record.get("idx", -1))
        exit_reason = str(exit_record.get("exit_reason", "UNKNOWN"))
        trace = None
        for ev in self.trace_events:
            payload = ev.get("payload", {})
            if int(payload.get("idx", -2)) == exit_idx:
                trace = payload
                break

        if trace is None:
            return {
                "exit_reason": exit_reason,
                "trigger_tick": exit_idx,
                "error": "missing_trace_event_for_exit_tick",
            }

        metric_map = {
            "PANIC_EXIT": "velocity/pullback",
            "NEAR_TP_STALL_CAPTURE": "near_tp stall condition",
            "PULSE_STALL_CAPTURE": "pulse_exit_line cross",
            "FAILED_TO_CONTINUE_DECAY": "pullback_rate or giveback_cap",
        }

        return {
            "exit_reason": exit_reason,
            "trigger_tick": exit_idx,
            "trigger_metric": metric_map.get(exit_reason, "unknown"),
            "metrics_at_exit": trace.get("metrics", {}),
            "aee_true": trace.get("aee_true", []),
            "aee_chosen": trace.get("aee_chosen"),
            "pair_eval_idx": trace.get("pair_eval_idx"),
            "instrument": trace.get("instrument"),

            "PANIC_EXIT": "velocity/pullback",
