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

    def validate_cadence_switching(self) -> Dict[str, Any]:
        if len(self.trace_events) < 3:
            return {"error": "not_enough_trace_events"}

        far_idx: List[int] = []
        near_idx: List[int] = []

        for ev in self.trace_events:
            payload = ev.get("payload", {})
            pair_eval_idx = int(payload.get("pair_eval_idx", -1))
            metrics = payload.get("metrics", {}) or {}
            dist = float(metrics.get("dist_to_tp", 9e9) or 9e9)
            band = float(metrics.get("near_tp_band", 0.25) or 0.25)
            if pair_eval_idx < 0:
                continue
            if dist <= band:
                near_idx.append(pair_eval_idx)
            else:
                far_idx.append(pair_eval_idx)

        far_gaps = [far_idx[i + 1] - far_idx[i] for i in range(len(far_idx) - 1)]
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
            "ts": trace.get("ts"),
        }

    def analyze_left_on_table(self) -> Dict[str, Any]:
        records = self.tick_records
        trade = self.results.get("trade", {})
        exit_record = self.results.get("exit")
        if not records or not trade or not exit_record:
            return {"error": "missing_required_data"}

        mids = [float(r.get("mid", 0.0)) for r in records if r.get("mid") is not None]
        if not mids:
            return {"error": "no_mid_prices"}

        entry = float(trade.get("entry", 0.0))
        direction = str(trade.get("dir", "LONG"))
        if direction == "LONG":
            best = max(mids)
            exit_px = float(exit_record.get("bid", exit_record.get("mid", 0.0)))
            mfe_pips = (best - entry) * 10000.0
            pnl_pips = (exit_px - entry) * 10000.0
        else:
            best = min(mids)
            exit_px = float(exit_record.get("ask", exit_record.get("mid", 0.0)))
            mfe_pips = (entry - best) * 10000.0
            pnl_pips = (entry - exit_px) * 10000.0

        capture = (pnl_pips / mfe_pips) if mfe_pips > 0 else 0.0
        quality = "PREMATURE"
        if capture >= 0.95:
            quality = "OPTIMAL"
        elif capture >= 0.80:
            quality = "GOOD"

        return {
            "mfe_pips": mfe_pips,
            "exit_pnl_pips": pnl_pips,
            "capture_rate": capture,
            "left_on_table_pips": mfe_pips - pnl_pips,
            "quality": quality,
        }

    def run_all_validations(self) -> Dict[str, Any]:
        return {
            "cadence_switching": self.validate_cadence_switching(),
            "priority_override": self.validate_priority_override(),
            "exit_triggers": self.validate_exit_triggers(),
            "left_on_table": self.analyze_left_on_table(),
        }

    def export_report(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.run_all_validations(), f, indent=2)


if __name__ == "__main__":
    with open("sim_results.json", "r", encoding="utf-8") as f:
        results = json.load(f)
    AEEValidator(results).export_report("aee_validation_report.json")
    print("[AEEValidator] Report exported to aee_validation_report.json")
