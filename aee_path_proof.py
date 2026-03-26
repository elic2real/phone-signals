#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(".")

ACTION_HOLD = "HOLD"
ACTION_PARTIAL = "PARTIAL"
ACTION_TIGHTEN = "TIGHTEN"
ACTION_CLOSE = "CLOSE"

MODE_HARVESTER = "HARVESTER"
MODE_RUNNER = "RUNNER"


@dataclass
class AEEPathState:
    mode: str
    step: int
    current_r: float
    mfe_r: float
    mae_r: float
    bars_since_entry: int
    bars_since_progress: int
    giveback_from_peak_r: float
    velocity_r: float
    energy: float
    went_meaningfully_green: bool
    partial_taken: bool
    tightened: bool
    state: str


@dataclass
class ModeContext:
    mode: str
    mfe_r: float = 0.0
    mae_r: float = 0.0
    last_progress_step: int = 0
    went_green: bool = False
    partial_taken: bool = False
    tightened: bool = False
    closed: bool = False


def _energy_from_velocity(velocity_r: float) -> float:
    if velocity_r <= 0.0:
        return 0.0
    return min(1.0, velocity_r / 0.25)


def _state_name(mode: str, ctx: ModeContext, current_r: float, bars_since_progress: int, giveback: float, strong_cont: bool) -> str:
    if ctx.closed:
        return "CLOSED"

    if mode == MODE_HARVESTER:
        if not ctx.went_green:
            return "OPEN"
        if giveback >= 0.20 or current_r < 0.0:
            return "DECAYING"
        if bars_since_progress >= 2 and not strong_cont:
            return "STALLING"
        return "GREEN"

    if not ctx.partial_taken:
        return "PROVED" if current_r >= 0.30 and strong_cont else "OPEN"
    if giveback >= 0.25 or current_r < 0.0:
        return "DECAYING"
    if strong_cont:
        return "CONTINUING"
    return "PARTIAL_TAKEN"


def _decide_action(state: AEEPathState, ctx: ModeContext) -> str:
    if state.current_r <= -0.30:
        return ACTION_CLOSE

    strong_cont = state.velocity_r >= 0.12 and state.current_r >= (state.mfe_r - 0.05)

    if ctx.mode == MODE_HARVESTER:
        if state.went_meaningfully_green and state.giveback_from_peak_r >= 0.12:
            return ACTION_CLOSE
        if state.went_meaningfully_green and state.bars_since_progress >= 2 and state.energy < 0.35:
            return ACTION_CLOSE
        if state.went_meaningfully_green and (not state.partial_taken) and (not strong_cont):
            return ACTION_PARTIAL
        if state.state == "DECAYING":
            return ACTION_CLOSE
        return ACTION_HOLD

    # Runner doctrine
    if (not state.partial_taken) and state.current_r >= 0.30 and strong_cont:
        return ACTION_PARTIAL

    if state.partial_taken:
        if state.giveback_from_peak_r >= 0.20 and (not state.tightened):
            return ACTION_TIGHTEN
        if state.tightened and (state.giveback_from_peak_r >= 0.25 or (state.bars_since_progress >= 2 and state.energy < 0.25)):
            return ACTION_CLOSE
        if state.state == "DECAYING":
            return ACTION_CLOSE
        return ACTION_HOLD

    if state.went_meaningfully_green and state.giveback_from_peak_r >= 0.14 and (not state.tightened):
        return ACTION_TIGHTEN
    if state.tightened and state.current_r <= 0.0:
        return ACTION_CLOSE

    if state.went_meaningfully_green and state.bars_since_progress >= 2 and state.energy < 0.25 and state.current_r <= 0.0:
        return ACTION_CLOSE

    return ACTION_HOLD


def _build_state(mode: str, ctx: ModeContext, step: int, current_r: float, prev_r: float) -> AEEPathState:
    if current_r > ctx.mfe_r:
        ctx.mfe_r = current_r
        ctx.last_progress_step = step
    if current_r < ctx.mae_r:
        ctx.mae_r = current_r
    if current_r >= 0.12:
        ctx.went_green = True

    velocity_r = current_r - prev_r
    bars_since_progress = step - ctx.last_progress_step
    giveback = max(0.0, ctx.mfe_r - current_r)
    strong_cont = velocity_r >= 0.12 and current_r >= (ctx.mfe_r - 0.05)

    state_name = _state_name(mode, ctx, current_r, bars_since_progress, giveback, strong_cont)
    return AEEPathState(
        mode=mode,
        step=step,
        current_r=round(current_r, 4),
        mfe_r=round(ctx.mfe_r, 4),
        mae_r=round(ctx.mae_r, 4),
        bars_since_entry=step,
        bars_since_progress=bars_since_progress,
        giveback_from_peak_r=round(giveback, 4),
        velocity_r=round(velocity_r, 4),
        energy=round(_energy_from_velocity(velocity_r), 4),
        went_meaningfully_green=ctx.went_green,
        partial_taken=ctx.partial_taken,
        tightened=ctx.tightened,
        state=state_name,
    )


def run_fixture(path_points_r: List[float], mode: str, expected_final_action: str) -> Dict[str, object]:
    ctx = ModeContext(mode=mode)
    traces: List[Dict[str, object]] = []
    prev_r = path_points_r[0]
    final_action = ACTION_HOLD

    for step in range(1, len(path_points_r)):
        current_r = path_points_r[step]
        state = _build_state(mode, ctx, step, current_r, prev_r)
        action = _decide_action(state, ctx)
        final_action = action

        if action == ACTION_PARTIAL:
            ctx.partial_taken = True
        elif action == ACTION_TIGHTEN:
            ctx.tightened = True
        elif action == ACTION_CLOSE:
            ctx.closed = True

        # Capture post-action flags for trace readability.
        state_after = asdict(state)
        state_after["partial_taken"] = ctx.partial_taken
        state_after["tightened"] = ctx.tightened
        state_after["closed"] = ctx.closed

        traces.append(
            {
                "step": step,
                "r": round(current_r, 4),
                "state": state_after,
                "action": action,
            }
        )

        prev_r = current_r
        if ctx.closed:
            break

    return {
        "mode": mode,
        "path_points_r": path_points_r,
        "action_trace": traces,
        "final_action": final_action,
        "expected_final_action": expected_final_action,
        "pass": final_action == expected_final_action,
    }


def _write_json(name: str, payload: Dict[str, object]) -> None:
    (ROOT / name).write_text(json.dumps(payload, indent=2))


def build_proofs() -> Dict[str, object]:
    fixtures = [
        {
            "name": "touch_green_stall",
            "file": "aee_proof_touch_green_stall.json",
            "path": [0.0, 0.10, 0.18, 0.16, 0.15, 0.15],
            "expected": {
                MODE_HARVESTER: ACTION_CLOSE,
                MODE_RUNNER: ACTION_HOLD,
            },
            "doctrine": "First green then stall should be harvested quickly in harvester mode.",
        },
        {
            "name": "touch_green_decay",
            "file": "aee_proof_touch_green_decay.json",
            "path": [0.0, 0.20, 0.12, 0.05, 0.0],
            "expected": {
                MODE_HARVESTER: ACTION_CLOSE,
                MODE_RUNNER: ACTION_CLOSE,
            },
            "doctrine": "Decay after early green should force fast extraction response.",
            "required_trace_actions": {
                MODE_RUNNER: [ACTION_TIGHTEN],
            },
        },
        {
            "name": "strong_extension",
            "file": "aee_proof_strong_extension.json",
            "path": [0.0, 0.15, 0.35, 0.60, 1.10],
            "expected": {
                MODE_HARVESTER: ACTION_HOLD,
                MODE_RUNNER: ACTION_HOLD,
            },
            "doctrine": "Strong continuation should not be choked; runner should partial then continue.",
            "required_trace_actions": {
                MODE_RUNNER: [ACTION_PARTIAL],
            },
        },
        {
            "name": "immediate_failure",
            "file": "aee_proof_immediate_failure.json",
            "path": [0.0, -0.10, -0.20, -0.40],
            "expected": {
                MODE_HARVESTER: ACTION_CLOSE,
                MODE_RUNNER: ACTION_CLOSE,
            },
            "doctrine": "Immediate adverse failure should close.",
        },
        {
            "name": "whipsaw",
            "file": "aee_proof_whipsaw.json",
            "path": [0.0, 0.18, -0.08, 0.12, -0.15],
            "expected": {
                MODE_HARVESTER: ACTION_CLOSE,
                MODE_RUNNER: ACTION_CLOSE,
            },
            "doctrine": "Whipsaw should not be mistaken for real continuation.",
        },
    ]

    summary_rows: List[Dict[str, object]] = []

    for fixture in fixtures:
        runs: List[Dict[str, object]] = []
        for mode in (MODE_HARVESTER, MODE_RUNNER):
            run = run_fixture(fixture["path"], mode, fixture["expected"][mode])

            required_actions = (fixture.get("required_trace_actions") or {}).get(mode, [])
            missing_required = []
            if required_actions:
                action_trace = run.get("action_trace", [])
                if not isinstance(action_trace, list):
                    action_trace = []
                observed = [step.get("action") for step in action_trace if isinstance(step, dict)]
                for req in required_actions:
                    if req not in observed:
                        missing_required.append(req)
                if missing_required:
                    run["pass"] = False
                    run["missing_required_actions"] = missing_required

            runs.append(run)
            summary_rows.append(
                {
                    "fixture": fixture["name"],
                    "mode": mode,
                    "expected_final_action": fixture["expected"][mode],
                    "final_action": run["final_action"],
                    "pass": bool(run["pass"]),
                }
            )

        fixture_payload = {
            "fixture": fixture["name"],
            "doctrine": fixture["doctrine"],
            "path_points_r": fixture["path"],
            "results": runs,
        }
        _write_json(fixture["file"], fixture_payload)

    pass_count = sum(1 for row in summary_rows if row["pass"])
    total = len(summary_rows)
    summary = {
        "proof_standard": [
            "input path",
            "state updates per step",
            "AEE action per step",
            "final action",
            "pass/fail against expected doctrine",
        ],
        "total_checks": total,
        "passed_checks": pass_count,
        "failed_checks": total - pass_count,
        "all_passed": pass_count == total,
        "checks": summary_rows,
    }
    _write_json("aee_proof_summary.json", summary)
    return summary


def main() -> None:
    build_proofs()


if __name__ == "__main__":
    main()
