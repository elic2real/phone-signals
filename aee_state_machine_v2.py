from dataclasses import dataclass
from typing import Literal

AEEState = Literal["PROTECT", "BUILD", "HARVEST", "RUNNER", "PANIC", "CLOSED"]
AEEAction = Literal["HOLD", "TIGHTEN", "PARTIAL_EXIT", "FULL_EXIT"]


@dataclass
class AEEContext:
    progress_r: float
    unrealized_pips: float
    giveback_r: float
    continuation_score: float
    stall_score: float
    panic_trigger: bool


@dataclass
class TransitionResult:
    next_state: AEEState
    action: AEEAction
    reason: str


def transition_aee_state(current_state: AEEState, ctx: AEEContext) -> TransitionResult:
    if current_state == "CLOSED":
        return TransitionResult("CLOSED", "HOLD", "already_closed")

    if current_state == "PANIC":
        return TransitionResult("CLOSED", "FULL_EXIT", "panic_forced_exit")

    if ctx.panic_trigger:
        return TransitionResult("PANIC", "FULL_EXIT", "panic_trigger")

    if current_state == "PROTECT":
        if ctx.progress_r >= 0.25 and ctx.continuation_score >= 0.45:
            return TransitionResult("BUILD", "HOLD", "protect_to_build")
        return TransitionResult("PROTECT", "TIGHTEN", "protect_risk_control")

    if current_state == "BUILD":
        if ctx.unrealized_pips >= 4.0 and ctx.progress_r >= 0.60:
            return TransitionResult("HARVEST", "PARTIAL_EXIT", "build_to_harvest")
        if ctx.giveback_r >= 0.90:
            return TransitionResult("PANIC", "FULL_EXIT", "build_safety_breach")
        return TransitionResult("BUILD", "HOLD", "build_continue")

    if current_state == "HARVEST":
        if ctx.continuation_score >= 0.75 and ctx.stall_score <= 0.30:
            return TransitionResult("RUNNER", "HOLD", "harvest_to_runner")
        if ctx.giveback_r >= 0.70:
            return TransitionResult("PANIC", "FULL_EXIT", "harvest_giveback_breach")
        return TransitionResult("HARVEST", "PARTIAL_EXIT", "harvest_protect")

    if current_state == "RUNNER":
        if ctx.stall_score >= 0.65:
            return TransitionResult("HARVEST", "PARTIAL_EXIT", "runner_stall_back_to_harvest")
        if ctx.giveback_r >= 0.85:
            return TransitionResult("PANIC", "FULL_EXIT", "runner_safety_breach")
        return TransitionResult("RUNNER", "HOLD", "runner_continue")

    return TransitionResult("PANIC", "FULL_EXIT", "unknown_state_fallback")
