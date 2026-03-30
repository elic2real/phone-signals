from dataclasses import dataclass
from typing import Any
from typing import Literal

from aee_trade_state_packet import build_trade_state_packet

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


def _policy_value(policy: dict[str, float] | None, key: str, default: float) -> float:
    if not policy:
        return default
    try:
        return float(policy.get(key, default))
    except Exception:
        return default


def transition_aee_state(current_state: AEEState, ctx: AEEContext, *, policy: dict[str, float] | None = None) -> TransitionResult:
    protect_progress_r = _policy_value(policy, "protect_progress_r", 0.25)
    protect_continuation_score = _policy_value(policy, "protect_continuation_score", 0.45)
    build_to_harvest_unrealized_pips = _policy_value(policy, "build_to_harvest_unrealized_pips", 4.0)
    build_to_harvest_progress_r = _policy_value(policy, "build_to_harvest_progress_r", 0.60)
    build_safety_giveback_r = _policy_value(policy, "build_safety_giveback_r", 0.90)
    harvest_to_runner_continuation_score = _policy_value(policy, "harvest_to_runner_continuation_score", 0.75)
    harvest_to_runner_max_stall_score = _policy_value(policy, "harvest_to_runner_max_stall_score", 0.30)
    harvest_giveback_r = _policy_value(policy, "harvest_giveback_r", 0.70)
    runner_stall_score = _policy_value(policy, "runner_stall_score", 0.65)
    runner_safety_giveback_r = _policy_value(policy, "runner_safety_giveback_r", 0.85)

    if current_state == "CLOSED":
        return TransitionResult("CLOSED", "HOLD", "already_closed")

    if current_state == "PANIC":
        return TransitionResult("CLOSED", "FULL_EXIT", "panic_forced_exit")

    if ctx.panic_trigger:
        return TransitionResult("PANIC", "FULL_EXIT", "panic_trigger")

    if current_state == "PROTECT":
        if ctx.progress_r >= protect_progress_r and ctx.continuation_score >= protect_continuation_score:
            return TransitionResult("BUILD", "HOLD", "protect_to_build")
        return TransitionResult("PROTECT", "TIGHTEN", "protect_risk_control")

    if current_state == "BUILD":
        if ctx.unrealized_pips >= build_to_harvest_unrealized_pips and ctx.progress_r >= build_to_harvest_progress_r:
            return TransitionResult("HARVEST", "PARTIAL_EXIT", "build_to_harvest")
        if ctx.giveback_r >= build_safety_giveback_r:
            return TransitionResult("PANIC", "FULL_EXIT", "build_safety_breach")
        return TransitionResult("BUILD", "HOLD", "build_continue")

    if current_state == "HARVEST":
        if ctx.continuation_score >= harvest_to_runner_continuation_score and ctx.stall_score <= harvest_to_runner_max_stall_score:
            return TransitionResult("RUNNER", "HOLD", "harvest_to_runner")
        if ctx.giveback_r >= harvest_giveback_r:
            return TransitionResult("PANIC", "FULL_EXIT", "harvest_giveback_breach")
        return TransitionResult("HARVEST", "PARTIAL_EXIT", "harvest_protect")

    if current_state == "RUNNER":
        if ctx.stall_score >= runner_stall_score:
            return TransitionResult("HARVEST", "PARTIAL_EXIT", "runner_stall_back_to_harvest")
        if ctx.giveback_r >= runner_safety_giveback_r:
            return TransitionResult("PANIC", "FULL_EXIT", "runner_safety_breach")
        return TransitionResult("RUNNER", "HOLD", "runner_continue")

    return TransitionResult("PANIC", "FULL_EXIT", "unknown_state_fallback")


def transition_aee_state_with_packet(
    current_state: AEEState,
    ctx: AEEContext,
    *,
    trade_id: str,
    bar_index: int,
    timestamp: str | None = None,
    meta: dict[str, Any] | None = None,
    policy: dict[str, float] | None = None,
) -> dict[str, Any]:
    transition = transition_aee_state(current_state, ctx, policy=policy)
    return build_trade_state_packet(
        trade_id=trade_id,
        bar_index=bar_index,
        state_before=current_state,
        state_after=transition.next_state,
        action=transition.action,
        reason_code=transition.reason,
        progress_r=ctx.progress_r,
        unrealized_pips=ctx.unrealized_pips,
        giveback_r=ctx.giveback_r,
        continuation_score=ctx.continuation_score,
        stall_score=ctx.stall_score,
        panic_trigger=ctx.panic_trigger,
        timestamp=timestamp,
        meta=meta,
    )
