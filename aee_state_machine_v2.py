from dataclasses import dataclass
from typing import Any
from typing import Literal

from aee_trade_state_packet import build_trade_state_packet

AEEState = Literal["PROTECT", "BUILD", "HARVEST", "RUNNER", "PANIC", "CLOSED"]
AEEAction = Literal["HOLD", "TIGHTEN", "PARTIAL_EXIT", "FULL_EXIT"]
ObjectiveState = Literal["MAXIMIZE_CONTINUATION", "MAXIMIZE_FLOOR", "RELEASE_CAPITAL"]


@dataclass
class AEEContext:
    progress_r: float
    unrealized_pips: float
    giveback_r: float
    continuation_score: float
    stall_score: float
    panic_trigger: bool
    open_pnl_r: float = 0.0
    locked_floor_r: float = 0.0
    giveback_from_peak_r: float = 0.0
    inefficiency_cost_r: float = 0.0
    continuation_proxy_r: float = 0.0
    t_norm: float = 0.0
    time_unproductive_ratio: float = 0.0
    time_since_last_progress: float = 0.0
    productivity_rate: float = 0.0
    objective_state: ObjectiveState = "MAXIMIZE_CONTINUATION"
    objective_dwell_bars: int = 0
    objective_confirm_count: int = 0
    objective_pending_target: str = ""
    action_dwell_bars: int = 0
    last_action: str = "HOLD"


@dataclass
class TransitionResult:
    next_state: AEEState
    action: AEEAction
    reason: str
    objective_before: ObjectiveState = "MAXIMIZE_CONTINUATION"
    objective_after: ObjectiveState = "MAXIMIZE_CONTINUATION"
    objective_transition_reason: str = "objective_hold"
    objective_dwell_bars: int = 0
    objective_confirm_count: int = 0
    objective_pending_target: str = ""
    selected_internal_action: str = "HOLD"
    action_dwell_bars: int = 0
    action_values: dict[str, float] | None = None
    masked_actions: list[str] | None = None
    confidence_gap: float = 0.0


def _policy_value(policy: dict[str, float] | None, key: str, default: float) -> float:
    if not policy:
        return default
    try:
        return float(policy.get(key, default))
    except Exception:
        return default


def _select_objective_v1(ctx: AEEContext, policy: dict[str, float] | None = None) -> tuple[ObjectiveState, int, int, str, str]:
    objective_min_dwell = int(_policy_value(policy, "objective_min_dwell", 2.0))
    objective_confirm_bars = int(_policy_value(policy, "objective_confirm_bars", 2.0))
    floor_giveback_trigger_r = _policy_value(policy, "floor_giveback_trigger_r", 0.30)
    release_giveback_trigger_r = _policy_value(policy, "release_giveback_trigger_r", 0.85)
    release_continuation_max_r = _policy_value(policy, "release_continuation_max_r", 0.25)
    continuation_proxy_enter_r = _policy_value(policy, "continuation_proxy_enter_r", 0.55)
    floor_productivity_min = _policy_value(policy, "floor_productivity_min", -0.15)
    release_inefficiency_min_r = _policy_value(policy, "release_inefficiency_min_r", 0.45)

    current = ctx.objective_state
    candidate = current
    signal_reason = "signal_none"
    # High-urgency: panic or large-drawdown override any dwell lock.
    is_urgent = ctx.panic_trigger or ctx.giveback_from_peak_r >= release_giveback_trigger_r

    if ctx.panic_trigger or ctx.giveback_from_peak_r >= release_giveback_trigger_r:
        candidate = "RELEASE_CAPITAL"
        signal_reason = "signal_release_risk"
    elif (
        ctx.locked_floor_r > 0.0
        and ctx.continuation_proxy_r <= release_continuation_max_r
        and ctx.inefficiency_cost_r >= release_inefficiency_min_r
    ):
        candidate = "RELEASE_CAPITAL"
        signal_reason = "signal_release_inefficient"
    elif ctx.giveback_from_peak_r >= floor_giveback_trigger_r or ctx.productivity_rate <= floor_productivity_min:
        candidate = "MAXIMIZE_FLOOR"
        signal_reason = "signal_floor_defense"
    elif ctx.continuation_proxy_r >= continuation_proxy_enter_r and ctx.open_pnl_r >= -0.10:
        candidate = "MAXIMIZE_CONTINUATION"
        signal_reason = "signal_continue_strength"

    if candidate == current:
        return current, ctx.objective_dwell_bars + 1, 0, "", "objective_hold"

    # Urgent transitions (panic / large giveback) bypass dwell lock and confirmation.
    if is_urgent and candidate == "RELEASE_CAPITAL":
        return candidate, 1, 0, "", f"objective_urgent_transition:{signal_reason}"

    if ctx.objective_dwell_bars < max(1, objective_min_dwell):
        return current, ctx.objective_dwell_bars + 1, 0, "", "objective_dwell_lock"

    pending = candidate
    if ctx.objective_pending_target != pending:
        confirm = 1
    else:
        confirm = ctx.objective_confirm_count + 1

    if confirm < max(1, objective_confirm_bars):
        return current, ctx.objective_dwell_bars + 1, confirm, pending, "objective_wait_confirm"

    return candidate, 1, 0, "", f"objective_transition:{signal_reason}"


def _compute_action_values_v1(ctx: AEEContext, objective: ObjectiveState, policy: dict[str, float] | None = None) -> tuple[str, AEEAction, dict[str, float], list[str], float, int]:
    min_action_dwell = int(_policy_value(policy, "min_action_dwell", 2.0))
    switch_confidence_gap = _policy_value(policy, "action_switch_confidence_gap", 0.20)
    release_close_bonus = _policy_value(policy, "release_close_bonus_r", 0.25)
    floor_tighten_bonus = _policy_value(policy, "floor_tighten_bonus_r", 0.20)
    continuation_extend_bonus = _policy_value(policy, "continuation_extend_bonus_r", 0.15)

    # Confidence hierarchy enforced mechanically in all values:
    # locked floor > open pnl > continuation proxy > inefficiency cost.
    value_close_now = 1.40 * ctx.locked_floor_r + 0.90 * max(0.0, ctx.open_pnl_r) - 0.20 * ctx.inefficiency_cost_r
    value_hold_now = 1.20 * ctx.locked_floor_r + 1.00 * ctx.open_pnl_r + 0.70 * ctx.continuation_proxy_r - 0.55 * ctx.inefficiency_cost_r
    value_tighten_now = 1.35 * ctx.locked_floor_r + 0.85 * ctx.open_pnl_r + 0.35 * ctx.continuation_proxy_r - 0.25 * ctx.giveback_from_peak_r - 0.50 * ctx.inefficiency_cost_r
    value_extend_now = 1.10 * ctx.locked_floor_r + 0.95 * ctx.open_pnl_r + 0.90 * ctx.continuation_proxy_r - 0.65 * ctx.inefficiency_cost_r

    values = {
        "value_close_now": value_close_now,
        "value_hold_now": value_hold_now,
        "value_tighten_now": value_tighten_now,
        "value_extend_now": value_extend_now,
    }
    masked_actions: list[str] = []

    # Objective-aware hard filtering via explicit masks and bonuses/penalties.
    # MAXIMIZE_CONTINUATION: keep trade alive for further gains.
    #   CLOSE is only masked when continuation signal is strong AND giveback is small
    #   (< ceil_for_masking_r). Once giveback reaches 0.35R, CLOSE is always unmasked.
    close_mask_giveback_ceil = _policy_value(policy, "close_mask_giveback_ceil_r", 0.35)
    if objective == "MAXIMIZE_CONTINUATION":
        values["value_close_now"] -= 0.80
        values["value_extend_now"] += continuation_extend_bonus
        if ctx.continuation_proxy_r > 0.35 and ctx.giveback_from_peak_r < close_mask_giveback_ceil:
            values["value_close_now"] = -999.0
            masked_actions.append("CLOSE")
    elif objective == "MAXIMIZE_FLOOR":
        values["value_tighten_now"] += floor_tighten_bonus
        values["value_extend_now"] -= 0.50
        if ctx.giveback_from_peak_r >= 0.25:
            values["value_extend_now"] = -999.0
            masked_actions.append("EXTEND")
    elif objective == "RELEASE_CAPITAL":
        values["value_close_now"] += release_close_bonus
        values["value_hold_now"] -= 0.40
        values["value_extend_now"] = -999.0
        masked_actions.append("EXTEND")
        if ctx.locked_floor_r > 0.0 or ctx.inefficiency_cost_r >= 0.35:
            values["value_hold_now"] = -999.0
            masked_actions.append("HOLD")

    action_map = {
        "CLOSE": "value_close_now",
        "HOLD": "value_hold_now",
        "TIGHTEN": "value_tighten_now",
        "EXTEND": "value_extend_now",
    }

    # ── Floor breach hard rule ────────────────────────────────────────────────
    # When current pnl has fallen below the locked floor (within tolerance), CLOSE
    # must beat every other action regardless of objective masking.  This protects
    # locked gains from being given back below the 1:1 target level.
    floor_breach_tolerance = _policy_value(policy, "floor_breach_tolerance_r", 0.05)
    is_floor_breach = ctx.locked_floor_r > 0.0 and ctx.open_pnl_r < ctx.locked_floor_r - floor_breach_tolerance
    if is_floor_breach:
        values["value_close_now"] = max(values["value_close_now"], 5.0)
        if "CLOSE" in masked_actions:
            masked_actions.remove("CLOSE")

    ranked = sorted(action_map.items(), key=lambda kv: values[kv[1]], reverse=True)
    best_action = ranked[0][0]
    second_score = values[ranked[1][1]] if len(ranked) > 1 else -999.0
    best_score = values[action_map[best_action]]
    confidence_gap = best_score - second_score

    selected = best_action
    action_dwell = ctx.action_dwell_bars + 1
    if ctx.last_action and selected != ctx.last_action:
        if ctx.action_dwell_bars < max(1, min_action_dwell):
            selected = ctx.last_action
            action_dwell = ctx.action_dwell_bars + 1
        elif confidence_gap < switch_confidence_gap:
            selected = ctx.last_action
            action_dwell = ctx.action_dwell_bars + 1
        else:
            action_dwell = 1

    if selected == "CLOSE":
        aee_action: AEEAction = "FULL_EXIT"
    elif selected == "TIGHTEN":
        aee_action = "TIGHTEN"
    elif selected == "EXTEND":
        aee_action = "HOLD"
    else:
        aee_action = "HOLD"

    return selected, aee_action, values, masked_actions, confidence_gap, action_dwell


def _next_state_from_objective_v1(objective: ObjectiveState, internal_action: str, ctx: AEEContext) -> tuple[AEEState, str]:
    if ctx.panic_trigger:
        return "PANIC", "panic_trigger"
    if internal_action == "CLOSE":
        return "PANIC", "objective_close_now"
    if objective == "RELEASE_CAPITAL":
        if internal_action == "TIGHTEN":
            return "HARVEST", "release_tighten"
        return "HARVEST", "release_hold"
    if objective == "MAXIMIZE_FLOOR":
        if internal_action == "TIGHTEN":
            return "HARVEST", "floor_tighten"
        return "BUILD", "floor_hold"
    if internal_action == "EXTEND":
        return "RUNNER", "continuation_extend"
    return "RUNNER", "continuation_hold"


def _transition_aee_state_v1(current_state: AEEState, ctx: AEEContext, policy: dict[str, float] | None = None) -> TransitionResult:
    objective_after, objective_dwell, objective_confirm, objective_pending, objective_reason = _select_objective_v1(ctx, policy)
    internal_action, action, action_values, masked_actions, confidence_gap, action_dwell = _compute_action_values_v1(
        ctx,
        objective_after,
        policy,
    )

    next_state, state_reason = _next_state_from_objective_v1(objective_after, internal_action, ctx)
    reason = f"v1_{state_reason}"
    if objective_reason != "objective_hold":
        reason = f"{reason}|{objective_reason}"

    if current_state == "CLOSED":
        return TransitionResult(
            "CLOSED",
            "HOLD",
            "already_closed",
            objective_before=ctx.objective_state,
            objective_after=ctx.objective_state,
            objective_transition_reason="objective_hold",
            objective_dwell_bars=ctx.objective_dwell_bars,
            objective_confirm_count=0,
            objective_pending_target="",
            selected_internal_action="HOLD",
            action_dwell_bars=ctx.action_dwell_bars,
            action_values=action_values,
            masked_actions=masked_actions,
            confidence_gap=0.0,
        )

    return TransitionResult(
        next_state,
        action,
        reason,
        objective_before=ctx.objective_state,
        objective_after=objective_after,
        objective_transition_reason=objective_reason,
        objective_dwell_bars=objective_dwell,
        objective_confirm_count=objective_confirm,
        objective_pending_target=objective_pending,
        selected_internal_action=internal_action,
        action_dwell_bars=action_dwell,
        action_values=action_values,
        masked_actions=masked_actions,
        confidence_gap=confidence_gap,
    )


def transition_aee_state(current_state: AEEState, ctx: AEEContext, *, policy: dict[str, float] | None = None) -> TransitionResult:
    if _policy_value(policy, "enable_objective_v1", 0.0) >= 0.5:
        return _transition_aee_state_v1(current_state, ctx, policy)

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
    packet_meta = dict(meta or {})
    packet_meta.update(
        {
            "objective_state_before": transition.objective_before,
            "objective_state_after": transition.objective_after,
            "objective_transition_reason": transition.objective_transition_reason,
            "objective_dwell_bars": transition.objective_dwell_bars,
            "objective_confirm_count": transition.objective_confirm_count,
            "objective_pending_target": transition.objective_pending_target,
            "selected_internal_action": transition.selected_internal_action,
            "action_dwell_bars": transition.action_dwell_bars,
            "masked_actions": list(transition.masked_actions or []),
            "confidence_gap": float(transition.confidence_gap),
            "action_values": dict(transition.action_values or {}),
        }
    )
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
        open_pnl_r=ctx.open_pnl_r,
        locked_floor_r=ctx.locked_floor_r,
        giveback_from_peak_r=ctx.giveback_from_peak_r,
        inefficiency_cost_r=ctx.inefficiency_cost_r,
        continuation_proxy_r=ctx.continuation_proxy_r,
        t_norm=ctx.t_norm,
        time_unproductive_ratio=ctx.time_unproductive_ratio,
        time_since_last_progress=ctx.time_since_last_progress,
        productivity_rate=ctx.productivity_rate,
        timestamp=timestamp,
        meta=packet_meta,
    )
