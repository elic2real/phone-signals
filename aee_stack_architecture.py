from __future__ import annotations

from typing import Any

from aee_kernel_combination import score_kernel_degradation, score_kernel_floor, score_kernel_time
from aee_state_machine_v2 import AEEContext

ACTIONS = ("CLOSE", "HOLD", "TIGHTEN", "EXTEND")


def _best_action(scores: dict[str, float]) -> str:
    return max(ACTIONS, key=lambda a: scores[a])


def _degradation_trigger(ctx: AEEContext, cfg: dict[str, Any]) -> tuple[bool, bool]:
    mode = str(cfg.get("deg_trigger_mode", "D_giveback"))
    gb = ctx.giveback_from_peak_r
    cp = ctx.continuation_proxy_r
    upr = ctx.time_unproductive_ratio
    prod = ctx.productivity_rate
    ineff = ctx.inefficiency_cost_r

    gb_min = float(cfg.get("deg_gb_min", 0.46))
    cp_max = float(cfg.get("deg_cp_max", 0.12))
    upr_min = float(cfg.get("deg_upr_min", 0.45))
    prod_max = float(cfg.get("deg_prod_max", -0.01))
    ineff_min = float(cfg.get("deg_ineff_min", 0.55))

    if mode in {"gb_cp", "D_giveback"}:
        hard = gb >= gb_min and cp <= cp_max
    elif mode in {"upr_cp", "D_stall"}:
        hard = upr >= upr_min and cp <= cp_max
    elif mode in {"gb_prod", "D_decay"}:
        hard = gb >= gb_min and prod <= prod_max
    elif mode in {"failed_push", "D_failed_push"}:
        hard = gb >= gb_min and upr >= upr_min and prod <= prod_max
    else:  # dual_confirm / fallback
        hits = 0
        if gb >= gb_min:
            hits += 1
        if cp <= cp_max:
            hits += 1
        if upr >= upr_min:
            hits += 1
        if prod <= prod_max:
            hits += 1
        if ineff >= ineff_min:
            hits += 1
        hard = hits >= int(cfg.get("deg_confirm_count", 2))

    weak = (
        (gb >= max(0.0, gb_min * 0.7))
        or (cp <= cp_max + 0.1)
        or (upr >= max(0.0, upr_min * 0.8))
        or (prod <= max(0.0, prod_max * 0.5))
    )
    return bool(hard), bool(weak)


def _floor_trigger(ctx: AEEContext, cfg: dict[str, Any]) -> tuple[bool, bool]:
    mode = str(cfg.get("floor_trigger_mode", "breach_or_risk"))
    tolerance = float(cfg.get("floor_breach_tolerance_r", 0.05))
    risk_gb_min = float(cfg.get("floor_risk_gb_min", 0.30))

    breach = (
        ctx.locked_floor_r > 0.0
        and ctx.open_pnl_r < (ctx.locked_floor_r - tolerance)
    )
    risk = (
        ctx.locked_floor_r > 0.0
        and ctx.giveback_from_peak_r >= risk_gb_min
    )

    if mode == "breach_only":
        return bool(breach), bool(breach)
    if mode == "risk_only":
        return bool(risk), bool(breach)
    return bool(breach or risk), bool(breach)


def _apply_intervention(action: str, requested: str, mode: str) -> str:
    if mode == "force_close":
        return "CLOSE"
    if mode == "force_tighten":
        return "TIGHTEN"
    if mode == "downgrade_extend_hold":
        if action == "EXTEND":
            return "HOLD"
        return action
    if mode == "downgrade_hold_tighten":
        if action == "HOLD":
            return "TIGHTEN"
        if action == "EXTEND":
            return "HOLD"
        return action
    # kernel_suggest
    if requested == "CLOSE":
        return "CLOSE"
    if requested == "TIGHTEN":
        return "TIGHTEN"
    return action


def _apply_permission_scope(base_action: str, candidate_action: str, permission: str) -> str:
    """Constrain layer action by explicit permission model.

    Permissions:
      observe_only
      downgrade_only
      tighten_only
      close_allowed
      hard_override
    """
    p = str(permission or "downgrade_only")
    if p == "observe_only":
        return base_action
    if p == "hard_override":
        return candidate_action
    if p == "tighten_only":
        return "TIGHTEN" if candidate_action in {"TIGHTEN", "CLOSE"} else base_action
    if p == "close_allowed":
        if candidate_action == "CLOSE":
            return "CLOSE"
        if candidate_action == "TIGHTEN":
            return "TIGHTEN"
        return base_action

    # downgrade_only (default): never escalate risk, only step down aggressiveness.
    order = {"EXTEND": 3, "HOLD": 2, "TIGHTEN": 1, "CLOSE": 0}
    if order.get(candidate_action, 2) < order.get(base_action, 2):
        # in downgrade_only mode, CLOSE is mapped to TIGHTEN for safety.
        return "TIGHTEN" if candidate_action == "CLOSE" else candidate_action
    return base_action


def decide_stacked_architecture(ctx: AEEContext, stack: dict[str, Any]) -> dict[str, Any]:
    """Sequential stacked decision architecture.

    Base engine is always T. Searchable layers are D and F; panic is always-on hard interrupt.
    """
    cfg = dict(stack.get("config") or {})
    layer_order = list(stack.get("layer_order") or ["D", "F"])
    include = set(stack.get("layers") or ["T", "D", "F"])
    permissions = dict(stack.get("permissions") or {})

    t_scores = score_kernel_time(ctx)
    d_scores = score_kernel_degradation(ctx)
    f_scores = score_kernel_floor(ctx)

    base_action = _best_action(t_scores)
    action = base_action

    productive_runner = (
        ctx.continuation_proxy_r >= float(cfg.get("veto_cp_min", 0.55))
        and ctx.productivity_rate >= float(cfg.get("veto_prod_min", 0.002))
        and ctx.time_unproductive_ratio <= float(cfg.get("veto_upr_max", 0.28))
        and ctx.giveback_from_peak_r <= float(cfg.get("veto_giveback_max", 0.22))
        and ctx.open_pnl_r >= float(cfg.get("veto_pnl_min", 0.20))
    )
    veto_locked = productive_runner and base_action in {"HOLD", "EXTEND"}

    hard_deg, weak_deg = _degradation_trigger(ctx, cfg)
    floor_trigger, floor_breach = _floor_trigger(ctx, cfg)

    layer_trace: list[str] = ["T"]
    interventions: list[str] = []

    for layer in layer_order:
        if layer == "D" and "D" in include:
            if hard_deg or (weak_deg and bool(cfg.get("deg_allow_weak", False))):
                if veto_locked and not bool(cfg.get("deg_allow_under_veto", False)) and not hard_deg:
                    layer_trace.append("D_SKIP_VETO")
                else:
                    d_action = _best_action(d_scores)
                    mode = str(cfg.get("deg_action_mode", "downgrade_hold_tighten"))
                    prev = action
                    proposal = _apply_intervention(action, d_action, mode)
                    action = _apply_permission_scope(action, proposal, permissions.get("D", cfg.get("deg_permission", "downgrade_only")))
                    if action != prev:
                        interventions.append("D")
                    layer_trace.append("D")
        elif layer == "F" and "F" in include:
            if floor_trigger:
                mode = str(cfg.get("floor_action_mode", "tighten_or_close"))
                prev = action
                if floor_breach:
                    proposal = "CLOSE"
                elif mode == "tighten_or_close":
                    f_action = _best_action(f_scores)
                    proposal = "CLOSE" if f_action == "CLOSE" else "TIGHTEN"
                elif mode == "force_tighten":
                    proposal = "TIGHTEN"
                else:
                    proposal = "CLOSE"
                action = _apply_permission_scope(action, proposal, permissions.get("F", cfg.get("floor_permission", "close_allowed")))
                if action != prev:
                    interventions.append("F")
                layer_trace.append("F")

    # Panic hard interrupt: outside searchable stack ordering/composition.
    if ctx.panic_trigger:
        prev = action
        action = "CLOSE"
        if action != prev:
            interventions.append("PANIC")
        layer_trace.append("PANIC_INTERRUPT")

    # Global constraints.
    close_allowed = ctx.panic_trigger or floor_breach or hard_deg
    if action == "CLOSE" and not close_allowed:
        action = "HOLD" if veto_locked else "TIGHTEN"
        layer_trace.append("NO_CLOSE_CONSTRAINT")

    if veto_locked and not (ctx.panic_trigger or floor_breach or hard_deg):
        action = base_action
        if action != base_action:
            interventions.append("VETO")
        layer_trace.append("VETO_T")

    return {
        "action": action,
        "base_action": base_action,
        "layer_trace": layer_trace,
        "interventions": interventions,
        "permission_model": {
            "D": permissions.get("D", cfg.get("deg_permission", "downgrade_only")),
            "F": permissions.get("F", cfg.get("floor_permission", "close_allowed")),
            "panic": "hard_interrupt",
        },
        "flags": {
            "productive_runner": productive_runner,
            "veto_locked": veto_locked,
            "hard_degradation": hard_deg,
            "weak_degradation": weak_deg,
            "floor_trigger": floor_trigger,
            "floor_breach": floor_breach,
            "panic": bool(ctx.panic_trigger),
        },
    }
