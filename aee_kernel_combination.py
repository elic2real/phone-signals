#!/usr/bin/env python3
"""Parallel kernel scoring and fusion for AEE kernel combination sweep.

Three independent scoring kernels, each operating on a distinct signal domain:

  P   (Progress) : progress_r, continuation_proxy_r, giveback_from_peak_r, continuation_score
  T   (Time)     : t_norm, time_unproductive_ratio, productivity_rate, inefficiency_cost_r
  PnL (PnL)      : open_pnl_r, locked_floor_r, giveback_from_peak_r

Three fusion modes:
  weighted_sum        : V_final[a] = Σ w_i * V_kernel_i[a]
  gated               : select one kernel by regime (trend→P, stall→T, reversal→PnL)
  confidence_weighted : V_final[a] = Σ (conf_i / Σconf_j) * V_kernel_i[a]

Attribution contract: every result carries per-kernel weight at the final decision,
so every downstream report can say which kernel dominated and by how much.
"""
from __future__ import annotations

from typing import Any

from aee_state_machine_v2 import AEEContext

ACTIONS = ("CLOSE", "HOLD", "TIGHTEN", "EXTEND")


# ─────────────────────────────────────────────────────────────────────────────
# Regime detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_regime(ctx: AEEContext) -> str:
    """Classify the current market regime from context signals.

    Returns one of: "trend", "stall", "reversal", "neutral".
    """
    is_reversal = ctx.panic_trigger or ctx.giveback_from_peak_r >= 0.40
    is_stall = ctx.stall_score >= 0.50 or ctx.time_unproductive_ratio >= 0.40
    is_trend = ctx.continuation_score >= 0.60 and abs(ctx.progress_r) >= 0.25

    if is_reversal:
        return "reversal"
    if is_stall and not is_trend:
        return "stall"
    if is_trend:
        return "trend"
    return "neutral"


# ─────────────────────────────────────────────────────────────────────────────
# Pure kernel scoring functions
# Each returns: dict with CLOSE/HOLD/TIGHTEN/EXTEND (action values) + confidence.
# Conventions:
#   - Higher value = more preferred.
#   - At-origin (all signals zero) → HOLD should win (default: let trade breathe).
#   - confidence ∈ [0, 1.5] — strength/clarity of this kernel's signal.
# ─────────────────────────────────────────────────────────────────────────────

def score_kernel_progress(ctx: AEEContext) -> dict[str, float]:
    """P kernel: score from progress + continuation proxy + giveback.

    Semantics:
      EXTEND  — continuation proxy strong, giveback small, progress positive
      HOLD    — some continuation, moderate giveback  (default +0.2 bias)
      TIGHTEN — positive progress but giveback building
      CLOSE   — continuation gone, large giveback, or progress reversed
    """
    cp = ctx.continuation_proxy_r       # 0–2R, high = strong continuation
    gb = ctx.giveback_from_peak_r       # 0–inf, high = large drawdown from peak
    pr = ctx.progress_r                 # negative to +2R

    v_extend  =  cp + 0.50 * max(0.0, pr) - 0.70 * gb
    v_hold    =  0.20 + 0.70 * cp - 0.40 * gb
    v_tighten =  0.50 * max(0.0, pr) - 0.50 * gb
    v_close   = -0.80 * cp + 0.90 * gb + 0.50 * max(0.0, -pr)

    # Confidence: clear signal when either continuation or giveback is strong.
    confidence = min(1.5, max(cp, gb))

    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_time(ctx: AEEContext) -> dict[str, float]:
    """T kernel: score from time pressure, unproductiveness, and efficiency.

    Semantics:
      EXTEND  — early trade, positive productivity
      HOLD    — moderate time, some productivity
      TIGHTEN — time pressure building with declining productivity
      CLOSE   — late + unproductive (inefficiency cost high)
    """
    tn  = ctx.t_norm                    # 0–1, 0=start, 1=end
    upr = ctx.time_unproductive_ratio   # 0–1, high = mostly stalling
    pr  = ctx.productivity_rate         # progress per bar, tiny values
    ic  = ctx.inefficiency_cost_r       # weighted time unproductiveness

    time_pressure = tn * upr

    v_extend  = (1.0 - tn) * 0.80 - ic * 0.60
    v_hold    = (1.0 - tn) * 0.40 - upr * 0.30 + max(0.0, pr) * 2.0
    v_tighten = time_pressure * 0.50 - ic * 0.30
    v_close   = ic * 0.80 + time_pressure * 0.40

    # Confidence: clear signal at extremes (late+idle or early+productive).
    confidence = min(1.5, max(time_pressure, abs(pr) * 10.0))

    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_pnl(ctx: AEEContext) -> dict[str, float]:
    """PnL kernel: score from open P&L, locked floor, and giveback.

    Semantics:
      CLOSE   — floor breach (pnl below locked floor)
      TIGHTEN — locked floor exists with some giveback building
      HOLD    — positive pnl, no floor threat  (default +0.30 bias)
      EXTEND  — strong positive pnl, minimal giveback
    """
    pnl = ctx.open_pnl_r               # current PnL / target
    lf  = ctx.locked_floor_r           # locked floor / target, ≥ 0
    gb  = ctx.giveback_from_peak_r     # giveback / target

    floor_breach = max(0.0, lf - pnl)  # positive when pnl has gone below floor

    v_close   = floor_breach * 2.0 + lf * 0.50 * max(0.0, gb - 0.10)
    v_hold    = 0.30 + pnl * 0.60 - gb * 0.40
    v_tighten = lf * 1.00 + pnl * 0.30 - gb * 0.60
    v_extend  = 0.15 + pnl * 0.80 - gb * 0.50 - floor_breach * 1.50

    # Confidence: meaningful when we have a locked floor or clear pnl.
    confidence = min(1.5, lf + max(0.0, pnl) * 0.50)

    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_floor(ctx: AEEContext) -> dict[str, float]:
    """Floor/safety kernel: explicit safety-state scoring.

    Captures locked-floor economics distinct from open PnL:
      - floor level itself (safety built)
      - breach risk (pnl relative to floor)
      - leak pressure above floor (giveback while floor exists)
    """
    lf = ctx.locked_floor_r
    pnl = ctx.open_pnl_r
    gb = ctx.giveback_from_peak_r

    floor_buffer = pnl - lf
    breach_risk = max(0.0, lf - pnl)
    leak_pressure = max(0.0, gb - 0.10) * (1.0 + lf)

    v_close = breach_risk * 2.20 + leak_pressure * 0.90
    v_tighten = lf * 1.10 + leak_pressure * 0.60 - max(0.0, floor_buffer) * 0.15
    v_hold = 0.20 + max(0.0, floor_buffer) * 0.60 + lf * 0.30 - leak_pressure * 0.40
    v_extend = 0.10 + max(0.0, floor_buffer) * 0.50 - leak_pressure * 0.55 - breach_risk * 1.00

    confidence = min(1.5, max(lf, breach_risk + leak_pressure * 0.5))
    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_degradation(ctx: AEEContext) -> dict[str, float]:
    """Degradation kernel: continuation weakening and leak acceleration.

    Designed as non-T intervention family:
      CLOSE   when continuation decays + leak rises + productivity weakens
      TIGHTEN for medium degradation
    """
    cp = ctx.continuation_proxy_r
    gb = ctx.giveback_from_peak_r
    upr = ctx.time_unproductive_ratio
    prod = ctx.productivity_rate
    ineff = ctx.inefficiency_cost_r
    stall = ctx.stall_score

    continuation_weak = max(0.0, 0.50 - cp)
    leak = gb
    failed_push = max(0.0, -prod)
    stall_density = max(stall, upr)

    deg = 0.35 * continuation_weak + 0.30 * leak + 0.20 * failed_push + 0.15 * stall_density

    v_close = 1.40 * deg + 0.80 * ineff + 0.35 * leak
    v_tighten = 0.95 * deg + 0.45 * leak - 0.15 * max(0.0, cp - 0.4)
    v_hold = 0.20 + 0.35 * max(0.0, cp - 0.35) - 0.55 * deg
    v_extend = 0.10 + 0.45 * max(0.0, cp - 0.45) - 0.75 * deg

    confidence = min(1.5, max(deg, leak, continuation_weak))
    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_productivity(ctx: AEEContext) -> dict[str, float]:
    """Productivity kernel: economic output per unit time state.

    Distinguishes dead capital from productive continuation.
    """
    prod = ctx.productivity_rate
    upr = ctx.time_unproductive_ratio
    t = ctx.t_norm
    cp = ctx.continuation_proxy_r
    ineff = ctx.inefficiency_cost_r

    productive = max(0.0, prod)
    dead_capital = max(0.0, -prod) + upr * 0.6 + ineff * 0.5

    v_extend = 0.20 + 0.95 * productive + 0.35 * max(0.0, cp - 0.4) - 0.55 * dead_capital
    v_hold = 0.20 + 0.55 * productive + 0.20 * cp - 0.35 * dead_capital
    v_tighten = 0.45 * dead_capital + 0.15 * t - 0.20 * productive
    v_close = 0.85 * dead_capital + 0.40 * t - 0.25 * productive

    confidence = min(1.5, max(productive * 8.0, dead_capital))
    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


def score_kernel_regime(ctx: AEEContext) -> dict[str, float]:
    """Path-shape/regime kernel: smooth continuation vs chop/reversal onset."""
    regime = detect_regime(ctx)
    cp = ctx.continuation_proxy_r
    gb = ctx.giveback_from_peak_r

    if regime == "trend":
        v_extend = 0.90 + 0.60 * max(0.0, cp - 0.35)
        v_hold = 0.55 + 0.25 * cp
        v_tighten = 0.15
        v_close = -0.30 + 0.40 * gb
    elif regime == "stall":
        v_extend = -0.10
        v_hold = 0.25
        v_tighten = 0.70 + 0.35 * gb
        v_close = 0.45 + 0.45 * gb
    elif regime == "reversal":
        v_extend = -0.40
        v_hold = -0.10
        v_tighten = 0.70 + 0.45 * gb
        v_close = 0.95 + 0.70 * gb
    else:  # neutral
        v_extend = 0.20 + 0.25 * cp
        v_hold = 0.30
        v_tighten = 0.25 + 0.20 * gb
        v_close = 0.15 + 0.35 * gb

    confidence = min(1.5, 0.5 + abs(v_close - v_hold) * 0.5)
    return {"CLOSE": v_close, "HOLD": v_hold, "TIGHTEN": v_tighten, "EXTEND": v_extend,
            "confidence": confidence}


# ─────────────────────────────────────────────────────────────────────────────
# Fusion functions
# ─────────────────────────────────────────────────────────────────────────────

_KERNEL_FNS = {
    "P": score_kernel_progress,
    "T": score_kernel_time,
    "PnL": score_kernel_pnl,
    "F": score_kernel_floor,
    "D": score_kernel_degradation,
    "Pr": score_kernel_productivity,
    "R": score_kernel_regime,
}


def fuse_weighted_sum(
    kernel_scores: dict[str, dict[str, float]],
    weights: dict[str, float],
) -> tuple[dict[str, float], dict[str, float]]:
    """Weighted sum: V_final[a] = Σ (w_i / Σw) * V_kernel_i[a].

    Returns: (fused_action_values, normalized_attribution_weights)
    """
    total_w = sum(max(0.0, weights.get(k, 0.0)) for k in kernel_scores)
    if total_w <= 0.0:
        total_w = float(len(kernel_scores)) or 1.0

    fused: dict[str, float] = {a: 0.0 for a in ACTIONS}
    attribution: dict[str, float] = {}
    for kid, scores in kernel_scores.items():
        w = max(0.0, weights.get(kid, 1.0 / len(kernel_scores))) / total_w
        for a in ACTIONS:
            fused[a] += w * scores[a]
        attribution[kid] = w
    return fused, attribution


def fuse_gated(
    kernel_scores: dict[str, dict[str, float]],
    ctx: AEEContext,
) -> tuple[dict[str, float], dict[str, float], str]:
    """Gated: select exactly one kernel based on regime.

    Regime → preferred kernel:
      trend     → P
      stall     → T
      reversal  → PnL
      neutral   → P (default)

    Falls back to first available kernel if preferred is not in combo.
    Returns: (fused_values, attribution, regime_str)
    """
    regime = detect_regime(ctx)
    _regime_preference: dict[str, list[str]] = {
        "trend":    ["P", "T", "PnL"],
        "stall":    ["T", "P", "PnL"],
        "reversal": ["PnL", "P", "T"],
        "neutral":  ["P", "T", "PnL"],
    }
    preferred_order = _regime_preference.get(regime, ["P", "T", "PnL"])
    selected = next((k for k in preferred_order if k in kernel_scores), list(kernel_scores.keys())[0])

    fused = {a: kernel_scores[selected][a] for a in ACTIONS}
    attribution = {k: (1.0 if k == selected else 0.0) for k in kernel_scores}
    return fused, attribution, regime


def fuse_confidence_weighted(
    kernel_scores: dict[str, dict[str, float]],
) -> tuple[dict[str, float], dict[str, float]]:
    """Confidence-weighted: weight by each kernel's self-reported confidence.

    Falls back to equal weighting when all confidences are zero.
    """
    total_conf = sum(max(0.0, scores["confidence"]) for scores in kernel_scores.values())
    if total_conf <= 0.0:
        weights = {k: 1.0 / len(kernel_scores) for k in kernel_scores}
    else:
        weights = {k: max(0.0, scores["confidence"]) / total_conf for k, scores in kernel_scores.items()}
    return fuse_weighted_sum(kernel_scores, weights)


def fuse_tfirst_asymmetric(
    kernel_scores: dict[str, dict[str, float]],
    ctx: AEEContext,
    config: dict[str, float] | None = None,
) -> tuple[dict[str, float], dict[str, float], str, bool, dict[str, bool]]:
    """T-first asymmetric fusion with runner-preserving veto.

        Rules:
      1) T is anchor and default controller.
            2) Non-T kernels can only contribute when degradation is explicit.
      3) If T says HOLD/EXTEND in productive runner state, veto CLOSE from others
         unless hard degradation is true.
    """
    cfg = config or {}

    # Productive-runner veto gate.
    veto_cp_min = float(cfg.get("veto_cp_min", 0.55))
    veto_prod_min = float(cfg.get("veto_prod_min", 0.002))
    veto_upr_max = float(cfg.get("veto_upr_max", 0.28))
    veto_giveback_max = float(cfg.get("veto_giveback_max", 0.22))
    veto_pnl_min = float(cfg.get("veto_pnl_min", 0.20))

    # Hard degradation (can break veto and allow close-biased intervention).
    hard_giveback_min = float(cfg.get("hard_giveback_min", 0.42))
    hard_cp_max = float(cfg.get("hard_cp_max", 0.22))
    hard_prod_max = float(cfg.get("hard_prod_max", -0.001))
    hard_ineff_min = float(cfg.get("hard_ineff_min", 0.40))
    floor_breach_tolerance_r = float(cfg.get("floor_breach_tolerance_r", 0.05))

    # Weak degradation (allows gentle intervention).
    weak_cp_max = float(cfg.get("weak_cp_max", 0.42))
    weak_upr_min = float(cfg.get("weak_upr_min", 0.35))
    weak_prod_max = float(cfg.get("weak_prod_max", 0.0))
    weak_ineff_min = float(cfg.get("weak_ineff_min", 0.22))
    weak_stall_min = float(cfg.get("weak_stall_min", 0.45))

    # Intervention weights.
    weak_w_t = float(cfg.get("weak_w_t", 0.80))
    weak_w_intervention = float(cfg.get("weak_w_intervention", 0.20))
    hard_w_t = float(cfg.get("hard_w_t", 0.30))
    hard_w_intervention = float(cfg.get("hard_w_intervention", 0.70))

    # Back-compat: allow older split weight keys.
    if "weak_w_p" in cfg or "weak_w_q" in cfg:
        weak_w_intervention = max(0.0, float(cfg.get("weak_w_p", 0.0)) + float(cfg.get("weak_w_q", 0.0)))
    if "hard_w_p" in cfg or "hard_w_q" in cfg:
        hard_w_intervention = max(0.0, float(cfg.get("hard_w_p", 0.0)) + float(cfg.get("hard_w_q", 0.0)))

    # Resolve available kernels; T is mandatory for this fusion mode.
    if "T" not in kernel_scores:
        raise ValueError("fusion_mode='tfirst_asymmetric' requires 'T' kernel")
    t_scores = kernel_scores["T"]
    intervention_ids = [k for k in kernel_scores.keys() if k != "T"]

    def _intervention_scores() -> dict[str, float]:
        if not intervention_ids:
            return {a: 0.0 for a in ACTIONS}
        out = {a: 0.0 for a in ACTIONS}
        for kid in intervention_ids:
            for a in ACTIONS:
                out[a] += kernel_scores[kid][a]
        n = float(len(intervention_ids))
        for a in ACTIONS:
            out[a] /= n
        return out

    i_scores = _intervention_scores()

    t_best = max(ACTIONS, key=lambda a: t_scores[a])

    floor_breach = (
        ctx.locked_floor_r > 0.0
        and ctx.open_pnl_r < (ctx.locked_floor_r - floor_breach_tolerance_r)
    )
    hard_degradation = (
        ctx.panic_trigger
        or floor_breach
        or ctx.giveback_from_peak_r >= hard_giveback_min
        or (ctx.continuation_proxy_r <= hard_cp_max and ctx.productivity_rate <= hard_prod_max)
        or ctx.inefficiency_cost_r >= hard_ineff_min
    )
    weak_degradation = (
        ctx.continuation_proxy_r <= weak_cp_max
        or ctx.time_unproductive_ratio >= weak_upr_min
        or ctx.productivity_rate <= weak_prod_max
        or ctx.inefficiency_cost_r >= weak_ineff_min
        or ctx.stall_score >= weak_stall_min
    )
    productive_runner = (
        ctx.continuation_proxy_r >= veto_cp_min
        and ctx.productivity_rate >= veto_prod_min
        and ctx.time_unproductive_ratio <= veto_upr_max
        and ctx.giveback_from_peak_r <= veto_giveback_max
        and ctx.open_pnl_r >= veto_pnl_min
    )

    degradation_flags = {
        "hard": bool(hard_degradation),
        "weak": bool(weak_degradation),
        "productive_runner": bool(productive_runner),
        "floor_breach": bool(floor_breach),
    }

    # 1) Veto path: T owns productive runners unless hard degradation.
    if productive_runner and (t_best in {"HOLD", "EXTEND"}) and not hard_degradation:
        fused = {a: t_scores[a] for a in ACTIONS}
        attribution = {k: (1.0 if k == "T" else 0.0) for k in kernel_scores}
        return fused, attribution, "trend", True, degradation_flags

    # 2) Hard degradation: allow close-biased intervention (PnL heavier).
    if hard_degradation:
        w_t = max(0.0, hard_w_t)
        w_i = max(0.0, hard_w_intervention) if intervention_ids else 0.0
        fused = {a: w_t * t_scores[a] + w_i * i_scores[a] for a in ACTIONS}
        total = (w_t + w_i) or 1.0
        attribution = {"T": w_t / total}
        for kid in intervention_ids:
            attribution[kid] = (w_i / total) / max(1.0, float(len(intervention_ids)))
        return fused, attribution, "reversal", False, degradation_flags

    # 3) Weak degradation: gentle intervention while keeping T dominant.
    if weak_degradation:
        w_t = max(0.0, weak_w_t)
        w_i = max(0.0, weak_w_intervention) if intervention_ids else 0.0
        fused = {a: w_t * t_scores[a] + w_i * i_scores[a] for a in ACTIONS}
        total = (w_t + w_i) or 1.0
        attribution = {"T": w_t / total}
        for kid in intervention_ids:
            attribution[kid] = (w_i / total) / max(1.0, float(len(intervention_ids)))
        return fused, attribution, "stall", False, degradation_flags

    # 4) Default: pure T control.
    fused = {a: t_scores[a] for a in ACTIONS}
    attribution = {k: (1.0 if k == "T" else 0.0) for k in kernel_scores}
    return fused, attribution, "neutral", False, degradation_flags


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def score_kernels_and_fuse(
    ctx: AEEContext,
    kernel_ids: list[str],
    fusion_mode: str,
    weights: dict[str, float] | None = None,
    fusion_config: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute parallel kernel scores and fuse into a single action decision.

    Parameters
    ----------
    ctx          : AEEContext built from the current replay bar.
    kernel_ids   : List of kernel identifiers, e.g. ["P", "T", "PnL"].
    fusion_mode  : One of "weighted_sum", "gated", "confidence_weighted", "tfirst_asymmetric".
    weights      : Weight per kernel for weighted_sum mode. Defaults to equal.
    fusion_config: Thresholds/weights for fusion-specific behavior.

    Returns
    -------
    dict with:
      action_values     : {CLOSE, HOLD, TIGHTEN, EXTEND} — fused scores
      best_action       : argmax of action_values (panic always → CLOSE)
      confidence_gap    : best_score - second_score
      attribution       : {kernel_id: weight_at_this_step}
      regime            : detected regime string
      kernel_scores     : raw per-kernel action values (no confidence key)
      kernel_confidences: {kernel_id: confidence_score}
    """
    kernel_scores: dict[str, dict[str, float]] = {}
    for kid in kernel_ids:
        fn = _KERNEL_FNS.get(kid)
        if fn is None:
            raise ValueError(f"Unknown kernel: {kid!r}. Valid: {list(_KERNEL_FNS)}")
        kernel_scores[kid] = fn(ctx)

    regime = detect_regime(ctx)

    veto_applied = False
    degradation_flags: dict[str, bool] | None = None
    if fusion_mode == "gated":
        fused, attribution, regime = fuse_gated(kernel_scores, ctx)
    elif fusion_mode == "confidence_weighted":
        fused, attribution = fuse_confidence_weighted(kernel_scores)
    elif fusion_mode == "tfirst_asymmetric":
        fused, attribution, regime, veto_applied, degradation_flags = fuse_tfirst_asymmetric(
            kernel_scores,
            ctx,
            fusion_config,
        )
    else:  # weighted_sum (default)
        w = weights if weights else {k: 1.0 for k in kernel_ids}
        fused, attribution = fuse_weighted_sum(kernel_scores, w)

    # Panic override: always CLOSE immediately.
    if ctx.panic_trigger:
        fused["CLOSE"] = 999.0

    sorted_values = sorted(fused.values(), reverse=True)
    best_action = max(fused, key=lambda a: fused[a])
    confidence_gap = sorted_values[0] - sorted_values[1] if len(sorted_values) > 1 else 0.0

    return {
        "action_values": fused,
        "best_action": best_action,
        "confidence_gap": confidence_gap,
        "attribution": attribution,
        "regime": regime,
        "veto_applied": veto_applied,
        "degradation_flags": degradation_flags,
        # Strip confidence key from per-kernel scores for clean output.
        "kernel_scores": {k: {a: s[a] for a in ACTIONS} for k, s in kernel_scores.items()},
        "kernel_confidences": {k: s.get("confidence", 0.0) for k, s in kernel_scores.items()},
    }
