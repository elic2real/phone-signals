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


# ─────────────────────────────────────────────────────────────────────────────
# Fusion functions
# ─────────────────────────────────────────────────────────────────────────────

_KERNEL_FNS = {
    "P": score_kernel_progress,
    "T": score_kernel_time,
    "PnL": score_kernel_pnl,
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


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def score_kernels_and_fuse(
    ctx: AEEContext,
    kernel_ids: list[str],
    fusion_mode: str,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compute parallel kernel scores and fuse into a single action decision.

    Parameters
    ----------
    ctx          : AEEContext built from the current replay bar.
    kernel_ids   : List of kernel identifiers, e.g. ["P", "T", "PnL"].
    fusion_mode  : One of "weighted_sum", "gated", "confidence_weighted".
    weights      : Weight per kernel for weighted_sum mode. Defaults to equal.

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

    if fusion_mode == "gated":
        fused, attribution, regime = fuse_gated(kernel_scores, ctx)
    elif fusion_mode == "confidence_weighted":
        fused, attribution = fuse_confidence_weighted(kernel_scores)
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
        # Strip confidence key from per-kernel scores for clean output.
        "kernel_scores": {k: {a: s[a] for a in ACTIONS} for k, s in kernel_scores.items()},
        "kernel_confidences": {k: s.get("confidence", 0.0) for k, s in kernel_scores.items()},
    }
