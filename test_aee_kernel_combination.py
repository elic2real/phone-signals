"""Tests for aee_kernel_combination — parallel kernel scoring and fusion."""
from __future__ import annotations

import pytest
from aee_state_machine_v2 import AEEContext
from aee_kernel_combination import (
    ACTIONS,
    detect_regime,
    score_kernel_progress,
    score_kernel_time,
    score_kernel_pnl,
    fuse_weighted_sum,
    fuse_gated,
    fuse_confidence_weighted,
    score_kernels_and_fuse,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ctx(**kwargs) -> AEEContext:
    defaults = dict(
        progress_r=0.0, unrealized_pips=0.0, giveback_r=0.0,
        continuation_score=0.5, stall_score=0.0, panic_trigger=False,
        open_pnl_r=0.0, locked_floor_r=0.0, giveback_from_peak_r=0.0,
        inefficiency_cost_r=0.0, continuation_proxy_r=0.0,
        t_norm=0.0, time_unproductive_ratio=0.0, productivity_rate=0.0,
    )
    defaults.update(kwargs)
    return AEEContext(**defaults)


ORIGIN = _ctx()


# ─────────────────────────────────────────────────────────────────────────────
# Regime detection
# ─────────────────────────────────────────────────────────────────────────────

def test_regime_trend():
    ctx = _ctx(continuation_score=0.8, progress_r=0.5)
    assert detect_regime(ctx) == "trend"


def test_regime_stall():
    ctx = _ctx(stall_score=0.7, time_unproductive_ratio=0.6, progress_r=0.1)
    assert detect_regime(ctx) == "stall"


def test_regime_reversal_panic():
    ctx = _ctx(panic_trigger=True)
    assert detect_regime(ctx) == "reversal"


def test_regime_reversal_giveback():
    ctx = _ctx(giveback_from_peak_r=0.5)
    assert detect_regime(ctx) == "reversal"


def test_regime_neutral():
    assert detect_regime(ORIGIN) == "neutral"


# ─────────────────────────────────────────────────────────────────────────────
# Pure kernel at-origin behavior
# ─────────────────────────────────────────────────────────────────────────────

def test_progress_kernel_origin_hold():
    """At origin, Progress kernel should favour HOLD over CLOSE."""
    s = score_kernel_progress(ORIGIN)
    assert all(a in s for a in (*ACTIONS, "confidence"))
    assert s["HOLD"] > s["CLOSE"], "P kernel should default to HOLD at origin"


def test_time_kernel_origin_extend():
    """At origin (t=0), Time kernel should favour EXTEND (early, no pressure)."""
    s = score_kernel_time(ORIGIN)
    assert s["EXTEND"] > s["CLOSE"], "T kernel should favour EXTEND at trade start"


def test_pnl_kernel_origin_hold():
    """At origin (no pnl, no floor), PnL kernel should favour HOLD."""
    s = score_kernel_pnl(ORIGIN)
    assert s["HOLD"] > s["CLOSE"], "PnL kernel should default to HOLD at origin"


def test_progress_kernel_strong_runner():
    """P kernel should say EXTEND for high continuation + positive progress."""
    ctx = _ctx(continuation_proxy_r=1.2, progress_r=0.9, giveback_from_peak_r=0.0)
    s = score_kernel_progress(ctx)
    assert s["EXTEND"] == max(s[a] for a in ACTIONS)


def test_progress_kernel_reversal_close():
    """P kernel should say CLOSE when progress reversed + large giveback."""
    ctx = _ctx(continuation_proxy_r=0.0, progress_r=-0.5, giveback_from_peak_r=0.8)
    s = score_kernel_progress(ctx)
    assert s["CLOSE"] == max(s[a] for a in ACTIONS)


def test_time_kernel_late_idle_close():
    """T kernel should say CLOSE when t_norm high + high unproductive ratio."""
    ctx = _ctx(t_norm=0.9, time_unproductive_ratio=0.9, inefficiency_cost_r=0.7, productivity_rate=0.0)
    s = score_kernel_time(ctx)
    assert s["CLOSE"] == max(s[a] for a in ACTIONS)


def test_pnl_kernel_floor_breach_close():
    """PnL kernel should say CLOSE when pnl has dropped below locked floor."""
    ctx = _ctx(locked_floor_r=0.8, open_pnl_r=0.4, giveback_from_peak_r=0.5)
    s = score_kernel_pnl(ctx)
    assert s["CLOSE"] == max(s[a] for a in ACTIONS)


def test_all_kernels_return_required_fields():
    for fn in (score_kernel_progress, score_kernel_time, score_kernel_pnl):
        s = fn(ORIGIN)
        for key in (*ACTIONS, "confidence"):
            assert key in s, f"Missing key {key} in {fn.__name__}"


# ─────────────────────────────────────────────────────────────────────────────
# Fusion functions
# ─────────────────────────────────────────────────────────────────────────────

def test_fuse_weighted_sum_equal_weights():
    ks = {
        "P":   {"CLOSE": 0.5, "HOLD": 1.0, "TIGHTEN": 0.3, "EXTEND": 0.8, "confidence": 0.6},
        "PnL": {"CLOSE": 1.0, "HOLD": 0.6, "TIGHTEN": 0.4, "EXTEND": 0.2, "confidence": 0.4},
    }
    fused, attr = fuse_weighted_sum(ks, {"P": 1.0, "PnL": 1.0})
    assert abs(fused["CLOSE"] - 0.75) < 1e-9
    assert abs(fused["HOLD"] - 0.80) < 1e-9
    assert abs(attr["P"] + attr["PnL"] - 1.0) < 1e-9


def test_fuse_weighted_sum_attribution_sums_to_one():
    ks = {k: {a: 0.5 for a in ACTIONS} | {"confidence": 0.5} for k in ("P", "T", "PnL")}
    _, attr = fuse_weighted_sum(ks, {"P": 1.0, "T": 2.0, "PnL": 1.0})
    assert abs(sum(attr.values()) - 1.0) < 1e-9


def test_fuse_gated_selects_T_in_stall():
    ctx = _ctx(stall_score=0.9, time_unproductive_ratio=0.8)
    ks = {
        "P": {a: 0.0 for a in ACTIONS} | {"confidence": 0.1},
        "T": {a: 0.9 for a in ACTIONS} | {"confidence": 0.9},
    }
    _, attr, regime = fuse_gated(ks, ctx)
    assert regime == "stall"
    assert attr["T"] == 1.0 and attr["P"] == 0.0


def test_fuse_gated_selects_PnL_in_reversal():
    ctx = _ctx(giveback_from_peak_r=0.6)
    ks = {
        "P": {a: 0.0 for a in ACTIONS} | {"confidence": 0.3},
        "PnL": {a: 1.0 for a in ACTIONS} | {"confidence": 0.7},
    }
    _, attr, regime = fuse_gated(ks, ctx)
    assert regime == "reversal"
    assert attr["PnL"] == 1.0


def test_fuse_confidence_weighted_high_conf_dominates():
    ks = {
        "P":   {a: 1.0 for a in ACTIONS} | {"confidence": 0.9},
        "T":   {a: 0.0 for a in ACTIONS} | {"confidence": 0.1},
    }
    fused, attr = fuse_confidence_weighted(ks)
    # P has 90% confidence, so fused values should be near P's values (1.0)
    assert fused["CLOSE"] > 0.8
    assert attr["P"] > attr["T"]


def test_fuse_confidence_weighted_zero_conf_equal():
    """When all confidences are zero, fallback to equal weighting."""
    ks = {
        "P":   {"CLOSE": 1.0, "HOLD": 0.0, "TIGHTEN": 0.0, "EXTEND": 0.0, "confidence": 0.0},
        "PnL": {"CLOSE": 0.0, "HOLD": 1.0, "TIGHTEN": 0.0, "EXTEND": 0.0, "confidence": 0.0},
    }
    fused, attr = fuse_confidence_weighted(ks)
    assert abs(fused["CLOSE"] - 0.5) < 1e-9
    assert abs(fused["HOLD"] - 0.5) < 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# Main entry: score_kernels_and_fuse
# ─────────────────────────────────────────────────────────────────────────────

def test_score_and_fuse_returns_required_keys():
    r = score_kernels_and_fuse(ORIGIN, ["P", "T", "PnL"], "weighted_sum")
    for key in ("action_values", "best_action", "confidence_gap", "attribution",
                "regime", "kernel_scores", "kernel_confidences"):
        assert key in r, f"Missing key {key}"


def test_score_and_fuse_best_action_is_argmax():
    ctx = _ctx(continuation_proxy_r=1.2, progress_r=0.9, giveback_from_peak_r=0.0, t_norm=0.1)
    r = score_kernels_and_fuse(ctx, ["P", "T"], "weighted_sum")
    av = r["action_values"]
    expected_best = max(av, key=av.get)
    assert r["best_action"] == expected_best


def test_panic_always_closes():
    ctx = _ctx(panic_trigger=True, progress_r=-0.9, t_norm=0.8,
               time_unproductive_ratio=0.8, continuation_proxy_r=0.0)
    for kernels, fusion in [
        (["P"], "weighted_sum"),
        (["T"], "weighted_sum"),
        (["PnL"], "weighted_sum"),
        (["P", "T", "PnL"], "weighted_sum"),
        (["P", "T", "PnL"], "gated"),
        (["P", "T", "PnL"], "confidence_weighted"),
    ]:
        r = score_kernels_and_fuse(ctx, kernels, fusion)
        assert r["best_action"] == "CLOSE", f"Expected CLOSE under panic for {kernels}/{fusion}"


def test_attribution_sums_to_one_all_modes():
    ctx = _ctx(continuation_proxy_r=0.5, t_norm=0.4, locked_floor_r=0.3, progress_r=0.4)
    for fusion in ("weighted_sum", "gated", "confidence_weighted"):
        r = score_kernels_and_fuse(ctx, ["P", "T", "PnL"], fusion)
        total = sum(r["attribution"].values())
        assert abs(total - 1.0) < 1e-9, f"Attribution doesn't sum to 1 for {fusion}: {r['attribution']}"


def test_pure_single_kernel_attribution_is_one():
    for kid in ("P", "T", "PnL"):
        r = score_kernels_and_fuse(ORIGIN, [kid], "weighted_sum")
        assert r["attribution"][kid] == 1.0


def test_unknown_kernel_raises():
    with pytest.raises(ValueError, match="Unknown kernel"):
        score_kernels_and_fuse(ORIGIN, ["X"], "weighted_sum")


def test_t_kernel_does_not_fire_on_productive_early_trade():
    """T kernel should not select CLOSE on a productive trade early in its path."""
    ctx = _ctx(t_norm=0.1, time_unproductive_ratio=0.05, productivity_rate=0.08,
               inefficiency_cost_r=0.02)
    r = score_kernels_and_fuse(ctx, ["T"], "weighted_sum")
    assert r["best_action"] != "CLOSE", "T kernel should not fire CLOSE early on productive trade"


def test_gated_triple_selects_correct_kernel_per_regime():
    """Gated triple: trend→P, stall→T, reversal→PnL."""
    # trend
    ctx_trend = _ctx(continuation_score=0.8, progress_r=0.6)
    r = score_kernels_and_fuse(ctx_trend, ["P", "T", "PnL"], "gated")
    assert r["attribution"]["P"] == 1.0, "Gated: trend should select P"

    # stall
    ctx_stall = _ctx(stall_score=0.7, time_unproductive_ratio=0.6)
    r = score_kernels_and_fuse(ctx_stall, ["P", "T", "PnL"], "gated")
    assert r["attribution"]["T"] == 1.0, "Gated: stall should select T"

    # reversal
    ctx_rev = _ctx(giveback_from_peak_r=0.5)
    r = score_kernels_and_fuse(ctx_rev, ["P", "T", "PnL"], "gated")
    assert r["attribution"]["PnL"] == 1.0, "Gated: reversal should select PnL"


def test_kernel_scores_in_output_exclude_confidence():
    r = score_kernels_and_fuse(ORIGIN, ["P", "T"], "weighted_sum")
    for kid, scores in r["kernel_scores"].items():
        assert "confidence" not in scores, f"kernel_scores should not contain 'confidence' key for {kid}"
        assert set(scores.keys()) == set(ACTIONS)
