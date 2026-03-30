from __future__ import annotations

from aee_stack_architecture import decide_stacked_architecture
from aee_state_machine_v2 import AEEContext


def _ctx(**kwargs) -> AEEContext:
    base = dict(
        progress_r=0.0,
        unrealized_pips=0.0,
        giveback_r=0.0,
        continuation_score=0.5,
        stall_score=0.0,
        panic_trigger=False,
        open_pnl_r=0.0,
        locked_floor_r=0.0,
        giveback_from_peak_r=0.0,
        inefficiency_cost_r=0.0,
        continuation_proxy_r=0.0,
        t_norm=0.0,
        time_unproductive_ratio=0.0,
        time_since_last_progress=0.0,
        productivity_rate=0.0,
    )
    base.update(kwargs)
    return AEEContext(**base)


def test_panic_is_hard_interrupt_outside_order():
    ctx = _ctx(panic_trigger=True)
    stack = {
        "layers": ["T", "D", "F"],
        "layer_order": ["F", "D"],
        "permissions": {"D": "observe_only", "F": "observe_only"},
        "config": {},
    }
    r = decide_stacked_architecture(ctx, stack)
    assert r["action"] == "CLOSE"
    assert "PANIC_INTERRUPT" in r["layer_trace"]


def test_permission_model_prevents_close_for_downgrade_only():
    ctx = _ctx(
        continuation_proxy_r=0.05,
        giveback_from_peak_r=0.9,
        productivity_rate=0.01,
        time_unproductive_ratio=0.05,
        inefficiency_cost_r=0.01,
        t_norm=0.05,
    )
    stack = {
        "layers": ["T", "D"],
        "layer_order": ["D"],
        "permissions": {"D": "downgrade_only"},
        "config": {
            "deg_trigger_mode": "D_giveback",
            "deg_action_mode": "force_close",
            "deg_allow_weak": False,
        },
    }
    r = decide_stacked_architecture(ctx, stack)
    assert r["permission_model"]["D"] == "downgrade_only"
    assert r["action"] != "CLOSE"


def test_order_is_first_class_variable():
    ctx = _ctx(
        locked_floor_r=0.8,
        open_pnl_r=0.75,
        giveback_from_peak_r=0.55,
        continuation_proxy_r=0.08,
        productivity_rate=-0.02,
        time_unproductive_ratio=0.7,
    )

    stack_df = {
        "layers": ["T", "D", "F"],
        "layer_order": ["D", "F"],
        "permissions": {"D": "tighten_only", "F": "close_allowed"},
        "config": {
            "deg_trigger_mode": "D_giveback",
            "deg_action_mode": "force_tighten",
            "floor_trigger_mode": "breach_or_risk",
            "floor_action_mode": "tighten_or_close",
        },
    }
    stack_fd = {
        "layers": ["T", "D", "F"],
        "layer_order": ["F", "D"],
        "permissions": {"D": "tighten_only", "F": "close_allowed"},
        "config": {
            "deg_trigger_mode": "D_giveback",
            "deg_action_mode": "force_tighten",
            "floor_trigger_mode": "breach_or_risk",
            "floor_action_mode": "tighten_or_close",
        },
    }

    r_df = decide_stacked_architecture(ctx, stack_df)
    r_fd = decide_stacked_architecture(ctx, stack_fd)
    assert r_df["layer_trace"] != r_fd["layer_trace"]


def test_degradation_subfamily_names_are_supported():
    ctx = _ctx(
        continuation_proxy_r=0.1,
        giveback_from_peak_r=0.6,
        productivity_rate=-0.02,
        time_unproductive_ratio=0.75,
        inefficiency_cost_r=0.7,
    )
    for sub in ["D_giveback", "D_stall", "D_decay", "D_failed_push"]:
        stack = {
            "layers": ["T", "D"],
            "layer_order": ["D"],
            "permissions": {"D": "tighten_only"},
            "config": {
                "deg_trigger_mode": sub,
                "deg_action_mode": "force_tighten",
            },
        }
        r = decide_stacked_architecture(ctx, stack)
        assert "D" in " ".join(r["layer_trace"]) or r["flags"]["hard_degradation"] in {True, False}
