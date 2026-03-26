from __future__ import annotations

from typing import Any


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def _family_profile(family: str) -> dict[str, float]:
    """Family-specific tolerances for v3 state logic.

    Values are expressed in pips or minutes and are intentionally coarse to
    implement behavior classes from TASK-010 without threshold tuning sweeps.
    """
    if family == "EXPANSION_BREAKOUT":
        return {
            "fast_fail_pips": -1.6,
            "never_green_min": 0.3,
            "never_green_timeout_min": 3.0,
            "promote_pips": 1.1,
            "stall_timeout_min": 1.5,
            "promoted_stall_timeout_min": 2.5,
            "soft_giveback": 0.6,
            "hard_giveback": 1.2,
        }
    if family == "PULLBACK_CONTINUATION":
        return {
            "fast_fail_pips": -2.0,
            "never_green_min": 0.2,
            "never_green_timeout_min": 3.5,
            "promote_pips": 1.0,
            "stall_timeout_min": 3.0,
            "promoted_stall_timeout_min": 4.5,
            "soft_giveback": 0.9,
            "hard_giveback": 1.3,
        }
    if family == "RECLAIM_CONTINUATION":
        return {
            "fast_fail_pips": -2.3,
            "never_green_min": 0.15,
            "never_green_timeout_min": 4.0,
            "promote_pips": 0.9,
            "stall_timeout_min": 3.5,
            "promoted_stall_timeout_min": 5.0,
            "soft_giveback": 1.0,
            "hard_giveback": 1.5,
        }
    if family == "RANGE_ESCAPE":
        return {
            "fast_fail_pips": -1.8,
            "never_green_min": 0.2,
            "never_green_timeout_min": 3.0,
            "promote_pips": 1.0,
            "stall_timeout_min": 3.0,
            "promoted_stall_timeout_min": 4.0,
            "soft_giveback": 0.9,
            "hard_giveback": 1.3,
        }
    return {
        "fast_fail_pips": -1.7,
        "never_green_min": 0.2,
        "never_green_timeout_min": 2.5,
        "promote_pips": 1.0,
        "stall_timeout_min": 2.5,
        "promoted_stall_timeout_min": 3.5,
        "soft_giveback": 0.8,
        "hard_giveback": 1.2,
    }


def evaluate_trade_v3(
    trade_rows: list[dict[str, str]],
    cfg: dict[str, Any],
    family_hint: str,
    spread_fallback_pips: float = 0.8,
) -> dict[str, Any]:
    first = trade_rows[0]
    family = family_hint if family_hint else "OTHER"
    profile = _family_profile(family)

    extraction = cfg.get("extraction") or {}
    mode = str(extraction.get("execution_price_mode", cfg.get("execution_price_mode", "side_aware"))).strip().lower()
    use_side_aware_pricing = mode in {"side_aware", "side-aware", "bid_ask", "bid-ask", "execution"}

    open_spread = _f(first.get("open_spread_pips", first.get("spread_pips", first.get("spread", spread_fallback_pips))), spread_fallback_pips)

    state = "INIT"
    best_real = -1e9
    best_r = -1e9
    peak_r = -1e9
    promoted = False
    last_improve_sec = 0.0
    reclaimed_sec: float | None = None

    time_value_floor = _f(extraction.get("time_value_floor_pips_per_min", 0.10), 0.10)

    def _economic_state(
        *,
        hold_seconds: float,
        best_real_pips: float,
        real_pips_now: float,
        since_improve_sec: float,
    ) -> str:
        bankable = profile["never_green_min"]
        runner_trigger = profile["promote_pips"]
        protected_floor = max(bankable, runner_trigger * 0.5)
        mins_open = max(hold_seconds / 60.0, 1e-9)
        value_rate = real_pips_now / mins_open
        is_green_unprotected = best_real_pips >= bankable and real_pips_now < protected_floor
        is_runner_eligible = (
            best_real_pips >= runner_trigger
            and real_pips_now >= protected_floor
            and (
                since_improve_sec <= profile["promoted_stall_timeout_min"] * 60.0
                or value_rate >= time_value_floor
            )
        )

        if best_real_pips < bankable:
            return "NEGATIVE_UNPROVEN"
        # GREEN_UNPROTECTED is owned exclusively by protection logic until
        # the trade reclaims the protected floor.
        if is_green_unprotected:
            return "GREEN_UNPROTECTED"
        # RUNNER receives ownership only when the trade is both protected and
        # still productive enough to justify extension.
        if is_runner_eligible:
            return "RUNNER"
        # TIME_INEFFICIENT only claims protected trades that are no longer
        # productive, avoiding overlap with runner extension ownership.
        if (
            hold_seconds >= profile["promoted_stall_timeout_min"] * 60.0
            and since_improve_sec >= profile["stall_timeout_min"] * 60.0
            and value_rate < time_value_floor
            and real_pips_now >= protected_floor
        ):
            return "TIME_INEFFICIENT"
        return "GREEN_PROTECTED"

    for i, row in enumerate(trade_rows):
        bar_idx = max(1, _i(row.get("bar_index", i + 1), i + 1))
        hold_sec = bar_idx * 60.0

        pips = _f(row.get("profit_now", 0.0), 0.0)
        exit_spread = _f(row.get("exit_spread_pips", row.get("spread_pips", row.get("spread", open_spread))), open_spread)
        break_even = open_spread + exit_spread
        real_pips = pips - break_even

        td = max(0.1, _f(first.get("target_distance", 1.0), 1.0))
        r_now = (real_pips if use_side_aware_pricing else pips) / td

        if r_now > peak_r:
            peak_r = r_now
        if r_now > best_r:
            best_r = r_now

        if real_pips > best_real + 0.2:
            best_real = real_pips
            last_improve_sec = hold_sec

        if best_real <= -1e8:
            best_real = real_pips
            last_improve_sec = hold_sec

        if reclaimed_sec is None and best_real >= profile["never_green_min"]:
            reclaimed_sec = hold_sec

        giveback = max(0.0, best_real - real_pips)
        since_improve = max(0.0, hold_sec - last_improve_sec)

        if hold_sec < 60.0:
            state = "INIT"
        else:
            state = _economic_state(
                hold_seconds=hold_sec,
                best_real_pips=best_real,
                real_pips_now=real_pips,
                since_improve_sec=since_improve,
            )

        promoted = bool(best_real >= profile["promote_pips"])

        # Module ownership is exclusive by state.
        if state == "NEGATIVE_UNPROVEN":
            fast_fail_gate = real_pips <= profile["fast_fail_pips"]
            if family == "EXPANSION_BREAKOUT":
                fast_fail_gate = fast_fail_gate and hold_sec >= 120.0

            if fast_fail_gate:
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_FAST_FAILURE_EXIT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

            if hold_sec > profile["never_green_timeout_min"] * 60.0:
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_NEVER_GREEN_TIMEOUT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

        elif state == "GREEN_UNPROTECTED":
            if since_improve > profile["stall_timeout_min"] * 60.0 and giveback >= profile["soft_giveback"]:
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_PROTECTION_EXIT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

            # Narrow reclaim-stall eject for non-runners only.
            if (
                (not promoted)
                and reclaimed_sec is not None
                and (hold_sec - reclaimed_sec) > profile["promoted_stall_timeout_min"] * 60.0
                and since_improve > profile["stall_timeout_min"] * 60.0
                and giveback > 0.0
            ):
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_PROTECTION_EXIT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

        elif state == "RUNNER":
            if giveback >= profile["hard_giveback"]:
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_HARD_GIVEBACK_EXIT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

            if since_improve > profile["promoted_stall_timeout_min"] * 60.0 and giveback >= profile["soft_giveback"]:
                return {
                    "decision": "CLOSE",
                    "reason": "AEE_V3_FAMILY_STALL_GIVEBACK_EXIT",
                    "pips": pips,
                    "gross_pips": pips,
                    "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                    "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                    "hold_sec": hold_sec,
                    "best_r": best_r,
                    "giveback_r": max(0.0, peak_r - r_now),
                    "family": family,
                    "state": state,
                    "promoted": promoted,
                }

        elif state == "TIME_INEFFICIENT":
            return {
                "decision": "CLOSE",
                "reason": "AEE_V3_FAMILY_TIME_INEFF_EXIT",
                "pips": pips,
                "gross_pips": pips,
                "net_spread_pips": real_pips if use_side_aware_pricing else pips,
                "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                "family": family,
                "state": state,
                "promoted": promoted,
            }

    last = trade_rows[-1]
    pips = _f(last.get("profit_now", _f(last.get("static_pips", 0.0))), 0.0)
    exit_spread = _f(last.get("exit_spread_pips", last.get("spread_pips", last.get("spread", open_spread))), open_spread)
    break_even = open_spread + exit_spread
    real_pips = pips - break_even
    td = max(0.1, _f(first.get("target_distance", 1.0), 1.0))
    final_r = (real_pips if use_side_aware_pricing else pips) / td
    hold_sec = max(1, _i(last.get("bar_index", len(trade_rows)), len(trade_rows))) * 60.0

    return {
        "decision": "HOLD",
        "reason": "AEE_V3_FAMILY_EXTENSION_HOLD",
        "pips": pips,
        "gross_pips": pips,
        "net_spread_pips": real_pips if use_side_aware_pricing else pips,
        "spread_pips_applied": break_even if use_side_aware_pricing else 0.0,
        "hold_sec": hold_sec,
        "best_r": best_r,
        "giveback_r": max(0.0, peak_r - final_r),
        "family": family,
        "state": state,
        "promoted": promoted,
    }
