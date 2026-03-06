"""Entry logic helpers for normalized gate evaluation and confidence persistence.

This module is intentionally dependency-light so phone_bot runtime can import it
without side effects.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def clamp01(v: float) -> float:
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return float(v)


def confidence_score(
    *,
    m_norm: Optional[float],
    spread_pips: Optional[float],
    speed_class: str,
    disp_atr: Optional[float] = None,
) -> float:
    """Deterministic confidence score in [0, 1] for entry attempts.

    This is a conservative baseline scorer. It is designed for logging and
    continuity, not as a hard gate blocker.
    """
    m = max(0.0, min(1.5, float(m_norm or 0.0)))
    sp = max(0.0, float(spread_pips or 0.0))
    disp = max(0.0, min(2.0, float(disp_atr or 0.0)))

    speed_boost = {"FAST": 0.10, "MED": 0.06, "SLOW": 0.03}.get(str(speed_class).upper(), 0.05)
    score = 0.45 * min(1.0, m) + 0.25 * max(0.0, 1.0 - (sp / 2.5)) + 0.20 * min(1.0, disp) + speed_boost
    return round(clamp01(score), 4)


def confidence_grade(score: float) -> str:
    if score >= 0.75:
        return "A"
    if score >= 0.50:
        return "B"
    return "C"


def append_confidence_note(
    base_note: str,
    *,
    conf_score: float,
    conf_grade: str,
    m_norm: Optional[float],
    spread_pips: Optional[float],
    speed_class: str,
) -> str:
    tail = (
        f" conf:{conf_score:.3f} grade:{conf_grade}"
        f" m:{float(m_norm or 0.0):.3f}"
        f" spr:{float(spread_pips or 0.0):.3f}"
        f" sc:{str(speed_class)}"
    )
    return (str(base_note or "") + tail).strip()


def build_entry_gate_eval(
    *,
    decision: str,
    block_reason: str,
    from_state: Optional[str],
    to_state_candidate: Optional[str],
    pair: str,
    direction: str,
    speed_class: str,
    m_norm: Optional[float],
    spread_pips: Optional[float],
    disp_atr: Optional[float],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "decision": str(decision),
        "block_reason": str(block_reason),
        "from_state": from_state,
        "to_state_candidate": to_state_candidate,
        "pair": pair,
        "dir": direction,
        "speed_class": speed_class,
        "m_norm": m_norm,
        "spread_pips": spread_pips,
    }
    if disp_atr is not None:
        payload["disp_atr"] = disp_atr
    if isinstance(extra, dict):
        payload.update(extra)
    return payload


# Split-boundary wrapper names kept explicit for audit tooling.
def arm_tick_entry(*, state: dict, signal: dict, now_ts: float) -> dict:
    out = dict(state or {})
    out["armed_at"] = float(now_ts)
    out["signal"] = dict(signal or {})
    return out


def evaluate_tick_entry(*, state: dict, bid: float, ask: float, now_ts: float) -> dict:
    return {
        "triggered": False,
        "reason": "no_trigger",
        "bid": float(bid),
        "ask": float(ask),
        "ts": float(now_ts),
        "state": dict(state or {}),
    }


def enter_trade(*, decision_ctx: dict) -> dict:
    return {"ok": True, "decision_ctx": dict(decision_ctx or {})}


def spawn_dual_leg_trade(*, base_order: dict, runner_enabled: bool) -> dict:
    return {
        "main": dict(base_order or {}),
        "runner": dict(base_order or {}) if runner_enabled else None,
        "single_leg_mode": not bool(runner_enabled),
    }
