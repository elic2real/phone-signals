"""entry_pullback_v2.py

PULLBACK_CONTINUATION explicit state machine detector — RCP TASK-003.

Structural contract:
  A valid PULLBACK_CONTINUATION signal requires sequential evidence of three states:
    1. SWING_ESTABLISHED — a prior directional run exists in the look-back window
    2. PULLBACK_DEPTH    — price retraced from the swing extreme by a meaningful amount
    3. RESUMPTION        — directional close back toward trend, above pullback reference

  No bias-only or position-only entry is permitted. Each state must be independently
  observable in the price sequence. Removal of any single state check suppresses the signal.

  v1 (CONTINUATION_PUSH) fired on: close >= cont_lo - 0.08*atr AND close >= prev_close - 0.02*atr
  with macro_bias support — a pure bias+position check with no swing or retrace evidence.

  This module corrects that by requiring all three state conditions to hold.

Spec reference: entry_pullback_v2_spec.json
RCP task: TASK-003
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

# ---------------------------------------------------------------------------
# Tunable parameters
# ---------------------------------------------------------------------------
DEFAULT_SWING_WINDOW: int = 12          # bars to measure the prior swing
DEFAULT_PULLBACK_WINDOW: int = 6        # bars to find the pullback extreme within
DEFAULT_MIN_SWING_ATR: float = 0.6      # minimum swing magnitude (fraction of ATR)
DEFAULT_MIN_RETRACE_ATR: float = 0.25   # minimum pullback depth from swing extreme
DEFAULT_MAX_RETRACE_ATR: float = 1.8    # maximum pullback depth — beyond negates swing
DEFAULT_RESUME_BUFFER_ATR: float = 0.10 # minimum distance above pullback low for resumption
DEFAULT_MAX_FRESHNESS_ATR: float = 1.5  # reject if too far from pullback reference (stale)
DEFAULT_SPREAD_LIMIT_PIPS: float = 2.8  # spread gate (same as live system default)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class StateResult:
    """Outcome of a single structural state check."""
    name: str           # "SWING_ESTABLISHED" | "PULLBACK_DEPTH" | "RESUMPTION"
    observed: bool      # True if this state passed
    reason: str         # compact diagnostic string
    metric: Optional[float] = None  # key numeric evidence


@dataclass
class PullbackStructureResult:
    """Full structural verdict for one PULLBACK_CONTINUATION detection attempt."""
    is_valid: bool
    direction: str                  # "LONG" | "SHORT" | "NONE"
    states: List[StateResult] = field(default_factory=list)
    swing_magnitude_atr: float = float("nan")
    retrace_depth_atr: float = float("nan")
    fail_state: Optional[str] = None
    reason: str = ""


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def detect_pullback_continuation(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    atr: float,
    spread_pips: float = 0.0,
    swing_window: int = DEFAULT_SWING_WINDOW,
    pullback_window: int = DEFAULT_PULLBACK_WINDOW,
    min_swing_atr: float = DEFAULT_MIN_SWING_ATR,
    min_retrace_atr: float = DEFAULT_MIN_RETRACE_ATR,
    max_retrace_atr: float = DEFAULT_MAX_RETRACE_ATR,
    resume_buffer_atr: float = DEFAULT_RESUME_BUFFER_ATR,
    max_freshness_atr: float = DEFAULT_MAX_FRESHNESS_ATR,
    spread_limit_pips: float = DEFAULT_SPREAD_LIMIT_PIPS,
) -> PullbackStructureResult:
    """
    Detect a PULLBACK_CONTINUATION using explicit sequential state evidence.

    Parameters
    ----------
    closes, highs, lows : price arrays, most recent last
    atr                  : medium-term average true range (same units as price)
    spread_pips          : current spread in pips (pre-structural gate)

    Returns
    -------
    PullbackStructureResult — is_valid=True only when all three states confirmed.
    """
    def _reject(reason: str) -> PullbackStructureResult:
        return PullbackStructureResult(is_valid=False, direction="NONE", reason=reason)

    # ------------------------------------------------------------------
    # Pre-structural guards
    # ------------------------------------------------------------------
    min_len = max(swing_window + 2, pullback_window + 3)
    if len(closes) < min_len or len(highs) < min_len or len(lows) < min_len:
        return _reject("insufficient_data")

    if not (math.isfinite(atr) and atr > 0.0):
        return _reject("invalid_atr")

    if not (math.isfinite(closes[-1]) and closes[-1] > 0.0):
        return _reject("invalid_close")

    if math.isfinite(spread_pips) and spread_pips > spread_limit_pips:
        return _reject("spread_too_high")

    close = float(closes[-1])
    prev_close = float(closes[-2]) if len(closes) >= 2 and math.isfinite(closes[-2]) else close

    # Swing window: all bars excluding the current bar
    swing_highs = highs[-(swing_window + 1):-1]
    swing_lows = lows[-(swing_window + 1):-1]
    valid_sh = [h for h in swing_highs if math.isfinite(h)]
    valid_sl = [l for l in swing_lows if math.isfinite(l)]

    if len(valid_sh) < 3 or len(valid_sl) < 3:
        return _reject("swing_window_too_short")

    swing_high = max(valid_sh)
    swing_low = min(valid_sl)
    swing_range = swing_high - swing_low

    if not (swing_high > swing_low):
        return _reject("degenerate_swing_range")

    # Determine candidate direction from price position within swing range
    swing_mid = (swing_high + swing_low) * 0.5
    if close >= swing_mid:
        candidate_direction = "LONG"
    else:
        candidate_direction = "SHORT"

    # ------------------------------------------------------------------
    # STATE 1: SWING_ESTABLISHED
    # Structural requirement: the look-back window contains a real directional
    # run. The swing magnitude (range / atr) must meet the minimum threshold,
    # confirming there was an established move to pull back from.
    # ------------------------------------------------------------------
    swing_magnitude_atr = swing_range / atr
    swing_observed = swing_magnitude_atr >= min_swing_atr
    state_swing = StateResult(
        name="SWING_ESTABLISHED",
        observed=swing_observed,
        reason=(
            f"swing_magnitude_atr={swing_magnitude_atr:.3f} "
            f"{'>=': '<'[not swing_observed]}{min_swing_atr} "
            f"(swing_high={swing_high:.6f} swing_low={swing_low:.6f})"
        ),
        metric=swing_magnitude_atr,
    )

    if not swing_observed:
        return PullbackStructureResult(
            is_valid=False,
            direction=candidate_direction,
            states=[state_swing],
            swing_magnitude_atr=swing_magnitude_atr,
            fail_state="SWING_ESTABLISHED",
            reason="no_prior_swing",
        )

    # ------------------------------------------------------------------
    # STATE 2: PULLBACK_DEPTH
    # Structural requirement: price retraced from the swing extreme by a
    # meaningful amount. Measure the pullback within the pullback_window bars.
    # For LONG: pullback_low must be below swing_high by min_retrace_atr* atr.
    # For SHORT: pullback_high must be above swing_low by min_retrace_atr * atr.
    # ------------------------------------------------------------------
    pb_highs = highs[-(pullback_window + 1):-1]
    pb_lows = lows[-(pullback_window + 1):-1]
    valid_pbh = [h for h in pb_highs if math.isfinite(h)]
    valid_pbl = [l for l in pb_lows if math.isfinite(l)]

    if candidate_direction == "LONG":
        pullback_extreme = min(valid_pbl) if valid_pbl else close
        retrace_depth_atr = (swing_high - pullback_extreme) / atr
        retrace_in_range = min_retrace_atr <= retrace_depth_atr <= max_retrace_atr
        still_in_pullback = close <= (swing_high - (min_retrace_atr * 0.5 * atr))
    else:
        pullback_extreme = max(valid_pbh) if valid_pbh else close
        retrace_depth_atr = (pullback_extreme - swing_low) / atr
        retrace_in_range = min_retrace_atr <= retrace_depth_atr <= max_retrace_atr
        still_in_pullback = close >= (swing_low + (min_retrace_atr * 0.5 * atr))

    pullback_observed = retrace_in_range and still_in_pullback
    state_pullback = StateResult(
        name="PULLBACK_DEPTH",
        observed=pullback_observed,
        reason=(
            f"retrace_depth_atr={retrace_depth_atr:.3f} "
            f"[min={min_retrace_atr} max={max_retrace_atr}] "
            f"in_range={retrace_in_range} still_in_pullback={still_in_pullback}"
        ),
        metric=retrace_depth_atr,
    )

    if not pullback_observed:
        return PullbackStructureResult(
            is_valid=False,
            direction=candidate_direction,
            states=[state_swing, state_pullback],
            swing_magnitude_atr=swing_magnitude_atr,
            retrace_depth_atr=retrace_depth_atr,
            fail_state="PULLBACK_DEPTH",
            reason="pullback_depth_not_valid",
        )

    # ------------------------------------------------------------------
    # STATE 3: RESUMPTION
    # Structural requirement: price is showing directional resumption —
    # it has bounced from the pullback extreme with a directional close
    # and is above (LONG) / below (SHORT) the pullback reference + buffer.
    # ------------------------------------------------------------------
    if candidate_direction == "LONG":
        above_pullback = close >= (pullback_extreme + resume_buffer_atr * atr)
        directional_close = close > prev_close
    else:
        above_pullback = close <= (pullback_extreme - resume_buffer_atr * atr)
        directional_close = close < prev_close

    resumption_observed = above_pullback and directional_close

    # Freshness: how far is the close from the pullback extreme (in ATR)
    freshness_atr = abs(close - pullback_extreme) / atr

    state_resume = StateResult(
        name="RESUMPTION",
        observed=resumption_observed,
        reason=(
            f"above_pullback={above_pullback} directional_close={directional_close} "
            f"freshness_atr={freshness_atr:.3f}"
        ),
        metric=freshness_atr,
    )

    if not resumption_observed:
        return PullbackStructureResult(
            is_valid=False,
            direction=candidate_direction,
            states=[state_swing, state_pullback, state_resume],
            swing_magnitude_atr=swing_magnitude_atr,
            retrace_depth_atr=retrace_depth_atr,
            fail_state="RESUMPTION",
            reason="no_resumption_close",
        )

    # Freshness gate
    if freshness_atr > max_freshness_atr:
        return PullbackStructureResult(
            is_valid=False,
            direction=candidate_direction,
            states=[state_swing, state_pullback, state_resume],
            swing_magnitude_atr=swing_magnitude_atr,
            retrace_depth_atr=retrace_depth_atr,
            fail_state="RESUMPTION_STALE",
            reason="setup_too_late",
        )

    # All three states confirmed
    return PullbackStructureResult(
        is_valid=True,
        direction=candidate_direction,
        states=[state_swing, state_pullback, state_resume],
        swing_magnitude_atr=swing_magnitude_atr,
        retrace_depth_atr=retrace_depth_atr,
        reason="all_states_confirmed",
    )


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

def classify_pullback_family(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    atr: float,
    **kwargs,
) -> str:
    """
    Returns "PULLBACK_CONTINUATION" if detect_pullback_continuation fires, else "OTHER".
    Drop-in for the heuristic _infer_trade_family pullback path.
    """
    result = detect_pullback_continuation(closes, highs, lows, atr, **kwargs)
    return "PULLBACK_CONTINUATION" if result.is_valid else "OTHER"
