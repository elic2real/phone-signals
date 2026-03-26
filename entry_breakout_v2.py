"""entry_breakout_v2.py

EXPANSION_BREAKOUT explicit structure detector — RCP TASK-001.

Structural contract:
  A valid EXPANSION_BREAKOUT signal requires sequential evidence of three phases:
    1. COMPRESSION   — short-term range is tight relative to medium-term ATR (price coiling)
    2. RELEASE       — directional close escape outside the compression zone boundary
    3. EXPANSION_HOLD — breakout holds without immediate rejection back into the zone

  No threshold-only entry is permitted. Each phase must be independently observable
  in the price sequence. Removal of any single phase check suppresses the signal.

  This module is a standalone detector. It takes closes/highs/lows + ATR and returns
  a BreakoutStructureResult with named phase states and an overall is_valid flag.

Spec reference: entry_breakout_v2_spec.json
RCP task: TASK-001
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

# ---------------------------------------------------------------------------
# Tunable parameters (defaults match spec; can be overridden by caller)
# ---------------------------------------------------------------------------
DEFAULT_COMPRESSION_WINDOW: int = 8       # bars to measure the compression range
DEFAULT_COMPRESSION_RATIO_MAX: float = 1.2  # tight range: zone_range / atr <= this
DEFAULT_COMPRESSION_MIN_BARS: int = 5     # minimum valid bars in the compression window
DEFAULT_RELEASE_MIN_ATR: float = 0.05     # close must escape zone by at least this * atr
DEFAULT_HOLD_CLEAR_ATR: float = 0.15      # hard-hold clear distance from zone edge (fraction of atr)
DEFAULT_MAX_FRESHNESS_ATR: float = 1.6    # reject if close is too far from zone (stale)
DEFAULT_SPREAD_LIMIT_PIPS: float = 2.8    # spread gate (same as live system default)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class PhaseResult:
    """Outcome of a single structural phase check."""
    name: str           # "COMPRESSION" | "RELEASE" | "EXPANSION_HOLD"
    observed: bool      # True if this phase passed
    reason: str         # compact diagnostic string
    metric: Optional[float] = None  # key numeric evidence


@dataclass
class BreakoutStructureResult:
    """Full structural verdict for one EXPANSION_BREAKOUT detection attempt."""
    is_valid: bool
    direction: str          # "LONG" | "SHORT" | "NONE"
    phases: List[PhaseResult] = field(default_factory=list)
    freshness_atr: float = float("nan")
    fail_phase: Optional[str] = None   # name of first phase that failed
    reason: str = ""


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def detect_expansion_breakout(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    atr: float,
    spread_pips: float = 0.0,
    compression_window: int = DEFAULT_COMPRESSION_WINDOW,
    compression_ratio_max: float = DEFAULT_COMPRESSION_RATIO_MAX,
    compression_min_bars: int = DEFAULT_COMPRESSION_MIN_BARS,
    release_min_atr: float = DEFAULT_RELEASE_MIN_ATR,
    hold_clear_atr: float = DEFAULT_HOLD_CLEAR_ATR,
    max_freshness_atr: float = DEFAULT_MAX_FRESHNESS_ATR,
    spread_limit_pips: float = DEFAULT_SPREAD_LIMIT_PIPS,
) -> BreakoutStructureResult:
    """
    Detect an EXPANSION_BREAKOUT using explicit sequential phase evidence.

    Parameters
    ----------
    closes, highs, lows : price arrays, most recent last (same length required)
    atr                 : medium-term average true range (same units as price)
    spread_pips         : current spread in pips (used as a gate, not a signal input)

    Returns
    -------
    BreakoutStructureResult — is_valid=True only when all three phases are confirmed.
    """
    def _reject(reason: str) -> BreakoutStructureResult:
        return BreakoutStructureResult(is_valid=False, direction="NONE", reason=reason)

    # ------------------------------------------------------------------
    # Pre-structural guards (data quality + spread; not part of the three phases)
    # ------------------------------------------------------------------
    min_len = max(compression_window + 2, compression_min_bars + 2)
    if len(closes) < min_len or len(highs) < min_len or len(lows) < min_len:
        return _reject("insufficient_data")

    if not (math.isfinite(atr) and atr > 0.0):
        return _reject("invalid_atr")

    if not (math.isfinite(closes[-1]) and closes[-1] > 0.0):
        return _reject("invalid_close")

    # Spread gate — independent of structural phases; checked before phase evaluation
    if math.isfinite(spread_pips) and spread_pips > spread_limit_pips:
        return _reject("spread_too_high")

    close = float(closes[-1])
    prev_close = float(closes[-2]) if len(closes) >= 2 and math.isfinite(closes[-2]) else close

    # Compute the compression zone from the look-back window (excludes the current bar)
    comp_highs = highs[-(compression_window + 1):-1]
    comp_lows = lows[-(compression_window + 1):-1]
    valid_h = [h for h in comp_highs if math.isfinite(h)]
    valid_l = [l for l in comp_lows if math.isfinite(l)]

    if len(valid_h) < compression_min_bars or len(valid_l) < compression_min_bars:
        return _reject("compression_window_too_short")

    zone_high = max(valid_h)
    zone_low = min(valid_l)

    if not (math.isfinite(zone_high) and math.isfinite(zone_low) and zone_high > zone_low):
        return _reject("degenerate_zone")

    zone_range = zone_high - zone_low
    range_ratio = zone_range / atr

    # ------------------------------------------------------------------
    # PHASE 1: COMPRESSION
    # Structural requirement: the look-back range is tight relative to ATR,
    # confirming a real coil / consolidation prior to the breakout bar.
    # ------------------------------------------------------------------
    compression_observed = range_ratio <= compression_ratio_max
    phase_compression = PhaseResult(
        name="COMPRESSION",
        observed=compression_observed,
        reason=(
            f"range_ratio={range_ratio:.3f} "
            f"{'<=' if compression_observed else '>'} {compression_ratio_max} "
            f"(zone_range={zone_range:.6f} atr={atr:.6f})"
        ),
        metric=range_ratio,
    )

    if not compression_observed:
        return BreakoutStructureResult(
            is_valid=False,
            direction="NONE",
            phases=[phase_compression],
            fail_phase="COMPRESSION",
            reason="no_compression_coil",
        )

    # ------------------------------------------------------------------
    # PHASE 2: RELEASE
    # Structural requirement: the current close has escaped the compression zone
    # with a directional close (green for LONG, red for SHORT) and by a minimum
    # escape distance. BOTH sub-conditions are required simultaneously.
    # ------------------------------------------------------------------
    escaped_high = close > (zone_high + release_min_atr * atr)
    escaped_low = close < (zone_low - release_min_atr * atr)
    directional_long = close > prev_close   # bullish close = conviction
    directional_short = close < prev_close  # bearish close = conviction

    release_long = escaped_high and directional_long
    release_short = escaped_low and directional_short

    if not (release_long or release_short):
        # Determine best candidate direction for diagnostic output
        if escaped_high or (close > zone_high and directional_long):
            cand_dir = "LONG"
        elif escaped_low or (close < zone_low and directional_short):
            cand_dir = "SHORT"
        else:
            cand_dir = "NONE"
        phase_release = PhaseResult(
            name="RELEASE",
            observed=False,
            reason=(
                f"escaped_high={escaped_high} escaped_low={escaped_low} "
                f"directional_long={directional_long} directional_short={directional_short}"
            ),
        )
        return BreakoutStructureResult(
            is_valid=False,
            direction=cand_dir,
            phases=[phase_compression, phase_release],
            fail_phase="RELEASE",
            reason="no_directional_close_escape",
        )

    direction = "LONG" if release_long else "SHORT"
    zone_edge = zone_high if direction == "LONG" else zone_low
    freshness_atr = abs(close - zone_edge) / atr

    phase_release = PhaseResult(
        name="RELEASE",
        observed=True,
        reason=f"direction={direction} escape+directional_close confirmed freshness_atr={freshness_atr:.3f}",
        metric=freshness_atr,
    )

    # Freshness gate — stale setups are structurally suspect
    if freshness_atr > max_freshness_atr:
        return BreakoutStructureResult(
            is_valid=False,
            direction=direction,
            phases=[phase_compression, phase_release],
            freshness_atr=freshness_atr,
            fail_phase="RELEASE_STALE",
            reason="setup_too_late",
        )

    # ------------------------------------------------------------------
    # PHASE 3: EXPANSION_HOLD
    # Structural requirement: the breakout shows holding evidence — not an
    # immediate spike reversal. Holding is confirmed when EITHER:
    #   (a) the PRIOR close was also outside the zone (prior bar held), OR
    #   (b) the current close is clearly away from the zone edge (hard hold).
    # ------------------------------------------------------------------
    prior_close = float(closes[-2]) if len(closes) >= 2 and math.isfinite(closes[-2]) else close

    if direction == "LONG":
        prior_outside = prior_close > zone_high
        clearly_away = close >= (zone_high + hold_clear_atr * atr)
    else:
        prior_outside = prior_close < zone_low
        clearly_away = close <= (zone_low - hold_clear_atr * atr)

    hold_observed = prior_outside or clearly_away
    phase_hold = PhaseResult(
        name="EXPANSION_HOLD",
        observed=hold_observed,
        reason=(
            f"prior_outside={prior_outside} clearly_away={clearly_away} "
            f"(hold_clear_atr={hold_clear_atr} prior_close={prior_close:.6f} zone_edge={zone_edge:.6f})"
        ),
    )

    if not hold_observed:
        return BreakoutStructureResult(
            is_valid=False,
            direction=direction,
            phases=[phase_compression, phase_release, phase_hold],
            freshness_atr=freshness_atr,
            fail_phase="EXPANSION_HOLD",
            reason="breakout_not_holding",
        )

    # All three phases confirmed
    return BreakoutStructureResult(
        is_valid=True,
        direction=direction,
        phases=[phase_compression, phase_release, phase_hold],
        freshness_atr=freshness_atr,
        reason="all_phases_confirmed",
    )


# ---------------------------------------------------------------------------
# Convenience: batch detect over a rolling window (for replay classification)
# ---------------------------------------------------------------------------

def classify_breakout_family(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    atr: float,
    **kwargs,
) -> str:
    """
    Returns "EXPANSION_BREAKOUT" if detect_expansion_breakout fires, else "OTHER".
    Drop-in replacement for the heuristic _infer_trade_family fallback path.
    """
    result = detect_expansion_breakout(closes, highs, lows, atr, **kwargs)
    return "EXPANSION_BREAKOUT" if result.is_valid else "OTHER"
