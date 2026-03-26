from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ReclaimResult:
    is_valid: bool
    direction: str
    reclaim_level: Optional[float]
    reclaim_index: Optional[int]
    acceptance_bars: int
    reason: str
    fail_state: Optional[str]


def _safe_level(closes: List[float], lookback: int = 6) -> Optional[float]:
    if len(closes) < lookback:
        return None
    window = closes[-lookback:]
    return sum(window) / float(len(window))


def detect_reclaim_continuation(
    closes: List[float],
    highs: List[float],
    lows: List[float],
    atr: float,
    spread_pips: float,
    max_spread_pips: float = 2.8,
    acceptance_window: int = 2,
) -> ReclaimResult:
    if spread_pips > max_spread_pips:
        return ReclaimResult(False, "NONE", None, None, 0, "spread_too_wide", "RECLAIM_PRINT")

    if min(len(closes), len(highs), len(lows)) < 12:
        return ReclaimResult(False, "NONE", None, None, 0, "insufficient_history", "LEVEL_BREACH")

    level = _safe_level(closes[:-3], lookback=6)
    if level is None:
        return ReclaimResult(False, "NONE", None, None, 0, "insufficient_history", "LEVEL_BREACH")

    recent = closes[-7:]
    if max(recent) - min(recent) < max(0.35 * atr, 0.02):
        return ReclaimResult(False, "NONE", level, None, 0, "micro_range_only", "LEVEL_BREACH")

    breach_idx = None
    breach_dir = "NONE"
    for i in range(len(closes) - 8, len(closes) - 2):
        if closes[i] < level and highs[i] < level:
            breach_idx = i
            breach_dir = "LONG"
        if closes[i] > level and lows[i] > level:
            breach_idx = i
            breach_dir = "SHORT"

    if breach_idx is None:
        return ReclaimResult(False, "NONE", level, None, 0, "no_clear_breach", "LEVEL_BREACH")

    reclaim_idx = None
    if breach_dir == "LONG":
        for j in range(breach_idx + 1, len(closes)):
            if closes[j] > level:
                reclaim_idx = j
                break
    else:
        for j in range(breach_idx + 1, len(closes)):
            if closes[j] < level:
                reclaim_idx = j
                break

    if reclaim_idx is None:
        return ReclaimResult(False, "NONE", level, None, 0, "no_close_back_across_level", "RECLAIM_PRINT")

    post = closes[reclaim_idx + 1:reclaim_idx + 1 + acceptance_window]
    if len(post) < acceptance_window:
        return ReclaimResult(False, "NONE", level, reclaim_idx, len(post), "acceptance_window_not_met", "ACCEPTANCE_HOLD")

    if breach_dir == "LONG":
        if any(p <= level for p in post):
            return ReclaimResult(False, "NONE", level, reclaim_idx, len(post), "immediate_reject_back_through_level", "ACCEPTANCE_HOLD")
    else:
        if any(p >= level for p in post):
            return ReclaimResult(False, "NONE", level, reclaim_idx, len(post), "immediate_reject_back_through_level", "ACCEPTANCE_HOLD")

    return ReclaimResult(True, breach_dir, level, reclaim_idx, len(post), "reclaim_accepted", None)


def classify_reclaim_family(result: ReclaimResult) -> str:
    if not result.is_valid:
        return "NONE"
    return "RECLAIM_CONTINUATION"
