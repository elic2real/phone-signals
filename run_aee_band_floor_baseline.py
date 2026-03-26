#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _parse_ts(ts: str) -> float | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bootstrap_ci_mean(values: list[float], samples: int, seed: int) -> tuple[float, float] | None:
    if len(values) < 2 or samples <= 0:
        return None
    rnd = random.Random(seed)
    n = len(values)
    means: list[float] = []
    for _ in range(samples):
        draw = [values[rnd.randrange(n)] for __ in range(n)]
        means.append(sum(draw) / n)
    means.sort()
    lo_idx = max(0, int(0.025 * (samples - 1)))
    hi_idx = min(samples - 1, int(0.975 * (samples - 1)))
    return means[lo_idx], means[hi_idx]


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def _stream_duration_hours(rows: list[dict[str, str]]) -> float:
    ts = [_parse_ts(str(r.get("timestamp", ""))) for r in rows]
    ts = [t for t in ts if t is not None]
    if len(ts) < 2:
        return 1.0
    return max(1.0 / 60.0, (max(ts) - min(ts)) / 3600.0)


def _r_bin(r: float) -> str:
    if r <= -1.0:
        return "R<=-1.0"
    if r <= -0.4:
        return "-1.0<R<=-0.4"
    if r <= 0.0:
        return "-0.4<R<=0"
    if r <= 0.15:
        return "0<R<=0.15"
    if r <= 0.4:
        return "0.15<R<=0.4"
    if r <= 1.0:
        return "0.4<R<=1.0"
    return "R>1.0"


def _timeout_to_seconds(
    extraction: dict[str, Any],
    key_sec: str,
    default_minutes: float,
) -> float:
    # Legacy configs use *_sec keys but values were tuned on minute bars.
    key_min = key_sec.replace("_sec", "_min")
    key_bars = key_sec.replace("_sec", "_bars")
    if key_bars in extraction:
        return max(0.0, _safe_float(extraction.get(key_bars, default_minutes), default_minutes) * 60.0)
    if key_min in extraction:
        return max(0.0, _safe_float(extraction.get(key_min, default_minutes), default_minutes) * 60.0)

    raw = _safe_float(extraction.get(key_sec, default_minutes), default_minutes)
    timeout_units = str(extraction.get("timeout_units", "legacy_minutes")).strip().lower()
    if timeout_units in {"seconds", "second", "sec", "s"}:
        return max(0.0, raw)
    return max(0.0, raw * 60.0)


def _eval_trade_baseline(
    trade_rows: list[dict[str, str]],
    cfg: dict[str, Any],
    friction_per_trade_pips: float,
    economic_value_margin_mult: float,
    spread_fallback_pips: float = 0.8,
) -> dict[str, Any]:
    first = trade_rows[0]
    td = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))

    # Support both legacy and PED naming.
    band_w = max(0.02, _safe_float(cfg.get("band_size_r", cfg.get("band_width_r", 0.10)), 0.10))

    near = cfg.get("near_entry") or {}
    extraction = cfg.get("extraction") or {}
    aee_mode = str(
        extraction.get("aee_version", extraction.get("aee_mode", cfg.get("aee_version", "v2")))
    ).strip().lower()

    if aee_mode in {"v3", "family_v3", "family-aware-v3", "family_aware_v3"}:
        # Opt-in family-aware AEE path. V2 remains default unless explicitly enabled.
        from aee_family_state_machine_v3 import evaluate_trade_v3  # local import avoids hard dependency in legacy paths

        family_hint = _infer_trade_family(trade_rows)
        return evaluate_trade_v3(
            trade_rows=trade_rows,
            cfg=cfg,
            family_hint=family_hint,
            spread_fallback_pips=spread_fallback_pips,
        )

    execution_price_mode = str(extraction.get("execution_price_mode", cfg.get("execution_price_mode", "side_aware"))).strip().lower()
    use_side_aware_pricing = execution_price_mode in {"side_aware", "side-aware", "bid_ask", "bid-ask", "execution"}

    never_green_timeout_sec = _timeout_to_seconds(
        extraction,
        "never_green_timeout_sec",
        _safe_float(near.get("never_green_timeout_sec", near.get("timeout_sec", 1.5)), 1.5),
    )
    pair = str(first.get("pair", first.get("symbol", first.get("instrument", "")))).upper().strip()
    majors = {
        "EUR_USD",
        "GBP_USD",
        "USD_JPY",
        "USD_CHF",
        "USD_CAD",
        "AUD_USD",
        "NZD_USD",
    }
    never_green_min_life_pips = _safe_float(extraction.get("never_green_min_life_pips", 0.3), 0.3)
    min_lock_profit_pips = _safe_float(
        extraction.get(
            "min_lock_profit_pips",
            extraction.get("min_lock_profit_pips_major", 0.8 if pair in majors else 1.2),
        ),
        0.8 if pair in majors else 1.2,
    )
    min_improvement_pips = _safe_float(extraction.get("min_improvement_pips", 0.2), 0.2)
    weak_green_cap_pips = _safe_float(extraction.get("weak_green_cap_pips", 1.0), 1.0)
    weak_green_timeout_sec = _timeout_to_seconds(extraction, "weak_green_timeout_sec", 1.5)
    promotion_profit_pips = _safe_float(extraction.get("promotion_profit_pips", 1.2), 1.2)
    non_promoted_stall_sec = _timeout_to_seconds(extraction, "non_promoted_stall_sec", 1.5)
    promoted_stall_sec = _timeout_to_seconds(extraction, "promoted_stall_sec", 3.0)
    promoted_soft_giveback_pips = _safe_float(extraction.get("promoted_soft_giveback_pips", 0.7), 0.7)
    promoted_hard_giveback_pips = _safe_float(extraction.get("promoted_hard_giveback_pips", 1.0), 1.0)

    open_spread_pips = _safe_float(
        extraction.get(
            "open_spread_pips",
            first.get("open_spread_pips", first.get("spread_pips", first.get("spread", _safe_float(extraction.get("fallback_spread_pips", spread_fallback_pips), spread_fallback_pips)))),
        ),
        _safe_float(extraction.get("fallback_spread_pips", spread_fallback_pips), spread_fallback_pips),
    )
    expected_exit_spread_pips = _safe_float(
        extraction.get("expected_exit_spread_pips", open_spread_pips),
        open_spread_pips,
    )
    break_even_pips_default = _safe_float(
        extraction.get("break_even_pips", open_spread_pips + expected_exit_spread_pips),
        open_spread_pips + expected_exit_spread_pips,
    )

    fast_adverse_r = _safe_float((cfg.get("fast_adverse") or {}).get("adverse_r", 0.35), 0.35)
    fast_cfg = cfg.get("fast_adverse") or {}
    fast_adverse_window_sec = _timeout_to_seconds(
        fast_cfg,
        "window_sec",
        _safe_float(fast_cfg.get("window_sec", 1.5), 1.5),
    )

    pre_sl_on = bool((cfg.get("defensive") or {}).get("pre_sl_exit_enabled", True))
    panic_on = bool((cfg.get("defensive") or {}).get("panic_exit_enabled", True))

    peak_r = -1e9
    best_r = -1e9
    green_armed = False
    promoted = False
    best_current_profit_pips = 0.0
    best_real_profit_pips = 0.0
    last_improvement_sec = 0.0

    for i, row in enumerate(trade_rows):
        bar_idx = max(1, _safe_int(row.get("bar_index", i + 1), i + 1))
        hold_sec = bar_idx * 60.0
        pips = _safe_float(row.get("profit_now", 0.0), 0.0)
        row_exit_spread_pips = _safe_float(
            row.get("exit_spread_pips", row.get("spread_pips", row.get("spread", expected_exit_spread_pips))),
            expected_exit_spread_pips,
        )
        break_even_pips = break_even_pips_default if "break_even_pips" in extraction else (open_spread_pips + row_exit_spread_pips)
        real_profit_pips = pips - break_even_pips
        effective_profit_pips = real_profit_pips if use_side_aware_pricing else pips
        score_r_now = effective_profit_pips / td
        r_now = effective_profit_pips / td
        vel = _safe_float(row.get("velocity_now", 0.0), 0.0)
        best_current_profit_pips = max(best_current_profit_pips, effective_profit_pips)

        if r_now > peak_r:
            peak_r = r_now
        best_r = max(best_r, r_now)

        time_since_last_improvement_sec = max(0.0, hold_sec - last_improvement_sec)
        giveback_pips = max(0.0, best_real_profit_pips - real_profit_pips)

        telemetry = {
            "current_profit_pips": pips,
            "break_even_pips": break_even_pips,
            "real_profit_pips": real_profit_pips,
            "best_profit_pips": best_real_profit_pips,
            "time_in_trade_sec": hold_sec,
            "time_since_last_improvement_sec": time_since_last_improvement_sec,
            "giveback_pips": giveback_pips,
            "green_armed": bool(green_armed),
            "promoted": bool(promoted),
            "timeout_used_sec": 0.0,
        }

        if hold_sec <= max(60.0, fast_adverse_window_sec * 60.0) and r_now <= -abs(fast_adverse_r):
            return {
                "decision": "CLOSE",
                "reason": "AEE_BAND_FAST_FAILURE_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        if pre_sl_on and r_now <= -0.82:
            return {
                "decision": "CLOSE",
                "reason": "AEE_PRE_SL_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        if panic_on and r_now <= -0.60 and vel <= -0.08:
            return {
                "decision": "CLOSE",
                "reason": "AEE_PANIC_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        if best_current_profit_pips < never_green_min_life_pips and hold_sec > never_green_timeout_sec:
            return {
                "decision": "CLOSE",
                "reason": "AEE_NEVER_GREEN_TIMEOUT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        # Real green activation only after cost-cleared profit.
        if real_profit_pips >= min_lock_profit_pips:
            green_armed = True
            telemetry["green_armed"] = True

        # Meaningful highs only.
        if real_profit_pips > best_real_profit_pips + min_improvement_pips:
            best_real_profit_pips = real_profit_pips
            last_improvement_sec = hold_sec
            time_since_last_improvement_sec = 0.0
            giveback_pips = 0.0
            telemetry["best_profit_pips"] = best_real_profit_pips
            telemetry["time_since_last_improvement_sec"] = 0.0
            telemetry["giveback_pips"] = 0.0

        if best_real_profit_pips >= promotion_profit_pips:
            promoted = True
            telemetry["promoted"] = True

        # Green but weak and stalling: close quickly.
        if green_armed and best_real_profit_pips < weak_green_cap_pips and time_since_last_improvement_sec > weak_green_timeout_sec:
            telemetry["timeout_used_sec"] = weak_green_timeout_sec
            return {
                "decision": "CLOSE",
                "reason": "AEE_WEAK_GREEN_TIMEOUT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        # Non-promoted winners should not stall long.
        if green_armed and (not promoted) and time_since_last_improvement_sec > non_promoted_stall_sec:
            telemetry["timeout_used_sec"] = non_promoted_stall_sec
            return {
                "decision": "CLOSE",
                "reason": "AEE_CONTINUATION_FAILED_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        # Promoted giveback hard stop.
        if promoted and giveback_pips >= promoted_hard_giveback_pips:
            telemetry["timeout_used_sec"] = promoted_stall_sec
            return {
                "decision": "CLOSE",
                "reason": "AEE_GIVEBACK_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

        # Promoted stall logic: allow pause, exit only on meaningful giveback.
        if promoted and time_since_last_improvement_sec > promoted_stall_sec and giveback_pips >= promoted_soft_giveback_pips:
            telemetry["timeout_used_sec"] = promoted_stall_sec
            return {
                "decision": "CLOSE",
                "reason": "AEE_GIVEBACK_EXIT",
                "r": score_r_now,
                "pips": pips,
                "gross_pips": pips,
                "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
                "net_spread_pips": effective_profit_pips,
                "hold_sec": hold_sec,
                "best_r": best_r,
                "giveback_r": max(0.0, peak_r - r_now),
                **telemetry,
            }

    last = trade_rows[-1]
    final_gross_pips = _safe_float(last.get("profit_now", _safe_float(last.get("static_pips", 0.0))), 0.0)
    final_exit_spread_pips = _safe_float(
        last.get("exit_spread_pips", last.get("spread_pips", last.get("spread", expected_exit_spread_pips))),
        expected_exit_spread_pips,
    )
    final_break_even_pips = break_even_pips_default if "break_even_pips" in extraction else (open_spread_pips + final_exit_spread_pips)
    final_real_pips = final_gross_pips - final_break_even_pips
    final_pips = final_real_pips if use_side_aware_pricing else final_gross_pips
    final_r = final_pips / td
    hold_sec = max(1, _safe_int(last.get("bar_index", len(trade_rows)), len(trade_rows))) * 60.0
    return {
        "decision": "HOLD",
        "reason": "AEE_BAND_EXTENSION_HOLD",
        "r": final_r,
        "pips": final_gross_pips,
        "gross_pips": final_gross_pips,
        "spread_pips_applied": final_break_even_pips if use_side_aware_pricing else 0.0,
        "net_spread_pips": final_pips,
        "hold_sec": hold_sec,
        "best_r": best_r,
        "giveback_r": max(0.0, peak_r - final_r),
        "current_profit_pips": final_gross_pips,
        "break_even_pips": final_break_even_pips,
        "real_profit_pips": final_real_pips,
        "best_profit_pips": best_real_profit_pips,
        "time_in_trade_sec": hold_sec,
        "time_since_last_improvement_sec": max(0.0, hold_sec - last_improvement_sec),
        "giveback_pips": max(0.0, best_real_profit_pips - final_real_pips),
        "green_armed": bool(green_armed),
        "promoted": bool(promoted),
        "timeout_used_sec": 0.0,
    }


def _eval_trade_static(
    trade_rows: list[dict[str, str]],
    execution_price_mode: str,
    spread_fallback_pips: float = 0.8,
) -> dict[str, Any]:
    first = trade_rows[0]
    last = trade_rows[-1]
    td = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))
    mode = str(execution_price_mode).strip().lower()
    use_side_aware_pricing = mode in {"side_aware", "side-aware", "bid_ask", "bid-ask", "execution"}

    gross_pips = _safe_float(last.get("static_pips", 0.0), 0.0)
    open_spread_pips = _safe_float(first.get("open_spread_pips", first.get("spread_pips", first.get("spread", spread_fallback_pips))), spread_fallback_pips)
    exit_spread_pips = _safe_float(last.get("exit_spread_pips", last.get("spread_pips", last.get("spread", open_spread_pips))), open_spread_pips)
    break_even_pips = _safe_float(first.get("break_even_pips", open_spread_pips + exit_spread_pips), open_spread_pips + exit_spread_pips)
    spread_net_pips = gross_pips - break_even_pips if use_side_aware_pricing else gross_pips
    r = spread_net_pips / td
    hold_sec = max(1, _safe_int(last.get("static_exit_bar", last.get("bar_index", len(trade_rows))), len(trade_rows))) * 60.0
    return {
        "reason": str(last.get("static_reason", "STATIC")),
        "r": r,
        "pips": gross_pips,
        "gross_pips": gross_pips,
        "spread_pips_applied": break_even_pips if use_side_aware_pricing else 0.0,
        "net_spread_pips": spread_net_pips,
        "hold_sec": hold_sec,
        "best_r": max(((_safe_float(x.get("profit_now", 0.0), 0.0) - break_even_pips) / td) for x in trade_rows)
        if use_side_aware_pricing
        else max((_safe_float(x.get("profit_now", 0.0), 0.0) / td) for x in trade_rows),
    }


def _context_from_stream(root: Path, stream_path: Path) -> tuple[str, str, str, str]:
    rel = stream_path.relative_to(root)
    parts = rel.parts
    pair = "UNKNOWN"
    day = "unknown"
    session = "unknown"
    if "compiled_market_nodes" in parts:
        idx = parts.index("compiled_market_nodes")
        if idx + 1 < len(parts):
            node = parts[idx + 1]
            bits = node.split("__")
            if len(bits) > 0:
                pair = bits[0]
            if len(bits) > 1:
                day = bits[1]
            if len(bits) > 2:
                session = bits[2]
    context = f"{pair}__{day}__{session}"
    return pair, day, session, context


def _row_at_or_after_bar(trade_rows: list[dict[str, str]], bar: int) -> dict[str, str]:
    target = max(1, int(bar))
    for row in trade_rows:
        if _safe_int(row.get("bar_index", 0), 0) >= target:
            return row
    return trade_rows[-1]


def _max_profit_within_window(
    trade_rows: list[dict[str, str]],
    window_sec: float,
) -> float:
    if not trade_rows:
        return 0.0

    # Prefer timestamp-based windowing when available.
    t0 = _parse_ts(str(trade_rows[0].get("timestamp", "")))
    if t0 is not None:
        vals: list[float] = []
        for row in trade_rows:
            ts = _parse_ts(str(row.get("timestamp", "")))
            if ts is None:
                continue
            if (ts - t0) <= max(0.0, float(window_sec)):
                vals.append(_safe_float(row.get("profit_now", 0.0), 0.0))
            else:
                break
        if vals:
            return max(vals)

    # Fallback for coarse streams: map window to at least first bar.
    bars = max(1, int((max(0.0, float(window_sec)) + 59.999) // 60))
    vals = [_safe_float(_row_at_or_after_bar(trade_rows, b).get("profit_now", 0.0), 0.0) for b in range(1, bars + 1)]
    return max(vals) if vals else 0.0


def _family_from_strategy_name(strategy: str) -> str:
    s = str(strategy or "").upper()
    if "FAILED_BREAKOUT_FADE" in s:
        return "RECLAIM_CONTINUATION"
    if "INTENTIONAL_RUNNER" in s:
        return "PULLBACK_CONTINUATION"
    if "COMPRESSION_EXPANSION_RUN" in s or "VOL_REIGNITE" in s:
        return "EXPANSION_BREAKOUT"
    if "RANGE" in s:
        return "RANGE_ESCAPE"
    return "OTHER"


def _infer_trade_family(trade_rows: list[dict[str, str]]) -> str:
    if not trade_rows:
        return "OTHER"
    first = trade_rows[0]

    explicit = str(first.get("entry_family", "")).upper().strip()
    if explicit:
        return explicit

    for key in ("setup_name", "setup", "reason_code", "strategy"):
        v = str(first.get(key, "")).strip()
        if v:
            return _family_from_strategy_name(v)

    # Fallback: infer family mechanically from pre-entry shape when explicit labels are absent.
    bar2 = _row_at_or_after_bar(trade_rows, 2)
    bar3 = _row_at_or_after_bar(trade_rows, 3)

    compression = _safe_float(first.get("compression", 0.0), 0.0)
    release_quality = _safe_float(first.get("release_quality", 0.0), 0.0)
    noise = _safe_float(first.get("noise", 1.0), 1.0)
    pre_align = _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)
    pre_rel = _safe_float(first.get("pre_compression_release_delta", 0.0), 0.0)
    pre_slope = _safe_float(first.get("pre_build_slope", 0.0), 0.0)
    pre_accel = _safe_float(first.get("pre_build_accel", 0.0), 0.0)
    pre_noise_slope = _safe_float(first.get("pre_noise_slope", 0.0), 0.0)
    pre_budget_slope = _safe_float(first.get("pre_budget_slope", 0.0), 0.0)
    macro_dir = _safe_float(first.get("macro_dir_score", 0.0), 0.0)
    micro_dir = _safe_float(first.get("micro_dir_score", 0.0), 0.0)
    progress2 = _safe_float(bar2.get("progress_ratio", 0.0), 0.0)
    progress3 = _safe_float(bar3.get("progress_ratio", 0.0), 0.0)

    td = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))
    pmax_2m = _max_profit_within_window(trade_rows, 120.0)
    pmax_3m = _max_profit_within_window(trade_rows, 180.0)

    life = str(first.get("lifecycle_label", "")).upper().strip()
    early = trade_rows[: min(4, len(trade_rows))]
    early_pips = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in early]
    early_range = (max(early_pips) - min(early_pips)) if early_pips else 0.0
    monotonic_push = sum(1 for i in range(1, len(early_pips)) if early_pips[i] >= early_pips[i - 1] - 0.05) >= max(1, len(early_pips) - 1)

    reclaim_score = 0
    pullback_score = 0
    breakout_pre_compression = False
    breakout_compression = False
    breakout_break = False
    breakout_post_expansion = False
    range_score = 0

    if pre_align < -0.08:
        reclaim_score += 2
    if pre_rel > 0.06:
        reclaim_score += 1
    if progress2 > 0.03:
        reclaim_score += 1
    if progress3 > progress2 + 0.08:
        reclaim_score += 1
    if abs(macro_dir) < 0.12 and abs(micro_dir) > 0.12:
        reclaim_score += 1
    if progress2 < 0.10 and progress3 > 0.30 and pre_align < -0.02:
        reclaim_score += 2

    if abs(pre_slope) > 0.03:
        pullback_score += 1
    if pre_accel < -0.01:
        pullback_score += 1
    if pre_noise_slope < 0.0:
        pullback_score += 1
    if 0.06 <= progress2 <= 0.55:
        pullback_score += 1
    if progress3 > progress2 + 0.06:
        pullback_score += 1
    if life in {"FRAGILE", "PROVING"}:
        pullback_score += 1
    if progress2 < 0.12 and progress3 > 0.25:
        pullback_score += 2

    breakout_pre_compression = compression <= 0.62 and noise <= 0.92
    breakout_compression = breakout_pre_compression and compression <= 0.58 and abs(pre_slope) <= 0.035
    breakout_break = breakout_compression and release_quality >= 0.12 and pre_rel >= 0.08
    breakout_post_expansion = (
        breakout_break
        and progress2 >= 0.05
        and (progress3 >= progress2 or monotonic_push)
        and pre_budget_slope >= 0.0
    )

    if compression >= 0.60:
        range_score += 2
    if noise <= 0.78:
        range_score += 1
    if early_range <= max(0.8, 0.35 * td):
        range_score += 1
    if pmax_2m >= 0.15 and pmax_3m >= pmax_2m:
        range_score += 1

    if reclaim_score >= 4:
        return "RECLAIM_CONTINUATION"
    if pullback_score >= 4 and reclaim_score < 4:
        return "PULLBACK_CONTINUATION"
    if breakout_post_expansion and reclaim_score < 4 and pullback_score < 5:
        return "EXPANSION_BREAKOUT"
    if range_score >= 3 and not breakout_post_expansion:
        return "RANGE_ESCAPE"

    return "OTHER"


def _entry_filter_reason(
    trade_rows: list[dict[str, str]],
    pair: str,
    context_lc: str,
    include_families: set[str],
    exclude_families: set[str],
    exclude_contexts_lc: set[str],
    min_profit_now_pips_by_bar: list[dict[str, Any]],
    min_progress_ratio_by_bar: list[dict[str, Any]],
    min_release_quality_by_bar: list[dict[str, Any]],
    max_noise_by_bar: list[dict[str, Any]],
    micro_confirm: dict[str, Any],
    include_pairs: set[str] | None = None,
    include_sessions: set[str] | None = None,
    family_specific_filters: dict[str, Any] | None = None,
) -> str | None:
    eval_result = _entry_filter_evaluate(
        trade_rows,
        pair,
        context_lc,
        include_families,
        exclude_families,
        exclude_contexts_lc,
        min_profit_now_pips_by_bar,
        min_progress_ratio_by_bar,
        min_release_quality_by_bar,
        max_noise_by_bar,
        micro_confirm,
        include_pairs,
        include_sessions,
        family_specific_filters,
    )
    return str(eval_result.get("reason")) if eval_result.get("blocked") else None


def _entry_filter_evaluate(
    trade_rows: list[dict[str, str]],
    pair: str,
    context_lc: str,
    include_families: set[str],
    exclude_families: set[str],
    exclude_contexts_lc: set[str],
    min_profit_now_pips_by_bar: list[dict[str, Any]],
    min_progress_ratio_by_bar: list[dict[str, Any]],
    min_release_quality_by_bar: list[dict[str, Any]],
    max_noise_by_bar: list[dict[str, Any]],
    micro_confirm: dict[str, Any],
    include_pairs: set[str] | None = None,
    include_sessions: set[str] | None = None,
    family_specific_filters: dict[str, Any] | None = None,
    inferred_family: str | None = None,
) -> dict[str, Any]:
    fam = _infer_trade_family(trade_rows)
    if inferred_family:
        fam = str(inferred_family).upper().strip() or fam

    def _blocked(reason: str, final_blocker: str, gate_values: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "blocked": True,
            "reason": reason,
            "final_blocker": final_blocker,
            "gate_values": gate_values or {},
            "family": fam,
        }

    pair_u = str(pair).upper().strip()
    session_u = ""
    parts = [p for p in str(context_lc).split("__") if p]
    if len(parts) >= 3:
        session_u = str(parts[-1]).upper().strip()

    include_pairs_u = {str(x).upper().strip() for x in (include_pairs or set()) if str(x).strip()}
    include_sessions_u = {str(x).upper().strip() for x in (include_sessions or set()) if str(x).strip()}

    if include_pairs_u and pair_u not in include_pairs_u:
        return _blocked(
            "ENTRY_PAIR_NOT_INCLUDED",
            "scope_lock",
            {
                "pair": pair_u,
                "include_pairs": sorted(include_pairs_u),
            },
        )

    if include_sessions_u and session_u and session_u not in include_sessions_u:
        return _blocked(
            "ENTRY_SESSION_NOT_INCLUDED",
            "scope_lock",
            {
                "session": session_u,
                "include_sessions": sorted(include_sessions_u),
            },
        )

    if include_families and fam not in include_families:
        return _blocked(
            "ENTRY_FAMILY_NOT_INCLUDED",
            "family_policy",
            {
                "inferred_family": fam,
                "include_families": sorted(include_families),
            },
        )
    if fam in exclude_families:
        return _blocked(
            "ENTRY_FAMILY_EXCLUDED",
            "family_policy",
            {
                "inferred_family": fam,
                "exclude_families": sorted(exclude_families),
            },
        )

    if context_lc in exclude_contexts_lc:
        return _blocked(
            "ENTRY_CONTEXT_CUT",
            "family_policy",
            {
                "context": context_lc,
                "exclude_contexts": sorted(exclude_contexts_lc),
            },
        )

    family_cfg = {}
    if isinstance(family_specific_filters, dict):
        family_cfg = dict(family_specific_filters.get(fam, {}) or {})

    min_profit_rules = list(min_profit_now_pips_by_bar) + list(family_cfg.get("min_profit_now_pips_by_bar", []))
    min_progress_rules = list(min_progress_ratio_by_bar) + list(family_cfg.get("min_progress_ratio_by_bar", []))
    min_release_rules = list(min_release_quality_by_bar) + list(family_cfg.get("min_release_quality_by_bar", []))
    max_noise_rules = list(max_noise_by_bar) + list(family_cfg.get("max_noise_by_bar", []))

    if bool(micro_confirm.get("enabled", False)):
        majors = {str(x).upper().strip() for x in micro_confirm.get("major_pairs", [])} or {
            "EUR_USD",
            "GBP_USD",
            "USD_JPY",
            "USD_CHF",
            "USD_CAD",
            "AUD_USD",
            "NZD_USD",
        }
        is_major = str(pair).upper().strip() in majors
        confirm_push_pips = _safe_float(
            micro_confirm.get("confirm_push_pips_major" if is_major else "confirm_push_pips_wide", 0.6 if is_major else 0.8),
            0.6 if is_major else 0.8,
        )
        confirm_window_sec = _safe_float(
            micro_confirm.get("confirm_window_sec_major" if is_major else "confirm_window_sec_wide", 2.0 if is_major else 2.5),
            2.0 if is_major else 2.5,
        )
        observed_push = _max_profit_within_window(trade_rows, confirm_window_sec)
        if observed_push < confirm_push_pips:
            return _blocked(
                "ENTRY_MICRO_CONFIRM_FILTER",
                "confirmation",
                {
                    "confirm_window_sec": confirm_window_sec,
                    "confirm_push_required_pips": confirm_push_pips,
                    "confirm_push_observed_pips": observed_push,
                    "is_major_pair": is_major,
                },
            )

    for rule in min_profit_rules:
        bar_idx = _safe_int(rule.get("bar", 1), 1)
        row = _row_at_or_after_bar(trade_rows, bar_idx)
        min_pips = _safe_float(rule.get("min_pips", 0.0), 0.0)
        observed_pips = _safe_float(row.get("profit_now", 0.0), 0.0)
        if observed_pips < min_pips:
            return _blocked(
                "ENTRY_DISPLACEMENT_FILTER",
                "displacement",
                {
                    "bar": bar_idx,
                    "min_pips_required": min_pips,
                    "profit_now_pips_observed": observed_pips,
                },
            )

    for rule in min_progress_rules:
        bar_idx = _safe_int(rule.get("bar", 1), 1)
        row = _row_at_or_after_bar(trade_rows, bar_idx)
        min_ratio = _safe_float(rule.get("min_ratio", 0.0), 0.0)
        observed_ratio = _safe_float(row.get("progress_ratio", 0.0), 0.0)
        if observed_ratio < min_ratio:
            return _blocked(
                "ENTRY_PROGRESS_FILTER",
                "distance",
                {
                    "bar": bar_idx,
                    "min_progress_ratio_required": min_ratio,
                    "progress_ratio_observed": observed_ratio,
                },
            )

    for rule in min_release_rules:
        bar_idx = _safe_int(rule.get("bar", 1), 1)
        row = _row_at_or_after_bar(trade_rows, bar_idx)
        min_rq = _safe_float(rule.get("min_release_quality", 0.0), 0.0)
        observed_rq = _safe_float(row.get("release_quality", 0.0), 0.0)
        if observed_rq < min_rq:
            return _blocked(
                "ENTRY_VOLATILITY_FILTER",
                "break_cross",
                {
                    "bar": bar_idx,
                    "min_release_quality_required": min_rq,
                    "release_quality_observed": observed_rq,
                },
            )

    for rule in max_noise_rules:
        bar_idx = _safe_int(rule.get("bar", 1), 1)
        row = _row_at_or_after_bar(trade_rows, bar_idx)
        max_noise = _safe_float(rule.get("max_noise", 1.0), 1.0)
        observed_noise = _safe_float(row.get("noise", 1.0), 1.0)
        if observed_noise > max_noise:
            return _blocked(
                "ENTRY_NOISE_FILTER",
                "noise",
                {
                    "bar": bar_idx,
                    "max_noise_allowed": max_noise,
                    "noise_observed": observed_noise,
                },
            )

    return {
        "blocked": False,
        "reason": None,
        "final_blocker": None,
        "gate_values": {},
        "family": fam,
    }


def _expansion_gate_categories() -> list[str]:
    return [
        "break_cross",
        "confirmation",
        "displacement",
        "noise",
        "distance",
        "reclaim",
        "pullback",
        "macro_opposition",
        "family_policy",
        "other",
    ]


def _empty_gate_counts() -> dict[str, int]:
    return {k: 0 for k in _expansion_gate_categories()}


def _normalize_gate_bucket(raw_bucket: str | None, reason: str | None, family: str | None) -> str:
    bucket = str(raw_bucket or "").strip().lower()
    if bucket in _empty_gate_counts():
        return bucket

    fam = str(family or "").upper().strip()
    if fam == "RECLAIM_CONTINUATION":
        return "reclaim"
    if fam == "PULLBACK_CONTINUATION":
        return "pullback"

    reason_u = str(reason or "").upper().strip()
    if reason_u in {"ENTRY_MICRO_CONFIRM_FILTER"}:
        return "confirmation"
    if reason_u in {"ENTRY_DISPLACEMENT_FILTER"}:
        return "displacement"
    if reason_u in {"ENTRY_PROGRESS_FILTER"}:
        return "distance"
    if reason_u in {"ENTRY_VOLATILITY_FILTER"}:
        return "break_cross"
    if reason_u in {"ENTRY_NOISE_FILTER"}:
        return "noise"
    if reason_u in {"ENTRY_FAMILY_NOT_INCLUDED", "ENTRY_FAMILY_EXCLUDED", "ENTRY_CONTEXT_CUT"}:
        return "family_policy"
    return "other"


def _build_breakout_gate_report(
    records: list[dict[str, Any]],
    group_key: str | None = None,
) -> dict[str, Any]:
    accepted = sum(1 for r in records if bool(r.get("became_trade", False)))
    blocked_records = [r for r in records if not bool(r.get("became_trade", False))]
    blocked_total = len(blocked_records)

    gate_counts = _empty_gate_counts()
    reason_counts: Counter[str] = Counter()
    for rec in blocked_records:
        gate_counts[str(rec.get("block_gate_bucket", "other"))] += 1
        reason_counts[str(rec.get("block_reason", "UNKNOWN"))] += 1

    payload: dict[str, Any] = {
        "group": str(group_key) if group_key is not None else "ALL",
        "total_breakout_candidates": len(records),
        "total_accepted_breakout_trades": accepted,
        "total_blocked_breakout_candidates": blocked_total,
        "acceptance_rate": (accepted / len(records)) if records else 0.0,
        "block_count_by_gate": gate_counts,
        "block_pct_by_gate_of_candidates": {
            k: (v / len(records)) if records else 0.0 for k, v in gate_counts.items()
        },
        "block_pct_by_gate_of_blocked": {
            k: (v / blocked_total) if blocked_total else 0.0 for k, v in gate_counts.items()
        },
        "block_count_by_reason": dict(reason_counts),
    }
    return payload


def _mean(v: list[float]) -> float:
    return sum(v) / len(v) if v else 0.0


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one cost-aware AEE doctrine replay pass.")
    ap.add_argument("--config", default="aee_band_baseline_floor_v1.json")
    ap.add_argument("--stream-glob", action="append", default=[])
    ap.add_argument("--max-streams", type=int, default=24)
    ap.add_argument("--usd-per-pip", type=float, default=0.8)

    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--economic-viability-mult", type=float, default=1.10)

    ap.add_argument("--dataset-id", default="D")
    ap.add_argument("--deep-loss-cap", type=float, default=0.040)
    ap.add_argument("--epsilon-pips-per-hour", type=float, default=0.02)
    ap.add_argument("--ci-bootstrap-samples", type=int, default=400)
    ap.add_argument("--ci-seed", type=int, default=1337)
    ap.add_argument("--min-trades-for-ci", type=int, default=1000)

    ap.add_argument("--run-out", default="aee_baseline_floor_run.json")
    ap.add_argument("--dist-out", default="aee_baseline_distribution_report.json")
    ap.add_argument("--runbook-out", default="runbook.json")
    ap.add_argument("--candidate-table-out", default="candidate_table.json")
    ap.add_argument("--ci-report-out", default="ci_report.json")
    ap.add_argument("--final-decision-out", default="final_decision.json")
    ap.add_argument("--decision-log-out", default="")
    ap.add_argument("--expansion-breakout-bottleneck-out", default="expansion_breakout_bottleneck_report.json")
    ap.add_argument("--expansion-breakout-bottleneck-by-context-out", default="expansion_breakout_bottleneck_by_context.json")
    ap.add_argument("--expansion-breakout-bottleneck-by-pair-out", default="expansion_breakout_bottleneck_by_pair.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    extraction_cfg = cfg.get("extraction") or {}
    execution_price_mode = str(extraction_cfg.get("execution_price_mode", cfg.get("execution_price_mode", "side_aware"))).strip().lower()

    entry_filters = cfg.get("entry_filters") or {}
    include_families = {
        str(x).upper().strip()
        for x in (entry_filters.get("include_entry_families") or entry_filters.get("include_entry_types") or [])
        if str(x).strip()
    }
    exclude_families = {
        str(x).upper().strip()
        for x in (entry_filters.get("exclude_entry_families") or entry_filters.get("exclude_entry_types") or [])
        if str(x).strip()
    }
    exclude_contexts_lc = {str(x).lower().strip() for x in entry_filters.get("exclude_contexts", [])}
    include_pairs = {
        str(x).upper().strip()
        for x in entry_filters.get("include_pairs", [])
        if str(x).strip()
    }
    include_sessions = {
        str(x).upper().strip()
        for x in entry_filters.get("include_sessions", [])
        if str(x).strip()
    }
    min_profit_now_pips_by_bar = list(entry_filters.get("min_profit_now_pips_by_bar", []))
    min_progress_ratio_by_bar = list(entry_filters.get("min_progress_ratio_by_bar", []))
    min_release_quality_by_bar = list(entry_filters.get("min_release_quality_by_bar", []))
    max_noise_by_bar = list(entry_filters.get("max_noise_by_bar", []))
    micro_confirm = dict(entry_filters.get("micro_confirm", {}))
    family_specific_filters = dict(entry_filters.get("family_specific_filters", {}))

    friction_per_trade = (
        max(0.0, float(args.spread_pips))
        + (2.0 * max(0.0, float(args.slippage_pips_per_side)))
        + max(0.0, float(args.commission_pips_roundtrip))
        + max(0.0, float(args.latency_penalty_pips))
    )

    globs = args.stream_glob or [
        "compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_CHF__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/USD_CAD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_GBP__*/aee_stage/aee_state_stream/aee_state_stream.csv",
    ]

    streams: list[Path] = []
    for g in globs:
        streams.extend([p.resolve() for p in root.glob(g) if p.is_file()])
    streams = sorted(set(streams))[: max(1, int(args.max_streams))]
    if not streams:
        raise SystemExit("no streams found")

    stream_manifest = [str(p.relative_to(root)) for p in streams]
    stream_set_hash = _sha256_text("\n".join(stream_manifest))

    outcomes_aee: list[dict[str, Any]] = []
    outcomes_static: list[dict[str, Any]] = []
    trade_delta_pips: list[float] = []
    decision_logs: list[dict[str, Any]] = []
    entry_filter_counts = Counter()
    breakout_bottleneck_records: list[dict[str, Any]] = []

    reason_counts = Counter()
    hist_bins = Counter()
    hold_by_reason = defaultdict(list)
    best_r_by_reason = defaultdict(list)
    giveback_by_reason = defaultdict(list)

    total_hours = 0.0
    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        pair, day, session, context = _context_from_stream(root, sp)
        context_lc = context.lower()
        total_hours += _stream_duration_hours(rows)

        by_trade = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for trade_id, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue

            inferred_family = _infer_trade_family(trows)
            filter_eval = _entry_filter_evaluate(
                trows,
                pair,
                context_lc,
                include_families,
                exclude_families,
                exclude_contexts_lc,
                min_profit_now_pips_by_bar,
                min_progress_ratio_by_bar,
                min_release_quality_by_bar,
                max_noise_by_bar,
                micro_confirm,
                include_pairs=include_pairs,
                include_sessions=include_sessions,
                family_specific_filters=family_specific_filters,
                inferred_family=inferred_family,
            )
            filter_reason = str(filter_eval.get("reason")) if filter_eval.get("blocked") else None

            if inferred_family == "EXPANSION_BREAKOUT":
                breakout_bottleneck_records.append(
                    {
                        "trade_id": str(trade_id),
                        "stream": str(sp.relative_to(root)),
                        "pair": pair,
                        "day": day,
                        "session": session,
                        "context": context,
                        "family": inferred_family,
                        "became_trade": filter_reason is None,
                        "block_reason": filter_reason,
                        "final_blocker": filter_eval.get("final_blocker"),
                        "block_gate_bucket": _normalize_gate_bucket(
                            str(filter_eval.get("final_blocker") or ""),
                            filter_reason,
                            inferred_family,
                        ),
                        "decisive_gate_values": dict(filter_eval.get("gate_values") or {}),
                    }
                )

            if filter_reason is not None:
                entry_filter_counts[filter_reason] += 1
                decision_logs.append(
                    {
                        "trade_id": str(trade_id),
                        "stream": str(sp.relative_to(root)),
                        "pair": pair,
                        "context": context,
                        "current_profit_pips": 0.0,
                        "break_even_pips": 0.0,
                        "real_profit_pips": 0.0,
                        "best_profit_pips": 0.0,
                        "time_in_trade_sec": 0.0,
                        "time_since_last_improvement_sec": 0.0,
                        "giveback_pips": 0.0,
                        "green_armed": False,
                        "promoted": False,
                        "timeout_used_sec": 0.0,
                        "decision": "SKIP_ENTRY",
                        "reason": filter_reason,
                    }
                )
                continue

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=friction_per_trade,
                economic_value_margin_mult=float(args.economic_viability_mult),
                spread_fallback_pips=max(0.0, float(args.spread_pips)),
            )
            sta = _eval_trade_static(
                trows,
                execution_price_mode=execution_price_mode,
                spread_fallback_pips=max(0.0, float(args.spread_pips)),
            )

            outcomes_aee.append(aee)
            outcomes_static.append(sta)
            trade_delta_pips.append(float(aee["pips"] - sta["pips"]))

            decision_logs.append(
                {
                    "trade_id": str(trade_id),
                    "stream": str(sp.relative_to(root)),
                    "pair": pair,
                    "context": context,
                        "gross_realized_pips": float(aee.get("gross_pips", aee.get("pips", 0.0))),
                        "spread_adjusted_realized_pips": float(aee.get("net_spread_pips", aee.get("pips", 0.0))),
                        "friction_pips": float(friction_per_trade),
                        "net_realized_pips": float(aee.get("gross_pips", aee.get("pips", 0.0))) - float(friction_per_trade),
                    "current_profit_pips": float(aee.get("current_profit_pips", aee.get("pips", 0.0))),
                    "break_even_pips": float(aee.get("break_even_pips", 0.0)),
                    "real_profit_pips": float(aee.get("real_profit_pips", 0.0)),
                    "best_profit_pips": float(aee.get("best_profit_pips", 0.0)),
                    "time_in_trade_sec": float(aee.get("time_in_trade_sec", aee.get("hold_sec", 0.0))),
                    "time_since_last_improvement_sec": float(aee.get("time_since_last_improvement_sec", 0.0)),
                    "giveback_pips": float(aee.get("giveback_pips", 0.0)),
                    "green_armed": bool(aee.get("green_armed", False)),
                    "promoted": bool(aee.get("promoted", False)),
                    "timeout_used_sec": float(aee.get("timeout_used_sec", 0.0)),
                    "decision": str(aee.get("decision", "HOLD")),
                    "reason": str(aee.get("reason", "")),
                }
            )

            reason_counts[aee["reason"]] += 1
            hist_bins[_r_bin(aee["r"])] += 1
            hold_by_reason[aee["reason"]].append(aee["hold_sec"])
            best_r_by_reason[aee["reason"]].append(aee["best_r"])
            giveback_by_reason[aee["reason"]].append(aee.get("giveback_r", 0.0))

    n = max(1, len(outcomes_aee))
    total_hours = max(total_hours, 1.0 / 60.0)

    aee_pips = sum(x["pips"] for x in outcomes_aee)
    sta_pips = sum(x["pips"] for x in outcomes_static)
    aee_r_sum = sum(x["r"] for x in outcomes_aee)
    sta_r_sum = sum(x["r"] for x in outcomes_static)

    aee_pph = aee_pips / total_hours
    sta_pph = sta_pips / total_hours
    aee_rph = aee_r_sum / total_hours
    sta_rph = sta_r_sum / total_hours

    trades_per_hour = n / total_hours
    close_rate = trades_per_hour
    avg_hold = _mean([x["hold_sec"] for x in outcomes_aee])
    avg_r = _mean([x["r"] for x in outcomes_aee])
    avg_pips_per_trade = _mean([x["pips"] for x in outcomes_aee])

    sl_hit_rate = sum(1 for x in outcomes_aee if x["reason"] in {"AEE_PRE_SL_EXIT", "AEE_PANIC_EXIT", "AEE_BAND_FAST_FAILURE_EXIT"}) / n
    deep_loss_freq = sum(1 for x in outcomes_aee if x["r"] <= -1.0) / n
    pre_sl_save_rate = sum(1 for x in outcomes_aee if x["reason"] == "AEE_PRE_SL_EXIT") / n
    giveback_avoided_est = _mean([max(0.0, x["best_r"] - x["r"]) for x in outcomes_aee if x["r"] > 0])

    friction_pips_per_hour = friction_per_trade * trades_per_hour
    net_aee_pph = aee_pph - friction_pips_per_hour
    net_sta_pph = sta_pph - friction_pips_per_hour
    net_delta_pph = net_aee_pph - net_sta_pph

    viability_floor = friction_per_trade * max(1.0, float(args.economic_viability_mult))
    economic_viability_ok = avg_pips_per_trade >= viability_floor

    ci_pph = None
    if n >= int(args.min_trades_for_ci):
        ci_per_trade = _bootstrap_ci_mean(trade_delta_pips, int(args.ci_bootstrap_samples), int(args.ci_seed))
        if ci_per_trade is not None:
            ci_pph = (ci_per_trade[0] * trades_per_hour, ci_per_trade[1] * trades_per_hour)

    run_payload = {
        "generated_at": _iso_now(),
        "config": str(cfg_path),
        "streams_used": len(streams),
        "trade_count": n,
        "dataset_id": str(args.dataset_id),
        "window_duration_hr": total_hours,
        "gross_realized_pips_per_hour": aee_pph,
        "realized_pips_per_hour": aee_pph,
        "gross_static_pips_per_hour": sta_pph,
        "realized_r_per_hour": aee_rph,
        "realized_usd_per_hour": aee_pph * float(args.usd_per_pip),
        "close_cycle_capture_rate": close_rate,
        "capital_recycling_rate": close_rate,
        "trades_per_hour": trades_per_hour,
        "avg_pips_per_trade": avg_pips_per_trade,
        "avg_hold_sec": avg_hold,
        "avg_realized_r": avg_r,
        "sl_hit_rate": sl_hit_rate,
        "deep_loss_frequency": deep_loss_freq,
        "pre_sl_save_rate": pre_sl_save_rate,
        "reason_counts": dict(reason_counts),
        "entry_filter_counts": dict(entry_filter_counts),
        "giveback_avoided_estimate_r": giveback_avoided_est,
        "friction_model": {
            "spread_pips": float(args.spread_pips),
            "slippage_pips_per_side": float(args.slippage_pips_per_side),
            "commission_pips_roundtrip": float(args.commission_pips_roundtrip),
            "latency_penalty_pips": float(args.latency_penalty_pips),
            "friction_per_trade_pips": friction_per_trade,
            "friction_pips_per_hour": friction_pips_per_hour,
        },
        "execution_price_mode": execution_price_mode,
        "timeout_units": str((cfg.get("extraction") or {}).get("timeout_units", "legacy_minutes")),
        "net_realized_pips_per_hour": net_aee_pph,
        "net_static_pips_per_hour": net_sta_pph,
        "net_delta_realized_pips_per_hour": net_delta_pph,
        "cost_model": {
            "canonical_trade_field": "gross_pips",
            "aggregate_net_formula": "net_realized_pips_per_hour = gross_realized_pips_per_hour - friction_pips_per_hour",
            "spread_fallback_source": "spread_pips",
        },
        "economic_viability": {
            "avg_pips_per_trade": avg_pips_per_trade,
            "required_min_pips_per_trade": viability_floor,
            "multiplier": float(args.economic_viability_mult),
            "ok": economic_viability_ok,
        },
        "confidence": {
            "bootstrap_samples": int(args.ci_bootstrap_samples),
            "min_trades_for_ci": int(args.min_trades_for_ci),
            "delta_pips_per_hour_95ci": [ci_pph[0], ci_pph[1]] if ci_pph else None,
        },
        "baseline_delta": {
            "delta_realized_pips_per_hour": aee_pph - sta_pph,
            "delta_realized_r_per_hour": aee_rph - sta_rph,
            "delta_avg_hold_sec": avg_hold - _mean([x["hold_sec"] for x in outcomes_static]),
        },
    }

    pos_bins = hist_bins["0<R<=0.15"] + hist_bins["0.15<R<=0.4"] + hist_bins["0.4<R<=1.0"]
    neg_bins = hist_bins["-1.0<R<=-0.4"] + hist_bins["R<=-1.0"]

    dist_ok = pos_bins >= neg_bins
    extraction_ok = run_payload["net_delta_realized_pips_per_hour"] > 0.0
    throughput_ok = close_rate > 0.0 and avg_hold <= (_mean([x["hold_sec"] for x in outcomes_static]) * 1.1)
    defensive_ok = deep_loss_freq <= float(args.deep_loss_cap)
    ci_ok = (ci_pph is not None and ci_pph[0] > -abs(float(args.epsilon_pips_per_hour)))

    stall_fires = (
        reason_counts["AEE_CONTINUATION_FAILED_EXIT"]
        + reason_counts["AEE_GIVEBACK_EXIT"]
        + reason_counts["AEE_BAND_POST_ESCAPE_PROFIT_STALL_EXIT"]
        + reason_counts["AEE_BAND_MEANINGFUL_PROFIT_STALL_EXIT"]
        + reason_counts["AEE_BAND_PROFIT_STALL_EXIT"]
    )
    fallback_fires = (
        reason_counts["AEE_NEVER_GREEN_TIMEOUT"]
        + reason_counts["AEE_WEAK_GREEN_TIMEOUT"]
        + reason_counts["AEE_BAND_POST_ESCAPE_FALLBACK_EXIT"]
        + reason_counts["AEE_BAND_MEANINGFUL_FALLBACK_EXIT"]
        + reason_counts["AEE_BAND_FALLBACK_EXIT"]
    )
    profit_branch_fire_ok = stall_fires > 0 and fallback_fires > 0

    verdict = "BASELINE_ACCEPTED" if (
        extraction_ok and dist_ok and throughput_ok and defensive_ok and profit_branch_fire_ok and economic_viability_ok and ci_ok
    ) else "BASELINE_REJECTED"
    run_payload["verdict"] = verdict
    run_payload["gate_checks"] = {
        "extraction_ok": extraction_ok,
        "distribution_ok": dist_ok,
        "throughput_ok": throughput_ok,
        "defensive_ok": defensive_ok,
        "profit_branch_fire_ok": profit_branch_fire_ok,
        "economic_viability_ok": economic_viability_ok,
        "ci_ok": ci_ok,
    }

    evaluation_script_hash = _sha256_file(Path(__file__).resolve())
    config_hash = _sha256_file(cfg_path)
    friction_model_hash = _sha256_text(json.dumps(run_payload["friction_model"], sort_keys=True))

    run_payload["reproducibility"] = {
        "code_hash": evaluation_script_hash,
        "evaluation_script_hash": evaluation_script_hash,
        "config_hash": config_hash,
        "stream_set_hash": stream_set_hash,
        "run_timestamp": run_payload["generated_at"],
        "friction_model_hash": friction_model_hash,
        "dataset_id": str(args.dataset_id),
        "stream_manifest": stream_manifest,
    }

    dist_payload = {
        "generated_at": _iso_now(),
        "trade_count": n,
        "dataset_id": str(args.dataset_id),
        "histogram_r_bins": {
            "R<=-1.0": hist_bins["R<=-1.0"],
            "-1.0<R<=-0.4": hist_bins["-1.0<R<=-0.4"],
            "-0.4<R<=0": hist_bins["-0.4<R<=0"],
            "0<R<=0.15": hist_bins["0<R<=0.15"],
            "0.15<R<=0.4": hist_bins["0.15<R<=0.4"],
            "0.4<R<=1.0": hist_bins["0.4<R<=1.0"],
            "R>1.0": hist_bins["R>1.0"],
        },
        "reason_code_counts": dict(reason_counts),
        "entry_filter_counts": dict(entry_filter_counts),
        "avg_hold_sec_by_reason": {k: _mean(v) for k, v in hold_by_reason.items()},
        "avg_best_favorable_r_by_reason": {k: _mean(v) for k, v in best_r_by_reason.items()},
        "avg_giveback_r_before_exit_by_reason": {k: _mean(v) for k, v in giveback_by_reason.items()},
        "branch_close_share": {k: (v / n) for k, v in reason_counts.items()},
    }

    run_out = Path(args.run_out)
    if not run_out.is_absolute():
        run_out = (root / run_out).resolve()
    dist_out = Path(args.dist_out)
    if not dist_out.is_absolute():
        dist_out = (root / dist_out).resolve()

    runbook_out = Path(args.runbook_out)
    if not runbook_out.is_absolute():
        runbook_out = (root / runbook_out).resolve()
    candidate_table_out = Path(args.candidate_table_out)
    if not candidate_table_out.is_absolute():
        candidate_table_out = (root / candidate_table_out).resolve()
    ci_report_out = Path(args.ci_report_out)
    if not ci_report_out.is_absolute():
        ci_report_out = (root / ci_report_out).resolve()
    final_decision_out = Path(args.final_decision_out)
    if not final_decision_out.is_absolute():
        final_decision_out = (root / final_decision_out).resolve()

    breakout_bottleneck_out = Path(args.expansion_breakout_bottleneck_out)
    if not breakout_bottleneck_out.is_absolute():
        breakout_bottleneck_out = (root / breakout_bottleneck_out).resolve()
    breakout_bottleneck_by_context_out = Path(args.expansion_breakout_bottleneck_by_context_out)
    if not breakout_bottleneck_by_context_out.is_absolute():
        breakout_bottleneck_by_context_out = (root / breakout_bottleneck_by_context_out).resolve()
    breakout_bottleneck_by_pair_out = Path(args.expansion_breakout_bottleneck_by_pair_out)
    if not breakout_bottleneck_by_pair_out.is_absolute():
        breakout_bottleneck_by_pair_out = (root / breakout_bottleneck_by_pair_out).resolve()

    decision_log_out = None
    if str(args.decision_log_out).strip():
        decision_log_out = Path(str(args.decision_log_out).strip())
        if not decision_log_out.is_absolute():
            decision_log_out = (root / decision_log_out).resolve()

    runbook_payload = {
        "generated_at": _iso_now(),
        "mission_lock": "optimize_only_net_realized_extraction_per_hour",
        "dataset_id": str(args.dataset_id),
        "thresholds": {
            "promotion_net_delta_pph_gt": 0.0,
            "deep_loss_cap": float(args.deep_loss_cap),
            "epsilon_pips_per_hour": float(args.epsilon_pips_per_hour),
            "economic_viability_mult": float(args.economic_viability_mult),
        },
        "friction_model": run_payload["friction_model"],
        "reproducibility": run_payload["reproducibility"],
    }

    candidate_table_payload = {
        "generated_at": _iso_now(),
        "candidates": [
            {
                "name": cfg.get("name", cfg_path.name),
                "dataset_id": str(args.dataset_id),
                "config": str(cfg_path),
                "trade_count": n,
                "net_extraction_per_hour": net_aee_pph,
                "net_delta_extraction_per_hour": net_delta_pph,
                "gross_delta_extraction_per_hour": run_payload["baseline_delta"]["delta_realized_pips_per_hour"],
                "verdict": verdict,
            }
        ],
    }

    ci_report_payload = {
        "generated_at": _iso_now(),
        "candidate": cfg.get("name", cfg_path.name),
        "dataset_id": str(args.dataset_id),
        "trade_count": n,
        "bootstrap_samples": int(args.ci_bootstrap_samples),
        "mean_delta_pips_per_hour": net_delta_pph,
        "delta_95ci_pips_per_hour": [ci_pph[0], ci_pph[1]] if ci_pph else None,
        "epsilon_pips_per_hour": float(args.epsilon_pips_per_hour),
        "ci_ok": ci_ok,
    }

    final_decision_payload = {
        "generated_at": _iso_now(),
        "decision": "PROMOTE" if verdict == "BASELINE_ACCEPTED" else "REJECT",
        "candidate": cfg.get("name", cfg_path.name),
        "dataset_id": str(args.dataset_id),
        "metrics": {
            "net_delta_realized_pips_per_hour": net_delta_pph,
            "gross_delta_realized_pips_per_hour": run_payload["baseline_delta"]["delta_realized_pips_per_hour"],
            "deep_loss_frequency": deep_loss_freq,
            "avg_pips_per_trade": avg_pips_per_trade,
            "required_min_pips_per_trade": viability_floor,
            "friction_per_trade_pips": friction_per_trade,
        },
        "gate_checks": run_payload["gate_checks"],
        "reproducibility": run_payload["reproducibility"],
    }

    breakout_overall = _build_breakout_gate_report(breakout_bottleneck_records)
    by_context: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    by_pair: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in breakout_bottleneck_records:
        by_context[str(rec.get("context", "UNKNOWN"))].append(rec)
        by_pair[str(rec.get("pair", "UNKNOWN"))].append(rec)

    breakout_overall_payload = {
        "generated_at": _iso_now(),
        "dataset_id": str(args.dataset_id),
        "config": str(cfg_path),
        "family": "EXPANSION_BREAKOUT",
        "goal": "identify binding gate limiting strict-baseline EXPANSION_BREAKOUT throughput",
        "summary": breakout_overall,
        "candidate_events": breakout_bottleneck_records,
    }
    breakout_by_context_payload = {
        "generated_at": _iso_now(),
        "dataset_id": str(args.dataset_id),
        "family": "EXPANSION_BREAKOUT",
        "groups": {
            ctx: _build_breakout_gate_report(ctx_records, group_key=ctx)
            for ctx, ctx_records in sorted(by_context.items())
        },
    }
    breakout_by_pair_payload = {
        "generated_at": _iso_now(),
        "dataset_id": str(args.dataset_id),
        "family": "EXPANSION_BREAKOUT",
        "groups": {
            p: _build_breakout_gate_report(pair_records, group_key=p)
            for p, pair_records in sorted(by_pair.items())
        },
    }

    run_out.write_text(json.dumps(run_payload, indent=2) + "\n", encoding="utf-8")
    dist_out.write_text(json.dumps(dist_payload, indent=2) + "\n", encoding="utf-8")
    runbook_out.write_text(json.dumps(runbook_payload, indent=2) + "\n", encoding="utf-8")
    candidate_table_out.write_text(json.dumps(candidate_table_payload, indent=2) + "\n", encoding="utf-8")
    ci_report_out.write_text(json.dumps(ci_report_payload, indent=2) + "\n", encoding="utf-8")
    final_decision_out.write_text(json.dumps(final_decision_payload, indent=2) + "\n", encoding="utf-8")
    breakout_bottleneck_out.write_text(json.dumps(breakout_overall_payload, indent=2) + "\n", encoding="utf-8")
    breakout_bottleneck_by_context_out.write_text(json.dumps(breakout_by_context_payload, indent=2) + "\n", encoding="utf-8")
    breakout_bottleneck_by_pair_out.write_text(json.dumps(breakout_by_pair_payload, indent=2) + "\n", encoding="utf-8")
    if decision_log_out is not None:
        decision_log_out.write_text(
            "\n".join(json.dumps(x, ensure_ascii=True) for x in decision_logs) + "\n",
            encoding="utf-8",
        )

    print(
        json.dumps(
            {
                "run_out": str(run_out),
                "dist_out": str(dist_out),
                "runbook_out": str(runbook_out),
                "candidate_table_out": str(candidate_table_out),
                "ci_report_out": str(ci_report_out),
                "final_decision_out": str(final_decision_out),
                "expansion_breakout_bottleneck_out": str(breakout_bottleneck_out),
                "expansion_breakout_bottleneck_by_context_out": str(breakout_bottleneck_by_context_out),
                "expansion_breakout_bottleneck_by_pair_out": str(breakout_bottleneck_by_pair_out),
                "decision_log_out": str(decision_log_out) if decision_log_out is not None else None,
                "verdict": verdict,
                "trade_count": n,
                "streams_used": len(streams),
                "gross_delta_realized_pips_per_hour": run_payload["baseline_delta"]["delta_realized_pips_per_hour"],
                "net_delta_realized_pips_per_hour": run_payload["net_delta_realized_pips_per_hour"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
