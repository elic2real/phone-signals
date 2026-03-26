#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import statistics


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _to_float(value: Any) -> float | None:
    try:
        num = float(value)
    except Exception:
        return None
    return num


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    num = _to_float(value)
    if num is not None:
        return num
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _row_ts(row: dict[str, Any]) -> float | None:
    for key in ("ts", "ts_utc", "timestamp", "time"):
        ts = _parse_ts(row.get(key))
        if ts is not None:
            return ts
    return None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    v = sorted(values)
    qq = max(0.0, min(1.0, float(q)))
    idx = qq * (len(v) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(v) - 1)
    frac = idx - lo
    return v[lo] * (1.0 - frac) + v[hi] * frac


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades-log", default="logs/trades.jsonl")
    ap.add_argument("--runtime-log", default="logs/runtime.log")
    ap.add_argument("--out", default="")
    ap.add_argument("--window-minutes", type=float, default=0.0)
    args = ap.parse_args()

    trades = Path(args.trades_log)
    runtime = Path(args.runtime_log)
    if not trades.exists():
        raise SystemExit(f"TRADES_LOG_MISSING: {trades}")
    if not runtime.exists():
        raise SystemExit(f"RUNTIME_LOG_MISSING: {runtime}")

    tune_levels: Counter[str] = Counter()
    tune_keys: Counter[str] = Counter()
    entry_gate_eval = 0
    aee_exits = 0
    fallback_events = 0
    hold_blocked_events = 0

    exit_total = 0
    runner_exits = 0
    green_exits = 0
    green_to_red_leakage = 0
    profit_exit_count = 0
    sum_pnl_pips = 0.0
    sum_positive_pnl_from_green = 0.0
    sum_mfe_pips_from_green = 0.0
    exit_ts_values: list[float] = []

    handoff_first_green: dict[int, float] = {}
    latency_sec_values: list[float] = []
    immediate_green_sec = 2.0
    capture_ratio_values: list[float] = []

    now_epoch = datetime.now(timezone.utc).timestamp()
    window_start = (now_epoch - (float(args.window_minutes) * 60.0)) if float(args.window_minutes) > 0.0 else None

    profit_exit_reasons = {
        "PROFIT_CAPTURE_EXIT",
        "EXTRACTION_LOSS_EXIT",
        "HARVEST_EXIT",
        "STALL_EXIT",
        "AEE_PROFIT_CAPTURE_EXIT",
    }

    for row in _iter_jsonl(trades):
        row_epoch = _row_ts(row)
        if window_start is not None and row_epoch is not None and row_epoch < window_start:
            continue

        kind = str(row.get("kind", "") or row.get("event_type", "") or "")
        if kind == "TUNE_MATCH":
            lvl = str(row.get("matched_level", "") or "UNKNOWN")
            key = str(row.get("matched_key", "") or "UNKNOWN")
            tune_levels[lvl] += 1
            tune_keys[key] += 1
        if kind == "ENTRY_GATE_EVAL":
            entry_gate_eval += 1
        if kind == "AEE_EXIT_SNAPSHOT_POST":
            aee_exits += 1
        if "FALLBACK" in kind:
            fallback_events += 1
        if kind == "AEE_PROFIT_EXIT_HOLD_BLOCKED":
            hold_blocked_events += 1

        if kind == "AEE_HANDOFF_TRACE":
            trade_id = int(row.get("trade_id", 0) or 0)
            if trade_id > 0:
                fg = _parse_ts(row.get("first_green_ts"))
                if fg is not None and trade_id not in handoff_first_green:
                    handoff_first_green[trade_id] = fg

        if kind == "EXIT_RESULT":
            exit_total += 1
            trade_id = int(row.get("trade_id", 0) or 0)
            leg_type = str(row.get("leg_type", "") or "").upper()
            if "RUN" in leg_type:
                runner_exits += 1

            pnl_pips = float(row.get("pnl_pips", 0.0) or 0.0)
            mfe_pips = float(row.get("mfe_pips", 0.0) or 0.0)
            sum_pnl_pips += pnl_pips
            if row_epoch is not None:
                exit_ts_values.append(row_epoch)

            if mfe_pips > 0.0:
                green_exits += 1
                if pnl_pips < 0.0:
                    green_to_red_leakage += 1
                sum_positive_pnl_from_green += max(0.0, pnl_pips)
                sum_mfe_pips_from_green += mfe_pips
                capture_ratio_values.append(max(0.0, pnl_pips) / mfe_pips)

            exit_reason = str(row.get("exit_reason", "") or row.get("aee_reason", "") or "")
            if exit_reason in profit_exit_reasons or (mfe_pips > 0.0 and pnl_pips > 0.0):
                profit_exit_count += 1

            fg = handoff_first_green.get(trade_id)
            if fg is not None and row_epoch is not None and row_epoch >= fg:
                latency_sec_values.append(row_epoch - fg)

    text = runtime.read_text(encoding="utf-8", errors="ignore")
    for pat in (r"\bFALLBACK\b", r"SIZING_META_FALLBACK_USED", r"EMPTY_SCAN_FALLBACK"):
        fallback_events += len(re.findall(pat, text))
    aee_decisions = len(re.findall(r"\bAEE_DECISION\b", text))
    enters = len(re.findall(r"\bENTER\b", text))
    active_rows = len(re.findall(r"\bACTIVE_ARTIFACT\b", text))

    total_signal = max(1, entry_gate_eval + enters + aee_decisions)
    fallback_rate = fallback_events / float(total_signal)

    leakage_rate = (green_to_red_leakage / float(green_exits)) if green_exits > 0 else 0.0
    runner_share = (runner_exits / float(exit_total)) if exit_total > 0 else 0.0
    profit_capture_efficiency = (
        (sum_positive_pnl_from_green / float(sum_mfe_pips_from_green)) if sum_mfe_pips_from_green > 0.0 else 0.0
    )
    avg_capture_ratio = float(statistics.mean(capture_ratio_values)) if capture_ratio_values else 0.0
    median_capture_ratio = float(statistics.median(capture_ratio_values)) if capture_ratio_values else 0.0

    avg_first_green_to_exit_sec = (sum(latency_sec_values) / float(len(latency_sec_values))) if latency_sec_values else None
    p50_first_green_to_exit_sec = _percentile(latency_sec_values, 0.50)
    p90_first_green_to_exit_sec = _percentile(latency_sec_values, 0.90)
    immediate_green_capture_count = sum(1 for v in latency_sec_values if v <= immediate_green_sec)

    if len(exit_ts_values) >= 2:
        span_sec = max(exit_ts_values) - min(exit_ts_values)
    elif float(args.window_minutes) > 0.0:
        span_sec = float(args.window_minutes) * 60.0
    else:
        span_sec = 0.0
    extraction_per_hour = (sum_pnl_pips / (span_sec / 3600.0)) if span_sec > 0.0 else 0.0

    out_path = Path(args.out) if args.out else Path("proof_artifacts") / f"RUNTIME_PROOF_{_now_tag()}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append(f"# Runtime Proof {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("## Inputs")
    lines.append(f"- trades_log: `{trades}`")
    lines.append(f"- runtime_log: `{runtime}`")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- ACTIVE_ARTIFACT lines: `{active_rows}`")
    lines.append(f"- ENTRY_GATE_EVAL count: `{entry_gate_eval}`")
    lines.append(f"- ENTER count (runtime.log): `{enters}`")
    lines.append(f"- AEE_DECISION count (runtime.log): `{aee_decisions}`")
    lines.append(f"- AEE exit snapshot count (trades): `{aee_exits}`")
    lines.append(f"- fallback_events: `{fallback_events}`")
    lines.append(f"- fallback_rate: `{fallback_rate:.6f}`")
    lines.append(f"- EXIT_RESULT count: `{exit_total}`")
    lines.append(f"- runner_share: `{runner_share:.4f}` ({runner_exits}/{exit_total if exit_total > 0 else 1})")
    lines.append(f"- green_to_red_roundtrip: `{green_to_red_leakage}`")
    lines.append(f"- green_to_red_roundtrip_rate: `{leakage_rate:.4f}` ({green_to_red_leakage}/{green_exits if green_exits > 0 else 1})")
    lines.append(f"- profit_capture_efficiency: `{profit_capture_efficiency:.4f}`")
    lines.append(f"- extraction_efficiency_avg_capture_ratio: `{avg_capture_ratio:.4f}`")
    lines.append(f"- extraction_efficiency_median_capture_ratio: `{median_capture_ratio:.4f}`")
    lines.append(f"- profit_exit_count: `{profit_exit_count}`")
    if avg_first_green_to_exit_sec is None:
        lines.append("- first_green_to_exit_latency_sec_avg: `n/a` (missing first_green_ts or exit timestamps)")
        lines.append("- first_green_to_exit_latency_sec_p50: `n/a`")
        lines.append("- first_green_to_exit_latency_sec_p90: `n/a`")
    else:
        lines.append(f"- first_green_to_exit_latency_sec_avg: `{avg_first_green_to_exit_sec:.3f}`")
        lines.append(f"- first_green_to_exit_latency_sec_p50: `{float(p50_first_green_to_exit_sec or 0.0):.3f}`")
        lines.append(f"- first_green_to_exit_latency_sec_p90: `{float(p90_first_green_to_exit_sec or 0.0):.3f}`")
    lines.append(f"- immediate_green_capture_count (<= {immediate_green_sec:.1f}s): `{immediate_green_capture_count}`")
    lines.append(f"- extraction_per_hour_pips: `{extraction_per_hour:.4f}`")
    lines.append(f"- profit_exits_blocked_by_hold_timer: `{hold_blocked_events}`")
    lines.append(f"- doctrine_assert_profit_hold_blocks_zero: `{'PASS' if hold_blocked_events == 0 else 'FAIL'}`")
    lines.append("")
    lines.append("## TUNE_MATCH Levels")
    if tune_levels:
        for k, v in tune_levels.most_common():
            lines.append(f"- {k}: `{v}`")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Top Matched Keys")
    if tune_keys:
        for k, v in tune_keys.most_common(20):
            lines.append(f"- `{k}`: `{v}`")
    else:
        lines.append("- none")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
