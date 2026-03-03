#!/usr/bin/env python3
import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def load_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def infer_run_bounds(trades: List[dict], metrics: List[dict], bot_log: str) -> tuple[Optional[datetime], Optional[datetime]]:
    cands: List[datetime] = []
    for row in trades + metrics:
        ts = parse_ts(row.get("ts"))
        if ts:
            cands.append(ts.astimezone(timezone.utc))
    if cands:
        return min(cands), max(cands)
    m = re.findall(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", bot_log)
    if m:
        try:
            dts = [datetime.strptime(x, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) for x in m]
            return min(dts), max(dts)
        except Exception:
            pass
    return None, None


def extract_trade_kpis(trades: List[dict], bot_log: str) -> Dict[str, Any]:
    signals = [t for t in trades if str(t.get("event")) == "SIGNAL_GENERATED"]
    attempts = [t for t in trades if str(t.get("event")) == "TRADE_ATTEMPT"]
    fills = [
        t for t in trades
        if str(t.get("event")) in {"ORDER_FILLED", "TRADE_FILLED", "FILL", "POSITION_OPENED", "TRADE_CLOSED"}
        or "filled" in str(t.get("event", "")).lower()
    ]

    # Attrition from structured trade events + log mentions.
    reject_reason_counts = Counter()
    for t in attempts:
        r = str(t.get("reason") or t.get("reason_code") or "")
        if r:
            reject_reason_counts[r] += 1

    # Extract exit reasons / pips / durations from structured rows if present.
    exit_reason_counts = Counter()
    closed_pips: List[float] = []
    durations_sec: List[float] = []
    realized_pnl: List[float] = []
    for t in trades:
        ev = str(t.get("event", "")).upper()
        if "CLOSE" in ev or "EXIT" in ev:
            for k in ("exit_reason", "reason_code", "reason"):
                if t.get(k):
                    exit_reason_counts[str(t[k])] += 1
                    break
        for k in ("pips", "pnl_pips", "pips_realized", "realized_pips"):
            if k in t:
                try:
                    closed_pips.append(float(t[k]))
                    break
                except Exception:
                    pass
        for k in ("duration_sec", "hold_sec", "age_sec"):
            if k in t:
                try:
                    durations_sec.append(float(t[k]))
                    break
                except Exception:
                    pass
        for k in ("realized_pnl", "pnl", "profit", "equity_delta"):
            if k in t:
                try:
                    realized_pnl.append(float(t[k]))
                    break
                except Exception:
                    pass

    # Fallback parse from bot.log for exit reasons and pips.
    if bot_log:
        for m in re.findall(r"\b(PANIC|NEAR_TP_STALL|TIME_EXIT|TTL_[A-Z_]+|TTL_TAKE_PROFIT|TTL_NO_FOLLOWTHROUGH)\b", bot_log):
            exit_reason_counts[m] += 1
        for m in re.findall(r"\b(?:pips|Pips)[=: ]+(-?\d+(?:\.\d+)?)", bot_log):
            try:
                closed_pips.append(float(m))
            except Exception:
                pass
        for m in re.findall(r"\b(?:duration|hold_sec|age_sec)[=: ]+(\d+(?:\.\d+)?)", bot_log):
            try:
                durations_sec.append(float(m))
            except Exception:
                pass

    return {
        "signals_generated": len(signals),
        "trade_attempts": len(attempts),
        "fills_or_closes_detected": len(fills),
        "reject_reason_counts": dict(reject_reason_counts.most_common()),
        "exit_reason_counts": dict(exit_reason_counts.most_common()),
        "closed_pips_samples": closed_pips,
        "durations_sec_samples": durations_sec,
        "realized_pnl_samples": realized_pnl,
    }


def extract_metrics_kpis(metrics: List[dict]) -> Dict[str, Any]:
    mpe_hr_series = []
    eg_hr_series = []
    for m in metrics:
        for k in ("mpe_per_hour", "mpe_hr", "current_mpe_hr"):
            if k in m:
                try:
                    mpe_hr_series.append(float(m[k]))
                    break
                except Exception:
                    pass
        for k in ("eg_per_hour", "equity_gain_per_hour", "eg_hr"):
            if k in m:
                try:
                    eg_hr_series.append(float(m[k]))
                    break
                except Exception:
                    pass
    return {
        "mpe_hr_series": mpe_hr_series,
        "eg_hr_series": eg_hr_series,
    }


def summarize(run_start: Optional[datetime], run_end: Optional[datetime], trade_kpis: Dict[str, Any], metric_kpis: Dict[str, Any], audit_summaries: List[dict]) -> Dict[str, Any]:
    duration_hours = None
    if run_start and run_end and run_end > run_start:
        duration_hours = (run_end - run_start).total_seconds() / 3600.0

    signals = trade_kpis["signals_generated"]
    fills = trade_kpis["fills_or_closes_detected"]
    friction_attrition_rate = None
    if signals > 0:
        friction_attrition_rate = max(0.0, 1.0 - (fills / signals))

    pips = trade_kpis["closed_pips_samples"]
    pnls = trade_kpis["realized_pnl_samples"]
    durs = trade_kpis["durations_sec_samples"]

    # KPI 1 MPE/H
    mpe_h = None
    if pips and duration_hours and duration_hours > 0:
        mpe_h = sum(pips) / duration_hours
    elif metric_kpis["mpe_hr_series"]:
        mpe_h = metric_kpis["mpe_hr_series"][-1]

    # KPI 2 EG/H
    eg_h = None
    if pnls and duration_hours and duration_hours > 0:
        eg_h = sum(pnls) / duration_hours
    elif metric_kpis["eg_hr_series"]:
        eg_h = metric_kpis["eg_hr_series"][-1]

    avg_trade_duration_sec = (sum(durs) / len(durs)) if durs else None

    # Tail attrition hints from monitor snapshots.
    tail_reject_totals = Counter()
    for row in audit_summaries:
        if row.get("kind") in {"cadence_snapshot", "hourly_summary"}:
            tail_reject_totals.update((row.get("reject_mentions_tail") or {}))

    bottleneck = []
    if mpe_h is not None and mpe_h < 150:
        bottleneck.append("MPE/H below target (150)")
    if tail_reject_totals.get("FRICTION_NOT_COVERED", 0) > 0:
        bottleneck.append("Friction attrition observed (FRICTION_NOT_COVERED)")
    if tail_reject_totals.get("SPREAD_GATE", 0) > 0:
        bottleneck.append("Spread gate attrition observed (SPREAD_GATE)")

    return {
        "run_start_utc": run_start.isoformat() if run_start else None,
        "run_end_utc": run_end.isoformat() if run_end else None,
        "run_duration_hours": duration_hours,
        "volume_density": {
            "signals_generated": signals,
            "fills_or_closes_detected": fills,
            "trade_attempts": trade_kpis["trade_attempts"],
            "friction_attrition_rate": friction_attrition_rate,
        },
        "kpi_mpe_per_hour": mpe_h,
        "kpi_eg_per_hour": eg_h,
        "aee_phase_survival_exit_breakdown": trade_kpis["exit_reason_counts"],
        "average_trade_duration_sec": avg_trade_duration_sec,
        "tail_reject_mentions_aggregate": dict(tail_reject_totals),
        "bottleneck_report": bottleneck,
        "data_quality_notes": {
            "pips_samples_detected": len(pips),
            "pnl_samples_detected": len(pnls),
            "duration_samples_detected": len(durs),
            "mpe_hr_series_points": len(metric_kpis["mpe_hr_series"]),
            "eg_hr_series_points": len(metric_kpis["eg_hr_series"]),
        },
    }


def main():
    ap = argparse.ArgumentParser(description="Post-run KPI extraction for 4-hour phone_bot audit")
    ap.add_argument("--run-dir", required=True, help="Run directory from run_4h_audit.sh")
    ap.add_argument("--bot-log", default=None, help="Optional explicit bot.log path")
    ap.add_argument("--trades", default=None, help="Optional explicit trades.jsonl path")
    ap.add_argument("--metrics", default=None, help="Optional explicit metrics.jsonl path")
    ap.add_argument("--out", default=None, help="Optional output JSON path (default: <run-dir>/kpi_report.json)")
    args = ap.parse_args()

    run_dir = args.run_dir
    bot_log_path = args.bot_log or os.path.join(run_dir, "bot.log")
    trades_path = args.trades or os.path.join(run_dir, "trades.jsonl")
    metrics_path = args.metrics or os.path.join(run_dir, "metrics.jsonl")
    audit_summaries_path = os.path.join(run_dir, "audit_summaries.jsonl")
    out_path = args.out or os.path.join(run_dir, "kpi_report.json")

    trades = list(iter_jsonl(trades_path))
    metrics = list(iter_jsonl(metrics_path))
    audit_summaries = list(iter_jsonl(audit_summaries_path))
    bot_log = load_text(bot_log_path)

    run_start, run_end = infer_run_bounds(trades, metrics, bot_log)
    trade_kpis = extract_trade_kpis(trades, bot_log)
    metric_kpis = extract_metrics_kpis(metrics)
    report = summarize(run_start, run_end, trade_kpis, metric_kpis, audit_summaries)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\n[WROTE] {out_path}")


if __name__ == "__main__":
    main()
