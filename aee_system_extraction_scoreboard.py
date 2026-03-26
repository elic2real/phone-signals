#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_payload(line: str, marker: str) -> dict[str, Any] | None:
    m = re.search(rf"{re.escape(marker)}\s*\|\s*(\{{.*\}})", line)
    if not m:
        return None
    try:
        obj = ast.literal_eval(m.group(1))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _line_ts_epoch(line: str) -> float | None:
    m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})", line)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.timestamp() + (int(m.group(2)) / 1000.0)
    except Exception:
        return None


def compute_scoreboard(log_path: Path) -> dict[str, Any]:
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()

    periodic_rows: list[dict[str, Any]] = []
    branch_rows: list[dict[str, Any]] = []
    exit_rows: list[dict[str, Any]] = []

    for line in lines:
        p = _extract_payload(line, "AEE_PERIODIC_DECISION")
        if p is not None:
            periodic_rows.append(p)
            continue
        b = _extract_payload(line, "AEE_BRANCH_TRIGGER")
        if b is not None:
            branch_rows.append(b)
            continue
        x = _extract_payload(line, "EXIT_RESULT")
        if x is not None:
            exit_rows.append(x)
            continue

    ts_vals = [_line_ts_epoch(line) for line in lines]
    ts_vals = [t for t in ts_vals if t is not None]
    if ts_vals:
        duration_sec = max(1.0, float(max(ts_vals) - min(ts_vals)))
    else:
        # Fallback keeps rate metrics defined even if no timestamped lines are present.
        duration_sec = max(1.0, _safe_float(log_path.stat().st_mtime - log_path.stat().st_ctime, 1.0))
    duration_hr = duration_sec / 3600.0

    exit_decisions = [r for r in periodic_rows if str(r.get("exit_reason", "") or "").strip()]
    exit_reason_counts = Counter(str(r.get("exit_reason", "") or "") for r in exit_decisions)

    hold_vals = [_safe_float(r.get("hold_sec")) for r in exit_decisions]
    realized_r_vals = [_safe_float(r.get("net_r")) for r in exit_decisions]

    green_rows = 0
    green_roundtrip_losses = 0
    for r in exit_decisions:
        best_r = _safe_float(r.get("best_favorable_r"))
        net_r = _safe_float(r.get("net_r"))
        if best_r > 0.0:
            green_rows += 1
            if net_r <= 0.0:
                green_roundtrip_losses += 1

    sl_like = {
        "AEE_PRE_SL_EXIT",
        "AEE_PANIC_EXIT",
        "PANIC_EXIT",
        "AEE_BAND_FAST_FAILURE_EXIT",
        "AEE_BAND_NEVER_GREEN_TIMEOUT",
        "AEE_FAST_ADVERSE_EXIT",
        "AEE_NEVER_GREEN_TIMEOUT",
    }
    sl_hits = sum(1 for r in exit_decisions if str(r.get("exit_reason", "") or "") in sl_like)

    realized_pips_vals = [_safe_float(r.get("realized_pips", r.get("pnl_pips", 0.0))) for r in exit_rows]
    realized_usd_vals = [_safe_float(r.get("realized_usd", r.get("pnl_usd", 0.0))) for r in exit_rows]

    close_count = len(exit_decisions)
    close_per_hour = close_count / duration_hr if duration_hr > 0 else 0.0

    scoreboard = {
        "generated_at": _iso_now(),
        "source_log": str(log_path),
        "window_duration_sec": duration_sec,
        "window_duration_hr": duration_hr,
        "top_aee_reasons": exit_reason_counts.most_common(12),
        "counts_per_reason": dict(exit_reason_counts),
        "realized_pips_per_hour": (sum(realized_pips_vals) / duration_hr) if (duration_hr > 0 and realized_pips_vals) else 0.0,
        "realized_usd_per_hour": (sum(realized_usd_vals) / duration_hr) if (duration_hr > 0 and realized_usd_vals) else 0.0,
        "close_cycle_capture_rate": close_per_hour,
        "avg_hold_sec": (sum(hold_vals) / len(hold_vals)) if hold_vals else 0.0,
        "avg_realized_r": (sum(realized_r_vals) / len(realized_r_vals)) if realized_r_vals else 0.0,
        "green_roundtrip_loss_rate": (green_roundtrip_losses / green_rows) if green_rows > 0 else 0.0,
        "sl_hit_rate": (sl_hits / close_count) if close_count > 0 else 0.0,
        "capital_recycling_rate": close_per_hour,
        "supporting_counts": {
            "periodic_rows": len(periodic_rows),
            "exit_decisions": close_count,
            "branch_trigger_rows": len(branch_rows),
            "exit_result_rows": len(exit_rows),
            "band_debug_rows": sum(1 for line in lines if "AEE_BAND_LOOP_DEBUG" in line),
        },
    }

    return scoreboard


def main() -> None:
    ap = argparse.ArgumentParser(description="Build system-level AEE extraction scoreboard from a bounded runtime log.")
    ap.add_argument("--log", required=True, help="Path to bounded runtime log file")
    ap.add_argument("--out", default="aee_system_extraction_scoreboard.json", help="Output JSON file")
    args = ap.parse_args()

    log_path = Path(args.log)
    if not log_path.exists():
        raise SystemExit(f"log not found: {log_path}")

    out = compute_scoreboard(log_path)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
