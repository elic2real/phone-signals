#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _mode_balance_from_bucket_rows(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    vals = []
    for r in rows:
        try:
            rh = float(r.get("ratio_harvest", 0.0) or 0.0)
            re = float(r.get("ratio_extension", 0.0) or 0.0)
            vals.append(rh + 0.5 * re)
        except Exception:
            vals.append(0.0)
    return sum(vals) / max(1, len(vals))


def _tail_ok(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    return all(bool((r.get("guardrails") or {}).get("tail_ok", False)) for r in rows)


def _max_bucket_n(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    return max(int(r.get("n", 0) or 0) for r in rows)


def evaluate_slice(
    name: str,
    art: dict[str, Any],
    min_exit_n: int,
    min_bucket_n: int,
    min_delta_extraction: float,
    min_delta_h1800: float,
    min_delta_h3600: float,
    min_delta_mode_balance: float,
    min_mode_balance_abs: float,
    max_exit_drop_frac: float,
) -> dict[str, Any]:
    prev = art.get("prev", {}) or {}
    curr = art.get("curr", {}) or {}
    bprev = (art.get("bucket_stats", {}) or {}).get("prev", []) or []
    bcurr = (art.get("bucket_stats", {}) or {}).get("curr", []) or []
    ceiling_prev = (art.get("ceiling", {}) or {}).get("prev", {}) or {}
    ceiling_curr = (art.get("ceiling", {}) or {}).get("curr", {}) or {}
    delta = art.get("delta", {}) or {}

    prev_exit_n = int((prev.get("quality", {}) or {}).get("exit_n", 0) or 0)
    curr_exit_n = int((curr.get("quality", {}) or {}).get("exit_n", 0) or 0)
    curr_exit_per_h = float((curr.get("rates_per_hour", {}) or {}).get("exit_result_per_h", 0.0) or 0.0)
    prev_exit_per_h = float((prev.get("rates_per_hour", {}) or {}).get("exit_result_per_h", 0.0) or 0.0)

    eligible = (curr_exit_n >= min_exit_n) and (_max_bucket_n(bcurr) >= min_bucket_n)
    if not eligible:
        return {
            "slice": name,
            "eligible": False,
            "status": "INSUFFICIENT_DATA",
            "metrics": {
                "prev_exit_n": prev_exit_n,
                "curr_exit_n": curr_exit_n,
                "max_bucket_n_curr": _max_bucket_n(bcurr),
            },
        }

    de = float(delta.get("pnl_atr_mean", 0.0) or 0.0)
    # Prefer explicit expected_extraction delta if provided by artifact.
    prev_ee = float((prev.get("quality", {}) or {}).get("expected_extraction_atr", 0.0) or 0.0)
    curr_ee = float((curr.get("quality", {}) or {}).get("expected_extraction_atr", 0.0) or 0.0)
    d_expected_extraction = curr_ee - prev_ee if (curr_ee or prev_ee) else de

    prev_h1800 = float(ceiling_prev.get("ceiling_capture_mean_h1800_x_1p0", ceiling_prev.get("ceiling_capture_mean", 0.0)) or 0.0)
    curr_h1800 = float(ceiling_curr.get("ceiling_capture_mean_h1800_x_1p0", ceiling_curr.get("ceiling_capture_mean", 0.0)) or 0.0)
    prev_h3600 = float(ceiling_prev.get("ceiling_capture_mean_h3600_x_1p0", ceiling_prev.get("ceiling_capture_mean", 0.0)) or 0.0)
    curr_h3600 = float(ceiling_curr.get("ceiling_capture_mean_h3600_x_1p0", ceiling_curr.get("ceiling_capture_mean", 0.0)) or 0.0)
    d_h1800 = curr_h1800 - prev_h1800
    d_h3600 = curr_h3600 - prev_h3600

    prev_mode_balance = _mode_balance_from_bucket_rows(bprev)
    curr_mode_balance = _mode_balance_from_bucket_rows(bcurr)
    d_mode_balance = curr_mode_balance - prev_mode_balance

    improvements = []
    if d_expected_extraction >= min_delta_extraction:
        improvements.append("expected_extraction_atr")
    if d_h1800 >= min_delta_h1800:
        improvements.append("ceiling_capture_h1800_x_1p0")
    if d_h3600 >= min_delta_h3600:
        improvements.append("ceiling_capture_h3600_x_1p0")
    if d_mode_balance >= min_delta_mode_balance and curr_mode_balance >= min_mode_balance_abs:
        improvements.append("mode_balance")

    cross_horizon_ok = (d_h1800 >= 0.0 and d_h3600 >= 0.0) and (d_h1800 >= min_delta_h1800 or d_h3600 >= min_delta_h3600)
    tail_ok = _tail_ok(bcurr)
    throughput_ok = curr_exit_per_h >= ((1.0 - max_exit_drop_frac) * prev_exit_per_h)
    guardrails_ok = bool(tail_ok and throughput_ok and cross_horizon_ok)
    passed = bool(len(improvements) >= 2 and guardrails_ok)

    return {
        "slice": name,
        "eligible": True,
        "status": "PASS" if passed else "FAIL",
        "improved_count": len(improvements),
        "improvements": improvements,
        "guardrails_ok": guardrails_ok,
        "metrics": {
            "delta_expected_extraction_atr": round(d_expected_extraction, 6),
            "delta_ceiling_capture_mean_h1800_x_1p0": round(d_h1800, 6),
            "delta_ceiling_capture_mean_h3600_x_1p0": round(d_h3600, 6),
            "delta_mode_balance": round(d_mode_balance, 6),
            "mode_balance_curr": round(curr_mode_balance, 6),
            "exit_result_per_h_prev": round(prev_exit_per_h, 6),
            "exit_result_per_h_curr": round(curr_exit_per_h, 6),
            "tail_ok": bool(tail_ok),
            "cross_horizon_ok": bool(cross_horizon_ok),
            "throughput_ok": bool(throughput_ok),
            "prev_exit_n": prev_exit_n,
            "curr_exit_n": curr_exit_n,
            "max_bucket_n_curr": _max_bucket_n(bcurr),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slice-a", required=True)
    ap.add_argument("--slice-b", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--min-exit-n", type=int, default=80)
    ap.add_argument("--min-bucket-n", type=int, default=30)
    ap.add_argument("--min-delta-extraction", type=float, default=1e-5)
    ap.add_argument("--min-delta-h1800", type=float, default=0.03)
    ap.add_argument("--min-delta-h3600", type=float, default=0.02)
    ap.add_argument("--min-delta-mode-balance", type=float, default=0.05)
    ap.add_argument("--min-mode-balance-abs", type=float, default=0.20)
    ap.add_argument("--max-exit-drop-frac", type=float, default=0.10)
    args = ap.parse_args()

    a = _load(args.slice_a)
    b = _load(args.slice_b)

    sa = evaluate_slice(
        "A",
        a,
        args.min_exit_n,
        args.min_bucket_n,
        args.min_delta_extraction,
        args.min_delta_h1800,
        args.min_delta_h3600,
        args.min_delta_mode_balance,
        args.min_mode_balance_abs,
        args.max_exit_drop_frac,
    )
    sb = evaluate_slice(
        "B",
        b,
        args.min_exit_n,
        args.min_bucket_n,
        args.min_delta_extraction,
        args.min_delta_h1800,
        args.min_delta_h3600,
        args.min_delta_mode_balance,
        args.min_mode_balance_abs,
        args.max_exit_drop_frac,
    )

    if not sa["eligible"] or not sb["eligible"]:
        decision = "INSUFFICIENT_DATA"
    else:
        decision = "PASS" if (sa["status"] == "PASS" and sb["status"] == "PASS") else "FAIL"

    report = {
        "promotion_decision": decision,
        "config": {
            "min_exit_n": args.min_exit_n,
            "min_bucket_n": args.min_bucket_n,
            "min_delta_extraction": args.min_delta_extraction,
            "min_delta_h1800": args.min_delta_h1800,
            "min_delta_h3600": args.min_delta_h3600,
            "min_delta_mode_balance": args.min_delta_mode_balance,
            "min_mode_balance_abs": args.min_mode_balance_abs,
            "max_exit_drop_frac": args.max_exit_drop_frac,
        },
        "slices": [sa, sb],
    }

    text = json.dumps(report, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

