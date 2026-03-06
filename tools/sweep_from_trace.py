#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path


def _row_target_key(row: dict) -> str:
    base = "|".join([str(row.get("pair", "")), str(row.get("session", "")), str(row.get("weekday", "")), str(row.get("quarter", ""))])
    base_v3 = "|".join([str(row.get("pair", "")), str(row.get("session", ""))])
    atr = str(row.get("atr_bucket", ""))
    sp = str(row.get("spread_bucket", ""))
    return {
        "ATR": f"{base}|{atr}",
        "SPREAD": f"{base}|{sp}",
        "ATR_V3": f"{base_v3}|{atr}",
        "SPREAD_V3": f"{base_v3}|{sp}",
    }


def _load_trace(path: str, target_key: str = "") -> dict[str, list[dict]]:
    trades: dict[str, list[dict]] = defaultdict(list)
    mode = ""
    use_v3 = False
    if target_key:
        if "|ATR_" in target_key:
            mode = "ATR"
        elif "|SB_" in target_key:
            mode = "SPREAD"
        parts = target_key.split("|")
        if len(parts) == 3:
            use_v3 = True
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if target_key:
                keys = _row_target_key(r)
                if not mode:
                    continue
                kcmp = keys.get(f"{mode}_V3") if use_v3 else keys.get(mode)
                if kcmp != target_key:
                    continue
            k = r.get("trade_key")
            if not k:
                continue
            trades[str(k)].append(r)
    for k in list(trades.keys()):
        trades[k].sort(key=lambda x: int(x.get("eval_seq", 0)))
    return trades


def _decide_close(row: dict, knobs: dict) -> bool:
    strict = float(knobs.get("aee.strictness_mult", 1.0) or 1.0)
    near_tp = float(knobs.get("aee.near_tp_band_atr", 0.26) or 0.26)
    fail_w = int(knobs.get("aee.fail_windows", 2) or 2)
    prom = float(knobs.get("promote_mfe_atr", 0.25) or 0.25)
    ext_min = float(knobs.get("extension_allow_energy_min", 0.95) or 0.95)

    pnl = float(row.get("pnl_atr", 0.0) or 0.0)
    mfe = float(row.get("mfe_atr", 0.0) or 0.0)
    energy = float(row.get("energy_ratio", 0.0) or 0.0)
    giveback = float(row.get("giveback_ratio", 0.0) or 0.0)
    fails = int(row.get("consecutive_fail_windows", 0) or 0)

    # Loss control
    if pnl <= -0.5 * strict:
        return True
    # Stall/fail windows
    if fails >= fail_w and energy < (0.95 * strict):
        return True
    # Runner guard
    if mfe >= prom and energy < ext_min and giveback > near_tp:
        return True
    return False


def _score_candidate(trades: dict[str, list[dict]], knobs: dict) -> dict:
    pnl = []
    cap = []
    holds = []
    for rows in trades.values():
        exit_row = rows[-1]
        for r in rows:
            if _decide_close(r, knobs):
                exit_row = r
                break
        p = float(exit_row.get("pnl_atr", 0.0) or 0.0)
        m = float(exit_row.get("mfe_atr", 0.0) or 0.0)
        c = (p / m) if m > 1e-9 else 0.0
        h = float(exit_row.get("time_in_trade_sec", 0.0) or 0.0)
        pnl.append(p)
        cap.append(c)
        holds.append(max(1.0, h))
    n_real = len(pnl)
    if n_real == 0:
        return {
            "n_trades": 0,
            "expected_extraction_atr": 0.0,
            "capture_to_ceiling": 0.0,
            "avg_hold_sec": 0.0,
            "extraction_per_hour": 0.0,
            "exits_per_hour": 0.0,
            "pnl_atr_p10": 0.0,
        }
    n = n_real
    pnl_sorted = sorted(pnl)
    p10 = pnl_sorted[int(0.10 * (n - 1))] if pnl_sorted else 0.0
    avg_hold_sec = sum(holds) / n
    extraction_per_hour = (sum(pnl) / n) * (3600.0 / avg_hold_sec)
    exits_per_hour = 3600.0 / avg_hold_sec
    return {
        "n_trades": len(pnl),
        "expected_extraction_atr": sum(pnl) / n,
        "capture_to_ceiling": sum(cap) / n,
        "avg_hold_sec": avg_hold_sec,
        "extraction_per_hour": extraction_per_hour,
        "exits_per_hour": exits_per_hour,
        "pnl_atr_p10": p10,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", required=True)
    ap.add_argument("--candidates", required=True, help="JSON list: [{name, knobs}]")
    ap.add_argument("--target-key", default="", help="State selector key e.g. pair|session|weekday|quarter|ATR_HIGH or ...|SB_1p5_3")
    ap.add_argument("--out", default="proof_artifacts/CALIBRATION_SWEEP_TRACE.json")
    args = ap.parse_args()

    trades = _load_trace(args.trace, target_key=args.target_key)
    cand = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    if not isinstance(cand, list):
        raise SystemExit("candidates must be JSON list")

    baseline = _score_candidate(trades, {})
    rows = []
    for c in cand:
        name = c.get("name", "unnamed")
        knobs = c.get("knobs", {}) or {}
        s = _score_candidate(trades, knobs)
        rows.append(
            {
                "candidate": name,
                "knobs": knobs,
                "n_trades": s["n_trades"],
                "expected_extraction_atr": s["expected_extraction_atr"],
                "capture_to_ceiling": s["capture_to_ceiling"],
                "avg_hold_sec": s["avg_hold_sec"],
                "extraction_per_hour": s["extraction_per_hour"],
                "exits_per_hour": s["exits_per_hour"],
                "pnl_atr_p10": s["pnl_atr_p10"],
                "delta_expected_extraction_atr": s["expected_extraction_atr"] - baseline["expected_extraction_atr"],
                "delta_capture_to_ceiling": s["capture_to_ceiling"] - baseline["capture_to_ceiling"],
                "delta_extraction_per_hour": s["extraction_per_hour"] - baseline["extraction_per_hour"],
                "delta_exits_per_hour": s["exits_per_hour"] - baseline["exits_per_hour"],
                "delta_pnl_atr_p10": s["pnl_atr_p10"] - baseline["pnl_atr_p10"],
            }
        )
    rows.sort(key=lambda x: (x["delta_extraction_per_hour"], x["delta_expected_extraction_atr"]), reverse=True)

    out = {
        "source": {"trace": args.trace, "target_key": args.target_key},
        "baseline": baseline,
        "ranked_candidates": rows,
    }
    op = Path(args.out)
    op.parent.mkdir(parents=True, exist_ok=True)
    op.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"TRACE_SWEEP_OK {op} candidates={len(rows)} trades={len(trades)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
