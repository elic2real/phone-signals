#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any


def parse_ts(v: Any) -> datetime | None:
    if not v:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def parse_state_key_core(s: str) -> dict[str, str]:
    s = str(s or "").strip()
    out: dict[str, str] = {"pair": "", "session": "", "weekday": "", "quarter": "", "regime": "unknown"}
    if not s:
        return out
    if "pair=" in s:
        for part in s.split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k, v = k.strip(), v.strip()
            if k == "pair":
                out["pair"] = v
            elif k == "session":
                out["session"] = v
            elif k == "dow":
                out["weekday"] = v
            elif k == "quarter":
                out["quarter"] = v
            elif k == "regime":
                out["regime"] = v or "unknown"
        return out
    p = s.split("|")
    if len(p) >= 7:
        out["pair"] = p[0]
        out["session"] = p[4]
        out["quarter"] = p[5]
        out["weekday"] = p[6]
    return out


def iter_exits(log_glob: str, start: datetime, end: datetime):
    for fp in sorted(glob.glob(log_glob)):
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                if str(o.get("kind") or "").upper() != "EXIT_RESULT":
                    continue
                dt = parse_ts(o.get("ts_utc"))
                if not dt or not (start <= dt <= end):
                    continue
                yield o


def summarize_bucket(rows: list[dict], hours: float) -> dict[str, Any]:
    n = len(rows)
    pnl_atr, pnl_pips, mfe, mae = [], [], [], []
    wins = 0
    regions = Counter()
    ext_eligible = 0
    ext_killed = 0
    captures = []
    for r in rows:
        pa = None
        mf = None
        try:
            pa = float(r.get("pnl_atr"))
            pnl_atr.append(pa)
            if pa > 0:
                wins += 1
            if pa >= 1.0:
                regions["EXTENSION"] += 1
            elif pa > 0.0:
                regions["HARVEST"] += 1
            else:
                regions["LOSS_CTRL"] += 1
        except Exception:
            pass
        try:
            pnl_pips.append(float(r.get("pnl_pips")))
        except Exception:
            pass
        try:
            mf = float(r.get("MFE_atr"))
            mfe.append(mf)
        except Exception:
            pass
        try:
            mae.append(float(r.get("MAE_atr")))
        except Exception:
            pass
        if pa is not None and mf is not None and mf > 0:
            captures.append(pa / mf)
        if mf is not None and mf >= 1.0:
            ext_eligible += 1
            if pa is not None and pa < 1.0:
                ext_killed += 1

    losses = [x for x in pnl_atr if x <= 0]
    avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
    avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
    winrate = (wins / max(1, len(pnl_atr))) if pnl_atr else 0.0
    expected_extraction_atr = (winrate * avg_win) + ((1.0 - winrate) * avg_loss)

    tail_val = 0.0
    if pnl_atr:
        idx = max(0, int(0.05 * max(1, len(pnl_atr))) - 1)
        tail_val = sorted(pnl_atr)[idx]

    ratio_ext = regions["EXTENSION"] / max(1, n)
    ratio_h = regions["HARVEST"] / max(1, n)
    ratio_l = regions["LOSS_CTRL"] / max(1, n)
    mode_balance = ratio_h + 0.5 * ratio_ext

    return {
        "n": n,
        "expected_extraction_atr": round(expected_extraction_atr, 6),
        "mode_balance": round(mode_balance, 6),
        "ratio_extension": round(ratio_ext, 6),
        "ratio_harvest": round(ratio_h, 6),
        "ratio_loss_ctrl": round(ratio_l, 6),
        "exit_result_per_h": round(n / max(hours, 1e-9), 4),
        "tail_ok": bool(tail_val >= -0.15),
        "pnl_atr_mean": round((sum(pnl_atr) / len(pnl_atr)) if pnl_atr else 0.0, 6),
        "pnl_atr_median": round(median(pnl_atr), 6) if pnl_atr else 0.0,
        "capture_ratio_atr_mean": round((sum(captures) / len(captures)) if captures else 0.0, 6),
        "extension_kill_rate": round(ext_killed / max(1, ext_eligible), 6),
    }


def rank_rows(rows: list[dict], top_n: int):
    scored = sorted(rows, key=lambda r: (r["expected_extraction_atr"], r["mode_balance"], r["exit_result_per_h"]), reverse=True)
    worst = sorted(rows, key=lambda r: (r["expected_extraction_atr"], r["mode_balance"]))
    return scored[:top_n], worst[:top_n]


def print_table(title: str, rows: list[dict], key_fields: list[str]):
    print(f"\n{title}")
    print("-" * len(title))
    for r in rows:
        key = r["key"]
        ktxt = " | ".join(f"{k}={key.get(k, '')}" for k in key_fields)
        print(
            f"{ktxt} :: n={r['n']} ee={r['expected_extraction_atr']:.6f} "
            f"mode={r['mode_balance']:.3f} exit/h={r['exit_result_per_h']:.3f} tail_ok={r['tail_ok']}"
        )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-glob", default="logs/trades.jsonl*")
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--end-utc", default="")
    ap.add_argument("--tier0-min-n", type=int, default=30)
    ap.add_argument("--tier1-min-n", type=int, default=20)
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--out-dir", default="proof_artifacts")
    args = ap.parse_args()

    end = parse_ts(args.end_utc) if args.end_utc else datetime.now(timezone.utc)
    if end is None:
        raise SystemExit("invalid --end-utc")
    start = end - timedelta(hours=max(args.hours, 0.01))

    exits = list(iter_exits(args.log_glob, start, end))
    hours = max(args.hours, 1e-9)

    by_tier0: dict[tuple, list[dict]] = defaultdict(list)
    by_tier1: dict[tuple, list[dict]] = defaultdict(list)

    for r in exits:
        core = parse_state_key_core(r.get("state_key_core_str", ""))
        pair = core.get("pair") or str(r.get("pair") or "")
        session = core.get("session") or ""
        weekday = core.get("weekday") or ""
        quarter = core.get("quarter") or ""
        regime = core.get("regime") or "unknown"

        by_tier0[(session, weekday, quarter)].append(r)
        by_tier1[(pair, session, weekday, quarter, regime)].append(r)

    tier0_rows = []
    for k, rows in by_tier0.items():
        if len(rows) < args.tier0_min_n:
            continue
        m = summarize_bucket(rows, hours)
        m["key"] = {"session": k[0], "weekday": k[1], "quarter": k[2]}
        tier0_rows.append(m)

    tier1_rows = []
    for k, rows in by_tier1.items():
        if len(rows) < args.tier1_min_n:
            continue
        m = summarize_bucket(rows, hours)
        m["key"] = {"pair": k[0], "session": k[1], "weekday": k[2], "quarter": k[3], "regime": k[4]}
        tier1_rows.append(m)

    t0_top, t0_worst = rank_rows(tier0_rows, args.top_n)
    t1_top, t1_worst = rank_rows(tier1_rows, max(25, args.top_n))

    out_t0 = {
        "window": {"start": start.isoformat(), "end": end.isoformat(), "hours": args.hours},
        "counts": {"exit_rows_total": len(exits), "buckets_total": len(by_tier0), "buckets_active": len(tier0_rows)},
        "thresholds": {"min_n": args.tier0_min_n},
        "top": t0_top,
        "worst": t0_worst,
        "all_active": tier0_rows,
    }
    out_t1 = {
        "window": {"start": start.isoformat(), "end": end.isoformat(), "hours": args.hours},
        "counts": {"exit_rows_total": len(exits), "buckets_total": len(by_tier1), "buckets_active": len(tier1_rows)},
        "thresholds": {"min_n": args.tier1_min_n},
        "top": t1_top,
        "worst": t1_worst,
        "all_active": tier1_rows,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    p0 = out_dir / "MAP_24H_TIER0.json"
    p1 = out_dir / "MAP_24H_TIER1.json"
    p0.write_text(json.dumps(out_t0, indent=2), encoding="utf-8")
    p1.write_text(json.dumps(out_t1, indent=2), encoding="utf-8")

    print(f"Wrote {p0}")
    print(f"Wrote {p1}")
    print_table("Tier-0 Top", t0_top, ["session", "weekday", "quarter"])
    print_table("Tier-0 Worst", t0_worst, ["session", "weekday", "quarter"])
    print_table("Tier-1 Top", t1_top[:args.top_n], ["pair", "session", "weekday", "quarter", "regime"])
    print_table("Tier-1 Worst", t1_worst[:args.top_n], ["pair", "session", "weekday", "quarter", "regime"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

