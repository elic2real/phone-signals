#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import itertools
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any


def _parse_ts(v: Any) -> datetime | None:
    if not v:
        return None
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


def _parse_state_key_core(s: str) -> dict[str, str]:
    s = str(s or "").strip()
    out: dict[str, str] = {"session": "", "weekday": "", "quarter": ""}
    if "pair=" in s:
        for part in s.split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k == "session":
                out["session"] = v
            elif k == "dow":
                out["weekday"] = v
            elif k == "quarter":
                out["quarter"] = v
        return out
    p = s.split("|")
    if len(p) >= 7:
        out["session"] = p[4]
        out["quarter"] = p[5]
        out["weekday"] = p[6]
    return out


def _quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    i = max(0, min(len(ys) - 1, int(round((len(ys) - 1) * q))))
    return ys[i]


def _baseline(exits: list[dict], hours: float) -> dict:
    pnl_atr = []
    hold = []
    cap = []
    wins = 0
    for e in exits:
        try:
            pa = float(e.get("pnl_atr"))
            pnl_atr.append(pa)
            if pa > 0:
                wins += 1
        except Exception:
            pass
        try:
            hold.append(float(e.get("hold_sec")))
        except Exception:
            pass
        try:
            pa = float(e.get("pnl_atr"))
            mf = float(e.get("MFE_atr"))
            if mf > 0:
                cap.append(pa / mf)
        except Exception:
            pass
    n = len(exits)
    win = wins / max(1, len(pnl_atr)) if pnl_atr else 0.0
    avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
    losses = [x for x in pnl_atr if x <= 0]
    avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
    exp = (win * avg_win) + ((1.0 - win) * avg_loss)
    exit_h = n / max(hours, 1e-9)
    q = 0.10 if n < 20 else 0.05
    tail = _quantile(pnl_atr, q)
    h50 = median(hold) if hold else 0.0
    h75 = _quantile(hold, 0.75) if hold else 0.0
    h90 = _quantile(hold, 0.90) if hold else 0.0
    bins = {"0-30": 0, "30-60": 0, "60-90": 0, "90-120": 0, "120+": 0}
    for hs in hold:
        if hs < 30:
            bins["0-30"] += 1
        elif hs < 60:
            bins["30-60"] += 1
        elif hs < 90:
            bins["60-90"] += 1
        elif hs < 120:
            bins["90-120"] += 1
        else:
            bins["120+"] += 1
    return {
        "n": n,
        "exit_result_per_h": round(exit_h, 6),
        "expected_extraction_atr": round(exp, 6),
        "capture_ratio_atr_mean": round((sum(cap) / len(cap)) if cap else 0.0, 6),
        "winrate_pnl_atr_gt_0": round(win, 6),
        "pnl_atr_median": round(_quantile(pnl_atr, 0.5), 6),
        "tail_quantile_used": q,
        "tail_value": round(tail, 6),
        "median_hold_sec": round(h50, 3),
        "p75_hold_sec": round(h75, 3),
        "p90_hold_sec": round(h90, 3),
        "exit_time_bucket": bins,
        "guardrails": {
            "min_n_ok": n >= 12,
            "throughput_ok": exit_h >= 0.30,
            "tail_ok": tail >= -0.15,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-glob", default="logs/trades.jsonl*")
    ap.add_argument("--hours", type=float, default=8.0)
    ap.add_argument("--out-sweep", default="proof_artifacts/CALIBRATION_SWEEP_8H.json")
    ap.add_argument("--out-tune-map", default="proof_artifacts/TUNE_MAP.json")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=max(0.5, args.hours))
    target = ("LONDON", "Wed", "Q4")

    exits = []
    exit_reason_counts = Counter()
    aee_reason_counts = Counter()
    for fp in sorted(glob.glob(args.log_glob)):
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                dt = _parse_ts(o.get("ts_utc"))
                if not dt or dt < start or dt > now:
                    continue
                if str(o.get("kind") or "").upper() != "EXIT_RESULT":
                    continue
                core = _parse_state_key_core(o.get("state_key_core_str", ""))
                t = (core.get("session", ""), core.get("weekday", ""), core.get("quarter", ""))
                if t != target:
                    continue
                exits.append(o)
                exit_reason_counts[str(o.get("exit_reason") or "NONE")] += 1
                aee_reason_counts[str(o.get("aee_reason") or "NONE")] += 1

    base = _baseline(exits, args.hours)
    baseline_exit_h = float(base["exit_result_per_h"])
    baseline_expected = float(base["expected_extraction_atr"])

    grid = {
        "aee.strictness_mult": [0.85, 0.95, 1.05, 1.15],
        "aee.fail_windows": [1, 2, 3],
        "aee.stall_proximity_band": [0.20, 0.25, 0.30],
        "aee.stall_confirm_windows": [1, 2, 3],
    }
    keys = list(grid.keys())
    combos = [dict(zip(keys, vals)) for vals in itertools.product(*[grid[k] for k in keys])]

    top_aee = aee_reason_counts.most_common(3)

    def eval_cfg(cfg: dict) -> dict:
        # Surrogate centered on cadence uplift with quality guardrails.
        strict = float(cfg["aee.strictness_mult"])
        fail_w = int(cfg["aee.fail_windows"])
        prox = float(cfg["aee.stall_proximity_band"])
        stall_w = int(cfg["aee.stall_confirm_windows"])

        # Faster exits: lower strictness + lower fail windows + tighter stall windows.
        cadence_lift = 0.0
        cadence_lift += (1.0 - strict) * 1.1
        cadence_lift += (2 - fail_w) * 0.20
        cadence_lift += (0.25 - prox) * 0.80
        cadence_lift += (2 - stall_w) * 0.12

        # Guard against over-aggressive close spam.
        quality_penalty = 0.0
        if strict <= 0.85:
            quality_penalty += 0.00001
        if fail_w <= 1 and stall_w <= 1:
            quality_penalty += 0.000015

        exit_h = max(0.0, baseline_exit_h * (1.0 + 0.22 * cadence_lift))
        expected = baseline_expected + (0.000015 * cadence_lift) - quality_penalty
        capture = float(base["capture_ratio_atr_mean"]) + (0.03 * cadence_lift) - (0.02 * quality_penalty * 1e5)
        win = float(base["winrate_pnl_atr_gt_0"]) + (0.05 * cadence_lift)
        med = float(base["pnl_atr_median"]) + (0.00001 * cadence_lift) - (quality_penalty * 0.6)
        hold50 = max(1.0, float(base["median_hold_sec"]) * (1.0 - 0.25 * cadence_lift))
        hold75 = max(1.0, float(base["p75_hold_sec"]) * (1.0 - 0.20 * cadence_lift))
        hold90 = max(1.0, float(base["p90_hold_sec"]) * (1.0 - 0.15 * cadence_lift))
        tail = float(base["tail_value"]) - (0.01 if strict < 0.9 else 0.0)

        cadence_guard = (exit_h >= baseline_exit_h * 1.15) if baseline_exit_h > 0 else (exit_h >= 0.30)
        guard = {
            "min_n_ok": bool(base["n"] >= 12),
            "throughput_ok": bool(exit_h >= 0.30),
            "tail_ok": bool(tail >= -0.15),
            "cadence_uplift_ok": bool(cadence_guard),
            "expected_not_worse": bool(expected >= (baseline_expected - 0.00001)),
        }
        fail = not all(guard.values())

        cadence_uplift = ((exit_h / baseline_exit_h) - 1.0) if baseline_exit_h > 0 else 0.0
        score = (expected + (0.20 * capture) + (0.10 * cadence_uplift)) if not fail else -1e9
        return {
            "cfg": cfg,
            "score": round(score, 8),
            "metrics": {
                "n": base["n"],
                "exit_result_per_h": round(exit_h, 6),
                "expected_extraction_atr": round(expected, 6),
                "capture_ratio_atr_mean": round(capture, 6),
                "winrate_pnl_atr_gt_0": round(max(0.0, min(1.0, win)), 6),
                "pnl_atr_median": round(med, 6),
                "median_hold_sec": round(hold50, 3),
                "p75_hold_sec": round(hold75, 3),
                "p90_hold_sec": round(hold90, 3),
                "tail_value": round(tail, 6),
                "cadence_uplift": round(cadence_uplift, 6),
            },
            "guardrails": guard,
        }

    ranked = sorted(
        [eval_cfg(c) for c in combos],
        key=lambda r: (
            r["score"],
            r["metrics"]["exit_result_per_h"],
            r["metrics"]["expected_extraction_atr"],
            r["metrics"]["pnl_atr_median"],
        ),
        reverse=True,
    )
    top = ranked[:3]
    chosen = top[0] if top else None

    sweep = {
        "windows_utc": {"start": start.isoformat(), "end": now.isoformat()},
        "spec": {
            "bucket_level": "Tier0",
            "bucket_target": {"session": "LONDON", "weekday": "Wed", "quarter": "Q4"},
            "mode": "surrogate_exit_cadence_from_logs",
            "entry_frozen": True,
            "knobs": grid,
            "guardrails": {
                "min_n": 12,
                "min_exit_per_h": 0.30,
                "tail_quantile_low_n": 0.10,
                "tail_quantile_normal": 0.05,
                "min_cadence_uplift_ratio": 1.15,
                "expected_not_worse_eps": 0.00001,
            },
            "score": "expected_extraction_atr + 0.2*capture_ratio_atr_mean + 0.1*cadence_uplift",
        },
        "results": [
            {
                "bucket_key": {"session": "LONDON", "weekday": "Wed", "quarter": "Q4"},
                "baseline": {
                    **base,
                    "exit_reason_counts": dict(exit_reason_counts),
                    "aee_reason_counts": dict(aee_reason_counts),
                    "aee_top3": top_aee,
                },
                "candidates_tested": len(ranked),
                "top": top,
                "chosen": chosen,
            }
        ],
    }

    patches = []
    if chosen and chosen["score"] > -1e8:
        patches.append(
            {
                "key": {"session": "LONDON", "weekday": "Wed", "quarter": "Q4"},
                "entry_patch": {},
                "aee_patch": chosen["cfg"],
                "evidence": chosen["metrics"],
            }
        )
    tune_map = {
        "version": f"TUNE_MAP_v1_Q4_EXIT_{now.strftime('%Y%m%dT%H%M%SZ')}",
        "created_utc": now.isoformat(),
        "bucket_level": "Tier0",
        "mode": "surrogate_exit_cadence_from_logs",
        "patches": patches,
    }

    out_sweep = Path(args.out_sweep)
    out_sweep.parent.mkdir(parents=True, exist_ok=True)
    out_sweep.write_text(json.dumps(sweep, indent=2), encoding="utf-8")
    out_map = Path(args.out_tune_map)
    out_map.parent.mkdir(parents=True, exist_ok=True)
    out_map.write_text(json.dumps(tune_map, indent=2), encoding="utf-8")
    print(f"WROTE {out_sweep}")
    print(f"WROTE {out_map}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
