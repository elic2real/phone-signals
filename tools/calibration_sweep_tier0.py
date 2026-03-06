#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import itertools
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
    out: dict[str, str] = {"pair": "", "session": "", "weekday": "", "quarter": "", "regime": ""}
    if "pair=" in s:
        for part in s.split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "pair":
                out["pair"] = v
            elif k == "session":
                out["session"] = v
            elif k == "dow":
                out["weekday"] = v
            elif k == "quarter":
                out["quarter"] = v
            elif k == "regime":
                out["regime"] = v
        return out
    p = s.split("|")
    if len(p) >= 7:
        out["pair"] = p[0]
        out["session"] = p[4]
        out["quarter"] = p[5]
        out["weekday"] = p[6]
    return out


def _tier0_key_from_row(o: dict) -> tuple[str, str, str]:
    core = _parse_state_key_core(o.get("state_key_core_str", ""))
    return (core.get("session", ""), core.get("weekday", ""), core.get("quarter", ""))


def _quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    i = max(0, min(len(ys) - 1, int(round((len(ys) - 1) * q))))
    return ys[i]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-glob", default="logs/trades.jsonl*")
    ap.add_argument("--hours", type=float, default=8.0)
    ap.add_argument("--out-sweep", default="proof_artifacts/CALIBRATION_SWEEP_8H.json")
    ap.add_argument("--out-tune-map", default="proof_artifacts/TUNE_MAP.json")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=max(0.5, args.hours))

    # Targets as requested.
    targets = [
        ("LONDON", "Wed", "Q4"),
        ("LONDON", "Wed", "Q3"),
    ]

    # Collect baseline metrics + reasons per Tier-0 bucket.
    exits_by_t0: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    entry_block_reasons: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    exit_reasons: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    aee_reasons: dict[tuple[str, str, str], Counter] = defaultdict(Counter)

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
                k = str(o.get("kind") or "").upper()
                t0 = _tier0_key_from_row(o)
                if t0 not in targets:
                    continue
                if k == "ENTRY_GATE_EVAL":
                    br = str(o.get("block_reason") or "").strip()
                    if br:
                        entry_block_reasons[t0][br] += 1
                elif k == "EXIT_RESULT":
                    exits_by_t0[t0].append(o)
                    exit_reasons[t0][str(o.get("exit_reason") or "NONE")] += 1
                    aee_reasons[t0][str(o.get("aee_reason") or "NONE")] += 1

    def baseline_row(t0: tuple[str, str, str]) -> dict:
        exits = exits_by_t0.get(t0, [])
        n = len(exits)
        pnl_atr = []
        capture = []
        for e in exits:
            try:
                pa = float(e.get("pnl_atr"))
                pnl_atr.append(pa)
            except Exception:
                pass
            try:
                pa = float(e.get("pnl_atr"))
                mf = float(e.get("MFE_atr"))
                if mf > 0:
                    capture.append(pa / mf)
            except Exception:
                pass
        wins = sum(1 for x in pnl_atr if x > 0)
        winrate = (wins / max(1, len(pnl_atr))) if pnl_atr else 0.0
        avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
        losses = [x for x in pnl_atr if x <= 0]
        avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
        expected = (winrate * avg_win) + ((1.0 - winrate) * avg_loss)
        exit_h = n / max(args.hours, 1e-9)
        q = 0.10 if n < 20 else 0.05
        tail = _quantile(pnl_atr, q)
        return {
            "n": n,
            "exit_result_per_h": round(exit_h, 4),
            "expected_extraction_atr": round(expected, 6),
            "capture_ratio_atr_mean": round((sum(capture) / len(capture)) if capture else 0.0, 6),
            "winrate_pnl_atr_gt_0": round(winrate, 4),
            "pnl_atr_median": round(_quantile(pnl_atr, 0.5), 6),
            "tail_quantile_used": q,
            "tail_value": round(tail, 6),
            "guardrails": {
                "min_n_ok": n >= 12,
                "throughput_ok": exit_h >= 0.30,
                "tail_ok": tail >= -0.15,
            },
        }

    # 5-knob sweep grid.
    grid = {
        "max_dist_atr": [0.15, 0.25, 0.35],
        "confirm_disp_atr": [0.08, 0.12, 0.16],
        "confirm_sec": [1.0, 2.0, 3.0],
        "aee_strictness_scalar": [0.90, 1.00, 1.10],
        "aee_fail_windows": [1, 2, 3],
    }
    keys = list(grid.keys())
    combos = [dict(zip(keys, vals)) for vals in itertools.product(*[grid[k] for k in keys])]

    def surrogate_eval(base: dict, blocks: Counter, cfg: dict) -> dict:
        # Deterministic surrogate model from observed blockers/exits for attribution-clean first sweep.
        total_blocks = sum(blocks.values()) or 1
        dist_w = blocks.get("tick_entry_dist_too_far", 0) / total_blocks
        break_w = blocks.get("tick_entry_break_not_crossed", 0) / total_blocks
        rec_w = blocks.get("tick_entry_reclaim_wait_reclaim", 0) / total_blocks
        resume_w = blocks.get("tick_entry_resume_wait_pullback", 0) / total_blocks
        conf_w = blocks.get("tick_entry_resume_wait_confirm", 0) / total_blocks

        # Positive if loosening aligns with observed blockers.
        entry_lift = 0.0
        entry_lift += dist_w * ((cfg["max_dist_atr"] - 0.25) / 0.10)
        entry_lift += (break_w + rec_w + resume_w) * ((0.12 - cfg["confirm_disp_atr"]) / 0.04)
        entry_lift += conf_w * ((2.0 - cfg["confirm_sec"]) / 1.0)

        aee_lift = 0.0
        aee_lift += (1.0 - cfg["aee_strictness_scalar"]) * 0.8
        aee_lift += (2 - cfg["aee_fail_windows"]) * 0.15

        base_exit_h = float(base.get("exit_result_per_h", 0.0))
        base_exp = float(base.get("expected_extraction_atr", 0.0))
        base_cap = float(base.get("capture_ratio_atr_mean", 0.0))
        base_win = float(base.get("winrate_pnl_atr_gt_0", 0.0))
        base_med = float(base.get("pnl_atr_median", 0.0))
        n = int(base.get("n", 0))

        exit_h = max(0.0, base_exit_h * (1.0 + 0.28 * entry_lift + 0.10 * aee_lift))
        expected = base_exp + (0.00005 * entry_lift) + (0.00003 * aee_lift)
        cap = base_cap + (0.03 * entry_lift) + (0.05 * aee_lift)
        win = min(1.0, max(0.0, base_win + 0.05 * entry_lift + 0.03 * aee_lift))
        med = base_med + (0.00003 * entry_lift) + (0.00002 * aee_lift)

        # Risk penalties for too-loose settings.
        tail = float(base.get("tail_value", 0.0))
        if cfg["max_dist_atr"] >= 0.35:
            tail -= 0.03
        if cfg["aee_strictness_scalar"] <= 0.90:
            tail -= 0.02
        if cfg["confirm_sec"] <= 1.0 and cfg["confirm_disp_atr"] <= 0.08:
            tail -= 0.02

        guards = {
            "min_n_ok": n >= 12,
            "throughput_ok": exit_h >= 0.30,
            "tail_ok": tail >= -0.15,
        }
        fail = not all(guards.values())
        score = (expected + (0.20 * cap)) if not fail else -1e9
        return {
            "cfg": cfg,
            "score": round(score, 8),
            "metrics": {
                "n": n,
                "expected_extraction_atr": round(expected, 6),
                "capture_ratio_atr_mean": round(cap, 6),
                "exit_result_per_h": round(exit_h, 6),
                "winrate_pnl_atr_gt_0": round(win, 6),
                "pnl_atr_median": round(med, 6),
                "tail_value": round(tail, 6),
            },
            "guardrails": guards,
            "block_reason_counts": dict(blocks),
        }

    results = []
    patches = []
    for t0 in targets:
        base = baseline_row(t0)
        blocks = entry_block_reasons.get(t0, Counter())
        evaluated = [surrogate_eval(base, blocks, cfg) for cfg in combos]
        ranked = sorted(
            evaluated,
            key=lambda r: (
                r["score"],
                r["metrics"]["exit_result_per_h"],
                r["metrics"]["winrate_pnl_atr_gt_0"],
                r["metrics"]["pnl_atr_median"],
            ),
            reverse=True,
        )
        top3 = ranked[:3]
        chosen = top3[0] if top3 else None
        bucket_key = {"session": t0[0], "weekday": t0[1], "quarter": t0[2]}
        results.append(
            {
                "bucket_key": bucket_key,
                "baseline": {
                    **base,
                    "block_reason_counts": dict(blocks),
                    "exit_reason_counts": dict(exit_reasons.get(t0, Counter())),
                    "aee_reason_counts": dict(aee_reasons.get(t0, Counter())),
                },
                "candidates_tested": len(evaluated),
                "top": top3,
                "chosen": chosen,
            }
        )
        if chosen and chosen["score"] > -1e8:
            cfg = chosen["cfg"]
            patches.append(
                {
                    "key": bucket_key,
                    "entry_patch": {
                        "max_dist_atr": cfg["max_dist_atr"],
                        "confirm_disp_atr": cfg["confirm_disp_atr"],
                        "confirm_sec": cfg["confirm_sec"],
                    },
                    "aee_patch": {
                        "aee_strictness_scalar": cfg["aee_strictness_scalar"],
                        "aee_fail_windows": cfg["aee_fail_windows"],
                    },
                    "evidence": chosen["metrics"],
                }
            )

    sweep = {
        "windows_utc": {"start": start.isoformat(), "end": now.isoformat()},
        "spec": {
            "bucket_level": "Tier0",
            "mode": "surrogate_from_logs",
            "buckets_targeted": [{"session": s, "weekday": w, "quarter": q} for s, w, q in targets],
            "knobs": grid,
            "guardrails": {"min_n": 12, "min_exit_per_h": 0.30, "tail_quantile_low_n": 0.10, "tail_quantile_normal": 0.05},
            "score": "expected_extraction_atr + 0.2*capture_ratio_atr_mean",
        },
        "results": results,
    }
    tune_map = {
        "version": f"TUNE_MAP_v1_{now.strftime('%Y%m%dT%H%M%SZ')}",
        "created_utc": now.isoformat(),
        "bucket_level": "Tier0",
        "mode": "surrogate_from_logs",
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
