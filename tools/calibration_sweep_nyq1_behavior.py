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


def _parse_core(s: str) -> dict[str, str]:
    s = str(s or "").strip()
    out = {"pair": "", "session": "", "quarter": "", "weekday": ""}
    if "pair=" in s:
        for part in s.split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k in ("pair", "session", "quarter"):
                out[k] = v
            elif k == "dow":
                out["weekday"] = v
        return out
    p = s.split("|")
    if len(p) >= 7:
        out["pair"] = p[0]
        out["session"] = p[4]
        out["quarter"] = p[5]
        out["weekday"] = p[6]
    return out


def _q(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    i = max(0, min(len(ys) - 1, int(round((len(ys) - 1) * q))))
    return ys[i]


def _bucket_metrics(exits: list[dict], hours: float) -> dict[str, Any]:
    n = len(exits)
    pnl_atr = []
    capture = []
    wins = 0
    regions = Counter()
    extension_eligible = 0
    extension_killed = 0
    extension_attempts = 0
    hold = []
    for e in exits:
        pa = None
        mf = None
        try:
            pa = float(e.get("pnl_atr"))
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
            mf = float(e.get("MFE_atr"))
        except Exception:
            mf = None
        if mf is not None and pa is not None:
            if mf > 0:
                capture.append(pa / mf)
            if mf >= 1.0:
                extension_eligible += 1
                if pa < 1.0:
                    extension_killed += 1
        if str(e.get("leg_type") or "").upper() == "RUNNER":
            extension_attempts += 1
        try:
            hold.append(float(e.get("hold_sec")))
        except Exception:
            pass

    winrate = wins / max(1, len(pnl_atr)) if pnl_atr else 0.0
    avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
    losses = [x for x in pnl_atr if x <= 0]
    avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
    expected = (winrate * avg_win) + ((1.0 - winrate) * avg_loss)
    exit_h = n / max(hours, 1e-9)
    tail_q = 0.10 if n < 20 else 0.05
    tail = _q(pnl_atr, tail_q)
    ratio_ext = regions["EXTENSION"] / max(1, n)
    ratio_h = regions["HARVEST"] / max(1, n)
    ratio_l = regions["LOSS_CTRL"] / max(1, n)
    mode_balance = ratio_h + (0.5 * ratio_ext)
    return {
        "n": n,
        "exit_result_per_h": round(exit_h, 6),
        "expected_extraction_atr": round(expected, 6),
        "capture_ratio_atr_mean": round((sum(capture) / len(capture)) if capture else 0.0, 6),
        "ratio_extension": round(ratio_ext, 6),
        "ratio_harvest": round(ratio_h, 6),
        "ratio_loss_ctrl": round(ratio_l, 6),
        "mode_balance": round(mode_balance, 6),
        "extension_kill_rate": round(extension_killed / max(1, extension_eligible), 6),
        "extension_attempt_rate": round(extension_attempts / max(1, n), 6),
        "tail_quantile_used": tail_q,
        "tail_value": round(tail, 6),
        "median_hold_sec": round(_q(hold, 0.5), 3),
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
    targets = [("EUR_USD", "NY", "Q1"), ("USD_CAD", "NY", "Q1"), ("AUD_USD", "NY", "Q1")]

    exits_by_pair: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    aee_reasons: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    for fp in sorted(glob.glob(args.log_glob)):
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                if str(o.get("kind") or "").upper() != "EXIT_RESULT":
                    continue
                dt = _parse_ts(o.get("ts_utc"))
                if not dt or dt < start or dt > now:
                    continue
                c = _parse_core(o.get("state_key_core_str", ""))
                key = (str(c.get("pair") or o.get("pair") or ""), c.get("session", ""), c.get("quarter", ""))
                if key not in targets:
                    continue
                exits_by_pair[key].append(o)
                aee_reasons[key][str(o.get("aee_reason") or "NONE")] += 1

    grid = {
        "aee.strictness_mult": [0.85, 0.95, 1.05, 1.15],
        "aee.fail_windows": [1, 2, 3],
        "promote_mfe_atr": [0.15, 0.25, 0.35, 0.50],
        "extension_allow_energy_min": [0.85, 0.95, 1.05],
    }
    keys = list(grid.keys())
    configs = [dict(zip(keys, vals)) for vals in itertools.product(*[grid[k] for k in keys])]

    results = []
    patches = []
    for pair, session, quarter in targets:
        base = _bucket_metrics(exits_by_pair.get((pair, session, quarter), []), args.hours)
        base_exit_h = float(base["exit_result_per_h"])
        base_exp = float(base["expected_extraction_atr"])
        base_cap = float(base["capture_ratio_atr_mean"])
        base_mode = float(base["mode_balance"])
        base_tail = float(base["tail_value"])
        n = int(base["n"])

        evaluated = []
        for cfg in configs:
            strict = float(cfg["aee.strictness_mult"])
            fw = int(cfg["aee.fail_windows"])
            promote = float(cfg["promote_mfe_atr"])
            ext_e = float(cfg["extension_allow_energy_min"])

            # Surrogate behavior model.
            lift_exit = (1.0 - strict) * 0.30 + (2 - fw) * 0.12 + (0.35 - promote) * 0.08 + (1.0 - ext_e) * 0.05
            mode_shift = (0.35 - promote) * 0.55 + (1.0 - ext_e) * 0.25 - (strict - 1.0) * 0.10
            cap_shift = (0.30 - promote) * 0.06 + (1.0 - ext_e) * 0.03 - max(0.0, 0.95 - strict) * 0.01
            tail_penalty = 0.0
            if strict <= 0.85 and fw <= 1:
                tail_penalty += 0.01
            if promote <= 0.15:
                tail_penalty += 0.005

            exit_h = max(0.0, base_exit_h * (1.0 + lift_exit))
            expected = base_exp + (0.00001 * lift_exit) + (0.00002 * mode_shift) - (0.00001 * tail_penalty)
            capture = base_cap + cap_shift
            mode_balance = max(0.0, min(1.0, base_mode + mode_shift))
            ratio_ext = max(0.0, min(1.0, float(base["ratio_extension"]) + max(0.0, (1.0 - ext_e) * 0.20)))
            ratio_h = max(0.0, min(1.0, float(base["ratio_harvest"]) + max(0.0, (0.35 - promote) * 0.80)))
            ratio_l = max(0.0, min(1.0, 1.0 - ratio_h - ratio_ext))
            tail = base_tail - tail_penalty

            guard = {
                "min_n_ok": n >= 12,
                "mode_balance_ok": (ratio_h + ratio_ext) >= 0.20,
                "tail_ok": tail >= -0.15,
                "expected_not_worse": expected >= (base_exp - 0.00001),
                "exit_not_down": exit_h >= (base_exit_h * 0.90 if base_exit_h > 0 else 0.30),
            }
            fail = not all(guard.values())
            score = (expected + (0.20 * capture) + (0.05 * (ratio_h + 0.5 * ratio_ext))) if not fail else -1e9
            evaluated.append(
                {
                    "cfg": cfg,
                    "score": round(score, 8),
                    "metrics": {
                        "n": n,
                        "exit_result_per_h": round(exit_h, 6),
                        "expected_extraction_atr": round(expected, 6),
                        "capture_ratio_atr_mean": round(capture, 6),
                        "ratio_extension": round(ratio_ext, 6),
                        "ratio_harvest": round(ratio_h, 6),
                        "ratio_loss_ctrl": round(ratio_l, 6),
                        "mode_balance": round(ratio_h + 0.5 * ratio_ext, 6),
                        "tail_value": round(tail, 6),
                    },
                    "guardrails": guard,
                }
            )

        ranked = sorted(
            evaluated,
            key=lambda r: (
                r["score"],
                r["metrics"]["expected_extraction_atr"],
                r["metrics"]["mode_balance"],
                r["metrics"]["capture_ratio_atr_mean"],
            ),
            reverse=True,
        )
        top = ranked[:3]
        chosen = top[0] if top else None
        results.append(
            {
                "bucket_key": {"pair": pair, "session": session, "quarter": quarter},
                "baseline": {**base, "aee_reason_top": aee_reasons[(pair, session, quarter)].most_common(5)},
                "candidates_tested": len(configs),
                "top": top,
                "chosen": chosen,
            }
        )
        if chosen and chosen["score"] > -1e8:
            patches.append(
                {
                    "key": {"pair": pair, "session": session, "quarter": quarter},
                    "entry_patch": {},
                    "aee_patch": chosen["cfg"],
                    "evidence": chosen["metrics"],
                }
            )

    sweep = {
        "windows_utc": {"start": start.isoformat(), "end": now.isoformat()},
        "spec": {
            "bucket_level": "Tier0_pair",
            "targets": [{"pair": p, "session": s, "quarter": q} for p, s, q in targets],
            "mode": "surrogate_behavior_from_logs",
            "entry_frozen": True,
            "knobs": grid,
            "guardrails": {
                "min_n": 12,
                "mode_balance_min": 0.20,
                "tail_ok_floor": -0.15,
                "expected_not_worse_eps": 0.00001,
                "exit_not_down_ratio": 0.90,
            },
            "score_formula": "expected_extraction_atr + 0.2*capture_ratio_atr_mean + 0.05*(ratio_harvest + 0.5*ratio_extension)",
        },
        "results": results,
    }
    tune = {
        "version": f"TUNE_MAP_v1_NYQ1_BEHAV_{now.strftime('%Y%m%dT%H%M%SZ')}",
        "created_utc": now.isoformat(),
        "bucket_level": "Tier0_pair",
        "mode": "surrogate_behavior_from_logs",
        "patches": patches,
    }

    out_sweep = Path(args.out_sweep)
    out_sweep.parent.mkdir(parents=True, exist_ok=True)
    out_sweep.write_text(json.dumps(sweep, indent=2), encoding="utf-8")
    out_map = Path(args.out_tune_map)
    out_map.parent.mkdir(parents=True, exist_ok=True)
    out_map.write_text(json.dumps(tune, indent=2), encoding="utf-8")
    print(f"WROTE {out_sweep}")
    print(f"WROTE {out_map}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

