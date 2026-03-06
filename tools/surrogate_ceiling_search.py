#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
except Exception:  # pragma: no cover
    RandomForestClassifier = None
    RandomForestRegressor = None

PAIRS_15 = "EUR_USD,GBP_USD,USD_JPY,USD_CHF,AUD_USD,USD_CAD,NZD_USD,EUR_GBP,EUR_JPY,GBP_JPY,AUD_JPY,CHF_JPY,EUR_CHF,AUD_CAD,NZD_JPY"


@dataclass
class Trial:
    knobs: dict[str, float | int]
    s1: dict[str, float]
    s2: dict[str, float]
    passed: bool
    score: float


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save_json(path: str, obj: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _sample_knobs(space: dict[str, dict[str, Any]], rng: random.Random) -> dict[str, float | int]:
    out: dict[str, float | int] = {}
    for k, spec in space.items():
        kind = str(spec.get("type", "float"))
        if kind == "int":
            lo = int(spec["min"])
            hi = int(spec["max"])
            step = int(spec.get("step", 1))
            vals = list(range(lo, hi + 1, max(1, step)))
            out[k] = int(rng.choice(vals))
        else:
            lo = float(spec["min"])
            hi = float(spec["max"])
            step = float(spec.get("step", 0.01))
            n = int(round((hi - lo) / step))
            v = lo + step * rng.randint(0, max(0, n))
            out[k] = round(float(v), 6)
    return out


def _knob_id(knobs: dict[str, float | int]) -> str:
    parts = [f"{k}={knobs[k]}" for k in sorted(knobs.keys())]
    return "|".join(parts)


def _merge_patch(base_patch: dict[str, Any], pocket_keys: list[str], knobs: dict[str, float | int]) -> dict[str, Any]:
    out = json.loads(json.dumps(base_patch))
    patches = out.setdefault("patches", [])
    for key in pocket_keys:
        patches.append({"level": "SESSION_PAIR", "key": key, "knobs": dict(knobs)})
    return out


def _run_verify(args: argparse.Namespace, cache: str, patch_path: str, out_path: str) -> dict[str, Any]:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--tape-root",
        args.tape_root,
        "--pairs",
        args.pairs,
        "--start-utc",
        args.start_utc,
        "--end-utc",
        args.end_utc,
        "--ceiling-mode",
        args.ceiling_mode,
        "--base-cache-in",
        cache,
        "--active-artifacts",
        args.active_artifacts,
        "--patch",
        patch_path,
        "--min-touched-targets",
        str(args.min_touched_targets),
        "--min-vol-bucket-touched",
        str(args.min_vol_bucket_touched),
        "--out",
        out_path,
    ]
    if args.enforce_tier_touches:
        cmd.append("--enforce-tier-touches")
    if args.enforce_quarter_no_shadow:
        cmd.append("--enforce-quarter-no-shadow")
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-1000:])
    return _load_json(out_path)


def _extract_metrics(doc: dict[str, Any]) -> dict[str, float]:
    d = doc.get("delta_vs_nopatch") or {}
    return {
        "ddEph": float(d.get("ddEph", 0.0) or 0.0),
        "ddCAP": float(d.get("ddCAP_mean", 0.0) or 0.0),
        "ddTail": float(d.get("ddTail_mean_Eph", 0.0) or 0.0),
        "ddEE": float(d.get("ddEE_mean", 0.0) or 0.0),
        "ddExits": float(d.get("ddExits_per_hour", 0.0) or 0.0),
        "touched": float(d.get("touched_targets", 0.0) or 0.0),
    }


def _passes(m1: dict[str, float], m2: dict[str, float], args: argparse.Namespace) -> bool:
    return (
        m1["ddEph"] >= args.min_dd_eph
        and m2["ddEph"] >= args.min_dd_eph
        and m1["ddCAP"] >= args.min_dd_cap
        and m2["ddCAP"] >= args.min_dd_cap
        and m1["ddTail"] >= args.min_dd_tail
        and m2["ddTail"] >= args.min_dd_tail
        and m1["ddEE"] >= args.min_dd_ee
        and m2["ddEE"] >= args.min_dd_ee
        and m1["ddExits"] >= args.min_dd_exits
        and m2["ddExits"] >= args.min_dd_exits
        and m1["touched"] >= args.min_touched_targets
        and m2["touched"] >= args.min_touched_targets
    )


def _score(m1: dict[str, float], m2: dict[str, float], args: argparse.Namespace) -> float:
    dd_eph_min = min(m1["ddEph"], m2["ddEph"])
    dd_cap_min = min(m1["ddCAP"], m2["ddCAP"])
    tail_worst = max(-m1["ddTail"], -m2["ddTail"], 0.0)
    touched_drop = max(0.0, args.min_touched_targets - min(m1["touched"], m2["touched"]))
    return (
        args.w_eph * dd_eph_min
        + args.w_cap * dd_cap_min
        - args.w_tail * tail_worst
        - args.w_touch * touched_drop
    )


def _vectorize(knobs: dict[str, float | int], keys: list[str]) -> list[float]:
    return [float(knobs[k]) for k in keys]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True, choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])
    ap.add_argument("--pair", required=True)
    ap.add_argument("--base-patch", required=True)
    ap.add_argument("--space-json", default="")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--bootstrap", type=int, default=16)
    ap.add_argument("--rounds", type=int, default=6)
    ap.add_argument("--batch-size", type=int, default=6)
    ap.add_argument("--cand-pool", type=int, default=2000)
    ap.add_argument("--plateau-eps", type=float, default=1e-9)
    ap.add_argument("--plateau-rounds", type=int, default=1)
    ap.add_argument("--base-cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--base-cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--tape-root", default="data_tape_oanda_m5_15_stitched")
    ap.add_argument("--pairs", default=PAIRS_15)
    ap.add_argument("--start-utc", default="2025-12-01T00:00:00Z")
    ap.add_argument("--end-utc", default="2026-03-01T00:00:00Z")
    ap.add_argument("--ceiling-mode", default="first_passage", choices=["proxy", "first_passage"])
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--enforce-tier-touches", action="store_true", default=True)
    ap.add_argument("--enforce-quarter-no-shadow", action="store_true", default=True)
    ap.add_argument("--min-touched-targets", type=int, default=24)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=4)
    ap.add_argument("--min-dd-eph", type=float, default=0.0)
    ap.add_argument("--min-dd-cap", type=float, default=0.0)
    ap.add_argument("--min-dd-tail", type=float, default=0.0)
    ap.add_argument("--min-dd-ee", type=float, default=0.0)
    ap.add_argument("--min-dd-exits", type=float, default=0.0)
    ap.add_argument("--w-eph", type=float, default=1.0)
    ap.add_argument("--w-cap", type=float, default=0.8)
    ap.add_argument("--w-tail", type=float, default=1.3)
    ap.add_argument("--w-touch", type=float, default=0.6)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    base_patch = _load_json(args.base_patch)

    if args.space_json:
        space = _load_json(args.space_json)
    else:
        space = {
            "aee.fail_windows": {"type": "int", "min": 2, "max": 6, "step": 1},
            "aee.strictness_mult": {"type": "float", "min": 0.9, "max": 1.25, "step": 0.05},
            "extension_allow_energy_min": {"type": "float", "min": 0.9, "max": 1.3, "step": 0.05},
            "promote_mfe_atr": {"type": "float", "min": 0.10, "max": 0.45, "step": 0.05},
            "entry.tick.base_max_dist_atr": {"type": "float", "min": 0.05, "max": 0.30, "step": 0.05},
            "entry.tick.confirm_disp_atr": {"type": "float", "min": 0.10, "max": 0.35, "step": 0.05},
        }

    p = args.pair.upper()
    sq = f"{args.session}_{args.quarter}"
    pocket_keys = [f"{p}|{sq}|VOL_LOW", f"{p}|{sq}|VOL_MID", f"{p}|{sq}|VOL_HIGH"]

    knob_keys = sorted(space.keys())
    trials: list[Trial] = []
    seen: set[str] = set()
    work = Path("calibration/candidates/surrogate")
    work.mkdir(parents=True, exist_ok=True)

    def evaluate(knobs: dict[str, float | int], idx: int) -> Trial | None:
        kid = _knob_id(knobs)
        if kid in seen:
            return None
        seen.add(kid)
        patch_obj = _merge_patch(base_patch, pocket_keys, knobs)
        patch_path = work / f"{p}_{sq}_trial_{idx:04d}.json"
        _save_json(str(patch_path), patch_obj)
        try:
            s1_doc = _run_verify(args, args.base_cache_s1, str(patch_path), f"proof_artifacts/SURR_{p}_{sq}_{idx:04d}_S1.json")
            s2_doc = _run_verify(args, args.base_cache_s2, str(patch_path), f"proof_artifacts/SURR_{p}_{sq}_{idx:04d}_S2.json")
        except Exception:
            return Trial(knobs=knobs, s1={}, s2={}, passed=False, score=-1e9)
        m1 = _extract_metrics(s1_doc)
        m2 = _extract_metrics(s2_doc)
        passed = _passes(m1, m2, args)
        sc = _score(m1, m2, args) if passed else -1e6
        return Trial(knobs=knobs, s1=m1, s2=m2, passed=passed, score=sc)

    idx = 0
    while len(trials) < args.bootstrap:
        t = evaluate(_sample_knobs(space, rng), idx)
        idx += 1
        if t is not None:
            trials.append(t)

    no_improve_rounds = 0
    prev_best = max((t.score for t in trials), default=-1e9)
    for _ in range(args.rounds):
        if RandomForestRegressor is None:
            for _i in range(args.batch_size):
                t = evaluate(_sample_knobs(space, rng), idx)
                idx += 1
                if t is not None:
                    trials.append(t)
            cur_best = max((t.score for t in trials), default=-1e9)
            if cur_best <= prev_best + float(args.plateau_eps):
                no_improve_rounds += 1
            else:
                no_improve_rounds = 0
            prev_best = cur_best
            if no_improve_rounds >= int(args.plateau_rounds):
                break
            continue

        xs = [_vectorize(t.knobs, knob_keys) for t in trials]
        ys = [float(t.score) for t in trials]
        ypass = [1 if t.passed else 0 for t in trials]
        reg = RandomForestRegressor(n_estimators=200, random_state=args.seed)
        clf = RandomForestClassifier(n_estimators=200, random_state=args.seed)
        reg.fit(xs, ys)
        clf.fit(xs, ypass)

        pool: list[tuple[float, dict[str, float | int]]] = []
        best_score = max(ys) if ys else -1e9
        for _j in range(args.cand_pool):
            k = _sample_knobs(space, rng)
            kid = _knob_id(k)
            if kid in seen:
                continue
            x = _vectorize(k, knob_keys)
            trees = reg.estimators_
            preds = [float(est.predict([x])[0]) for est in trees]
            mu = sum(preds) / len(preds)
            std = math.sqrt(sum((p_ - mu) ** 2 for p_ in preds) / max(1, len(preds) - 1))
            p_pass = float(clf.predict_proba([x])[0][1])
            acq = ((mu - best_score) + 0.5 * std) * p_pass
            pool.append((acq, k))
        pool.sort(key=lambda z: z[0], reverse=True)
        picked = 0
        for _acq, k in pool:
            t = evaluate(k, idx)
            idx += 1
            if t is not None:
                trials.append(t)
                picked += 1
            if picked >= args.batch_size:
                break
        if picked == 0:
            break
        cur_best = max((t.score for t in trials), default=-1e9)
        if cur_best <= prev_best + float(args.plateau_eps):
            no_improve_rounds += 1
        else:
            no_improve_rounds = 0
        prev_best = cur_best
        if no_improve_rounds >= int(args.plateau_rounds):
            break

    ranked = sorted(trials, key=lambda t: t.score, reverse=True)
    out_rows = []
    for t in ranked:
        out_rows.append(
            {
                "knobs": t.knobs,
                "passed": t.passed,
                "score": t.score,
                "s1": t.s1,
                "s2": t.s2,
            }
        )

    out = {
        "pair": p,
        "session": args.session,
        "quarter": args.quarter,
        "pocket_keys": pocket_keys,
        "base_patch": args.base_patch,
        "space": space,
        "trials_total": len(trials),
        "ranked": out_rows,
    }
    _save_json(args.out_json, out)

    md = [f"# Surrogate Ceiling Search {p} {sq}", "", f"- trials: `{len(trials)}`", f"- base_patch: `{args.base_patch}`", ""]
    md.append("## Top 10")
    for i, t in enumerate(out_rows[:10], start=1):
        md.append(
            f"- {i}. score={t['score']:.6f} pass={t['passed']} knobs={t['knobs']} "
            f"S1(ddEph={t['s1'].get('ddEph',0):.6f},ddCAP={t['s1'].get('ddCAP',0):.6f},ddTail={t['s1'].get('ddTail',0):.6f}) "
            f"S2(ddEph={t['s2'].get('ddEph',0):.6f},ddCAP={t['s2'].get('ddCAP',0):.6f},ddTail={t['s2'].get('ddTail',0):.6f})"
        )
    Path(args.out_md).write_text("\n".join(md) + "\n", encoding="utf-8")

    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
