#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
import subprocess
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save(path: str, obj: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _run_replay(cache: str, patch: str, out: str, args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--base-cache-in",
        cache,
        "--active-artifacts",
        args.active_artifacts,
        "--vol-spec",
        args.vol_spec,
        "--patch",
        patch,
        "--ceiling-mode",
        args.ceiling_mode,
        "--enforce-tier-touches",
        "--enforce-quarter-no-shadow",
        "--min-touched-targets",
        str(args.min_touched_targets),
        "--min-vol-bucket-touched",
        str(args.min_vol_bucket_touched),
        "--out",
        out,
    ]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-1200:])
    return _load(out)


def _metrics(obj: dict[str, Any]) -> dict[str, float]:
    dd = obj.get("delta_vs_nopatch") or {}
    return {
        "ddEph": float(dd.get("ddEph", 0.0) or 0.0),
        "ddCAP": float(dd.get("ddCAP_mean", 0.0) or 0.0),
        "ddTail": float(dd.get("ddTail_mean_Eph", 0.0) or 0.0),
        "ddEE": float(dd.get("ddEE_mean", 0.0) or 0.0),
        "ddExitsH": float(dd.get("ddExits_per_hour", 0.0) or 0.0),
    }


def _gated(m1: dict[str, float], m2: dict[str, float]) -> bool:
    return (
        m1["ddEE"] > 0.0
        and m2["ddEE"] > 0.0
        and m1["ddCAP"] >= 0.0
        and m2["ddCAP"] >= 0.0
        and m1["ddEph"] > 0.0
        and m2["ddEph"] > 0.0
        and m1["ddTail"] >= 0.0
        and m2["ddTail"] >= 0.0
        and m1["ddExitsH"] >= 0.0
        and m2["ddExitsH"] >= 0.0
    )


def _apply_monotone(base_patch: dict[str, Any], low: float, mid: float, high: float) -> dict[str, Any]:
    out = json.loads(json.dumps(base_patch))
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        key = str(p.get("key", "") or "")
        if "|ASIA_Q" not in key:
            continue
        k = p.setdefault("knobs", {})
        if key.endswith("|VOL_LOW"):
            k["aee.strictness_mult"] = float(low)
        elif key.endswith("|VOL_MID"):
            k["aee.strictness_mult"] = float(mid)
        elif key.endswith("|VOL_HIGH"):
            k["aee.strictness_mult"] = float(high)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-patch", default="calibration/active/tune_map_patch_active_asia.json")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--vol-spec", default="calibration/active/vol_bucket_spec_active_asia.json")
    ap.add_argument("--base-cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--base-cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--ceiling-mode", default="first_passage")
    ap.add_argument("--min-touched-targets", type=int, default=24)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=4)
    ap.add_argument("--out-json", default="proof_artifacts/ASIA_VOL_MONOTONE_SWEEP.json")
    ap.add_argument("--out-md", default="proof_artifacts/ASIA_VOL_MONOTONE_SWEEP.md")
    ap.add_argument("--promote-threshold", type=float, default=0.01)
    args = ap.parse_args()

    base_patch = _load(args.start_patch)
    work = Path("calibration/candidates/asia_vol_monotone")
    work.mkdir(parents=True, exist_ok=True)

    baseline_s1 = _run_replay(args.base_cache_s1, args.start_patch, "proof_artifacts/ASIA_BASELINE_ACTIVE_S1.json", args)
    baseline_s2 = _run_replay(args.base_cache_s2, args.start_patch, "proof_artifacts/ASIA_BASELINE_ACTIVE_S2.json", args)
    bm1 = _metrics(baseline_s1)
    bm2 = _metrics(baseline_s2)
    base_min_ddeph = min(bm1["ddEph"], bm2["ddEph"])

    lows = [1.03, 1.05, 1.07]
    mids = [1.00]
    highs = [0.97, 0.95, 0.93]
    candidates: list[dict[str, Any]] = []
    for low, mid, high in itertools.product(lows, mids, highs):
        if not (low >= mid >= high):
            continue
        tag = f"low{low:.2f}_mid{mid:.2f}_high{high:.2f}".replace(".", "p")
        cp = str(work / f"{tag}.json")
        cand_patch = _apply_monotone(base_patch, low, mid, high)
        _save(cp, cand_patch)
        try:
            o1 = _run_replay(args.base_cache_s1, cp, f"proof_artifacts/ASIA_VOL_{tag}_S1.json", args)
            o2 = _run_replay(args.base_cache_s2, cp, f"proof_artifacts/ASIA_VOL_{tag}_S2.json", args)
            m1 = _metrics(o1)
            m2 = _metrics(o2)
            gated = _gated(m1, m2)
            row = {
                "tag": tag,
                "patch": cp,
                "low": low,
                "mid": mid,
                "high": high,
                "s1": m1,
                "s2": m2,
                "gated": gated,
                "min_ddEph": min(m1["ddEph"], m2["ddEph"]),
                "avg_ddEph": (m1["ddEph"] + m2["ddEph"]) / 2.0,
                "avg_ddTail": (m1["ddTail"] + m2["ddTail"]) / 2.0,
                "avg_ddCAP": (m1["ddCAP"] + m2["ddCAP"]) / 2.0,
            }
        except Exception as e:
            row = {"tag": tag, "patch": cp, "low": low, "mid": mid, "high": high, "gated": False, "error": str(e)}
        candidates.append(row)

    gated_rows = [r for r in candidates if r.get("gated")]
    gated_rows.sort(key=lambda r: (r.get("min_ddEph", -1e9), r.get("avg_ddTail", -1e9), r.get("avg_ddCAP", -1e9)), reverse=True)
    best = gated_rows[0] if gated_rows else None
    frontier_dominant = False
    if best is not None:
        frontier_dominant = (best["min_ddEph"] - base_min_ddeph) >= float(args.promote_threshold)

    payload = {
        "baseline": {"s1": bm1, "s2": bm2, "min_ddEph": base_min_ddeph},
        "best": best,
        "frontier_dominant_vs_active": frontier_dominant,
        "promote_threshold": float(args.promote_threshold),
        "candidates": candidates,
    }
    _save(args.out_json, payload)

    lines = ["# ASIA VOL Monotone Sweep", ""]
    lines.append(
        f"- baseline min(ddEph): {base_min_ddeph:.6f} "
        f"(S1={bm1['ddEph']:.6f}, S2={bm2['ddEph']:.6f})"
    )
    if best is not None:
        lines.append(
            f"- best: `{best['tag']}` low/mid/high={best['low']:.2f}/{best['mid']:.2f}/{best['high']:.2f} "
            f"min(ddEph)={best['min_ddEph']:.6f} frontier_dominant={frontier_dominant}"
        )
    else:
        lines.append("- best: none (no gated candidates)")
    lines.append("")
    lines.append("| tag | low | mid | high | gated | min_ddEph | avg_ddTail | avg_ddCAP |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in candidates:
        lines.append(
            f"| {r.get('tag')} | {r.get('low',0):.2f} | {r.get('mid',0):.2f} | {r.get('high',0):.2f} | "
            f"{'Y' if r.get('gated') else 'N'} | {float(r.get('min_ddEph',0.0)):.6f} | "
            f"{float(r.get('avg_ddTail',0.0)):.6f} | {float(r.get('avg_ddCAP',0.0)):.6f} |"
        )
    Path(args.out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
