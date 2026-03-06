#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


QUARTERS = ("Q1", "Q2", "Q3", "Q4")


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


def _quarter_anchor(patch: dict[str, Any], quarter: str) -> float:
    vals: list[float] = []
    for p in patch.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        key = str(p.get("key", "") or "")
        if f"|ASIA_{quarter}|" not in key:
            continue
        if not key.endswith("|VOL_MID"):
            continue
        k = p.get("knobs") or {}
        vals.append(float(k.get("aee.strictness_mult", 1.0) or 1.0))
    return sum(vals) / len(vals) if vals else 1.0


def _apply_quarter_anchor_delta(base_patch: dict[str, Any], quarter: str, delta: float) -> dict[str, Any]:
    out = json.loads(json.dumps(base_patch))
    # derive per-family offsets from current quarter MID
    fam_mid: dict[str, float] = {}
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        key = str(p.get("key", "") or "")
        if not (str(p.get("level", "")) == "SESSION_FAMILY" and f"|ASIA_{quarter}|" in key and key.endswith("|VOL_MID")):
            continue
        fam = key.split("|", 1)[0]
        fam_mid[fam] = float((p.get("knobs") or {}).get("aee.strictness_mult", 1.0) or 1.0)
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        key = str(p.get("key", "") or "")
        if f"|ASIA_{quarter}|" not in key:
            continue
        fam = key.split("|", 1)[0]
        mid0 = fam_mid.get(fam, 1.0)
        knobs = p.setdefault("knobs", {})
        cur = float(knobs.get("aee.strictness_mult", 1.0) or 1.0)
        offset = cur - mid0
        knobs["aee.strictness_mult"] = float(mid0 + delta + offset)
    return out


def _score(m1: dict[str, float], m2: dict[str, float]) -> tuple[float, float, float]:
    return (min(m1["ddEph"], m2["ddEph"]), (m1["ddTail"] + m2["ddTail"]) / 2.0, (m1["ddCAP"] + m2["ddCAP"]) / 2.0)


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
    ap.add_argument("--promote-threshold", type=float, default=0.01)
    ap.add_argument("--max-iters", type=int, default=2)
    ap.add_argument("--out-json", default="proof_artifacts/ASIA_QUARTER_MID_SWEEP.json")
    ap.add_argument("--out-md", default="proof_artifacts/ASIA_QUARTER_MID_SWEEP.md")
    args = ap.parse_args()

    base_patch = _load(args.start_patch)
    baseline_s1 = _run_replay(args.base_cache_s1, args.start_patch, "proof_artifacts/ASIA_QMID_BASE_S1.json", args)
    baseline_s2 = _run_replay(args.base_cache_s2, args.start_patch, "proof_artifacts/ASIA_QMID_BASE_S2.json", args)
    bm1 = _metrics(baseline_s1)
    bm2 = _metrics(baseline_s2)
    base_score = _score(bm1, bm2)

    work = Path("calibration/candidates/asia_quarter_mid")
    work.mkdir(parents=True, exist_ok=True)
    cur_patch = base_patch
    cur_best_score = base_score
    history: list[dict[str, Any]] = []
    deltas = [-0.03, 0.0, 0.03]

    for it in range(1, int(args.max_iters) + 1):
        best_row = None
        for q in QUARTERS:
            for d in deltas:
                cand = _apply_quarter_anchor_delta(cur_patch, q, d)
                tag = f"it{it}_{q}_d{d:+.2f}".replace(".", "p").replace("+", "p").replace("-", "m")
                cp = str(work / f"{tag}.json")
                _save(cp, cand)
                try:
                    o1 = _run_replay(args.base_cache_s1, cp, f"proof_artifacts/ASIA_QMID_{tag}_S1.json", args)
                    o2 = _run_replay(args.base_cache_s2, cp, f"proof_artifacts/ASIA_QMID_{tag}_S2.json", args)
                    m1 = _metrics(o1)
                    m2 = _metrics(o2)
                    gated = _gated(m1, m2)
                    row = {
                        "iter": it,
                        "quarter": q,
                        "delta": d,
                        "patch": cp,
                        "s1": m1,
                        "s2": m2,
                        "gated": gated,
                        "score": _score(m1, m2),
                    }
                except Exception as e:
                    row = {"iter": it, "quarter": q, "delta": d, "patch": cp, "gated": False, "error": str(e), "score": (-1e9, -1e9, -1e9)}
                history.append(row)
                if row.get("gated"):
                    if best_row is None or tuple(row["score"]) > tuple(best_row["score"]):
                        best_row = row
        if best_row is None:
            break
        if tuple(best_row["score"]) <= tuple(cur_best_score):
            break
        cur_patch = _load(best_row["patch"])
        cur_best_score = tuple(best_row["score"])

    gated_rows = [r for r in history if r.get("gated")]
    gated_rows.sort(key=lambda r: tuple(r["score"]), reverse=True)
    best = gated_rows[0] if gated_rows else None
    frontier_dominant = bool(best and (best["score"][0] - base_score[0]) >= float(args.promote_threshold))

    out = {
        "baseline": {"s1": bm1, "s2": bm2, "score": base_score},
        "best": best,
        "frontier_dominant_vs_active": frontier_dominant,
        "promote_threshold": float(args.promote_threshold),
        "history": history,
    }
    _save(args.out_json, out)

    lines = ["# ASIA Quarter MID Sweep", ""]
    lines.append(f"- baseline score min(ddEph)={base_score[0]:.6f} avgTail={base_score[1]:.6f} avgCAP={base_score[2]:.6f}")
    if best:
        lines.append(
            f"- best iter={best['iter']} quarter={best['quarter']} delta={best['delta']:+.2f} "
            f"min(ddEph)={best['score'][0]:.6f} frontier_dominant={frontier_dominant}"
        )
    else:
        lines.append("- best: none")
    lines.append("")
    lines.append("| iter | quarter | delta | gated | min_ddEph | avg_ddTail | avg_ddCAP |")
    lines.append("|---:|---|---:|:---:|---:|---:|---:|")
    for r in history:
        sc = r.get("score", [0, 0, 0])
        lines.append(
            f"| {r.get('iter')} | {r.get('quarter')} | {float(r.get('delta',0)):+.2f} | "
            f"{'Y' if r.get('gated') else 'N'} | {float(sc[0]):.6f} | {float(sc[1]):.6f} | {float(sc[2]):.6f} |"
        )
    Path(args.out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
