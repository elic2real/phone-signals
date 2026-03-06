#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


def _run(cache: str, patch: str, out: str, args: argparse.Namespace) -> dict[str, Any]:
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


def _m(o: dict[str, Any]) -> dict[str, float]:
    d = o.get("delta_vs_nopatch") or {}
    return {
        "ddEph": float(d.get("ddEph", 0.0) or 0.0),
        "ddCAP": float(d.get("ddCAP_mean", 0.0) or 0.0),
        "ddTail": float(d.get("ddTail_mean_Eph", 0.0) or 0.0),
        "ddEE": float(d.get("ddEE_mean", 0.0) or 0.0),
        "ddExitsH": float(d.get("ddExits_per_hour", 0.0) or 0.0),
    }


def _gated(a: dict[str, float], b: dict[str, float]) -> bool:
    return (
        a["ddEE"] > 0.0 and b["ddEE"] > 0.0 and
        a["ddCAP"] >= 0.0 and b["ddCAP"] >= 0.0 and
        a["ddEph"] > 0.0 and b["ddEph"] > 0.0 and
        a["ddTail"] >= 0.0 and b["ddTail"] >= 0.0 and
        a["ddExitsH"] >= 0.0 and b["ddExitsH"] >= 0.0
    )


def _score(a: dict[str, float], b: dict[str, float]) -> tuple[float, float, float]:
    return (min(a["ddEph"], b["ddEph"]), (a["ddTail"] + b["ddTail"]) / 2.0, (a["ddCAP"] + b["ddCAP"]) / 2.0)


def _apply_delta(base: dict[str, Any], session: str, quarter: str, delta: float) -> dict[str, Any]:
    out = json.loads(json.dumps(base))
    fam_mid: dict[str, float] = {}
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        key = str(p.get("key", "") or "")
        if f"|{session}_{quarter}|" in key and key.endswith("|VOL_MID"):
            fam_mid[key.split("|", 1)[0]] = float((p.get("knobs") or {}).get("aee.strictness_mult", 1.0) or 1.0)
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        key = str(p.get("key", "") or "")
        if f"|{session}_{quarter}|" not in key:
            continue
        fam = key.split("|", 1)[0]
        mid0 = fam_mid.get(fam, 1.0)
        knobs = p.setdefault("knobs", {})
        cur = float(knobs.get("aee.strictness_mult", 1.0) or 1.0)
        offset = cur - mid0
        knobs["aee.strictness_mult"] = float(mid0 + delta + offset)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True, choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--start-patch", required=True)
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--vol-spec", required=True)
    ap.add_argument("--base-cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--base-cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--ceiling-mode", default="first_passage")
    ap.add_argument("--quarters", default="Q2,Q3")
    ap.add_argument("--deltas", default="-0.03,0.00,0.03")
    ap.add_argument("--min-touched-targets", type=int, default=24)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=4)
    ap.add_argument("--promote-threshold", type=float, default=0.01)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    base = _load(args.start_patch)
    qlist = [q.strip() for q in args.quarters.split(",") if q.strip()]
    dlist = [float(x.strip()) for x in args.deltas.split(",") if x.strip()]

    b1 = _run(args.base_cache_s1, args.start_patch, f"proof_artifacts/{args.session}_FOCUS_BASE_S1.json", args)
    b2 = _run(args.base_cache_s2, args.start_patch, f"proof_artifacts/{args.session}_FOCUS_BASE_S2.json", args)
    bm1, bm2 = _m(b1), _m(b2)
    bscore = _score(bm1, bm2)

    rows = []
    best = None
    outdir = Path(f"calibration/candidates/{args.session.lower()}_quarter_focus")
    outdir.mkdir(parents=True, exist_ok=True)
    for q in qlist:
        for d in dlist:
            cp = str(outdir / f"{args.session}_{q}_d{d:+.2f}.json".replace(".", "p").replace("+", "p").replace("-", "m"))
            cand = _apply_delta(base, args.session, q, d)
            _save(cp, cand)
            try:
                o1 = _run(args.base_cache_s1, cp, f"proof_artifacts/{args.session}_FOCUS_{q}_{d:+.2f}_S1.json".replace(".", "p").replace("+", "p").replace("-", "m"), args)
                o2 = _run(args.base_cache_s2, cp, f"proof_artifacts/{args.session}_FOCUS_{q}_{d:+.2f}_S2.json".replace(".", "p").replace("+", "p").replace("-", "m"), args)
                m1, m2 = _m(o1), _m(o2)
                gated = _gated(m1, m2)
                score = _score(m1, m2)
                row = {"quarter": q, "delta": d, "patch": cp, "s1": m1, "s2": m2, "gated": gated, "score": score}
            except Exception as e:
                row = {"quarter": q, "delta": d, "patch": cp, "gated": False, "score": (-1e9, -1e9, -1e9), "error": str(e)}
            rows.append(row)
            if row.get("gated"):
                if best is None or tuple(row["score"]) > tuple(best["score"]):
                    best = row

    dominant = bool(best and (best["score"][0] - bscore[0]) >= float(args.promote_threshold))
    out = {
        "session": args.session,
        "baseline": {"s1": bm1, "s2": bm2, "score": bscore},
        "best": best,
        "frontier_dominant_vs_active": dominant,
        "promote_threshold": float(args.promote_threshold),
        "candidates": rows,
    }
    _save(args.out_json, out)

    md = [f"# {args.session} Quarter Focus Sweep", ""]
    md.append(f"- baseline min(ddEph)={bscore[0]:.6f} avgTail={bscore[1]:.6f} avgCAP={bscore[2]:.6f}")
    if best:
        md.append(f"- best {best['quarter']} {best['delta']:+.2f}: min(ddEph)={best['score'][0]:.6f} dominant={dominant}")
    else:
        md.append("- best: none")
    md.append("")
    md.append("| quarter | delta | gated | min_ddEph | avg_ddTail | avg_ddCAP |")
    md.append("|---|---:|:---:|---:|---:|---:|")
    for r in rows:
        s = r.get("score", [0, 0, 0])
        md.append(f"| {r.get('quarter')} | {float(r.get('delta',0)):+.2f} | {'Y' if r.get('gated') else 'N'} | {float(s[0]):.6f} | {float(s[1]):.6f} | {float(s[2]):.6f} |")
    Path(args.out_md).write_text("\n".join(md) + "\n", encoding="utf-8")
    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

