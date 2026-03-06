#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import math
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _i(x: Any) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0


def _infer_target_ddcap(obj: dict[str, Any]) -> dict[str, float]:
    rows = obj.get("rows") or []
    targets = obj.get("targets") or []
    out: dict[str, float] = {}
    if not rows or not targets:
        return out
    for t in targets:
        pair = str(t.get("pair", "") or "")
        session = str(t.get("session", "") or "")
        quarter = str(t.get("quarter", "") or "")
        vol_bucket = str(t.get("vol_bucket", "") or "")
        key = str(t.get("target_key", "") or "")
        if not key:
            continue
        num = 0.0
        den = 0.0
        for r in rows:
            if str(r.get("pair", "") or "") != pair:
                continue
            if str(r.get("session", "") or "") != session:
                continue
            if quarter and str(r.get("quarter", "") or "") != quarter:
                continue
            if vol_bucket and str(r.get("vol_bucket", "") or "") != vol_bucket:
                continue
            w = _f(r.get("n"))
            num += _f(r.get("delta_capture_to_ceiling_vs_nopatch")) * w
            den += w
        out[key] = num / den if den > 0 else 0.0
    return out


def evaluate(
    path: str,
    min_touched: int,
    min_vol_bucket: int,
    max_neg_ddcap_count: int,
    max_neg_ddcap_frac: float,
    min_worst_ddcap: float,
    skip_worst_ddcap: bool = False,
) -> list[str]:
    obj = _load(path)
    errs: list[str] = []
    dd = obj.get("delta_vs_nopatch") or {}
    src = obj.get("source") or {}

    required_top = ("cache_fingerprint", "delta_vs_nopatch")
    for k in required_top:
        if k not in obj:
            errs.append(f"{path}: missing required field '{k}'")

    if not str(src.get("active_artifacts_sha256", "") or ""):
        errs.append(f"{path}: missing source.active_artifacts_sha256")
    session_spec_sha = src.get("session_vol_specs_sha256") or {}
    for s in ("ASIA", "LONDON", "NY"):
        if not str((session_spec_sha.get(s, "") or "")).strip():
            errs.append(f"{path}: missing source.session_vol_specs_sha256[{s}]")

    dd_ee = _f(dd.get("ddEE_mean"))
    dd_cap = _f(dd.get("ddCAP_mean"))
    dd_eph = _f(dd.get("ddEph"))
    dd_tail = _f(dd.get("ddTail_mean_Eph"))
    dd_exits_h = _f(dd.get("ddExits_per_hour"))
    touched = _i(dd.get("touched_targets"))
    touched_neg_cap = _i(dd.get("touched_targets_neg_ddCAP"))
    vol = dd.get("vol_bucket_distribution") or {}
    low = _i(vol.get("VOL_LOW"))
    mid = _i(vol.get("VOL_MID"))
    high = _i(vol.get("VOL_HIGH"))

    if dd_ee <= 0.0:
        errs.append(f"{path}: ddEE_mean <= 0 ({dd_ee})")
    if dd_cap < 0.0:
        errs.append(f"{path}: ddCAP_mean < 0 ({dd_cap})")
    if dd_eph <= 0.0:
        errs.append(f"{path}: ddEph <= 0 ({dd_eph})")
    if dd_tail < 0.0:
        errs.append(f"{path}: ddTail_mean_Eph < 0 ({dd_tail})")
    if dd_exits_h < 0.0:
        errs.append(f"{path}: ddExits_per_hour < 0 ({dd_exits_h})")
    if touched < min_touched:
        errs.append(f"{path}: touched_targets {touched} < {min_touched}")
    allowed_neg = max_neg_ddcap_count
    if allowed_neg < 0:
        allowed_neg = max(1, int(math.floor(max(0.0, max_neg_ddcap_frac) * max(0, touched))))
    if touched_neg_cap > allowed_neg:
        errs.append(f"{path}: touched_targets_neg_ddCAP {touched_neg_cap} > allowed {allowed_neg}")
    if not skip_worst_ddcap:
        worst_ddcap = None
        target_ddcap = _infer_target_ddcap(obj)
        touched_keys = set(dd.get("touched_keys") or [])
        if touched_keys:
            vals = [target_ddcap.get(k, 0.0) for k in touched_keys]
            if vals:
                worst_ddcap = min(vals)
        if worst_ddcap is not None and worst_ddcap < min_worst_ddcap:
            errs.append(f"{path}: worst_ddCAP {worst_ddcap} < min_worst_ddcap {min_worst_ddcap}")
    if min(low, mid, high) < min_vol_bucket:
        errs.append(
            f"{path}: min VOL touched {min(low, mid, high)} < {min_vol_bucket} "
            f"(low={low}, mid={mid}, high={high})"
        )
    return errs


def _metrics(path: str) -> dict[str, float]:
    obj = _load(path)
    dd = obj.get("delta_vs_nopatch") or {}
    return {
        "ddEph": _f(dd.get("ddEph")),
        "ddTail": _f(dd.get("ddTail_mean_Eph")),
        "ddCAP": _f(dd.get("ddCAP_mean")),
    }


def _pareto_frontier(cands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, a in enumerate(cands):
        dominated = False
        for j, b in enumerate(cands):
            if i == j:
                continue
            no_worse = (
                float(b["ddEph"]) >= float(a["ddEph"])
                and float(b["ddTail"]) >= float(a["ddTail"])
                and float(b["ddCAP"]) >= float(a["ddCAP"])
            )
            strictly_better = (
                float(b["ddEph"]) > float(a["ddEph"])
                or float(b["ddTail"]) > float(a["ddTail"])
                or float(b["ddCAP"]) > float(a["ddCAP"])
            )
            if no_worse and strictly_better:
                dominated = True
                break
        if not dominated:
            out.append(a)
    out.sort(key=lambda x: (x["ddEph"], x["ddTail"], x["ddCAP"]), reverse=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s1", required=True)
    ap.add_argument("--s2", required=True)
    ap.add_argument("--min-touched-targets", type=int, default=24)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=4)
    ap.add_argument(
        "--max-neg-ddcap-count",
        type=int,
        default=0,
        help="Max allowed touched targets with negative ddCAP; set -1 to use fractional threshold",
    )
    ap.add_argument(
        "--max-neg-ddcap-frac",
        type=float,
        default=0.02,
        help="Fractional threshold used when --max-neg-ddcap-count=-1",
    )
    ap.add_argument(
        "--min-worst-ddcap",
        type=float,
        default=-0.002,
        help="Reject when the worst touched-target ddCAP is below this floor",
    )
    ap.add_argument(
        "--skip-worst-ddcap",
        action="store_true",
        help="Skip expensive worst-ddCAP computation (useful for Pareto-only runs on very large artifacts)",
    )
    ap.add_argument(
        "--candidate",
        action="append",
        default=[],
        help="Candidate in form name:s1_path:s2_path; can repeat",
    )
    ap.add_argument(
        "--pareto-out",
        default="",
        help="When --candidate is provided, write Pareto frontier JSON to this path",
    )
    args = ap.parse_args()

    errs = []
    errs.extend(
        evaluate(
            args.s1,
            args.min_touched_targets,
            args.min_vol_bucket_touched,
            args.max_neg_ddcap_count,
            args.max_neg_ddcap_frac,
            args.min_worst_ddcap,
            args.skip_worst_ddcap,
        )
    )
    errs.extend(
        evaluate(
            args.s2,
            args.min_touched_targets,
            args.min_vol_bucket_touched,
            args.max_neg_ddcap_count,
            args.max_neg_ddcap_frac,
            args.min_worst_ddcap,
            args.skip_worst_ddcap,
        )
    )
    if errs:
        print("DD_GATE_FAIL")
        for e in errs:
            print(f"- {e}")
        rc = 1
    else:
        print("DD_GATE_PASS")
        rc = 0

    if args.candidate:
        cand_rows: list[dict[str, Any]] = []
        for c in args.candidate:
            parts = str(c).split(":", 2)
            if len(parts) != 3:
                continue
            name, s1, s2 = parts
            e = []
            e.extend(
                evaluate(
                    s1,
                    args.min_touched_targets,
                    args.min_vol_bucket_touched,
                    args.max_neg_ddcap_count,
                    args.max_neg_ddcap_frac,
                    args.min_worst_ddcap,
                    args.skip_worst_ddcap,
                )
            )
            e.extend(
                evaluate(
                    s2,
                    args.min_touched_targets,
                    args.min_vol_bucket_touched,
                    args.max_neg_ddcap_count,
                    args.max_neg_ddcap_frac,
                    args.min_worst_ddcap,
                    args.skip_worst_ddcap,
                )
            )
            m1 = _metrics(s1)
            m2 = _metrics(s2)
            cand_rows.append(
                {
                    "name": name,
                    "s1": s1,
                    "s2": s2,
                    "gated": len(e) == 0,
                    "errors": e,
                    "ddEph": (m1["ddEph"] + m2["ddEph"]) / 2.0,
                    "ddTail": (m1["ddTail"] + m2["ddTail"]) / 2.0,
                    "ddCAP": (m1["ddCAP"] + m2["ddCAP"]) / 2.0,
                }
            )
        feasible = [r for r in cand_rows if bool(r.get("gated"))]
        frontier = _pareto_frontier(feasible)
        payload = {"candidates": cand_rows, "pareto_frontier": frontier}
        if args.pareto_out:
            Path(args.pareto_out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.pareto_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"PARETO_WRITTEN {args.pareto_out}")
        else:
            print(json.dumps(payload, indent=2))

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
