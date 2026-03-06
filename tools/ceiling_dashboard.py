#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class BucketAgg:
    n_rows: int = 0
    n_events: float = 0.0
    eph_patch_sum: float = 0.0
    dd_eph_sum: float = 0.0
    dd_e_sum: float = 0.0
    cap_patch_sum: float = 0.0
    dd_cap_sum: float = 0.0
    ee_patch_sum: float = 0.0
    dd_ee_sum: float = 0.0


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _norm(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def _iter_targets(path: str):
    obj = _load(path)
    for t in obj.get("targets") or []:
        k = str(t.get("target_key", "") or "")
        if not k:
            continue
        yield {
            "key": k,
            "n": _f(t.get("n")),
            "eph_patch": _f(t.get("Eph_patch")),
            "dd_eph": _f(t.get("ddEph_vs_nopatch")),
            "dd_e": _f(t.get("ddE_vs_nopatch")),
        }


def _iter_rows(path: str):
    obj = _load(path)
    for r in obj.get("rows") or []:
        pair = str(r.get("pair", "") or "")
        session = str(r.get("session", "") or "")
        quarter = str(r.get("quarter", "") or "")
        vol = str(r.get("vol_bucket", "") or "")
        if not (pair and session and quarter and vol):
            continue
        k = f"{pair}|{session}_{quarter}|{vol}"
        yield {
            "key": k,
            "n": _f(r.get("n")),
            "cap_patch": _f(r.get("after_capture_to_ceiling")),
            "dd_cap": _f(r.get("delta_capture_to_ceiling_vs_nopatch")),
            "ee_patch": _f(r.get("after_expected_extraction_atr")),
            "dd_ee": _f(r.get("delta_expected_extraction_atr_vs_nopatch")),
        }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s1", required=True)
    ap.add_argument("--s2", required=True)
    ap.add_argument("--out-json", default="proof_artifacts/CEILING_DASHBOARD.json")
    ap.add_argument("--out-md", default="proof_artifacts/CEILING_DASHBOARD.md")
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--w-eph", type=float, default=0.6)
    ap.add_argument("--w-cap", type=float, default=0.25)
    ap.add_argument("--w-ee", type=float, default=0.15)
    args = ap.parse_args()

    agg: dict[str, BucketAgg] = {}
    for p in (args.s1, args.s2):
        for t in _iter_targets(p):
            a = agg.setdefault(t["key"], BucketAgg())
            a.n_rows += 1
            a.n_events += t["n"]
            a.eph_patch_sum += t["eph_patch"]
            a.dd_eph_sum += t["dd_eph"]
            a.dd_e_sum += t["dd_e"]
        for r in _iter_rows(p):
            a = agg.setdefault(r["key"], BucketAgg())
            a.cap_patch_sum += r["cap_patch"] * r["n"]
            a.dd_cap_sum += r["dd_cap"] * r["n"]
            a.ee_patch_sum += r["ee_patch"] * r["n"]
            a.dd_ee_sum += r["dd_ee"] * r["n"]

    rows = []
    for k, a in agg.items():
        n = max(a.n_events, 1e-9)
        rows.append(
            {
                "bucket_key": k,
                "touch_n": int(round(a.n_events)),
                "Eph_patch_mean": a.eph_patch_sum / max(1, a.n_rows),
                "ddEph_mean": a.dd_eph_sum / max(1, a.n_rows),
                "ddE_total": a.dd_e_sum,
                "CAP_patch_mean": a.cap_patch_sum / n,
                "ddCAP_mean": a.dd_cap_sum / n,
                "EE_patch_mean": a.ee_patch_sum / n,
                "ddEE_mean": a.dd_ee_sum / n,
            }
        )

    if not rows:
        raise SystemExit("No bucket rows found in provided artifacts")

    dd_eph_vals = [r["ddEph_mean"] for r in rows]
    dd_cap_vals = [r["ddCAP_mean"] for r in rows]
    dd_ee_vals = [r["ddEE_mean"] for r in rows]
    lo_eph, hi_eph = min(dd_eph_vals), max(dd_eph_vals)
    lo_cap, hi_cap = min(dd_cap_vals), max(dd_cap_vals)
    lo_ee, hi_ee = min(dd_ee_vals), max(dd_ee_vals)

    for r in rows:
        s = (
            args.w_eph * _norm(r["ddEph_mean"], lo_eph, hi_eph)
            + args.w_cap * _norm(r["ddCAP_mean"], lo_cap, hi_cap)
            + args.w_ee * _norm(r["ddEE_mean"], lo_ee, hi_ee)
        )
        # penalize CAP-negative buckets directly
        if r["ddCAP_mean"] < 0:
            s -= 0.15
        r["ceiling_score"] = s
        r["policy_gear"] = "FAST" if r["ddEph_mean"] > 0.002 else ("MED" if r["ddEph_mean"] > 0 else "SLOW")

    rows.sort(key=lambda x: (x["ceiling_score"], x["ddEph_mean"], x["touch_n"]), reverse=True)
    top = rows[: max(1, int(args.top_n))]
    tail = sorted(rows, key=lambda x: (x["ddEph_mean"], x["ddCAP_mean"]))[: max(1, int(args.top_n))]

    out = {
        "inputs": {"s1": args.s1, "s2": args.s2},
        "notes": [
            "pips/h and friction/slippage columns are not present in replay artifacts yet.",
            "This dashboard uses ddEph/ddCAP/ddEE and touch coverage as the current opportunity surface.",
        ],
        "top_ceiling_buckets": top,
        "worst_tail_buckets": tail,
    }
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(out, indent=2), encoding="utf-8")

    lines = [
        "# Ceiling Dashboard",
        "",
        f"- s1: `{args.s1}`",
        f"- s2: `{args.s2}`",
        "",
        "## Top Ceiling Buckets",
    ]
    for r in top:
        lines.append(
            f"- `{r['bucket_key']}` score={r['ceiling_score']:.4f} gear={r['policy_gear']} "
            f"ddEph={r['ddEph_mean']:.6f} ddCAP={r['ddCAP_mean']:.6f} ddEE={r['ddEE_mean']:.6f} touch={r['touch_n']}"
        )
    lines.append("")
    lines.append("## Worst Tail Buckets")
    for r in tail:
        lines.append(
            f"- `{r['bucket_key']}` ddEph={r['ddEph_mean']:.6f} ddCAP={r['ddCAP_mean']:.6f} ddEE={r['ddEE_mean']:.6f} touch={r['touch_n']}"
        )
    Path(args.out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
