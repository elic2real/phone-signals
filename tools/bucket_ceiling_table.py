#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Replay artifact JSON from state_replay_metrics.py")
    ap.add_argument("--out-json", default="proof_artifacts/BUCKET_CEILING_TABLE.json")
    ap.add_argument("--out-md", default="proof_artifacts/BUCKET_CEILING_TABLE.md")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()

    obj = _load(args.inp)
    targets = obj.get("targets") or []
    dd = obj.get("delta_vs_nopatch") or {}
    fallback_rate = float(dd.get("fallback_rate", 0.0) or 0.0)

    rows = []
    for t in targets:
        rows.append(
            {
                "key": str(t.get("target_key", "") or ""),
                "Eph_base": float(t.get("Eph_base", 0.0) or 0.0),
                "Eph_patch": float(t.get("Eph_patch", 0.0) or 0.0),
                "ddEph": float(t.get("ddEph_vs_nopatch", 0.0) or 0.0),
                "Tail_mean_Eph_base": float(obj.get("summary", {}).get("tail_mean_Eph_base", 0.0) or 0.0),
                "ddTail_mean_Eph": float(obj.get("delta_vs_nopatch", {}).get("ddTail_mean_Eph", 0.0) or 0.0),
                "ddCAP_mean": float(obj.get("delta_vs_nopatch", {}).get("ddCAP_mean", 0.0) or 0.0),
                "signals_h": float(t.get("entries_per_hour_patch", t.get("exits_per_hour_patch", 0.0)) or 0.0),
                "exits_h": float(t.get("exits_per_hour_patch", 0.0) or 0.0),
                "fallback_rate": fallback_rate,
            }
        )

    rows = [r for r in rows if r["key"]]
    top = sorted(rows, key=lambda r: (r["ddEph"], r["Eph_patch"]), reverse=True)[: max(1, args.top_n)]
    bottom = sorted(rows, key=lambda r: (r["ddTail_mean_Eph"], r["ddCAP_mean"], r["ddEph"]))[: max(1, args.top_n)]

    out = {
        "source": args.inp,
        "cache_fingerprint": obj.get("cache_fingerprint", ""),
        "active_artifacts_sha256": (obj.get("source", {}) or {}).get("active_artifacts_sha256", ""),
        "top_ceiling_buckets": top,
        "bottom_tail_risk_buckets": bottom,
    }
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(out, indent=2), encoding="utf-8")

    md = ["# Bucket Ceiling Table", "", f"- source: `{args.inp}`", ""]
    md.append("## Top Ceiling Buckets")
    for r in top:
        md.append(
            f"- `{r['key']}` ddEph={r['ddEph']:.6f} Eph(base->patch)={r['Eph_base']:.6f}->{r['Eph_patch']:.6f} "
            f"ddCAP={r['ddCAP_mean']:.6f} ddTail={r['ddTail_mean_Eph']:.6f} signals/h={r['signals_h']:.3f} exits/h={r['exits_h']:.3f}"
        )
    md.append("")
    md.append("## Bottom Tail-Risk Buckets")
    for r in bottom:
        md.append(
            f"- `{r['key']}` ddEph={r['ddEph']:.6f} ddCAP={r['ddCAP_mean']:.6f} ddTail={r['ddTail_mean_Eph']:.6f} "
            f"signals/h={r['signals_h']:.3f} exits/h={r['exits_h']:.3f}"
        )
    Path(args.out_md).write_text("\n".join(md) + "\n", encoding="utf-8")
    print(args.out_json)
    print(args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
