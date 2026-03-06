#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _as_rows(obj: dict[str, Any]) -> list[dict[str, Any]]:
    targets = obj.get("targets") or []
    dd = obj.get("delta_vs_nopatch") or {}
    matched = dd.get("touched_patch_keys_counts") or {}
    out = []
    for t in targets:
        k = str(t.get("target_key", "") or "")
        if not k:
            continue
        out.append(
            {
                "target_key": k,
                "ddEph": float(t.get("ddEph_vs_nopatch", 0.0) or 0.0),
                "ddCAP": 0.0,
                "ddTail": float(dd.get("ddTail_mean_Eph", 0.0) or 0.0),
                "entries_h": float(t.get("entries_per_hour_patch", 0.0) or 0.0),
                "exits_h": float(t.get("exits_per_hour_patch", 0.0) or 0.0),
                "matched_key_hint": next((mk for mk in matched.keys() if k.split("|", 1)[0] in mk), ""),
                "matched_tier_hint": "SESSION_PAIR" if "|" in k else "",
                "knobs_hash": str(dd.get("knobs_hash", "") or ""),
            }
        )
    # derive ddCAP from rows
    rows = obj.get("rows") or []
    cap_acc: dict[str, list[float]] = {}
    for r in rows:
        key = f"{r.get('pair')}|{r.get('session')}_{r.get('quarter','')}|{r.get('vol_bucket','VOL_MID')}"
        n = float(r.get("n", 0.0) or 0.0)
        d = float(r.get("delta_capture_to_ceiling_vs_nopatch", 0.0) or 0.0)
        a = cap_acc.setdefault(key, [0.0, 0.0])
        a[0] += d * n
        a[1] += n
    cap_map = {k: (v[0] / v[1] if v[1] > 0 else 0.0) for k, v in cap_acc.items()}
    for r in out:
        r["ddCAP"] = float(cap_map.get(r["target_key"], 0.0))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--session", default="")
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    obj = _load(args.inp)
    rows = _as_rows(obj)
    n = max(1, int(args.top_n))
    w_eph = sorted(rows, key=lambda r: r["ddEph"])[:n]
    w_cap = sorted(rows, key=lambda r: r["ddCAP"])[:n]
    w_tail = sorted(rows, key=lambda r: r["ddTail"])[:n]

    session = args.session or Path(args.inp).stem
    out_md = args.out_md or f"proof_artifacts/NEG_POCKETS_{session}.md"
    lines = [f"# Negative Pockets {session}", "", f"- source: `{args.inp}`", ""]
    for title, arr, fld in (
        ("Worst ddEph", w_eph, "ddEph"),
        ("Worst ddCAP", w_cap, "ddCAP"),
        ("Worst ddTail", w_tail, "ddTail"),
    ):
        lines.append(f"## {title}")
        for r in arr:
            lines.append(
                f"- `{r['target_key']}` {fld}={r[fld]:.6f} key={r['matched_key_hint']} tier={r['matched_tier_hint']} hash={r['knobs_hash']}"
            )
        lines.append("")
    Path(out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
