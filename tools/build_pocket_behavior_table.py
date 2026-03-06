#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s1", required=True)
    ap.add_argument("--s2", required=True)
    ap.add_argument("--out-csv", default="artifacts/pocket_behavior_table.csv")
    ap.add_argument("--out-json", default="artifacts/pocket_behavior_table.json")
    args = ap.parse_args()

    d1 = _load(args.s1)
    d2 = _load(args.s2)
    t1 = {str(r.get("target_key", "")): r for r in d1.get("targets", [])}
    t2 = {str(r.get("target_key", "")): r for r in d2.get("targets", [])}
    keys = sorted(set(k for k in t1.keys() | t2.keys() if k))

    rows: list[dict[str, Any]] = []
    for k in keys:
        a = t1.get(k, {})
        b = t2.get(k, {})
        pair, sq, vol = (k.split("|") + ["", "", ""])[:3]
        session = sq.split("_")[0] if "_" in sq else ""
        quarter = sq.split("_")[1] if "_" in sq else ""
        def f(x: Any) -> float:
            try:
                return float(x or 0.0)
            except Exception:
                return 0.0
        # conservative merge: good=min, risk=max
        dd_eph = min(f(a.get("ddEph_vs_nopatch")), f(b.get("ddEph_vs_nopatch")))
        d_cap = min(f(a.get("dEph")), f(b.get("dEph")))
        e_patch = min(f(a.get("Eph_patch")), f(b.get("Eph_patch")))
        entries_h = min(f(a.get("entries_per_hour_patch")), f(b.get("entries_per_hour_patch")))
        exits_h = min(f(a.get("exits_per_hour_patch")), f(b.get("exits_per_hour_patch")))
        hold = max(f(a.get("avg_hold_sec_patch")), f(b.get("avg_hold_sec_patch")))
        e_trade = min(f(a.get("E_per_trade_patch")), f(b.get("E_per_trade_patch")))
        n_touch = min(f(a.get("n")), f(b.get("n")))
        rows.append(
            {
                "target_key": k,
                "pair": pair,
                "session": session,
                "quarter": quarter,
                "vol_bucket": vol,
                "ddEph_min": dd_eph,
                "Eph_patch_min": e_patch,
                "E_per_trade_min": e_trade,
                "entries_h_min": entries_h,
                "exits_h_min": exits_h,
                "hold_sec_max": hold,
                "touches_min": n_touch,
                "dEph_proxy_min": d_cap,
            }
        )

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["target_key"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps({"rows": rows, "count": len(rows), "source": {"s1": args.s1, "s2": args.s2}}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(out_csv)
    print(out_json)
    print(len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

