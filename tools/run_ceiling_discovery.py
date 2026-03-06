#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from ceiling_search_space import load_space


def _save(path: str, obj: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--session", required=True, choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])
    ap.add_argument("--base-patch", required=True)
    ap.add_argument("--space-json", default="")
    ap.add_argument("--budget-trials", type=int, default=52)
    ap.add_argument("--bootstrap", type=int, default=16)
    ap.add_argument("--batch-size", type=int, default=6)
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--cand-pool", type=int, default=2000)
    ap.add_argument("--base-cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--base-cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--tape-root", default="data_tape_oanda_m5_15_stitched")
    ap.add_argument("--pairs", default="EUR_USD,GBP_USD,USD_JPY,USD_CHF,AUD_USD,USD_CAD,NZD_USD,EUR_GBP,EUR_JPY,GBP_JPY,AUD_JPY,CHF_JPY,EUR_CHF,AUD_CAD,NZD_JPY")
    ap.add_argument("--start-utc", default="2025-12-01T00:00:00Z")
    ap.add_argument("--end-utc", default="2026-03-01T00:00:00Z")
    ap.add_argument("--out-prefix", default="")
    args = ap.parse_args()

    rounds = max(0, (int(args.budget_trials) - int(args.bootstrap)) // max(1, int(args.batch_size)))
    pair = args.pair.upper()
    prefix = args.out_prefix or f"proof_artifacts/SURROGATE_{pair}_{args.session}_{args.quarter}"
    out_json = f"{prefix}.json"
    out_md = f"{prefix}.md"
    space = load_space(args.space_json)
    space_path = f"{prefix}_SPACE.json"
    _save(space_path, space)

    cmd = [
        "python3",
        "tools/surrogate_ceiling_search.py",
        "--session",
        args.session,
        "--quarter",
        args.quarter,
        "--pair",
        pair,
        "--base-patch",
        args.base_patch,
        "--space-json",
        space_path,
        "--seed",
        str(args.seed),
        "--bootstrap",
        str(args.bootstrap),
        "--rounds",
        str(rounds),
        "--batch-size",
        str(args.batch_size),
        "--cand-pool",
        str(args.cand_pool),
        "--base-cache-s1",
        args.base_cache_s1,
        "--base-cache-s2",
        args.base_cache_s2,
        "--tape-root",
        args.tape_root,
        "--pairs",
        args.pairs,
        "--start-utc",
        args.start_utc,
        "--end-utc",
        args.end_utc,
        "--ceiling-mode",
        "first_passage",
        "--active-artifacts",
        args.active_artifacts,
        "--out-json",
        out_json,
        "--out-md",
        out_md,
    ]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        print(r.stdout)
        return r.returncode

    doc = json.loads(Path(out_json).read_text(encoding="utf-8"))
    ranked = doc.get("ranked") or []
    best = ranked[0] if ranked else {}
    summary = {
        "pair": pair,
        "session": args.session,
        "quarter": args.quarter,
        "base_patch": args.base_patch,
        "space_file": space_path,
        "trials_total": int(doc.get("trials_total", 0) or 0),
        "best": best,
        "top3": ranked[:3],
    }
    summary_path = f"{prefix}_SUMMARY.json"
    _save(summary_path, summary)
    print(out_json)
    print(out_md)
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

