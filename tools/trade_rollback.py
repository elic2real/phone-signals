#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _iter(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="logs/trades.jsonl")
    ap.add_argument("--last", type=int, default=10)
    ap.add_argument("--out", default="proof_artifacts/ROLLBACK_10.json")
    args = ap.parse_args()

    entries = []
    exits = {}
    for e in _iter(Path(args.log)):
        if str(e.get("kind")) == "ENTRY_RESULT":
            entries.append(e)
        elif str(e.get("kind")) == "EXIT_RESULT":
            exits[str(e.get("trade_id"))] = e

    rows = []
    for en in entries[-args.last :]:
        tid = str(en.get("trade_id"))
        ex = exits.get(tid, {})
        rows.append(
            {
                "trade_id": en.get("trade_id"),
                "entry_group_id": en.get("entry_group_id"),
                "state_key_core_str": en.get("state_key_core_str"),
                "setup": en.get("setup"),
                "source_level": en.get("source_level"),
                "source_key": en.get("source_key"),
                "tune_hash": en.get("tune_hash"),
                "patch_version": en.get("patch_version"),
                "manual_version": en.get("manual_version"),
                "entry_knobs_eff": en.get("entry_knobs_eff") or en.get("knobs_eff") or {},
                "aee_knobs_eff": ex.get("aee_knobs_eff") or {},
                "entry_conf_score": en.get("conf_score"),
                "aee_reason": ex.get("aee_reason"),
                "exit_reason": ex.get("aee_reason") or ex.get("exit_reason"),
                "pnl_atr": ex.get("pnl_atr"),
                "MFE_atr": ex.get("MFE_atr"),
                "MAE_atr": ex.get("MAE_atr"),
                "GB": ex.get("GB"),
                "hold_sec": ex.get("hold_sec"),
                "suggestion": "tighten aee.strictness_mult by +0.02" if float(ex.get("pnl_atr", 0) or 0) < 0 else "hold",
            }
        )

    out = {"trades": rows}
    p = Path(args.out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("ROLLBACK_OK", p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
