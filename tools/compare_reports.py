#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_rows(pattern: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(glob.glob(pattern)):
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        legs = d.get("legs") or {}
        core = legs.get("core") or {}
        runner = legs.get("runner") or {}
        rows.append(
            {
                "file": path,
                "pph": float(((d.get("pips_per_hour") or {}).get("weighted", 0.0)) or 0.0),
                "pips": float(d.get("weighted_pips", 0.0) or 0.0),
                "core_reason": str(((core.get("exit") or {}).get("reason")) or "NONE"),
                "runner_reason": str(((runner.get("exit") or {}).get("reason")) or "NONE"),
                "core_hold": float(core.get("hold_sec", 0.0) or 0.0),
                "runner_hold": float(runner.get("hold_sec", 0.0) or 0.0),
                "core_capture": core.get("capture"),
                "runner_capture": runner.get("capture"),
                "core_left": float(core.get("left_on_table_pips", 0.0) or 0.0),
                "runner_left": float(runner.get("left_on_table_pips", 0.0) or 0.0),
            }
        )
    return rows


def mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    core_reasons = Counter(r["core_reason"] for r in rows)
    runner_reasons = Counter(r["runner_reason"] for r in rows)
    return {
        "n": len(rows),
        "pph_mean": mean([r["pph"] for r in rows]),
        "pips_mean": mean([r["pips"] for r in rows]),
        "core_hold_mean_sec": mean([r["core_hold"] for r in rows]),
        "runner_hold_mean_sec": mean([r["runner_hold"] for r in rows]),
        "core_left_on_table_mean": mean([r["core_left"] for r in rows]),
        "runner_left_on_table_mean": mean([r["runner_left"] for r in rows]),
        "core_exit_reason_counts": dict(core_reasons),
        "runner_exit_reason_counts": dict(runner_reasons),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare offline replay report sets using leg-level authoritative exits.")
    ap.add_argument("--before", required=True, help="Glob for baseline json files")
    ap.add_argument("--after", required=True, help="Glob for candidate json files")
    ap.add_argument("--out", required=True, help="Output json path")
    args = ap.parse_args()

    before_rows = load_rows(args.before)
    after_rows = load_rows(args.after)

    before = summarize(before_rows)
    after = summarize(after_rows)
    delta = {k: after[k] - before[k] for k in before.keys() if k.endswith("_mean") or k.endswith("_sec")}

    out = {
        "before_pattern": args.before,
        "after_pattern": args.after,
        "before": before,
        "after": after,
        "delta_after_minus_before": delta,
        "note": "Leg-level exits are authoritative: core_exit_reason_counts and runner_exit_reason_counts.",
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

