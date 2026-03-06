#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
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
    ap.add_argument("--out", default="proof_artifacts/funnel_report.json")
    args = ap.parse_args()

    c = Counter()
    block = Counter()
    state_keys = Counter()
    source_levels = Counter()
    exit_reason_counter = Counter()
    holds = []
    for e in _iter(Path(args.log)):
        kind = str(e.get("kind") or e.get("event") or "")
        c[kind] += 1
        if kind == "ENTRY_GATE_EVAL" and e.get("block_reason"):
            block[str(e.get("block_reason"))] += 1
        if kind == "EXIT_RESULT" and e.get("exit_reason"):
            exit_reason_counter[str(e.get("exit_reason"))] += 1
        if e.get("state_key_core_str"):
            state_keys[str(e.get("state_key_core_str"))] += 1
        if e.get("source_level"):
            source_levels[str(e.get("source_level"))] += 1
        if "hold_sec" in e:
            try:
                holds.append(float(e["hold_sec"]))
            except ValueError:
                pass

    holds.sort()
    n = len(holds)
    median_hold_sec = holds[n // 2] if n else None
    p75_hold_sec = holds[int(n * 0.75)] if n else None
    p90_hold_sec = holds[int(n * 0.9)] if n else None

    out = {
        "counts": dict(c),
        "top_block_reasons": block.most_common(10),
        "top_exit_reasons": exit_reason_counter.most_common(10),
        "top_state_keys": state_keys.most_common(20),
        "source_levels": dict(source_levels),
        "median_hold_sec": median_hold_sec,
        "p75_hold_sec": p75_hold_sec,
        "p90_hold_sec": p90_hold_sec,
    }
    op = Path(args.out)
    op.parent.mkdir(parents=True, exist_ok=True)
    op.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
