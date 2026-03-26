#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


BLOCKER_TO_KNOBS: Dict[str, List[str]] = {
    "FRICTION_NOT_COVERED": [
        "FRICTION_SEVERITY_MULT",
        "friction_multiplier",
        "min_edge_after_friction",
    ],
    "spread_too_high": [
        "max_spread_pips",
        "spread_atr_ratio_limit",
        "spread_penalty_factor",
    ],
    "tick_entry_break_not_crossed": [
        "entry.tick.confirm_disp_atr",
        "entry.tick.base_max_dist_atr",
    ],
    "watch_not_promoted": [
        "entry.tick.confirm_disp_atr",
        "entry.tick.confirm_m1_closes",
        "entry.tick.confirm_sec",
    ],
}


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _collect(path: Path, last_n: int) -> Tuple[Counter, Counter, Counter, Counter]:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines() if path.exists() else []
    if last_n > 0 and len(lines) > last_n:
        lines = lines[-last_n:]

    block = Counter()
    skip = Counter()
    skip_actionable = Counter()
    stage = Counter()
    for raw in lines:
        try:
            e = json.loads(raw)
        except Exception:
            continue
        kind = str(e.get("kind") or e.get("event") or "")
        if kind:
            stage[kind] += 1
        if kind == "ENTRY_GATE_EVAL":
            br = str(e.get("block_reason") or "")
            if br:
                block[br] += 1
        if kind == "ENTRY_PATH_SKIP_REASON":
            reason = str(e.get("reason") or "")
            subreason = str(e.get("subreason") or "")
            if reason:
                skip[reason] += 1
                # "state_not_actionable:state=MANAGING" is operationally expected noise.
                if not (reason == "state_not_actionable" and subreason == "state=MANAGING"):
                    skip_actionable[reason] += 1
    return block, skip, skip_actionable, stage


def _rank_knobs(block: Counter, skip_actionable: Counter) -> List[Tuple[str, float]]:
    knob_score: Counter = Counter()
    merged = Counter()
    merged.update(block)
    merged.update(skip_actionable)
    total = sum(merged.values()) or 1
    for reason, n in merged.items():
        knobs = BLOCKER_TO_KNOBS.get(reason, [])
        if not knobs:
            continue
        w = float(n) / float(total)
        for k in knobs:
            knob_score[k] += w
    return knob_score.most_common()


def main() -> int:
    ap = argparse.ArgumentParser(description="Build blocker heatmap and knob priority list from trade events.")
    ap.add_argument("--log", default="logs/trades.jsonl")
    ap.add_argument("--last-n", type=int, default=5000)
    ap.add_argument("--out", default="reports/block_reason_heatmap.json")
    args = ap.parse_args()

    log_path = Path(args.log)
    block, skip, skip_actionable, stage = _collect(log_path, max(0, int(args.last_n)))
    ranked_knobs = _rank_knobs(block, skip_actionable)

    out = {
        "log": str(log_path),
        "last_n": int(args.last_n),
        "stage_counts": dict(stage),
        "entry_gate_block_reasons": block.most_common(),
        "entry_path_skip_reasons": skip.most_common(),
        "entry_path_skip_reasons_actionable": skip_actionable.most_common(),
        "knob_priority": [
            {"knob": knob, "score": float(score)} for knob, score in ranked_knobs
        ],
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
