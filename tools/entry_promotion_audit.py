#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _iter_jsonl(path: Path, last_n: int) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if last_n > 0 and len(lines) > last_n:
        lines = lines[-last_n:]
    for line in lines:
        if not line.strip():
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit promotion blockers + theoretical fill economics.")
    ap.add_argument("--log", default="logs/trades.jsonl")
    ap.add_argument("--last-n", type=int, default=20000)
    ap.add_argument("--target-pips", type=float, default=2.5)
    ap.add_argument("--slippage-pips", type=float, default=0.2)
    ap.add_argument("--out", default="reports/entry_promotion_audit.json")
    args = ap.parse_args()

    path = Path(args.log)
    target_pips = max(0.01, float(args.target_pips))
    slippage_pips = max(0.0, float(args.slippage_pips))

    stage = Counter()
    watch_skip = Counter()
    promote_blocked = Counter()
    theoretical_rows: List[Dict[str, Any]] = []

    for e in _iter_jsonl(path, int(args.last_n)):
        kind = str(e.get("kind") or e.get("event") or "")
        if kind:
            stage[kind] += 1

        if kind == "ENTRY_PATH_SKIP_REASON" and str(e.get("reason") or "") == "watch_not_promoted":
            sub = str(e.get("subreason") or "unknown")
            watch_skip[sub] += 1

        if kind == "STATE_PROMOTE_BLOCKED":
            r = str(e.get("reason") or "unknown")
            promote_blocked[r] += 1

        if kind in ("ENTRY_EVAL", "ENTRY_GATE_EVAL"):
            spread_pips = _safe_float(e.get("spread_pips"), 0.0)
            total_cost = spread_pips + slippage_pips
            rr = (target_pips / total_cost) if total_cost > 0.0 else 999.0
            theoretical_rows.append(
                {
                    "kind": kind,
                    "pair": str(e.get("pair") or ""),
                    "spread_pips": spread_pips,
                    "target_pips": target_pips,
                    "slippage_pips": slippage_pips,
                    "total_cost_pips": total_cost,
                    "reward_to_friction": rr,
                    "economically_viable": rr >= 1.0,
                }
            )

    viable = sum(1 for r in theoretical_rows if r["economically_viable"])
    total = len(theoretical_rows)
    viability_rate = (viable / total) if total else 0.0

    primitive_disagreement_n = watch_skip.get("primitive_disagreement", 0)
    no_directional_primitive_n = watch_skip.get("no_directional_primitive", 0)

    out = {
        "source": str(path),
        "last_n": int(args.last_n),
        "params": {
            "target_pips": target_pips,
            "slippage_pips": slippage_pips,
        },
        "stage_counts": dict(stage),
        "watch_not_promoted_subreasons": watch_skip.most_common(),
        "state_promote_blocked_reasons": promote_blocked.most_common(),
        "promotion_summary": {
            "primitive_disagreement": primitive_disagreement_n,
            "no_directional_primitive": no_directional_primitive_n,
            "other": max(0, sum(watch_skip.values()) - primitive_disagreement_n - no_directional_primitive_n),
        },
        "theoretical_fill": {
            "sample_n": total,
            "viable_n": viable,
            "viability_rate": viability_rate,
            "avg_reward_to_friction": (
                sum(r["reward_to_friction"] for r in theoretical_rows) / total if total else 0.0
            ),
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

