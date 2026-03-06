#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _iter_events(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def build_patch_from_logs(log_path: Path, lookback_days: int = 7) -> Dict[str, Any]:
    exits = []
    for e in _iter_events(log_path):
        if str(e.get("kind", "")) != "EXIT_RESULT":
            continue
        if e.get("is_selftest"):
            continue
        exits.append(e)

    buckets: Dict[tuple[str, str], list[dict]] = defaultdict(list)
    for e in exits:
        buckets[(str(e.get("source_level", "NONE")), str(e.get("source_key", "")))].append(e)

    patches = []
    for (level, key), rows in buckets.items():
        n = len(rows)
        if n < 8:
            # Guardrail: do not emit patches below low-n safe threshold.
            print(f"CAL_PATCH_SKIP_INSUFFICIENT_N level={level} key={key} n={n}")
            continue
        pnl_atr = [float(r.get("pnl_atr", 0.0) or 0.0) for r in rows]
        avg = sum(pnl_atr) / max(1, len(pnl_atr))
        strict_delta = 0.0
        if n >= 20:
            strict_delta = 0.05 if avg < 0 else -0.03
        elif n >= 8:
            strict_delta = 0.02 if avg < 0 else -0.01
        patches.append(
            {
                "mode": "ENTRY",
                "level": level,
                "key": key,
                "n": n,
                "knobs": {"aee.strictness_mult": round(1.0 + strict_delta, 4)},
            }
        )

    return {
        "version": f"patch-{_iso_now()}",
        "window": {"lookback_days": int(lookback_days), "today_weight": 0.35, "week_weight": 0.65},
        "inputs": {"log_path": str(log_path), "n_exit_events_used": len(exits)},
        "patches": patches,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-logs", default="logs/trades.jsonl")
    ap.add_argument("--out", default="calibration/tune_map_patch.json")
    ap.add_argument("--lookback-days", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write", action="store_true", help="compat flag; writes patch when set")
    args = ap.parse_args()

    patch = build_patch_from_logs(Path(args.from_logs), lookback_days=args.lookback_days)
    if args.dry_run:
        print(json.dumps(patch["inputs"], indent=2))
        print(f"N_PATCHES={len(patch.get('patches', []))}")
        return 0

    _atomic_write(Path(args.out), patch)
    print("CAL_PATCH_WRITE_OK", args.out, patch.get("version"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
