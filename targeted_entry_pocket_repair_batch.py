#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parent


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dataset_lock_for_node(node: str) -> Optional[Path]:
    parts = str(node or "").split("__")
    if len(parts) != 3:
        return None
    pair, weekday, session = parts
    name = f"dataset_lock__{pair.lower()}__{weekday.lower()}__{session.lower()}__11.json"
    p = ROOT / name
    return p if p.exists() else None


def _choose_targets(report: Dict[str, Any], min_sample: int, limit: int) -> List[Dict[str, Any]]:
    by_node: Dict[str, Dict[str, Any]] = {}
    details = report.get("strategy_details", {})
    for _, strategy_block in (details or {}).items():
        for pocket in strategy_block.get("pockets", []) or []:
            if str(pocket.get("status", "")) != "borderline":
                continue
            sample = _safe_int(pocket.get("trade_count"))
            if sample < min_sample:
                continue
            node = str(pocket.get("node", "")).strip()
            if not node:
                continue
            rate = _safe_float(pocket.get("entry_hit_rate"))
            # Prioritize highest-sample, nearest-to-50% borderline pockets.
            score = (sample, -abs(0.50 - rate), rate)
            prev = by_node.get(node)
            if prev is None or score > prev["score"]:
                by_node[node] = {
                    "node": node,
                    "pair": str((pocket.get("pocket_context") or {}).get("pair", "")),
                    "weekday": str((pocket.get("pocket_context") or {}).get("weekday", "")),
                    "session": str((pocket.get("pocket_context") or {}).get("session", "")),
                    "quarter": str((pocket.get("pocket_context") or {}).get("quarter", "")),
                    "entry_hit_rate": rate,
                    "trade_count": sample,
                    "strategy_key": str(pocket.get("strategy_identity", {})),
                    "score": score,
                }
    targets = sorted(by_node.values(), key=lambda r: r["score"], reverse=True)
    for t in targets:
        t.pop("score", None)
    return targets[:limit]


def _run_node(lock_path: Path) -> Dict[str, Any]:
    cmd = [
        "python3",
        "run_market_node_compiler.py",
        "--dataset-lock",
        str(lock_path),
        "--output-root",
        "compiled_market_nodes",
        "--template-root",
        "compiled_session_templates",
        "--pipeline-mode",
        "entry-only",
        "--batch-compile",
        "--force-heavy-delta-optimize",
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return {
        "lock": str(lock_path),
        "returncode": int(proc.returncode),
        "stdout_tail": proc.stdout[-1200:],
        "stderr_tail": proc.stderr[-1200:],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Targeted entry-only repair batch for borderline pockets.")
    ap.add_argument("--report", type=Path, default=Path("artifacts/entry_only_pocket_optimization_report_before.json"))
    ap.add_argument("--out", type=Path, default=Path("artifacts/targeted_entry_pocket_repair_batch_report.json"))
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--min-sample", type=int, default=80)
    args = ap.parse_args()

    report_path = args.report if args.report.is_absolute() else ROOT / args.report
    out_path = args.out if args.out.is_absolute() else ROOT / args.out

    report = _load(report_path)
    chosen = _choose_targets(report, min_sample=max(1, args.min_sample), limit=max(1, args.limit))

    run_list: List[Tuple[Dict[str, Any], Path]] = []
    skipped: List[Dict[str, Any]] = []
    seen_lock: Set[str] = set()
    for target in chosen:
        lock = _dataset_lock_for_node(str(target.get("node", "")))
        if lock is None:
            skipped.append({**target, "skip_reason": "missing_dataset_lock"})
            continue
        if str(lock) in seen_lock:
            continue
        seen_lock.add(str(lock))
        run_list.append((target, lock))

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(_run_node, lock): (target, lock) for target, lock in run_list}
        for fut in as_completed(futs):
            target, lock = futs[fut]
            try:
                res = fut.result()
            except Exception as exc:
                res = {
                    "lock": str(lock),
                    "returncode": 999,
                    "stdout_tail": "",
                    "stderr_tail": str(exc),
                }
            results.append({**target, **res})

    ok = [r for r in results if _safe_int(r.get("returncode"), 1) == 0]
    failed = [r for r in results if _safe_int(r.get("returncode"), 0) != 0]

    payload = {
        "status": "DONE",
        "input_report": str(report_path),
        "limit": args.limit,
        "workers": args.workers,
        "min_sample": args.min_sample,
        "selected_nodes": len(chosen),
        "attempted_nodes": len(results),
        "succeeded": len(ok),
        "failed": len(failed),
        "skipped": len(skipped),
        "results": results,
        "failed_items": failed,
        "skipped_items": skipped,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": "DONE",
        "out": str(out_path),
        "attempted_nodes": len(results),
        "succeeded": len(ok),
        "failed": len(failed),
        "skipped": len(skipped),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
