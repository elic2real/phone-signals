#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _node_to_dataset_lock(root: Path, node: str) -> Path:
    parts = node.split("__")
    if len(parts) != 3:
        raise ValueError(f"invalid node key: {node}")
    pair, weekday, session = parts
    lock_name = f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
    return root / lock_name


def _candidate_nodes(report: Dict[str, Any], min_sample: int) -> List[Dict[str, Any]]:
    by_node: Dict[str, Dict[str, Any]] = {}
    details = dict(report.get("strategy_details") or {})
    for strategy_data in details.values():
        for pocket in strategy_data.get("pockets", []) or []:
            if pocket.get("status") != "borderline":
                continue
            sample = _safe_int(pocket.get("trade_count"))
            if sample < min_sample:
                continue
            node = str(pocket.get("node") or "").strip()
            if not node:
                continue
            seed = pocket.get("seed_suggestion") or {}
            transfer = pocket.get("transfer_analysis") or {}
            likelihood = str(transfer.get("transfer_success_likelihood") or "low")
            if likelihood not in {"high", "medium"}:
                continue

            hit = _safe_float(pocket.get("entry_hit_rate"))
            score = (
                1 if likelihood == "high" else 0,
                hit,
                sample,
            )
            row = {
                "node": node,
                "entry_hit_rate": hit,
                "trade_count": sample,
                "likelihood": likelihood,
                "seed": seed,
                "strategy_key": strategy_data.get("summary", {}).get("strategy_key", ""),
                "score": score,
            }
            prev = by_node.get(node)
            if prev is None or row["score"] > prev["score"]:
                by_node[node] = row

    rows = list(by_node.values())
    rows.sort(key=lambda r: r["score"], reverse=True)
    for r in rows:
        r.pop("score", None)
    return rows


def _run_cmd(cmd: List[str], cwd: Path) -> Tuple[int, str, str]:
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    return proc.returncode, proc.stdout[-2000:], proc.stderr[-2000:]


def main() -> int:
    ap = argparse.ArgumentParser(description="Run targeted entry-only repair batch for borderline pockets.")
    ap.add_argument("--report", type=Path, default=Path("artifacts/entry_only_pocket_optimization_report.json"))
    ap.add_argument("--min-sample", type=int, default=50)
    ap.add_argument("--limit", type=int, default=12)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--out", type=Path, default=Path("artifacts/entry_only_repair_batch_result.json"))
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    report_path = args.report if args.report.is_absolute() else (root / args.report)
    report = _load(report_path)

    candidates = _candidate_nodes(report, args.min_sample)
    selected = candidates[: max(0, args.limit)]

    results: List[Dict[str, Any]] = []
    for row in selected:
        node = row["node"]
        lock = _node_to_dataset_lock(root, node)
        if not lock.exists():
            results.append(
                {
                    "node": node,
                    "dataset_lock": str(lock),
                    "status": "SKIP",
                    "reason": "missing_dataset_lock",
                    **{k: v for k, v in row.items() if k != "node"},
                }
            )
            continue

        cmd = [
            "python3",
            "run_market_node_compiler.py",
            "--dataset-lock",
            str(lock),
            "--output-root",
            "compiled_market_nodes",
            "--template-root",
            "compiled_session_templates",
            "--pipeline-mode",
            "entry-only",
            "--batch-compile",
            "--force-heavy-delta-optimize",
            "--failure-route-override",
            "quality_repair",
        ]

        code, out, err = _run_cmd(cmd, root)
        results.append(
            {
                "node": node,
                "dataset_lock": str(lock),
                "status": "PASS" if code == 0 else "FAIL",
                "returncode": code,
                "stdout_tail": out,
                "stderr_tail": err,
                **{k: v for k, v in row.items() if k != "node"},
            }
        )

    payload = {
        "status": "DONE",
        "report_source": str(report_path),
        "selected_count": len(selected),
        "success_count": sum(1 for r in results if r.get("status") == "PASS"),
        "fail_count": sum(1 for r in results if r.get("status") == "FAIL"),
        "skip_count": sum(1 for r in results if r.get("status") == "SKIP"),
        "selected": selected,
        "results": results,
    }

    out_path = args.out if args.out.is_absolute() else (root / args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ["status", "selected_count", "success_count", "fail_count", "skip_count"]}, indent=2))
    print(str(out_path))
    return 0 if payload["fail_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
