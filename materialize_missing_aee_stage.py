#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
ARTIFACTS = ROOT / "artifacts"
DEFAULT_SOURCE = ARTIFACTS / "aee_stage_c_node_set.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def resolve_dataset_lock(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def build_targets(source: Path, explicit_nodes: list[str] | None) -> list[str]:
    if explicit_nodes:
        return explicit_nodes
    payload = load_json(source)
    return list(payload.get("uncovered_allowlist_nodes", []))


def stage_paths(node: str) -> dict[str, Path]:
    node_dir = ROOT / "compiled_market_nodes" / node
    manifest = load_json(node_dir / "node_manifest.json")
    dataset_lock_path = resolve_dataset_lock(str(manifest["dataset_lock_path"]))
    return {
        "node_dir": node_dir,
        "dataset_lock": dataset_lock_path,
        "truth_csv": node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv",
        "entry_rules_json": node_dir / "target_entry_no_timeouts" / "target_entry_classes.json",
        "output_dir": node_dir / "aee_stage",
    }


def aee_stage_ready(node_dir: Path) -> bool:
    required = [
        node_dir / "aee_stage" / "aee_state_stream" / "aee_state_stream.csv",
        node_dir / "aee_stage" / "aee_rules" / "aee_rules.json",
        node_dir / "aee_stage" / "aee_replay" / "baseline_static.json",
        node_dir / "aee_stage" / "aee_stage_report.json",
    ]
    if not all(path.exists() for path in required):
        return False
    try:
        report = load_json(node_dir / "aee_stage" / "aee_stage_report.json")
        trade_count = int((report.get("performance", {}).get("aee_metrics", {}).get("trade_count", 0)) or 0)
    except Exception:
        return False
    entry_population = node_dir / "target_entry_no_timeouts" / "target_entry_population.csv"
    if entry_population.exists() and entry_population.stat().st_size > 0 and trade_count == 0:
        return False
    return True


def run_one(node: str) -> dict[str, Any]:
    paths = stage_paths(node)
    node_dir = paths["node_dir"]
    if aee_stage_ready(node_dir):
        return {"node": node, "status": "skip", "reason": "aee_stage_ready"}
    for key in ("dataset_lock", "truth_csv", "entry_rules_json"):
        if not paths[key].exists():
            return {
                "node": node,
                "status": "missing_prereq",
                "reason": key,
                "path": str(paths[key]),
            }
    cmd = [
        "python3",
        str(ROOT / "run_aee_stage_compiler.py"),
        "--dataset-lock",
        str(paths["dataset_lock"]),
        "--truth-csv",
        str(paths["truth_csv"]),
        "--entry-rules-json",
        str(paths["entry_rules_json"]),
        "--output-dir",
        str(paths["output_dir"]),
    ]
    proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if proc.returncode == 0 and aee_stage_ready(node_dir):
        return {
            "node": node,
            "status": "ok",
        }
    return {
        "node": node,
        "status": "error",
        "returncode": proc.returncode,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Materialize missing aee_stage artifacts for uncovered allowlist nodes.")
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--node", action="append", dest="nodes")
    ap.add_argument("--report", type=Path, default=ARTIFACTS / "aee_stage_materialization_report.json")
    args = ap.parse_args()

    targets = build_targets(args.source, args.nodes)
    if args.limit > 0:
        targets = targets[: args.limit]

    results: list[dict[str, Any]] = []
    for idx, node in enumerate(targets, start=1):
        res = run_one(node)
        res["index"] = idx
        res["total"] = len(targets)
        results.append(res)
        args.report.write_text(
            json.dumps(
                {
                    "generated_at": now_iso(),
                    "source": str(args.source),
                    "target_count": len(targets),
                    "completed": len(results),
                    "results": results,
                },
                indent=2,
            )
        )
        print(json.dumps(res))


if __name__ == "__main__":
    main()
