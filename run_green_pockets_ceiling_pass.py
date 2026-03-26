#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMPILED_ROOT = ROOT / "compiled_market_nodes"
TARGET_PAIRS = {
    "AUD_USD",
    "EUR_JPY",
    "EUR_USD",
    "GBP_JPY",
    "GBP_USD",
    "NZD_USD",
    "USD_CAD",
    "USD_CHF",
    "USD_JPY",
}
TARGET_WEEKDAYS = {"thursday", "friday"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def iter_side_rows(node_dir: Path) -> list[dict]:
    report_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
    if not report_path.exists():
        return []
    report = load_json(report_path)
    if report.get("status") != "PASS":
        return []
    parts = node_dir.name.split("__")
    if len(parts) != 3:
        return []
    pair, weekday, session = parts
    if pair not in TARGET_PAIRS or weekday not in TARGET_WEEKDAYS:
        return []
    sides = report.get("sides", {})
    items = sides.items() if isinstance(sides, dict) else []
    rows = []
    for side_name, side in items:
        if not isinstance(side, dict):
            continue
        rows.append(
            {
                "node": node_dir.name,
                "pair": pair,
                "weekday": weekday,
                "session": session,
                "direction": side.get("direction", side_name),
                "wr": float(side.get("effective_win_rate") or 0.0),
                "tph": float(side.get("trades_per_hour") or 0.0),
                "util": float(side.get("utilization_ratio") or 0.0),
                "rec": float(side.get("recycling_utilization_ratio") or 0.0),
                "count": int(side.get("selected_count") or 0),
            }
        )
    return rows


def choose_route(row: dict, wr_threshold: float, util_threshold: float, tph_threshold: float) -> str | None:
    wr = row["wr"]
    util = row["util"]
    tph = row["tph"]
    count = row["count"]
    if wr >= wr_threshold and util >= util_threshold and tph >= tph_threshold:
        return None
    # Pathological thin / underfiring pockets need local rebuild.
    if count <= 25 or tph < 5.0 or util < 0.03:
        return "state_surface_rebuild"
    # If edge is weak but the pocket is active, tighten quality first.
    if wr < wr_threshold:
        return "quality_repair"
    # Otherwise supply/recycling is the main gap.
    return "supply_expand"


def build_targets(wr_threshold: float, util_threshold: float, tph_threshold: float, limit: int | None) -> list[dict]:
    per_node: dict[str, dict] = {}
    for node_dir in sorted(COMPILED_ROOT.iterdir()):
        if not node_dir.is_dir():
            continue
        for row in iter_side_rows(node_dir):
            route = choose_route(row, wr_threshold, util_threshold, tph_threshold)
            if route is None:
                continue
            lock = f"dataset_lock__{row['pair'].lower()}__{row['weekday']}__{row['session']}__11.json"
            score = (
                row["wr"],
                row["util"],
                row["tph"],
                row["count"],
            )
            current = per_node.get(row["node"])
            # Keep the worst side for the node.
            if current is None or score < current["score"]:
                per_node[row["node"]] = {
                    "node": row["node"],
                    "lock": lock,
                    "route": route,
                    "worst_side": row["direction"],
                    "wr": row["wr"],
                    "util": row["util"],
                    "tph": row["tph"],
                    "count": row["count"],
                    "score": score,
                }
    targets = sorted(per_node.values(), key=lambda item: item["score"])
    if limit is not None:
        targets = targets[:limit]
    for item in targets:
        item.pop("score", None)
    return targets


def run_target(lock: str, route: str) -> dict:
    cmd = [
        "python3",
        "run_market_node_compiler.py",
        "--dataset-lock",
        lock,
        "--output-root",
        "compiled_market_nodes",
        "--template-root",
        "compiled_session_templates",
        "--pipeline-mode",
        "entry-only",
        "--batch-compile",
        "--force-heavy-delta-optimize",
        "--failure-route-override",
        route,
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return {
        "lock": lock,
        "route": route,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-1200:],
        "stderr_tail": proc.stderr[-1200:],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wr-threshold", type=float, default=0.54)
    ap.add_argument("--util-threshold", type=float, default=0.10)
    ap.add_argument("--tph-threshold", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    targets = build_targets(args.wr_threshold, args.util_threshold, args.tph_threshold, args.limit)
    print(
        json.dumps(
            {
                "status": "STARTED",
                "targets": len(targets),
                "workers": args.workers,
                "wr_threshold": args.wr_threshold,
                "util_threshold": args.util_threshold,
                "tph_threshold": args.tph_threshold,
                "sample_targets": targets[:10],
            },
            indent=2,
        )
    )
    if not targets:
        return 0

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(run_target, row["lock"], row["route"]) for row in targets]
        for future in as_completed(futures):
            result = future.result()
            compact = {k: v for k, v in result.items() if k not in {"stdout_tail", "stderr_tail"}}
            print(json.dumps(compact, indent=2))
            if result["returncode"] != 0:
                failures.append(result)

    print(json.dumps({"status": "DONE", "failures": len(failures)}, indent=2))
    if failures:
        print(json.dumps(failures[:5], indent=2))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
