#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _node_rank_from_before_report(before: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    node_rows: Dict[str, Dict[str, Any]] = {}
    for strategy in (before.get("strategy_details") or {}).values():
        for pocket in strategy.get("pockets") or []:
            if pocket.get("status") != "borderline":
                continue
            node = str(pocket.get("node") or "").strip()
            if not node:
                continue
            hit = _safe_float(pocket.get("entry_hit_rate"), 0.0)
            sample = int(pocket.get("trade_count") or 0)
            row = node_rows.setdefault(
                node,
                {
                    "node": node,
                    "borderline_count": 0,
                    "hit_sum": 0.0,
                    "hit_min": 1.0,
                    "sample_sum": 0,
                },
            )
            row["borderline_count"] += 1
            row["hit_sum"] += hit
            row["sample_sum"] += sample
            row["hit_min"] = min(float(row["hit_min"]), hit)

    ranked = []
    for node, row in node_rows.items():
        cnt = max(1, int(row["borderline_count"]))
        row["hit_avg"] = float(row["hit_sum"]) / cnt
        ranked.append((node, row))

    ranked.sort(
        key=lambda x: (
            float(x[1]["hit_min"]),
            float(x[1]["hit_avg"]),
            -int(x[1]["borderline_count"]),
            -int(x[1]["sample_sum"]),
        )
    )
    return ranked


def _parse_csv_set(value: str) -> Set[str]:
    out: Set[str] = set()
    for token in (value or "").split(","):
        t = token.strip().lower()
        if t:
            out.add(t)
    return out


def _load_damage_rows(damage_report_path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    if damage_report_path is None or not damage_report_path.exists():
        return {}
    payload = _load_json(damage_report_path)
    rows = payload.get("per_node_rows") or []
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        node = str(row.get("node_id") or "").strip()
        if not node:
            continue
        out[node] = row
    return out


def _load_allowlist_nodes(allowlist_path: Optional[Path]) -> Dict[str, Any]:
    if allowlist_path is None:
        return {
            "enabled": False,
            "path": None,
            "nodes": set(),
            "source_damage_report": None,
            "policy": None,
            "allow_count_declared": None,
        }
    if not allowlist_path.exists():
        raise FileNotFoundError(f"Allowlist file does not exist: {allowlist_path}")

    payload = _load_json(allowlist_path)
    nodes = {
        str(n).strip()
        for n in (payload.get("allow_nodes") or [])
        if str(n).strip()
    }
    return {
        "enabled": True,
        "path": str(allowlist_path),
        "nodes": nodes,
        "source_damage_report": payload.get("source_damage_report"),
        "policy": payload.get("policy"),
        "allow_count_declared": int(payload.get("allow_count") or len(nodes)),
    }


def _load_node_win_rates(node_win_rates_path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    if node_win_rates_path is None or not node_win_rates_path.exists():
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with node_win_rates_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            node = str(row.get("node_id") or "").strip()
            if not node:
                continue
            out[node] = {
                "combined_wr": _safe_float(row.get("combined_entry_win_rate_weighted"), 0.0),
                "total_selected": int(_safe_float(row.get("total_selected_count"), 0.0)),
                "long_n": int(_safe_float(row.get("long_selected_count"), 0.0)),
                "short_n": int(_safe_float(row.get("short_selected_count"), 0.0)),
            }
    return out


def _is_locked_node(
    node: str,
    *,
    node_win_rates: Dict[str, Dict[str, Any]],
    lock_combined_wr: float,
    lock_total_selected: int,
    lock_min_side_selected: int,
) -> bool:
    row = node_win_rates.get(node) or {}
    return (
        _safe_float(row.get("combined_wr"), 0.0) >= float(lock_combined_wr)
        and int(row.get("total_selected") or 0) >= int(lock_total_selected)
        and min(int(row.get("long_n") or 0), int(row.get("short_n") or 0)) >= int(lock_min_side_selected)
    )


def _validate_damage_report(
    *,
    damage_report_path: Path,
    before_report_path: Path,
    ranked_nodes: List[Tuple[str, Dict[str, Any]]],
    max_age_hours: int,
    min_node_overlap: float,
    allow_stale: bool,
) -> Dict[str, Any]:
    if not damage_report_path.exists():
        raise FileNotFoundError(f"Damage report is required but missing: {damage_report_path}")

    if min_node_overlap < 0.0 or min_node_overlap > 1.0:
        raise ValueError("--damage-report-min-node-overlap must be between 0.0 and 1.0")

    payload = _load_json(damage_report_path)
    source_artifacts = payload.get("source_artifacts") or {}
    source_before = str(source_artifacts.get("before") or "").strip()
    rows = payload.get("per_node_rows") or []
    damage_nodes = {
        str(r.get("node_id") or "").strip()
        for r in rows
        if str(r.get("node_id") or "").strip()
    }
    ranked_node_ids = {node for node, _ in ranked_nodes}

    overlap_count = len(damage_nodes & ranked_node_ids)
    overlap_ratio = (float(overlap_count) / float(max(1, len(ranked_node_ids))))

    now = datetime.now(timezone.utc).timestamp()
    age_hours = float(max(0.0, now - damage_report_path.stat().st_mtime) / 3600.0)
    is_stale = age_hours > float(max(0, max_age_hours))

    source_before_name = Path(source_before).name if source_before else ""
    before_name = before_report_path.name
    before_match = bool(source_before_name and source_before_name == before_name)

    if is_stale and not allow_stale:
        raise RuntimeError(
            f"Damage report is stale ({age_hours:.1f}h old > {max_age_hours}h). "
            "Regenerate report or use --allow-stale-damage-report."
        )
    if overlap_ratio < float(min_node_overlap):
        raise RuntimeError(
            f"Damage report compatibility failed: overlap_ratio={overlap_ratio:.3f} "
            f"< required={min_node_overlap:.3f}."
        )
    if not before_match:
        raise RuntimeError(
            "Damage report source before-artifact does not match current before report "
            f"({source_before_name} != {before_name})."
        )

    return {
        "path": str(damage_report_path),
        "age_hours": round(age_hours, 3),
        "max_age_hours": int(max_age_hours),
        "is_stale": bool(is_stale),
        "allow_stale": bool(allow_stale),
        "before_report_match": bool(before_match),
        "damage_node_count": len(damage_nodes),
        "ranked_node_count": len(ranked_node_ids),
        "overlap_count": overlap_count,
        "overlap_ratio": overlap_ratio,
        "min_node_overlap": float(min_node_overlap),
    }


def _safe_selected_nodes(
    ranked: List[Tuple[str, Dict[str, Any]]],
    *,
    limit_nodes: int,
    damage_rows: Dict[str, Dict[str, Any]],
    node_win_rates: Dict[str, Dict[str, Any]],
    deny_pairs: Set[str],
    deny_days: Set[str],
    deny_quarters: Set[str],
    deny_reasons: Set[str],
    low_sample_perf_fail_threshold: int,
    only_improving_or_neutral: bool,
    hard_borderline_cap: int,
    hard_viable_loss_cap: int,
    lock_combined_wr: float,
    lock_total_selected: int,
    lock_min_side_selected: int,
) -> List[Tuple[str, Dict[str, Any]]]:
    # Start from default priority ordering and apply damage-aware safety gates.
    # In constrained-safe mode this is strict: deferred/unsafe nodes are never
    # reintroduced to fill the requested quota.
    selected: List[Tuple[str, Dict[str, Any]]] = []

    for node, row in ranked:
        parts = node.split("__")
        pair = parts[0].lower() if len(parts) >= 1 else ""
        day = parts[1].lower() if len(parts) >= 2 else ""

        drow = damage_rows.get(node, {})
        quarter = str(drow.get("quarter") or "").lower()
        reason = str(drow.get("final_reason") or "").lower()
        sample_size = int(drow.get("sample_size") or 0)
        d_viable = int(drow.get("delta_viable") or 0)
        d_borderline = int(drow.get("delta_borderline") or 0)
        quality_node_class = str(drow.get("quality_node_class") or "").lower()
        process_status = str(drow.get("process_status") or "").lower()
        status = str(drow.get("status") or "").lower()

        if pair in deny_pairs:
            continue
        if day in deny_days:
            continue
        if quarter and quarter in deny_quarters:
            continue
        if reason and reason in deny_reasons:
            continue
        if (
            reason == "performance_check_failed"
            and sample_size > 0
            and sample_size < low_sample_perf_fail_threshold
        ):
            continue
        if _is_locked_node(
            node,
            node_win_rates=node_win_rates,
            lock_combined_wr=lock_combined_wr,
            lock_total_selected=lock_total_selected,
            lock_min_side_selected=lock_min_side_selected,
        ):
            continue
        if process_status == "error" or status == "process_error":
            continue
        if quality_node_class == "heavy_delta":
            continue
        if d_borderline > int(hard_borderline_cap):
            continue
        if d_viable < -int(hard_viable_loss_cap):
            continue

        if only_improving_or_neutral:
            if drow:
                is_improve_or_neutral = (d_viable >= 0 or d_borderline <= 0)
                if is_improve_or_neutral:
                    selected.append((node, row))
            # If there is no damage row or it is worsening, skip in strict mode.
        else:
            selected.append((node, row))

        if len(selected) >= max(0, limit_nodes):
            break

    return selected


def _clear_entry_only_outputs(node_dir: Path) -> None:
    # Keep expensive stage artifacts when available, refresh entry-only outputs.
    for name in [
        "target_entry_no_timeouts",
        "session_calibration",
        "session_opportunity_map",
        "session_potential",
        "session_performance_check",
    ]:
        shutil.rmtree(node_dir / name, ignore_errors=True)


def _resolve_dataset_lock(node_dir: Path) -> Optional[Path]:
    exact = node_dir / "dataset_lock_11_sessions.json"
    if exact.exists():
        return exact

    candidates = sorted(node_dir.glob("dataset_lock*.json"))
    if candidates:
        return candidates[0]

    # Fallback: derive canonical lock name from node key.
    parts = node_dir.name.split("__")
    if len(parts) == 3:
        pair, weekday, session = parts
        derived = node_dir.parent.parent / f"dataset_lock__{pair.lower()}__{weekday.lower()}__{session.lower()}__11.json"
        if derived.exists():
            return derived
    return None


def _run_node(
    *,
    project_root: Path,
    python_cmd: str,
    compiled_root: Path,
    node: str,
    force_rerun: bool,
) -> Dict[str, Any]:
    node_dir = compiled_root / node
    lock_path = _resolve_dataset_lock(node_dir)
    if lock_path is None:
        return {"node": node, "status": "skip", "reason": "missing_dataset_lock"}

    if force_rerun:
        _clear_entry_only_outputs(node_dir)

    cmd = [
        python_cmd,
        "run_market_node_compiler.py",
        "--dataset-lock",
        str(lock_path),
        "--output-root",
        str(compiled_root),
        "--pipeline-mode",
        "entry-only",
        "--batch-compile",
        "--force-heavy-delta-optimize",
    ]
    proc = subprocess.run(
        cmd,
        cwd=project_root,
        text=True,
        capture_output=True,
    )

    quality_report = _load_node_quality_report(compiled_root=compiled_root, node=node)
    quality_status = _classify_quality_status(returncode=proc.returncode, quality_report=quality_report)

    return {
        "node": node,
        "status": quality_status,
        "process_status": "ok" if proc.returncode == 0 else "error",
        "returncode": proc.returncode,
        "quality_report": quality_report,
        "stdout_tail": proc.stdout[-800:],
        "stderr_tail": proc.stderr[-800:],
    }


def _delta_summary(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    b = dict(before.get("overall") or {})
    a = dict(after.get("overall") or {})
    return {
        "before": b,
        "after": a,
        "delta": {
            "viable_pockets": int(a.get("viable_pockets", 0)) - int(b.get("viable_pockets", 0)),
            "borderline_pockets": int(a.get("borderline_pockets", 0)) - int(b.get("borderline_pockets", 0)),
            "dead_pockets": int(a.get("dead_pockets", 0)) - int(b.get("dead_pockets", 0)),
            "strategies_with_viable_pockets": int(a.get("strategies_with_viable_pockets", 0)) - int(b.get("strategies_with_viable_pockets", 0)),
        },
    }


def _load_node_quality_report(*, compiled_root: Path, node: str) -> Dict[str, Any]:
    report_path = compiled_root / node / "batch_compile" / "template_score_report.json"
    if not report_path.exists():
        return {"available": False, "path": str(report_path)}
    try:
        payload = _load_json(report_path)
        return {
            "available": True,
            "path": str(report_path),
            "node_class": str(payload.get("node_class") or "").strip().lower(),
            "reason": str(payload.get("reason") or "").strip().lower(),
            "failure_route": str(payload.get("failure_route") or "").strip().lower(),
            "status": str(payload.get("status") or "").strip(),
        }
    except Exception as exc:
        return {
            "available": False,
            "path": str(report_path),
            "parse_error": str(exc),
        }


def _classify_quality_status(*, returncode: int, quality_report: Dict[str, Any]) -> str:
    if returncode != 0:
        return "process_error"

    reason = str(quality_report.get("reason") or "").lower()
    node_class = str(quality_report.get("node_class") or "").lower()

    if reason == "performance_check_failed":
        return "process_ok_perf_failed"
    if node_class == "heavy_delta":
        return "process_ok_heavy_delta"
    if quality_report.get("available"):
        return "process_ok_quality_ok"
    return "process_ok_quality_unknown"


def _create_force_rerun_checkpoint(
    *,
    project_root: Path,
    compiled_root: Path,
    selected_nodes: List[str],
    checkpoint_root: Path,
    checkpoint_label: str,
) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_label = (checkpoint_label or "").strip().replace(" ", "_")
    if not safe_label:
        safe_label = f"force_rerun_{ts}"

    root_abs = checkpoint_root if checkpoint_root.is_absolute() else project_root / checkpoint_root
    checkpoint_dir = root_abs / safe_label
    if checkpoint_dir.exists():
        raise FileExistsError(f"Checkpoint directory already exists: {checkpoint_dir}")

    copied_paths: List[str] = []
    missing_paths: List[str] = []
    outputs_to_copy = [
        "target_entry_no_timeouts",
        "session_calibration",
        "session_opportunity_map",
        "session_potential",
        "session_performance_check",
        "batch_compile/template_score_report.json",
        "node_status_summary.json",
    ]

    for node in selected_nodes:
        node_src = compiled_root / node
        node_dst = checkpoint_dir / "nodes" / node
        for rel in outputs_to_copy:
            src = node_src / rel
            dst = node_dst / rel
            if not src.exists():
                missing_paths.append(f"{node}/{rel}")
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            copied_paths.append(f"{node}/{rel}")

    manifest = {
        "created_at_utc": ts,
        "checkpoint_dir": str(checkpoint_dir),
        "selected_nodes": list(selected_nodes),
        "copied_paths_count": len(copied_paths),
        "missing_paths_count": len(missing_paths),
        "copied_paths": copied_paths,
        "missing_paths": missing_paths,
    }
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    (checkpoint_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def _selected_node_metrics(report: Dict[str, Any], selected_nodes: Set[str]) -> Dict[str, int]:
    out = {
        "viable_pockets": 0,
        "borderline_pockets": 0,
        "dead_pockets": 0,
        "strategies_with_viable_pockets": 0,
    }
    strategy_viable: Dict[str, bool] = {}

    for strategy_name, strategy in (report.get("strategy_details") or {}).items():
        has_viable = False
        for pocket in strategy.get("pockets") or []:
            node = str(pocket.get("node") or "").strip()
            if node not in selected_nodes:
                continue
            status = str(pocket.get("status") or "").strip().lower()
            if status == "viable":
                out["viable_pockets"] += 1
                has_viable = True
            elif status == "borderline":
                out["borderline_pockets"] += 1
            elif status == "dead":
                out["dead_pockets"] += 1
        strategy_viable[str(strategy_name)] = has_viable

    out["strategies_with_viable_pockets"] = sum(1 for v in strategy_viable.values() if v)
    return out


def _selected_node_metrics_by_node(
    report: Dict[str, Any],
    selected_nodes: Set[str],
) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {
        node: {
            "viable_pockets": 0,
            "borderline_pockets": 0,
            "dead_pockets": 0,
            "strategies_with_viable_pockets": 0,
        }
        for node in selected_nodes
    }
    strategy_viable_by_node: Dict[str, Set[str]] = {node: set() for node in selected_nodes}

    for strategy_name, strategy in (report.get("strategy_details") or {}).items():
        for pocket in strategy.get("pockets") or []:
            node = str(pocket.get("node") or "").strip()
            if node not in selected_nodes:
                continue
            status = str(pocket.get("status") or "").strip().lower()
            row = out[node]
            if status == "viable":
                row["viable_pockets"] += 1
                strategy_viable_by_node[node].add(str(strategy_name))
            elif status == "borderline":
                row["borderline_pockets"] += 1
            elif status == "dead":
                row["dead_pockets"] += 1

    for node in selected_nodes:
        out[node]["strategies_with_viable_pockets"] = len(strategy_viable_by_node[node])
    return out


def _node_parts(node: str) -> Dict[str, str]:
    parts = str(node or "").split("__")
    return {
        "pair": parts[0] if len(parts) >= 1 else "",
        "day": parts[1] if len(parts) >= 2 else "",
        "session": parts[2] if len(parts) >= 3 else "",
    }


def _selected_node_delta_summary(
    before: Dict[str, Any],
    after: Dict[str, Any],
    selected_nodes: List[str],
) -> Dict[str, Any]:
    selected = set(selected_nodes)
    b = _selected_node_metrics(before, selected)
    a = _selected_node_metrics(after, selected)
    b_by_node = _selected_node_metrics_by_node(before, selected)
    a_by_node = _selected_node_metrics_by_node(after, selected)

    per_node: List[Dict[str, Any]] = []
    for node in sorted(selected):
        b_node = b_by_node.get(
            node,
            {
                "viable_pockets": 0,
                "borderline_pockets": 0,
                "dead_pockets": 0,
                "strategies_with_viable_pockets": 0,
            },
        )
        a_node = a_by_node.get(
            node,
            {
                "viable_pockets": 0,
                "borderline_pockets": 0,
                "dead_pockets": 0,
                "strategies_with_viable_pockets": 0,
            },
        )
        delta_node = {
            "viable_pockets": int(a_node.get("viable_pockets", 0)) - int(b_node.get("viable_pockets", 0)),
            "borderline_pockets": int(a_node.get("borderline_pockets", 0)) - int(b_node.get("borderline_pockets", 0)),
            "dead_pockets": int(a_node.get("dead_pockets", 0)) - int(b_node.get("dead_pockets", 0)),
            "strategies_with_viable_pockets": int(a_node.get("strategies_with_viable_pockets", 0)) - int(b_node.get("strategies_with_viable_pockets", 0)),
        }
        per_node.append(
            {
                "node": node,
                **_node_parts(node),
                "before": b_node,
                "after": a_node,
                "delta": delta_node,
            }
        )

    return {
        "scope": "selected_nodes_only",
        "selected_nodes": len(selected),
        "before": b,
        "after": a,
        "delta": {
            "viable_pockets": int(a.get("viable_pockets", 0)) - int(b.get("viable_pockets", 0)),
            "borderline_pockets": int(a.get("borderline_pockets", 0)) - int(b.get("borderline_pockets", 0)),
            "dead_pockets": int(a.get("dead_pockets", 0)) - int(b.get("dead_pockets", 0)),
            "strategies_with_viable_pockets": int(a.get("strategies_with_viable_pockets", 0)) - int(b.get("strategies_with_viable_pockets", 0)),
        },
        "per_node": per_node,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run bounded entry-only pocket repair fast pass and produce before/after deltas.")
    ap.add_argument("--before-report", type=Path, default=Path("artifacts/entry_only_pocket_optimization_report_before.json"))
    ap.add_argument("--after-report", type=Path, default=Path("artifacts/entry_only_pocket_optimization_report_after_fastpass.json"))
    ap.add_argument("--after-survivors", type=Path, default=Path("artifacts/entry_only_pocket_survivors_after_fastpass.json"))
    ap.add_argument("--delta-report", type=Path, default=Path("artifacts/entry_only_fastpass_delta.json"))
    ap.add_argument("--compiled-root", type=Path, default=Path("compiled_market_nodes"))
    ap.add_argument("--limit-nodes", type=int, default=25)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--min-sample", type=int, default=50)
    ap.add_argument("--python-cmd", type=str, default="python3")
    ap.add_argument("--force-rerun", action="store_true")
    ap.add_argument("--damage-report", type=Path, default=Path("artifacts/entry_only_per_node_damage_report_rerun.json"))
    ap.add_argument("--allowlist-file", type=Path, default=None)
    ap.add_argument("--selection-mode", choices=["default", "constrained-safe"], default="default")
    ap.add_argument("--deny-pairs", type=str, default="")
    ap.add_argument("--deny-days", type=str, default="")
    ap.add_argument("--deny-quarters", type=str, default="")
    ap.add_argument("--deny-reasons", type=str, default="")
    ap.add_argument("--low-sample-perf-fail-threshold", type=int, default=200)
    ap.add_argument("--only-improving-or-neutral", action="store_true")
    ap.add_argument("--damage-report-max-age-hours", type=int, default=168)
    ap.add_argument("--damage-report-min-node-overlap", type=float, default=0.50)
    ap.add_argument("--allow-stale-damage-report", action="store_true")
    ap.add_argument("--checkpoint-root", type=Path, default=Path("artifacts/entry_only_force_rerun_checkpoints"))
    ap.add_argument("--checkpoint-label", type=str, default="")
    ap.add_argument("--skip-force-rerun-checkpoint", action="store_true")
    ap.add_argument("--node-win-rates", type=Path, default=Path("artifacts/node_win_rates_rerun_nodes.csv"))
    ap.add_argument("--hard-borderline-cap", type=int, default=15)
    ap.add_argument("--hard-viable-loss-cap", type=int, default=2)
    ap.add_argument("--lock-combined-wr", type=float, default=0.58)
    ap.add_argument("--lock-total-selected", type=int, default=300)
    ap.add_argument("--lock-min-side-selected", type=int, default=50)
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parent
    compiled_root = args.compiled_root if args.compiled_root.is_absolute() else project_root / args.compiled_root
    before_report = args.before_report if args.before_report.is_absolute() else project_root / args.before_report
    after_report = args.after_report if args.after_report.is_absolute() else project_root / args.after_report
    after_survivors = args.after_survivors if args.after_survivors.is_absolute() else project_root / args.after_survivors
    delta_report = args.delta_report if args.delta_report.is_absolute() else project_root / args.delta_report
    damage_report = args.damage_report if args.damage_report.is_absolute() else project_root / args.damage_report
    node_win_rates_path = args.node_win_rates if args.node_win_rates.is_absolute() else project_root / args.node_win_rates
    allowlist_file = None
    if args.allowlist_file is not None:
        allowlist_file = args.allowlist_file if args.allowlist_file.is_absolute() else project_root / args.allowlist_file

    if not before_report.exists():
        raise FileNotFoundError(f"Missing before report: {before_report}")

    before = _load_json(before_report)
    ranked = _node_rank_from_before_report(before)
    allowlist_meta = _load_allowlist_nodes(allowlist_file)
    node_win_rates = _load_node_win_rates(node_win_rates_path)
    if allowlist_meta["enabled"]:
        allow_nodes = allowlist_meta["nodes"]
        ranked = [(node, row) for node, row in ranked if node in allow_nodes]

    damage_validation: Optional[Dict[str, Any]] = None
    selected: List[Tuple[str, Dict[str, Any]]]
    if args.selection_mode == "constrained-safe":
        damage_validation = _validate_damage_report(
            damage_report_path=damage_report,
            before_report_path=before_report,
            ranked_nodes=ranked,
            max_age_hours=max(0, int(args.damage_report_max_age_hours)),
            min_node_overlap=float(args.damage_report_min_node_overlap),
            allow_stale=bool(args.allow_stale_damage_report),
        )
        damage_rows = _load_damage_rows(damage_report)
        selected = _safe_selected_nodes(
            ranked,
            limit_nodes=args.limit_nodes,
            damage_rows=damage_rows,
            node_win_rates=node_win_rates,
            deny_pairs=_parse_csv_set(args.deny_pairs),
            deny_days=_parse_csv_set(args.deny_days),
            deny_quarters=_parse_csv_set(args.deny_quarters),
            deny_reasons=_parse_csv_set(args.deny_reasons),
            low_sample_perf_fail_threshold=max(1, int(args.low_sample_perf_fail_threshold)),
            only_improving_or_neutral=bool(args.only_improving_or_neutral),
            hard_borderline_cap=max(0, int(args.hard_borderline_cap)),
            hard_viable_loss_cap=max(0, int(args.hard_viable_loss_cap)),
            lock_combined_wr=float(args.lock_combined_wr),
            lock_total_selected=max(0, int(args.lock_total_selected)),
            lock_min_side_selected=max(0, int(args.lock_min_side_selected)),
        )
    else:
        selected = ranked[: max(0, args.limit_nodes)]

    selected_nodes = [node for node, _ in selected]

    checkpoint_info: Optional[Dict[str, Any]] = None
    if bool(args.force_rerun) and selected_nodes:
        if bool(args.skip_force_rerun_checkpoint):
            checkpoint_info = {
                "skipped": True,
                "reason": "skip_force_rerun_checkpoint_enabled",
            }
        else:
            checkpoint_info = _create_force_rerun_checkpoint(
                project_root=project_root,
                compiled_root=compiled_root,
                selected_nodes=selected_nodes,
                checkpoint_root=args.checkpoint_root,
                checkpoint_label=args.checkpoint_label,
            )

    run_results: List[Dict[str, Any]] = []
    if selected_nodes:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futures = [
                ex.submit(
                    _run_node,
                    project_root=project_root,
                    python_cmd=args.python_cmd,
                    compiled_root=compiled_root,
                    node=node,
                    force_rerun=bool(args.force_rerun),
                )
                for node in selected_nodes
            ]
            for fut in as_completed(futures):
                run_results.append(fut.result())

    # Regenerate after report from current compiled state.
    cmd = [
        args.python_cmd,
        "entry_only_pocket_optimizer.py",
        "--compiled-root",
        str(compiled_root),
        "--out-json",
        str(after_report),
        "--out-survivors",
        str(after_survivors),
        "--min-sample",
        str(args.min_sample),
    ]
    regen = subprocess.run(cmd, cwd=project_root, text=True, capture_output=True)
    if regen.returncode != 0:
        raise RuntimeError(
            "Failed to regenerate after report: "
            + (regen.stderr[-600:] or regen.stdout[-600:])
        )

    after = _load_json(after_report)
    selected_node_delta = _selected_node_delta_summary(before, after, selected_nodes)
    global_delta = _delta_summary(before, after)

    status_counts: Dict[str, int] = {}
    for row in run_results:
        key = str(row.get("status") or "unknown")
        status_counts[key] = status_counts.get(key, 0) + 1

    payload = {
        "status": "PASS",
        "selected_nodes": len(selected_nodes),
        "requested_limit_nodes": args.limit_nodes,
        "workers": args.workers,
        "force_rerun": bool(args.force_rerun),
        "selection_mode": args.selection_mode,
        "selection_filters": {
            "damage_report": str(damage_report),
            "allowlist_file": allowlist_meta["path"],
            "allowlist_enabled": bool(allowlist_meta["enabled"]),
            "allowlist_declared_count": allowlist_meta["allow_count_declared"],
            "deny_pairs": sorted(list(_parse_csv_set(args.deny_pairs))),
            "deny_days": sorted(list(_parse_csv_set(args.deny_days))),
            "deny_quarters": sorted(list(_parse_csv_set(args.deny_quarters))),
            "deny_reasons": sorted(list(_parse_csv_set(args.deny_reasons))),
            "low_sample_perf_fail_threshold": int(args.low_sample_perf_fail_threshold),
            "only_improving_or_neutral": bool(args.only_improving_or_neutral),
        },
        "damage_report_validation": damage_validation,
        "force_rerun_checkpoint": checkpoint_info,
        "selected_node_ranking": [row for _, row in selected],
        "selected_nodes_list": selected_nodes,
        "run_result_status_counts": status_counts,
        "run_results": sorted(run_results, key=lambda r: str(r.get("node", ""))),
        "delta_summary": selected_node_delta,
        "global_delta_context": global_delta,
        "artifacts": {
            "before_report": str(before_report),
            "after_report": str(after_report),
            "after_survivors": str(after_survivors),
            "delta_report": str(delta_report),
        },
    }

    delta_report.parent.mkdir(parents=True, exist_ok=True)
    delta_report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": "PASS",
        "selected_nodes": len(selected_nodes),
        "delta": selected_node_delta["delta"],
        "status_counts": status_counts,
        "delta_report": str(delta_report),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
