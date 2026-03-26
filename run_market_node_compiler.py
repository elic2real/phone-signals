#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import optimize_aee_target_local_from_entry_population as local_fixedpop
import optimize_aee_target_theoretical_ceiling as theoretical_ceiling
import repair_playbook
import session_calibration
import session_opportunity_map
import session_performance_check
import session_potential
import session_template
import run_target_entry_stage_compiler as target_stage_compiler
from compiler_blockers import write_blocker_report


ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET_LOCK = ROOT / "dataset_lock_11_sessions.json"
DEFAULT_OUTPUT_ROOT = ROOT / "compiled_market_nodes"
DEFAULT_TEMPLATE_ROOT = ROOT / "compiled_session_templates"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def summary_best_score(report: dict[str, Any]) -> tuple[float, float, float, int]:
    best = (float("-inf"), float("-inf"), float("-inf"), -1)
    for row in report.get("summary", []) or []:
        score = (
            float(row.get("tp_hit_rate", 0.0)),
            float(row.get("total_pips", 0.0)),
            float(row.get("pips_per_hour", 0.0)),
            int(row.get("trade_count", 0)),
        )
        if score > best:
            best = score
    return best


def find_cross_day_rule_source(
    output_root: Path,
    pair: str,
    session: str,
    current_weekday: str,
) -> tuple[Path, Path] | None:
    preferred_days = ["monday", "tuesday", "wednesday"]
    candidates: list[tuple[tuple[float, float, float, int], int, Path, Path]] = []
    for order, day in enumerate(preferred_days):
        if day == current_weekday:
            continue
        node_root = output_root / f"{pair}__{day}__{session}"
        report_path = node_root / "target_entry_no_timeouts" / "target_entry_class_report.json"
        base_rules = node_root / "target_entry_stage" / "target_contextual_v2" / "target_entry_classes.json"
        targeted_rules = node_root / "target_entry_stage" / "target_contextual_v2_targeted" / "target_entry_classes.json"
        if not has_files(report_path, base_rules, targeted_rules):
            continue
        try:
            report = load_json(report_path)
        except Exception:
            continue
        if report.get("empty_population"):
            continue
        score = summary_best_score(report)
        if score[-1] <= 0:
            continue
        candidates.append((score, -order, base_rules, targeted_rules))
    if not candidates:
        return None
    _, _, base_rules, targeted_rules = max(candidates)
    return base_rules, targeted_rules


def _pair_tokens(pair: str) -> tuple[str, str]:
    parts = str(pair or "").upper().split("_")
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(pair or "").upper(), ""


def _pair_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    ab, aq = _pair_tokens(a)
    bb, bq = _pair_tokens(b)
    if not (ab and aq and bb and bq):
        return 0.0
    overlap = len({ab, aq}.intersection({bb, bq}))
    if overlap == 2:
        return 0.75
    if overlap == 1:
        return 0.4
    return 0.0


def find_cross_pair_rule_source(
    output_root: Path,
    pair: str,
    session: str,
    weekday: str,
    min_similarity: float = 0.4,
) -> tuple[Path, Path, str, float] | None:
    candidates: list[tuple[tuple[float, float, float, int], float, str, Path, Path]] = []
    for node_root in sorted(output_root.glob(f"*__{weekday}__{session}")):
        if not node_root.is_dir():
            continue
        node_pair = node_root.name.split("__", 1)[0]
        if node_pair == pair:
            continue
        similarity = _pair_similarity(pair, node_pair)
        if similarity < min_similarity:
            continue
        report_path = node_root / "target_entry_no_timeouts" / "target_entry_class_report.json"
        base_rules = node_root / "target_entry_stage" / "target_contextual_v2" / "target_entry_classes.json"
        targeted_rules = node_root / "target_entry_stage" / "target_contextual_v2_targeted" / "target_entry_classes.json"
        if not has_files(report_path, base_rules, targeted_rules):
            continue
        try:
            report = load_json(report_path)
        except Exception:
            continue
        if report.get("empty_population"):
            continue
        score = summary_best_score(report)
        if score[-1] <= 0:
            continue
        candidates.append((score, similarity, node_root.name, base_rules, targeted_rules))
    if not candidates:
        return None
    _, similarity, node_name, base_rules, targeted_rules = max(candidates, key=lambda x: (x[1], x[0]))
    return base_rules, targeted_rules, node_name, similarity


def node_key(lock: dict[str, Any]) -> str:
    pair = str(lock.get("pair", "UNKNOWN"))
    weekday = str(lock.get("weekday", "unknown")).lower()
    session = str(lock.get("session", "unknown")).lower()
    return f"{pair}__{weekday}__{session}"


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def load_trade_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return 0
    return len(payload) if isinstance(payload, list) else 0


def performance_failure_route(session_performance_report: dict[str, Any]) -> str:
    if stale_rescue_surface_requires_stage_rebuild(session_performance_report):
        return "state_surface_rebuild"
    perf_issue_names = {
        str(issue.get("issue")) if isinstance(issue, dict) else str(issue)
        for issue in session_performance_report.get("issues", [])
    }
    if not perf_issue_names:
        return "manual_review"
    return repair_playbook.route_for_issues(perf_issue_names)


def stale_rescue_surface_requires_stage_rebuild(session_performance_report: dict[str, Any]) -> bool:
    if session_performance_report.get("status") != "REPAIR_REQUIRED":
        return False
    best_direction = str(session_performance_report.get("best_class_direction", "")).upper()
    try:
        best_target = float(session_performance_report.get("best_class_target_distance", 0.0) or 0.0)
    except Exception:
        best_target = 0.0
    rescue_rule_ids = [
        str(rule_id)
        for rule_id in (session_performance_report.get("best_class_rescue_rule_ids", []) or [])
        if rule_id
    ]
    if not rescue_rule_ids:
        return False
    is_toxic_long = best_direction == "LONG" and best_target >= 11.0
    is_scalp_short = best_direction == "SHORT" and best_target <= 1.5
    if not (is_toxic_long or is_scalp_short):
        return False
    for issue in session_performance_report.get("issues", []) or []:
        if not isinstance(issue, dict):
            continue
        if str(issue.get("issue")) != "below_symmetric_break_even":
            continue
        issue_direction = str(issue.get("direction", "")).upper()
        if is_toxic_long and issue_direction == "LONG":
            return True
        if is_scalp_short and issue_direction == "SHORT":
            return True
    return False


def should_force_local_stage_rules(
    out_root: Path,
    failure_route_override: str | None,
    weekday: str,
) -> bool:
    if failure_route_override == "state_surface_rebuild":
        return True
    if failure_route_override == "quality_repair" and str(weekday).lower() == "friday":
        return True
    report_path = out_root / "session_performance_check" / "session_performance_check_report.json"
    if not report_path.exists():
        return False
    try:
        report = load_json(report_path)
    except Exception:
        return False
    return stale_rescue_surface_requires_stage_rebuild(report)


def apply_performance_override(
    node_classification: dict[str, Any],
    session_performance_report: dict[str, Any],
) -> dict[str, Any]:
    if session_performance_report.get("status") != "REPAIR_REQUIRED":
        return node_classification
    route = performance_failure_route(session_performance_report)
    return {
        "node_class": "heavy_delta",
        "failure_route": route,
        "reason": "performance_check_failed"
        if route != "state_surface_rebuild"
        else "stale_rescue_surface",
    }


def classify_post_entry_reports(
    template_report: dict[str, Any] | None,
    session_calibration_report: dict[str, Any],
    session_potential_report: dict[str, Any],
    session_performance_report: dict[str, Any],
    *,
    batch_compile: bool,
) -> dict[str, Any]:
    if batch_compile:
        return apply_performance_override(
            session_template.classify_node(
                template_report,
                session_calibration_report,
                session_potential_report,
            ),
            session_performance_report,
        )
    if session_calibration_report.get("status") != "PASS" or session_potential_report.get("status") != "PASS":
        return {
            "node_class": "invalid",
            "failure_route": "invalid",
            "reason": "invalid_calibration_or_potential",
        }
    if session_performance_report.get("status") == "PASS":
        return {
            "node_class": "accept",
            "failure_route": "none",
            "reason": "performance_pass",
        }
    return apply_performance_override(
        {
            "node_class": "accept",
            "failure_route": "none",
            "reason": "performance_probe",
        },
        session_performance_report,
    )


def no_timeout_priority_mode_for_route(route: str) -> str:
    if route == "quality_repair":
        return "winrate_first"
    if route == "state_surface_rebuild":
        return "balanced"
    return "expand_quality_entries"


STATE_SURFACE_FAILURE_ISSUES = {
    "missing_directional_coverage",
    "pathological_best_class_trade_count",
    "ultra_thin_best_class_trade_count",
    "underutilized_expected_direction",
    "total_opportunity_count_too_low",
    "pathological_total_opportunity_count",
    "directional_overfit",
}

LOCKED_CLASS_WIN_RATE = 0.58
LOCKED_CLASS_TRADE_COUNT = 300
QUALITY_REPAIR_MAX_ADDED_GROUPS = 1
REPAIR_SELECTED_GROWTH_CAP = 2.0
UNTOUCHED_SIDE_GROWTH_RATIO_CAP = 1.5
UNTOUCHED_SIDE_GROWTH_ABS_CAP = 25
REPAIR_ZONE_GROWTH_RATIO_CAP = 1.25
REPAIR_ZONE_GROWTH_ABS_CAP = 8
TARGETED_RULE_KEYS = {
    ("LONG", 11.0),
    ("LONG", 13.0),
    ("LONG", 15.0),
    ("SHORT", 1.5),
}


def load_merged_rule_group_targets(base_rules_path: Path, targeted_rules_path: Path) -> dict[str, list[float]]:
    grouped: dict[str, set[float]] = {"LONG": set(), "SHORT": set()}
    base_rules = load_json(base_rules_path).get("entry_classes", [])
    targeted_rules = load_json(targeted_rules_path).get("entry_classes", [])
    for rule in base_rules:
        key = (str(rule.get("direction", "")).upper(), float(rule.get("target_distance", 0.0) or 0.0))
        if key not in TARGETED_RULE_KEYS:
            grouped.setdefault(key[0], set()).add(key[1])
    for rule in targeted_rules:
        key = (str(rule.get("direction", "")).upper(), float(rule.get("target_distance", 0.0) or 0.0))
        if key in TARGETED_RULE_KEYS:
            grouped.setdefault(key[0], set()).add(key[1])
    return {direction: sorted(targets) for direction, targets in grouped.items() if targets}


def load_class_summary_by_key(report_path: Path) -> dict[tuple[str, float], dict[str, Any]]:
    if not report_path.exists():
        return {}
    try:
        report = load_json(report_path)
    except Exception:
        return {}
    summary = report.get("summary", []) or []
    rows: dict[tuple[str, float], dict[str, Any]] = {}
    for row in summary:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction", "")).upper()
        if direction not in {"LONG", "SHORT"}:
            continue
        try:
            target = float(row.get("target_distance", 0.0) or 0.0)
        except Exception:
            continue
        rows[(direction, target)] = row
    return rows


def repair_directions_from_performance_report(session_performance_report: dict[str, Any]) -> list[str]:
    directions: list[str] = []
    seen: set[str] = set()
    for issue in session_performance_report.get("issues", []) or []:
        if not isinstance(issue, dict):
            continue
        explicit_direction = str(issue.get("direction", "")).upper()
        if explicit_direction in {"LONG", "SHORT"} and explicit_direction not in seen:
            directions.append(explicit_direction)
            seen.add(explicit_direction)
        weak_direction = str(issue.get("weak_direction", "")).upper()
        if weak_direction in {"LONG", "SHORT"} and weak_direction not in seen:
            directions.append(weak_direction)
            seen.add(weak_direction)
        for missing_direction in issue.get("missing_directions", []) or []:
            normalized = str(missing_direction).upper()
            if normalized in {"LONG", "SHORT"} and normalized not in seen:
                directions.append(normalized)
                seen.add(normalized)
    best_direction = str(session_performance_report.get("best_class_direction", "")).upper()
    if not directions and best_direction in {"LONG", "SHORT"}:
        directions.append(best_direction)
    if not directions:
        directions = ["LONG", "SHORT"]
    return directions


def best_target_for_direction(
    summary_by_key: dict[tuple[str, float], dict[str, Any]],
    direction: str,
    available_targets: list[float],
    fallback_target: float | None = None,
) -> float | None:
    candidates = []
    for target in available_targets:
        row = summary_by_key.get((direction, target), {})
        score = (
            int(row.get("trade_count", 0) or 0),
            float(row.get("win_rate", row.get("tp_hit_rate", 0.0)) or 0.0),
            float(row.get("pips_per_hour", 0.0) or 0.0),
        )
        candidates.append((score, target))
    if candidates:
        return max(candidates)[1]
    if fallback_target is not None:
        return fallback_target
    return available_targets[0] if available_targets else None


def nearest_targets(available_targets: list[float], anchor: float | None, limit: int) -> list[float]:
    if not available_targets or limit <= 0:
        return []
    if anchor is None:
        return available_targets[:limit]
    ranked = sorted(available_targets, key=lambda target: (abs(target - anchor), target))
    return sorted(ranked[:limit])


def target_neighbors(available_targets: list[float], selected_targets: list[float], limit: int) -> list[float]:
    if limit <= 0:
        return []
    selected = {float(target) for target in selected_targets}
    out: list[float] = []
    for target in sorted(selected):
        try:
            idx = available_targets.index(target)
        except ValueError:
            continue
        for neighbor_idx in (idx - 1, idx + 1):
            if 0 <= neighbor_idx < len(available_targets):
                neighbor = float(available_targets[neighbor_idx])
                if neighbor in selected or neighbor in out:
                    continue
                out.append(neighbor)
                if len(out) >= limit:
                    return sorted(out)
    return sorted(out)


def protected_group_keys(summary_by_key: dict[tuple[str, float], dict[str, Any]]) -> set[tuple[str, float]]:
    protected: set[tuple[str, float]] = set()
    for key, row in summary_by_key.items():
        win_rate = float(row.get("win_rate", row.get("tp_hit_rate", 0.0)) or 0.0)
        trade_count = int(row.get("trade_count", 0) or 0)
        if win_rate >= LOCKED_CLASS_WIN_RATE and trade_count >= LOCKED_CLASS_TRADE_COUNT:
            protected.add(key)
    return protected


def build_no_timeout_optimization_scope(
    route: str,
    session_performance_report: dict[str, Any],
    class_report_path: Path,
    base_rules_path: Path,
    targeted_rules_path: Path,
) -> dict[str, Any]:
    available_targets_by_direction = load_merged_rule_group_targets(base_rules_path, targeted_rules_path)
    summary_by_key = load_class_summary_by_key(class_report_path)
    protected_keys = protected_group_keys(summary_by_key)
    repair_directions = repair_directions_from_performance_report(session_performance_report)
    requested_groups: list[tuple[str, float]] = []
    seen: set[tuple[str, float]] = set()
    best_direction = str(session_performance_report.get("best_class_direction", "")).upper()
    best_target_raw = session_performance_report.get("best_class_target_distance")
    try:
        best_target = float(best_target_raw) if best_target_raw is not None else None
    except Exception:
        best_target = None

    for direction in repair_directions:
        available_targets = available_targets_by_direction.get(direction, [])
        if not available_targets:
            continue
        anchor_fallback = best_target if best_direction == direction else None
        anchor = best_target_for_direction(summary_by_key, direction, available_targets, fallback_target=anchor_fallback)
        if route == "quality_repair":
            candidate_targets = nearest_targets(available_targets, anchor, 2)
        elif route == "supply_expand":
            candidate_targets = sorted(set(available_targets[:3]).union(nearest_targets(available_targets, anchor, 1)))
        elif route == "state_surface_rebuild":
            candidate_targets = sorted(set(available_targets[:4]).union(nearest_targets(available_targets, anchor, 1)))
        else:
            candidate_targets = nearest_targets(available_targets, anchor, 1)
        filtered = [
            (direction, target)
            for target in candidate_targets
            if (direction, target) not in protected_keys
        ]
        if not filtered:
            filtered = [(direction, target) for target in candidate_targets]
        for key in filtered:
            if key not in seen:
                requested_groups.append(key)
                seen.add(key)

    thaw_groups: list[tuple[str, float]] = []
    if route == "quality_repair":
        for direction in repair_directions:
            available_targets = available_targets_by_direction.get(direction, [])
            selected_targets = [target for dir_name, target in requested_groups if dir_name == direction]
            for thaw_target in target_neighbors(available_targets, selected_targets, QUALITY_REPAIR_MAX_ADDED_GROUPS):
                thaw_key = (direction, thaw_target)
                if thaw_key in seen or thaw_key in protected_keys:
                    continue
                thaw_groups.append(thaw_key)
                if len(thaw_groups) >= QUALITY_REPAIR_MAX_ADDED_GROUPS:
                    break
            if thaw_groups:
                break

    if not requested_groups:
        for direction, targets in available_targets_by_direction.items():
            if not targets:
                continue
            fallback_key = (direction, targets[0])
            requested_groups.append(fallback_key)
            seen.add(fallback_key)
            break

    return {
        "route": route,
        "repair_directions": repair_directions,
        "optimize_groups": [f"{direction}:{target:.1f}" for direction, target in requested_groups],
        "thaw_groups": [f"{direction}:{target:.1f}" for direction, target in thaw_groups],
        "protected_groups": [f"{direction}:{target:.1f}" for direction, target in sorted(protected_keys)],
        "freeze_unlisted_groups": True,
    }


def scope_variants_for_repair(repair_scope: dict[str, Any]) -> list[dict[str, Any]]:
    base_groups = [str(spec) for spec in repair_scope.get("optimize_groups", [])]
    variants = [
        {
            "name": "base_scope",
            "optimize_groups": base_groups,
        }
    ]
    thaw_groups = [str(spec) for spec in repair_scope.get("thaw_groups", [])]
    if repair_scope.get("route") == "quality_repair" and thaw_groups:
        variants.append(
            {
                "name": "base_plus_thaw",
                "optimize_groups": base_groups + thaw_groups[:QUALITY_REPAIR_MAX_ADDED_GROUPS],
            }
        )
    return variants


def repair_guard_result(
    baseline_report: dict[str, Any],
    candidate_report: dict[str, Any],
    repair_scope: dict[str, Any],
    baseline_calibration_report: dict[str, Any] | None = None,
    candidate_calibration_report: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    baseline_issue_names = set(performance_issue_names(baseline_report))
    structural_rebuild_baseline = (
        str(repair_scope.get("route")) == "state_surface_rebuild"
        and bool(baseline_issue_names & STATE_SURFACE_FAILURE_ISSUES)
    )
    baseline_selected = int(baseline_report.get("selected_opportunity_count", 0) or 0)
    candidate_selected = int(candidate_report.get("selected_opportunity_count", 0) or 0)
    if (
        not structural_rebuild_baseline
        and baseline_selected > 0
        and candidate_selected > int(baseline_selected * REPAIR_SELECTED_GROWTH_CAP)
    ):
        return False, "selected_opportunity_growth_cap_exceeded"

    baseline_action_counts = (baseline_calibration_report or {}).get("action_counts", {}) or {}
    candidate_action_counts = (candidate_calibration_report or {}).get("action_counts", {}) or {}
    baseline_repair_zones = int(baseline_action_counts.get("repair", 0) or 0)
    candidate_repair_zones = int(candidate_action_counts.get("repair", 0) or 0)
    if baseline_repair_zones > 0:
        allowed_repair_zones = max(
            baseline_repair_zones + REPAIR_ZONE_GROWTH_ABS_CAP,
            int(max(1, baseline_repair_zones) * REPAIR_ZONE_GROWTH_RATIO_CAP),
        )
        if candidate_repair_zones > allowed_repair_zones:
            return False, "repair_zone_growth_cap_exceeded"

    repair_directions = {str(direction).upper() for direction in repair_scope.get("repair_directions", [])}
    baseline_sides = baseline_report.get("sides", {}) or {}
    candidate_sides = candidate_report.get("sides", {}) or {}
    for direction in ("LONG", "SHORT"):
        if direction in repair_directions:
            continue
        if structural_rebuild_baseline:
            continue
        baseline_count = int(((baseline_sides.get(direction) or {}).get("selected_count")) or 0)
        candidate_count = int(((candidate_sides.get(direction) or {}).get("selected_count")) or 0)
        allowed_count = max(
            baseline_count + UNTOUCHED_SIDE_GROWTH_ABS_CAP,
            int(max(1, baseline_count) * UNTOUCHED_SIDE_GROWTH_RATIO_CAP),
        )
        if candidate_count > allowed_count:
            return False, f"untouched_side_growth_cap_exceeded:{direction}"

    return True, "accepted"


def performance_issue_names(report: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for issue in report.get("issues", []):
        if isinstance(issue, dict):
            name = issue.get("issue")
            if name:
                names.append(str(name))
        elif issue:
            names.append(str(issue))
    return names


def performance_report_rank(report: dict[str, Any]) -> tuple[Any, ...]:
    status = str(report.get("status", "UNKNOWN"))
    issue_names = performance_issue_names(report)
    sides = report.get("sides", {}) or {}
    selected_total = int(report.get("selected_opportunity_count", 0) or 0)
    worst_wr = 1.0
    active_sides = 0
    for payload in sides.values():
        selected = int(payload.get("selected_count", 0) or 0)
        if selected <= 0:
            continue
        active_sides += 1
        wr = float(payload.get("effective_win_rate", payload.get("entry_win_rate", 0.0)) or 0.0)
        if wr < worst_wr:
            worst_wr = wr
    if active_sides == 0:
        worst_wr = 0.0
    repair_count = sum(1 for issue in report.get("issues", []) if isinstance(issue, dict) and issue.get("severity") == "repair")
    warning_count = sum(1 for issue in report.get("issues", []) if isinstance(issue, dict) and issue.get("severity") == "warn")
    dense_total = 0.0
    for payload in sides.values():
        dense_total += float(payload.get("trades_per_hour", 0.0) or 0.0)
    return (
        1 if status == "PASS" else 0,
        -repair_count,
        -len([n for n in issue_names if n in STATE_SURFACE_FAILURE_ISSUES]),
        active_sides,
        round(worst_wr, 6),
        selected_total,
        round(dense_total, 6),
        -warning_count,
    )


def state_surface_modes_for_report(report: dict[str, Any]) -> list[str]:
    issue_names = set(performance_issue_names(report))
    if issue_names & STATE_SURFACE_FAILURE_ISSUES:
        return ["expand_quality_entries", "balanced", "winrate_first"]
    return ["balanced", "expand_quality_entries"]


def find_template_header(output_root: Path, relative_path: Path, fallback: str = "timestamp") -> str:
    for node_dir in sorted(output_root.glob("*")):
        candidate = node_dir / relative_path
        if candidate.exists():
            lines = candidate.read_text().splitlines()
            if lines:
                return lines[0]
    return fallback


def write_csv_header(path: Path, header: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{header}\n")


def node_outputs_exist(out_root: Path) -> bool:
    return has_files(
        out_root / "node_manifest.json",
        out_root / "stage1_6" / "compiler_report.json",
        out_root / "target_entry_stage" / "target_stage_manifest.json",
        out_root / "target_entry_no_timeouts" / "target_entry_class_report.json",
        out_root / "aee_stage" / "aee_stage_report.json",
        out_root / "trade_type_truth" / "trade_type_truth_report.json",
        out_root / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_report.json",
        out_root / "aee_target_theoretical_ceiling" / "aee_target_theoretical_ceiling_report.json",
        out_root / "session_calibration" / "session_calibration_report.json",
        out_root / "session_opportunity_map" / "session_opportunity_map_report.json",
        out_root / "session_potential" / "session_potential_report.json",
    )


def node_manifest_matches(out_root: Path, dataset_hash: str, historical_fast: bool, research_lite: bool) -> bool:
    manifest_path = out_root / "node_manifest.json"
    if not manifest_path.exists():
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    return (
        manifest.get("dataset_hash") == dataset_hash
        and bool(manifest.get("historical_fast", False)) == historical_fast
        and bool(manifest.get("research_lite", False)) == research_lite
    )


def target_stage_is_current(
    target_entry_stage_dir: Path,
    dataset_lock: Path,
    data_root: Path,
    historical_fast: bool,
    research_lite: bool,
    research_max_sessions: int,
    research_row_stride: int,
    research_max_rows_per_session: int,
) -> bool:
    truth_csv = target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"
    manifest_path = target_entry_stage_dir / "target_stage_manifest.json"
    if not has_files(
        truth_csv,
        target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json",
        target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json",
        target_entry_stage_dir / "stream_seed" / "session_energy_state_stream.csv",
        target_entry_stage_dir / "context_seed" / "session_energy_context_stream.csv",
        target_entry_stage_dir / "trajectory_seed" / "point_energy_trajectory.csv",
    ):
        return False
    try:
        lock = load_json(dataset_lock)
    except Exception:
        return False
    if not target_truth_matches_lock(truth_csv, lock):
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "data_root": str(data_root.resolve()),
        "historical_fast": historical_fast,
        "research_lite": research_lite,
        "research_max_sessions": research_max_sessions,
        "research_row_stride": research_row_stride,
        "research_max_rows_per_session": research_max_rows_per_session,
        "script_hashes": {
            "run_target_entry_stage_compiler.py": sha256_file(ROOT / "run_target_entry_stage_compiler.py"),
            "stage1_5_deterministic_compiler.py": sha256_file(ROOT / "stage1_5_deterministic_compiler.py"),
            "build_session_state_stream.py": sha256_file(ROOT / "build_session_state_stream.py"),
            "build_energy_context_engine.py": sha256_file(ROOT / "build_energy_context_engine.py"),
            "build_point_energy_trajectory.py": sha256_file(ROOT / "build_point_energy_trajectory.py"),
            "optimize_target_entry_classes_contextual_v2.py": sha256_file(ROOT / "optimize_target_entry_classes_contextual_v2.py"),
            "optimize_target_entry_classes_no_timeouts.py": sha256_file(ROOT / "optimize_target_entry_classes_no_timeouts.py"),
        },
        "config_hashes": {
            "entry_trigger_state_machine.json": (
                sha256_file(ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json")
                if (ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json").exists()
                else None
            ),
        },
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    if manifest_path.exists():
        try:
            manifest = load_json(manifest_path)
        except Exception:
            manifest = {}
        if manifest.get("stage_inputs_hash") == expected:
            return True
    contextual_manifest = target_entry_stage_dir / "target_contextual_v2" / "contextual_v2_manifest.json"
    targeted_manifest = target_entry_stage_dir / "target_contextual_v2_targeted" / "contextual_v2_manifest.json"
    if contextual_manifest.exists() and targeted_manifest.exists():
        try:
            contextual = load_json(contextual_manifest)
            targeted = load_json(targeted_manifest)
        except Exception:
            return False
        if contextual.get("dataset_hash") != sha256_file(dataset_lock):
            return False
        if targeted.get("dataset_hash") != sha256_file(dataset_lock):
            return False
        return True
    return False


def target_truth_matches_lock(truth_csv: Path, dataset_lock: dict[str, Any], sample_limit: int = 200) -> bool:
    valid_dates = {str(d) for d in dataset_lock.get("dates", [])}
    if not valid_dates:
        return True
    checked = 0
    try:
        with truth_csv.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session_id = str(row.get("session_id") or "").strip()
                ts = str(row.get("timestamp") or "").strip()
                ts_date = ts[:10] if len(ts) >= 10 else ""
                if session_id and session_id not in valid_dates:
                    return False
                if ts_date and ts_date not in valid_dates:
                    return False
                checked += 1
                if checked >= sample_limit:
                    break
    except Exception:
        return False
    return True


def no_timeout_stage_is_current(
    target_entry_no_timeouts_dir: Path,
    base_rules_path: Path,
    targeted_rules_path: Path,
    truth_csv_path: Path,
    historical_fast: bool,
    priority_mode: str,
) -> bool:
    manifest_path = target_entry_no_timeouts_dir / "runner_manifest.json"
    if not has_files(
        manifest_path,
        target_entry_no_timeouts_dir / "target_entry_classes.json",
        target_entry_no_timeouts_dir / "target_entry_population.csv",
        target_entry_no_timeouts_dir / "target_entry_class_report.json",
    ):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "base_rules_hash": sha256_file(base_rules_path),
        "targeted_rules_hash": sha256_file(targeted_rules_path),
        "truth_csv_hash": sha256_file(truth_csv_path),
        "historical_fast": historical_fast,
        "priority_mode": priority_mode,
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def aee_stage_is_current(aee_stage_dir: Path, dataset_lock: dict[str, Any], target_entry_stage_dir: Path, target_entry_no_timeouts_dir: Path) -> bool:
    manifest_path = aee_stage_dir / "aee_manifest.json"
    truth_csv_path = target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"
    if not has_files(
        manifest_path,
        aee_stage_dir / "aee_stage_report.json",
        aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv",
    ):
        return False
    if not truth_csv_path.exists():
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    seed_rule_path = manifest.get("seed_rule_path")
    seed_rule_hash = sha256_file(Path(seed_rule_path)) if seed_rule_path and Path(seed_rule_path).exists() else None
    source_entry_population_path = target_entry_no_timeouts_dir / "target_entry_population.csv"
    source_entry_population_hash = (
        sha256_file(source_entry_population_path)
        if source_entry_population_path.exists() and source_entry_population_path.stat().st_size > 0
        else None
    )
    stage_report_path = aee_stage_dir / "aee_stage_report.json"
    try:
        stage_report = load_json(stage_report_path)
    except Exception:
        stage_report = None
    source_population_rows = 0
    if source_entry_population_path.exists():
        try:
            source_population_rows = count_csv_rows(source_entry_population_path)
        except Exception:
            source_population_rows = 0
    aee_trade_count = 0
    if isinstance(stage_report, dict):
        aee_trade_count = int(
            (
                stage_report.get("performance", {})
                .get("aee_metrics", {})
                .get("trade_count", 0)
            )
            or 0
        )
    return (
        manifest.get("dataset_hash") == dataset_lock["hash"]
        and manifest.get("truth_csv_hash") == sha256_file(truth_csv_path)
        and manifest.get("entry_rules_json_hash") == sha256_file(target_entry_no_timeouts_dir / "target_entry_classes.json")
        and manifest.get("source_entry_population_hash") == source_entry_population_hash
        and manifest.get("seed_rule_hash") == seed_rule_hash
        and not (source_population_rows > 0 and aee_trade_count == 0)
    )


def trade_type_truth_is_current(trade_type_truth_dir: Path, target_entry_stage_dir: Path, aee_stage_dir: Path) -> bool:
    manifest_path = trade_type_truth_dir / "trade_type_truth_manifest.json"
    if not has_files(
        manifest_path,
        trade_type_truth_dir / "trade_type_truth_report.json",
        trade_type_truth_dir / "harvester_truth_table.csv",
        trade_type_truth_dir / "runner_truth_table.csv",
    ):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "entry_truth_hash": sha256_file(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
        "aee_state_hash": sha256_file(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv"),
        "script_hash": sha256_file(ROOT / "build_trade_type_truth.py"),
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def local_fixedpop_is_current(aee_target_local_dir: Path, dataset_lock: Path, target_entry_no_timeouts_dir: Path, aee_stage_dir: Path) -> bool:
    manifest_path = aee_target_local_dir / "local_fixedpop_manifest.json"
    if not has_files(
        manifest_path,
        aee_target_local_dir / "target_local_fixedpop_aee_report.json",
        aee_target_local_dir / "target_local_fixedpop_aee_classes.json",
    ):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "entry_population_hash": sha256_file(target_entry_no_timeouts_dir / "target_entry_population.csv"),
        "seed_report_hash": sha256_file(aee_stage_dir / "aee_stage_report.json"),
        "seed_rules_hash": sha256_file(aee_stage_dir / "aee_rules" / "aee_rule_derivation_report.json"),
        "seed_explain_hash": sha256_file(aee_stage_dir / "aee_rules" / "aee_rules.json"),
        "seed_state_hash": sha256_file(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv"),
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def theoretical_ceiling_is_current(aee_target_ceiling_dir: Path, dataset_lock: Path, target_entry_no_timeouts_dir: Path, aee_stage_dir: Path) -> bool:
    manifest_path = aee_target_ceiling_dir / "theoretical_ceiling_manifest.json"
    if not has_files(
        manifest_path,
        aee_target_ceiling_dir / "aee_target_theoretical_ceiling_report.json",
        aee_target_ceiling_dir / "aee_target_theoretical_ceiling_classes.json",
    ):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "entry_population_hash": sha256_file(target_entry_no_timeouts_dir / "target_entry_population.csv"),
        "seed_report_hash": sha256_file(aee_stage_dir / "aee_stage_report.json"),
        "seed_rules_hash": sha256_file(aee_stage_dir / "aee_rules" / "aee_rule_derivation_report.json"),
        "seed_state_hash": sha256_file(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv"),
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def session_calibration_is_current(
    session_calibration_dir: Path,
    dataset_lock: Path,
    target_entry_stage_dir: Path,
    target_entry_no_timeouts_dir: Path,
    trade_rows_json: Path | None,
) -> bool:
    manifest_path = session_calibration_dir / "session_calibration_manifest.json"
    report_path = session_calibration_dir / "session_calibration_report.json"
    if not has_files(manifest_path, report_path):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "truth_csv_hash": sha256_file(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
        "entry_population_hash": sha256_file(target_entry_no_timeouts_dir / "target_entry_population.csv"),
        "trade_rows_hash": (
            sha256_file(trade_rows_json)
            if trade_rows_json and trade_rows_json.exists()
            else None
        ),
        "script_hash": sha256_file(ROOT / "session_calibration.py"),
        "symmetric_break_even": 0.505,
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def session_potential_is_current(
    session_potential_dir: Path,
    dataset_lock: Path,
    target_entry_stage_dir: Path,
    target_entry_no_timeouts_dir: Path,
) -> bool:
    manifest_path = session_potential_dir / "session_potential_manifest.json"
    report_path = session_potential_dir / "session_potential_report.json"
    if not has_files(manifest_path, report_path):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "truth_csv_hash": sha256_file(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
        "entry_population_hash": sha256_file(target_entry_no_timeouts_dir / "target_entry_population.csv"),
        "script_hash": sha256_file(ROOT / "session_potential.py"),
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def session_opportunity_map_is_current(
    session_opportunity_map_dir: Path,
    dataset_lock: Path,
    target_entry_stage_dir: Path,
) -> bool:
    manifest_path = session_opportunity_map_dir / "session_opportunity_map_manifest.json"
    report_path = session_opportunity_map_dir / "session_opportunity_map_report.json"
    if not has_files(manifest_path, report_path):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "truth_csv_hash": sha256_file(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
        "script_hash": sha256_file(ROOT / "session_opportunity_map.py"),
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def session_performance_check_is_current(
    performance_dir: Path,
    dataset_lock: Path,
    target_entry_no_timeouts_dir: Path,
    trade_rows_json: Path | None,
    session_opportunity_map_dir: Path,
    require_aee_trade_rows: bool = False,
) -> bool:
    manifest_path = performance_dir / "session_performance_check_manifest.json"
    report_path = performance_dir / "session_performance_check_report.json"
    if not has_files(manifest_path, report_path):
        return False
    try:
        manifest = load_json(manifest_path)
    except Exception:
        return False
    payload = {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "entry_population_hash": sha256_file(target_entry_no_timeouts_dir / "target_entry_population.csv"),
        "trade_rows_hash": (
            sha256_file(trade_rows_json)
            if trade_rows_json and trade_rows_json.exists()
            else None
        ),
        "session_opportunity_map_hash": (
            sha256_file(session_opportunity_map_dir / "session_opportunity_map_report.json")
            if (session_opportunity_map_dir / "session_opportunity_map_report.json").exists()
            else None
        ),
        "script_hash": sha256_file(ROOT / "session_performance_check.py"),
        "symmetric_break_even": 0.505,
        "min_side_trades": 25,
        "min_side_trades_per_hour": 0.20,
        "min_opportunities": 100,
        "require_aee_trade_rows": require_aee_trade_rows,
    }
    expected = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return manifest.get("inputs_hash") == expected


def clear_node_artifacts(out_root: Path) -> None:
    for name in [
        "stage1_6",
        "target_entry_stage",
        "target_entry_no_timeouts",
        "aee_stage",
        "trade_type_truth",
        "aee_target_local_fixedpop",
        "aee_target_theoretical_ceiling",
        "session_calibration",
        "session_opportunity_map",
        "session_potential",
        "batch_compile",
    ]:
        shutil.rmtree(out_root / name, ignore_errors=True)
    (out_root / "node_manifest.json").unlink(missing_ok=True)


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def materialize_template_target_stage(
    target_entry_stage_dir: Path,
    template_stage1_root: Path | None,
    template_seed_root: Path | None,
    template_context_root: Path | None,
    dataset_lock_path: Path,
    dataset_data_root: Path,
    historical_fast: bool,
    research_lite: bool,
    research_max_sessions: int,
    research_row_stride: int,
    research_max_rows_per_session: int,
) -> bool:
    if not template_stage1_root or not template_seed_root or not template_context_root:
        return False
    if not template_stage1_root.exists() or not template_seed_root.exists() or not template_context_root.exists():
        return False
    stage1_6_dir = target_entry_stage_dir / "stage1_6"
    copy_tree(template_stage1_root, stage1_6_dir)
    copy_tree(template_seed_root / "stream_seed", target_entry_stage_dir / "stream_seed")
    copy_tree(template_seed_root / "context_seed", target_entry_stage_dir / "context_seed")
    copy_tree(template_seed_root / "trajectory_seed", target_entry_stage_dir / "trajectory_seed")
    copy_tree(template_context_root / "target_contextual_v2", target_entry_stage_dir / "target_contextual_v2")
    targeted_src = template_context_root / "target_contextual_v2_targeted"
    targeted_dst = target_entry_stage_dir / "target_contextual_v2_targeted"
    if targeted_src.exists():
        copy_tree(targeted_src, targeted_dst)
    else:
        copy_tree(template_context_root / "target_contextual_v2", targeted_dst)

    args_ns = argparse.Namespace(
        dataset_lock=dataset_lock_path,
        data_root=dataset_data_root,
        historical_fast=historical_fast,
        research_lite=research_lite,
        research_max_sessions=research_max_sessions,
        research_row_stride=research_row_stride,
        research_max_rows_per_session=research_max_rows_per_session,
    )
    if not (
        target_stage_compiler.truth_matches_lock_dates(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv", dataset_lock_path)
        and target_stage_compiler.truth_matches_lock_dates(target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_truth_table.csv", dataset_lock_path)
    ):
        shutil.rmtree(target_entry_stage_dir, ignore_errors=True)
        return False
    target_stage_compiler.validate_truth_opportunity_sanity(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv")
    target_stage_compiler.validate_truth_opportunity_sanity(target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_truth_table.csv")
    target_stage_compiler.write_template_apply_manifest(
        target_entry_stage_dir,
        target_stage_compiler.build_stage_inputs_hash(args_ns),
        args_ns,
        stage1_6_dir,
        target_entry_stage_dir / "stream_seed",
        target_entry_stage_dir / "context_seed",
        target_entry_stage_dir / "trajectory_seed",
        target_entry_stage_dir / "target_contextual_v2",
        target_entry_stage_dir / "target_contextual_v2_targeted",
    )
    return True


def phase1_proof(stage1_6_dir: Path, dataset_lock: dict[str, Any]) -> dict[str, Any]:
    summary_path = stage1_6_dir / "phase1" / "opportunity_map_summary.json"
    audit_path = stage1_6_dir / "phase1" / "opportunity_map_audit.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing phase-1 summary: {summary_path}")
    if not audit_path.exists():
        raise FileNotFoundError(f"Missing phase-1 audit: {audit_path}")

    summary = load_json(summary_path)
    audit = load_json(audit_path)
    phase1_status = audit.get("overall_phase1_status")
    if phase1_status != "PHASE1_PASS":
        raise RuntimeError(f"Phase-1 proof failed: {phase1_status}")

    total_rows = int(summary.get("total_rows_processed", 0))
    directional_total = (
        int(summary.get("total_LONG_opportunities", 0))
        + int(summary.get("total_SHORT_opportunities", 0))
        + 2 * int(summary.get("total_BOTH_opportunities", 0))
    )
    if total_rows <= 0 or directional_total <= 0:
        return {
            "empty": True,
            "summary": summary,
            "audit": audit,
            "total_rows_processed": total_rows,
            "directional_total": directional_total,
        }

    by_session = summary.get("opportunities_by_session", {})
    by_weekday = summary.get("opportunities_by_weekday", {})
    session = str(dataset_lock["session"])
    weekday = str(dataset_lock["weekday"])
    if session not in by_session:
        raise RuntimeError(f"Phase-1 proof missing session {session}")
    if weekday not in by_weekday:
        raise RuntimeError(f"Phase-1 proof missing weekday {weekday}")

    return {
        "empty": False,
        "summary": summary,
        "audit": audit,
        "total_rows_processed": total_rows,
        "directional_total": directional_total,
    }


def materialize_empty_node(
    output_root: Path,
    out_root: Path,
    dataset_lock_path: Path,
    dataset_lock: dict[str, Any],
    dataset_hash: str,
    stage1_6_dir: Path,
    seed_entry_node: str,
    seed_aee_node: str,
) -> None:
    target_entry_stage_dir = out_root / "target_entry_stage"
    target_entry_no_timeouts_dir = out_root / "target_entry_no_timeouts"
    aee_stage_dir = out_root / "aee_stage"
    trade_type_truth_dir = out_root / "trade_type_truth"
    aee_target_local_dir = out_root / "aee_target_local_fixedpop"
    aee_target_ceiling_dir = out_root / "aee_target_theoretical_ceiling"

    truth_header = find_template_header(
        output_root,
        Path("target_entry_stage/target_contextual_v2/target_entry_truth_table.csv"),
    )
    entry_population_header = find_template_header(
        output_root,
        Path("target_entry_no_timeouts/target_entry_population.csv"),
    )
    aee_state_header = find_template_header(
        output_root,
        Path("aee_stage/aee_state_stream/aee_state_stream.csv"),
    )
    harvester_header = find_template_header(
        output_root,
        Path("trade_type_truth/harvester_truth_table.csv"),
    )
    runner_header = find_template_header(
        output_root,
        Path("trade_type_truth/runner_truth_table.csv"),
    )
    fixedpop_summary_header = find_template_header(
        output_root,
        Path("aee_target_local_fixedpop/target_local_fixedpop_aee_summary.csv"),
        fallback="class_id,trade_count,pips_per_hour,estimated_equity_per_hour",
    )
    ceiling_summary_header = find_template_header(
        output_root,
        Path("aee_target_theoretical_ceiling/aee_target_theoretical_ceiling_summary.csv"),
        fallback="class_id,trade_count,pips_per_hour,estimated_equity_per_hour",
    )

    write_csv_header(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv", truth_header)
    write_json(target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json", {"entry_classes": []})
    write_csv_header(target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_truth_table.csv", truth_header)
    write_json(target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json", {"entry_classes": []})
    write_json(
        target_entry_stage_dir / "target_no_timeouts" / "target_entry_class_report.json",
        {
            "summary": [],
            "best_class": None,
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "aggregate_pips_per_hour": 0.0,
            "class_count": 0,
            "empty_population": True,
        },
    )
    write_json(
        target_entry_stage_dir / "target_stage_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "empty_population": True,
        },
    )
    write_json(
        target_entry_stage_dir / "target_stage_manifest.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "empty_population": True,
            "dataset_lock_path": str(dataset_lock_path.resolve()),
        },
    )

    write_json(target_entry_no_timeouts_dir / "target_entry_classes.json", {"entry_classes": []})
    write_csv_header(target_entry_no_timeouts_dir / "target_entry_population.csv", entry_population_header)
    write_json(
        target_entry_no_timeouts_dir / "target_entry_class_report.json",
        {
            "summary": [],
            "best_class": None,
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "aggregate_pips_per_hour": 0.0,
            "class_count": 0,
            "empty_population": True,
        },
    )

    zero_metrics = {
        "trade_count": 0,
        "tp_hits": 0,
        "sl_hits": 0,
        "timeouts": 0,
        "avg_static_pips": 0.0,
        "avg_aee_pips": 0.0,
        "avg_static_R": 0.0,
        "avg_aee_R": 0.0,
        "pips_per_hour": 0.0,
        "estimated_equity_per_hour": 0.0,
        "delta_pips_per_hour": 0.0,
        "delta_avg_R": 0.0,
        "action_frequency": {
            "HOLD": 0,
            "HARVEST": 0,
            "PANIC": 0,
            "DECAY_EXIT": 0,
            "EXTEND": 0,
            "TP_HIT": 0,
            "SL_HIT": 0,
        },
        "time_to_action_distribution": {
            "count": 0,
            "mean": 0.0,
            "median": 0.0,
            "min": 0,
            "max": 0,
        },
    }
    write_csv_header(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv", aee_state_header)
    write_json(
        aee_stage_dir / "aee_stage_report.json",
        {
            "metadata": {
                "dataset_hash": dataset_hash,
                "entry_population_hash": None,
                "rules_hash": None,
                "compiler_version": "stage8_aee_compiler_v1",
                "inherited_seed_rules": None,
                "champion_variant": None,
                "empty_population": True,
            },
            "performance": {
                "static_metrics": zero_metrics,
                "aee_metrics": zero_metrics,
                "delta_metrics": {
                    "delta_pips_per_hour": 0.0,
                    "delta_avg_R": 0.0,
                },
                "benchmarks": {},
            },
            "status": "NO_POPULATION",
            "mode": "empty_node",
        },
    )

    write_csv_header(trade_type_truth_dir / "harvester_truth_table.csv", harvester_header)
    write_csv_header(trade_type_truth_dir / "runner_truth_table.csv", runner_header)
    write_json(trade_type_truth_dir / "trade_type_assignment.json", {"trade_type_assignment": [], "empty_population": True})
    write_json(
        trade_type_truth_dir / "trade_type_truth_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "entry_truth_source": str(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
            "aee_state_source": str(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv"),
            "harvester_row_count": 0,
            "runner_row_count": 0,
            "by_trade_type": {
                "harvester": {"targets": [1.5, 2.5], "rows": 0, "by_direction_target": {}, "quick_tp_rate": 0.0, "speed_objective_mean": 0.0},
                "runner": {"targets": [4.5, 6.0, 7.0, 8.0, 9.0, 11.0, 13.0, 15.0], "rows": 0, "by_direction_target": {}, "extension_available_rate": 0.0, "runner_objective_mean": 0.0},
            },
            "empty_population": True,
        },
    )

    write_csv_header(aee_target_local_dir / "aee_state_stream.csv", aee_state_header)
    write_json(aee_target_local_dir / "target_local_fixedpop_aee_classes.json", {"classes": [], "empty_population": True})
    write_json(aee_target_local_dir / "target_local_fixedpop_aee_trade_rows.json", [])
    write_csv_header(aee_target_local_dir / "target_local_fixedpop_aee_summary.csv", fixedpop_summary_header)
    write_json(
        aee_target_local_dir / "target_local_fixedpop_aee_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "aggregate_metrics": {
                "trade_count": 0,
                "tp_hits": 0,
                "sl_hits": 0,
                "timeouts": 0,
                "tp_hit_rate": 0.0,
                "avg_static_pips": 0.0,
                "avg_aee_pips": 0.0,
                "avg_static_R": 0.0,
                "avg_aee_R": 0.0,
                "pips_per_hour": 0.0,
                "estimated_equity_per_hour": 0.0,
                "delta_pips_per_hour": 0.0,
                "delta_avg_R": 0.0,
            },
            "class_reports": {},
            "empty_population": True,
        },
    )

    write_csv_header(aee_target_ceiling_dir / "aee_state_stream.csv", aee_state_header)
    write_json(aee_target_ceiling_dir / "aee_target_theoretical_ceiling_classes.json", {"classes": [], "empty_population": True})
    write_json(aee_target_ceiling_dir / "aee_target_theoretical_settings.json", {"settings": {}, "empty_population": True})
    write_csv_header(aee_target_ceiling_dir / "aee_target_theoretical_ceiling_summary.csv", ceiling_summary_header)
    write_json(
        aee_target_ceiling_dir / "aee_target_theoretical_ceiling_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "aggregate_metrics": {
                "trade_count": 0,
                "tp_hits": 0,
                "sl_hits": 0,
                "timeouts": 0,
                "tp_hit_rate": 0.0,
                "avg_static_pips": 0.0,
                "avg_aee_pips": 0.0,
                "avg_static_R": 0.0,
                "avg_aee_R": 0.0,
                "pips_per_hour": 0.0,
                "estimated_equity_per_hour": 0.0,
                "delta_pips_per_hour": 0.0,
                "delta_avg_R": 0.0,
            },
            "class_reports": {},
            "empty_population": True,
        },
    )

    session_calibration_dir = out_root / "session_calibration"
    write_json(
        session_calibration_dir / "session_calibration_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": {
                "pair": dataset_lock.get("pair"),
                "weekday": dataset_lock.get("weekday"),
                "session": dataset_lock.get("session"),
            },
            "symmetric_break_even": 0.505,
            "pair_summary": [],
            "action_counts": {},
            "zones": [],
            "empty_population": True,
        },
    )
    write_json(
        session_calibration_dir / "session_calibration_manifest.json",
        {
            "runner": "session_calibration.py",
            "inputs_hash": None,
            "status": "NO_POPULATION",
            "report": str(session_calibration_dir / "session_calibration_report.json"),
        },
    )

    session_opportunity_map_dir = out_root / "session_opportunity_map"
    write_json(
        session_opportunity_map_dir / "session_opportunity_map_report.json",
        {
            "status": "NO_POPULATION",
            "mode": "empty_node",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": {
                "pair": dataset_lock.get("pair"),
                "weekday": dataset_lock.get("weekday"),
                "session": dataset_lock.get("session"),
            },
            "pair_rollup": {
                "total_opportunities": 0,
                "total_opportunity_density_per_hour": 0.0,
                "distinct_timestamps": 0,
                "distinct_session_ids": 0,
                "long_opportunity_count": 0,
                "long_opportunity_density_per_hour": 0.0,
                "short_opportunity_count": 0,
                "short_opportunity_density_per_hour": 0.0,
            },
            "zones": [],
            "sanity_anomalies": [],
            "empty_population": True,
        },
    )
    write_json(
        session_opportunity_map_dir / "session_opportunity_map_manifest.json",
        {
            "runner": "session_opportunity_map.py",
            "inputs_hash": None,
            "status": "NO_POPULATION",
            "report": str(session_opportunity_map_dir / "session_opportunity_map_report.json"),
        },
    )

    write_json(
        out_root / "node_manifest.json",
        {
            "compiler": "market_node_compiler_v1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": {
                "pair": dataset_lock.get("pair"),
                "weekday": dataset_lock.get("weekday"),
                "session": dataset_lock.get("session"),
            },
            "dataset_lock_path": str(dataset_lock_path.resolve()),
            "dataset_hash": dataset_hash,
            "historical_fast": False,
            "research_lite": False,
            "data_root": str(dataset_lock.get("data_root")),
            "seed_entry_node": seed_entry_node or None,
            "seed_aee_node": seed_aee_node or None,
            "status": "NO_POPULATION",
            "empty_population": True,
            "artifacts": {
                "dataset_lock": str(dataset_lock_path.resolve()),
                "stage1_6": str(stage1_6_dir),
                "target_entry_stage": str(target_entry_stage_dir),
                "target_entry_no_timeouts": str(target_entry_no_timeouts_dir),
                "trade_type_truth": str(trade_type_truth_dir),
                "aee_stage": str(aee_stage_dir),
                "aee_target_local_fixedpop": str(aee_target_local_dir),
                "aee_target_theoretical_ceiling": str(aee_target_ceiling_dir),
                "session_calibration": str(out_root / "session_calibration"),
                "session_opportunity_map": str(out_root / "session_opportunity_map"),
            },
        },
    )


def require_phase1_proof(stage1_6_dir: Path, dataset_lock: dict[str, Any]) -> dict[str, Any]:
    return phase1_proof(stage1_6_dir, dataset_lock)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, default=DEFAULT_DATASET_LOCK)
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument("--seed-entry-node", type=str, default="")
    ap.add_argument("--seed-aee-node", type=str, default="")
    ap.add_argument("--historical-fast", action="store_true", help="Use frozen target-entry rules and skip research-time no-timeout optimization.")
    ap.add_argument("--research-lite", action="store_true", help="Use scoped target-entry research instead of full target fitting.")
    ap.add_argument("--research-max-sessions", type=int, default=3)
    ap.add_argument("--research-row-stride", type=int, default=3)
    ap.add_argument("--research-max-rows-per-session", type=int, default=180)
    ap.add_argument("--template-root", type=Path, default=DEFAULT_TEMPLATE_ROOT)
    ap.add_argument(
        "--pipeline-mode",
        choices=["full", "entry-only", "downstream-only", "aee-only", "aee-fixedpop-only"],
        default="full",
    )
    ap.add_argument("--batch-compile", action="store_true", help="Use template-first score/apply logic.")
    ap.add_argument("--force-heavy-delta-optimize", action="store_true", help="Allow heavy-delta entry optimization in batch mode.")
    ap.add_argument("--batch-tiny-sample", action="store_true", help="Force lightweight target-stage sampling in batch mode.")
    ap.add_argument(
        "--failure-route-override",
        choices=["quality_repair", "supply_expand", "state_surface_rebuild", "truth_rebuild"],
        help="Force a specific repair route for targeted reruns.",
    )
    args = ap.parse_args()

    dataset_lock = load_json(args.dataset_lock)
    key = node_key(dataset_lock)
    output_root_resolved = args.output_root.resolve()
    if output_root_resolved.name == key:
        out_root = output_root_resolved
    else:
        out_root = output_root_resolved / key
    out_root.mkdir(parents=True, exist_ok=True)
    batch_dir = out_root / "batch_compile"
    data_root = Path(str(dataset_lock["data_root"]))
    dataset_data_root = data_root if data_root.is_absolute() else ROOT / data_root
    dataset_hash = sha256_file(args.dataset_lock)

    if (
        args.pipeline_mode == "full"
        and node_outputs_exist(out_root)
        and node_manifest_matches(out_root, dataset_hash, args.historical_fast, args.research_lite)
    ):
        print(json.dumps({"status": "SKIP", "output_dir": str(out_root), "reason": "node_artifacts_current"}, indent=2))
        return
    if args.pipeline_mode == "full" and (out_root / "node_manifest.json").exists():
        clear_node_artifacts(out_root)

    stage_times: dict[str, float] = {}
    template_report: dict[str, Any] | None = None
    if args.batch_compile:
        template_report = session_template.ensure_template(
            args.output_root,
            str(dataset_lock["pair"]),
            str(dataset_lock["session"]),
            args.template_root,
        )
    batch_research_lite = args.research_lite or (args.batch_compile and args.batch_tiny_sample)
    batch_research_max_sessions = 2 if args.batch_compile and args.batch_tiny_sample else args.research_max_sessions
    batch_research_row_stride = 4 if args.batch_compile and args.batch_tiny_sample else args.research_row_stride
    batch_research_max_rows = 120 if args.batch_compile and args.batch_tiny_sample else args.research_max_rows_per_session

    # Stage 1-6 deterministic substrate.
    stage1_6_dir = out_root / "stage1_6"
    if not has_files(
        stage1_6_dir / "compiler_report.json",
        stage1_6_dir / "phase1" / "opportunity_map_summary.json",
        stage1_6_dir / "phase2" / "opportunity_clusters.csv",
        stage1_6_dir / "phase3" / "entry_window_states.csv",
        stage1_6_dir / "phase4" / "opportunity_zones_labeled.csv",
    ):
        t0 = time.time()
        run(
            [
                "python3",
                "stage1_5_deterministic_compiler.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--data-root",
                str(dataset_data_root),
                "--pair",
                str(dataset_lock["pair"]),
                "--output-root",
                str(stage1_6_dir),
            ]
        )
        stage_times["stage1_6"] = round(time.time() - t0, 4)
    proof = require_phase1_proof(stage1_6_dir, dataset_lock)
    if proof["empty"]:
        materialize_empty_node(
            args.output_root,
            out_root,
            args.dataset_lock,
            dataset_lock,
            sha256_file(args.dataset_lock),
            stage1_6_dir,
            args.seed_entry_node,
            args.seed_aee_node,
        )
        return

    # Target entry endpoint.
    target_entry_stage_dir = out_root / "target_entry_stage"
    if not target_stage_is_current(
        target_entry_stage_dir,
        args.dataset_lock,
        dataset_data_root,
        args.historical_fast,
        batch_research_lite,
        batch_research_max_sessions,
        batch_research_row_stride,
        batch_research_max_rows,
    ):
        t0 = time.time()
        template_stage1_root = None
        template_seed_root = None
        template_context_root = None
        if args.batch_compile and template_report and template_report.get("status") == "PASS":
            stage1_6_cache_dir = template_report.get("stage1_6_cache_dir")
            if stage1_6_cache_dir:
                template_stage1_root = Path(str(stage1_6_cache_dir))
            seed_cache_dir = template_report.get("seed_cache_dir")
            if seed_cache_dir:
                template_seed_root = Path(str(seed_cache_dir))
            contextual_cache_dir = template_report.get("contextual_cache_dir")
            if contextual_cache_dir:
                template_context_root = Path(str(contextual_cache_dir))
        cache_hit = False
        if args.batch_compile:
            cache_hit = materialize_template_target_stage(
                target_entry_stage_dir,
                template_stage1_root,
                template_seed_root,
                template_context_root,
                args.dataset_lock,
                dataset_data_root,
                args.historical_fast,
                batch_research_lite,
                batch_research_max_sessions,
                batch_research_row_stride,
                batch_research_max_rows,
            )
        if not cache_hit:
            run(
                [
                    "python3",
                    "run_target_entry_stage_compiler.py",
                    "--dataset-lock",
                    str(args.dataset_lock),
                    "--data-root",
                    str(dataset_data_root),
                    "--output-dir",
                    str(target_entry_stage_dir),
                    *(["--template-stage1-root", str(template_stage1_root)] if template_stage1_root and template_stage1_root.exists() else []),
                    *(["--template-seed-root", str(template_seed_root)] if template_seed_root and template_seed_root.exists() else []),
                    *(["--template-context-root", str(template_context_root)] if template_context_root and template_context_root.exists() else []),
                    *(["--historical-fast"] if args.historical_fast else []),
                    *(["--research-lite"] if batch_research_lite else []),
                    "--research-max-sessions",
                    str(batch_research_max_sessions),
                    "--research-row-stride",
                    str(batch_research_row_stride),
                    "--research-max-rows-per-session",
                    str(batch_research_max_rows),
                ]
            )
        stage_times["target_entry_stage"] = round(time.time() - t0, 4)

    # Exact no-timeout target entry endpoint.
    target_entry_no_timeouts_dir = out_root / "target_entry_no_timeouts"
    no_timeout_historical_fast = args.historical_fast
    no_timeout_priority_mode = "balanced"
    template_rules_path = None
    borrowed_rule_source = None
    no_timeout_seed_source: dict[str, Any] | None = None
    force_local_stage_rules = should_force_local_stage_rules(
        out_root,
        args.failure_route_override,
        str(dataset_lock.get("weekday", "")),
    )
    if args.batch_compile and template_report and template_report.get("status") == "PASS":
        template_rules_path = Path(str(template_report["entry_template_rules"]))
        no_timeout_historical_fast = True
    if (
        not force_local_stage_rules
        and str(dataset_lock.get("weekday", "")).lower() in {"thursday", "friday"}
    ):
        borrowed_rule_source = find_cross_day_rule_source(
            args.output_root,
            str(dataset_lock.get("pair", "")),
            str(dataset_lock.get("session", "")),
            str(dataset_lock.get("weekday", "")).lower(),
        )
    base_rules_path = template_rules_path or (target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json")
    targeted_rules_path = template_rules_path or (target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json")

    # Entry-only doctrine: start from related winning nodes before local rediscovery.
    if borrowed_rule_source:
        base_rules_path, targeted_rules_path = borrowed_rule_source
        no_timeout_seed_source = {
            "mode": "same_pair_cross_day",
            "pair": str(dataset_lock.get("pair", "")),
            "session": str(dataset_lock.get("session", "")),
            "weekday": str(dataset_lock.get("weekday", "")).lower(),
        }
    else:
        if force_local_stage_rules:
            no_timeout_seed_source = {
                "mode": "local_stage_rebuild",
                "reason": "stale_rescue_surface",
            }
        else:
            cross_pair_seed = find_cross_pair_rule_source(
                args.output_root,
                str(dataset_lock.get("pair", "")),
                str(dataset_lock.get("session", "")),
                str(dataset_lock.get("weekday", "")).lower(),
            )
            if cross_pair_seed:
                base_rules_path, targeted_rules_path, seed_node, similarity = cross_pair_seed
                no_timeout_seed_source = {
                    "mode": "cross_pair_same_session_day",
                    "seed_node": seed_node,
                    "pair_similarity": round(float(similarity), 4),
                }
    truth_csv_path = target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"
    if not no_timeout_stage_is_current(
        target_entry_no_timeouts_dir,
        base_rules_path,
        targeted_rules_path,
        truth_csv_path,
        no_timeout_historical_fast,
        no_timeout_priority_mode,
    ):
        t0 = time.time()
        run(
            [
                "python3",
                "run_target_entry_no_timeout.py",
                "--base-rules",
                str(base_rules_path),
                "--targeted-rules",
                str(targeted_rules_path),
                "--truth-csv",
                str(truth_csv_path),
                "--output-dir",
                str(target_entry_no_timeouts_dir),
                "--priority-mode",
                no_timeout_priority_mode,
                *(["--historical-fast"] if no_timeout_historical_fast else []),
            ]
        )
        stage_times["target_entry_no_timeouts"] = round(time.time() - t0, 4)

    session_calibration_dir = out_root / "session_calibration"
    session_opportunity_map_dir = out_root / "session_opportunity_map"
    entry_trade_rows_json = (
        None
        if args.pipeline_mode == "entry-only"
        else (out_root / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json")
    )
    if not session_calibration_is_current(
        session_calibration_dir,
        args.dataset_lock,
        target_entry_stage_dir,
        target_entry_no_timeouts_dir,
        entry_trade_rows_json,
    ):
        session_calibration.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=session_calibration_dir,
            trade_rows_json=entry_trade_rows_json,
            symmetric_break_even=0.505,
        )

    if not session_opportunity_map_is_current(
        session_opportunity_map_dir,
        args.dataset_lock,
        target_entry_stage_dir,
    ):
        session_opportunity_map.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            output_dir=session_opportunity_map_dir,
        )

    session_potential_dir = out_root / "session_potential"
    if not session_potential_is_current(
        session_potential_dir,
        args.dataset_lock,
        target_entry_stage_dir,
        target_entry_no_timeouts_dir,
    ):
        session_potential.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=session_potential_dir,
        )

    session_performance_dir = out_root / "session_performance_check"
    if not session_performance_check_is_current(
        session_performance_dir,
        args.dataset_lock,
        target_entry_no_timeouts_dir,
        entry_trade_rows_json,
        session_opportunity_map_dir,
        require_aee_trade_rows=(args.pipeline_mode != "entry-only"),
    ):
        session_performance_check.run(
            dataset_lock=args.dataset_lock,
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=session_performance_dir,
            trade_rows_json=entry_trade_rows_json,
            session_potential_json=(session_potential_dir / "session_potential_report.json"),
            session_opportunity_map_json=(session_opportunity_map_dir / "session_opportunity_map_report.json"),
            session_calibration_json=(session_calibration_dir / "session_calibration_report.json"),
            symmetric_break_even=0.505,
            min_side_trades=25,
            min_side_trades_per_hour=0.20,
            min_opportunities=100,
            require_aee_trade_rows=(args.pipeline_mode != "entry-only"),
        )

    session_calibration_report = load_json(session_calibration_dir / "session_calibration_report.json")
    session_potential_report = load_json(session_potential_dir / "session_potential_report.json")
    session_performance_report = load_json(session_performance_dir / "session_performance_check_report.json")
    if session_calibration_report.get("status") != "PASS":
        raise RuntimeError(
            json.dumps(
                {
                    "status": "INVALID_OPPORTUNITY_SANITY",
                    "node": key,
                    "report": str(session_calibration_dir / "session_calibration_report.json"),
                    "details": session_calibration_report.get("sanity_anomalies", []),
                },
                indent=2,
            )
        )

    node_classification = {
        "node_class": "accept",
        "failure_route": "none",
        "reason": "full_compile",
    }
    if args.batch_compile:
        node_classification = session_template.classify_node(
            template_report,
            session_calibration_report,
            session_potential_report,
        )
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "template_score_report.json").write_text(json.dumps(
            {
                "status": "PASS" if node_classification["node_class"] != "invalid" else "INVALID",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "node": key,
                "template_pair_session": f"{dataset_lock.get('pair')}__{dataset_lock.get('session')}",
                "template_report": (
                    str(session_template.template_dir(args.template_root, str(dataset_lock["pair"]), str(dataset_lock["session"])) / "session_template_report.json")
                    if template_report else None
                ),
                **node_classification,
            },
            indent=2,
        ))
        node_classification = classify_post_entry_reports(
            template_report,
            session_calibration_report,
            session_potential_report,
            session_performance_report,
            batch_compile=True,
        )
        if node_classification["node_class"] == "light_delta":
            route = node_classification["failure_route"]
            no_timeout_priority_mode = no_timeout_priority_mode_for_route(route)
            repair_base_rules = (
                borrowed_rule_source[0]
                if borrowed_rule_source
                else (template_rules_path or (target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json"))
            )
            repair_targeted_rules = (
                borrowed_rule_source[1]
                if borrowed_rule_source
                else (template_rules_path or (target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json"))
            )
            t0 = time.time()
            run(
                [
                    "python3",
                    "run_target_entry_no_timeout.py",
                    "--base-rules",
                    str(repair_base_rules),
                    "--targeted-rules",
                    str(repair_targeted_rules),
                    "--truth-csv",
                    str(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
                    "--output-dir",
                    str(target_entry_no_timeouts_dir),
                    "--priority-mode",
                    no_timeout_priority_mode,
                ]
            )
            stage_times["light_delta_no_timeout"] = round(time.time() - t0, 4)
            session_calibration.run(
                dataset_lock=args.dataset_lock,
                truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
                entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                output_dir=session_calibration_dir,
                trade_rows_json=entry_trade_rows_json,
                symmetric_break_even=0.505,
            )
            session_potential.run(
                dataset_lock=args.dataset_lock,
                truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
                entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                output_dir=session_potential_dir,
            )
            session_performance_check.run(
                dataset_lock=args.dataset_lock,
                entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                output_dir=session_performance_dir,
                trade_rows_json=entry_trade_rows_json,
                session_potential_json=(session_potential_dir / "session_potential_report.json"),
                session_opportunity_map_json=(session_opportunity_map_dir / "session_opportunity_map_report.json"),
                session_calibration_json=(session_calibration_dir / "session_calibration_report.json"),
                symmetric_break_even=0.505,
                min_side_trades=25,
                min_side_trades_per_hour=0.20,
                min_opportunities=100,
            )
            session_calibration_report = load_json(session_calibration_dir / "session_calibration_report.json")
            session_potential_report = load_json(session_potential_dir / "session_potential_report.json")
            session_performance_report = load_json(session_performance_dir / "session_performance_check_report.json")
            node_classification = classify_post_entry_reports(
                template_report,
                session_calibration_report,
                session_potential_report,
                session_performance_report,
                batch_compile=True,
            )
    else:
        node_classification = classify_post_entry_reports(
            template_report,
            session_calibration_report,
            session_potential_report,
            session_performance_report,
            batch_compile=False,
        )

    if args.failure_route_override and session_performance_report.get("status") == "REPAIR_REQUIRED":
        node_classification = {
            "node_class": "heavy_delta",
            "failure_route": args.failure_route_override,
            "reason": "manual_failure_route_override",
        }

    if node_classification["node_class"] == "heavy_delta" and args.force_heavy_delta_optimize:
            route = node_classification["failure_route"]
            no_timeout_priority_mode = no_timeout_priority_mode_for_route(route)
            if route == "state_surface_rebuild":
                shutil.rmtree(target_entry_stage_dir, ignore_errors=True)
                shutil.rmtree(target_entry_no_timeouts_dir, ignore_errors=True)
                t_stage = time.time()
                run(
                    [
                        "python3",
                        "run_target_entry_stage_compiler.py",
                        "--dataset-lock",
                        str(args.dataset_lock),
                        "--data-root",
                        str(dataset_data_root),
                        "--output-dir",
                        str(target_entry_stage_dir),
                        *(["--historical-fast"] if args.historical_fast else []),
                        *(["--research-lite"] if batch_research_lite else []),
                        "--research-max-sessions",
                        str(batch_research_max_sessions),
                        "--research-row-stride",
                        str(batch_research_row_stride),
                        "--research-max-rows-per-session",
                        str(batch_research_max_rows),
                    ]
                )
                stage_times["heavy_delta_target_stage_rebuild"] = round(time.time() - t_stage, 4)
                repair_base_rules = target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json"
                repair_targeted_rules = target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json"
            else:
                repair_base_rules = (
                    borrowed_rule_source[0]
                    if borrowed_rule_source
                    else (target_entry_stage_dir / "target_contextual_v2" / "target_entry_classes.json")
                )
                repair_targeted_rules = (
                    borrowed_rule_source[1]
                    if borrowed_rule_source
                    else (target_entry_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json")
                )
            truth_csv = target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"
            repair_scope = build_no_timeout_optimization_scope(
                route,
                session_performance_report,
                target_entry_stage_dir / "target_contextual_v2" / "target_entry_class_report.json",
                repair_base_rules,
                repair_targeted_rules,
            )
            preserved_rules_snapshot = out_root / "target_entry_no_timeouts_preserve_rules.json"
            preserved_no_timeout_dir = out_root / "target_entry_no_timeouts_preserve_snapshot"
            current_no_timeout_rules = target_entry_no_timeouts_dir / "target_entry_classes.json"
            if target_entry_no_timeouts_dir.exists():
                copy_tree(target_entry_no_timeouts_dir, preserved_no_timeout_dir)
            elif preserved_no_timeout_dir.exists():
                shutil.rmtree(preserved_no_timeout_dir, ignore_errors=True)
            if current_no_timeout_rules.exists():
                shutil.copy2(current_no_timeout_rules, preserved_rules_snapshot)
            elif preserved_rules_snapshot.exists():
                preserved_rules_snapshot.unlink()
            candidate_modes = [no_timeout_priority_mode]
            if route == "state_surface_rebuild":
                for mode in state_surface_modes_for_report(session_performance_report):
                    if mode not in candidate_modes:
                        candidate_modes.append(mode)
            scope_variants = scope_variants_for_repair(repair_scope)

            best_report = None
            best_mode = no_timeout_priority_mode
            best_scope = scope_variants[0]
            total_no_timeout_time = 0.0
            best_guard_reason = "no_candidate_accepted"
            for scope_variant in scope_variants:
                for candidate_mode in candidate_modes:
                    shutil.rmtree(target_entry_no_timeouts_dir, ignore_errors=True)
                    t0 = time.time()
                    run(
                        [
                            "python3",
                            "run_target_entry_no_timeout.py",
                            "--base-rules",
                            str(repair_base_rules),
                            "--targeted-rules",
                            str(repair_targeted_rules),
                            "--truth-csv",
                            str(truth_csv),
                            "--output-dir",
                            str(target_entry_no_timeouts_dir),
                            "--priority-mode",
                            candidate_mode,
                            *(["--frozen-rules", str(preserved_rules_snapshot)] if preserved_rules_snapshot.exists() else []),
                            *(["--freeze-unlisted-groups"] if repair_scope.get("freeze_unlisted_groups") else []),
                            *[
                                arg
                                for spec in scope_variant.get("optimize_groups", [])
                                for arg in ("--optimize-group", str(spec))
                            ],
                        ]
                    )
                    total_no_timeout_time += time.time() - t0
                    session_calibration.run(
                        dataset_lock=args.dataset_lock,
                        truth_csv=truth_csv,
                        entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                        output_dir=session_calibration_dir,
                        trade_rows_json=entry_trade_rows_json,
                        symmetric_break_even=0.505,
                    )
                    session_potential.run(
                        dataset_lock=args.dataset_lock,
                        truth_csv=truth_csv,
                        entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                        output_dir=session_potential_dir,
                    )
                    session_performance_check.run(
                        dataset_lock=args.dataset_lock,
                        entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
                        output_dir=session_performance_dir,
                        trade_rows_json=entry_trade_rows_json,
                        session_potential_json=(session_potential_dir / "session_potential_report.json"),
                        session_opportunity_map_json=(session_opportunity_map_dir / "session_opportunity_map_report.json"),
                        session_calibration_json=(session_calibration_dir / "session_calibration_report.json"),
                        symmetric_break_even=0.505,
                        min_side_trades=25,
                        min_side_trades_per_hour=0.20,
                        min_opportunities=100,
                    )
                    candidate_calibration_report = load_json(session_calibration_dir / "session_calibration_report.json")
                    candidate_report = load_json(session_performance_dir / "session_performance_check_report.json")
                    accepted, guard_reason = repair_guard_result(
                        session_performance_report,
                        candidate_report,
                        repair_scope,
                        session_calibration_report,
                        candidate_calibration_report,
                    )
                    if not accepted:
                        best_guard_reason = guard_reason
                        continue
                    if best_report is None or performance_report_rank(candidate_report) > performance_report_rank(best_report):
                        best_report = candidate_report
                        best_mode = candidate_mode
                        best_scope = scope_variant
                        best_guard_reason = guard_reason
                    if candidate_report.get("status") == "PASS":
                        break
                if best_report is not None and best_report.get("status") == "PASS":
                    break
            stage_times["heavy_delta_no_timeout"] = round(total_no_timeout_time, 4)
            if best_report is None:
                if preserved_no_timeout_dir.exists():
                    shutil.rmtree(target_entry_no_timeouts_dir, ignore_errors=True)
                    copy_tree(preserved_no_timeout_dir, target_entry_no_timeouts_dir)
                elif preserved_rules_snapshot.exists():
                    shutil.rmtree(target_entry_no_timeouts_dir, ignore_errors=True)
                    target_entry_no_timeouts_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(preserved_rules_snapshot, target_entry_no_timeouts_dir / "target_entry_classes.json")
                node_classification = {
                    "node_class": "heavy_delta",
                    "failure_route": route,
                    "reason": f"repair_rejected:{best_guard_reason}",
                }
            else:
                no_timeout_priority_mode = best_mode
                session_calibration_report = load_json(session_calibration_dir / "session_calibration_report.json")
                session_potential_report = load_json(session_potential_dir / "session_potential_report.json")
                session_performance_report = best_report
                node_classification = classify_post_entry_reports(
                    template_report,
                    session_calibration_report,
                    session_potential_report,
                    session_performance_report,
                    batch_compile=args.batch_compile,
                )

    if args.pipeline_mode == "entry-only":
        result = {
            "status": "ENTRY_PASS" if node_classification["node_class"] in {"accept", "light_delta"} else node_classification["node_class"].upper(),
            "node": key,
            "node_class": node_classification["node_class"],
            "failure_route": node_classification["failure_route"],
            "reason": node_classification["reason"],
            "stage_times": stage_times,
            "output_root": str(out_root),
        }
        (out_root / "node_manifest.json").write_text(
            json.dumps(
                {
                    "compiler": "market_node_compiler_v2",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "node": {
                        "pair": dataset_lock.get("pair"),
                        "weekday": dataset_lock.get("weekday"),
                        "session": dataset_lock.get("session"),
                    },
                    "dataset_lock_path": str(args.dataset_lock),
                    "dataset_hash": dataset_hash,
                    "historical_fast": args.historical_fast,
                    "research_lite": batch_research_lite,
                    "pipeline_mode": args.pipeline_mode,
                    "batch_compile": args.batch_compile,
                    "entry_rule_seed_source": no_timeout_seed_source,
                    "node_class": node_classification["node_class"],
                    "failure_route": node_classification["failure_route"],
                    "reason": node_classification["reason"],
                    "stage_times": stage_times,
                    "artifacts": {
                        "dataset_lock": str(args.dataset_lock.resolve()),
                        "stage1_6": str(stage1_6_dir.resolve()),
                        "target_entry_stage": str(target_entry_stage_dir.resolve()),
                        "target_entry_no_timeouts": str(target_entry_no_timeouts_dir.resolve()),
                        "session_calibration": str(session_calibration_dir.resolve()),
                        "session_opportunity_map": str(session_opportunity_map_dir.resolve()),
                        "session_potential": str(session_potential_dir.resolve()),
                        "session_performance_check": str(session_performance_dir.resolve()),
                    },
                },
                indent=2,
            )
        )
        print(json.dumps(result, indent=2))
        return

    # Canonical AEE stage.
    aee_stage_dir = out_root / "aee_stage"
    if not aee_stage_is_current(aee_stage_dir, dataset_lock, target_entry_stage_dir, target_entry_no_timeouts_dir):
        t0 = time.time()
        run(
            [
                "python3",
                "run_aee_stage_compiler.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--truth-csv",
                str(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
                "--entry-rules-json",
                str(target_entry_no_timeouts_dir / "target_entry_classes.json"),
                "--output-dir",
                str(aee_stage_dir),
            ]
        )
        stage_times["aee_stage"] = round(time.time() - t0, 4)

    # Exact per-class AEE on fixed selected population.
    aee_target_local_dir = out_root / "aee_target_local_fixedpop"
    if not local_fixedpop_is_current(aee_target_local_dir, args.dataset_lock, target_entry_no_timeouts_dir, aee_stage_dir):
        t0 = time.time()
        local_fixedpop.run(
            args.dataset_lock,
            target_entry_no_timeouts_dir / "target_entry_population.csv",
            aee_stage_dir,
            aee_target_local_dir,
        )
        stage_times["aee_target_local_fixedpop"] = round(time.time() - t0, 4)

    if args.pipeline_mode == "aee-fixedpop-only":
        fixedpop_trade_rows_path = aee_target_local_dir / "target_local_fixedpop_aee_trade_rows.json"
        fixedpop_trade_count = load_trade_row_count(fixedpop_trade_rows_path)
        if fixedpop_trade_count <= 0:
            write_blocker_report(
                out_root / "compiler_blockers",
                "zero_fixedpop_trade_rows",
                {
                    "node": key,
                    "dataset_lock": str(args.dataset_lock),
                    "aee_stage_dir": str(aee_stage_dir),
                    "aee_target_local_fixedpop_dir": str(aee_target_local_dir),
                    "trade_rows_json": str(fixedpop_trade_rows_path),
                    "trade_row_count": fixedpop_trade_count,
                },
            )
            print(
                json.dumps(
                    {
                        "status": "AEE_FIXEDPOP_BLOCKED",
                        "node": key,
                        "reason": "zero_fixedpop_trade_rows",
                        "output_root": str(out_root),
                    },
                    indent=2,
                )
            )
            return
        manifest = {
            "compiler": "market_node_compiler_v2",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": {
                "pair": dataset_lock.get("pair"),
                "weekday": dataset_lock.get("weekday"),
                "session": dataset_lock.get("session"),
            },
            "dataset_lock_path": str(args.dataset_lock),
            "dataset_hash": dataset_hash,
            "historical_fast": args.historical_fast,
            "pipeline_mode": args.pipeline_mode,
            "stage_times": stage_times,
            "artifacts": {
                "dataset_lock": str(args.dataset_lock.resolve()),
                "aee_stage": str(aee_stage_dir.resolve()),
                "aee_target_local_fixedpop": str(aee_target_local_dir.resolve()),
            },
        }
        (out_root / "node_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps({"status": "AEE_FIXEDPOP_PASS", "node": key, "output_root": str(out_root)}, indent=2))
        return

    # Trade-type truth rebuilt from this node's own AEE state stream.
    trade_type_truth_dir = out_root / "trade_type_truth"
    if not trade_type_truth_is_current(trade_type_truth_dir, target_entry_stage_dir, aee_stage_dir):
        t0 = time.time()
        run(
            [
                "python3",
                "build_trade_type_truth.py",
                "--entry-truth",
                str(target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
                "--aee-state",
                str(aee_stage_dir / "aee_state_stream" / "aee_state_stream.csv"),
                "--output-dir",
                str(trade_type_truth_dir),
            ]
        )
        stage_times["trade_type_truth"] = round(time.time() - t0, 4)

    if args.pipeline_mode == "aee-only":
        manifest = {
            "compiler": "market_node_compiler_v2",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node": {
                "pair": dataset_lock.get("pair"),
                "weekday": dataset_lock.get("weekday"),
                "session": dataset_lock.get("session"),
            },
            "dataset_lock_path": str(args.dataset_lock),
            "dataset_hash": dataset_hash,
            "historical_fast": args.historical_fast,
            "pipeline_mode": args.pipeline_mode,
            "stage_times": stage_times,
            "artifacts": {
                "dataset_lock": str(args.dataset_lock.resolve()),
                "aee_stage": str(aee_stage_dir.resolve()),
                "aee_target_local_fixedpop": str(aee_target_local_dir.resolve()),
            },
        }
        (out_root / "node_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps({"status": "AEE_PASS", "node": key, "output_root": str(out_root)}, indent=2))
        return

    # Theoretical exact-class AEE ceiling pass.
    aee_target_ceiling_dir = out_root / "aee_target_theoretical_ceiling"
    if not theoretical_ceiling_is_current(aee_target_ceiling_dir, args.dataset_lock, target_entry_no_timeouts_dir, aee_stage_dir):
        t0 = time.time()
        theoretical_ceiling.run(
            args.dataset_lock,
            target_entry_no_timeouts_dir / "target_entry_population.csv",
            aee_stage_dir,
            aee_target_ceiling_dir,
        )
        stage_times["aee_target_theoretical_ceiling"] = round(time.time() - t0, 4)

    if not session_calibration_is_current(
        session_calibration_dir,
        args.dataset_lock,
        target_entry_stage_dir,
        target_entry_no_timeouts_dir,
        aee_target_local_dir,
    ):
        session_calibration.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=session_calibration_dir,
            trade_rows_json=aee_target_local_dir / "target_local_fixedpop_aee_trade_rows.json",
            symmetric_break_even=0.505,
        )
    if not session_opportunity_map_is_current(
        out_root / "session_opportunity_map",
        args.dataset_lock,
        target_entry_stage_dir,
    ):
        session_opportunity_map.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            output_dir=out_root / "session_opportunity_map",
        )
    if not session_potential_is_current(
        session_potential_dir,
        args.dataset_lock,
        target_entry_stage_dir,
        target_entry_no_timeouts_dir,
    ):
        session_potential.run(
            dataset_lock=args.dataset_lock,
            truth_csv=target_entry_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv",
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=session_potential_dir,
        )
    if not session_performance_check_is_current(
        out_root / "session_performance_check",
        args.dataset_lock,
        target_entry_no_timeouts_dir,
        aee_target_local_dir,
        out_root / "session_opportunity_map",
        require_aee_trade_rows=(args.pipeline_mode != "entry-only"),
    ):
        session_performance_check.run(
            dataset_lock=args.dataset_lock,
            entry_population_csv=target_entry_no_timeouts_dir / "target_entry_population.csv",
            output_dir=out_root / "session_performance_check",
            trade_rows_json=aee_target_local_dir / "target_local_fixedpop_aee_trade_rows.json",
            session_potential_json=(out_root / "session_potential" / "session_potential_report.json"),
            session_opportunity_map_json=(out_root / "session_opportunity_map" / "session_opportunity_map_report.json"),
            session_calibration_json=(out_root / "session_calibration" / "session_calibration_report.json"),
            symmetric_break_even=0.505,
            min_side_trades=25,
            min_side_trades_per_hour=0.20,
            min_opportunities=100,
            require_aee_trade_rows=(args.pipeline_mode != "entry-only"),
        )

    manifest = {
        "compiler": "market_node_compiler_v2",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node": {
            "pair": dataset_lock.get("pair"),
            "weekday": dataset_lock.get("weekday"),
            "session": dataset_lock.get("session"),
        },
        "dataset_lock_path": str(args.dataset_lock),
        "dataset_hash": dataset_hash,
        "historical_fast": args.historical_fast,
        "research_lite": batch_research_lite,
        "pipeline_mode": args.pipeline_mode,
        "batch_compile": args.batch_compile,
        "data_root": str(dataset_data_root),
        "seed_entry_node": args.seed_entry_node or None,
        "seed_aee_node": args.seed_aee_node or None,
        "entry_rule_seed_source": no_timeout_seed_source,
        "node_class": node_classification["node_class"],
        "failure_route": node_classification["failure_route"],
        "stage_times": stage_times,
        "artifacts": {
            "dataset_lock": str(args.dataset_lock.resolve().relative_to(ROOT)) if args.dataset_lock.exists() else None,
            "stage1_6": str(stage1_6_dir.resolve().relative_to(ROOT)),
            "target_entry_stage": str(target_entry_stage_dir.resolve().relative_to(ROOT)),
            "target_entry_no_timeouts": str(target_entry_no_timeouts_dir.resolve().relative_to(ROOT)),
            "trade_type_truth": str(trade_type_truth_dir.resolve().relative_to(ROOT)),
            "aee_stage": str(aee_stage_dir.resolve().relative_to(ROOT)),
            "aee_target_local_fixedpop": str(aee_target_local_dir.resolve().relative_to(ROOT)),
            "aee_target_theoretical_ceiling": str(aee_target_ceiling_dir.resolve().relative_to(ROOT)),
            "session_calibration": str(session_calibration_dir.resolve().relative_to(ROOT)),
            "session_opportunity_map": str((out_root / "session_opportunity_map").resolve().relative_to(ROOT)),
            "session_potential": str(session_potential_dir.resolve().relative_to(ROOT)),
            "session_performance_check": str((out_root / "session_performance_check").resolve().relative_to(ROOT)),
        },
    }
    (out_root / "node_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"status": "PASS", "node": key, "output_root": str(out_root)}, indent=2))


if __name__ == "__main__":
    main()
