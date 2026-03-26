#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime_settings import write_runtime_settings

ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "compiled_market_nodes"
DEFAULT_TEMPLATE_ROOT = ROOT / "compiled_session_templates"
MIN_TEMPLATE_SOURCES = 1


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_rmtree(path: Path) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def jload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def safe_median(values: list[float]) -> float:
    clean = [float(v) for v in values if isinstance(v, (int, float))]
    return float(statistics.median(clean)) if clean else 0.0


def zone_key(zone: dict[str, Any]) -> tuple[str, str, float]:
    return (
        str(zone.get("quarter")),
        str(zone.get("direction")),
        float(zone.get("target_distance", 0.0)),
    )


def template_dir(template_root: Path, pair: str, session: str) -> Path:
    return template_root / f"{pair.lower()}__{session.lower()}"


def find_source_nodes(output_root: Path, pair: str, session: str) -> list[Path]:
    pattern = f"{pair}__*__{session}"
    return sorted(p for p in output_root.glob(pattern) if p.is_dir())


def find_pair_fallback_nodes(output_root: Path, pair: str) -> list[Path]:
    pattern = f"{pair}__*__*"
    return sorted(p for p in output_root.glob(pattern) if p.is_dir())


def node_has_template_inputs(node_dir: Path) -> bool:
    required = (
        node_dir / "session_calibration" / "session_calibration_report.json",
        node_dir / "session_potential" / "session_potential_report.json",
        node_dir / "target_entry_no_timeouts" / "target_entry_classes.json",
        node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv",
    )
    if not all(path.exists() for path in required):
        return False
    truth_csv = node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"
    try:
        with truth_csv.open() as f:
            row_count = max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return False
    return row_count > 0


def score_source_node(node_dir: Path) -> float:
    try:
        calibration = jload(node_dir / "session_calibration" / "session_calibration_report.json")
        potential = jload(node_dir / "session_potential" / "session_potential_report.json")
    except Exception:
        return float("-inf")
    if calibration.get("status") != "PASS" or potential.get("status") != "PASS":
        return float("-inf")
    pair_summary = calibration.get("pair_summary", [])
    wr_floor = min(float(row.get("entry_win_rate", 0.0)) for row in pair_summary) if pair_summary else 0.0
    pair_rollup = potential.get("pair_rollup", {})
    util = float(pair_rollup.get("long_utilization_ratio", 0.0)) + float(pair_rollup.get("short_utilization_ratio", 0.0))
    density = float(pair_rollup.get("actual_long_trades_per_hour", 0.0)) + float(pair_rollup.get("actual_short_trades_per_hour", 0.0))
    return wr_floor * 1000.0 + util * 100.0 + density


def build_template(output_root: Path, pair: str, session: str, template_root: Path) -> dict[str, Any]:
    source_nodes = [n for n in find_source_nodes(output_root, pair, session) if node_has_template_inputs(n)]
    usable = []
    for node_dir in source_nodes:
        try:
            calibration = jload(node_dir / "session_calibration" / "session_calibration_report.json")
            potential = jload(node_dir / "session_potential" / "session_potential_report.json")
            opportunity_map = (
                jload(node_dir / "session_opportunity_map" / "session_opportunity_map_report.json")
                if (node_dir / "session_opportunity_map" / "session_opportunity_map_report.json").exists()
                else {}
            )
        except Exception:
            continue
        if calibration.get("status") != "PASS" or potential.get("status") != "PASS":
            continue
        usable.append((node_dir, calibration, potential, opportunity_map))

    tpl_dir = template_dir(template_root, pair, session)
    tpl_dir.mkdir(parents=True, exist_ok=True)
    report_path = tpl_dir / "session_template_report.json"
    manifest_path = tpl_dir / "session_template_manifest.json"
    entry_rules_path = tpl_dir / "entry_template_rules.json"
    aee_rules_path = tpl_dir / "aee_template_rules.json"

    fallback_mode = False
    if len(usable) < MIN_TEMPLATE_SOURCES:
        fallback_usable = []
        for node_dir in [n for n in find_pair_fallback_nodes(output_root, pair) if node_has_template_inputs(n)]:
            try:
                calibration = jload(node_dir / "session_calibration" / "session_calibration_report.json")
                potential = jload(node_dir / "session_potential" / "session_potential_report.json")
                opportunity_map = (
                    jload(node_dir / "session_opportunity_map" / "session_opportunity_map_report.json")
                    if (node_dir / "session_opportunity_map" / "session_opportunity_map_report.json").exists()
                    else {}
                )
            except Exception:
                continue
            if calibration.get("status") != "PASS" or potential.get("status") != "PASS":
                continue
            fallback_usable.append((node_dir, calibration, potential, opportunity_map))
        if fallback_usable:
            usable = fallback_usable
            fallback_mode = True
        else:
            report = {
                "status": "MISSING_SOURCES",
                "timestamp": iso_now(),
                "pair": pair,
                "session": session,
                "source_count": len(usable),
            }
            report_path.write_text(json.dumps(report, indent=2))
            manifest_path.write_text(json.dumps({"status": "MISSING_SOURCES", "report": str(report_path)}, indent=2))
            return report

    best_node = max((node for node, _, _, _ in usable), key=score_source_node)
    best_calibration = jload(best_node / "session_calibration" / "session_calibration_report.json")
    best_potential = jload(best_node / "session_potential" / "session_potential_report.json")
    shutil.copy2(best_node / "target_entry_no_timeouts" / "target_entry_classes.json", entry_rules_path)
    aee_src = best_node / "aee_stage" / "aee_rules" / "aee_rules.json"
    if aee_src.exists():
        shutil.copy2(aee_src, aee_rules_path)
    stage1_6_cache_dir = tpl_dir / "stage1_6_cache"
    stage1_6_src = best_node / "target_entry_stage" / "stage1_6"
    if stage1_6_src.exists():
        if stage1_6_cache_dir.exists():
            safe_rmtree(stage1_6_cache_dir)
        shutil.copytree(stage1_6_src, stage1_6_cache_dir)
    seed_cache_dir = tpl_dir / "seed_cache"
    for seed_name in ["stream_seed", "context_seed", "trajectory_seed"]:
        src = best_node / "target_entry_stage" / seed_name
        dst = seed_cache_dir / seed_name
        if src.exists():
            if dst.exists():
                safe_rmtree(dst)
            shutil.copytree(src, dst)
    contextual_cache_dir = tpl_dir / "contextual_cache"
    for ctx_name in ["target_contextual_v2", "target_contextual_v2_targeted"]:
        src = best_node / "target_entry_stage" / ctx_name
        dst = contextual_cache_dir / ctx_name
        if src.exists():
            if dst.exists():
                safe_rmtree(dst)
            shutil.copytree(src, dst)

    zone_buckets: dict[tuple[str, str, float], list[dict[str, Any]]] = {}
    for _, calibration, potential, opportunity_map in usable:
        potential_by_key = {zone_key(z): z for z in potential.get("zones", [])}
        opportunity_by_key = {zone_key(z): z for z in opportunity_map.get("zones", [])} if isinstance(opportunity_map, dict) else {}
        for zone in calibration.get("zones", []):
            key = zone_key(zone)
            zone_buckets.setdefault(key, []).append(
                {
                    "calibration": zone,
                    "potential": potential_by_key.get(key, {}),
                    "opportunity_map": opportunity_by_key.get(key, {}),
                }
            )

    template_zones = []
    for key in sorted(zone_buckets.keys()):
        samples = zone_buckets[key]
        template_zones.append(
            {
                "quarter": key[0],
                "direction": key[1],
                "target_distance": key[2],
                "template_opportunity_density_per_hour": safe_median(
                    [
                        s["opportunity_map"].get(
                            "opportunity_density_per_hour",
                            s["calibration"].get("opportunity_density_per_hour", 0.0),
                        )
                        for s in samples
                    ]
                ),
                "template_opportunity_count": safe_median(
                    [s["opportunity_map"].get("opportunity_count", 0.0) for s in samples]
                ),
                "template_selected_density_per_hour": safe_median(
                    [s["calibration"].get("selected_density_per_hour", 0.0) for s in samples]
                ),
                "template_entry_win_rate": safe_median(
                    [s["calibration"].get("entry_win_rate", 0.0) for s in samples]
                ),
                "template_downstream_density_per_hour": safe_median(
                    [s["calibration"].get("downstream_density_per_hour", 0.0) for s in samples]
                ),
                "template_expected_opportunities_per_hour": safe_median(
                    [s["potential"].get("expected_opportunities_per_hour", 0.0) for s in samples]
                ),
                "template_expected_recyclable_opportunities_per_hour": safe_median(
                    [s["potential"].get("expected_recyclable_opportunities_per_hour", 0.0) for s in samples]
                ),
                "template_tp_feasible_density_per_hour": safe_median(
                    [s["opportunity_map"].get("tp_feasible_density_per_hour", 0.0) for s in samples]
                ),
                "template_actual_trades_per_hour": safe_median(
                    [s["potential"].get("actual_trades_per_hour", 0.0) for s in samples]
                ),
                "template_utilization_ratio": safe_median(
                    [s["potential"].get("utilization_ratio", 0.0) for s in samples]
                ),
                "template_recycling_utilization_ratio": safe_median(
                    [s["potential"].get("recycling_utilization_ratio", 0.0) for s in samples]
                ),
                "preferred_action": max(
                    (s["calibration"].get("action", "stabilize") for s in samples),
                    key=lambda action: sum(1 for s in samples if s["calibration"].get("action") == action),
                ),
            }
        )

    pair_summaries = [c.get("pair_summary", []) for _, c, _, _ in usable]
    pair_summary_by_dir: dict[str, dict[str, float]] = {}
    for direction in ["LONG", "SHORT"]:
        rows = [
            row
            for summary in pair_summaries
            for row in summary
            if row.get("dir") == direction or row.get("direction") == direction
        ]
        pair_summary_by_dir[direction] = {
            "template_trades_per_hour": safe_median(
                [
                    row.get(
                        "trades_per_hour",
                        row.get(
                            "selected_density_per_hour",
                            (float(row.get("selected_count", 0.0)) / 88.0 if row.get("selected_count") is not None else 0.0),
                        ),
                    )
                    for row in rows
                ]
            ),
            "template_entry_win_rate": safe_median([row.get("entry_win_rate", 0.0) for row in rows]),
            "template_selected_count": safe_median([row.get("selected_count", 0.0) for row in rows]),
        }

    rollups = [p.get("pair_rollup", {}) for _, _, p, _ in usable]
    map_rollups = [m.get("pair_rollup", {}) for _, _, _, m in usable if isinstance(m, dict)]
    report = {
        "status": "PASS",
        "timestamp": iso_now(),
        "pair": pair,
        "session": session,
        "bootstrap_mode": "pair_fallback" if fallback_mode else "session_native",
        "source_count": len(usable),
        "source_nodes": [node.name for node, _, _, _ in usable],
        "best_source_node": best_node.name,
        "entry_template_rules": str(entry_rules_path),
        "aee_template_rules": str(aee_rules_path) if aee_rules_path.exists() else None,
        "stage1_6_cache_dir": str(stage1_6_cache_dir) if stage1_6_cache_dir.exists() else None,
        "seed_cache_dir": str(seed_cache_dir) if seed_cache_dir.exists() else None,
        "contextual_cache_dir": str(contextual_cache_dir) if contextual_cache_dir.exists() else None,
        "pair_template": {
            "LONG": {
                **pair_summary_by_dir["LONG"],
                "template_expected_opportunities_per_hour": safe_median(
                    [r.get("expected_long_opportunities_per_hour", 0.0) for r in rollups]
                ),
                "template_mapped_opportunity_density_per_hour": safe_median(
                    [r.get("long_opportunity_density_per_hour", 0.0) for r in map_rollups]
                ),
                "template_utilization_ratio": safe_median(
                    [r.get("long_utilization_ratio", 0.0) for r in rollups]
                ),
                "template_recycling_utilization_ratio": safe_median(
                    [r.get("long_recycling_utilization_ratio", 0.0) for r in rollups]
                ),
            },
            "SHORT": {
                **pair_summary_by_dir["SHORT"],
                "template_expected_opportunities_per_hour": safe_median(
                    [r.get("expected_short_opportunities_per_hour", 0.0) for r in rollups]
                ),
                "template_mapped_opportunity_density_per_hour": safe_median(
                    [r.get("short_opportunity_density_per_hour", 0.0) for r in map_rollups]
                ),
                "template_utilization_ratio": safe_median(
                    [r.get("short_utilization_ratio", 0.0) for r in rollups]
                ),
                "template_recycling_utilization_ratio": safe_median(
                    [r.get("short_recycling_utilization_ratio", 0.0) for r in rollups]
                ),
            },
        },
        "zones": template_zones,
    }
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "script_hash": sha256_file(Path(__file__)),
                "source_nodes": [
                    {
                        "node": node.name,
                        "calibration_hash": sha256_file(node / "session_calibration" / "session_calibration_report.json"),
                        "potential_hash": sha256_file(node / "session_potential" / "session_potential_report.json"),
                        "opportunity_map_hash": (
                            sha256_file(node / "session_opportunity_map" / "session_opportunity_map_report.json")
                            if (node / "session_opportunity_map" / "session_opportunity_map_report.json").exists()
                            else None
                        ),
                        "rules_hash": sha256_file(node / "target_entry_no_timeouts" / "target_entry_classes.json"),
                    }
                    for node, _, _, _ in usable
                ],
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "runner": "session_template.py",
        "inputs_hash": inputs_hash,
        "pair": pair,
        "session": session,
        "report": str(report_path),
        "entry_template_rules": str(entry_rules_path),
        "aee_template_rules": str(aee_rules_path) if aee_rules_path.exists() else None,
    }
    report_path.write_text(json.dumps(report, indent=2))
    manifest_path.write_text(json.dumps(manifest, indent=2))
    write_runtime_settings({**report, "source_template_report": str(report_path)}, template_root=template_root)
    return report


def template_current(output_root: Path, pair: str, session: str, template_root: Path) -> bool:
    tpl_dir = template_dir(template_root, pair, session)
    report_path = tpl_dir / "session_template_report.json"
    manifest_path = tpl_dir / "session_template_manifest.json"
    if not report_path.exists() or not manifest_path.exists():
        return False
    try:
        manifest = jload(manifest_path)
    except Exception:
        return False
    source_nodes = [n for n in find_source_nodes(output_root, pair, session) if node_has_template_inputs(n)]
    usable = []
    for node_dir in source_nodes:
        try:
            calibration = jload(node_dir / "session_calibration" / "session_calibration_report.json")
            potential = jload(node_dir / "session_potential" / "session_potential_report.json")
        except Exception:
            continue
        if calibration.get("status") == "PASS" and potential.get("status") == "PASS":
            usable.append(node_dir)
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "script_hash": sha256_file(Path(__file__)),
                "source_nodes": [
                    {
                        "node": node.name,
                        "calibration_hash": sha256_file(node / "session_calibration" / "session_calibration_report.json"),
                        "potential_hash": sha256_file(node / "session_potential" / "session_potential_report.json"),
                        "opportunity_map_hash": (
                            sha256_file(node / "session_opportunity_map" / "session_opportunity_map_report.json")
                            if (node / "session_opportunity_map" / "session_opportunity_map_report.json").exists()
                            else None
                        ),
                        "rules_hash": sha256_file(node / "target_entry_no_timeouts" / "target_entry_classes.json"),
                    }
                    for node in usable
                ],
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return manifest.get("inputs_hash") == inputs_hash


def ensure_template(output_root: Path, pair: str, session: str, template_root: Path) -> dict[str, Any]:
    if template_current(output_root, pair, session, template_root):
        return jload(template_dir(template_root, pair, session) / "session_template_report.json")
    return build_template(output_root, pair, session, template_root)


def classify_node(
    template_report: dict[str, Any] | None,
    calibration_report: dict[str, Any],
    potential_report: dict[str, Any],
) -> dict[str, Any]:
    if calibration_report.get("status") != "PASS" or potential_report.get("status") != "PASS":
        return {
            "node_class": "invalid",
            "failure_route": "invalid",
            "reason": "invalid_calibration_or_potential",
        }
    pair_summary = {row.get("dir"): row for row in calibration_report.get("pair_summary", [])}
    if any(float(row.get("entry_win_rate", 0.0)) < 0.505 for row in pair_summary.values()):
        return {
            "node_class": "heavy_delta",
            "failure_route": "quality_repair",
            "reason": "below_break_even",
        }
    if not template_report or template_report.get("status") != "PASS":
        return {
            "node_class": "heavy_delta",
            "failure_route": "supply_expand",
            "reason": "missing_template",
        }

    pair_template = template_report.get("pair_template", {})
    pair_rollup = potential_report.get("pair_rollup", {})
    gaps = []
    routes = []
    for direction, prefix in [("LONG", "long"), ("SHORT", "short")]:
        tpl = pair_template.get(direction, {})
        actual_tph = float(pair_rollup.get(f"actual_{prefix}_trades_per_hour", 0.0))
        actual_util = float(pair_rollup.get(f"{prefix}_utilization_ratio", 0.0))
        actual_rec = float(pair_rollup.get(f"{prefix}_recycling_utilization_ratio", 0.0))
        tpl_tph = max(float(tpl.get("template_trades_per_hour", 0.0)), 0.05)
        tpl_util = max(float(tpl.get("template_utilization_ratio", 0.0)), 0.05)
        tpl_rec = max(float(tpl.get("template_recycling_utilization_ratio", 0.0)), 0.05)
        tph_gap = abs(actual_tph - tpl_tph) / tpl_tph
        util_gap = abs(actual_util - tpl_util) / tpl_util
        rec_gap = abs(actual_rec - tpl_rec) / tpl_rec
        gaps.append(max(tph_gap, util_gap, rec_gap))
        if actual_util < tpl_util * 0.7 and float(pair_summary.get(direction, {}).get("entry_win_rate", 0.0)) >= 0.55:
            routes.append("supply_expand")
        elif actual_rec < tpl_rec * 0.7:
            routes.append("recycling_repair")

    repair_count = int(calibration_report.get("action_counts", {}).get("repair", 0))
    refine_count = int(calibration_report.get("action_counts", {}).get("refine", 0))
    max_gap = max(gaps) if gaps else 1.0
    if repair_count >= 6:
        return {
            "node_class": "heavy_delta",
            "failure_route": "quality_repair",
            "reason": "too_many_repair_zones",
            "max_gap": max_gap,
        }
    if max_gap <= 0.35 and repair_count <= 2:
        return {
            "node_class": "accept",
            "failure_route": "none",
            "reason": "template_conforming",
            "max_gap": max_gap,
        }
    if max_gap <= 0.85:
        return {
            "node_class": "light_delta",
            "failure_route": routes[0] if routes else ("quarter_rebalance" if refine_count > 0 else "supply_expand"),
            "reason": "moderate_template_delta",
            "max_gap": max_gap,
        }
    return {
        "node_class": "heavy_delta",
        "failure_route": routes[0] if routes else "quarter_rebalance",
        "reason": "large_template_delta",
        "max_gap": max_gap,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--session", required=True)
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument("--template-root", type=Path, default=DEFAULT_TEMPLATE_ROOT)
    args = ap.parse_args()
    report = ensure_template(args.output_root, args.pair, args.session, args.template_root)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
