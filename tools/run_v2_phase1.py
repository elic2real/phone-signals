from __future__ import annotations

import json
import random
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

try:
    from github_repo import tick_generator as synthetic_ticks  # noqa: E402
except ImportError:
    import tick_generator as synthetic_ticks  # type: ignore[no-redef]  # noqa: E402
from tools.v2_phase1_physics_engine import Phase1Config, run_phase1  # noqa: E402
from tools.v2_tier1_truth_kernel import build_truth_kernel  # noqa: E402


OUT_DIR = WORKSPACE / "control" / "v2_engine" / "phase1"
TIER1_DIR = WORKSPACE / "control" / "v2_engine" / "tier1"
DETERMINISM_LOCK = WORKSPACE / "control" / "v2_engine" / "v2_determinism_lock.json"
BOUNDARY_LOCK = WORKSPACE / "control" / "v2_engine" / "v2_boundary_lock.json"
PHASE_CONTRACTS = WORKSPACE / "control" / "v2_engine" / "v2_phase_contracts.json"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def make_config(lock: Dict[str, Any]) -> Phase1Config:
    cfg = lock["config_lock"]
    return Phase1Config(
        spread_anomaly_multiplier=float(cfg["spread_anomaly_multiplier"]),
        jump_sigma_multiplier=float(cfg["jump_sigma_multiplier"]),
        displacement_window=int(cfg["displacement_window"]),
        compression_short_window=int(cfg["compression_short_window"]),
        compression_long_window=int(cfg["compression_long_window"]),
        velocity_window=int(cfg["velocity_window"]),
        acceleration_window=int(cfg["acceleration_window"]),
        commission_pips=float(cfg["commission_pips"]),
        slippage_pips=float(cfg["slippage_pips"]),
        epsilon=float(cfg["epsilon"]),
    )


def scenario_ticks(name: str, seed: int) -> List[Dict[str, Any]]:
    scenario_name = str(name or "").strip()
    legacy_aliases = {
        "chop_mean_reversion": "low_energy_range",
    }
    scenario_name = legacy_aliases.get(scenario_name, scenario_name)
    random.seed(seed)
    generator = synthetic_ticks.SCENARIO_REGISTRY[scenario_name]
    return generator()


def sample_profiles(rows: List[Dict[str, Any]], stride: int, limit: int) -> List[Dict[str, Any]]:
    sampled: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        if index % max(stride, 1) == 0:
            sampled.append(row)
        if len(sampled) >= limit:
            break
    return sampled


def top_counts(values: Dict[str, int], limit: int = 10) -> Dict[str, int]:
    ranked = sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return {key: count for key, count in ranked[:limit]}


def _top_counter(counter: Dict[str, int], limit: int = 10) -> List[Dict[str, Any]]:
    total = sum(counter.values())
    ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [
        {
            "value": key,
            "count": count,
            "share": round(count / max(total, 1), 6),
        }
        for key, count in ranked[:limit]
    ]


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    TIER1_DIR.mkdir(parents=True, exist_ok=True)
    determinism = read_json(DETERMINISM_LOCK)
    phase_contracts = read_json(PHASE_CONTRACTS)
    boundary_lock = read_json(BOUNDARY_LOCK)
    config = make_config(determinism)

    scenario_names = determinism["sampling_policy"]["fixed_phase1_scenarios"]
    stride = int(determinism["sampling_policy"]["fixed_profile_stride"])
    profile_limit = int(determinism["sampling_policy"]["fixed_max_profiles_per_scenario"])
    base_seed = int(determinism["seed"])

    scenario_summary: List[Dict[str, Any]] = []
    sample_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_SAMPLE_PROFILES",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "profiles_by_scenario": {},
    }
    event_discovery_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_EVENT_DISCOVERY_ROWS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows_by_scenario": {},
    }
    market_mapping_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_MARKET_MAPPING_ROWS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows_by_scenario": {},
    }
    opportunity_fit_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_OPPORTUNITY_FIT_ROWS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows_by_scenario": {},
    }
    tier0_handoff_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_TIER0_HANDOFF_ROWS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows_by_scenario": {},
    }
    discovery_map_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_DISCOVERY_MAP",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "scenarios": {},
    }
    truth_kernel_payload: Dict[str, Any] = {
        "artifact_id": "V2_PHASE1_TRUTH_KERNEL_ROWS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "kernels_by_scenario": {},
    }

    compression_present = False
    expansion_present = False
    spread_stress_detected = False
    nan_guard_pass = True

    for index, name in enumerate(scenario_names):
        ticks = scenario_ticks(name, base_seed + index)
        result = run_phase1(ticks, config)
        summary = result["summary"]
        sanitizer = result["sanitizer"]
        scenario_summary.append(
            {
                "scenario": name,
                "seed": base_seed + index,
                "tick_input_count": sanitizer["input_count"],
                "tick_kept_count": sanitizer["kept_count"],
                "tick_dropped_count": sanitizer["dropped_count"],
                "drop_reasons": sanitizer["drop_reasons"],
                "profile_count": summary["profile_count"],
                "extractable_count": summary["extractable_count"],
                "conservative_opportunity_count": summary["conservative_opportunity_count"],
                "aggressive_path_opportunity_count": summary["aggressive_path_opportunity_count"],
                "pattern_qualified_opportunity_count": summary["pattern_qualified_opportunity_count"],
                "movement_detected_count": summary["movement_detected_count"],
                "cost_covering_count": summary["cost_covering_count"],
                "energy_states": summary["energy_states"],
                "zone_states": summary["zone_states"],
                "direction_groups": summary["direction_groups"],
                "target_distance_buckets": summary["target_distance_buckets"],
                "top_precursor_families": top_counts(summary["precursor_family_ids"], limit=8),
                "top_topology_families": top_counts(summary["topology_family_ids"], limit=8),
                "top_distance_families": top_counts(summary["distance_family_ids"], limit=8),
                "lifecycle_stages": summary["lifecycle_stages"],
                "opportunity_confidence_tiers": summary["opportunity_confidence_tiers"],
                "top_extraction_signatures": top_counts(summary["extraction_signatures"], limit=8),
                "compression_present": summary["compression_present"],
                "expansion_present": summary["expansion_present"],
            }
        )
        sampled_profiles = sample_profiles(result["profiles"], stride, profile_limit)
        kernel_rows = []
        annotated_profiles = []
        kernel_signature_counts: Dict[str, int] = {}
        handoff_by_profile_id = {
            str(row.get("profile_id", "") or ""): row
            for row in list(result["tier0"]["tier0_handoff_rows"])
        }
        for profile in sampled_profiles:
            kernel = build_truth_kernel(
                profile,
                result["profiles"],
                handoff_by_profile_id.get(str(profile.get("profile_id", "") or "")),
            )
            kernel_rows.append(kernel)
            signature = str(kernel["kernel_signature"])
            kernel_signature_counts[signature] = kernel_signature_counts.get(signature, 0) + 1
            annotated_profiles.append(
                {
                    **profile,
                    "truth_kernel_signature": signature,
                    "truth_kernel": kernel,
                }
            )

        sample_payload["profiles_by_scenario"][name] = annotated_profiles
        event_discovery_payload["rows_by_scenario"][name] = result["tier0"]["event_discovery_rows"]
        market_mapping_payload["rows_by_scenario"][name] = result["tier0"]["market_mapping_rows"]
        opportunity_fit_payload["rows_by_scenario"][name] = result["tier0"]["opportunity_fit_rows"]
        tier0_handoff_payload["rows_by_scenario"][name] = result["tier0"]["tier0_handoff_rows"]
        truth_kernel_payload["kernels_by_scenario"][name] = kernel_rows
        discovery_map_payload["scenarios"][name] = {
            "extractable_count": summary["extractable_count"],
            "conservative_opportunity_count": summary["conservative_opportunity_count"],
            "aggressive_path_opportunity_count": summary["aggressive_path_opportunity_count"],
            "pattern_qualified_opportunity_count": summary["pattern_qualified_opportunity_count"],
            "movement_detected_count": summary["movement_detected_count"],
            "cost_covering_count": summary["cost_covering_count"],
            "tier0_primary_counts": summary.get("tier0_primary_counts", {}),
            "direction_groups": summary["direction_groups"],
            "target_distance_buckets": summary["target_distance_buckets"],
            "legacy_interpretation_summary": summary.get("legacy_interpretation_summary", {}),
            "top_precursor_families": top_counts(summary["precursor_family_ids"], limit=12),
            "top_topology_families": top_counts(summary["topology_family_ids"], limit=12),
            "top_distance_families": top_counts(summary["distance_family_ids"], limit=12),
            "lifecycle_stages": summary["lifecycle_stages"],
            "data_confidence": summary["data_confidence"],
            "opportunity_confidence_tiers": summary["opportunity_confidence_tiers"],
            "top_extraction_signatures": top_counts(summary["extraction_signatures"], limit=12),
            "top_truth_kernel_signatures": _top_counter(kernel_signature_counts, limit=10),
        }

        compression_present = compression_present or bool(summary["compression_present"])
        expansion_present = expansion_present or bool(summary["expansion_present"])
        if "spread_anomaly" in sanitizer["drop_reasons"] or bool(sanitizer.get("spread_stress_detected")):
            spread_stress_detected = True
        for profile in result["profiles"]:
            for key, value in profile.items():
                if isinstance(value, float) and (value != value or value in {float("inf"), float("-inf")}):
                    nan_guard_pass = False
                    raise ValueError(f"Invalid numeric output in {name}:{key}")

    pass_gates = {
        "all_scenarios_produced_sanitized_output": all(row["tick_kept_count"] > 0 for row in scenario_summary),
        "no_unhandled_nan_or_inf": nan_guard_pass,
        "spread_anomaly_detected": spread_stress_detected,
        "compression_present": compression_present,
        "expansion_present": expansion_present,
    }

    execution_report = {
        "artifact_id": "V2_PHASE1_EXECUTION_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": "PASS" if all(pass_gates.values()) else "FAIL",
        "determinism_lock": str(DETERMINISM_LOCK),
        "boundary_lock": str(BOUNDARY_LOCK),
        "phase_contract": str(PHASE_CONTRACTS),
        "config": asdict(config),
        "phase_contract_required_outputs": phase_contracts["phases"][0]["required_outputs"],
        "pass_gates": pass_gates,
        "scenario_count": len(scenario_summary),
        "notes": [
            "Phase 1 remains physics-only and carries no pnl fields in profile payloads.",
            "T1 clustering remains unimplemented by design in this phase.",
            "Phase 1 now ends with direction-aware distance discovery, but no doctrine formalization is performed here.",
            "The distance buckets in Phase 1 are discovered movement classes, not strategy targets.",
            "extractable_count now represents raw Tier 0 discovered opportunities; pattern_qualified_opportunity_count tracks the subset that matches known recognition grammar.",
            "Tier 0 now also emits precursor families, topology families, distance families, lifecycle states, and grouped opportunity hierarchy fields for downstream doctrine building.",
            "Opportunity reporting is now split into conservative, aggressive-path, and pattern-qualified layers for sanity checking.",
            "Phase 1 sample payloads now also carry non-breaking Tier 1 truth-kernel annotations.",
            "Phase 1 now publishes explicit Tier 0 sublayer artifacts for event discovery, independent market mapping, opportunity fit, and handoff.",
            "Legacy interpretation fields are still emitted for migration compatibility, but discovery-map summaries now treat them as legacy side channels rather than primary Tier 0 outputs."
        ],
    }

    scenario_artifact = {
        "artifact_id": "V2_PHASE1_SCENARIO_SUMMARY",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "boundary_lock_ref": str(BOUNDARY_LOCK),
        "scenarios": scenario_summary,
    }

    (OUT_DIR / "v2_phase1_execution_report.json").write_text(json.dumps(execution_report, indent=2), encoding="utf-8")
    (OUT_DIR / "v2_phase1_scenario_summary.json").write_text(json.dumps(scenario_artifact, indent=2), encoding="utf-8")
    (OUT_DIR / "v2_phase1_sample_profiles.json").write_text(json.dumps(sample_payload, indent=2), encoding="utf-8")
    (OUT_DIR / "phase1_event_discovery_rows.json").write_text(json.dumps(event_discovery_payload, indent=2), encoding="utf-8")
    (OUT_DIR / "phase1_market_mapping_rows.json").write_text(json.dumps(market_mapping_payload, indent=2), encoding="utf-8")
    (OUT_DIR / "phase1_opportunity_fit_rows.json").write_text(json.dumps(opportunity_fit_payload, indent=2), encoding="utf-8")
    (OUT_DIR / "phase1_tier0_handoff_rows.json").write_text(json.dumps(tier0_handoff_payload, indent=2), encoding="utf-8")
    (OUT_DIR / "v2_phase1_discovery_map.json").write_text(json.dumps(discovery_map_payload, indent=2), encoding="utf-8")
    (TIER1_DIR / "v2_phase1_truth_kernel_rows.json").write_text(json.dumps(truth_kernel_payload, indent=2), encoding="utf-8")

    print(json.dumps({"status": execution_report["status"], "pass_gates": pass_gates}, indent=2))
    return 0 if execution_report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
