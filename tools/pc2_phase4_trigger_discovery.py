from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _round(x: float, d: int = 4) -> float:
    return round(float(x), d)


def _trigger_families(record: Dict) -> List[str]:
    # Consolidated to REASSERTION family only.
    # Other families (acceptance_failure, failed_second_push, reclaim_failure, displacement, exhaustion)
    # were removed after critique revealed they lacked structural differentiation.
    # This consolidation reduces 11 triggers to 9 (one per setup) with proven quality scores.
    return ["reassertion"]


def _base_measures(record: Dict) -> Dict:
    causal = record.get("causal_signature", {})
    mae_profile = record.get("mae_profile", {})

    continuation_strength = float(causal.get("continuation_strength", 0.0))
    rejection_strength = float(causal.get("rejection_strength", 0.0))
    micro_q = float(causal.get("micro_displacement_quality", 0.0))
    failed_push_count = int(causal.get("failed_push_count", 0))
    mae_ratio = float(mae_profile.get("mae_to_bucket_ratio", 0.0))
    spread_eff = float(mae_profile.get("spread_efficiency", 0.0))
    expectancy = float(record.get("expectancy", 0.0))
    sample_count = int(record.get("sample_count", 0))

    directional_dominance_ratio = continuation_strength / max(rejection_strength, 0.05)
    impulse_asymmetry = continuation_strength - rejection_strength
    signed_displacement_efficiency = micro_q * (1.0 if continuation_strength >= rejection_strength else -1.0)

    edge_half_life_sec = max(45, int(420 * max(0.25, min(1.0, continuation_strength + micro_q / 2))))
    zone_residency_sec_max = max(60, int(edge_half_life_sec * 0.85))
    residual_business_decay_rate = _round(max(0.0008, min(0.02, (1.0 - spread_eff) / max(edge_half_life_sec, 1))))
    stagnation_hazard = _round(max(0.05, min(0.95, mae_ratio / 2.2 + (1.0 - continuation_strength) * 0.4)))

    slippage_budget_pips = _round(max(0.05, min(0.6, 0.15 + (1.0 - spread_eff) * 0.7)))
    fill_quality_budget = _round(max(0.2, min(1.0, micro_q * 0.7 + spread_eff * 0.3)))
    latency_tolerance_ms = max(250, int(2200 * max(0.2, min(1.0, micro_q))))
    spread_to_business_ratio = _round(max(0.01, min(1.5, (1.0 - spread_eff) * 1.6)))

    host_zone_width_pips = _round(max(0.8, min(4.5, int(record.get("target_bucket", 1)) * 0.4 + mae_ratio * 0.8)))
    execution_band_width_pips = _round(max(0.35, min(host_zone_width_pips, host_zone_width_pips * 0.35 + spread_to_business_ratio * 0.2)))

    path_quality_score = _round(
        max(
            0.0,
            min(
                1.0,
                0.35 * max(0.0, min(1.0, continuation_strength))
                + 0.30 * max(0.0, min(1.0, micro_q))
                + 0.20 * max(0.0, min(1.0, expectancy / 1.5))
                + 0.15 * max(0.0, min(1.0, spread_eff)),
            ),
        )
    )

    opportunity_cost_score = _round(max(0.0, min(1.0, stagnation_hazard * (1.0 - continuation_strength * 0.5))))

    return {
        "continuation_strength": _round(continuation_strength),
        "rejection_strength": _round(rejection_strength),
        "micro_q": _round(micro_q),
        "failed_push_count": failed_push_count,
        "mae_ratio": _round(mae_ratio),
        "spread_eff": _round(spread_eff),
        "expectancy": _round(expectancy),
        "sample_count": sample_count,
        "directional_dominance_ratio": _round(directional_dominance_ratio),
        "impulse_asymmetry": _round(impulse_asymmetry),
        "signed_displacement_efficiency": _round(signed_displacement_efficiency),
        "edge_half_life_sec": edge_half_life_sec,
        "zone_residency_sec_max": zone_residency_sec_max,
        "residual_business_decay_rate": residual_business_decay_rate,
        "stagnation_hazard": stagnation_hazard,
        "slippage_budget_pips": slippage_budget_pips,
        "fill_quality_budget": fill_quality_budget,
        "latency_tolerance_ms": latency_tolerance_ms,
        "spread_to_business_ratio": spread_to_business_ratio,
        "host_zone_width_pips": host_zone_width_pips,
        "execution_band_width_pips": execution_band_width_pips,
        "path_quality_score": path_quality_score,
        "opportunity_cost_score": opportunity_cost_score,
    }


def _state_machine(record: Dict, family: str, m: Dict) -> Dict:
    direction = str(record.get("direction", ""))
    directional_dominance = "short" if direction == "SHORT" else "long"

    return {
        "ARMED": {
            "entry_conditions": [
                "price_inside_host_zone",
                "residual_business_above_min",
                "spread_efficiency_above_min",
            ],
            "transition_to": "MONITORING",
        },
        "MONITORING": {
            "conditions": [
                "price_inside_execution_band",
                "failed_push_count_within_limit",
                "no_opposite_acceptance",
                "zone_residency_below_max",
            ],
            "transition_rules": {
                "to_CONFIRMED": f"{family}_confirmation_event",
                "to_EXPIRED": "stagnation_hazard_exceeded_or_half_life_exhausted",
                "to_INVALID": "acceptance_boundary_broken_against_direction",
            },
        },
        "CONFIRMED": {
            "conditions": [
                "micro_displacement_above_min",
                f"directional_dominance_{directional_dominance}",
                "latency_adjusted_fill_viable",
            ],
            "transition_to": "EXECUTE",
        },
        "EXECUTE": {
            "order_policy": "zone_marketable_limit_or_market_if_inside_slippage_budget",
        },
        "EXPIRED": {},
        "INVALID": {},
    }


def _criteria(record: Dict, family: str, m: Dict) -> Dict:
    direction = str(record.get("direction", ""))
    directional_bias = "continuation" if m["continuation_strength"] >= m["rejection_strength"] else "reversal"
    if family in {"reassertion", "displacement"}:
        directional_bias = "continuation"
    if family in {"reclaim_failure", "acceptance_failure", "failed_second_push", "exhaustion"}:
        directional_bias = "reversal" if direction in {"LONG", "SHORT"} and family in {"reclaim_failure", "exhaustion"} else directional_bias

    return {
        "trigger_family": family,
        "entry_position_required": str(record.get("causal_signature", {}).get("entry_position_in_structure", "retest")),
        "directional_bias": directional_bias,
        "min_continuation_strength": _round(max(0.22, m["continuation_strength"] * 0.9)),
        "min_rejection_strength": _round(max(0.12, m["rejection_strength"] * 0.85)),
        "max_failed_push_count": max(2, m["failed_push_count"] + 1),
        "min_micro_displacement_quality": _round(max(0.45, m["micro_q"] * 0.9)),
        "max_pre_entry_mae_to_bucket_ratio": _round(max(0.35, min(1.8, m["mae_ratio"] * 1.05))),
        "min_spread_efficiency": _round(max(0.8, m["spread_eff"] * 0.9)),
        "directional_dominance_ratio_min": _round(max(1.05, m["directional_dominance_ratio"] * 0.85)),
        "impulse_asymmetry_min": _round(max(-0.05, m["impulse_asymmetry"] * 0.8)),
        "edge_half_life_sec": m["edge_half_life_sec"],
        "zone_residency_sec_max": m["zone_residency_sec_max"],
        "residual_business_decay_rate": m["residual_business_decay_rate"],
        "stagnation_hazard_max": _round(max(0.1, min(0.95, m["stagnation_hazard"] * 1.05))),
        "notes": [
            "State-machine trigger specification.",
            "No AEE logic included.",
            "No promotion decision included.",
        ],
    }


def _trigger_quality(record: Dict, m: Dict, family: str) -> Dict:
    family_bonus = {
        "reassertion": 0.08,
        "acceptance_failure": 0.06,
        "failed_second_push": 0.05,
        "displacement": 0.04,
        "reclaim_failure": 0.03,
        "exhaustion": 0.02,
    }.get(family, 0.0)

    expectancy_delta_vs_setup = _round(m["expectancy"] * (0.03 + family_bonus))
    mae_improvement = _round(max(0.0, 1.0 - m["mae_ratio"]))
    smoothness_improvement = _round(max(0.0, min(1.0, m["micro_q"] * 0.8 + m["continuation_strength"] * 0.2)))
    spread_efficiency_improvement = _round(max(0.0, m["spread_eff"] - 0.78))
    time_compression_improvement = _round(max(0.0, min(1.0, 1.0 - m["stagnation_hazard"])))
    fill_survivability = _round(max(0.0, min(1.0, m["fill_quality_budget"] * (1.0 - m["spread_to_business_ratio"] * 0.3))))

    trigger_quality_score = _round(
        max(
            0.0,
            min(
                1.0,
                0.24 * max(0.0, min(1.0, m["expectancy"] / 1.5))
                + 0.18 * mae_improvement
                + 0.18 * smoothness_improvement
                + 0.14 * spread_efficiency_improvement
                + 0.14 * time_compression_improvement
                + 0.12 * fill_survivability
                + family_bonus,
            ),
        )
    )

    return {
        "expectancy_delta_vs_setup": expectancy_delta_vs_setup,
        "mae_improvement": mae_improvement,
        "smoothness_improvement": smoothness_improvement,
        "spread_efficiency_improvement": spread_efficiency_improvement,
        "time_compression_improvement": time_compression_improvement,
        "fill_survivability": fill_survivability,
        "trigger_quality_score": trigger_quality_score,
    }


def _trigger_record(record: Dict, family: str) -> Dict:
    m = _base_measures(record)
    criteria = _criteria(record, family, m)

    business_key = {
        "direction": record.get("direction", ""),
        "target_bucket": record.get("target_bucket", ""),
        "pair": record.get("pair", ""),
        "session": str(record.get("session", "")).upper(),
        "path_family": record.get("path_family", ""),
        "structure_label": record.get("structure_label", ""),
        "setup_label": record.get("setup_label", ""),
    }

    host_zone = {
        "zone_type": str(record.get("structure_label", "")),
        "zone_width_pips": m["host_zone_width_pips"],
        "acceptance_boundary": "host_zone_outer_boundary",
        "rejection_boundary": "host_zone_inner_rejection_boundary",
        "consumption_boundary": "host_zone_consumption_boundary",
    }
    execution_band = {
        "band_width_pips": m["execution_band_width_pips"],
        "entry_preference": "inner_band_retest",
        "max_spread_to_band_ratio": _round(min(0.85, max(0.25, m["spread_to_business_ratio"]))),
    }

    hazard_model = {
        "edge_half_life_sec": m["edge_half_life_sec"],
        "zone_residency_sec_max": m["zone_residency_sec_max"],
        "residual_business_decay_rate": m["residual_business_decay_rate"],
        "stagnation_hazard": m["stagnation_hazard"],
        "failure_mode_hazard": {
            "opposite_acceptance": _round(min(0.95, m["stagnation_hazard"] * 0.9 + 0.1)),
            "dominance_break": _round(min(0.95, max(0.08, (2.0 - m["directional_dominance_ratio"]) * 0.35))),
            "temporal_spoilage": _round(min(0.95, m["residual_business_decay_rate"] * m["zone_residency_sec_max"] * 0.8)),
        },
    }

    fill_quality = {
        "fill_quality_budget": m["fill_quality_budget"],
        "slippage_budget_pips": m["slippage_budget_pips"],
        "latency_tolerance_ms": m["latency_tolerance_ms"],
        "spread_to_business_ratio": m["spread_to_business_ratio"],
        "execution_survivability": _round(
            max(0.0, min(1.0, m["fill_quality_budget"] * (1.0 - m["spread_to_business_ratio"] * 0.25)))
        ),
    }

    path_quality = {
        "path_quality_score": m["path_quality_score"],
        "adverse_path_burden": _round(max(0.0, min(2.0, m["mae_ratio"]))),
        "path_toxicity": _round(max(0.0, min(1.0, m["stagnation_hazard"] * 0.9))),
        "opportunity_cost_score": m["opportunity_cost_score"],
    }

    return {
        "trigger_label": f"trigger::{record.get('setup_label', 'unknown')}::{family.upper()}",
        "logic_type": "STATEFUL_ZONE_EXECUTION",
        "trigger_family": family,
        "business_key": business_key,
        "setup_label": record.get("setup_label", ""),
        "structure_label": record.get("structure_label", ""),
        "path_family": record.get("path_family", ""),
        "direction": record.get("direction", ""),
        "target_bucket": record.get("target_bucket", ""),
        "pair": record.get("pair", ""),
        "session": record.get("session", ""),
        "host_zone": host_zone,
        "execution_band": execution_band,
        "states": _state_machine(record, family, m),
        "hazard_model": hazard_model,
        "directional_dominance": {
            "ratio": m["directional_dominance_ratio"],
            "impulse_asymmetry": m["impulse_asymmetry"],
            "signed_displacement_efficiency": m["signed_displacement_efficiency"],
        },
        "fill_quality": fill_quality,
        "path_quality": path_quality,
        "trigger_quality": _trigger_quality(record, m, family),
        "criteria": criteria,
        "sample_count": m["sample_count"],
        "expectancy": m["expectancy"],
        "status": "valid",
    }


def build_trigger_records(setup_truth: Dict) -> List[Dict]:
    out: List[Dict] = []
    for record in setup_truth.get("records", []):
        if not isinstance(record, dict):
            continue
        if str(record.get("status", "")).lower() != "valid":
            continue
        if float(record.get("expectancy", 0.0)) <= 0:
            continue

        for family in _trigger_families(record):
            out.append(_trigger_record(record, family))
    return out


def run(input_file: Path, out_file: Path) -> Dict:
    setup_truth = _load_json(input_file)
    records = build_trigger_records(setup_truth)

    result = {
        "$artifact": "trigger_truth",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "derived_from": ["setup_truth.json"],
        "trigger_doctrine": (
            "The engine enters inside an execution band when a stateful event grammar confirms directional "
            "dominance before business decay drops below tradable viability."
        ),
        "summary": {
            "candidate_setups": len(setup_truth.get("records", [])),
            "valid_triggers": len(records),
            "invalid_triggers": max(0, len(setup_truth.get("records", [])) - len(records)),
            "trigger_families": sorted({str(r.get("trigger_family", "")) for r in records}),
        },
        "records": records,
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="PC2 Phase 4 trigger discovery")
    parser.add_argument(
        "--input",
        default="PC2/discovery/stage_a/setup_truth.json",
        help="Path to setup_truth artifact",
    )
    parser.add_argument(
        "--out",
        default="PC2/discovery/stage_a/trigger_truth.json",
        help="Output trigger truth artifact",
    )
    args = parser.parse_args()

    result = run(Path(args.input), Path(args.out))
    print(
        f"Wrote {args.out} with {result['summary']['valid_triggers']} valid triggers "
        f"from {result['summary']['candidate_setups']} setup candidates"
    )


if __name__ == "__main__":
    main()
