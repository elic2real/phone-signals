from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List


_BLITZ_CONFIG_CACHE: Dict[str, Any] | None = None
_TIER3_POCKET_COVERAGE_CACHE: Dict[str, Any] | None = None
_DOCTRINE_LAYER_CONTRACTS_CACHE: Dict[str, Any] | None = None
_PHASE2_OPERATING_MODEL_CACHE: Dict[str, Any] | None = None
_WORKSPACE = Path(__file__).resolve().parents[1]
_CONTROL_DIR = _WORKSPACE / "control" / "v2_engine"
_TIER3_DIR = _CONTROL_DIR / "tier3"
_PHASE2_DIR = _CONTROL_DIR / "phase2"


def _load_blitz_config() -> Dict[str, Any]:
    global _BLITZ_CONFIG_CACHE
    if _BLITZ_CONFIG_CACHE is not None:
        return _BLITZ_CONFIG_CACHE
    path = str(os.environ.get("V2_BLITZ_CONFIG", "") or "").strip()
    if not path:
        _BLITZ_CONFIG_CACHE = {}
        return _BLITZ_CONFIG_CACHE
    config_path = Path(path)
    if not config_path.exists():
        _BLITZ_CONFIG_CACHE = {}
        return _BLITZ_CONFIG_CACHE
    _BLITZ_CONFIG_CACHE = json.loads(config_path.read_text(encoding="utf-8"))
    return _BLITZ_CONFIG_CACHE


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_tier3_pocket_coverage() -> Dict[str, Any]:
    global _TIER3_POCKET_COVERAGE_CACHE
    if _TIER3_POCKET_COVERAGE_CACHE is None:
        _TIER3_POCKET_COVERAGE_CACHE = _read_json(_TIER3_DIR / "v2_tier3_pocket_coverage_report.json")
    return _TIER3_POCKET_COVERAGE_CACHE


def _load_doctrine_layer_contracts() -> Dict[str, Any]:
    global _DOCTRINE_LAYER_CONTRACTS_CACHE
    if _DOCTRINE_LAYER_CONTRACTS_CACHE is None:
        _DOCTRINE_LAYER_CONTRACTS_CACHE = _read_json(_TIER3_DIR / "v2_doctrine_layer_contracts.json")
    return _DOCTRINE_LAYER_CONTRACTS_CACHE


def _load_phase2_operating_model() -> Dict[str, Any]:
    global _PHASE2_OPERATING_MODEL_CACHE
    if _PHASE2_OPERATING_MODEL_CACHE is None:
        _PHASE2_OPERATING_MODEL_CACHE = _read_json(_PHASE2_DIR / "phase2_doctrine_operating_model.json")
    return _PHASE2_OPERATING_MODEL_CACHE


def _doctrine_operating_tier(doctrine_id: str) -> str:
    doctrine_id = str(doctrine_id or "").upper()
    rows = list(_load_phase2_operating_model().get("doctrines", []) or [])
    for row in rows:
        if str(row.get("doctrine_id", "") or "").upper() == doctrine_id:
            return str(row.get("doctrine_operating_tier", "") or "UNCLASSIFIED").upper()
    return "UNCLASSIFIED"


def _doctrine_window_diagnosis(doctrine_id: str) -> List[str]:
    doctrine_id = str(doctrine_id or "").upper()
    rows = list(_load_doctrine_layer_contracts().get("doctrines", []) or [])
    for row in rows:
        if str(row.get("doctrine_id", "") or "").upper() == doctrine_id:
            return [str(item or "").upper() for item in list(row.get("window_contract_diagnosis", []) or [])]
    return []


def _dynamic_profitable_pocket_plan() -> Dict[str, Dict[str, Any]]:
    report = _load_tier3_pocket_coverage()
    pocket_rows = list(report.get("pocket_rows", []) or [])
    strategy_summary = {
        str(row.get("strategy_id", "") or "").upper(): row
        for row in list(report.get("strategy_summary", []) or [])
    }
    dynamic: Dict[str, Dict[str, Any]] = {}
    for row in pocket_rows:
        doctrine_id = str(row.get("strategy_id", "") or "").upper()
        if not doctrine_id:
            continue
        summary = strategy_summary.get(doctrine_id, {})
        if float(row.get("net_pnl_pips", 0.0) or 0.0) <= 0.0:
            continue
        diagnosis = set(_doctrine_window_diagnosis(doctrine_id))
        operating_tier = _doctrine_operating_tier(doctrine_id)
        # Only convert the saved pocket evidence into a hard whitelist when the
        # doctrine is explicitly noisy/fragile enough to benefit from window isolation.
        if "WINDOW_EXISTS_BUT_IS_NOISY" not in diagnosis and operating_tier not in {"FRAGILE", "FROZEN"}:
            continue
        allowed = dynamic.setdefault(doctrine_id, {"allowed_pockets": [], "notes": []})
        allowed["allowed_pockets"].append(
            {
                "session_state": str(row.get("session_state", "UNKNOWN") or "UNKNOWN").upper(),
                "regime_state": str(row.get("regime_state", "UNKNOWN") or "UNKNOWN").upper(),
                "route_operating_mode": str(row.get("route_operating_mode", "UNKNOWN") or "UNKNOWN").upper(),
                "target_distance_bucket": str(row.get("target_distance_bucket", "UNKNOWN") or "UNKNOWN").upper(),
            }
        )
        if not allowed["notes"]:
            profitable_count = int(summary.get("profitable_pocket_count", 0) or 0)
            active_count = int(summary.get("active_pocket_count", 0) or 0)
            allowed["notes"].append(
                f"Dynamic whitelist recovered from tier3 profitable pocket coverage: {profitable_count}/{active_count} pockets positive."
            )
    return dynamic


def _apply_blitz_phase4_option_mutation(
    doctrine_id: str,
    route_operating_mode: str,
    option: Dict[str, Any],
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    mutation = (
        _load_blitz_config()
        .get("phase4_option_mutation", {})
        .get(doctrine_id, {})
        .get("route_modes", {})
        .get(route_operating_mode, {})
    )
    if not mutation:
        return option

    mutated = dict(option)
    ttl_scale = float(mutated.get("ttl_scale", 1.0) or 1.0)
    ttl_scale *= float(mutation.get("ttl_scale_multiplier", 1.0) or 1.0)
    ttl_scale += float(mutation.get("ttl_scale_delta", 0.0) or 0.0)
    mutated["ttl_scale"] = round(ttl_scale, 6)
    mutated["ttl_min"] = int(mutated.get("ttl_min", 3) or 3) + int(mutation.get("ttl_min_delta", 0) or 0)
    mutated["ttl_max"] = int(mutated.get("ttl_max", 99) or 99) + int(mutation.get("ttl_max_delta", 0) or 0)
    if mutated["ttl_max"] < mutated["ttl_min"]:
        mutated["ttl_max"] = mutated["ttl_min"]
    mutated["blitz_mutation"] = {
        "applied": True,
        "ttl_scale_multiplier": float(mutation.get("ttl_scale_multiplier", 1.0) or 1.0),
        "ttl_scale_delta": float(mutation.get("ttl_scale_delta", 0.0) or 0.0),
        "ttl_min_delta": int(mutation.get("ttl_min_delta", 0) or 0),
        "ttl_max_delta": int(mutation.get("ttl_max_delta", 0) or 0),
    }
    return mutated


def _variant_config(selected_route: Dict[str, Any] | None) -> Dict[str, Any]:
    variant = dict((selected_route or {}).get("selected_variant", {}) or {})
    return {
        "variant_id": str(variant.get("variant_id", "CAPTURE_FULL") or "CAPTURE_FULL"),
        "target_multiplier": float(variant.get("target_multiplier", 1.0) or 1.0),
        "stop_multiplier": float(variant.get("stop_multiplier", 1.0) or 1.0),
        "ttl_multiplier": float(variant.get("ttl_multiplier", 1.0) or 1.0),
        "entry_offset_ticks": int(variant.get("entry_offset_ticks", 0) or 0),
        "route_id": str((selected_route or {}).get("route_id", "") or ""),
        "segment_type": str((selected_route or {}).get("segment_type", "BASE") or "BASE"),
        "segment_value": str((selected_route or {}).get("segment_value", "ALL") or "ALL"),
        "route_operating_mode": str((selected_route or {}).get("route_operating_mode", "GENERAL_CAPTURE") or "GENERAL_CAPTURE"),
    }


def get_track2_pocket_whitelist_plan() -> Dict[str, Dict[str, Any]]:
    static_plan = {
        "COILED_COMPRESSION_SHORT": {
            "allowed_pockets": [
                {
                    "session_state": "STABLE",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "COIL_BREAK",
                    "target_distance_bucket": "MEDIUM",
                },
            ],
            "notes": [
                "Only the expansion coil-break medium pocket has positive evidence so far.",
            ],
        },
        "COMPRESSION_PRESSURE_DROP_SHORT": {
            "allowed_pockets": [
                {
                    "session_state": "STABLE",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "PRESSURE_DROP_CAPTURE",
                    "target_distance_bucket": "EXTENDED",
                },
            ],
            "notes": [
                "The short pressure-drop doctrine only earns its place in the stable expansion capture pocket.",
            ],
        },
        "FAILED_PUSH_LONG_REVERSAL_SCALP": {
            "allowed_pockets": [],
            "notes": [
                "No profitable pocket survived deterministic replay; this doctrine is effectively disabled by whitelist until new evidence appears.",
            ],
        },
        "FAILED_PUSH_SHORT_REVERSAL_SCALP": {
            "allowed_pockets": [
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "FAILED_PUSH_REJECTION",
                    "target_distance_bucket": "LARGE",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "FAILED_PUSH_REJECTION",
                    "target_distance_bucket": "LARGE",
                },
            ],
            "notes": [
                "Keep only the stable reversal pockets that remain positive after replay; injecting expansion rejection is currently contaminating the doctrine.",
            ],
        },
        "EXPANSION_RELEASE_LONG": {
            "allowed_pockets": [
                {
                    "session_state": "INJECTING",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "EXPANSION_RELEASE_LAUNCH",
                    "target_distance_bucket": "MICRO",
                },
                {
                    "session_state": "INJECTING",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "EXPANSION_RELEASE_LAUNCH",
                    "target_distance_bucket": "EXTENDED",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "EXPANSION_RELEASE_LAUNCH",
                    "target_distance_bucket": "EXTENDED",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "EXPANSION_RELEASE_LAUNCH",
                    "target_distance_bucket": "MEDIUM",
                },
            ],
            "notes": [
                "Keep expansion-release long on launch-only pockets for now; the hold window is currently the losing contamination path.",
            ],
        },
        "OSCILLATION_EDGE_LONG_SCALP": {
            "allowed_pockets": [
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "HARD_REBOUND",
                    "target_distance_bucket": "LARGE",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "SOFT_REBOUND",
                    "target_distance_bucket": "MEDIUM",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "HARD_REBOUND",
                    "target_distance_bucket": "MEDIUM",
                },
                {
                    "session_state": "BLEEDING",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "SOFT_REBOUND",
                    "target_distance_bucket": "LARGE",
                },
            ],
            "notes": [
                "Allow only the rebound pockets that have shown positive expectancy in replay.",
            ],
        },
        "OSCILLATION_PRESSURE_BUILD_SHORT": {
            "allowed_pockets": [
                {
                    "session_state": "STABLE",
                    "regime_state": "COMPRESSION",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "EXTENDED",
                },
                {
                    "session_state": "BLEEDING",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "MEDIUM",
                },
                {
                    "session_state": "BLEEDING",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "MEDIUM",
                },
                {
                    "session_state": "INJECTING",
                    "regime_state": "COMPRESSION",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "LARGE",
                },
                {
                    "session_state": "INJECTING",
                    "regime_state": "EXPANSION",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "LARGE",
                },
                {
                    "session_state": "STABLE",
                    "regime_state": "BALANCED",
                    "route_operating_mode": "PRESSURE_BUILD_CAPTURE",
                    "target_distance_bucket": "EXTENDED",
                },
            ],
            "notes": [
                "Only the positive pressure-build capture pockets remain eligible.",
            ],
        },
    }
    merged: Dict[str, Dict[str, Any]] = {
        doctrine_id: {
            "allowed_pockets": list(entry.get("allowed_pockets", []) or []),
            "notes": list(entry.get("notes", []) or []),
        }
        for doctrine_id, entry in static_plan.items()
    }
    for doctrine_id, entry in _dynamic_profitable_pocket_plan().items():
        target = merged.setdefault(doctrine_id, {"allowed_pockets": [], "notes": []})
        existing = {
            json.dumps(item, sort_keys=True)
            for item in list(target.get("allowed_pockets", []) or [])
        }
        for item in list(entry.get("allowed_pockets", []) or []):
            encoded = json.dumps(item, sort_keys=True)
            if encoded not in existing:
                target["allowed_pockets"].append(item)
                existing.add(encoded)
        for note in list(entry.get("notes", []) or []):
            if note not in target["notes"]:
                target["notes"].append(note)
    return merged


def _track2_pocket_key(
    *,
    session_state: str,
    regime_state: str,
    route_operating_mode: str,
    target_distance_bucket: str,
) -> str:
    return "|".join(
        [
            str(session_state or "UNKNOWN").upper(),
            str(regime_state or "UNKNOWN").upper(),
            str(route_operating_mode or "UNKNOWN").upper(),
            str(target_distance_bucket or "UNKNOWN").upper(),
        ]
    )


def _pocket_whitelist_status(
    doctrine_id: str,
    *,
    session_state: str,
    regime_state: str,
    route_operating_mode: str,
    target_distance_bucket: str,
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    plan = get_track2_pocket_whitelist_plan().get(doctrine_id)
    current_key = _track2_pocket_key(
        session_state=session_state,
        regime_state=regime_state,
        route_operating_mode=route_operating_mode,
        target_distance_bucket=target_distance_bucket,
    )
    if not plan:
        return {
            "is_whitelisted": True,
            "current_pocket_id": current_key,
            "allowed_pocket_ids": [],
            "whitelist_active": False,
        }
    allowed_keys = [
        _track2_pocket_key(
            session_state=str(item.get("session_state", "UNKNOWN") or "UNKNOWN"),
            regime_state=str(item.get("regime_state", "UNKNOWN") or "UNKNOWN"),
            route_operating_mode=str(item.get("route_operating_mode", "UNKNOWN") or "UNKNOWN"),
            target_distance_bucket=str(item.get("target_distance_bucket", "UNKNOWN") or "UNKNOWN"),
        )
        for item in list(plan.get("allowed_pockets", []) or [])
    ]
    return {
        "is_whitelisted": current_key in set(allowed_keys),
        "current_pocket_id": current_key,
        "allowed_pocket_ids": allowed_keys,
        "whitelist_active": True,
    }


def _expected_zone(profile: Dict[str, Any], selected_route: Dict[str, Any] | None = None) -> str:
    expression_id = str((selected_route or {}).get("expression_id", "") or profile.get("distance_family_id", "") or "").upper()
    if not expression_id:
        return "UNKNOWN"
    parts = expression_id.split("|")
    if len(parts) < 2:
        return "UNKNOWN"
    zone = str(parts[1] or "UNKNOWN").upper()
    if zone in {"NEAR_FLOOR", "NEAR_CEILING", "MID_ZONE"}:
        return zone
    return "UNKNOWN"


def _zone_ok(doctrine_id: str, direction: str, zone_state: str, expected_zone: str = "UNKNOWN") -> bool:
    expected_zone = str(expected_zone or "UNKNOWN").upper()
    if expected_zone in {"NEAR_FLOOR", "NEAR_CEILING", "MID_ZONE"}:
        return zone_state == expected_zone
    if "TRANSITION_RELEASE_LONG" in doctrine_id:
        return zone_state == "NEAR_CEILING"
    if "TRANSITION_RELEASE_SHORT" in doctrine_id:
        return zone_state == "NEAR_FLOOR"
    if "COMPRESSION_PRESSURE_LIFT_LONG" in doctrine_id:
        return zone_state == "NEAR_CEILING"
    if "COMPRESSION_PRESSURE_DROP_SHORT" in doctrine_id:
        return zone_state == "NEAR_FLOOR"
    if "OSCILLATION_EDGE_LONG" in doctrine_id:
        return zone_state == "NEAR_FLOOR"
    if "OSCILLATION_EDGE_SHORT" in doctrine_id:
        return zone_state == "NEAR_CEILING"
    if "OSCILLATION_PRESSURE_BUILD_LONG" in doctrine_id:
        return zone_state in {"NEAR_CEILING", "MID_ZONE"}
    if "OSCILLATION_PRESSURE_BUILD_SHORT" in doctrine_id:
        return zone_state in {"NEAR_FLOOR", "MID_ZONE"}
    if "FLOW_DRIFT" in doctrine_id or "FAILED_PUSH_" in doctrine_id or "PRESSURE_DRIVE" in doctrine_id:
        return zone_state == "MID_ZONE"
    return zone_state != ("NEAR_CEILING" if direction == "LONG" else "NEAR_FLOOR")


def _profile_quality_score(profile: Dict[str, Any]) -> float:
    friction = max(float(profile.get("friction_threshold_pips", 0.0) or 0.0), 0.05)
    path_budget = float(profile.get("path_discovery_pips", 0.0) or 0.0)
    boundary_width = float(profile.get("boundary_width_pips", 0.0) or 0.0)
    discovered_distance = float(profile.get("discovered_distance_pips", 0.0) or 0.0)
    energy = str(profile.get("energy_state", "") or "").upper()
    lifecycle = str(profile.get("lifecycle_stage", "") or "").upper()
    precursor = str(profile.get("precursor_state", "") or "").upper()

    score = 0.0
    score += min(2.4, path_budget / friction)
    score += min(1.6, discovered_distance / max(boundary_width, friction))
    if energy in {"IGNITION", "DRIVE"}:
        score += 0.8
    elif energy == "DRIFT":
        score += 0.35
    if lifecycle in {"RELEASE", "EXPLOIT"}:
        score += 0.6
    elif lifecycle in {"PATTERN_HARVEST", "LATE"}:
        score -= 0.45
    if precursor == "PRESSURED":
        score += 0.45
    elif precursor == "BALANCED":
        score += 0.2
    return round(score, 6)


def _make_adjustment_option(
    option_id: str,
    *,
    label: str,
    energy_floor_multiplier: float,
    confirmation_window_delta: int,
    confirmation_ratio: float,
    target_scale: float,
    stop_scale: float,
    ttl_scale: float,
    ttl_min: int,
    ttl_max: int,
) -> Dict[str, Any]:
    return {
        "option_id": option_id,
        "label": label,
        "energy_floor_multiplier": float(energy_floor_multiplier),
        "confirmation_window_delta": int(confirmation_window_delta),
        "confirmation_ratio": float(confirmation_ratio),
        "target_scale": float(target_scale),
        "stop_scale": float(stop_scale),
        "ttl_scale": float(ttl_scale),
        "ttl_min": int(ttl_min),
        "ttl_max": int(ttl_max),
    }


def get_doctrine_option_lock_plan() -> Dict[str, Dict[str, Any]]:
    return {
        "COILED_COMPRESSION_LONG": {
            "live": [
                {"route_operating_mode": "COIL_EXPAND", "option_id": "BALANCED"},
                {"route_operating_mode": "COIL_EXPAND", "option_id": "CONSERVATIVE"},
            ],
            "probationary": [
                {"route_operating_mode": "COIL_BREAK", "option_id": "CONSERVATIVE"},
            ],
            "notes": [
                "Keep the profitable coil-expand settings live and leave the smaller coil-break branch as probationary only.",
            ],
        },
        "TRANSITION_RELEASE_SHORT_STANDARD": {
            "live": [
                {"route_operating_mode": "IGNITION_RELEASE_FAST", "option_id": "FAST_BALANCED"},
                {"route_operating_mode": "RELEASE_CONFIRM", "option_id": "CONFIRM_BALANCED"},
            ],
            "probationary": [
                {"route_operating_mode": "RELEASE_EXTENSION", "option_id": "EXTENSION_BALANCED"},
            ],
            "notes": [
                "Keep the backbone doctrine centered on balanced ignition and confirm behaviors.",
                "Extension remains probationary until it proves economic value beyond readiness.",
            ],
        },
        "FLOW_DRIFT_SHORT": {
            "live": [
                {"route_operating_mode": "DRIFT_REACCEL", "option_id": "REACCEL_BALANCED"},
                {"route_operating_mode": "DRIFT_CONFIRM", "option_id": "CONFIRM_BALANCED"},
            ],
            "probationary": [
                {"route_operating_mode": "DRIFT_HARVEST", "option_id": "HARVEST_BALANCED"},
            ],
            "notes": [
                "Preserve the stronger expectancy branches while reintroducing one less-tight throughput branch.",
            ],
        },
        "FLOW_DRIFT_LONG": {
            "live": [
                {"route_operating_mode": "LONG_DRIFT_CONFIRM", "option_id": "LONG_CONFIRM_BALANCED"},
                {"route_operating_mode": "LONG_DRIFT_REACCEL", "option_id": "LONG_REACCEL_TIGHT"},
            ],
            "probationary": [
                {"route_operating_mode": "LONG_DRIFT_REACCEL", "option_id": "LONG_REACCEL_BALANCED"},
            ],
            "notes": [
                "Hold the current viable shape and test one softer reacceleration sibling only.",
            ],
        },
        "COMPRESSION_PRESSURE_LIFT_LONG": {
            "live": [
                {"route_operating_mode": "PRESSURE_EXTENSION_QUALIFIED", "option_id": "EXT_BALANCED"},
                {"route_operating_mode": "PRESSURE_LIFT_CAPTURE", "option_id": "CAPTURE_FAST"},
            ],
            "probationary": [
                {"route_operating_mode": "COMPRESSION_SWING", "option_id": "SWING_TIGHT"},
            ],
            "notes": [
                "Tighten the shortlist around the higher-readiness capture and extension branches.",
                "Keep one swing branch only as probation while damage is investigated.",
            ],
        },
        "OSCILLATION_EDGE_SHORT_SCALP": {
            "live": [
                {"route_operating_mode": "HARD_REJECTION", "option_id": "REJECT_FAST"},
            ],
            "probationary": [],
            "notes": [
                "Remove soft rejection behavior from default circulation until it proves it can fire cleanly.",
            ],
        },
        "OSCILLATION_PRESSURE_BUILD_LONG": {
            "live": [
                {"route_operating_mode": "PRESSURE_BUILD_CAPTURE", "option_id": "BUILD_BALANCED"},
            ],
            "probationary": [
                {"route_operating_mode": "PRESSURE_BUILD_SWING", "option_id": "SWING_STRICT"},
            ],
            "notes": [
                "Do not broaden this doctrine further. Leave only the cleanest capture branch live.",
            ],
        },
        "PRESSURE_DRIVE_LONG": {
            "live": [
                {"route_operating_mode": "PRESSURE_DRIVE_EDGE", "option_id": "CONSERVATIVE"},
                {"route_operating_mode": "PRESSURE_DRIVE_EDGE", "option_id": "BALANCED"},
            ],
            "probationary": [],
            "notes": [
                "Freeze the profitable edge-drive settings and keep hold or mid variants out until they show positive evidence.",
            ],
        },
    }


def _apply_doctrine_option_policy(doctrine_id: str, route_operating_mode: str, ladder: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    policy = get_doctrine_option_lock_plan().get(doctrine_id)
    if not policy:
        return [{**option, "option_status": "UNLOCKED"} for option in ladder]

    allowed: Dict[str, str] = {}
    for entry in policy.get("live", []):
        if str(entry.get("route_operating_mode", "") or "").upper() == route_operating_mode:
            allowed[str(entry.get("option_id", "") or "").upper()] = "LIVE"
    for entry in policy.get("probationary", []):
        if str(entry.get("route_operating_mode", "") or "").upper() == route_operating_mode:
            allowed[str(entry.get("option_id", "") or "").upper()] = "PROBATIONARY"

    filtered = []
    for option in ladder:
        option_id = str(option.get("option_id", "") or "").upper()
        if option_id in allowed:
            filtered.append({**option, "option_status": allowed[option_id]})
    return filtered


def _adjustment_option_ladder(doctrine_id: str, route_operating_mode: str) -> List[Dict[str, Any]]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()

    if doctrine_id == "EXPANSION_RELEASE_LONG":
        if route_operating_mode == "EXPANSION_RELEASE_LAUNCH":
            return [
                _make_adjustment_option("LAUNCH_TIGHT", label="Launch tight", energy_floor_multiplier=0.96, confirmation_window_delta=-1, confirmation_ratio=0.8, target_scale=0.94, stop_scale=0.92, ttl_scale=0.9, ttl_min=5, ttl_max=10),
                _make_adjustment_option("LAUNCH_BALANCED", label="Launch balanced", energy_floor_multiplier=0.9, confirmation_window_delta=-1, confirmation_ratio=0.76, target_scale=1.0, stop_scale=0.9, ttl_scale=0.96, ttl_min=5, ttl_max=12),
                _make_adjustment_option("LAUNCH_STRETCH", label="Launch stretch", energy_floor_multiplier=0.86, confirmation_window_delta=0, confirmation_ratio=0.72, target_scale=1.08, stop_scale=0.94, ttl_scale=1.04, ttl_min=6, ttl_max=13),
            ]
        if route_operating_mode == "EXPANSION_RELEASE_HOLD":
            return [
                _make_adjustment_option("HOLD_TIGHT", label="Hold tight", energy_floor_multiplier=1.0, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=0.92, stop_scale=0.94, ttl_scale=0.92, ttl_min=6, ttl_max=11),
                _make_adjustment_option("HOLD_BALANCED", label="Hold balanced", energy_floor_multiplier=0.96, confirmation_window_delta=0, confirmation_ratio=0.79, target_scale=0.98, stop_scale=0.96, ttl_scale=0.98, ttl_min=6, ttl_max=13),
                _make_adjustment_option("HOLD_PATIENT", label="Hold patient", energy_floor_multiplier=0.93, confirmation_window_delta=1, confirmation_ratio=0.76, target_scale=1.04, stop_scale=0.98, ttl_scale=1.04, ttl_min=7, ttl_max=14),
            ]
        return [
            _make_adjustment_option("BALANCED", label="Balanced", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=1.0, stop_scale=0.95, ttl_scale=1.0, ttl_min=6, ttl_max=12),
        ]

    if doctrine_id == "TRANSITION_RELEASE_SHORT_STANDARD":
        if route_operating_mode == "IGNITION_RELEASE_FAST":
            return [
                _make_adjustment_option("FAST_TIGHT", label="Fast tight", energy_floor_multiplier=0.98, confirmation_window_delta=-1, confirmation_ratio=0.82, target_scale=0.95, stop_scale=0.93, ttl_scale=0.9, ttl_min=5, ttl_max=10),
                _make_adjustment_option("FAST_BALANCED", label="Fast balanced", energy_floor_multiplier=0.93, confirmation_window_delta=-2, confirmation_ratio=0.76, target_scale=1.02, stop_scale=0.9, ttl_scale=0.96, ttl_min=5, ttl_max=12),
                _make_adjustment_option("FAST_RUNNER", label="Fast runner", energy_floor_multiplier=0.89, confirmation_window_delta=-1, confirmation_ratio=0.72, target_scale=1.12, stop_scale=0.96, ttl_scale=1.06, ttl_min=6, ttl_max=15),
            ]
        if route_operating_mode == "RELEASE_EXTENSION":
            return [
                _make_adjustment_option("EXTENSION_QUALIFIED", label="Qualified extension", energy_floor_multiplier=0.98, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=1.02, stop_scale=0.95, ttl_scale=1.02, ttl_min=7, ttl_max=14),
                _make_adjustment_option("EXTENSION_BALANCED", label="Balanced extension", energy_floor_multiplier=0.94, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.1, stop_scale=0.98, ttl_scale=1.08, ttl_min=8, ttl_max=16),
                _make_adjustment_option("EXTENSION_RUNNER", label="Runner extension", energy_floor_multiplier=0.9, confirmation_window_delta=1, confirmation_ratio=0.74, target_scale=1.18, stop_scale=1.02, ttl_scale=1.14, ttl_min=9, ttl_max=18),
            ]
        return [
            _make_adjustment_option("CONFIRM_TIGHT", label="Confirm tight", energy_floor_multiplier=1.0, confirmation_window_delta=0, confirmation_ratio=0.84, target_scale=0.94, stop_scale=0.94, ttl_scale=0.92, ttl_min=6, ttl_max=12),
            _make_adjustment_option("CONFIRM_BALANCED", label="Confirm balanced", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=1.0, stop_scale=0.92, ttl_scale=1.0, ttl_min=6, ttl_max=14),
            _make_adjustment_option("CONFIRM_PATIENT", label="Confirm patient", energy_floor_multiplier=0.92, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.06, stop_scale=0.95, ttl_scale=1.08, ttl_min=7, ttl_max=15),
        ]

    if doctrine_id == "FLOW_DRIFT_SHORT":
        if route_operating_mode == "DRIFT_REACCEL":
            return [
                _make_adjustment_option("REACCEL_TIGHT", label="Reaccel tight", energy_floor_multiplier=0.96, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=0.98, stop_scale=0.94, ttl_scale=1.045, ttl_min=6, ttl_max=12),
                _make_adjustment_option("REACCEL_BALANCED", label="Reaccel balanced", energy_floor_multiplier=0.92, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.05, stop_scale=0.95, ttl_scale=1.1, ttl_min=6, ttl_max=14),
                _make_adjustment_option("REACCEL_EXTEND", label="Reaccel extend", energy_floor_multiplier=0.88, confirmation_window_delta=1, confirmation_ratio=0.74, target_scale=1.12, stop_scale=0.98, ttl_scale=1.188, ttl_min=7, ttl_max=16),
            ]
        if route_operating_mode == "DRIFT_HARVEST":
            return [
                _make_adjustment_option("HARVEST_FAST", label="Harvest fast", energy_floor_multiplier=1.02, confirmation_window_delta=-1, confirmation_ratio=0.85, target_scale=0.9, stop_scale=0.9, ttl_scale=0.8925, ttl_min=4, ttl_max=9),
                _make_adjustment_option("HARVEST_BALANCED", label="Harvest balanced", energy_floor_multiplier=0.98, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=0.95, stop_scale=0.92, ttl_scale=0.945, ttl_min=4, ttl_max=10),
                _make_adjustment_option("HARVEST_HOLD", label="Harvest hold", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=1.0, stop_scale=0.94, ttl_scale=1.008, ttl_min=5, ttl_max=11),
            ]
        return [
            _make_adjustment_option("CONFIRM_HARVEST", label="Confirm harvest", energy_floor_multiplier=1.0, confirmation_window_delta=-1, confirmation_ratio=0.84, target_scale=0.92, stop_scale=0.92, ttl_scale=0.972, ttl_min=5, ttl_max=10),
            _make_adjustment_option("CONFIRM_BALANCED", label="Confirm balanced", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.98, stop_scale=0.94, ttl_scale=1.0368, ttl_min=5, ttl_max=12),
            _make_adjustment_option("CONFIRM_PATIENT", label="Confirm patient", energy_floor_multiplier=0.92, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.04, stop_scale=0.96, ttl_scale=1.1232, ttl_min=6, ttl_max=13),
        ]

    if doctrine_id == "FLOW_DRIFT_LONG":
        if route_operating_mode == "LONG_DRIFT_REACCEL":
            return [
                _make_adjustment_option("LONG_REACCEL_TIGHT", label="Long reaccel tight", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.98, stop_scale=0.94, ttl_scale=0.96, ttl_min=6, ttl_max=13),
                _make_adjustment_option("LONG_REACCEL_BALANCED", label="Long reaccel balanced", energy_floor_multiplier=0.9, confirmation_window_delta=1, confirmation_ratio=0.76, target_scale=1.08, stop_scale=0.95, ttl_scale=1.02, ttl_min=6, ttl_max=15),
                _make_adjustment_option("LONG_REACCEL_STRETCH", label="Long reaccel stretch", energy_floor_multiplier=0.86, confirmation_window_delta=1, confirmation_ratio=0.72, target_scale=1.16, stop_scale=0.98, ttl_scale=1.1, ttl_min=7, ttl_max=17),
            ]
        if route_operating_mode == "LONG_DRIFT_IGNITION":
            return [
                _make_adjustment_option("LONG_IGNITION_TIGHT", label="Long ignition tight", energy_floor_multiplier=0.93, confirmation_window_delta=-1, confirmation_ratio=0.78, target_scale=0.98, stop_scale=0.92, ttl_scale=0.94, ttl_min=5, ttl_max=11),
                _make_adjustment_option("LONG_IGNITION_BALANCED", label="Long ignition balanced", energy_floor_multiplier=0.88, confirmation_window_delta=-1, confirmation_ratio=0.74, target_scale=1.08, stop_scale=0.94, ttl_scale=1.0, ttl_min=5, ttl_max=13),
                _make_adjustment_option("LONG_IGNITION_STRETCH", label="Long ignition stretch", energy_floor_multiplier=0.84, confirmation_window_delta=0, confirmation_ratio=0.7, target_scale=1.16, stop_scale=0.98, ttl_scale=1.08, ttl_min=6, ttl_max=15),
            ]
        return [
            _make_adjustment_option("LONG_CONFIRM_TIGHT", label="Long confirm tight", energy_floor_multiplier=0.98, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=0.94, stop_scale=0.93, ttl_scale=0.92, ttl_min=5, ttl_max=11),
            _make_adjustment_option("LONG_CONFIRM_BALANCED", label="Long confirm balanced", energy_floor_multiplier=0.93, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.0, stop_scale=0.95, ttl_scale=0.98, ttl_min=6, ttl_max=13),
            _make_adjustment_option("LONG_CONFIRM_PATIENT", label="Long confirm patient", energy_floor_multiplier=0.9, confirmation_window_delta=1, confirmation_ratio=0.75, target_scale=1.06, stop_scale=0.97, ttl_scale=1.06, ttl_min=6, ttl_max=15),
        ]

    if doctrine_id == "COMPRESSION_PRESSURE_LIFT_LONG":
        if route_operating_mode == "COMPRESSION_SWING":
            return [
                _make_adjustment_option("SWING_TIGHT", label="Swing tight", energy_floor_multiplier=0.96, confirmation_window_delta=0, confirmation_ratio=0.82, target_scale=0.96, stop_scale=0.94, ttl_scale=0.96, ttl_min=6, ttl_max=13),
                _make_adjustment_option("SWING_BALANCED", label="Swing balanced", energy_floor_multiplier=0.92, confirmation_window_delta=1, confirmation_ratio=0.78, target_scale=1.04, stop_scale=0.96, ttl_scale=1.02, ttl_min=6, ttl_max=15),
                _make_adjustment_option("SWING_FULL", label="Swing full", energy_floor_multiplier=0.88, confirmation_window_delta=1, confirmation_ratio=0.74, target_scale=1.12, stop_scale=0.99, ttl_scale=1.1, ttl_min=7, ttl_max=17),
            ]
        if route_operating_mode == "PRESSURE_EXTENSION_QUALIFIED":
            return [
                _make_adjustment_option("EXT_STRICT", label="Extension strict", energy_floor_multiplier=1.0, confirmation_window_delta=0, confirmation_ratio=0.84, target_scale=0.96, stop_scale=0.95, ttl_scale=0.96, ttl_min=6, ttl_max=12),
                _make_adjustment_option("EXT_BALANCED", label="Extension balanced", energy_floor_multiplier=0.95, confirmation_window_delta=1, confirmation_ratio=0.8, target_scale=1.04, stop_scale=0.98, ttl_scale=1.02, ttl_min=7, ttl_max=14),
                _make_adjustment_option("EXT_RUNNER", label="Extension runner", energy_floor_multiplier=0.9, confirmation_window_delta=1, confirmation_ratio=0.76, target_scale=1.1, stop_scale=1.02, ttl_scale=1.08, ttl_min=8, ttl_max=16),
            ]
        return [
            _make_adjustment_option("CAPTURE_FAST", label="Capture fast", energy_floor_multiplier=0.98, confirmation_window_delta=-1, confirmation_ratio=0.84, target_scale=0.92, stop_scale=0.92, ttl_scale=0.9, ttl_min=5, ttl_max=11),
            _make_adjustment_option("CAPTURE_BALANCED", label="Capture balanced", energy_floor_multiplier=0.94, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=1.0, stop_scale=0.94, ttl_scale=0.96, ttl_min=5, ttl_max=13),
            _make_adjustment_option("CAPTURE_PRESS", label="Capture press", energy_floor_multiplier=0.9, confirmation_window_delta=0, confirmation_ratio=0.76, target_scale=1.06, stop_scale=0.97, ttl_scale=1.02, ttl_min=6, ttl_max=14),
        ]

    if doctrine_id == "OSCILLATION_EDGE_SHORT_SCALP":
        if route_operating_mode == "SOFT_REJECTION":
            return [
                _make_adjustment_option("SOFT_TIGHT", label="Soft tight", energy_floor_multiplier=1.02, confirmation_window_delta=-1, confirmation_ratio=0.86, target_scale=0.86, stop_scale=0.9, ttl_scale=0.78, ttl_min=3, ttl_max=6),
                _make_adjustment_option("SOFT_BALANCED", label="Soft balanced", energy_floor_multiplier=0.98, confirmation_window_delta=0, confirmation_ratio=0.83, target_scale=0.9, stop_scale=0.92, ttl_scale=0.84, ttl_min=3, ttl_max=7),
                _make_adjustment_option("SOFT_PATIENT", label="Soft patient", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.95, stop_scale=0.94, ttl_scale=0.9, ttl_min=4, ttl_max=8),
            ]
        return [
            _make_adjustment_option("REJECT_FAST", label="Reject fast", energy_floor_multiplier=0.96, confirmation_window_delta=-1, confirmation_ratio=0.83, target_scale=0.92, stop_scale=0.9, ttl_scale=0.82, ttl_min=3, ttl_max=7),
            _make_adjustment_option("REJECT_BALANCED", label="Reject balanced", energy_floor_multiplier=0.92, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.98, stop_scale=0.92, ttl_scale=0.88, ttl_min=3, ttl_max=8),
            _make_adjustment_option("REJECT_RELOAD", label="Reject reload", energy_floor_multiplier=0.9, confirmation_window_delta=0, confirmation_ratio=0.77, target_scale=1.04, stop_scale=0.95, ttl_scale=0.94, ttl_min=4, ttl_max=9),
        ]

    if doctrine_id == "OSCILLATION_EDGE_LONG_SCALP":
        if route_operating_mode == "SOFT_REBOUND":
            return [
                _make_adjustment_option("SOFT_TIGHT", label="Soft tight", energy_floor_multiplier=1.03, confirmation_window_delta=-1, confirmation_ratio=0.86, target_scale=0.84, stop_scale=0.9, ttl_scale=0.78, ttl_min=3, ttl_max=6),
                _make_adjustment_option("SOFT_BALANCED", label="Soft balanced", energy_floor_multiplier=0.99, confirmation_window_delta=0, confirmation_ratio=0.83, target_scale=0.9, stop_scale=0.92, ttl_scale=0.84, ttl_min=3, ttl_max=7),
                _make_adjustment_option("SOFT_PATIENT", label="Soft patient", energy_floor_multiplier=0.96, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.95, stop_scale=0.94, ttl_scale=0.9, ttl_min=4, ttl_max=8),
            ]
        return [
            _make_adjustment_option("REBOUND_TIGHT", label="Rebound tight", energy_floor_multiplier=0.96, confirmation_window_delta=-1, confirmation_ratio=0.83, target_scale=0.92, stop_scale=0.9, ttl_scale=0.84, ttl_min=3, ttl_max=7),
            _make_adjustment_option("REBOUND_BALANCED", label="Rebound balanced", energy_floor_multiplier=0.92, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.98, stop_scale=0.92, ttl_scale=0.9, ttl_min=3, ttl_max=8),
            _make_adjustment_option("REBOUND_PRESS", label="Rebound press", energy_floor_multiplier=0.9, confirmation_window_delta=0, confirmation_ratio=0.77, target_scale=1.04, stop_scale=0.95, ttl_scale=0.96, ttl_min=4, ttl_max=9),
        ]

    if doctrine_id == "OSCILLATION_PRESSURE_BUILD_LONG":
        if route_operating_mode == "PRESSURE_BUILD_SWING":
            return [
                _make_adjustment_option("SWING_STRICT", label="Swing strict", energy_floor_multiplier=1.02, confirmation_window_delta=0, confirmation_ratio=0.85, target_scale=0.92, stop_scale=0.96, ttl_scale=0.92, ttl_min=6, ttl_max=11),
                _make_adjustment_option("SWING_BALANCED", label="Swing balanced", energy_floor_multiplier=0.97, confirmation_window_delta=1, confirmation_ratio=0.81, target_scale=1.0, stop_scale=0.99, ttl_scale=0.98, ttl_min=6, ttl_max=13),
                _make_adjustment_option("SWING_STRETCH", label="Swing stretch", energy_floor_multiplier=0.93, confirmation_window_delta=1, confirmation_ratio=0.77, target_scale=1.06, stop_scale=1.02, ttl_scale=1.04, ttl_min=7, ttl_max=15),
            ]
        return [
            _make_adjustment_option("BUILD_FAST", label="Build fast", energy_floor_multiplier=0.99, confirmation_window_delta=-1, confirmation_ratio=0.84, target_scale=0.9, stop_scale=0.92, ttl_scale=0.88, ttl_min=5, ttl_max=10),
            _make_adjustment_option("BUILD_BALANCED", label="Build balanced", energy_floor_multiplier=0.95, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=0.98, stop_scale=0.95, ttl_scale=0.94, ttl_min=5, ttl_max=12),
            _make_adjustment_option("BUILD_PRESS", label="Build press", energy_floor_multiplier=0.91, confirmation_window_delta=0, confirmation_ratio=0.76, target_scale=1.04, stop_scale=0.98, ttl_scale=1.0, ttl_min=6, ttl_max=13),
        ]

    return [
        _make_adjustment_option("CONSERVATIVE", label="Conservative", energy_floor_multiplier=1.0, confirmation_window_delta=0, confirmation_ratio=0.84, target_scale=0.96, stop_scale=0.95, ttl_scale=0.94, ttl_min=5, ttl_max=12),
        _make_adjustment_option("BALANCED", label="Balanced", energy_floor_multiplier=0.96, confirmation_window_delta=0, confirmation_ratio=0.8, target_scale=1.0, stop_scale=0.97, ttl_scale=1.0, ttl_min=5, ttl_max=14),
        _make_adjustment_option("AGGRESSIVE", label="Aggressive", energy_floor_multiplier=0.92, confirmation_window_delta=1, confirmation_ratio=0.76, target_scale=1.06, stop_scale=1.0, ttl_scale=1.06, ttl_min=6, ttl_max=16),
    ]


def _select_adjustment_option(
    doctrine_id: str,
    route_operating_mode: str,
    profile: Dict[str, Any],
    ladder: List[Dict[str, Any]],
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    energy = str(profile.get("energy_state", "") or "").upper()
    lifecycle = str(profile.get("lifecycle_stage", "") or "").upper()
    precursor = str(profile.get("precursor_state", "") or "").upper()
    quality = _profile_quality_score(profile)

    aggressive = False
    conservative = False

    if energy == "DORMANT" or lifecycle in {"PATTERN_HARVEST", "LATE"}:
        conservative = True
    if route_operating_mode in {"IGNITION_RELEASE_FAST", "LONG_DRIFT_IGNITION"} and energy == "IGNITION" and quality >= 2.4:
        aggressive = True
    if route_operating_mode in {"DRIFT_REACCEL", "LONG_DRIFT_REACCEL"} and precursor == "PRESSURED" and quality >= 2.3:
        aggressive = True
    if doctrine_id in {"OSCILLATION_EDGE_LONG_SCALP", "OSCILLATION_EDGE_SHORT_SCALP", "OSCILLATION_PRESSURE_BUILD_LONG"} and quality < 2.35:
        conservative = True
    if route_operating_mode in {"PRESSURE_EXTENSION_QUALIFIED", "PRESSURE_BUILD_SWING"} and quality < 2.65:
        conservative = True

    if conservative and not aggressive:
        return ladder[0]
    if aggressive and not conservative:
        return ladder[-1]
    return ladder[min(1, len(ladder) - 1)]


def _energy_floor(doctrine_id: str, route_operating_mode: str, base_floor: float, option: Dict[str, Any]) -> float:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    if "TRANSITION_RELEASE" in doctrine_id:
        multiplier = 0.9
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id or "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        multiplier = 0.72
    elif "COILED_COMPRESSION" in doctrine_id:
        multiplier = 0.84
    elif "EXPANSION_RELEASE" in doctrine_id:
        multiplier = 0.84
    elif "FLOW_DRIFT" in doctrine_id:
        multiplier = 0.78
    elif "FAILED_PUSH_" in doctrine_id:
        multiplier = 0.75
    elif "OSCILLATION_EDGE" in doctrine_id:
        multiplier = 0.7
    elif "OSCILLATION_PRESSURE_BUILD" in doctrine_id:
        multiplier = 0.76
    elif "PRESSURE_DRIVE" in doctrine_id:
        multiplier = 0.84
    else:
        multiplier = 1.0

    if route_operating_mode in {"IGNITION_RELEASE_FAST", "LONG_DRIFT_IGNITION"}:
        multiplier *= 0.96
    elif route_operating_mode in {"DRIFT_REACCEL", "LONG_DRIFT_REACCEL"}:
        multiplier *= 0.97
    elif route_operating_mode in {"DRIFT_CONFIRM", "LONG_DRIFT_CONFIRM"}:
        multiplier *= 0.98
    elif route_operating_mode in {"DRIFT_HARVEST", "SOFT_REBOUND", "SOFT_REJECTION"}:
        multiplier *= 1.04
    elif route_operating_mode in {"PRESSURE_EXTENSION_QUALIFIED", "PRESSURE_BUILD_SWING"}:
        multiplier *= 1.03
    elif route_operating_mode in {"PRESSURE_LIFT_CAPTURE", "RELEASE_CONFIRM"}:
        multiplier *= 0.98
    elif route_operating_mode == "EXPANSION_RELEASE_LAUNCH":
        multiplier *= 0.93
    elif route_operating_mode == "EXPANSION_RELEASE_HOLD":
        multiplier *= 0.99
    elif route_operating_mode in {"COIL_BREAK", "COIL_EXPAND"}:
        multiplier *= 0.96 if route_operating_mode == "COIL_BREAK" else 0.98
    elif route_operating_mode in {"PRESSURE_DRIVE_EDGE", "PRESSURE_DRIVE_MID"}:
        multiplier *= 0.96
    elif route_operating_mode == "PRESSURE_DRIVE_HOLD":
        multiplier *= 0.92

    diagnosis = set(_doctrine_window_diagnosis(doctrine_id))
    if "T6_CHARGE_TOO_STRICT" in diagnosis:
        multiplier *= 0.94
    if "T6_DECAY_TOO_STRICT" in diagnosis and route_operating_mode in {"HARD_REJECTION", "HARD_REBOUND", "RELEASE_CONFIRM"}:
        multiplier *= 0.98

    return base_floor * multiplier * float(option.get("energy_floor_multiplier", 1.0) or 1.0)


def _supportive_htf_zone(direction: str, htf_zone_kernel: str) -> bool:
    direction = str(direction or "UNKNOWN").upper()
    htf_zone_kernel = str(htf_zone_kernel or "UNSPECIFIED").upper()
    return (
        (direction == "LONG" and htf_zone_kernel == "HTF_SUPPORT")
        or (direction == "SHORT" and htf_zone_kernel == "HTF_RESISTANCE")
    )


def _charge_truth_multiplier(
    *,
    doctrine_id: str,
    route_operating_mode: str,
    direction: str,
    profile: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    direction = str(direction or "UNKNOWN").upper()
    macro_bias_alignment = str(context.get("macro_bias_alignment", "NEUTRAL") or "NEUTRAL").upper()
    htf_zone_kernel = str(context.get("htf_zone_kernel", "UNSPECIFIED") or "UNSPECIFIED").upper()
    liquidity_map_kernel = str(context.get("liquidity_map_kernel", "UNSPECIFIED") or "UNSPECIFIED").upper()
    session_state = str(context.get("session_state", "STABLE") or "STABLE").upper()
    regime_state = str(context.get("regime_state", "BALANCED") or "BALANCED").upper()
    surface_quality = _profile_quality_score(profile)
    energy_state = str(profile.get("energy_state", "UNKNOWN") or "UNKNOWN").upper()
    precursor_state = str(profile.get("precursor_state", "UNKNOWN") or "UNKNOWN").upper()

    multiplier = 1.0
    reasons: List[str] = []

    if macro_bias_alignment == "ALIGNED":
        multiplier *= 0.9
        reasons.append("macro_bias_aligned")
    elif macro_bias_alignment == "COUNTER":
        multiplier *= 1.08
        reasons.append("macro_bias_counter")

    if _supportive_htf_zone(direction, htf_zone_kernel):
        multiplier *= 0.92
        reasons.append("supportive_htf_zone")
    elif htf_zone_kernel == "MID_VOID":
        multiplier *= 1.08
        reasons.append("mid_void_penalty")
    elif htf_zone_kernel == "REFERENCE_LEVEL":
        multiplier *= 0.97
        reasons.append("reference_level_support")

    if liquidity_map_kernel == "SWEEP_READY":
        if route_operating_mode in {
            "IGNITION_RELEASE_FAST",
            "RELEASE_CONFIRM",
            "HARD_REJECTION",
            "HARD_REBOUND",
            "PRESSURE_LIFT_CAPTURE",
            "PRESSURE_BUILD_CAPTURE",
        }:
            multiplier *= 0.92
            reasons.append("sweep_ready_trigger_support")
        else:
            multiplier *= 0.96
            reasons.append("sweep_ready_support")
    elif liquidity_map_kernel == "FLOW_IMBALANCED":
        if "FLOW_DRIFT" in doctrine_id or "PRESSURE" in doctrine_id:
            multiplier *= 0.94
            reasons.append("flow_imbalance_support")
        else:
            multiplier *= 0.98
            reasons.append("flow_imbalance_mild_support")
    elif liquidity_map_kernel == "TOXIC_THIN":
        multiplier *= 1.12
        reasons.append("toxic_thin_penalty")

    if surface_quality >= 3.6:
        multiplier *= 0.94
        reasons.append("high_surface_quality")
    elif surface_quality >= 2.8:
        multiplier *= 0.97
        reasons.append("workable_surface_quality")
    elif surface_quality <= 1.9:
        multiplier *= 1.08
        reasons.append("weak_surface_quality")

    if session_state == "INJECTING" and energy_state in {"IGNITION", "DRIVE"}:
        multiplier *= 0.95
        reasons.append("injecting_session_energy_support")
    elif session_state == "BLEEDING" and energy_state in {"DORMANT", "DRIFT"}:
        multiplier *= 1.06
        reasons.append("bleeding_session_penalty")

    if regime_state == "EXPANSION" and route_operating_mode in {"IGNITION_RELEASE_FAST", "DRIFT_REACCEL", "LONG_DRIFT_REACCEL"}:
        multiplier *= 0.96
        reasons.append("expansion_regime_support")
    elif regime_state == "BALANCED" and route_operating_mode in {"DRIFT_CONFIRM", "LONG_DRIFT_CONFIRM", "COIL_BREAK"}:
        multiplier *= 0.97
        reasons.append("balanced_regime_support")
    elif regime_state == "COMPRESSION" and route_operating_mode in {"PRESSURE_EXTENSION_QUALIFIED", "PRESSURE_BUILD_SWING"}:
        multiplier *= 1.03
        reasons.append("compression_extension_penalty")

    if precursor_state == "PRESSURED" and route_operating_mode in {"PRESSURE_LIFT_CAPTURE", "PRESSURE_BUILD_CAPTURE", "DRIFT_REACCEL", "LONG_DRIFT_REACCEL"}:
        multiplier *= 0.97
        reasons.append("pressured_precursor_support")

    if "EXPANSION_RELEASE" in doctrine_id:
        if route_operating_mode == "EXPANSION_RELEASE_LAUNCH" and regime_state in {"BALANCED", "EXPANSION"}:
            multiplier *= 0.95
            reasons.append("expansion_release_regime_support")
        if route_operating_mode == "EXPANSION_RELEASE_LAUNCH" and session_state == "INJECTING":
            multiplier *= 0.96
            reasons.append("expansion_release_injecting_support")
        if route_operating_mode == "EXPANSION_RELEASE_LAUNCH" and surface_quality >= 4.0:
            multiplier *= 0.94
            reasons.append("expansion_release_high_quality")
        if route_operating_mode == "EXPANSION_RELEASE_HOLD" and surface_quality <= 2.2:
            multiplier *= 1.04
            reasons.append("expansion_release_hold_low_quality_penalty")

    if "COILED_COMPRESSION" in doctrine_id:
        if route_operating_mode == "COIL_BREAK" and surface_quality >= 2.6:
            multiplier *= 0.96
            reasons.append("coil_break_quality_support")
        if route_operating_mode == "COIL_EXPAND" and liquidity_map_kernel in {"SWEEP_READY", "FLOW_IMBALANCED"}:
            multiplier *= 0.97
            reasons.append("coil_expand_liquidity_support")

    if "FLOW_DRIFT" in doctrine_id:
        if route_operating_mode in {"DRIFT_REACCEL", "LONG_DRIFT_REACCEL"} and surface_quality >= 2.5:
            multiplier *= 0.97
            reasons.append("flow_drift_reaccel_quality_support")
        if route_operating_mode in {"DRIFT_CONFIRM", "LONG_DRIFT_CONFIRM"} and htf_zone_kernel == "REFERENCE_LEVEL":
            multiplier *= 0.98
            reasons.append("flow_drift_reference_support")
        if route_operating_mode in {"DRIFT_CONFIRM", "LONG_DRIFT_CONFIRM"} and session_state == "STABLE":
            multiplier *= 0.98
            reasons.append("flow_drift_stable_session_support")

    if doctrine_id == "TRANSITION_RELEASE_SHORT_STANDARD" and route_operating_mode in {"IGNITION_RELEASE_FAST", "RELEASE_CONFIRM"} and surface_quality >= 2.2:
        multiplier *= 0.97
        reasons.append("transition_release_quality_support")
    if doctrine_id == "TRANSITION_RELEASE_SHORT_STANDARD" and route_operating_mode == "RELEASE_CONFIRM":
        if htf_zone_kernel == "REFERENCE_LEVEL":
            multiplier *= 0.95
            reasons.append("transition_release_reference_confirm_relief")
        if session_state in {"STABLE", "INJECTING"} and surface_quality >= 2.2:
            multiplier *= 0.97
            reasons.append("transition_release_confirm_nearmiss_relief")
    if doctrine_id == "TRANSITION_RELEASE_SHORT_STANDARD" and route_operating_mode == "RELEASE_EXTENSION":
        if htf_zone_kernel == "REFERENCE_LEVEL":
            multiplier *= 0.95
            reasons.append("transition_release_reference_extension_relief")
        if liquidity_map_kernel == "FLOW_IMBALANCED":
            multiplier *= 0.97
            reasons.append("transition_release_extension_flow_relief")

    if doctrine_id == "COMPRESSION_PRESSURE_LIFT_LONG" and route_operating_mode == "PRESSURE_LIFT_CAPTURE" and liquidity_map_kernel in {"SWEEP_READY", "FLOW_IMBALANCED"}:
        multiplier *= 0.97
        reasons.append("pressure_lift_liquidity_support")

    if "PRESSURE_DRIVE" in doctrine_id:
        if route_operating_mode in {"PRESSURE_DRIVE_EDGE", "PRESSURE_DRIVE_MID", "PRESSURE_DRIVE_HOLD"} and liquidity_map_kernel == "FLOW_IMBALANCED":
            multiplier *= 0.94
            reasons.append("pressure_drive_flow_lane")
        if route_operating_mode == "PRESSURE_DRIVE_HOLD" and htf_zone_kernel == "REFERENCE_LEVEL":
            multiplier *= 0.95
            reasons.append("pressure_drive_reference_hold")
        if route_operating_mode == "PRESSURE_DRIVE_EDGE" and htf_zone_kernel == "MID_VOID":
            multiplier *= 1.02
            reasons.append("pressure_drive_edge_mid_void_penalty")
        if session_state == "BLEEDING" and route_operating_mode in {"PRESSURE_DRIVE_MID", "PRESSURE_DRIVE_HOLD"} and energy_state in {"DRIFT", "DRIVE"}:
            multiplier *= 0.96
            reasons.append("pressure_drive_bleeding_relief")

    multiplier = max(0.8, min(1.16, multiplier))
    return {
        "multiplier": round(multiplier, 6),
        "surface_quality_score": round(surface_quality, 6),
        "reasons": reasons,
    }


def _sustained_charge_profile(
    *,
    doctrine_id: str,
    route_operating_mode: str,
    direction: str,
    anchor_index: int,
    profiles_by_anchor: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    direction = str(direction or "UNKNOWN").upper()

    if doctrine_id != "COILED_COMPRESSION_LONG" or route_operating_mode not in {"COIL_BREAK", "COIL_EXPAND"}:
        return {
            "enabled": False,
            "effective_energy": 0.0,
            "sustained_energy": 0.0,
            "sample_count": 0,
            "floor_ratio": 1.0,
        }

    floor_ratio = 0.95 if route_operating_mode == "COIL_BREAK" else 0.92
    energies: List[float] = []
    for idx in range(max(0, anchor_index - 2), anchor_index + 1):
        profile = profiles_by_anchor.get(idx)
        if not profile:
            continue
        profile_direction = str(profile.get("direction_group", "") or "").upper()
        if profile_direction != direction:
            continue
        energies.append(abs(float(profile.get("velocity_pips_per_sec", 0.0) or 0.0)))

    sustained_energy = sum(energies) / len(energies) if energies else 0.0
    return {
        "enabled": len(energies) >= 2,
        "effective_energy": sustained_energy,
        "sustained_energy": round(sustained_energy, 6),
        "sample_count": len(energies),
        "floor_ratio": floor_ratio,
    }


def _confirmation_ratio(
    *,
    base_ratio: float,
    doctrine_id: str,
    route_operating_mode: str,
    direction: str,
    profile: Dict[str, Any],
    context: Dict[str, Any],
) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    direction = str(direction or "UNKNOWN").upper()
    ratio = float(base_ratio)
    reasons: List[str] = []

    macro_bias_alignment = str(context.get("macro_bias_alignment", "NEUTRAL") or "NEUTRAL").upper()
    htf_zone_kernel = str(context.get("htf_zone_kernel", "UNSPECIFIED") or "UNSPECIFIED").upper()
    liquidity_map_kernel = str(context.get("liquidity_map_kernel", "UNSPECIFIED") or "UNSPECIFIED").upper()
    surface_quality = _profile_quality_score(profile)

    if macro_bias_alignment == "ALIGNED":
        ratio -= 0.03
        reasons.append("macro_bias_aligned")
    elif macro_bias_alignment == "COUNTER":
        ratio += 0.04
        reasons.append("macro_bias_counter")

    if _supportive_htf_zone(direction, htf_zone_kernel):
        ratio -= 0.02
        reasons.append("supportive_htf_zone")
    elif htf_zone_kernel == "MID_VOID":
        ratio += 0.03
        reasons.append("mid_void_penalty")

    if liquidity_map_kernel == "SWEEP_READY" and route_operating_mode in {"IGNITION_RELEASE_FAST", "HARD_REJECTION", "HARD_REBOUND"}:
        ratio -= 0.03
        reasons.append("sweep_ready_fast_path")
    elif liquidity_map_kernel == "TOXIC_THIN":
        ratio += 0.03
        reasons.append("toxic_thin_penalty")

    if surface_quality >= 3.5:
        ratio -= 0.02
        reasons.append("high_surface_quality")
    elif surface_quality <= 1.9:
        ratio += 0.03
        reasons.append("weak_surface_quality")

    if "OSCILLATION_PRESSURE_BUILD" in doctrine_id and route_operating_mode == "PRESSURE_BUILD_SWING":
        ratio += 0.02
        reasons.append("pressure_build_swing_extra_proof")
    if "PRESSURE_DRIVE" in doctrine_id and route_operating_mode in {"PRESSURE_DRIVE_MID", "PRESSURE_DRIVE_HOLD"}:
        if liquidity_map_kernel == "FLOW_IMBALANCED":
            ratio -= 0.02
            reasons.append("pressure_drive_flow_lane")
        if htf_zone_kernel == "REFERENCE_LEVEL":
            ratio -= 0.01
            reasons.append("pressure_drive_reference_hold")

    diagnosis = set(_doctrine_window_diagnosis(doctrine_id))
    if "T6_DECAY_TOO_STRICT" in diagnosis:
        ratio -= 0.03
        reasons.append("tier3_decay_relief")
    if "T6_CHARGE_TOO_STRICT" in diagnosis and route_operating_mode in {"COIL_EXPAND", "DRIFT_REACCEL", "LONG_DRIFT_REACCEL", "IGNITION_RELEASE_FAST"}:
        ratio -= 0.01
        reasons.append("tier3_charge_relief")

    ratio = max(0.68, min(0.92, ratio))
    return {
        "ratio": round(ratio, 6),
        "reasons": reasons,
    }


def _projection_policy(
    doctrine_id: str,
    route_operating_mode: str,
    profile: Dict[str, Any],
    context: Dict[str, Any],
    variant: Dict[str, Any],
    option: Dict[str, Any],
) -> Dict[str, float]:
    friction = float(profile.get("friction_threshold_pips", 0.0) or 0.0)
    boundary_width = float(profile.get("boundary_width_pips", 0.0) or 0.0)
    path_budget = float(profile.get("path_discovery_pips", 0.0) or 0.0)
    quality_score = _profile_quality_score(profile)
    precursor = str(profile.get("precursor_state", "") or "").upper()
    energy = str(profile.get("energy_state", "") or "").upper()
    context_target_distance = float(context["projection_axis"]["expected_target_distance_pips"])
    context_stop_distance = float(context["projection_axis"]["stop_distance_pips"])
    context_ttl_sec = float(context["projection_axis"]["expected_ttl_sec"])
    target_distance = max(friction, context_target_distance * float(variant["target_multiplier"]))
    stop_distance = max(friction, context_stop_distance * float(variant["stop_multiplier"]))
    ttl_ticks = max(3, int(round(context_ttl_sec * float(variant["ttl_multiplier"]))))
    doctrine_shape = {
        "target_distance": target_distance,
        "stop_distance": stop_distance,
        "ttl_ticks": ttl_ticks,
    }

    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()

    if route_operating_mode == "IGNITION_RELEASE_FAST":
        target_distance = max(friction * 1.8, min(target_distance * 0.88, max(path_budget * 0.82, friction * 2.0)))
        stop_distance = max(friction, min(stop_distance * 0.68, max(boundary_width * 0.22, friction * 1.15)))
        ttl_ticks = max(5, min(ttl_ticks, 12))
    elif route_operating_mode == "RELEASE_CONFIRM":
        target_distance = max(friction * 1.75, min(target_distance * 0.9, max(path_budget * 0.75, friction * 1.95)))
        stop_distance = max(friction, min(stop_distance * 0.72, max(boundary_width * 0.24, friction * 1.18)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif route_operating_mode == "RELEASE_EXTENSION":
        target_distance = max(friction * 1.9, min(target_distance * 0.96, max(path_budget * 0.9, friction * 2.1)))
        stop_distance = max(friction, min(stop_distance * 0.76, max(boundary_width * 0.26, friction * 1.2)))
        ttl_ticks = max(7, min(ttl_ticks, 17))
    elif route_operating_mode == "DRIFT_REACCEL":
        target_distance = max(friction * 1.3, min(target_distance * 0.82, max(path_budget * 0.55, friction * 1.45)))
        stop_distance = max(friction, min(stop_distance * 0.68, max(boundary_width * 0.2, friction * 1.12)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif route_operating_mode == "DRIFT_CONFIRM":
        target_distance = max(friction * 1.2, min(target_distance * 0.72, max(path_budget * 0.45, friction * 1.35)))
        stop_distance = max(friction, min(stop_distance * 0.65, max(boundary_width * 0.18, friction * 1.08)))
        ttl_ticks = max(5, min(ttl_ticks, 12))
    elif route_operating_mode == "DRIFT_HARVEST":
        target_distance = max(friction * 1.15, min(target_distance * 0.64, max(path_budget * 0.38, friction * 1.28)))
        stop_distance = max(friction, min(stop_distance * 0.6, max(boundary_width * 0.16, friction * 1.02)))
        ttl_ticks = max(4, min(ttl_ticks, 10))
    elif route_operating_mode == "LONG_DRIFT_REACCEL":
        target_distance = max(friction * 1.32, min(target_distance * 0.86, max(path_budget * 0.58, friction * 1.48)))
        stop_distance = max(friction, min(stop_distance * 0.69, max(boundary_width * 0.2, friction * 1.12)))
        ttl_ticks = max(6, min(ttl_ticks, 15))
    elif route_operating_mode == "LONG_DRIFT_IGNITION":
        target_distance = max(friction * 1.28, min(target_distance * 0.84, max(path_budget * 0.56, friction * 1.42)))
        stop_distance = max(friction, min(stop_distance * 0.67, max(boundary_width * 0.19, friction * 1.1)))
        ttl_ticks = max(5, min(ttl_ticks, 13))
    elif route_operating_mode == "LONG_DRIFT_CONFIRM":
        target_distance = max(friction * 1.22, min(target_distance * 0.76, max(path_budget * 0.48, friction * 1.36)))
        stop_distance = max(friction, min(stop_distance * 0.64, max(boundary_width * 0.18, friction * 1.08)))
        ttl_ticks = max(5, min(ttl_ticks, 13))
    elif route_operating_mode == "COMPRESSION_SWING":
        target_distance = max(friction * 1.55, min(target_distance * 0.9, max(path_budget * 0.72, friction * 1.75)))
        stop_distance = max(friction, min(stop_distance * 0.72, max(boundary_width * 0.28, friction * 1.18)))
        ttl_ticks = max(6, min(ttl_ticks, 16))
    elif route_operating_mode == "PRESSURE_LIFT_CAPTURE":
        target_distance = max(friction * 1.45, min(target_distance * 0.78, max(boundary_width * 0.48, friction * 1.65)))
        stop_distance = max(friction, min(stop_distance * 0.68, max(boundary_width * 0.24, friction * 1.12)))
        ttl_ticks = max(5, min(ttl_ticks, 13))
    elif route_operating_mode == "PRESSURE_EXTENSION_QUALIFIED":
        target_distance = max(friction * 1.5, min(target_distance * 0.88, max(path_budget * 0.68, friction * 1.75)))
        stop_distance = max(friction, min(stop_distance * 0.73, max(boundary_width * 0.28, friction * 1.16)))
        ttl_ticks = max(6, min(ttl_ticks, 15))
    elif route_operating_mode == "HARD_REJECTION":
        target_distance = max(friction * 1.15, min(target_distance * 0.68, max(boundary_width * 0.26, friction * 1.25)))
        stop_distance = max(friction, min(stop_distance * 0.52, max(boundary_width * 0.12, friction * 0.98)))
        ttl_ticks = max(3, min(ttl_ticks, 8))
    elif route_operating_mode == "SOFT_REJECTION":
        target_distance = max(friction * 1.08, min(target_distance * 0.6, max(boundary_width * 0.2, friction * 1.16)))
        stop_distance = max(friction, min(stop_distance * 0.5, max(boundary_width * 0.11, friction * 0.96)))
        ttl_ticks = max(3, min(ttl_ticks, 7))
    elif route_operating_mode == "HARD_REBOUND":
        target_distance = max(friction * 1.15, min(target_distance * 0.7, max(boundary_width * 0.26, friction * 1.24)))
        stop_distance = max(friction, min(stop_distance * 0.54, max(boundary_width * 0.12, friction * 0.99)))
        ttl_ticks = max(3, min(ttl_ticks, 8))
    elif route_operating_mode == "SOFT_REBOUND":
        target_distance = max(friction * 1.08, min(target_distance * 0.62, max(boundary_width * 0.2, friction * 1.14)))
        stop_distance = max(friction, min(stop_distance * 0.5, max(boundary_width * 0.11, friction * 0.97)))
        ttl_ticks = max(3, min(ttl_ticks, 7))
    elif route_operating_mode == "PRESSURE_BUILD_CAPTURE":
        target_distance = max(friction * 1.3, min(target_distance * 0.74, max(path_budget * 0.48, friction * 1.45)))
        stop_distance = max(friction, min(stop_distance * 0.64, max(boundary_width * 0.2, friction * 1.1)))
        ttl_ticks = max(5, min(ttl_ticks, 12))
    elif route_operating_mode == "PRESSURE_BUILD_SWING":
        target_distance = max(friction * 1.35, min(target_distance * 0.82, max(path_budget * 0.58, friction * 1.58)))
        stop_distance = max(friction, min(stop_distance * 0.7, max(boundary_width * 0.24, friction * 1.16)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif route_operating_mode == "EXPANSION_RELEASE_LAUNCH":
        target_distance = max(friction * 1.4, min(target_distance * 0.86, max(path_budget * 0.62, friction * 1.62)))
        stop_distance = max(friction, min(stop_distance * 0.66, max(boundary_width * 0.22, friction * 1.08)))
        ttl_ticks = max(5, min(ttl_ticks, 12))
    elif route_operating_mode == "EXPANSION_RELEASE_HOLD":
        target_distance = max(friction * 1.45, min(target_distance * 0.9, max(path_budget * 0.68, friction * 1.68)))
        stop_distance = max(friction, min(stop_distance * 0.7, max(boundary_width * 0.24, friction * 1.12)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif "TRANSITION_RELEASE" in doctrine_id:
        target_distance = max(friction * 1.8, min(target_distance * 0.82, max(path_budget * 0.72, friction * 2.0)))
        stop_distance = max(friction, min(stop_distance * 0.72, max(boundary_width * 0.24, friction * 1.2)))
        ttl_ticks = max(6, min(ttl_ticks, 15))
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id or "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        target_distance = max(friction * 1.5, min(target_distance * 0.8, max(boundary_width * 0.5, friction * 1.7)))
        stop_distance = max(friction, min(stop_distance * 0.7, max(boundary_width * 0.26, friction * 1.15)))
        ttl_ticks = max(6, min(ttl_ticks, 16))
    elif "FLOW_DRIFT" in doctrine_id:
        target_distance = max(friction * 1.25, min(target_distance * 0.72, max(path_budget * 0.45, friction * 1.4)))
        stop_distance = max(friction, min(stop_distance * 0.65, max(boundary_width * 0.2, friction * 1.15)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif "FAILED_PUSH_" in doctrine_id:
        target_distance = max(friction * 1.4, min(target_distance * 0.8, max(boundary_width * 0.35, friction * 1.6)))
        stop_distance = max(friction, min(stop_distance * 0.7, max(boundary_width * 0.18, friction * 1.1)))
        ttl_ticks = max(5, min(ttl_ticks, 12))
    elif "OSCILLATION_EDGE" in doctrine_id:
        target_distance = max(friction * 1.2, min(target_distance * 0.7, max(boundary_width * 0.28, friction * 1.3)))
        stop_distance = max(friction, min(stop_distance * 0.55, max(boundary_width * 0.14, friction * 1.0)))
        ttl_ticks = max(4, min(ttl_ticks, 10))
    elif "OSCILLATION_PRESSURE_BUILD" in doctrine_id:
        target_distance = max(friction * 1.35, min(target_distance * 0.8, max(path_budget * 0.55, friction * 1.5)))
        stop_distance = max(friction, min(stop_distance * 0.7, max(boundary_width * 0.22, friction * 1.2)))
        ttl_ticks = max(6, min(ttl_ticks, 14))
    elif "PRESSURE_DRIVE" in doctrine_id:
        target_distance = max(friction * 1.35, min(target_distance * 0.78, max(path_budget * 0.5, friction * 1.45)))
        stop_distance = max(friction, min(stop_distance * 0.68, max(boundary_width * 0.2, friction * 1.15)))
        ttl_ticks = max(6, min(ttl_ticks, 14))

    if doctrine_id in {"FLOW_DRIFT_SHORT", "FLOW_DRIFT_LONG"}:
        if precursor == "PRESSURED":
            target_distance *= 1.05
        elif precursor == "BALANCED":
            target_distance *= 0.95
            stop_distance *= 0.96
        if energy == "DORMANT":
            ttl_ticks = max(4, int(round(ttl_ticks * 0.9)))

    if doctrine_id == "COMPRESSION_PRESSURE_LIFT_LONG" and route_operating_mode == "PRESSURE_EXTENSION_QUALIFIED" and quality_score < 2.65:
        target_distance *= 0.92
        ttl_ticks = max(5, int(round(ttl_ticks * 0.9)))

    if doctrine_id == "OSCILLATION_EDGE_LONG_SCALP" and route_operating_mode == "SOFT_REBOUND":
        target_distance *= 0.94
    if doctrine_id == "OSCILLATION_EDGE_SHORT_SCALP" and route_operating_mode == "SOFT_REJECTION":
        target_distance *= 0.94

    if doctrine_id == "OSCILLATION_PRESSURE_BUILD_LONG" and route_operating_mode == "PRESSURE_BUILD_SWING" and quality_score < 2.7:
        target_distance *= 0.9
        ttl_ticks = max(5, int(round(ttl_ticks * 0.9)))

    target_distance = max(friction, target_distance * float(option.get("target_scale", 1.0) or 1.0))
    stop_distance = max(friction, stop_distance * float(option.get("stop_scale", 1.0) or 1.0))
    ttl_ticks = int(round(ttl_ticks * float(option.get("ttl_scale", 1.0) or 1.0)))
    ttl_ticks = max(int(option.get("ttl_min", 3) or 3), ttl_ticks)
    ttl_ticks = min(int(option.get("ttl_max", ttl_ticks) or ttl_ticks), ttl_ticks)

    return {
        "target_distance": round(target_distance, 6),
        "stop_distance": round(stop_distance, 6),
        "ttl_ticks": ttl_ticks,
        "policy_layers": {
            "context_projection": {
                "target_distance": round(context_target_distance, 6),
                "stop_distance": round(context_stop_distance, 6),
                "ttl_ticks": round(context_ttl_sec, 6),
            },
            "variant_projection": {
                "variant_id": str(variant["variant_id"]),
                "target_multiplier": round(float(variant["target_multiplier"]), 6),
                "stop_multiplier": round(float(variant["stop_multiplier"]), 6),
                "ttl_multiplier": round(float(variant["ttl_multiplier"]), 6),
                "entry_offset_ticks": int(variant["entry_offset_ticks"]),
                "route_id": str(variant.get("route_id", "") or ""),
                "segment_type": str(variant.get("segment_type", "BASE") or "BASE"),
                "segment_value": str(variant.get("segment_value", "ALL") or "ALL"),
                "route_operating_mode": str(variant.get("route_operating_mode", "GENERAL_CAPTURE") or "GENERAL_CAPTURE"),
            },
            "doctrine_shape": {
                "target_distance": round(doctrine_shape["target_distance"], 6),
                "stop_distance": round(doctrine_shape["stop_distance"], 6),
                "ttl_ticks": int(doctrine_shape["ttl_ticks"]),
            },
            "adjustment_option": {
                "option_id": str(option.get("option_id", "BALANCED") or "BALANCED"),
                "label": str(option.get("label", "Balanced") or "Balanced"),
                "energy_floor_multiplier": round(float(option.get("energy_floor_multiplier", 1.0) or 1.0), 6),
                "confirmation_window_delta": int(option.get("confirmation_window_delta", 0) or 0),
                "confirmation_ratio": round(float(option.get("confirmation_ratio", 0.8) or 0.8), 6),
                "target_scale": round(float(option.get("target_scale", 1.0) or 1.0), 6),
                "stop_scale": round(float(option.get("stop_scale", 1.0) or 1.0), 6),
                "ttl_scale": round(float(option.get("ttl_scale", 1.0) or 1.0), 6),
                "ttl_min": int(option.get("ttl_min", 3) or 3),
                "ttl_max": int(option.get("ttl_max", 99) or 99),
                "blitz_mutation": dict(option.get("blitz_mutation", {}) or {}),
            },
            "final_projection": {
                "target_distance": round(target_distance, 6),
                "stop_distance": round(stop_distance, 6),
                "ttl_ticks": int(ttl_ticks),
            },
        },
    }


def _confirmation_window(
    doctrine_id: str,
    route_operating_mode: str,
    anchor_index: int,
    ttl_end: int,
    variant: Dict[str, Any],
    option: Dict[str, Any],
) -> int:
    variant_id = str(variant.get("variant_id", "") or "")
    route_operating_mode = str(route_operating_mode or "GENERAL_CAPTURE").upper()
    if route_operating_mode == "IGNITION_RELEASE_FAST":
        window = 2
    elif route_operating_mode == "EXPANSION_RELEASE_LAUNCH":
        window = 3
    elif route_operating_mode == "EXPANSION_RELEASE_HOLD":
        window = 4
    elif route_operating_mode in {"HARD_REJECTION", "HARD_REBOUND"}:
        window = 2
    elif route_operating_mode in {"SOFT_REJECTION", "SOFT_REBOUND", "DRIFT_HARVEST"}:
        window = 1
    elif route_operating_mode in {"DRIFT_REACCEL", "LONG_DRIFT_REACCEL", "PRESSURE_BUILD_CAPTURE"}:
        window = 3
    elif route_operating_mode in {"RELEASE_CONFIRM", "DRIFT_CONFIRM", "LONG_DRIFT_CONFIRM", "LONG_DRIFT_IGNITION", "PRESSURE_LIFT_CAPTURE"}:
        window = 4
    elif route_operating_mode in {"RELEASE_EXTENSION", "COMPRESSION_SWING", "PRESSURE_EXTENSION_QUALIFIED", "PRESSURE_BUILD_SWING"}:
        window = 5
    elif "OSCILLATION_EDGE" in doctrine_id:
        window = 2
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id or "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        window = 5
    elif "TRANSITION_RELEASE" in doctrine_id or "FLOW_DRIFT" in doctrine_id:
        window = 4
    else:
        window = 3
    if "SCALP" in variant_id or "FRONT_RUN" in variant_id or "SNAP" in variant_id:
        window = max(1, window - 1)
    elif "CONFIRM" in variant_id or "HOLD" in variant_id or "RUNNER" in variant_id or "EXTENSION" in variant_id or "SWING" in variant_id:
        window += 1
    window += int(option.get("confirmation_window_delta", 0) or 0)
    diagnosis = set(_doctrine_window_diagnosis(doctrine_id))
    if "T6_DECAY_TOO_STRICT" in diagnosis:
        window += 1
    window = max(1, window)
    return min(ttl_end, anchor_index + window)


def build_trigger_candidate(
    *,
    profile: Dict[str, Any],
    context: Dict[str, Any],
    cluster: Dict[str, Any],
    ticks: List[Dict[str, Any]],
    profiles_by_anchor: Dict[int, Dict[str, Any]],
    selected_route: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    direction = cluster["direction_group"]
    doctrine_id = str(cluster["doctrine_id"])
    variant = _variant_config(selected_route)
    route_operating_mode = str(variant.get("route_operating_mode", "GENERAL_CAPTURE") or "GENERAL_CAPTURE")
    target_distance_bucket = str(profile.get("target_distance_bucket", "") or "")
    adjustment_option_ladder = _apply_doctrine_option_policy(
        doctrine_id,
        route_operating_mode,
        _adjustment_option_ladder(doctrine_id, route_operating_mode),
    )
    if not adjustment_option_ladder:
        return {
            "status": "INVALID",
            "reason": "option_pruned",
            "strategy_id": cluster["cluster_id"],
            "doctrine_id": cluster["doctrine_id"],
            "cluster_id": cluster["cluster_id"],
            "direction": cluster["direction_group"],
            "profile_id": profile["profile_id"],
            "distance_expression_id": str(profile.get("distance_family_id", "") or ""),
            "target_distance_bucket": str(profile.get("target_distance_bucket", "") or ""),
            "source_anchor_index": int(profile["anchor_index"]),
            "anchor_index": int(profile["anchor_index"]) + int(variant["entry_offset_ticks"]),
            "tier1_route_id": str(variant.get("route_id", "") or ""),
            "tier1_route_operating_mode": route_operating_mode,
            "tier1_variant_id": str(variant["variant_id"]),
            "adjustment_option_ladder": [],
        }
    selected_adjustment_option = _apply_blitz_phase4_option_mutation(
        doctrine_id,
        route_operating_mode,
        _select_adjustment_option(doctrine_id, route_operating_mode, profile, adjustment_option_ladder),
    )
    source_anchor_index = int(profile["anchor_index"])
    anchor_index = source_anchor_index + int(variant["entry_offset_ticks"])
    strategy_id = cluster["cluster_id"]
    if anchor_index >= len(ticks):
        return {
            "status": "ABORTED",
            "reason": "anchor_out_of_range",
            "strategy_id": strategy_id,
            "doctrine_id": cluster["doctrine_id"],
            "cluster_id": cluster["cluster_id"],
            "direction": cluster["direction_group"],
            "profile_id": profile["profile_id"],
            "distance_expression_id": str(profile.get("distance_family_id", "") or ""),
            "target_distance_bucket": str(profile.get("target_distance_bucket", "") or ""),
            "source_anchor_index": source_anchor_index,
            "anchor_index": anchor_index,
            "tier1_variant_id": str(variant["variant_id"]),
            "tier1_entry_offset_ticks": int(variant["entry_offset_ticks"]),
            "tier1_route_id": str(variant.get("route_id", "") or ""),
            "tier1_route_operating_mode": route_operating_mode,
        }
    entry_profile = profiles_by_anchor.get(anchor_index, profile)

    zone_state = str(entry_profile.get("zone_state", profile.get("zone_state", "")) or "")
    expected_zone = _expected_zone(profile, selected_route)
    zone_ok = _zone_ok(doctrine_id, direction, zone_state, expected_zone)
    pocket_whitelist = _pocket_whitelist_status(
        doctrine_id,
        session_state=str(context.get("session_state", "UNKNOWN") or "UNKNOWN"),
        regime_state=str(context.get("regime_state", "UNKNOWN") or "UNKNOWN"),
        route_operating_mode=route_operating_mode,
        target_distance_bucket=target_distance_bucket,
    )
    energy_now = abs(float(entry_profile.get("velocity_pips_per_sec", profile.get("velocity_pips_per_sec", 0.0)) or 0.0))
    base_energy_floor = _energy_floor(doctrine_id, route_operating_mode, float(context["energy_floor"]), selected_adjustment_option)
    charge_truth = _charge_truth_multiplier(
        doctrine_id=doctrine_id,
        route_operating_mode=route_operating_mode,
        direction=direction,
        profile=profile,
        context=context,
    )
    gated_energy_floor = base_energy_floor * float(charge_truth["multiplier"])
    sustained_charge = _sustained_charge_profile(
        doctrine_id=doctrine_id,
        route_operating_mode=route_operating_mode,
        direction=direction,
        anchor_index=anchor_index,
        profiles_by_anchor=profiles_by_anchor,
    )
    sustained_energy_ok = bool(sustained_charge["enabled"]) and float(sustained_charge["effective_energy"]) >= (
        gated_energy_floor * float(sustained_charge["floor_ratio"])
    )
    effective_entry_energy = max(
        energy_now,
        float(sustained_charge["effective_energy"]) if sustained_energy_ok else 0.0,
    )
    energy_ok = effective_entry_energy >= gated_energy_floor or sustained_energy_ok
    policy = _projection_policy(doctrine_id, route_operating_mode, profile, context, variant, selected_adjustment_option)
    ttl_ticks = int(policy["ttl_ticks"])
    ttl_end = min(len(ticks) - 1, anchor_index + ttl_ticks)

    confirmation_window = _confirmation_window(doctrine_id, route_operating_mode, anchor_index, ttl_end, variant, selected_adjustment_option)
    confirmed = False
    confirmation_ratio_info = _confirmation_ratio(
        base_ratio=float(selected_adjustment_option.get("confirmation_ratio", 0.8) or 0.8),
        doctrine_id=doctrine_id,
        route_operating_mode=route_operating_mode,
        direction=direction,
        profile=profile,
        context=context,
    )
    confirmation_ratio = float(confirmation_ratio_info["ratio"])
    for idx in range(anchor_index + 1, confirmation_window + 1):
        future_profile = profiles_by_anchor.get(idx)
        if not future_profile:
            continue
        future_energy = abs(float(future_profile.get("velocity_pips_per_sec", 0.0) or 0.0))
        future_bias = str(future_profile.get("direction_group", "") or "")
        if future_energy >= gated_energy_floor * confirmation_ratio and future_bias == direction:
            confirmed = True
            break

    shared_payload = {
        "strategy_id": strategy_id,
        "doctrine_id": cluster["doctrine_id"],
        "cluster_id": cluster["cluster_id"],
        "direction": direction,
        "profile_id": profile["profile_id"],
        "distance_expression_id": str(profile.get("distance_family_id", "") or ""),
        "target_distance_bucket": target_distance_bucket,
        "source_anchor_index": source_anchor_index,
        "anchor_index": anchor_index,
        "ttl_end_index": ttl_end,
        "ttl_ticks": ttl_ticks,
        "tier1_route_id": str(variant.get("route_id", "") or ""),
        "tier1_segment_type": str(variant.get("segment_type", "BASE") or "BASE"),
        "tier1_segment_value": str(variant.get("segment_value", "ALL") or "ALL"),
        "tier1_route_operating_mode": route_operating_mode,
        "tier1_variant_id": str(variant["variant_id"]),
        "tier1_entry_offset_ticks": int(variant["entry_offset_ticks"]),
        "selected_adjustment_option_id": str(selected_adjustment_option.get("option_id", "BALANCED") or "BALANCED"),
        "selected_adjustment_option_label": str(selected_adjustment_option.get("label", "Balanced") or "Balanced"),
        "selected_adjustment_option_status": str(selected_adjustment_option.get("option_status", "UNLOCKED") or "UNLOCKED"),
        "base_energy_floor": round(base_energy_floor, 6),
        "entry_energy_now": round(energy_now, 6),
        "effective_entry_energy": round(effective_entry_energy, 6),
        "charge_truth_multiplier": round(float(charge_truth["multiplier"]), 6),
        "charge_truth_reasons": list(charge_truth["reasons"]),
        "kill_switch_energy_floor": round(gated_energy_floor, 6),
        "sustained_charge_profile": sustained_charge,
        "surface_quality_score": _profile_quality_score(profile),
        "confirmation_ratio": round(confirmation_ratio, 6),
        "confirmation_ratio_reasons": list(confirmation_ratio_info["reasons"]),
        "projection_axis": context["projection_axis"],
        "trigger_policy_layers": policy["policy_layers"],
        "adjustment_option_ladder": adjustment_option_ladder,
        "pocket_whitelist_active": bool(pocket_whitelist["whitelist_active"]),
        "pocket_whitelist_allowed": bool(pocket_whitelist["is_whitelisted"]),
        "current_pocket_id": str(pocket_whitelist["current_pocket_id"]),
        "allowed_pocket_ids": list(pocket_whitelist["allowed_pocket_ids"]),
        "context": {
            "regime_state": context["regime_state"],
            "session_state": context["session_state"],
            "instrument_multiplier": context["instrument_multiplier"],
            "macro_bias_kernel": context.get("macro_bias_kernel"),
            "htf_zone_kernel": context.get("htf_zone_kernel"),
            "liquidity_map_kernel": context.get("liquidity_map_kernel"),
            "macro_bias_alignment": context.get("macro_bias_alignment"),
            "expected_zone": expected_zone,
            "entry_zone": zone_state,
        },
    }

    if not pocket_whitelist["is_whitelisted"]:
        return {"status": "INVALID", "reason": "pocket_not_whitelisted", **shared_payload}
    if not zone_ok:
        return {"status": "ABORTED", "reason": "zone_misaligned", **shared_payload}
    if not energy_ok:
        return {"status": "ABORTED", "reason": "insufficient_charge", **shared_payload}
    if not confirmed:
        return {"status": "ABORTED", "reason": "kill_switch_decay", **shared_payload}

    tick = ticks[anchor_index]
    entry_price = float(tick["ask"] if direction == "LONG" else tick["bid"])
    target_distance = float(policy["target_distance"])
    stop_distance = float(policy["stop_distance"])
    pip_size = 0.01 if "JPY" in str(profile["profile_id"]).upper() else 0.0001
    target_price = entry_price + target_distance * pip_size if direction == "LONG" else entry_price - target_distance * pip_size
    stop_price = entry_price - stop_distance * pip_size if direction == "LONG" else entry_price + stop_distance * pip_size
    return {
        "status": "READY",
        "strategy_id": strategy_id,
        "entry_price": round(entry_price, 6),
        "target_price": round(target_price, 6),
        "stop_price": round(stop_price, 6),
        **shared_payload,
    }
