from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


WORKSPACE = Path(__file__).resolve().parents[2]
CONTROL_DIR = WORKSPACE / "control" / "v2_engine"
DEFAULT_REGISTRY_PATH = CONTROL_DIR / "v2_entry_strategy_registry.json"
DEFAULT_PHASE5_PATH = CONTROL_DIR / "phase5" / "v2_phase5_evaluation_report.json"

SURFACE_TYPES = {
    "COMPRESSION",
    "BALANCED",
    "OSCILLATION",
    "TRENDING",
    "LEVEL_EVENT",
}

TRIGGER_STATES = {
    "FAILED_BREAK",
    "RECLAIM",
    "ONE_BAR_CONFIRM",
    "FAILED_PUSH_REVERSE",
    "RETEST_HOLD",
    "CONTINUATION_PUSH",
}

DISTANCE_MODES = {
    "SCALP_DISTANCE",
    "STANDARD_DISTANCE",
    "EXTENDED_DISTANCE",
}

LEVEL_TYPES = {
    "PRIOR_HIGH",
    "PRIOR_LOW",
    "ROUND_NUMBER",
    "VWAP",
    "SESSION_OPEN",
    "SESSION_HIGH",
    "SESSION_LOW",
}

DOCTRINE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "RAW_COMPRESSION_CEILING_REJECTION_SHORT_SCALP": {
        "surface": "COMPRESSION",
        "trigger": "FAILED_BREAK",
        "distance": "SCALP_DISTANCE",
        "direction": "SHORT",
        "energy_gates": {"OFI_max": -0.10, "RV_min": 0.8, "BTP_max": 1.5},
        "sl_rule": "high_of_rejection_bar + 1.5",
        "tp_rule": "CES_weighted",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
    "RAW_COMPRESSION_CEILING_REJECTION_SHORT_STANDARD": {
        "surface": "COMPRESSION",
        "trigger": "FAILED_BREAK",
        "distance": "STANDARD_DISTANCE",
        "direction": "SHORT",
        "energy_gates": {"OFI_max": -0.10, "RV_min": 0.8, "BTP_max": 1.5},
        "sl_rule": "high_of_rejection_bar + 1.5",
        "tp_rule": "CES_weighted",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
    "TRANSITION_RELEASE_SHORT_STANDARD": {
        "surface": "BALANCED",
        "trigger": "CONTINUATION_PUSH",
        "distance": "STANDARD_DISTANCE",
        "direction": "SHORT",
        "energy_gates": {"OFI_max": -0.20, "BTP_max": 1.3, "space_below_min": 6.0, "session_pct_max": 0.75},
        "sl_rule": "midpoint_of_balanced_range + 1.0",
        "tp_rule": "range_projection_down",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
    "OSCILLATION_EDGE_SHORT_SCALP": {
        "surface": "OSCILLATION",
        "trigger": "RETEST_HOLD",
        "distance": "SCALP_DISTANCE",
        "direction": "SHORT",
        "energy_gates": {"RV_min": 0.6, "BTP_max": 1.4, "cycle_count_min": 2},
        "sl_rule": "upper_boundary + 1.0",
        "tp_rule": "lower_oscillation_boundary - 0.5",
        "runner_logic": False,
        "partial_close_eligible": True,
    },
    "FAILED_BREAK_LONG_RECLAIM_SCALP": {
        "surface": "LEVEL_EVENT",
        "surface_class": "LEVEL_RELATIVE",
        "trigger": "RECLAIM",
        "distance": "SCALP_DISTANCE",
        "direction": "LONG",
        "energy_gates": {
            "OFI_min": 0.10,
            "BTP_max": 1.4,
            "space_above_min": 5.0,
            "level_types": ["PRIOR_LOW", "ROUND_NUMBER", "SESSION_OPEN"],
        },
        "sl_rule": "low_of_failed_break_bar - 1.0",
        "tp_rule": "nearest_resistance_1pt5R_min",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
    "FAILED_BREAK_LONG_RECLAIM_STANDARD": {
        "surface": "LEVEL_EVENT",
        "surface_class": "LEVEL_RELATIVE",
        "trigger": "RECLAIM",
        "distance": "STANDARD_DISTANCE",
        "direction": "LONG",
        "energy_gates": {
            "OFI_min": 0.10,
            "BTP_max": 1.4,
            "space_above_min": 5.0,
            "level_types": ["PRIOR_LOW", "ROUND_NUMBER", "SESSION_OPEN"],
        },
        "sl_rule": "low_of_failed_break_bar - 1.0",
        "tp_rule": "nearest_resistance_1pt5R_min",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
    "BALANCED_SURFACE_LONG_PUSH_STANDARD": {
        "surface": "BALANCED",
        "trigger": "ONE_BAR_CONFIRM",
        "distance": "STANDARD_DISTANCE",
        "direction": "LONG",
        "energy_gates": {
            "OFI_min": 0.20,
            "CES_min": 0.55,
            "BTP_max": 1.3,
            "space_above_min": 7.0,
            "session_pct_max": 0.70,
        },
        "sl_rule": "midpoint_of_prior_balanced_range",
        "tp_rule": "range_width_times_1pt5_above_breakout",
        "runner_logic": True,
        "partial_close_eligible": True,
    },
}


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_entry_strategy_registry(path: Path | None = None) -> Dict[str, Any]:
    """Load the selected entry strategy registry produced by the V2 runner."""
    target = path or DEFAULT_REGISTRY_PATH
    return _read_json(target)


def load_phase5_evaluation_report(path: Path | None = None) -> Dict[str, Any]:
    """Load the full Phase 5 evaluation report for non-selected survivor context."""
    target = path or DEFAULT_PHASE5_PATH
    return _read_json(target)


def load_selected_entry_strategies(path: Path | None = None) -> List[Dict[str, Any]]:
    """Return the currently selected executable entry strategies."""
    registry = load_entry_strategy_registry(path)
    return list(registry.get("strategies", []))


def build_entry_surface_snapshot() -> Dict[str, Any]:
    """Summarize canonical entry state for downstream wrappers."""
    registry = load_entry_strategy_registry()
    phase5 = load_phase5_evaluation_report()
    selected = list(registry.get("strategies", []))
    survivors = list(phase5.get("strategies", []))
    return {
        "artifact_id": "ENTRY_SURFACE_SNAPSHOT",
        "registry_status": registry.get("status", "UNKNOWN"),
        "selected_strategy_count": len(selected),
        "selected_strategy_ids": [str(row.get("strategy_id", "") or "") for row in selected],
        "survivor_strategy_count": len(survivors),
        "selected_strategies": selected,
        "survivor_strategies": survivors,
        "registry_path": str(DEFAULT_REGISTRY_PATH),
        "phase5_report_path": str(DEFAULT_PHASE5_PATH),
    }


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def validate_level_event_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    surface_type = _clean_text(payload.get("surface_type"))
    if surface_type != "LEVEL_EVENT":
        return True, "NOT_LEVEL_EVENT"
    level_type = _clean_text(payload.get("level_type"))
    if level_type not in LEVEL_TYPES:
        return False, f"INVALID_LEVEL_TYPE:{level_type or 'MISSING'}"
    return True, "VALID_LEVEL_EVENT"


def doctrine_identity(doctrine_id: str) -> Dict[str, str]:
    parts = [segment for segment in str(doctrine_id or "").split("_") if segment]
    variant = parts[-1] if parts else "UNKNOWN"
    family = "_".join(parts[:-1]) if len(parts) > 1 else (parts[0] if parts else "UNKNOWN")
    return {
        "doctrine_id": str(doctrine_id or ""),
        "doctrine_family": family or "UNKNOWN",
        "variant": variant or "UNKNOWN",
    }


def _energy_gates_pass(payload: Dict[str, Any], energy_gates: Dict[str, Any]) -> bool:
    energy = dict(payload.get("energy_snapshot", {}) or {})
    ofi = _safe_float(energy.get("OFI", payload.get("OFI")))
    ces = _safe_float(energy.get("CES", payload.get("CES")))
    rv = _safe_float(energy.get("RV", payload.get("RV")))
    btp = _safe_float(energy.get("BTP", payload.get("BTP")))
    session_pct = _safe_float(payload.get("session_elapsed_pct", payload.get("session_pct")))
    space_above = _safe_float(payload.get("space_above"))
    space_below = _safe_float(payload.get("space_below"))
    cycle_count = int(_safe_float(payload.get("cycle_count"), 0.0))
    if "OFI_min" in energy_gates and ofi < _safe_float(energy_gates.get("OFI_min")):
        return False
    if "OFI_max" in energy_gates and ofi > _safe_float(energy_gates.get("OFI_max")):
        return False
    if "CES_min" in energy_gates and ces < _safe_float(energy_gates.get("CES_min")):
        return False
    if "RV_min" in energy_gates and rv < _safe_float(energy_gates.get("RV_min")):
        return False
    if "BTP_max" in energy_gates and btp > _safe_float(energy_gates.get("BTP_max")):
        return False
    if "session_pct_max" in energy_gates and session_pct > _safe_float(energy_gates.get("session_pct_max")):
        return False
    if "space_above_min" in energy_gates and space_above < _safe_float(energy_gates.get("space_above_min")):
        return False
    if "space_below_min" in energy_gates and space_below < _safe_float(energy_gates.get("space_below_min")):
        return False
    if "cycle_count_min" in energy_gates and cycle_count < int(_safe_float(energy_gates.get("cycle_count_min"), 0.0)):
        return False
    return True


def match_doctrine(payload: Dict[str, Any]) -> Tuple[str | None, str]:
    direction = _clean_text(payload.get("direction"))
    trigger_state = _clean_text(payload.get("trigger_state"))
    distance_mode = _clean_text(payload.get("distance_mode"))
    surface_type = _clean_text(payload.get("surface_type"))
    level_type = _clean_text(payload.get("level_type"))
    for doctrine_id, spec in DOCTRINE_REGISTRY.items():
        if _clean_text(spec.get("direction")) != direction:
            continue
        if _clean_text(spec.get("trigger")) != trigger_state:
            continue
        if _clean_text(spec.get("distance")) != distance_mode:
            continue
        if _clean_text(spec.get("surface")) == "LEVEL_EVENT":
            allowed_level_types = {_clean_text(value) for value in list(spec.get("energy_gates", {}).get("level_types", []) or [])}
            if level_type not in allowed_level_types:
                continue
        elif _clean_text(spec.get("surface")) != surface_type:
            continue
        if not _energy_gates_pass(payload, dict(spec.get("energy_gates", {}) or {})):
            continue
        return doctrine_id, "MATCHED"
    return None, "NO_DOCTRINE_MATCH"
