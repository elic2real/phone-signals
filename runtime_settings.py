#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_TEMPLATE_ROOT = ROOT / "compiled_session_templates"
DEFAULT_NODE_ROOT = ROOT / "compiled_market_nodes"

GRADE_SCORE = {"A": 1.0, "B": 0.8, "C": 0.6, "D": 0.3, "E": 0.0}
RISK_BY_GRADE = {"A": 3.0, "B": 2.0, "C": 1.5, "D": 0.5, "E": 0.0}
PAIR_PARENT_CAP = 5
SAME_PAIR_LINEAR_PENALTY = 0.12
SPLIT_BY_SPEED = {
    "FAST": {"harvester": 0.85, "runner": 0.15},
    "MED": {"harvester": 0.80, "runner": 0.20},
    "SLOW": {"harvester": 0.75, "runner": 0.25},
}
MAJOR_PAIRS = {
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD", "EUR_JPY",
}
PRIMARY_LIVE_PAIRS = {"AUD_USD", "EUR_JPY", "USD_JPY", "EUR_USD"}
FALLBACK_LIVE_PAIRS = {"GBP_JPY", "GBP_USD", "NZD_USD", "USD_CAD", "USD_CHF"}
DEFAULT_AEE_FALLBACK_PROFILE = {
    "strictness_mult": 1.15,
    "near_tp_band_atr": 0.18,
    "harvester_time_fraction": 0.35,
    "first_partial_fraction": 0.35,
    "first_partial_trigger_r": 1.0,
    "stop_time_fraction": 0.40,
    "add_on_enabled": False,
    "add_on_risk_percent": 1.0,
    "add_on_min_parent_r": 1.00,
    "add_on_min_continuation": 0.72,
}


def live_priority_tier(pair: str) -> str:
    norm = normalize_pair(pair)
    if norm in PRIMARY_LIVE_PAIRS:
        return "primary"
    if norm in FALLBACK_LIVE_PAIRS:
        return "fallback"
    return "neutral"


def live_priority_adjustment(pair: str) -> float:
    tier = live_priority_tier(pair)
    if tier == "primary":
        return 0.18
    if tier == "fallback":
        return -0.06
    return 0.0


def normalize_pair(pair: str) -> str:
    return str(pair or "").strip().upper()


def historical_grade(
    win_rate: float,
    trades_per_hour: float,
    utilization: float,
    recycling_utilization: float,
) -> str:
    wr = max(0.0, min(1.0, float(win_rate or 0.0)))
    tph = max(0.0, float(trades_per_hour or 0.0))
    util = max(0.0, float(utilization or 0.0))
    recycle = max(0.0, float(recycling_utilization or 0.0))
    if wr >= 0.60 and tph >= 0.60 and util >= 0.15 and recycle >= 0.20:
        return "A"
    if wr >= 0.53 and tph >= 0.20 and util >= 0.05:
        return "B"
    if wr >= 0.505 and tph >= 0.10:
        return "C"
    if wr > 0.0:
        return "D"
    return "E"


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _target_speed_class(target_distance: float) -> str:
    t = float(target_distance or 0.0)
    if t <= 2.5:
        return "FAST"
    if t <= 7.0:
        return "MED"
    return "SLOW"


def _quarter_direction_richness(zones: list[dict[str, Any]], quarter: str, direction: str) -> dict[str, Any]:
    relevant = [
        z for z in zones
        if str(z.get("quarter")) == str(quarter)
        and str(z.get("direction", "")).upper() == str(direction).upper()
    ]
    relevant.sort(key=lambda z: _safe_float(z.get("target_distance")))
    a_count = 0
    b_count = 0
    best_a_run = 0
    current_run = 0
    for zone in relevant:
        grade = str(zone.get("historical_grade", "C")).upper()
        if grade == "A":
            a_count += 1
            current_run += 1
            best_a_run = max(best_a_run, current_run)
        else:
            current_run = 0
            if grade == "B":
                b_count += 1
    richness_score = min(1.0, (0.45 * min(4, a_count) + 0.35 * min(3, best_a_run) + 0.20 * min(4, b_count)) / 3.0)
    return {
        "quarter": quarter,
        "direction": direction,
        "market_richness": round(richness_score, 4),
        "market_richness_reason": (
            f"q={quarter} dir={direction} a_count={a_count} "
            f"best_a_run={best_a_run} b_count={b_count}"
        ),
        "a_count": a_count,
        "b_count": b_count,
        "best_a_run": best_a_run,
    }


def _zone_key(quarter: str, direction: str, target_distance: float) -> tuple[str, str, float]:
    return (str(quarter), str(direction).upper(), round(float(target_distance or 0.0), 6))


def _aggregate_node_aee_evidence(pair: str, session: str, node_root: Path = DEFAULT_NODE_ROOT) -> dict[tuple[str, str, float], dict[str, float]]:
    evidence: dict[tuple[str, str, float], dict[str, float]] = {}
    pattern = f"{normalize_pair(pair)}__*__{str(session).lower()}"
    for node_dir in sorted(node_root.glob(pattern)):
        report_path = node_dir / "session_calibration" / "session_calibration_report.json"
        if not report_path.exists():
            continue
        try:
            report = json.loads(report_path.read_text())
        except Exception:
            continue
        if str(report.get("status")) != "PASS":
            continue
        for zone in report.get("zones", []) or []:
            key = _zone_key(zone.get("quarter"), zone.get("direction"), _safe_float(zone.get("target_distance")))
            bucket = evidence.setdefault(
                key,
                {
                    "samples": 0.0,
                    "downstream_positive_samples": 0.0,
                    "max_downstream_density_per_hour": 0.0,
                    "max_downstream_trade_count": 0.0,
                    "median_like_downstream_density_sum": 0.0,
                },
            )
            dens = _safe_float(zone.get("downstream_density_per_hour"))
            trades = _safe_float(zone.get("downstream_trade_count"))
            bucket["samples"] += 1.0
            bucket["median_like_downstream_density_sum"] += dens
            bucket["max_downstream_density_per_hour"] = max(bucket["max_downstream_density_per_hour"], dens)
            bucket["max_downstream_trade_count"] = max(bucket["max_downstream_trade_count"], trades)
            if dens > 0.0 or trades > 0.0:
                bucket["downstream_positive_samples"] += 1.0
    for bucket in evidence.values():
        samples = max(1.0, bucket["samples"])
        bucket["avg_downstream_density_per_hour"] = bucket["median_like_downstream_density_sum"] / samples
        bucket["full_aee_supported"] = 1.0 if bucket["downstream_positive_samples"] > 0.0 else 0.0
    return evidence


def _find_fallback_template_donor(
    zones: list[dict[str, Any]],
    target_zone: dict[str, Any],
) -> dict[str, Any] | None:
    quarter = str(target_zone.get("quarter"))
    direction = str(target_zone.get("direction", "")).upper()
    target_distance = _safe_float(target_zone.get("target_distance"))
    full_zones = [z for z in zones if str(z.get("aee_mode")) == "full"]
    if not full_zones:
        return None

    def dist(zone: dict[str, Any]) -> tuple[int, float]:
        zq = str(zone.get("quarter"))
        zd = str(zone.get("direction", "")).upper()
        zt = _safe_float(zone.get("target_distance"))
        if zq == quarter and zd == direction:
            return (0, abs(zt - target_distance))
        if zq == quarter:
            return (1, abs(zt - target_distance))
        if zd == direction:
            return (2, abs(zt - target_distance))
        return (3, abs(zt - target_distance))

    return min(full_zones, key=dist)


def _load_runtime_zones(template_root: Path, pair: str, session: str) -> list[dict[str, Any]]:
    path = runtime_settings_path(template_root, pair, session)
    if not path.exists():
        return []
    try:
        settings = json.loads(path.read_text())
    except Exception:
        settings = None
    if not isinstance(settings, dict) or settings.get("status") != "PASS":
        return []
    return list(settings.get("zones", []) or [])


def _find_external_fallback_template_donor(
    template_root: Path,
    pair: str,
    session: str,
    target_zone: dict[str, Any],
) -> tuple[dict[str, Any] | None, str]:
    quarter = str(target_zone.get("quarter"))
    direction = str(target_zone.get("direction", "")).upper()
    target_distance = _safe_float(target_zone.get("target_distance"))
    candidate_groups: list[tuple[str, list[tuple[str, str]]]] = [
        (
            "same_pair_other_session",
            [
                (pair, s)
                for s in ("london", "new_york", "asia", "sydney")
                if str(s).lower() != str(session).lower()
            ],
        ),
        (
            "same_session_primary_pairs",
            [
                (p, session)
                for p in sorted(PRIMARY_LIVE_PAIRS)
                if normalize_pair(p) != normalize_pair(pair)
            ],
        ),
        (
            "same_session_all_pairs",
            [
                (p, session)
                for p in sorted(PRIMARY_LIVE_PAIRS | FALLBACK_LIVE_PAIRS | {pair})
                if normalize_pair(p) != normalize_pair(pair)
            ],
        ),
    ]

    donor_candidates: list[tuple[tuple[int, int, float], dict[str, Any], str]] = []
    for source_group, refs in candidate_groups:
        for donor_pair, donor_session in refs:
            for zone in _load_runtime_zones(template_root, donor_pair, donor_session):
                if str(zone.get("aee_mode")) != "full":
                    continue
                zq = str(zone.get("quarter"))
                zd = str(zone.get("direction", "")).upper()
                zt = _safe_float(zone.get("target_distance"))
                if zq == quarter and zd == direction:
                    pri = (0, 0, abs(zt - target_distance))
                elif zq == quarter:
                    pri = (0, 1, abs(zt - target_distance))
                elif zd == direction:
                    pri = (0, 2, abs(zt - target_distance))
                else:
                    pri = (0, 3, abs(zt - target_distance))
                # prioritize group ordering before shape distance
                if source_group == "same_pair_other_session":
                    group_rank = 0
                elif source_group == "same_session_primary_pairs":
                    group_rank = 1
                else:
                    group_rank = 2
                donor_candidates.append(((group_rank, *pri[1:]), zone, f"{source_group}:{donor_pair}|{donor_session}"))

    if not donor_candidates:
        return None, "no_donor"
    donor_candidates.sort(key=lambda item: item[0])
    best_zone, best_source = donor_candidates[0][1], donor_candidates[0][2]
    return best_zone, best_source


def build_runtime_settings_from_template_report(
    template_report: dict[str, Any],
    template_root: Path = DEFAULT_TEMPLATE_ROOT,
) -> dict[str, Any]:
    pair = normalize_pair(template_report.get("pair"))
    session = str(template_report.get("session", "")).lower()
    zones = list(template_report.get("zones", []) or [])
    pair_template = dict(template_report.get("pair_template", {}) or {})
    node_evidence = _aggregate_node_aee_evidence(pair, session)

    zone_runtime = []
    for zone in zones:
        grade = historical_grade(
            _safe_float(zone.get("template_entry_win_rate")),
            _safe_float(zone.get("template_actual_trades_per_hour", zone.get("template_selected_density_per_hour"))),
            _safe_float(zone.get("template_utilization_ratio")),
            _safe_float(zone.get("template_recycling_utilization_ratio")),
        )
        target_distance = _safe_float(zone.get("target_distance"))
        speed_class = _target_speed_class(target_distance)
        base_split = SPLIT_BY_SPEED[speed_class]
        recycle = _safe_float(zone.get("template_recycling_utilization_ratio"))
        cont_adj = min(0.10, recycle * 0.10)
        runner_ratio = min(0.40, max(0.10, base_split["runner"] + cont_adj))
        harvester_ratio = round(1.0 - runner_ratio, 4)
        runner_ratio = round(runner_ratio, 4)
        zone_runtime.append(
            {
                **zone,
                "speed_class": speed_class,
                "historical_grade": grade,
                "historical_grade_score": GRADE_SCORE[grade],
                "aee_mode": (
                    "full"
                    if node_evidence.get(
                        _zone_key(zone.get("quarter"), zone.get("direction"), target_distance),
                        {},
                    ).get("full_aee_supported", 0.0) > 0.0
                    or _safe_float(zone.get("template_downstream_density_per_hour")) > 0.0
                    else "fallback"
                ),
                "node_evidence_downstream_density_per_hour": node_evidence.get(
                    _zone_key(zone.get("quarter"), zone.get("direction"), target_distance), {}
                ).get("avg_downstream_density_per_hour", 0.0),
                "node_evidence_downstream_positive_samples": int(
                    node_evidence.get(
                        _zone_key(zone.get("quarter"), zone.get("direction"), target_distance), {}
                    ).get("downstream_positive_samples", 0.0)
                ),
                "preferred_trade_type_mix": {
                    "harvester": harvester_ratio,
                    "runner": runner_ratio,
                },
                "preferred_partial_close_profile": {
                    "first_partial_fraction": 0.35 if speed_class == "FAST" else (0.45 if speed_class == "MED" else 0.55),
                    "first_partial_trigger_r": 1.0 if speed_class == "FAST" else (1.25 if speed_class == "MED" else 1.5),
                    "harvester_time_fraction": 0.35 if speed_class == "FAST" else (0.45 if speed_class == "MED" else 0.55),
                    "stop_time_fraction": 0.50,
                },
                "aee_improvable_rate": round(
                    min(
                        1.0,
                        max(
                            0.0,
                            _safe_float(zone.get("template_downstream_density_per_hour"))
                            - _safe_float(zone.get("template_actual_trades_per_hour"))
                        ),
                    ),
                    4,
                ),
            }
        )

    for zone in zone_runtime:
        if str(zone.get("aee_mode")) == "full":
            zone["fallback_template_source"] = "self"
            zone["fallback_profile"] = {}
            continue
        donor = _find_fallback_template_donor(zone_runtime, zone)
        donor_source = "same_pair_session"
        if donor is None:
            donor, donor_source = _find_external_fallback_template_donor(template_root, pair, session, zone)
        if donor is None:
            zone["fallback_template_source"] = "no_donor"
            zone["fallback_profile"] = {}
            continue
        zone["fallback_template_source"] = (
            f"{donor_source}:{donor.get('quarter')}|{donor.get('direction')}|{float(donor.get('target_distance', 0.0)):.2f}"
        )
        donor_partial = dict(donor.get("preferred_partial_close_profile", {}) or {})
        donor_mix = dict(donor.get("preferred_trade_type_mix", {}) or {})
        donor_speed = str(donor.get("speed_class") or "")
        donor_improvable = _safe_float(donor.get("aee_improvable_rate"))
        fallback_strictness = max(
            1.02,
            min(
                1.18,
                1.04 + max(0.0, 0.10 - donor_improvable) * 0.40,
            ),
        )
        fallback_near_tp = max(
            0.16,
            min(0.24, _safe_float(donor_partial.get("stop_time_fraction", 0.50)) * 0.42),
        )
        zone["fallback_profile"] = {
            "borrowed_speed_class": donor_speed,
            "borrowed_trade_type_mix": donor_mix,
            "strictness_mult": round(fallback_strictness, 4),
            "near_tp_band_atr": round(fallback_near_tp, 4),
            "harvester_time_fraction": float(donor_partial.get("harvester_time_fraction", 0.40) or 0.40),
            "first_partial_fraction": float(donor_partial.get("first_partial_fraction", 0.35) or 0.35),
            "first_partial_trigger_r": float(donor_partial.get("first_partial_trigger_r", 1.10) or 1.10),
            "stop_time_fraction": min(
                0.50,
                float(donor_partial.get("stop_time_fraction", 0.50) or 0.50),
            ),
            "add_on_enabled": False,
            "add_on_risk_percent": 1.0,
            "add_on_min_parent_r": 1.0,
            "add_on_min_continuation": 0.78,
        }

    qd_richness = []
    for quarter in sorted({str(z.get("quarter")) for z in zone_runtime}):
        for direction in ("LONG", "SHORT"):
            qd_richness.append(_quarter_direction_richness(zone_runtime, quarter, direction))

    grade_distribution = {g: 0 for g in GRADE_SCORE}
    for zone in zone_runtime:
        grade_distribution[str(zone.get("historical_grade", "C")).upper()] += 1

    pair_runtime: dict[str, Any] = {}
    for direction in ("LONG", "SHORT"):
        tpl = dict(pair_template.get(direction, {}) or {})
        grade = historical_grade(
            _safe_float(tpl.get("template_entry_win_rate")),
            _safe_float(tpl.get("template_trades_per_hour")),
            _safe_float(tpl.get("template_utilization_ratio")),
            _safe_float(tpl.get("template_recycling_utilization_ratio")),
        )
        pair_runtime[direction] = {
            **tpl,
            "historical_grade": grade,
            "historical_grade_score": GRADE_SCORE[grade],
        }

    avg_recycling = 0.0
    recycle_samples = [_safe_float(z.get("template_recycling_utilization_ratio")) for z in zone_runtime]
    if recycle_samples:
        avg_recycling = sum(recycle_samples) / max(1, len(recycle_samples))
    add_on_continuation = 0.60 + min(0.15, avg_recycling * 0.10)

    return {
        "status": "PASS",
        "pair": pair,
        "session": session,
        "major_pair": pair in MAJOR_PAIRS,
        "live_priority_tier": live_priority_tier(pair),
        "live_priority_adjustment": live_priority_adjustment(pair),
        "source_template_report": template_report.get("source_template_report"),
        "priority": {
            "same_pair_parent_cap": PAIR_PARENT_CAP,
            "same_pair_linear_penalty": SAME_PAIR_LINEAR_PENALTY,
            "major_pair_tie_break": True,
            "primary_pairs": sorted(PRIMARY_LIVE_PAIRS),
            "fallback_pairs": sorted(FALLBACK_LIVE_PAIRS),
            "primary_pair_bonus": 0.18,
            "fallback_pair_penalty": 0.06,
            "stale_candidate_seconds": 90,
            "score_weights": {
                "energy": 0.18,
                "efficiency": 0.15,
                "speed": 0.11,
                "volatility": 0.07,
                "path": 0.07,
                "move": 0.07,
                "spread": 0.05,
                "freshness": 0.02,
                "pair_template": 0.16,
                "zone_template": 0.12,
                "richness": 0.10,
            },
        },
        "sizing": {
            "risk_by_grade": RISK_BY_GRADE,
            "add_on_once_risk_percent": 1.0,
            "add_on_allowed_grades": ["A", "B"],
        },
        "trade_family": {
            "split_by_speed_class": SPLIT_BY_SPEED,
            "partial_close_profile": {
                "FAST": {"first_partial_fraction": 0.35, "first_partial_trigger_r": 1.0, "harvester_time_fraction": 0.35, "stop_time_fraction": 0.50},
                "MED": {"first_partial_fraction": 0.45, "first_partial_trigger_r": 1.25, "harvester_time_fraction": 0.45, "stop_time_fraction": 0.50},
                "SLOW": {"first_partial_fraction": 0.55, "first_partial_trigger_r": 1.5, "harvester_time_fraction": 0.55, "stop_time_fraction": 0.50},
            },
            "runner_success_profile": {
                "continuation_bias": round(avg_recycling, 4),
                "prefer_runner_on_targets_above": 4.5,
            },
            "harvester_reentry_profile": {
                "prefer_harvester_on_targets_at_or_below": 2.5,
                "expected_recycling_bias": round(avg_recycling, 4),
            },
            "add_on_profile": {
                "enabled": True,
                "add_on_risk_percent": 1.0,
                "allowed_grades": ["A", "B"],
                "max_add_ons_per_parent": 1,
                "min_parent_r_multiple": 0.75,
                "min_continuation_strength": round(add_on_continuation, 4),
                "entry_delay_fraction": 0.40,
            },
        },
        "aee": {
            "default_mode": "fallback",
            "fallback_profile": DEFAULT_AEE_FALLBACK_PROFILE,
            "full_profile": {
                "strictness_mult": 1.0,
                "near_tp_band_atr": 0.25,
                "harvester_time_fraction": 0.45,
                "first_partial_fraction": 0.45,
                "first_partial_trigger_r": 1.25,
                "stop_time_fraction": 0.50,
                "add_on_enabled": True,
                "add_on_risk_percent": 1.0,
                "add_on_min_parent_r": 0.75,
                "add_on_min_continuation": round(add_on_continuation, 4),
            },
        },
        "grade_distribution": grade_distribution,
        "pair_runtime": pair_runtime,
        "quarter_direction_richness": qd_richness,
        "zones": zone_runtime,
    }


def runtime_settings_path(template_root: Path, pair: str, session: str) -> Path:
    return template_root / f"{normalize_pair(pair).lower()}__{str(session).lower()}" / "runtime_settings.json"


def session_template_report_path(template_root: Path, pair: str, session: str) -> Path:
    return template_root / f"{normalize_pair(pair).lower()}__{str(session).lower()}" / "session_template_report.json"


def write_runtime_settings(
    template_report: dict[str, Any],
    template_root: Path = DEFAULT_TEMPLATE_ROOT,
) -> dict[str, Any]:
    settings = build_runtime_settings_from_template_report(template_report, template_root=template_root)
    path = runtime_settings_path(template_root, settings["pair"], settings["session"])
    path.parent.mkdir(parents=True, exist_ok=True)
    settings["source_template_report"] = str(path.parent / "session_template_report.json")
    path.write_text(json.dumps(settings, indent=2))
    return settings


def load_runtime_settings(pair: str, session: str, template_root: Path = DEFAULT_TEMPLATE_ROOT) -> dict[str, Any] | None:
    path = runtime_settings_path(template_root, pair, session)
    if not path.exists():
        tpl_path = session_template_report_path(template_root, pair, session)
        if not tpl_path.exists():
            return None
        try:
            tpl = json.loads(tpl_path.read_text())
        except Exception:
            return None
        if not isinstance(tpl, dict) or tpl.get("status") != "PASS":
            return None
        try:
            return write_runtime_settings({**tpl, "source_template_report": str(tpl_path)}, template_root=template_root)
        except Exception:
            return None
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    if not (isinstance(data, dict) and data.get("status") == "PASS"):
        return None
    if "aee" not in data or any(
        "aee_mode" not in dict(z or {}) or "fallback_profile" not in dict(z or {})
        for z in list(data.get("zones", []) or [])
    ):
        tpl_path = session_template_report_path(template_root, pair, session)
        if tpl_path.exists():
            try:
                tpl = json.loads(tpl_path.read_text())
                if isinstance(tpl, dict) and tpl.get("status") == "PASS":
                    return write_runtime_settings({**tpl, "source_template_report": str(tpl_path)}, template_root=template_root)
            except Exception:
                pass
    return data


def best_zone_runtime(settings: dict[str, Any] | None, quarter: str, direction: str, target_distance: float) -> dict[str, Any] | None:
    if not isinstance(settings, dict):
        return None
    best_zone = None
    best_gap = float("inf")
    for zone in settings.get("zones", []) or []:
        if str(zone.get("quarter")) != str(quarter):
            continue
        if str(zone.get("direction", "")).upper() != str(direction).upper():
            continue
        gap = abs(_safe_float(zone.get("target_distance")) - float(target_distance or 0.0))
        if gap < best_gap:
            best_gap = gap
            best_zone = zone
    return best_zone


def quarter_direction_richness(settings: dict[str, Any] | None, quarter: str, direction: str) -> dict[str, Any]:
    if not isinstance(settings, dict):
        return {"market_richness": 0.0, "market_richness_reason": "no_runtime_settings"}
    for item in settings.get("quarter_direction_richness", []) or []:
        if str(item.get("quarter")) == str(quarter) and str(item.get("direction", "")).upper() == str(direction).upper():
            return item
    return {"market_richness": 0.0, "market_richness_reason": f"q={quarter} dir={direction} no_match"}
