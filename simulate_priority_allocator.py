#!/usr/bin/env python3
import argparse
import csv
import glob
import heapq
import json
import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import runtime_settings as runtime_cfg


ROOT = Path(__file__).parent
PAIR_PARENT_CAP = 5
SAME_PAIR_LINEAR_PENALTY = 0.12
GRADE_RISK_PCT = {"A": 0.03, "B": 0.02, "C": 0.015, "D": 0.005, "E": 0.0}


def normalize_pair(pair: str) -> str:
    return str(pair or "").strip().upper()


def load_template(pair: str, session: str) -> dict | None:
    path = ROOT / "compiled_session_templates" / f"{normalize_pair(pair).lower()}__{session.lower()}" / "session_template_report.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return data if isinstance(data, dict) and data.get("status") == "PASS" else None


def load_settings(pair: str, session: str) -> dict | None:
    settings = runtime_cfg.load_runtime_settings(pair, session)
    if isinstance(settings, dict) and settings.get("status") == "PASS":
        return settings
    return load_template(pair, session)


def _float_key(value: object) -> str:
    try:
        return f"{float(value or 0.0):.6f}"
    except Exception:
        return "0.000000"


def _row_match_key(row: dict) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("timestamp", row.get("entry_time", ""))),
        str(row.get("session_id", "")),
        str(row.get("quarter", "")),
        str(row.get("direction_assumed", row.get("direction", ""))).upper(),
        _float_key(row.get("target_distance", 0.0)),
    )


def _parse_dt_safe(ts: object) -> datetime | None:
    raw = str(ts or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def candidate_aee_trade_paths(node_dir: Path) -> list[Path]:
    paths = [
        node_dir / "aee_stage" / "target_local_hotspot_merged" / "aee_target_local_hotspot_merged_trade_rows.json",
        node_dir / "aee_stage" / "target_local_aee" / "target_local_aee_trade_rows.json",
        node_dir / "aee_target_local_hotspot_merged" / "aee_target_local_hotspot_merged_trade_rows.json",
        node_dir / "aee_hotspot" / "aee_hotspot_trade_rows.json",
        node_dir / "aee_stage" / "aee_hotspot" / "aee_hotspot_trade_rows.json",
        node_dir / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json",
        node_dir / "aee_stage" / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json",
        # Fallbacks: some nodes only persist managed trade rows inside replay payloads.
        node_dir / "aee_stage" / "aee_replay" / "target_local_hotspot_merged_aee.json",
        node_dir / "aee_stage" / "aee_replay" / "target_local_aee.json",
        node_dir / "aee_stage" / "aee_replay" / "aee_hotspot.json",
        node_dir / "aee_stage" / "aee_replay" / "target_selective_aee.json",
        node_dir / "aee_stage" / "aee_replay" / "bias_plus_context_aee.json",
        node_dir / "aee_stage" / "aee_replay" / "bias_aware_aee.json",
        node_dir / "aee_stage" / "aee_replay" / "baseline_static.json",
    ]
    return paths


def _extract_trade_rows(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        rows = payload.get("trade_rows")
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def load_aee_overlay(node_dir: Path) -> tuple[Path | None, dict[tuple[str, str, str, str, str], dict], list[dict]]:
    for path in candidate_aee_trade_paths(node_dir):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        rows = _extract_trade_rows(payload)
        if not rows:
            continue
        idx: dict[tuple[str, str, str, str, str], dict] = {}
        for row in rows:
            key = _row_match_key(row)
            idx[key] = row
        if idx:
            return path, idx, rows
    return None, {}, []


def load_static_population_index(node_dir: Path) -> dict[tuple[str, str, str, str, str], dict]:
    path = node_dir / "target_entry_no_timeouts" / "target_entry_population.csv"
    if not path.exists():
        return {}
    idx: dict[tuple[str, str, str, str, str], dict] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            rec = dict(row)
            idx[_row_match_key(rec)] = rec
    return idx


def historical_grade(win_rate: float, trades_per_hour: float, utilization: float, recycling_utilization: float) -> str:
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


def grade_bonus(grade: str) -> float:
    grade = str(grade or "").upper()
    if grade == "A":
        return 0.10
    if grade == "B":
        return 0.04
    if grade == "C":
        return -0.03
    if grade == "D":
        return -0.10
    return -0.20


def zone_grade(zone: dict | None) -> str:
    if not isinstance(zone, dict):
        return "C"
    return historical_grade(
        float(zone.get("template_entry_win_rate", 0.0) or 0.0),
        float(zone.get("template_actual_trades_per_hour", zone.get("template_selected_density_per_hour", 0.0)) or 0.0),
        float(zone.get("template_utilization_ratio", 0.0) or 0.0),
        float(zone.get("template_recycling_utilization_ratio", 0.0) or 0.0),
    )


def pair_grade(pair_tpl: dict | None) -> str:
    if not isinstance(pair_tpl, dict):
        return "C"
    return historical_grade(
        float(pair_tpl.get("template_entry_win_rate", 0.0) or 0.0),
        float(pair_tpl.get("template_trades_per_hour", 0.0) or 0.0),
        float(pair_tpl.get("template_utilization_ratio", 0.0) or 0.0),
        float(pair_tpl.get("template_recycling_utilization_ratio", 0.0) or 0.0),
    )


def template_richness(template: dict | None, quarter: str, direction: str) -> dict:
    if not isinstance(template, dict):
        return {"richness_score": 0.0, "a_count": 0, "best_a_run": 0}
    zones = [
        z for z in (template.get("zones") or [])
        if str(z.get("quarter")) == str(quarter)
        and str(z.get("direction", "")).upper() == str(direction).upper()
    ]
    zones.sort(key=lambda z: float(z.get("target_distance", 0.0) or 0.0))
    a_count = 0
    best_run = 0
    cur_run = 0
    b_count = 0
    for z in zones:
        g = zone_grade(z)
        if g == "A":
            a_count += 1
            cur_run += 1
            best_run = max(best_run, cur_run)
        else:
            cur_run = 0
            if g == "B":
                b_count += 1
    richness_score = min(1.0, (0.45 * min(4, a_count) + 0.35 * min(3, best_run) + 0.20 * min(4, b_count)) / 3.0)
    return {"richness_score": richness_score, "a_count": a_count, "best_a_run": best_run}


def score_row(row: dict, settings: dict | None) -> tuple[float, str]:
    session = row["session"]
    quarter = row["quarter"]
    direction = row["direction_assumed"]
    target = float(row["target_distance"])

    energy_score = max(0.0, min(1.0, float(row.get("release_quality_score", 0.0) or 0.0)))
    eff_score = max(0.0, min(1.0, float(row.get("macro_dir_score", 0.0) or 0.0)))
    speed_score = max(0.0, min(1.0, abs(float(row.get("velocity_now", 0.0) or 0.0)) / 3.0))
    vol_score = max(0.0, min(1.0, float(row.get("recent_vol_10", 0.0) or 0.0) / 2.0))
    path_score = max(0.0, min(1.0, float(row.get("remaining_budget_score", 0.0) or 0.0)))
    move_score = max(0.0, min(1.0, target / 2.5))

    template_pair_score = 0.0
    template_zone_score = 0.0
    template_action_bonus = 0.0
    p_grade = "C"
    z_grade = "C"
    richness = {"richness_score": 0.0}
    if isinstance(settings, dict):
        if "pair_runtime" in settings:
            pair_tpl = dict((settings.get("pair_runtime") or {}).get(str(direction).upper(), {}) or {})
            p_grade = str(pair_tpl.get("historical_grade", "C") or "C").upper()
            template_pair_score = (
                0.45 * float(pair_tpl.get("template_entry_win_rate", 0.0) or 0.0)
                + 0.20 * min(1.0, float(pair_tpl.get("template_trades_per_hour", 0.0) or 0.0) / 4.0)
                + 0.20 * min(1.0, float(pair_tpl.get("template_utilization_ratio", 0.0) or 0.0))
                + 0.15 * min(1.0, float(pair_tpl.get("template_recycling_utilization_ratio", 0.0) or 0.0))
            )
            best_zone = runtime_cfg.best_zone_runtime(settings, quarter, direction, target)
            richness = runtime_cfg.quarter_direction_richness(settings, quarter, direction)
            if best_zone is not None:
                z_grade = str(best_zone.get("historical_grade", "C") or "C").upper()
                template_zone_score = (
                    0.35 * float(best_zone.get("template_entry_win_rate", 0.0) or 0.0)
                    + 0.20 * min(1.0, float(best_zone.get("template_utilization_ratio", 0.0) or 0.0))
                    + 0.15 * min(1.0, float(best_zone.get("template_recycling_utilization_ratio", 0.0) or 0.0))
                    + 0.15 * min(1.0, float(best_zone.get("template_selected_density_per_hour", 0.0) or 0.0) / 2.0)
                    + 0.15 * min(1.0, float(best_zone.get("template_expected_opportunities_per_hour", 0.0) or 0.0) / 2.0)
                )
                act = str(best_zone.get("preferred_action", "")).lower()
                if act == "expand":
                    template_action_bonus = 0.12
                elif act == "refine":
                    template_action_bonus = 0.06
                elif act == "repair":
                    template_action_bonus = -0.04
        else:
            pair_tpl = (settings.get("pair_template") or {}).get(str(direction).upper(), {})
            p_grade = pair_grade(pair_tpl)
            template_pair_score = (
                0.45 * float(pair_tpl.get("template_entry_win_rate", 0.0) or 0.0)
                + 0.20 * min(1.0, float(pair_tpl.get("template_trades_per_hour", 0.0) or 0.0) / 4.0)
                + 0.20 * min(1.0, float(pair_tpl.get("template_utilization_ratio", 0.0) or 0.0))
                + 0.15 * min(1.0, float(pair_tpl.get("template_recycling_utilization_ratio", 0.0) or 0.0))
            )
            best_zone = None
            best_gap = float("inf")
            for zone in settings.get("zones", []):
                if str(zone.get("quarter")) != quarter:
                    continue
                if str(zone.get("direction", "")).upper() != str(direction).upper():
                    continue
                gap = abs(float(zone.get("target_distance", 0.0) or 0.0) - target)
                if gap < best_gap:
                    best_gap = gap
                    best_zone = zone
            richness = template_richness(settings, quarter, direction)
            if best_zone is not None:
                z_grade = zone_grade(best_zone)
                template_zone_score = (
                    0.35 * float(best_zone.get("template_entry_win_rate", 0.0) or 0.0)
                    + 0.20 * min(1.0, float(best_zone.get("template_utilization_ratio", 0.0) or 0.0))
                    + 0.15 * min(1.0, float(best_zone.get("template_recycling_utilization_ratio", 0.0) or 0.0))
                    + 0.15 * min(1.0, float(best_zone.get("template_selected_density_per_hour", 0.0) or 0.0) / 2.0)
                    + 0.15 * min(1.0, float(best_zone.get("template_expected_opportunities_per_hour", 0.0) or 0.0) / 2.0)
                )
                act = str(best_zone.get("preferred_action", "")).lower()
                if act == "expand":
                    template_action_bonus = 0.12
                elif act == "refine":
                    template_action_bonus = 0.06
                elif act == "repair":
                    template_action_bonus = -0.04
    hist_grade = z_grade if z_grade != "C" else p_grade
    score = (
        0.18 * energy_score
        + 0.15 * eff_score
        + 0.11 * speed_score
        + 0.07 * vol_score
        + 0.07 * path_score
        + 0.07 * move_score
        + 0.16 * template_pair_score
        + 0.12 * template_zone_score
        + template_action_bonus
        + 0.55 * grade_bonus(z_grade)
        + 0.45 * grade_bonus(p_grade)
        + 0.10 * float(richness.get("richness_score", 0.0) or 0.0)
    )
    return score, hist_grade


def speed_class_from_target(target_distance: float) -> str:
    target = float(target_distance or 0.0)
    if target <= 2.5:
        return "FAST"
    if target <= 7.0:
        return "MED"
    return "SLOW"


def family_profile(row: dict) -> dict:
    settings = row.get("_runtime_settings")
    zone = row.get("_runtime_zone")
    speed_class = str((zone or {}).get("speed_class") or speed_class_from_target(float(row.get("target_distance", 0.0) or 0.0))).upper()
    family_cfg = dict((settings or {}).get("trade_family", {}) or {})
    split_cfg = dict((zone or {}).get("preferred_trade_type_mix", {}) or {})
    if not split_cfg:
        split_cfg = dict((family_cfg.get("split_by_speed_class", {}) or {}).get(speed_class, {}) or {})
    harvester_ratio = float(split_cfg.get("harvester", 0.80) or 0.80)
    runner_ratio = float(split_cfg.get("runner", 0.20) or 0.20)
    total_ratio = max(1e-9, harvester_ratio + runner_ratio)
    harvester_ratio /= total_ratio
    runner_ratio /= total_ratio
    partial_cfg = dict((zone or {}).get("preferred_partial_close_profile", {}) or {})
    if not partial_cfg:
        partial_cfg = dict((family_cfg.get("partial_close_profile", {}) or {}).get(speed_class, {}) or {})
    add_on_cfg = dict(family_cfg.get("add_on_profile", {}) or {})
    return {
        "speed_class": speed_class,
        "harvester_ratio": harvester_ratio,
        "runner_ratio": runner_ratio,
        "harvester_time_fraction": float(partial_cfg.get("harvester_time_fraction", 0.45) or 0.45),
        "first_partial_fraction": float(partial_cfg.get("first_partial_fraction", 0.45) or 0.45),
        "first_partial_trigger_r": float(partial_cfg.get("first_partial_trigger_r", 1.25) or 1.25),
        "stop_time_fraction": float(partial_cfg.get("stop_time_fraction", 0.50) or 0.50),
        "add_on_enabled": bool(add_on_cfg.get("enabled", False)),
        "add_on_risk_percent": float(add_on_cfg.get("add_on_risk_percent", 1.0) or 1.0),
        "add_on_allowed_grades": list(add_on_cfg.get("allowed_grades", ["A", "B"]) or ["A", "B"]),
        "add_on_max_per_parent": int(add_on_cfg.get("max_add_ons_per_parent", 1) or 1),
        "add_on_min_parent_r": float(add_on_cfg.get("min_parent_r_multiple", 0.75) or 0.75),
        "add_on_min_continuation": float(add_on_cfg.get("min_continuation_strength", 0.60) or 0.60),
        "add_on_entry_delay_fraction": float(add_on_cfg.get("entry_delay_fraction", 0.40) or 0.40),
    }


def continuation_strength(row: dict, zone: dict | None) -> float:
    zone_improve = 0.0
    if isinstance(zone, dict):
        zone_improve = float(zone.get("aee_improvable_rate", 0.0) or 0.0)
    return max(
        0.0,
        min(
            1.0,
            (
                float(row.get("release_quality_score", 0.0) or 0.0) * 0.35
                + float(row.get("remaining_budget_score", 0.0) or 0.0) * 0.25
                + min(1.0, abs(float(row.get("velocity_now", 0.0) or 0.0)) / 3.0) * 0.20
                + float(row.get("macro_dir_score", 0.0) or 0.0) * 0.10
                + min(1.0, zone_improve) * 0.10
            ),
        )
    )


def risk_percent_for_grade(row: dict) -> float:
    settings = row.get("_runtime_settings")
    grade = str(row.get("historical_grade", "C")).upper()
    if isinstance(settings, dict):
        sizing_cfg = dict(settings.get("sizing", {}) or {})
        try:
            return float(dict(sizing_cfg.get("risk_by_grade", {}) or {}).get(grade, GRADE_RISK_PCT.get(grade, 0.015))) / 100.0
        except Exception:
            return GRADE_RISK_PCT.get(grade, 0.015)
    return GRADE_RISK_PCT.get(grade, 0.015)


def iter_rows(days: list[str], use_aee: bool = False, pairs: list[str] | None = None) -> list[dict]:
    rows = []
    allowed_pairs = {normalize_pair(p) for p in (pairs or []) if str(p).strip()}
    pat = "compiled_market_nodes/*__{}__*/target_entry_no_timeouts/target_entry_population.csv"
    for day in days:
        for path in glob.glob(str(ROOT / pat.format(day))):
            pop_path = Path(path)
            node_dir = pop_path.parent.parent
            node = node_dir.name
            pair, weekday, session = node.split("__")
            if allowed_pairs and normalize_pair(pair) not in allowed_pairs:
                continue
            source_path = pop_path
            aee_path = None
            aee_idx: dict[tuple[str, str, str, str, str], dict] = {}
            aee_rows: list[dict] = []
            if use_aee:
                aee_path, aee_idx, aee_rows = load_aee_overlay(node_dir)
                aee_selected = node_dir / "aee_stage" / "aee_state_stream" / "selected_entry_population.csv"
                if aee_rows:
                    source_path = aee_path or pop_path
                elif aee_selected.exists():
                    source_path = aee_selected
            if use_aee and aee_rows:
                source_iter = [dict(r) for r in aee_rows]
            else:
                with source_path.open(newline="") as f:
                    source_iter = [dict(r) for r in csv.DictReader(f)]
            for rec in source_iter:
                    if not rec.get("timestamp") and rec.get("entry_time"):
                        rec["timestamp"] = rec.get("entry_time")
                    if not rec.get("direction_assumed") and rec.get("direction"):
                        rec["direction_assumed"] = rec.get("direction")
                    rec["pair"] = pair
                    rec["weekday"] = weekday
                    rec["session"] = session
                    rec["node"] = node
                    rec["source_mode"] = "aee" if use_aee and source_path != pop_path else "static"
                    rec["aee_trade_rows_path"] = str(aee_path) if aee_path else ""
                    if use_aee and aee_rows:
                        rec["managed_pips"] = rec.get("aee_pips", rec.get("static_pips", 0.0))
                        rec["managed_R"] = rec.get("aee_R", rec.get("static_R", 0.0))
                        rec["managed_reason"] = rec.get("aee_reason", "")
                        rec["managed_action"] = rec.get("first_aee_action", "")
                        rec["managed_source"] = "aee_trade_rows_source"
                        rec["managed_minutes"] = _resolve_aee_minutes(rec)
                    elif use_aee and source_path != pop_path and aee_idx:
                        key = _row_match_key(rec)
                        aee_row = aee_idx.get(key)
                        if aee_row:
                            rec["managed_pips"] = aee_row.get("aee_pips", rec.get("static_pips", 0.0))
                            rec["managed_R"] = aee_row.get("aee_R", rec.get("static_R", 0.0))
                            rec["managed_reason"] = aee_row.get("aee_reason", "")
                            rec["managed_action"] = aee_row.get("first_aee_action", "")
                            rec["managed_source"] = "aee_trade_rows"
                            rec["managed_minutes"] = _resolve_aee_minutes(aee_row)
                        else:
                            rec["managed_source"] = "static_fallback_missing_aee_row"
                    rows.append(rec)
    return rows


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)


def resolve_minutes(row: dict) -> int:
    managed_minutes = row.get("managed_minutes")
    try:
        mv = int(float(managed_minutes or 0) or 0)
        if mv > 0:
            return mv
    except Exception:
        pass
    vals = []
    for key in ("tp_hit_min", "sl_hit_min"):
        try:
            v = int(float(row.get(key, 0) or 0))
            if v > 0:
                vals.append(v)
        except Exception:
            pass
    return min(vals) if vals else 60


def _resolve_aee_minutes(aee_row: dict) -> int:
    entry_dt = _parse_dt_safe(aee_row.get("entry_time"))
    action_dt = _parse_dt_safe(aee_row.get("action_timestamp"))
    if entry_dt and action_dt:
        delta_min = int((action_dt - entry_dt).total_seconds() // 60)
        if 0 < delta_min <= 24 * 60:
            return delta_min
    return 0


def _parent_count(open_trades: list[dict], pair: str) -> int:
    pair = normalize_pair(pair)
    return sum(1 for tr in open_trades if normalize_pair(tr["pair"]) == pair)


def _has_opposite_parent(open_trades: list[dict], pair: str, direction: str) -> bool:
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    opp = "SHORT" if direction == "LONG" else "LONG"
    return any(normalize_pair(tr["pair"]) == pair and str(tr["direction"]).upper() == opp for tr in open_trades)


def _active_parent_count(families: dict[str, dict], pair: str) -> int:
    pair = normalize_pair(pair)
    return sum(1 for fam in families.values() if fam.get("active") and normalize_pair(fam.get("pair")) == pair)


def _find_opposite_family(families: dict[str, dict], pair: str, direction: str) -> dict | None:
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    opp = "SHORT" if direction == "LONG" else "LONG"
    for fam in families.values():
        if not fam.get("active"):
            continue
        if normalize_pair(fam.get("pair")) != pair:
            continue
        if str(fam.get("direction", "")).upper() == opp:
            return fam
    return None


def _leg_prorated_value(leg: dict, event_ts: datetime) -> tuple[float, float]:
    planned = max(1.0, float(leg.get("planned_minutes", 1.0) or 1.0))
    opened = leg["open_ts"]
    elapsed = max(0.0, min(planned, (event_ts - opened).total_seconds() / 60.0))
    frac = max(0.0, min(1.0, elapsed / planned))
    current_r = float(leg.get("target_r", 0.0) or 0.0) * frac
    current_pips = float(leg.get("target_pips", 0.0) or 0.0) * frac
    return current_r, current_pips


def _close_leg(family: dict, leg: dict, event_ts: datetime, *, forced: bool = False) -> tuple[float, float]:
    if forced:
        realized_r, realized_pips = _leg_prorated_value(leg, event_ts)
    else:
        realized_r = float(leg.get("target_r", 0.0) or 0.0)
        realized_pips = float(leg.get("target_pips", 0.0) or 0.0)
    pnl = float(leg.get("risk_amount", 0.0) or 0.0) * realized_r
    family["realized_pnl"] = float(family.get("realized_pnl", 0.0) or 0.0) + pnl
    family["realized_pips"] = float(family.get("realized_pips", 0.0) or 0.0) + realized_pips
    family["closed_leg_count"] = int(family.get("closed_leg_count", 0) or 0) + 1
    family["open_legs"] = [l for l in family.get("open_legs", []) if l is not leg]
    if not family.get("open_legs") and not family.get("pending_add_on"):
        family["active"] = False
    return pnl, realized_pips


def _harvester_target_r(total_r: float, profile: dict) -> float:
    total_r = float(total_r or 0.0)
    trigger = float(profile.get("first_partial_trigger_r", 1.25) or 1.25)
    if total_r > 0:
        return min(total_r, max(0.60, trigger * 0.80))
    return max(total_r, -0.75)


def _pips_from_r(total_pips: float, total_r: float, leg_r: float) -> float:
    total_pips = float(total_pips or 0.0)
    total_r = float(total_r or 0.0)
    leg_r = float(leg_r or 0.0)
    if abs(total_r) < 1e-9:
        return 0.0
    return total_pips * (leg_r / total_r)


def _build_family_legs(
    row: dict,
    ts: datetime,
    balance: float,
    family_id: str,
    *,
    pips_field: str,
    r_field: str,
) -> tuple[dict, list[dict], dict | None]:
    profile = family_profile(row)
    total_minutes = max(1, resolve_minutes(row))
    total_r = float(row.get(r_field, 0.0) or 0.0)
    total_pips = float(row.get(pips_field, 0.0) or 0.0)
    risk_pct = risk_percent_for_grade(row)
    parent_risk_amount = float(balance) * risk_pct

    harvester_ratio = float(profile.get("harvester_ratio", 0.80) or 0.80)
    runner_ratio = float(profile.get("runner_ratio", 0.20) or 0.20)
    harvest_risk = parent_risk_amount * harvester_ratio
    runner_risk = parent_risk_amount * runner_ratio

    harvest_minutes = max(1, int(round(total_minutes * float(profile.get("harvester_time_fraction", 0.45) or 0.45))))
    partial_minutes = max(
        harvest_minutes,
        int(round(total_minutes * max(
            float(profile.get("harvester_time_fraction", 0.45) or 0.45),
            float(profile.get("stop_time_fraction", 0.50) or 0.50),
        ))),
    )
    partial_minutes = min(partial_minutes, total_minutes)

    harvest_r = _harvester_target_r(total_r, profile)
    harvest_pips = _pips_from_r(total_pips, total_r, harvest_r)

    legs: list[dict] = [
        {
            "trade_type": "HARVESTER_PARENT",
            "open_ts": ts,
            "close_ts": ts + timedelta(minutes=harvest_minutes),
            "planned_minutes": harvest_minutes,
            "risk_amount": harvest_risk,
            "target_r": harvest_r,
            "target_pips": harvest_pips,
        }
    ]

    partial_frac = max(0.0, min(1.0, float(profile.get("first_partial_fraction", 0.45) or 0.45)))
    partial_trigger = float(profile.get("first_partial_trigger_r", 1.25) or 1.25)
    partial_hits = total_r > 0 and total_r >= partial_trigger and partial_frac > 0.0 and runner_risk > 0.0
    if partial_hits:
        partial_r = min(total_r, partial_trigger)
        partial_pips = _pips_from_r(total_pips, total_r, partial_r)
        legs.append(
            {
                "trade_type": "RUNNER_PARENT_PARTIAL",
                "open_ts": ts,
                "close_ts": ts + timedelta(minutes=partial_minutes),
                "planned_minutes": partial_minutes,
                "risk_amount": runner_risk * partial_frac,
                "target_r": partial_r,
                "target_pips": partial_pips,
            }
        )
        legs.append(
            {
                "trade_type": "RUNNER_PARENT_CORE",
                "open_ts": ts,
                "close_ts": ts + timedelta(minutes=total_minutes),
                "planned_minutes": total_minutes,
                "risk_amount": runner_risk * (1.0 - partial_frac),
                "target_r": total_r,
                "target_pips": total_pips,
            }
        )
    else:
        legs.append(
            {
                "trade_type": "RUNNER_PARENT",
                "open_ts": ts,
                "close_ts": ts + timedelta(minutes=total_minutes),
                "planned_minutes": total_minutes,
                "risk_amount": runner_risk,
                "target_r": total_r,
                "target_pips": total_pips,
            }
        )

    add_on_event = None
    add_on_enabled = bool(profile.get("add_on_enabled", False))
    allowed_grades = {str(g).upper() for g in profile.get("add_on_allowed_grades", [])}
    grade = str(row.get("historical_grade", "C")).upper()
    zone = row.get("_runtime_zone")
    cont_strength = continuation_strength(row, zone)
    if (
        add_on_enabled
        and grade in allowed_grades
        and float(total_r or 0.0) >= float(profile.get("add_on_min_parent_r", 0.75) or 0.75)
        and cont_strength >= float(profile.get("add_on_min_continuation", 0.60) or 0.60)
        and int(profile.get("add_on_max_per_parent", 1) or 1) > 0
    ):
        add_minutes = max(1, int(round(total_minutes * float(profile.get("add_on_entry_delay_fraction", 0.40) or 0.40))))
        remaining = max(1, total_minutes - add_minutes)
        remaining_frac = max(0.25, min(1.0, remaining / max(1, total_minutes)))
        child_r = max(0.10, total_r * remaining_frac * max(0.50, cont_strength))
        child_pips = _pips_from_r(total_pips, total_r if abs(total_r) > 1e-9 else 1.0, child_r)
        add_on_event = {
            "parent_id": family_id,
            "open_ts": ts + timedelta(minutes=add_minutes),
            "planned_minutes": remaining,
            "target_r": child_r,
            "target_pips": child_pips,
            "risk_percent": float(profile.get("add_on_risk_percent", 1.0) or 1.0) / 100.0,
        }

    family = {
        "parent_id": family_id,
        "pair": normalize_pair(row["pair"]),
        "direction": str(row["direction_assumed"]).upper(),
        "entry_ts": ts,
        "total_minutes": total_minutes,
        "active": True,
        "pending_add_on": add_on_event,
        "add_on_opened": False,
        "realized_pnl": 0.0,
        "realized_pips": 0.0,
        "closed_leg_count": 0,
        "open_legs": [],
        "_event_seq": 0,
        "row": row,
    }
    return family, legs, add_on_event


def _process_family_events(
    families: dict[str, dict],
    event_queue: list[tuple[datetime, int, str, str, object]],
    ts: datetime,
    balance: float,
    *,
    total_pips_ref: list[float],
    selected_trade_objects_ref: list[int],
    opposite_replacements_ref: list[int],
) -> float:
    while event_queue and event_queue[0][0] <= ts:
        event_ts, _, family_id, event_type, payload = heapq.heappop(event_queue)
        fam = families.get(family_id)
        if not fam or not fam.get("active"):
            continue
        if event_type == "add_on":
            add_on = fam.get("pending_add_on")
            if not add_on or add_on is not payload:
                continue
            risk_amount = balance * float(add_on.get("risk_percent", 0.0) or 0.0)
            if risk_amount > 0.0:
                leg = {
                    "trade_type": "ADD_ON_CHILD",
                    "open_ts": add_on["open_ts"],
                    "close_ts": add_on["open_ts"] + timedelta(minutes=int(add_on.get("planned_minutes", 1) or 1)),
                    "planned_minutes": int(add_on.get("planned_minutes", 1) or 1),
                    "risk_amount": risk_amount,
                    "target_r": float(add_on.get("target_r", 0.0) or 0.0),
                    "target_pips": float(add_on.get("target_pips", 0.0) or 0.0),
                }
                fam["open_legs"].append(leg)
                fam["add_on_opened"] = True
                selected_trade_objects_ref[0] += 1
                fam["_event_seq"] += 1
                heapq.heappush(event_queue, (leg["close_ts"], fam["_event_seq"], family_id, "close_leg", leg))
            fam["pending_add_on"] = None
            continue
        leg = payload
        if leg not in fam.get("open_legs", []):
            continue
        pnl, pips = _close_leg(fam, leg, event_ts, forced=False)
        balance += pnl
        total_pips_ref[0] += pips
    return balance


def _family_profit_proxy(family: dict, ts: datetime) -> float:
    profit = float(family.get("realized_pnl", 0.0) or 0.0)
    for leg in family.get("open_legs", []):
        r_now, _ = _leg_prorated_value(leg, ts)
        profit += float(leg.get("risk_amount", 0.0) or 0.0) * r_now
    return profit


def _force_close_family(
    family: dict,
    ts: datetime,
    balance: float,
    *,
    total_pips_ref: list[float],
) -> float:
    for leg in list(family.get("open_legs", [])):
        pnl, pips = _close_leg(family, leg, ts, forced=True)
        balance += pnl
        total_pips_ref[0] += pips
    family["pending_add_on"] = None
    family["active"] = False
    return balance


def simulate_strategy(
    buckets: dict,
    cap: int,
    starting_balance: float,
    priority: bool,
    *,
    pips_field: str = "managed_pips_f",
    r_field: str = "managed_R_f",
) -> dict:
    balance = float(starting_balance)
    families: dict[str, dict] = {}
    total_pips = [0.0]
    selected_parent_entries = 0
    selected_trade_objects = [0]
    skipped_pair_cap = 0
    crowding_penalized = 0
    skipped_opposite_conflict = 0
    opposite_replacements = [0]
    family_seq = 0
    active_parent_observations = 0.0
    active_parent_samples = 0
    event_queue: list[tuple[datetime, int, str, str, object]] = []

    ordered_bucket_keys = sorted(buckets.keys(), key=lambda k: k[1])
    for bucket_key in ordered_bucket_keys:
        ts = parse_ts(bucket_key[1])
        balance = _process_family_events(
            families,
            event_queue,
            ts,
            balance,
            total_pips_ref=total_pips,
            selected_trade_objects_ref=selected_trade_objects,
            opposite_replacements_ref=opposite_replacements,
        )

        group = buckets[bucket_key]
        scored_group = []
        for row in group:
            rec = dict(row)
            active = _active_parent_count(families, rec["pair"])
            penalty = min(active, 4) * SAME_PAIR_LINEAR_PENALTY
            rec["active_parent_count_for_pair"] = active
            rec["same_pair_penalty"] = penalty
            rec["priority_score_before_pair_penalty"] = float(rec["priority_score"])
            rec["priority_score_after_pair_penalty"] = float(rec["priority_score"]) - penalty
            scored_group.append(rec)
        ordered = sorted(
            scored_group,
            key=(lambda r: (float(r["priority_score_after_pair_penalty"]), float(r["target_distance"]))),
            reverse=True,
        ) if priority else sorted(
            scored_group,
            key=lambda r: (normalize_pair(r["pair"]), r["direction_assumed"], float(r["target_distance"]))
        )

        taken_this_bucket = 0
        for row in ordered:
            if taken_this_bucket >= cap:
                break
            pair = normalize_pair(row["pair"])
            direction = str(row["direction_assumed"]).upper()
            active = _active_parent_count(families, pair)
            active_parent_observations += float(active)
            active_parent_samples += 1
            if active >= PAIR_PARENT_CAP:
                skipped_pair_cap += 1
                continue
            if priority and active > 0:
                crowding_penalized += 1
            opposing = _find_opposite_family(families, pair, direction)
            if opposing is not None:
                if _family_profit_proxy(opposing, ts) > 0.0:
                    balance = _force_close_family(opposing, ts, balance, total_pips_ref=total_pips)
                    opposite_replacements[0] += 1
                else:
                    skipped_opposite_conflict += 1
                    continue
            risk_pct = risk_percent_for_grade(row)
            if risk_pct <= 0.0:
                continue
            family_id = f"{pair}:{int(ts.timestamp() * 1000)}:{family_seq}"
            family_seq += 1
            family, legs, _ = _build_family_legs(
                row,
                ts,
                balance,
                family_id,
                pips_field=pips_field,
                r_field=r_field,
            )
            for leg in legs:
                family["open_legs"].append(leg)
                family["_event_seq"] += 1
                heapq.heappush(event_queue, (leg["close_ts"], family["_event_seq"], family_id, "close_leg", leg))
            if family.get("pending_add_on"):
                family["_event_seq"] += 1
                heapq.heappush(event_queue, (family["pending_add_on"]["open_ts"], family["_event_seq"], family_id, "add_on", family["pending_add_on"]))
            families[family_id] = family
            selected_parent_entries += 1
            selected_trade_objects[0] += len(legs)
            taken_this_bucket += 1

    final_ts = None
    if ordered_bucket_keys:
        final_ts = parse_ts(ordered_bucket_keys[-1][1]) + timedelta(days=10)
        balance = _process_family_events(
            families,
            event_queue,
            final_ts,
            balance,
            total_pips_ref=total_pips,
            selected_trade_objects_ref=selected_trade_objects,
            opposite_replacements_ref=opposite_replacements,
        )

    return {
        "selected": selected_parent_entries,
        "selected_parent_entries": selected_parent_entries,
        "selected_trade_objects": selected_trade_objects[0],
        "total_pips": round(total_pips[0], 2),
        "final_balance": round(balance, 2),
        "skipped_pair_parent_cap": skipped_pair_cap,
        "pair_crowding_penalized_candidates": crowding_penalized,
        "skipped_opposite_side_conflict": skipped_opposite_conflict,
        "opposite_side_replacements": opposite_replacements[0],
        "avg_active_parent_count": round(
            active_parent_observations / max(1, active_parent_samples),
            3,
        ),
    }


def simulate(rows: list[dict], cap: int, starting_balance: float) -> dict:
    settings_cache: dict[tuple[str, str], dict | None] = {}
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        key = (row["weekday"], row["timestamp"])
        buckets[key].append(row)

    for key, group in buckets.items():
        scored = []
        for row in group:
            tpl_key = (row["pair"], row["session"])
            if tpl_key not in settings_cache:
                settings_cache[tpl_key] = load_settings(row["pair"], row["session"])
            settings = settings_cache[tpl_key]
            score, grade = score_row(row, settings)
            rec = dict(row)
            rec["_runtime_settings"] = settings
            rec["_runtime_zone"] = runtime_cfg.best_zone_runtime(settings, row["quarter"], row["direction_assumed"], float(row.get("target_distance", 0.0) or 0.0)) if isinstance(settings, dict) and "pair_runtime" in settings else None
            rec["priority_score"] = score
            rec["historical_grade"] = grade
            rec["static_pips_f"] = float(row.get("static_pips", 0.0) or 0.0)
            rec["managed_pips_f"] = float(row.get("managed_pips", row.get("static_pips", 0.0)) or 0.0)
            rec["static_R_f"] = float(row.get("static_R", 0.0) or 0.0)
            rec["managed_R_f"] = float(row.get("managed_R", row.get("static_R", 0.0)) or 0.0)
            rec["target_distance"] = float(row.get("target_distance", 0.0) or 0.0)
            scored.append(rec)
        buckets[key] = scored

    pair_order = simulate_strategy(buckets, cap, starting_balance, priority=False, pips_field="managed_pips_f", r_field="managed_R_f")
    priority_order = simulate_strategy(buckets, cap, starting_balance, priority=True, pips_field="managed_pips_f", r_field="managed_R_f")

    return {
        "bucket_count": len(buckets),
        "cap": cap,
        "starting_balance": round(starting_balance, 2),
        "pair_parent_cap": PAIR_PARENT_CAP,
        "same_pair_linear_penalty": SAME_PAIR_LINEAR_PENALTY,
        "pair_order": pair_order,
        "priority_order": priority_order,
        "delta_pips": round(priority_order["total_pips"] - pair_order["total_pips"], 2),
        "delta_balance": round(priority_order["final_balance"] - pair_order["final_balance"], 2),
    }


def coverage_summary(rows: list[dict]) -> dict:
    nodes = sorted({str(r.get("node", "")) for r in rows if str(r.get("node", ""))})
    matched = [
        r for r in rows
        if str(r.get("managed_source", "")) in {"aee_trade_rows", "aee_trade_rows_source"}
    ]
    fallback = [r for r in rows if str(r.get("managed_source", "")).startswith("static_fallback")]
    static_only = [r for r in rows if str(r.get("source_mode", "")) == "static"]
    return {
        "node_count": len(nodes),
        "matched_row_count": len(matched),
        "fallback_row_count": len(fallback),
        "static_source_row_count": len(static_only),
        "matched_row_pct": round(len(matched) / max(1, len(rows)), 4),
        "nodes_sample": nodes[:20],
    }


def simulate_same_set_static_vs_aee(rows: list[dict], cap: int, starting_balance: float) -> dict:
    matched_rows = [
        dict(r) for r in rows
        if str(r.get("managed_source", "")) in {"aee_trade_rows", "aee_trade_rows_source"}
    ]
    if not matched_rows:
        return {
            "status": "NO_MATCHED_AEE_ROWS",
            "coverage": coverage_summary(rows),
        }
    template_cache: dict[tuple[str, str], dict | None] = {}
    static_idx_cache: dict[str, dict[tuple[str, str, str, str, str], dict]] = {}
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in matched_rows:
        key = (row["weekday"], row["timestamp"])
        tpl_key = (row["pair"], row["session"])
        if tpl_key not in template_cache:
            template_cache[tpl_key] = load_settings(row["pair"], row["session"])
        settings = template_cache[tpl_key]
        score, grade = score_row(row, settings)
        rec = dict(row)
        rec["_runtime_settings"] = settings
        rec["_runtime_zone"] = runtime_cfg.best_zone_runtime(settings, row["quarter"], row["direction_assumed"], float(row.get("target_distance", 0.0) or 0.0)) if isinstance(settings, dict) and "pair_runtime" in settings else None
        node = str(row.get("node", ""))
        if node and node not in static_idx_cache:
            static_idx_cache[node] = load_static_population_index(ROOT / "compiled_market_nodes" / node)
        static_row = static_idx_cache.get(node, {}).get(_row_match_key(row))
        rec["priority_score"] = score
        rec["historical_grade"] = grade
        rec["static_pips_f"] = float((static_row or {}).get("static_pips", row.get("static_pips", 0.0)) or 0.0)
        rec["managed_pips_f"] = float(row.get("managed_pips", row.get("static_pips", 0.0)) or 0.0)
        rec["static_R_f"] = float((static_row or {}).get("static_R", row.get("static_R", 0.0)) or 0.0)
        rec["managed_R_f"] = float(row.get("managed_R", row.get("static_R", 0.0)) or 0.0)
        rec["target_distance"] = float(row.get("target_distance", 0.0) or 0.0)
        buckets[key].append(rec)
    static_pair = simulate_strategy(buckets, cap, starting_balance, priority=False, pips_field="static_pips_f", r_field="static_R_f")
    static_priority = simulate_strategy(buckets, cap, starting_balance, priority=True, pips_field="static_pips_f", r_field="static_R_f")
    aee_pair = simulate_strategy(buckets, cap, starting_balance, priority=False, pips_field="managed_pips_f", r_field="managed_R_f")
    aee_priority = simulate_strategy(buckets, cap, starting_balance, priority=True, pips_field="managed_pips_f", r_field="managed_R_f")
    return {
        "status": "OK",
        "coverage": coverage_summary(rows),
        "matched_bucket_count": len(buckets),
        "matched_row_count": len(matched_rows),
        "static_same_set": {
            "pair_order": static_pair,
            "priority_order": static_priority,
            "delta_pips": round(static_priority["total_pips"] - static_pair["total_pips"], 2),
            "delta_balance": round(static_priority["final_balance"] - static_pair["final_balance"], 2),
        },
        "aee_same_set": {
            "pair_order": aee_pair,
            "priority_order": aee_priority,
            "delta_pips": round(aee_priority["total_pips"] - aee_pair["total_pips"], 2),
            "delta_balance": round(aee_priority["final_balance"] - aee_pair["final_balance"], 2),
        },
        "aee_vs_static_on_same_set": {
            "pair_order_delta_pips": round(aee_pair["total_pips"] - static_pair["total_pips"], 2),
            "priority_order_delta_pips": round(aee_priority["total_pips"] - static_priority["total_pips"], 2),
            "pair_order_delta_balance": round(aee_pair["final_balance"] - static_pair["final_balance"], 2),
            "priority_order_delta_balance": round(aee_priority["final_balance"] - static_priority["final_balance"], 2),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="+", default=["thursday", "friday"])
    ap.add_argument("--pairs", nargs="*", default=None)
    ap.add_argument("--cap", type=int, default=3)
    ap.add_argument("--starting-balance", type=float, default=1000.0)
    ap.add_argument("--use-aee", action="store_true", help="Use AEE-stage selected populations and managed AEE outcomes when available.")
    ap.add_argument("--compare-same-set", action="store_true", help="When using AEE, report static vs AEE on the exact matched subset only.")
    args = ap.parse_args()
    rows = iter_rows(args.days, use_aee=args.use_aee, pairs=args.pairs)
    if args.use_aee and args.compare_same_set:
        result = simulate_same_set_static_vs_aee(rows, args.cap, args.starting_balance)
    else:
        result = simulate(rows, args.cap, args.starting_balance)
        result["source_mode"] = "aee" if args.use_aee else "static"
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
