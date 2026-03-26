#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from typing import Any

import build_entry_trigger_state_machine as trig
import build_energy_regime_classifier as reg

POS_KEYS = [
    "pre_build_slope",
    "pre_build_accel",
    "pre_compression_release_delta",
    "pre_macro_micro_alignment",
    "release_to_exhaustion_delta",
    "post_continuation_persistence",
]
NEG_KEYS = [
    "post_noise_rise",
    "post_exhaustion_rise",
    "post_budget_decay",
]


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def rule_key(rule: dict[str, Any]) -> str:
    return f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"


def f(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except Exception:
        return 0.0


def quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    idx = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * q))))
    return vals[idx]


def derive_profiles(rows: list[dict[str, Any]], rules: list[dict[str, Any]], allowed: dict[str, list[str]]) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for rule in rules:
        rk = rule_key(rule)
        bucket: list[dict[str, Any]] = []
        for row in rows:
            if not trig.match_rule(row, rule):
                continue
            regime = row.get("energy_regime") or reg.classify_regime(row)
            if allowed.get(rk) and regime not in allowed[rk]:
                continue
            bucket.append({**row, "energy_regime": regime})
        wins = [r for r in bucket if f(r, "static_pips") > 0]
        losses = [r for r in bucket if f(r, "static_pips") <= 0]
        if len(wins) < 8:
            continue
        thresholds: dict[str, float] = {}
        active: list[str] = []
        for key in POS_KEYS:
            win_q = quantile([f(r, key) for r in wins], 0.35)
            loss_q = quantile([f(r, key) for r in losses] or [0.0], 0.55)
            if win_q > loss_q:
                thresholds[key] = win_q
                active.append(key)
        for key in NEG_KEYS:
            win_q = quantile([f(r, key) for r in wins], 0.65)
            loss_q = quantile([f(r, key) for r in losses] or [0.0], 0.45)
            if win_q < loss_q:
                thresholds[key] = win_q
                active.append(key)
        profiles[rk] = {
            "active_features": active,
            "thresholds": thresholds,
            "score_min": 0.6 if len(active) >= 5 else 0.5,
            "wins": len(wins),
            "losses": len(losses),
            "base_trade_count": len(bucket),
            "base_expectancy": mean(f(r, "static_pips") for r in bucket) if bucket else 0.0,
        }
    return profiles


def pass_profile(row: dict[str, Any], profile: dict[str, Any]) -> bool:
    active = profile["active_features"]
    if not active:
        return True
    passed = 0
    for key in active:
        value = f(row, key)
        threshold = profile["thresholds"][key]
        if key in POS_KEYS:
            passed += 1 if value >= threshold else 0
        else:
            passed += 1 if value <= threshold else 0
    return (passed / len(active)) >= profile["score_min"]


def replay(
    rows: list[dict[str, Any]],
    rules: list[dict[str, Any]],
    allowed: dict[str, list[str]],
    profiles: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        regime = row.get("energy_regime") or reg.classify_regime(row)
        enriched = {**row, "energy_regime": regime}
        for rule in rules:
            if not trig.match_rule(enriched, rule):
                continue
            rk = rule_key(rule)
            if allowed.get(rk) and regime not in allowed[rk]:
                continue
            if profiles is not None and rk in profiles and not pass_profile(enriched, profiles[rk]):
                continue
            selected.append(enriched)
            break
    return selected, trig.summarize_replay(selected, rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trajectory-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--regime-report-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    rows = load_csv(args.trajectory_csv)
    rules = json.loads(args.rules_json.read_text())["path_classes"]
    regime_report = json.loads(args.regime_report_json.read_text())
    allowed = regime_report["rule_allowed_regimes"]

    before_rows, before = replay(rows, rules, allowed, None)
    profiles = derive_profiles(rows, rules, allowed)
    after_rows, after = replay(rows, rules, allowed, profiles)

    report = {
        "before": before,
        "after": after,
        "delta": {
            key: after[key] - before[key]
            for key in [
                "trade_count",
                "win_rate",
                "expectancy",
                "avg_R",
                "pips_per_hour",
                "good_capture",
                "bad_trigger",
                "noise_trigger",
            ]
        },
        "profile_count": len(profiles),
        "profiles": profiles,
    }

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(out_dir / "before_population.csv", before_rows, list(before_rows[0].keys()) if before_rows else ["timestamp"])
    write_csv(out_dir / "after_population.csv", after_rows, list(after_rows[0].keys()) if after_rows else ["timestamp"])
    (out_dir / "island_point_trajectory_gate_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
