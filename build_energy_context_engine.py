#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import build_entry_trigger_state_machine as trig


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def signed_positive(x: float) -> float:
    return clamp01((x + 1.0) / 2.0)


def energy_context(row: dict[str, Any]) -> dict[str, float | str]:
    p5 = float(row["pressure_5"])
    p15 = float(row["pressure_15"])
    p30 = float(row["pressure_30"])
    p515 = float(row["pressure_ratio_5_15"])
    qtd = float(row["directional_dominance_qtd"])
    qbias = float(row["quarter_relative_bias"])
    close_pos = float(row["signed_close_position_5"])
    breakout = float(row["breakout_distance"])
    vel = float(row["velocity_now"])
    vel3 = float(row["velocity_3"])
    velchg = float(row["velocity_change"])
    compression = float(row["compression"])
    recent_range = float(row["recent_range_20"])
    recent_vol = float(row["recent_vol_10"])
    dist_low = float(row["distance_to_local_low"])
    dist_high = float(row["distance_to_local_high"])

    macro_dir = clamp01(0.55 * signed_positive(p30) + 0.45 * signed_positive(qtd))
    micro_dir = clamp01(0.45 * signed_positive(p5) + 0.35 * signed_positive(p515) + 0.20 * signed_positive(vel / 2.5))
    compression_score = clamp01(1.0 - compression)
    release_quality = clamp01(
        0.35 * signed_positive(p5)
        + 0.20 * signed_positive(p515)
        + 0.20 * clamp01(close_pos)
        + 0.15 * clamp01(breakout / 1.5)
        + 0.10 * signed_positive(vel / 2.5)
    )

    impulse_spent = clamp01((dist_low + dist_high) / max(recent_range, 1e-9) / 2.0)
    velocity_decay = clamp01((-velchg + 1.5) / 3.0)
    exhaustion = clamp01(
        0.40 * impulse_spent
        + 0.30 * velocity_decay
        + 0.15 * clamp01(recent_vol / 2.0)
        + 0.15 * clamp01(abs(qbias) < 0.05)
    )

    noise = clamp01(
        0.35 * clamp01(compression)
        + 0.25 * clamp01(abs(velchg) / 2.0)
        + 0.20 * clamp01(abs(p15) < 0.10)
        + 0.20 * clamp01(close_pos < 0.60)
    )

    total_dist = max(dist_low + dist_high, 1e-9)
    budget_fraction = dist_high / total_dist if row["direction_assumed"] == "LONG" else dist_low / total_dist
    remaining_budget = clamp01(
        0.55 * (1.0 - budget_fraction)
        + 0.20 * (1.0 - impulse_spent)
        + 0.25 * clamp01((float(row["future_mfe_pips"]) - 2.5) / 10.0)
    )

    if compression_score > 0.55 and release_quality < 0.55:
        state = "BUILDING"
    elif release_quality >= 0.55 and exhaustion < 0.45 and noise < 0.55:
        state = "FRESH_RELEASE"
    elif macro_dir >= 0.55 and micro_dir >= 0.50 and exhaustion < 0.65 and remaining_budget >= 0.40:
        state = "HEALTHY_CONTINUATION"
    elif exhaustion >= 0.60 and remaining_budget < 0.45:
        state = "LATE_CONTINUATION"
    elif noise >= 0.60:
        state = "CHOP"
    else:
        state = "EXHAUSTION_REVERSAL_RISK"

    return {
        "macro_dir_score": round(macro_dir, 6),
        "micro_dir_score": round(micro_dir, 6),
        "compression_score": round(compression_score, 6),
        "release_quality_score": round(release_quality, 6),
        "exhaustion_score": round(exhaustion, 6),
        "noise_score": round(noise, 6),
        "remaining_budget_score": round(remaining_budget, 6),
        "energy_state": state,
        # Keep both names so older contextual compilers and newer stream code
        # can consume the same node-local context file without translation.
        "energy_regime": state,
    }


def summarize_context(rows: list[dict[str, Any]]) -> dict[str, Any]:
    states = Counter(r["energy_state"] for r in rows)
    by_action = defaultdict(list)
    for r in rows:
        by_action[r["outcome_label"]].append(r)
    return {
        "row_count": len(rows),
        "state_counts": dict(states),
        "state_profiles": {
            state: {
                "count": sum(1 for r in rows if r["energy_state"] == state),
                "macro_dir_mean": mean(float(r["macro_dir_score"]) for r in rows if r["energy_state"] == state),
                "micro_dir_mean": mean(float(r["micro_dir_score"]) for r in rows if r["energy_state"] == state),
                "compression_mean": mean(float(r["compression_score"]) for r in rows if r["energy_state"] == state),
                "release_mean": mean(float(r["release_quality_score"]) for r in rows if r["energy_state"] == state),
                "exhaustion_mean": mean(float(r["exhaustion_score"]) for r in rows if r["energy_state"] == state),
                "noise_mean": mean(float(r["noise_score"]) for r in rows if r["energy_state"] == state),
                "budget_mean": mean(float(r["remaining_budget_score"]) for r in rows if r["energy_state"] == state),
            }
            for state in states
        },
        "outcome_state_counts": {
            f"{outcome}|{state}": count
            for outcome in ("GOOD", "BAD", "NOISE")
            for state, count in Counter(r["energy_state"] for r in by_action.get(outcome, [])).items()
        },
    }


def audit_islands(rows: list[dict[str, Any]], rules: list[dict[str, Any]]) -> dict[str, Any]:
    audit_rows = []
    for rule in rules:
        matched = [r for r in rows if trig.match_rule(r, rule)]
        states = Counter(r["energy_state"] for r in matched)
        audit_rows.append(
            {
                "rule_key": f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}",
                "trade_count": len(matched),
                "state_counts": dict(states),
                "dominant_energy_state": states.most_common(1)[0][0] if states else "NONE",
                "fresh_release_rate": sum(1 for r in matched if r["energy_state"] == "FRESH_RELEASE") / max(1, len(matched)),
                "healthy_continuation_rate": sum(1 for r in matched if r["energy_state"] == "HEALTHY_CONTINUATION") / max(1, len(matched)),
                "late_or_chop_rate": sum(1 for r in matched if r["energy_state"] in {"LATE_CONTINUATION", "CHOP", "EXHAUSTION_REVERSAL_RISK"}) / max(1, len(matched)),
                "macro_dir_mean": mean(float(r["macro_dir_score"]) for r in matched) if matched else 0.0,
                "release_mean": mean(float(r["release_quality_score"]) for r in matched) if matched else 0.0,
                "exhaustion_mean": mean(float(r["exhaustion_score"]) for r in matched) if matched else 0.0,
                "noise_mean": mean(float(r["noise_score"]) for r in matched) if matched else 0.0,
                "budget_mean": mean(float(r["remaining_budget_score"]) for r in matched) if matched else 0.0,
            }
        )
    return {"islands": audit_rows}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_csv(args.stream_csv)
    enriched = []
    for row in rows:
        enriched.append({**row, **energy_context(row)})

    report = summarize_context(enriched)
    rules = json.loads(args.rules_json.read_text())["path_classes"]
    island_report = audit_islands(enriched, rules)

    fields = list(enriched[0].keys()) if enriched else ["timestamp"]
    write_csv(out_dir / "session_energy_context_stream.csv", enriched, fields)
    (out_dir / "energy_context_report.json").write_text(json.dumps(report, indent=2))
    (out_dir / "island_energy_context_audit.json").write_text(json.dumps(island_report, indent=2))
    print(json.dumps({
        "rows": len(enriched),
        "state_counts": report["state_counts"],
        "island_count": len(island_report["islands"]),
    }, indent=2))


if __name__ == "__main__":
    main()
