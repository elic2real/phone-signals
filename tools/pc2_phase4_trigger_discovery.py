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


def _criteria_from_setup(record: Dict) -> Dict:
    causal = record.get("causal_signature", {})
    mae_profile = record.get("mae_profile", {})

    entry_position = str(causal.get("entry_position_in_structure", "retest"))
    continuation_strength = float(causal.get("continuation_strength", 0.0))
    rejection_strength = float(causal.get("rejection_strength", 0.0))
    micro_displacement_quality = float(causal.get("micro_displacement_quality", 0.0))
    failed_push_count = int(causal.get("failed_push_count", 0))
    mae_to_bucket_ratio = float(mae_profile.get("mae_to_bucket_ratio", 0.0))
    spread_efficiency = float(mae_profile.get("spread_efficiency", 0.0))

    directional_bias = "continuation" if continuation_strength >= rejection_strength else "reversal"
    family = str(record.get("path_family", ""))
    if family == "sweep":
        directional_bias = "reversal"
    if family in {"continuation", "drift", "breakout"}:
        directional_bias = "continuation"

    return {
        "entry_position_required": entry_position,
        "directional_bias": directional_bias,
        "min_continuation_strength": _round(max(0.25, continuation_strength * 0.9)),
        "min_rejection_strength": _round(max(0.12, rejection_strength * 0.85)),
        "max_failed_push_count": max(2, failed_push_count + 1),
        "min_micro_displacement_quality": _round(max(0.45, micro_displacement_quality * 0.9)),
        "max_pre_entry_mae_to_bucket_ratio": _round(max(0.35, min(1.8, mae_to_bucket_ratio * 1.05))),
        "min_spread_efficiency": _round(max(0.8, spread_efficiency * 0.9)),
        "notes": [
            "Derived directly from setup causal signature.",
            "No AEE logic included.",
            "No promotion decision included.",
        ],
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

        out.append(
            {
                "trigger_label": f"trigger::{record.get('setup_label', 'unknown')}",
                "setup_label": record.get("setup_label", ""),
                "structure_label": record.get("structure_label", ""),
                "path_family": record.get("path_family", ""),
                "direction": record.get("direction", ""),
                "target_bucket": record.get("target_bucket", ""),
                "pair": record.get("pair", ""),
                "session": record.get("session", ""),
                "criteria": _criteria_from_setup(record),
                "sample_count": int(record.get("sample_count", 0)),
                "expectancy": _round(float(record.get("expectancy", 0.0))),
                "status": "valid",
            }
        )
    return out


def run(input_file: Path, out_file: Path) -> Dict:
    setup_truth = _load_json(input_file)
    records = build_trigger_records(setup_truth)

    result = {
        "$artifact": "trigger_truth",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "derived_from": ["setup_truth.json"],
        "summary": {
            "candidate_setups": len(setup_truth.get("records", [])),
            "valid_triggers": len(records),
            "invalid_triggers": max(0, len(setup_truth.get("records", [])) - len(records)),
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
