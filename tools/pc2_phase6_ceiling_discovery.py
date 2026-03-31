from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _round(x: float, d: int = 4) -> float:
    return round(float(x), d)


def _k(record: Dict) -> Tuple[str, int, str, str, str, str]:
    return (
        str(record.get("direction", "")),
        int(record.get("target_bucket", 0)),
        str(record.get("pair", "")),
        str(record.get("session", "")),
        str(record.get("path_family", "")),
        str(record.get("structure_label", "")),
    )


def _idx(records: List[Dict]) -> Dict[Tuple[str, int, str, str, str, str], List[Dict]]:
    grouped: Dict[Tuple[str, int, str, str, str, str], List[Dict]] = {}
    for row in records:
        if not isinstance(row, dict):
            continue
        grouped.setdefault(_k(row), []).append(row)
    return grouped


def _select_best_trigger(candidates: List[Dict]) -> Dict:
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: float(row.get("trigger_quality", {}).get("trigger_quality_score", 0.0)),
    )


def build_ceiling_records(setup_truth: Dict, trigger_truth: Dict) -> List[Dict]:
    setup_index = _idx(setup_truth.get("records", []))
    trigger_index = _idx(trigger_truth.get("records", []))

    out: List[Dict] = []
    for key in sorted(set(setup_index) & set(trigger_index)):
        s = setup_index[key][0]
        t = _select_best_trigger(trigger_index[key])

        if str(s.get("status", "")).lower() != "valid":
            continue
        if str(t.get("status", "")).lower() != "valid":
            continue

        direction, target_bucket, pair, session, path_family, structure_label = key
        expectancy = float(s.get("expectancy", 0.0))
        sample_count = int(s.get("sample_count", 0))
        criteria = t.get("criteria", {})

        micro_q = float(s.get("causal_signature", {}).get("micro_displacement_quality", 0.0))
        cont = float(s.get("causal_signature", {}).get("continuation_strength", 0.0))
        rej = float(s.get("causal_signature", {}).get("rejection_strength", 0.0))
        mae_ratio = float(s.get("mae_profile", {}).get("mae_to_bucket_ratio", 0.0))

        execution_quality = max(0.05, min(1.0, 0.5 * micro_q + 0.25 * cont + 0.25 * (1.0 - min(mae_ratio, 1.0))))
        structure_efficiency = max(0.05, min(1.0, 0.5 * cont + 0.3 * (1.0 - min(mae_ratio, 1.0)) + 0.2 * rej))

        capped_expectancy = max(0.0, expectancy)
        theoretical_pips_per_setup = capped_expectancy * execution_quality * structure_efficiency
        hourly_multiplier = max(0.5, min(3.0, sample_count / 10.0))
        theoretical_pips_per_hour_ceiling = theoretical_pips_per_setup * hourly_multiplier

        out.append(
            {
                "direction": direction,
                "target_bucket": target_bucket,
                "pair": pair,
                "session": session,
                "path_family": path_family,
                "structure_label": structure_label,
                "setup_label": s.get("setup_label", ""),
                "trigger_label": t.get("trigger_label", ""),
                "trigger_family": t.get("trigger_family", ""),
                "ceiling": {
                    "status": "valid",
                    "metrics": {
                        "expectancy_pips": _round(capped_expectancy),
                        "sample_count": sample_count,
                        "execution_quality": _round(execution_quality),
                        "structure_efficiency": _round(structure_efficiency),
                        "theoretical_pips_per_setup": _round(theoretical_pips_per_setup),
                        "theoretical_pips_per_hour_ceiling": _round(theoretical_pips_per_hour_ceiling),
                        "criteria_snapshot": criteria,
                        "trigger_quality_score": _round(float(t.get("trigger_quality", {}).get("trigger_quality_score", 0.0))),
                    },
                },
            }
        )
    return out


def run(setup_file: Path, trigger_file: Path, out_file: Path) -> Dict:
    setup_truth = _load_json(setup_file)
    trigger_truth = _load_json(trigger_file)

    records = build_ceiling_records(setup_truth, trigger_truth)
    result = {
        "$artifact": "ceiling_report",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "derived_from": ["setup_truth.json", "trigger_truth.json"],
        "summary": {
            "candidate_triggers": len(trigger_truth.get("records", [])),
            "valid_ceiling_records": len(records),
            "invalid_ceiling_records": max(0, len(trigger_truth.get("records", [])) - len(records)),
        },
        "records": records,
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="PC2 Phase 6 ceiling discovery")
    parser.add_argument(
        "--setup",
        default="PC2/discovery/stage_a/setup_truth.json",
        help="Path to setup_truth artifact",
    )
    parser.add_argument(
        "--trigger",
        default="PC2/discovery/stage_a/trigger_truth.json",
        help="Path to trigger_truth artifact",
    )
    parser.add_argument(
        "--out",
        default="PC2/discovery/stage_a/ceiling_report.json",
        help="Output ceiling report artifact",
    )
    args = parser.parse_args()

    result = run(Path(args.setup), Path(args.trigger), Path(args.out))
    print(
        f"Wrote {args.out} with {result['summary']['valid_ceiling_records']} valid ceilings "
        f"from {result['summary']['candidate_triggers']} trigger candidates"
    )


if __name__ == "__main__":
    main()
