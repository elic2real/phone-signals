from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _k4(record: Dict) -> Tuple[str, int, str, str]:
    return (
        str(record["direction"]),
        int(record["target_bucket_pips"]),
        str(record["pair"]),
        str(record["session"]),
    )


def _index_by_k4(records: List[Dict]) -> Dict[Tuple[str, int, str, str], Dict]:
    return {_k4(r): r for r in records}


def _structure_pct(struct_record: Dict, label: str) -> float:
    for row in struct_record.get("struct_breakdown", []):
        if row.get("label") == label:
            return float(row.get("pct", 0.0))
    return 0.0


def _family_pct(path_record: Dict, family: str) -> float:
    for row in path_record.get("families", []):
        if row.get("family") == family:
            return float(row.get("pct", 0.0))
    return 0.0


def _family_count(path_record: Dict, family: str) -> int:
    for row in path_record.get("families", []):
        if row.get("family") == family:
            return int(row.get("count", 0))
    return 0


def _structure_count(struct_record: Dict, label: str) -> int:
    for row in struct_record.get("struct_breakdown", []):
        if row.get("label") == label:
            return int(row.get("count", 0))
    return 0


def _entry_position(structure_label: str) -> str:
    mapping = {
        "range_edge": "edge",
        "liquidity_sweep_zone": "edge",
        "break_level": "break",
        "retest_level": "retest",
        "drift_channel": "mid",
        "compression": "mid",
    }
    return mapping.get(structure_label, "mid")


def _round(x: float, d: int = 4) -> float:
    return round(float(x), d)


def build_setup_records(viability: Dict, path_family: Dict, structure: Dict) -> List[Dict]:
    v_index = _index_by_k4(viability["records"])
    p_index = _index_by_k4(path_family["records"])
    s_index = _index_by_k4(structure["records"])

    out: List[Dict] = []
    for key in sorted(set(v_index) & set(p_index) & set(s_index)):
        v = v_index[key]
        p = p_index[key]
        s = s_index[key]

        if not bool(v.get("viable", False)):
            continue
        if float(v.get("expectancy_pips", 0.0)) <= 0:
            continue
        if not bool(p.get("non_random_verdict", False)):
            continue
        if not bool(s.get("consistent_verdict", False)):
            continue

        direction, target_bucket, pair, session = key
        dominant_family = str(p.get("dominant_family", ""))
        dominant_structure = str(s.get("dominant_structure", ""))
        if not dominant_family or not dominant_structure:
            continue

        sample_size = int(v.get("sample_size", 0))
        family_count = _family_count(p, dominant_family)
        structure_count = _structure_count(s, dominant_structure)
        sample_count = min(family_count, structure_count)
        if sample_count <= 0:
            continue

        placement_rate = float(s.get("placement_rate", 0.0))
        unclassified_rate = float(p.get("unclassified_count", 0)) / max(sample_size, 1)
        dominant_family_pct = _family_pct(p, dominant_family)
        dominant_structure_pct = _structure_pct(s, dominant_structure)

        sweep_pct = _family_pct(p, "sweep")
        continuation_family_strength = (
            _family_pct(p, "continuation") + _family_pct(p, "breakout") + _family_pct(p, "drift")
        )
        liquidity_edge_pct = (
            _structure_pct(s, "liquidity_sweep_zone")
            + _structure_pct(s, "range_edge")
            + _structure_pct(s, "break_level")
        )

        entry_position = _entry_position(dominant_structure)
        entry_bonus = 0.1 if entry_position in {"edge", "retest"} else 0.0

        rejection_strength = _round(min(1.0, 0.55 * sweep_pct + 0.35 * liquidity_edge_pct + entry_bonus))
        continuation_strength = _round(
            min(
                1.0,
                0.45 * continuation_family_strength
                + 0.35 * float(v.get("smoothness", 0.0))
                + 0.2 * float(v.get("hit_rate", 0.0)),
            )
        )
        failed_push_count = int(round((sweep_pct + liquidity_edge_pct) * 6))
        micro_displacement_quality = _round(
            min(
                1.0,
                0.35 * float(v.get("smoothness", 0.0))
                + 0.25 * float(v.get("spread_efficiency", 0.0))
                + 0.2 * dominant_family_pct
                + 0.2 * placement_rate,
            )
        )
        pre_entry_adverse_movement = _round(float(v.get("avg_mae_pips", 0.0)))

        stability_score = _round(
            min(
                1.0,
                max(
                    0.0,
                    (1.0 - unclassified_rate) * placement_rate * (0.5 + 0.5 * min(dominant_family_pct, dominant_structure_pct)),
                ),
            )
        )

        setup_label = (
            f"{pair}_{session}_{direction}_{target_bucket}pip_{dominant_family}_{dominant_structure}"
        )

        out.append(
            {
                "setup_label": setup_label,
                "structure_label": dominant_structure,
                "path_family": dominant_family,
                "direction": direction,
                "target_bucket": target_bucket,
                "pair": pair,
                "session": session,
                "causal_signature": {
                    "entry_position_in_structure": entry_position,
                    "rejection_strength": rejection_strength,
                    "continuation_strength": continuation_strength,
                    "failed_push_count": failed_push_count,
                    "micro_displacement_quality": micro_displacement_quality,
                    "pre_entry_adverse_movement": pre_entry_adverse_movement,
                    "dominant_family_pct": _round(dominant_family_pct),
                    "dominant_structure_pct": _round(dominant_structure_pct),
                    "placement_rate": _round(placement_rate),
                    "stability_score": stability_score,
                },
                "expectancy": _round(float(v.get("expectancy_pips", 0.0))),
                "mae_profile": {
                    "avg_mae_pips": _round(float(v.get("avg_mae_pips", 0.0))),
                    "mae_to_bucket_ratio": _round(float(v.get("avg_mae_pips", 0.0)) / max(target_bucket, 1)),
                    "spread_efficiency": _round(float(v.get("spread_efficiency", 0.0))),
                },
                "sample_count": sample_count,
                "status": "valid",
            }
        )

    return out


def run(input_dir: Path, out_file: Path) -> Dict:
    viability = _load_json(input_dir / "business_viability_report.json")
    path_family = _load_json(input_dir / "path_family_report.json")
    structure = _load_json(input_dir / "structure_truth.json")

    records = build_setup_records(viability, path_family, structure)
    result = {
        "$artifact": "setup_truth",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "derived_from": [
            "business_viability_report.json",
            "path_family_report.json",
            "structure_truth.json",
        ],
        "summary": {
            "candidate_domains": len(records),
            "valid_setups": len(records),
            "invalid_setups": 0,
        },
        "records": records,
    }
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="PC2 Phase 3 setup discovery")
    parser.add_argument(
        "--input-dir",
        default="PC2/discovery/stage_a",
        help="Directory containing Stage A artifacts",
    )
    parser.add_argument(
        "--out",
        default="PC2/discovery/stage_a/setup_truth.json",
        help="Output setup truth artifact",
    )
    args = parser.parse_args()

    result = run(Path(args.input_dir), Path(args.out))
    print(
        f"Wrote {args.out} with {result['summary']['valid_setups']} valid setups "
        f"from {result['summary']['candidate_domains']} strict domains"
    )


if __name__ == "__main__":
    main()
