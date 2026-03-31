#!/usr/bin/env python3
"""
CODESPACES RCP — Setup Phase Validation
=======================================
Validates setup-phase PC2 outputs without performing discovery.

Inputs:
- business_viability_report.json
- path_family_report.json
- structure_truth.json
- setup_truth.json

This module enforces:
- correctness
- consistency
- dependency integrity
- promotion discipline (blocked in setup phase)

Hard rules:
- Does not modify PC2 outputs
- Does not infer missing setup data
- Rejects partial setups
- Rejects cross-layer smuggling
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from jsonschema import Draft7Validator
    _HAS_JSONSCHEMA = True
except ImportError:
    _HAS_JSONSCHEMA = False

from artifact_validator import validate_artifact, load_schema


MIN_SAMPLE_SIZE = 30


@dataclass
class CheckResult:
    name: str
    passed: bool
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SetupValidationResult:
    setup_id: str
    key: Dict[str, Any]
    checks: List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name: str, passed: bool, reason: str = "") -> None:
        self.checks.append(CheckResult(name=name, passed=passed, reason=reason))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "setup_id": self.setup_id,
            "key": self.key,
            "passed": self.passed,
            "checks": [c.to_dict() for c in self.checks],
        }


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> Any:
    with path.open() as f:
        return json.load(f)


def _expect_file(path: Path, errors: List[str]) -> bool:
    if not path.exists():
        errors.append(f"Missing required input file: {path}")
        return False
    return True


def _setup_records_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    return []


def _schema_validate_setup_records(
    setup_records: List[Dict[str, Any]],
    setup_path: Path,
) -> Tuple[List[str], List[str]]:
    """Validate setup records against setup_truth schema.

    Returns:
        schema_errors: schema-level violations
        constraint_errors: explicit required checks from Step 1
    """
    schema_errors: List[str] = []
    constraint_errors: List[str] = []

    if not _HAS_JSONSCHEMA:
        schema_errors.append("jsonschema not installed; cannot enforce setup schema validation.")
        return schema_errors, constraint_errors

    schema = load_schema("setup_truth")
    validator = Draft7Validator(schema)

    for i, setup in enumerate(setup_records):
        for error in sorted(validator.iter_errors(setup), key=lambda e: e.path):
            path_str = " -> ".join(str(p) for p in error.absolute_path) or "(root)"
            schema_errors.append(f"setup[{i}] [{path_str}] {error.message}")

        key_fields = ("direction", "target_bucket", "pair", "session")
        for field in key_fields:
            if field not in setup or setup[field] is None:
                constraint_errors.append(f"setup[{i}] missing required key field: {field}")

        if not setup.get("path_family"):
            constraint_errors.append(f"setup[{i}] path_family missing or null")
        if not setup.get("structure_label"):
            constraint_errors.append(f"setup[{i}] structure_label missing or null")

    return schema_errors, constraint_errors


def _validate_setup_ownership(setup_records: List[Dict[str, Any]]) -> List[str]:
    """Step 2 ownership validation for setup artifacts."""
    errors: List[str] = []

    seen_setup_ids: Dict[str, Dict[str, Any]] = {}
    for i, setup in enumerate(setup_records):
        setup_id = setup.get("setup_id")
        if not setup_id:
            errors.append(f"setup[{i}] missing setup_id (orphan fields).")
            continue

        ownership_tuple = (
            setup.get("structure_label"),
            setup.get("path_family"),
            setup.get("direction"),
            setup.get("target_bucket"),
        )

        if any(v in (None, "") for v in ownership_tuple):
            errors.append(
                f"setup[{i}] has orphan ownership field(s): "
                "structure_label/path_family/direction/target_bucket must all be present."
            )

        if setup_id in seen_setup_ids:
            prev = seen_setup_ids[setup_id]
            prev_tuple = (
                prev.get("structure_label"),
                prev.get("path_family"),
                prev.get("direction"),
                prev.get("target_bucket"),
            )
            if ownership_tuple != prev_tuple:
                errors.append(
                    f"Overlapping ownership for setup_id={setup_id}: "
                    f"{prev_tuple} vs {ownership_tuple}"
                )
        else:
            seen_setup_ids[setup_id] = setup

        if setup.get("direction") not in ("LONG", "SHORT"):
            errors.append(f"setup[{i}] invalid direction: {setup.get('direction')!r}")

    return errors


def _key_of(obj: Dict[str, Any]) -> Tuple[Any, Any, Any, Any]:
    return (obj.get("direction"), obj.get("target_bucket"), obj.get("pair"), obj.get("session"))


def _extract_family_names(path_family_report: Dict[str, Any]) -> List[str]:
    families = path_family_report.get("families", [])
    names = []
    for item in families:
        if isinstance(item, dict) and item.get("path_family"):
            names.append(item["path_family"])
    return names


def _domain_numeric(setup: Dict[str, Any], paths: List[Tuple[str, ...]]) -> Optional[float]:
    """Get first numeric value from candidate paths, else None.

    This does not infer values: it only reads explicit fields if present.
    """
    for path in paths:
        cursor: Any = setup
        ok = True
        for p in path:
            if not isinstance(cursor, dict) or p not in cursor:
                ok = False
                break
            cursor = cursor[p]
        if ok and isinstance(cursor, (int, float)):
            return float(cursor)
    return None


def _gold_case_checks(setup: Dict[str, Any]) -> List[CheckResult]:
    """Step 5: setup-level gold-case checks.

    Required explicit fields under setup.metadata.gold_case:
    - clean_winner_case
    - clean_loser_case
    - no_fake_runner_behavior
    - no_structural_ambiguity

    Missing fields are failures (no partial setups).
    """
    checks: List[CheckResult] = []
    gold_case = setup.get("metadata", {}).get("gold_case", {})

    required = [
        "clean_winner_case",
        "clean_loser_case",
        "no_fake_runner_behavior",
        "no_structural_ambiguity",
    ]
    for key in required:
        if key not in gold_case:
            checks.append(CheckResult(
                name=f"gold_case.{key}",
                passed=False,
                reason="missing required gold-case flag (partial setup not allowed)",
            ))
            continue
        if gold_case[key] is not True:
            checks.append(CheckResult(
                name=f"gold_case.{key}",
                passed=False,
                reason=f"expected true, got {gold_case[key]!r}",
            ))
        else:
            checks.append(CheckResult(name=f"gold_case.{key}", passed=True))

    return checks


def run_setup_phase_validation(
    artifact_dir: Path,
    output_dir: Path,
    min_sample_size: int,
) -> int:
    errors: List[str] = []

    bvr_path = artifact_dir / "business_viability_report.json"
    pfr_path = artifact_dir / "path_family_report.json"
    st_path = artifact_dir / "structure_truth.json"
    setup_path = artifact_dir / "setup_truth.json"

    for p in (bvr_path, pfr_path, st_path, setup_path):
        _expect_file(p, errors)

    output_dir.mkdir(parents=True, exist_ok=True)

    if errors:
        validation_report = {
            "generated_at": _now(),
            "phase": "setup",
            "status": "FAIL",
            "errors": errors,
        }
        with (output_dir / "validation_report.json").open("w") as f:
            json.dump(validation_report, f, indent=2)
        return 1

    # Load artifacts (read-only)
    business_viability = _load_json(bvr_path)
    path_family_report = _load_json(pfr_path)
    structure_truth = _load_json(st_path)
    setup_payload = _load_json(setup_path)
    setup_records = _setup_records_from_payload(setup_payload)

    if not setup_records:
        errors.append("setup_truth.json contains no setup records.")

    # Step 1: setup artifact schema validation
    setup_schema_errors, setup_constraint_errors = _schema_validate_setup_records(setup_records, setup_path)

    # Step 2: ownership validation (setup-specific)
    ownership_errors = _validate_setup_ownership(setup_records)

    # Step 3: dependency validation (setup-specific)
    dependency_errors: List[str] = []
    bvr_key = _key_of(business_viability)
    pfr_key = _key_of(path_family_report)
    st_key = _key_of(structure_truth)
    family_names = set(_extract_family_names(path_family_report))

    if bvr_key != pfr_key or pfr_key != st_key:
        dependency_errors.append(
            "Upstream key mismatch among business_viability_report, path_family_report, structure_truth."
        )

    if business_viability.get("viable") is not True:
        dependency_errors.append("setup requires viable business_viability_report (viable=true).")

    if not family_names:
        dependency_errors.append("setup requires non-empty path_family_report.families.")

    # Validate each setup against upstream key and labels
    setup_results: List[SetupValidationResult] = []
    for i, setup in enumerate(setup_records):
        setup_id = setup.get("setup_id", f"setup_index_{i}")
        key = {
            "direction": setup.get("direction"),
            "target_bucket": setup.get("target_bucket"),
            "pair": setup.get("pair"),
            "session": setup.get("session"),
        }
        result = SetupValidationResult(setup_id=setup_id, key=key)

        # Step 1 explicit checks (record-level)
        if i < len(setup_schema_errors) or i < len(setup_constraint_errors):
            # detailed errors are included globally; record-level result still keeps explicit checks below
            pass

        result.add(
            "setup.required.path_family",
            bool(setup.get("path_family")),
            "path_family missing" if not setup.get("path_family") else "",
        )
        result.add(
            "setup.required.structure_label",
            bool(setup.get("structure_label")),
            "structure_label missing" if not setup.get("structure_label") else "",
        )

        # Step 3 dependency checks
        setup_key = _key_of(setup)
        key_ok = setup_key == bvr_key == pfr_key == st_key
        result.add(
            "dependency.key_alignment",
            key_ok,
            "setup key does not align with all upstream artifacts" if not key_ok else "",
        )

        result.add(
            "dependency.viable_upstream",
            business_viability.get("viable") is True,
            "" if business_viability.get("viable") is True else "business_viability_report.viable is not true",
        )

        setup_path_family = setup.get("path_family")
        result.add(
            "dependency.path_family_exists_upstream",
            setup_path_family in family_names,
            "" if setup_path_family in family_names else f"setup.path_family={setup_path_family!r} not present in path_family_report.families",
        )

        st_label = structure_truth.get("structure_label")
        setup_label = setup.get("structure_label")
        result.add(
            "dependency.structure_match",
            setup_label == st_label,
            "" if setup_label == st_label else f"setup.structure_label={setup_label!r} != structure_truth.structure_label={st_label!r}",
        )

        # Step 4 domain constraints
        expectancy = _domain_numeric(setup, [
            ("expectancy",),
            ("metadata", "expectancy"),
            ("metadata", "domain", "expectancy"),
        ])
        if expectancy is None:
            result.add(
                "domain.expectancy_present",
                False,
                "missing expectancy field (no inference allowed)",
            )
        else:
            result.add(
                "domain.expectancy_non_negative",
                expectancy >= 0,
                f"negative expectancy={expectancy}",
            )

        mae_setup = _domain_numeric(setup, [
            ("mae_p95_pips",),
            ("metadata", "mae_p95_pips"),
            ("metadata", "domain", "mae_p95_pips"),
        ])
        mae_bound = _domain_numeric(business_viability, [
            ("max_mae_pips",),
            ("metadata", "viability_bounds", "max_mae_pips"),
            ("metadata", "max_mae_pips",),
        ])
        if mae_setup is None or mae_bound is None:
            result.add(
                "domain.mae_bound_present",
                False,
                "missing mae_p95_pips or viability max_mae_pips (partial setup)",
            )
        else:
            result.add(
                "domain.mae_within_viability_bound",
                mae_setup <= mae_bound,
                f"mae_p95_pips={mae_setup} exceeds max_mae_pips={mae_bound}",
            )

        result.add(
            "domain.path_family_match",
            setup.get("path_family") in family_names,
            "" if setup.get("path_family") in family_names else "setup path_family not in upstream families",
        )
        result.add(
            "domain.structure_match",
            setup.get("structure_label") == structure_truth.get("structure_label"),
            "" if setup.get("structure_label") == structure_truth.get("structure_label") else "setup structure_label mismatches structure_truth",
        )

        population_size = setup.get("population_size")
        pop_ok = isinstance(population_size, int) and population_size >= min_sample_size
        result.add(
            "domain.sample_size_floor",
            pop_ok,
            "" if pop_ok else f"population_size={population_size!r} below minimum={min_sample_size}",
        )

        # Step 5 gold-case checks
        for gc in _gold_case_checks(setup):
            result.checks.append(gc)

        setup_results.append(result)

    # Step 6 analyzer (initial)
    failures_by_check: Counter = Counter()
    passed_count = 0
    for sr in setup_results:
        if sr.passed:
            passed_count += 1
        for chk in sr.checks:
            if not chk.passed:
                failures_by_check[chk.name] += 1

    setup_consistency_report = {
        "generated_at": _now(),
        "phase": "setup",
        "total_setups": len(setup_results),
        "passed_setups": passed_count,
        "failed_setups": len(setup_results) - passed_count,
        "results": [sr.to_dict() for sr in setup_results],
    }

    distribution = {
        "direction": Counter(),
        "target_bucket": Counter(),
        "pair": Counter(),
        "session": Counter(),
        "path_family": Counter(),
        "structure_label": Counter(),
    }
    for setup in setup_records:
        for dim in ("direction", "target_bucket", "pair", "session", "path_family", "structure_label"):
            val = setup.get(dim)
            distribution[dim][str(val)] += 1

    setup_distribution_report = {
        "generated_at": _now(),
        "phase": "setup",
        "distribution": {k: dict(v) for k, v in distribution.items()},
    }

    failure_archetype_report = {
        "generated_at": _now(),
        "phase": "setup",
        "top_failure_archetypes": [
            {"check": name, "count": count}
            for name, count in failures_by_check.most_common()
        ],
    }

    # Step 7 promotion gate remains blocked in setup phase
    trigger_path = artifact_dir / "trigger_truth.json"
    ceiling_path = artifact_dir / "ceiling_report.json"
    promotion_gate_status = {
        "status": "BLOCKED",
        "reason": "Setup phase does not allow promotion; waiting for trigger_truth and ceiling_report.",
        "missing_required_artifacts": [
            name for name, p in [
                ("trigger_truth.json", trigger_path),
                ("ceiling_report.json", ceiling_path),
            ] if not p.exists()
        ],
    }

    # Global pass/fail
    all_errors = []
    all_errors.extend(setup_schema_errors)
    all_errors.extend(setup_constraint_errors)
    all_errors.extend(ownership_errors)
    all_errors.extend(dependency_errors)

    status = "PASS"
    if all_errors or any(not s.passed for s in setup_results):
        status = "FAIL"

    validation_report = {
        "generated_at": _now(),
        "phase": "setup",
        "status": status,
        "input_files": {
            "business_viability_report": str(bvr_path),
            "path_family_report": str(pfr_path),
            "structure_truth": str(st_path),
            "setup_truth": str(setup_path),
        },
        "schema_errors": setup_schema_errors,
        "setup_artifact_errors": setup_constraint_errors,
        "ownership_errors": ownership_errors,
        "dependency_errors": dependency_errors,
        "promotion_gate": promotion_gate_status,
        "summary": {
            "total_setups": len(setup_results),
            "passed_setups": passed_count,
            "failed_setups": len(setup_results) - passed_count,
        },
    }

    setup_failure_report = {
        "generated_at": _now(),
        "phase": "setup",
        "status": "FAIL" if status == "FAIL" else "PASS",
        "global_failures": all_errors,
        "failed_setups": [
            sr.to_dict() for sr in setup_results if not sr.passed
        ],
    }

    setup_consistency_metrics = {
        "generated_at": _now(),
        "phase": "setup",
        "consistency_ratio": (
            passed_count / len(setup_results) if setup_results else 0.0
        ),
        "failed_ratio": (
            (len(setup_results) - passed_count) / len(setup_results) if setup_results else 0.0
        ),
        "failure_counts_by_check": dict(failures_by_check),
    }

    # Write required outputs
    outputs = {
        "validation_report.json": validation_report,
        "setup_failure_report.json": setup_failure_report,
        "setup_consistency_metrics.json": setup_consistency_metrics,
        "setup_consistency_report.json": setup_consistency_report,
        "setup_distribution_report.json": setup_distribution_report,
        "failure_archetype_report.json": failure_archetype_report,
    }
    for name, payload in outputs.items():
        with (output_dir / name).open("w") as f:
            json.dump(payload, f, indent=2)

    print(f"Setup phase validation completed. Status={status}")
    print(f"Reports written to: {output_dir}")

    return 0 if status == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Codespaces setup-phase validation over PC2 setup artifacts."
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        required=True,
        help="Directory containing setup-phase PC2 input artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/setup_phase"),
        help="Directory where validation reports will be written.",
    )
    parser.add_argument(
        "--min-sample-size",
        type=int,
        default=MIN_SAMPLE_SIZE,
        help="Minimum setup population size required.",
    )
    args = parser.parse_args()

    return run_setup_phase_validation(
        artifact_dir=args.artifact_dir,
        output_dir=args.output_dir,
        min_sample_size=args.min_sample_size,
    )


if __name__ == "__main__":
    sys.exit(main())
