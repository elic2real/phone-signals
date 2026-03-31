#!/usr/bin/env python3
"""
Enforcement: Artifact Validator
================================
Validates PC2 discovery artifacts against their JSON schemas.

Usage:
    python artifact_validator.py --artifact path/to/report.json --schema business_viability_report
    python artifact_validator.py --artifact path/to/report.json  # auto-detects schema from artifact type field
    python artifact_validator.py --batch path/to/dir/           # validates all .json files in directory

Hard constraints enforced here:
- LONG and SHORT are never mixed in a single artifact
- path_family is NOT required in early-phase artifacts (business_viability, structure_truth, ceiling_report)
- path_family IS required in setup_truth, trigger_truth
- locked=true is required and enforced for setup_truth and trigger_truth
- No cross-pair or cross-session averaging is permitted (pair/session fields must be singular values)
"""
from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import jsonschema
    from jsonschema import Draft7Validator, ValidationError
    _JSONSCHEMA_AVAILABLE = True
except ImportError:
    _JSONSCHEMA_AVAILABLE = False

SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"

ARTIFACT_SCHEMA_MAP: Dict[str, str] = {
    "business_viability_report": "business_viability_report.schema.json",
    "path_family_report": "path_family_report.schema.json",
    "structure_truth": "structure_truth.schema.json",
    "setup_truth": "setup_truth.schema.json",
    "trigger_truth": "trigger_truth.schema.json",
    "ceiling_report": "ceiling_report.schema.json",
    "segmentation_gap_report": "segmentation_gap_report.schema.json",
    "intervention_basis": "intervention_basis.schema.json",
}

# These artifact types must NOT have path_family as a required field at runtime

EARLY_PHASE_ARTIFACTS = {
    "business_viability_report",
    "structure_truth",
    "ceiling_report",
    "segmentation_gap_report",
}

# These artifact types require path_family to be present and non-null
FAMILY_REQUIRED_ARTIFACTS = {
    "setup_truth",
    "trigger_truth",
}

# These artifact types require locked=true
LOCKED_ARTIFACTS = {
    "setup_truth",
    "trigger_truth",
}


def load_schema(artifact_type: str) -> Dict[str, Any]:
    """Load the JSON schema for the given artifact type."""
    schema_file = SCHEMAS_DIR / ARTIFACT_SCHEMA_MAP[artifact_type]
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    with schema_file.open() as f:
        return json.load(f)


def detect_artifact_type(artifact: Dict[str, Any]) -> Optional[str]:
    """Attempt to detect artifact type from content heuristics."""
    if "families" in artifact and "family_count" in artifact:
        return "path_family_report"
    if "structure_label" in artifact and "label_confidence" in artifact:
        return "structure_truth"
    if "setup_id" in artifact and "entry_filter" in artifact:
        return "setup_truth"
    if "trigger_id" in artifact and "trigger_conditions" in artifact:
        return "trigger_truth"
    if "ceiling_metrics" in artifact:
        return "ceiling_report"
    if "gap_type" in artifact and "recoverable" in artifact:
        return "segmentation_gap_report"
    if "viable" in artifact and "fail_reasons" in artifact:
        return "business_viability_report"
    if "intervention_type" in artifact and "intervention_id" in artifact:
        return "intervention_basis"
    return None


class ValidationResult:
    """Container for validation outcome."""

    def __init__(self, artifact_path: str, artifact_type: Optional[str]) -> None:
        self.artifact_path = artifact_path
        self.artifact_type = artifact_type
        self.schema_errors: List[str] = []
        self.constraint_errors: List[str] = []
        self.warnings: List[str] = []

    @property
    def passed(self) -> bool:
        return len(self.schema_errors) == 0 and len(self.constraint_errors) == 0

    def add_schema_error(self, msg: str) -> None:
        self.schema_errors.append(msg)

    def add_constraint_error(self, msg: str) -> None:
        self.constraint_errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"[{status}] {self.artifact_path} ({self.artifact_type or 'unknown type'})"]
        for e in self.schema_errors:
            lines.append(f"  SCHEMA ERROR: {e}")
        for e in self.constraint_errors:
            lines.append(f"  CONSTRAINT ERROR: {e}")
        for w in self.warnings:
            lines.append(f"  WARNING: {w}")
        return "\n".join(lines)


def _check_domain_constraints(
    artifact: Dict[str, Any],
    artifact_type: str,
    result: ValidationResult,
) -> None:
    """Apply hard domain constraints beyond what the JSON schema enforces."""

    # 1. Direction must be LONG or SHORT (never mixed/missing in keyed artifacts)
    if "direction" in artifact:
        direction = artifact["direction"]
        if direction not in ("LONG", "SHORT"):
            result.add_constraint_error(
                f"direction must be exactly 'LONG' or 'SHORT', got: {direction!r}"
            )

    # 2. Pair must be singular (no cross-pair)
    if "pair" in artifact:
        pair = artifact["pair"]
        if "," in str(pair) or isinstance(pair, list):
            result.add_constraint_error(
                "cross-pair averaging not permitted: pair must be a single instrument"
            )

    # 3. Session must be singular (no cross-session)
    if "session" in artifact:
        session = artifact["session"]
        if "," in str(session) or isinstance(session, list):
            result.add_constraint_error(
                "cross-session averaging not permitted: session must be a single session key"
            )

    # 4. path_family enforcement
    if artifact_type in FAMILY_REQUIRED_ARTIFACTS:
        path_family = artifact.get("path_family")
        if not path_family:
            result.add_constraint_error(
                f"path_family is required and must be non-null for artifact type: {artifact_type}"
            )

    if artifact_type in EARLY_PHASE_ARTIFACTS:
        path_family = artifact.get("path_family")
        if path_family is not None and not isinstance(path_family, str):
            result.add_constraint_error(
                "path_family must be a string or null in early-phase artifacts"
            )
        # No error if null — that is the correct state before family phase

    # 5. Locked artifacts must have locked=true
    if artifact_type in LOCKED_ARTIFACTS:
        locked = artifact.get("locked")
        if locked is not True:
            result.add_constraint_error(
                f"locked must be true for artifact type: {artifact_type}"
            )

    # 6. viable artifacts must have empty fail_reasons when viable=true
    if artifact_type == "business_viability_report":
        viable = artifact.get("viable")
        fail_reasons = artifact.get("fail_reasons", [])
        if viable is True and fail_reasons:
            result.add_constraint_error(
                f"viable=true but fail_reasons is non-empty: {fail_reasons}"
            )
        if viable is False and not fail_reasons:
            result.add_constraint_error(
                "viable=false but fail_reasons is empty — must explain why"
            )

    # 7. segmentation_gap must have recoverable field be a real boolean
    if artifact_type == "segmentation_gap_report":
        if "recoverable" in artifact and not isinstance(artifact["recoverable"], bool):
            result.add_constraint_error(
                "recoverable must be a boolean (not stringified)"
            )


def validate_artifact(
    artifact_path: Path,
    artifact_type: Optional[str] = None,
) -> ValidationResult:
    """Validate a single artifact file."""
    result = ValidationResult(str(artifact_path), artifact_type)

    # Load artifact
    try:
        with artifact_path.open() as f:
            artifact = json.load(f)
    except json.JSONDecodeError as e:
        result.add_schema_error(f"JSON parse error: {e}")
        return result
    except OSError as e:
        result.add_schema_error(f"Could not read file: {e}")
        return result

    # Detect type if not provided
    if artifact_type is None:
        artifact_type = detect_artifact_type(artifact)
        result.artifact_type = artifact_type

    if artifact_type is None:
        result.add_warning(
            "Could not detect artifact type — skipping schema validation. "
            "Pass --schema explicitly to enforce."
        )
        return result

    if artifact_type not in ARTIFACT_SCHEMA_MAP:
        result.add_constraint_error(f"Unknown artifact type: {artifact_type!r}")
        return result

    # Schema validation
    if not _JSONSCHEMA_AVAILABLE:
        result.add_warning(
            "jsonschema not installed — skipping schema validation. "
            "Install with: pip install jsonschema"
        )
    else:
        try:
            schema = load_schema(artifact_type)
            validator = Draft7Validator(schema)
            for error in sorted(validator.iter_errors(artifact), key=lambda e: e.path):
                path_str = " -> ".join(str(p) for p in error.absolute_path) or "(root)"
                result.add_schema_error(f"[{path_str}] {error.message}")
        except FileNotFoundError as e:
            result.add_schema_error(str(e))

    # Domain constraint checks (run regardless of schema availability)
    _check_domain_constraints(artifact, artifact_type, result)

    return result


def validate_batch(directory: Path, artifact_type: Optional[str] = None) -> List[ValidationResult]:
    """Validate all .json files in a directory."""
    results = []
    json_files = sorted(directory.glob("*.json"))
    if not json_files:
        print(f"No .json files found in {directory}", file=sys.stderr)
        return results
    for json_file in json_files:
        results.append(validate_artifact(json_file, artifact_type=artifact_type))
    return results


def print_results(results: List[ValidationResult], verbose: bool = False) -> int:
    """Print results and return exit code (0=all pass, 1=any failure)."""
    pass_count = 0
    fail_count = 0
    for result in results:
        if result.passed:
            pass_count += 1
            if verbose:
                print(result.summary())
            else:
                print(f"  PASS  {result.artifact_path}")
        else:
            fail_count += 1
            print(result.summary())

    print(f"\nTotal: {pass_count} passed, {fail_count} failed out of {len(results)} artifacts.")
    return 0 if fail_count == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate PC2 discovery artifacts against enforcement schemas."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--artifact", type=Path, help="Path to a single artifact JSON file.")
    group.add_argument("--batch", type=Path, help="Directory of artifact JSON files to validate.")
    parser.add_argument(
        "--schema",
        choices=list(ARTIFACT_SCHEMA_MAP.keys()),
        help="Artifact type / schema to validate against. Auto-detected if omitted.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print PASS results as well.")
    args = parser.parse_args()

    if args.artifact:
        result = validate_artifact(args.artifact, artifact_type=args.schema)
        print(result.summary())
        return 0 if result.passed else 1
    else:
        results = validate_batch(args.batch, artifact_type=args.schema)
        return print_results(results, verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
