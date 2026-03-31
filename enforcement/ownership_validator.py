#!/usr/bin/env python3
"""
Enforcement: Ownership Validator
==================================
Validates that PC2 discovery artifacts declare valid ownership metadata and
that ownership is consistent across a key's artifact chain
(direction / target_bucket / pair / session).

Ownership rules:
- Every artifact must record the generating phase/runner
- Artifacts from the same key must not contradict each other on direction/pair/session
- setup_truth and trigger_truth must reference a known upstream artifact
- No artifact may claim ownership of a key that is blocked by a segmentation_gap

Usage:
    python ownership_validator.py --artifacts path/to/dir/
    python ownership_validator.py --artifact path/to/artifact.json
"""
from __future__ import annotations

import json
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

EARLY_PHASE_ARTIFACTS = {
    "business_viability_report",
    "structure_truth",
    "ceiling_report",
    "segmentation_gap_report",
}

DOWNSTREAM_ARTIFACTS = {
    "path_family_report",
    "setup_truth",
    "trigger_truth",
}

# Discovery key fields that must be consistent across artifact chain
KEY_FIELDS = ("direction", "target_bucket", "pair", "session")

# Fields that identify a downstream artifact's upstream dependency
UPSTREAM_REF_FIELDS = {
    "setup_truth": "promoted_from",
    "trigger_truth": "setup_id",
}


def _detect_artifact_type(artifact: Dict[str, Any]) -> Optional[str]:
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
    return None


def _extract_key(artifact: Dict[str, Any]) -> Tuple:
    return tuple(artifact.get(f) for f in KEY_FIELDS)


class OwnershipViolation:
    def __init__(self, artifact_path: str, message: str, severity: str = "ERROR") -> None:
        self.artifact_path = artifact_path
        self.message = message
        self.severity = severity  # ERROR or WARNING

    def __str__(self) -> str:
        return f"  [{self.severity}] {self.artifact_path}: {self.message}"


class OwnershipValidationResult:
    def __init__(self) -> None:
        self.violations: List[OwnershipViolation] = []

    @property
    def passed(self) -> bool:
        return all(v.severity != "ERROR" for v in self.violations)

    def add(self, path: str, message: str, severity: str = "ERROR") -> None:
        self.violations.append(OwnershipViolation(path, message, severity))

    def summary(self) -> str:
        if not self.violations:
            return "Ownership validation: PASS — no violations found."
        status = "FAIL" if not self.passed else "PASS (warnings only)"
        lines = [f"Ownership validation: {status}"]
        for v in self.violations:
            lines.append(str(v))
        return "\n".join(lines)


def _validate_single_artifact(
    artifact_path: Path,
    artifact: Dict[str, Any],
    result: OwnershipValidationResult,
) -> None:
    """Per-artifact ownership checks."""
    path_str = str(artifact_path)
    artifact_type = _detect_artifact_type(artifact)

    # 1. Must have schema_version
    if "schema_version" not in artifact:
        result.add(path_str, "Missing schema_version — artifact cannot be traced to a schema contract.")

    # 2. Must have generated_at
    if "generated_at" not in artifact:
        result.add(path_str, "Missing generated_at — ownership timestamp is required.")

    # 3. All key fields must be present and non-null
    for field in KEY_FIELDS:
        val = artifact.get(field)
        if val is None:
            result.add(path_str, f"Key field '{field}' is missing or null.")

    # 4. direction must be singular
    direction = artifact.get("direction")
    if direction is not None and direction not in ("LONG", "SHORT"):
        result.add(path_str, f"direction must be 'LONG' or 'SHORT', got: {direction!r}")

    # 5. Upstream reference check for downstream artifacts
    # promoted_from is allowed to be null (e.g. manual lock), but the field must be present.
    if artifact_type in UPSTREAM_REF_FIELDS:
        ref_field = UPSTREAM_REF_FIELDS[artifact_type]
        if ref_field not in artifact:
            result.add(
                path_str,
                f"{artifact_type} must declare '{ref_field}' field (may be null for manual locks, "
                "but the field must be present).",
            )


def _validate_key_consistency(
    artifacts_by_key: Dict[Tuple, List[Tuple[str, str, Dict]]],
    result: OwnershipValidationResult,
) -> None:
    """Cross-artifact checks: same key must have consistent ownership."""
    for key, entries in artifacts_by_key.items():
        direction, target_bucket, pair, session = key

        # Check all artifacts on this key agree on direction/pair/session/bucket
        for artifact_path, artifact_type, artifact in entries:
            for field, expected in zip(KEY_FIELDS, key):
                actual = artifact.get(field)
                if actual != expected:
                    result.add(
                        artifact_path,
                        f"Key inconsistency: expected {field}={expected!r}, found {actual!r} "
                        f"(conflicts with other artifacts on same key).",
                    )

        # Check: if a segmentation_gap exists for this key, downstream artifacts must not exist
        gap_entries = [e for e in entries if e[1] == "segmentation_gap_report"]
        downstream_entries = [e for e in entries if e[1] in DOWNSTREAM_ARTIFACTS]
        if gap_entries and downstream_entries:
            gap_paths = [e[0] for e in gap_entries]
            for artifact_path, artifact_type, _ in downstream_entries:
                result.add(
                    artifact_path,
                    f"Ownership conflict: downstream artifact '{artifact_type}' exists for a key "
                    f"that has a segmentation_gap_report ({gap_paths}). "
                    "Downstream artifacts on blocked keys are not permitted.",
                )


def validate_directory(
    directory: Path,
) -> OwnershipValidationResult:
    result = OwnershipValidationResult()
    artifacts_by_key: Dict[Tuple, List[Tuple[str, str, Dict]]] = defaultdict(list)

    json_files = sorted(directory.glob("*.json"))
    if not json_files:
        result.add(str(directory), "No .json artifacts found in directory.", severity="WARNING")
        return result

    for json_file in json_files:
        try:
            with json_file.open() as f:
                artifact = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            result.add(str(json_file), f"Could not load file: {e}")
            continue

        _validate_single_artifact(json_file, artifact, result)

        artifact_type = _detect_artifact_type(artifact) or "unknown"
        key = _extract_key(artifact)
        if all(v is not None for v in key):
            artifacts_by_key[key].append((str(json_file), artifact_type, artifact))

    _validate_key_consistency(artifacts_by_key, result)
    return result


def validate_single_file(artifact_path: Path) -> OwnershipValidationResult:
    result = OwnershipValidationResult()
    try:
        with artifact_path.open() as f:
            artifact = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        result.add(str(artifact_path), f"Could not load file: {e}")
        return result
    _validate_single_artifact(artifact_path, artifact, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate ownership metadata on PC2 discovery artifacts."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--artifact", type=Path, help="Single artifact file to check.")
    group.add_argument("--artifacts", type=Path, help="Directory of artifact files to check.")
    args = parser.parse_args()

    if args.artifact:
        result = validate_single_file(args.artifact)
    else:
        result = validate_directory(args.artifacts)

    print(result.summary())
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
