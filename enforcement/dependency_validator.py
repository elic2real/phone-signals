#!/usr/bin/env python3
"""
Enforcement: Dependency Validator
===================================
Validates that PC2 artifacts are presented in valid phase order and that
downstream artifacts do not exist without their required upstream artifacts.

Dependency chain (Codespaces enforcement model):
    Phase 0: business_viability_report  (no upstream required)
    Phase 1: path_family_report         (requires viable business_viability_report)
    Phase 2: structure_truth            (requires business_viability_report; path_family optional)
             ceiling_report             (requires business_viability_report; path_family optional)
             segmentation_gap_report    (no blocked upstream required)
    Phase 3: setup_truth                (requires structure_truth + path_family_report)
             trigger_truth              (requires setup_truth)

Usage:
    python dependency_validator.py --artifacts path/to/dir/
    python dependency_validator.py --check-pair direction=LONG target_bucket=2.5 pair=EUR_USD session=london --artifacts path/to/dir/
"""
from __future__ import annotations

import json
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

KEY_FIELDS = ("direction", "target_bucket", "pair", "session")

# Phase ordering — lower number = earlier phase
PHASE_ORDER = {
    "business_viability_report": 0,
    "segmentation_gap_report": 0,
    "path_family_report": 1,
    "structure_truth": 2,
    "ceiling_report": 2,
    "setup_truth": 3,
    "trigger_truth": 4,
}

# Which upstream artifact types are REQUIRED before a given type can exist
REQUIRED_UPSTREAMS: Dict[str, List[str]] = {
    "path_family_report": ["business_viability_report"],
    "structure_truth": ["business_viability_report"],
    "ceiling_report": ["business_viability_report"],
    "setup_truth": ["business_viability_report", "structure_truth", "path_family_report"],
    "trigger_truth": ["setup_truth"],
}

# Upstream types that must be viable/non-blocked for downstream to proceed
VIABILITY_GATED: Dict[str, str] = {
    "path_family_report": "business_viability_report",
    "structure_truth": "business_viability_report",
    "ceiling_report": "business_viability_report",
    "setup_truth": "business_viability_report",
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


class DependencyViolation:
    def __init__(self, message: str, severity: str = "ERROR") -> None:
        self.message = message
        self.severity = severity

    def __str__(self) -> str:
        return f"  [{self.severity}] {self.message}"


class DependencyValidationResult:
    def __init__(self) -> None:
        self.violations: List[DependencyViolation] = []

    @property
    def passed(self) -> bool:
        return all(v.severity != "ERROR" for v in self.violations)

    def add(self, message: str, severity: str = "ERROR") -> None:
        self.violations.append(DependencyViolation(message, severity))

    def summary(self) -> str:
        if not self.violations:
            return "Dependency validation: PASS — no violations found."
        status = "FAIL" if not self.passed else "PASS (warnings only)"
        lines = [f"Dependency validation: {status}"]
        for v in self.violations:
            lines.append(str(v))
        return "\n".join(lines)


def _key_label(key: Tuple) -> str:
    return f"direction={key[0]} bucket={key[1]} pair={key[2]} session={key[3]}"


def validate_dependency_chain(
    artifacts_by_key: Dict[Tuple, Dict[str, Dict]],
    result: DependencyValidationResult,
) -> None:
    """
    For each discovery key, check that the artifact chain satisfies phase order
    and required upstreams.
    """
    for key, type_to_artifact in artifacts_by_key.items():
        present_types: Set[str] = set(type_to_artifact.keys())
        key_label = _key_label(key)

        # Check required upstreams
        for artifact_type, required in REQUIRED_UPSTREAMS.items():
            if artifact_type not in present_types:
                continue  # not present, no violation
            for upstream_type in required:
                if upstream_type not in present_types:
                    result.add(
                        f"Key [{key_label}]: '{artifact_type}' exists but required upstream "
                        f"'{upstream_type}' is missing."
                    )

        # Check viability gating
        for artifact_type, viability_source in VIABILITY_GATED.items():
            if artifact_type not in present_types:
                continue
            if viability_source not in present_types:
                continue  # already caught above
            upstream_artifact = type_to_artifact[viability_source]
            if viability_source == "business_viability_report":
                viable = upstream_artifact.get("viable")
                if viable is False:
                    result.add(
                        f"Key [{key_label}]: '{artifact_type}' exists but "
                        f"business_viability_report.viable=false — "
                        "downstream artifacts must not exist for non-viable keys."
                    )

        # Check: setup_truth requires path_family to be non-null
        if "setup_truth" in present_types:
            setup = type_to_artifact["setup_truth"]
            if not setup.get("path_family"):
                result.add(
                    f"Key [{key_label}]: setup_truth.path_family is null/missing — "
                    "setup cannot be locked without a path family."
                )

        # Check: trigger_truth must reference a setup_id that corresponds to setup_truth
        if "trigger_truth" in present_types and "setup_truth" in present_types:
            trigger = type_to_artifact["trigger_truth"]
            setup = type_to_artifact["setup_truth"]
            trigger_setup_id = trigger.get("setup_id")
            setup_id = setup.get("setup_id")
            if trigger_setup_id and setup_id and trigger_setup_id != setup_id:
                result.add(
                    f"Key [{key_label}]: trigger_truth.setup_id={trigger_setup_id!r} "
                    f"does not match setup_truth.setup_id={setup_id!r}."
                )

        # Check: if segmentation_gap exists and is non-recoverable, downstream must not proceed
        if "segmentation_gap_report" in present_types:
            gap = type_to_artifact["segmentation_gap_report"]
            recoverable = gap.get("recoverable", True)
            downstream_blocked = gap.get("downstream_blocked", [])
            for artifact_type in present_types:
                if PHASE_ORDER.get(artifact_type, 0) > 0 and artifact_type != "segmentation_gap_report":
                    if not recoverable or artifact_type in downstream_blocked:
                        result.add(
                            f"Key [{key_label}]: '{artifact_type}' exists but segmentation_gap_report "
                            f"marks this key as unrecoverable / blocking downstream."
                        )


def load_artifacts_from_directory(
    directory: Path,
) -> Tuple[Dict[Tuple, Dict[str, Dict]], List[str]]:
    """
    Load all .json artifacts from directory.
    Returns:
        artifacts_by_key: {key: {artifact_type: artifact_dict}}
        load_errors: list of error strings
    """
    artifacts_by_key: Dict[Tuple, Dict[str, Dict]] = defaultdict(dict)
    load_errors: List[str] = []

    for json_file in sorted(directory.glob("*.json")):
        try:
            with json_file.open() as f:
                artifact = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            load_errors.append(f"Could not load {json_file}: {e}")
            continue

        artifact_type = _detect_artifact_type(artifact)
        if artifact_type is None:
            continue  # not a known PC2 artifact, skip

        key = _extract_key(artifact)
        if any(v is None for v in key):
            load_errors.append(
                f"{json_file}: missing key fields {KEY_FIELDS} — cannot index for dependency check."
            )
            continue

        artifacts_by_key[key][artifact_type] = artifact

    return artifacts_by_key, load_errors


def validate_directory(directory: Path) -> DependencyValidationResult:
    result = DependencyValidationResult()
    artifacts_by_key, load_errors = load_artifacts_from_directory(directory)

    for err in load_errors:
        result.add(err, severity="WARNING")

    if not artifacts_by_key:
        result.add(f"No indexable PC2 artifacts found in {directory}", severity="WARNING")
        return result

    validate_dependency_chain(artifacts_by_key, result)
    return result


def validate_single_key(
    directory: Path,
    direction: str,
    target_bucket: float,
    pair: str,
    session: str,
) -> DependencyValidationResult:
    result = DependencyValidationResult()
    artifacts_by_key, load_errors = load_artifacts_from_directory(directory)

    for err in load_errors:
        result.add(err, severity="WARNING")

    key = (direction, target_bucket, pair, session)
    if key not in artifacts_by_key:
        result.add(
            f"No artifacts found for key {_key_label(key)} in {directory}.",
            severity="WARNING",
        )
        return result

    validate_dependency_chain({key: artifacts_by_key[key]}, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate PC2 artifact dependency chains."
    )
    parser.add_argument(
        "--artifacts", type=Path, required=True,
        help="Directory containing PC2 artifact .json files."
    )
    parser.add_argument("--direction", help="Filter to a specific direction (LONG/SHORT).")
    parser.add_argument("--target-bucket", type=float, help="Filter to a specific target bucket.")
    parser.add_argument("--pair", help="Filter to a specific pair (e.g. EUR_USD).")
    parser.add_argument("--session", help="Filter to a specific session.")
    args = parser.parse_args()

    if all([args.direction, args.target_bucket, args.pair, args.session]):
        result = validate_single_key(
            args.artifacts,
            args.direction,
            args.target_bucket,
            args.pair,
            args.session,
        )
    else:
        result = validate_directory(args.artifacts)

    print(result.summary())
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
