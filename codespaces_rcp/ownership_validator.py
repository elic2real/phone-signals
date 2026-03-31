from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .report_loaders import load_report


EXPECTED_OWNER = {
    "business_viability_report.json": "PC2_DISCOVERY",
    "path_family_report.json": "PC2_DISCOVERY",
    "structure_truth.json": "PC2_DISCOVERY",
    "setup_truth.json": "PC2_DISCOVERY",
    "trigger_truth.json": "PC2_DISCOVERY",
    "ceiling_report.json": "PC2_DISCOVERY",
    "segmentation_gap_report.json": "PC2_DISCOVERY",
}


@dataclass
class OwnershipIssue:
    code: str
    message: str
    severity: str = "error"


def validate_ownership(report_path: Path) -> List[OwnershipIssue]:
    loaded = load_report(report_path)
    metadata = loaded.payload.get("metadata", {})
    owner = metadata.get("owner") if isinstance(metadata, dict) else None
    if owner is None:
        owner = loaded.payload.get("produced_by")
    expected = EXPECTED_OWNER.get(loaded.artifact_name)

    issues: List[OwnershipIssue] = []
    if owner is None:
        issues.append(OwnershipIssue("OWNER_MISSING", "metadata.owner is required"))
    elif expected is not None and owner != expected:
        issues.append(
            OwnershipIssue(
                "OWNER_MISMATCH",
                f"Owner '{owner}' does not match expected '{expected}' for {loaded.artifact_name}",
            )
        )
    return issues
