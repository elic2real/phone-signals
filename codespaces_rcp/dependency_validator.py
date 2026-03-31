from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .report_loaders import load_report


DEPENDENCIES = {
    "business_viability_report.json": [],
    "path_family_report.json": ["business_viability_report.json"],
    "structure_truth.json": ["business_viability_report.json"],
    "setup_truth.json": ["structure_truth.json"],
    "trigger_truth.json": ["setup_truth.json"],
    "ceiling_report.json": ["trigger_truth.json"],
    "segmentation_gap_report.json": ["business_viability_report.json"],
}


@dataclass
class DependencyIssue:
    code: str
    message: str
    severity: str = "error"


def _artifact_exists(report_dir: Path, artifact_name: str) -> bool:
    return (report_dir / artifact_name).exists()


def validate_dependencies(report_path: Path, report_dir: Path) -> List[DependencyIssue]:
    loaded = load_report(report_path)
    required = DEPENDENCIES.get(loaded.artifact_name, [])
    issues: List[DependencyIssue] = []

    for dep in required:
        if not _artifact_exists(report_dir, dep):
            issues.append(
                DependencyIssue(
                    "DEPENDENCY_MISSING",
                    f"{loaded.artifact_name} requires {dep} to exist before validation/promotion phases.",
                )
            )

    return issues
