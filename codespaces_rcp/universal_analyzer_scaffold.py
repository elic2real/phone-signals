from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from .report_loaders import SUPPORTED_ARTIFACTS, load_report


@dataclass
class AnalyzerScaffoldResult:
    status: str
    present_artifacts: List[str]
    missing_artifacts: List[str]
    notes: List[str]


def run_scaffold(report_dir: Path) -> AnalyzerScaffoldResult:
    present = []
    missing = []

    for artifact in sorted(SUPPORTED_ARTIFACTS):
        path = report_dir / artifact
        if path.exists():
            try:
                load_report(path)
                present.append(artifact)
            except Exception:
                missing.append(artifact)
        else:
            missing.append(artifact)

    notes = [
        "Scaffold only: no candidate judgement performed.",
        "Promotion logic must remain blocked if required discovery outputs are missing.",
    ]

    status = "READY_FOR_SYNC" if present else "BLOCKED_NO_DISCOVERY_OUTPUT"
    return AnalyzerScaffoldResult(status=status, present_artifacts=present, missing_artifacts=missing, notes=notes)
