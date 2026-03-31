from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List


REQUIRED_FOR_PROMOTION = [
    "business_viability_report.json",
    "path_family_report.json",
    "structure_truth.json",
    "setup_truth.json",
    "trigger_truth.json",
    "ceiling_report.json",
]


@dataclass
class PromotionGateResult:
    status: str
    missing_artifacts: List[str]
    message: str


def evaluate_promotion_gate(report_dir: Path) -> PromotionGateResult:
    missing = [name for name in REQUIRED_FOR_PROMOTION if not (report_dir / name).exists()]
    if missing:
        return PromotionGateResult(
            status="BLOCKED_NO_DISCOVERY_OUTPUT",
            missing_artifacts=missing,
            message="Promotion gate blocked: required discovery outputs are missing.",
        )
    return PromotionGateResult(
        status="READY_FOR_PROMOTION_CHECK",
        missing_artifacts=[],
        message="All required discovery artifacts exist. Promotion checks may proceed in a later trunk.",
    )
