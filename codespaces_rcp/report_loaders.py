from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


SUPPORTED_ARTIFACTS = {
    "business_viability_report.json",
    "path_family_report.json",
    "structure_truth.json",
    "setup_truth.json",
    "trigger_truth.json",
    "ceiling_report.json",
    "segmentation_gap_report.json",
}


@dataclass
class LoadedReport:
    artifact_name: str
    path: Path
    payload: Dict[str, Any]


def load_json_report(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def detect_artifact_name(payload: Dict[str, Any], fallback_path: Path) -> Optional[str]:
    declared = payload.get("artifact")
    if isinstance(declared, str) and declared in SUPPORTED_ARTIFACTS:
        return declared
    if fallback_path.name in SUPPORTED_ARTIFACTS:
        return fallback_path.name
    return None


def load_report(path: Path) -> LoadedReport:
    payload = load_json_report(path)
    artifact_name = detect_artifact_name(payload, path)
    if artifact_name is None:
        raise ValueError(f"Unsupported artifact at {path}")
    return LoadedReport(artifact_name=artifact_name, path=path, payload=payload)
