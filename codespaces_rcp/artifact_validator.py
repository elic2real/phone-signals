from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from .report_loaders import LoadedReport, load_report

try:
    import jsonschema  # type: ignore
except Exception:  # pragma: no cover
    jsonschema = None


SCHEMA_BY_ARTIFACT = {
    "business_viability_report.json": "business_viability_report.schema.json",
    "path_family_report.json": "path_family_report.schema.json",
    "structure_truth.json": "structure_truth.schema.json",
    "setup_truth.json": "setup_truth.schema.json",
    "trigger_truth.json": "trigger_truth.schema.json",
    "ceiling_report.json": "ceiling_report.schema.json",
    "segmentation_gap_report.json": "segmentation_gap_report.schema.json",
}


@dataclass
class ValidationIssue:
    code: str
    message: str
    severity: str = "error"


def _load_schema(schema_dir: Path, artifact_name: str) -> Dict[str, Any]:
    schema_file = SCHEMA_BY_ARTIFACT[artifact_name]
    path = schema_dir / schema_file
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _fallback_validate(payload: Dict[str, Any], schema: Dict[str, Any]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    required = schema.get("required", [])
    for key in required:
        if key not in payload:
            issues.append(ValidationIssue("SCHEMA_REQUIRED", f"Missing required field: {key}"))
    return issues


def validate_report(loaded: LoadedReport, schema_dir: Path) -> List[ValidationIssue]:
    schema = _load_schema(schema_dir, loaded.artifact_name)
    payload = loaded.payload

    if jsonschema is not None:
        try:
            jsonschema.Draft202012Validator(schema).validate(payload)
            return []
        except Exception as exc:
            return [ValidationIssue("SCHEMA_INVALID", str(exc))]

    return _fallback_validate(payload, schema)


def validate_report_path(report_path: Path, schema_dir: Path) -> List[ValidationIssue]:
    loaded = load_report(report_path)
    return validate_report(loaded, schema_dir)
