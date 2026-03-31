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


STAGE_BY_ARTIFACT = {
    "business_viability_report.json": "phase0",
    "path_family_report.json": "phase1",
    "structure_truth.json": "phase2",
    "setup_truth.json": "phase3",
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


def _normalize_target_bucket(record: Dict[str, Any]) -> str:
    value = record.get("target_bucket")
    if value is None:
        value = record.get("target_bucket_pips")
    if value is None:
        return ""
    return str(value)


def _map_record_to_schema_payload(
    loaded: LoadedReport,
    root_payload: Dict[str, Any],
    record: Dict[str, Any],
) -> Dict[str, Any]:
    base = {
        "artifact": loaded.artifact_name,
        "metadata": {
            "owner": root_payload.get("produced_by") or root_payload.get("owner") or "",
            "stage": STAGE_BY_ARTIFACT.get(loaded.artifact_name, ""),
            "generated_at": root_payload.get("run_ts_utc") or root_payload.get("generated_at") or "",
        },
        "key": {
            "direction": record.get("direction", ""),
            "target_bucket": _normalize_target_bucket(record),
            "pair": record.get("pair", ""),
            "session": record.get("session", ""),
        },
    }

    if loaded.artifact_name == "business_viability_report.json":
        status = "viable" if bool(record.get("viable")) else "killed"
        base["viability"] = {
            "status": status,
            "metrics": record,
        }
    elif loaded.artifact_name == "path_family_report.json":
        sample_size = record.get("sample_size") or 0
        dominant_count = record.get("dominant_count") or 0
        confidence = float(dominant_count / sample_size) if sample_size else 0.0
        base["path_family"] = {
            "id": str(record.get("dominant_family", "")),
            "confidence": confidence,
            "features": {
                "dominant_count": dominant_count,
                "sample_size": sample_size,
                "non_random_verdict": bool(record.get("non_random_verdict")),
            },
        }
    elif loaded.artifact_name == "structure_truth.json":
        base["structure"] = {
            "label": str(record.get("dominant_structure", "")),
            "evidence": [
                f"placed_count={record.get('placed_count', 0)}",
                f"placement_rate={record.get('placement_rate', 0)}",
                f"consistent_verdict={bool(record.get('consistent_verdict'))}",
            ],
        }
    elif loaded.artifact_name == "setup_truth.json":
        base["key"]["path_family"] = str(record.get("path_family", ""))
        base["key"]["structure_label"] = str(record.get("structure_label", ""))
        base["setup"] = {
            "status": str(record.get("status", "")),
            "signals": {
                "setup_label": record.get("setup_label", ""),
                "causal_signature": record.get("causal_signature", {}),
                "expectancy": record.get("expectancy", 0.0),
                "mae_profile": record.get("mae_profile", {}),
                "sample_count": record.get("sample_count", 0),
            },
        }

    return base


def _expand_payloads_for_validation(loaded: LoadedReport) -> List[Dict[str, Any]]:
    payload = loaded.payload
    records = payload.get("records")
    if isinstance(records, list) and records:
        expanded: List[Dict[str, Any]] = []
        for record in records:
            if isinstance(record, dict):
                expanded.append(_map_record_to_schema_payload(loaded, payload, record))
        if expanded:
            return expanded
    return [payload]


def validate_report(loaded: LoadedReport, schema_dir: Path) -> List[ValidationIssue]:
    schema = _load_schema(schema_dir, loaded.artifact_name)
    payloads = _expand_payloads_for_validation(loaded)

    all_issues: List[ValidationIssue] = []
    for index, payload in enumerate(payloads):
        if jsonschema is not None:
            try:
                jsonschema.Draft202012Validator(schema).validate(payload)
                continue
            except Exception as exc:
                all_issues.append(ValidationIssue("SCHEMA_INVALID", f"record[{index}] {exc}"))
                continue

        fallback_issues = _fallback_validate(payload, schema)
        for issue in fallback_issues:
            all_issues.append(
                ValidationIssue(
                    code=issue.code,
                    message=f"record[{index}] {issue.message}",
                    severity=issue.severity,
                )
            )

    return all_issues


def validate_report_path(report_path: Path, schema_dir: Path) -> List[ValidationIssue]:
    loaded = load_report(report_path)
    return validate_report(loaded, schema_dir)
