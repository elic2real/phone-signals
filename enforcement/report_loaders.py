#!/usr/bin/env python3
"""
Enforcement: Report Loaders / Parsers
=======================================
Typed loaders for all PC2 discovery artifact types. These parsers:
- Load artifact JSON files and return typed Python objects
- Validate presence of required fields at load time
- Raise descriptive errors for malformed artifacts
- Do NOT perform business logic — they only parse and expose the data

These loaders will be used by the universal analyzer and promotion gate
once real PC2 artifacts exist (Trunk 3 onward).

Usage:
    from enforcement.report_loaders import load_business_viability_report
    report = load_business_viability_report("path/to/report.json")
    print(report.viable, report.pair, report.session)
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar


# ---------------------------------------------------------------------------
# Base loader
# ---------------------------------------------------------------------------

REQUIRED_KEY_FIELDS = ("direction", "target_bucket", "pair", "session")


class ArtifactLoadError(ValueError):
    """Raised when an artifact file cannot be parsed into the expected shape."""
    pass


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open() as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ArtifactLoadError(f"JSON parse error in {path}: {e}") from e
    except OSError as e:
        raise ArtifactLoadError(f"Cannot read {path}: {e}") from e


def _require(data: Dict[str, Any], field_name: str, path: Path) -> Any:
    if field_name not in data:
        raise ArtifactLoadError(f"Required field '{field_name}' missing in {path}")
    return data[field_name]


def _require_key_fields(data: Dict[str, Any], path: Path) -> None:
    for f in REQUIRED_KEY_FIELDS:
        _require(data, f, path)


# ---------------------------------------------------------------------------
# Typed artifact records
# ---------------------------------------------------------------------------

@dataclass
class BusinessViabilityReport:
    path: Path
    schema_version: str
    generated_at: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    trade_count: int
    viable: bool
    fail_reasons: List[str]
    win_rate: Optional[float] = None
    avg_capture_pips: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)


@dataclass
class PathFamilyEntry:
    path_family: str
    member_count: int
    centroid_features: Dict[str, Any]
    win_rate: Optional[float] = None
    avg_capture_pips: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PathFamilyReport:
    path: Path
    schema_version: str
    generated_at: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    family_count: int
    families: List[PathFamilyEntry]
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)

    def family_names(self) -> List[str]:
        return [f.path_family for f in self.families]


@dataclass
class StructureTruth:
    path: Path
    schema_version: str
    generated_at: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    structure_label: str
    label_confidence: float
    sample_count: int
    label_source: str
    path_family: Optional[str] = None
    feature_snapshot: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)

    def is_labeled(self) -> bool:
        return self.structure_label != "unknown"


@dataclass
class CeilingMetrics:
    win_rate_ceiling: float
    capture_pips_ceiling: float
    profit_ceiling: float
    pips_per_hour_ceiling: Optional[float] = None
    equity_per_hour_ceiling: Optional[float] = None


@dataclass
class CeilingReport:
    path: Path
    schema_version: str
    generated_at: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    ceiling_metrics: CeilingMetrics
    population_size: int
    path_family: Optional[str] = None
    ceiling_method: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)


@dataclass
class SegmentationGapReport:
    path: Path
    schema_version: str
    generated_at: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    gap_type: str
    gap_details: Dict[str, Any]
    recoverable: bool
    downstream_blocked: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)

    def blocks_setup(self) -> bool:
        return (not self.recoverable) or ("setup_lock" in self.downstream_blocked)


@dataclass
class EntryFilterCondition:
    field: str
    op: str
    value: Any


@dataclass
class EntryFilter:
    conditions: List[EntryFilterCondition]
    logic: str = "AND"


@dataclass
class SetupTruth:
    path: Path
    schema_version: str
    generated_at: str
    setup_id: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    path_family: str
    structure_label: str
    entry_filter: EntryFilter
    population_size: int
    locked: bool
    win_rate: Optional[float] = None
    avg_capture_pips: Optional[float] = None
    locked_at: Optional[str] = None
    promoted_from: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)


@dataclass
class TriggerConditionEntry:
    field: str
    op: str
    value: Any
    description: Optional[str] = None


@dataclass
class TriggerConditions:
    entry_signals: List[TriggerConditionEntry]
    kill_conditions: List[TriggerConditionEntry]
    confirmation_window_bars: Optional[int] = None
    logic: str = "AND"


@dataclass
class TriggerTruth:
    path: Path
    schema_version: str
    generated_at: str
    trigger_id: str
    setup_id: str
    direction: str
    target_bucket: float
    pair: str
    session: str
    path_family: str
    trigger_conditions: TriggerConditions
    locked: bool
    locked_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    _raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    def key(self) -> tuple:
        return (self.direction, self.target_bucket, self.pair, self.session)


# ---------------------------------------------------------------------------
# Loader functions
# ---------------------------------------------------------------------------

def load_business_viability_report(path: Path | str) -> BusinessViabilityReport:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    return BusinessViabilityReport(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        trade_count=_require(data, "trade_count", path),
        viable=_require(data, "viable", path),
        fail_reasons=_require(data, "fail_reasons", path),
        win_rate=data.get("win_rate"),
        avg_capture_pips=data.get("avg_capture_pips"),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_path_family_report(path: Path | str) -> PathFamilyReport:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    families_raw = _require(data, "families", path)
    families = []
    for entry in families_raw:
        families.append(PathFamilyEntry(
            path_family=entry["path_family"],
            member_count=entry["member_count"],
            centroid_features=entry.get("centroid_features", {}),
            win_rate=entry.get("win_rate"),
            avg_capture_pips=entry.get("avg_capture_pips"),
            metadata=entry.get("metadata", {}),
        ))
    return PathFamilyReport(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        family_count=_require(data, "family_count", path),
        families=families,
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_structure_truth(path: Path | str) -> StructureTruth:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    return StructureTruth(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        structure_label=_require(data, "structure_label", path),
        label_confidence=_require(data, "label_confidence", path),
        sample_count=_require(data, "sample_count", path),
        label_source=_require(data, "label_source", path),
        path_family=data.get("path_family"),
        feature_snapshot=data.get("feature_snapshot", {}),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_ceiling_report(path: Path | str) -> CeilingReport:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    cm_raw = _require(data, "ceiling_metrics", path)
    ceiling_metrics = CeilingMetrics(
        win_rate_ceiling=_require(cm_raw, "win_rate_ceiling", path),
        capture_pips_ceiling=_require(cm_raw, "capture_pips_ceiling", path),
        profit_ceiling=_require(cm_raw, "profit_ceiling", path),
        pips_per_hour_ceiling=cm_raw.get("pips_per_hour_ceiling"),
        equity_per_hour_ceiling=cm_raw.get("equity_per_hour_ceiling"),
    )
    return CeilingReport(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        ceiling_metrics=ceiling_metrics,
        population_size=_require(data, "population_size", path),
        path_family=data.get("path_family"),
        ceiling_method=data.get("ceiling_method"),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_segmentation_gap_report(path: Path | str) -> SegmentationGapReport:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    return SegmentationGapReport(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        gap_type=_require(data, "gap_type", path),
        gap_details=_require(data, "gap_details", path),
        recoverable=_require(data, "recoverable", path),
        downstream_blocked=data.get("downstream_blocked", []),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_setup_truth(path: Path | str) -> SetupTruth:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    ef_raw = _require(data, "entry_filter", path)
    conditions = [
        EntryFilterCondition(
            field=c["field"],
            op=c["op"],
            value=c["value"],
        )
        for c in ef_raw.get("conditions", [])
    ]
    entry_filter = EntryFilter(conditions=conditions, logic=ef_raw.get("logic", "AND"))
    return SetupTruth(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        setup_id=_require(data, "setup_id", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        path_family=_require(data, "path_family", path),
        structure_label=_require(data, "structure_label", path),
        entry_filter=entry_filter,
        population_size=_require(data, "population_size", path),
        locked=_require(data, "locked", path),
        win_rate=data.get("win_rate"),
        avg_capture_pips=data.get("avg_capture_pips"),
        locked_at=data.get("locked_at"),
        promoted_from=data.get("promoted_from"),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


def load_trigger_truth(path: Path | str) -> TriggerTruth:
    path = Path(path)
    data = _load_json(path)
    _require_key_fields(data, path)
    tc_raw = _require(data, "trigger_conditions", path)

    def _parse_conds(raw_list: List[Dict]) -> List[TriggerConditionEntry]:
        return [
            TriggerConditionEntry(
                field=c["field"],
                op=c["op"],
                value=c["value"],
                description=c.get("description"),
            )
            for c in raw_list
        ]

    trigger_conditions = TriggerConditions(
        entry_signals=_parse_conds(tc_raw.get("entry_signals", [])),
        kill_conditions=_parse_conds(tc_raw.get("kill_conditions", [])),
        confirmation_window_bars=tc_raw.get("confirmation_window_bars"),
        logic=tc_raw.get("logic", "AND"),
    )
    return TriggerTruth(
        path=path,
        schema_version=_require(data, "schema_version", path),
        generated_at=_require(data, "generated_at", path),
        trigger_id=_require(data, "trigger_id", path),
        setup_id=_require(data, "setup_id", path),
        direction=_require(data, "direction", path),
        target_bucket=_require(data, "target_bucket", path),
        pair=_require(data, "pair", path),
        session=_require(data, "session", path),
        path_family=_require(data, "path_family", path),
        trigger_conditions=trigger_conditions,
        locked=_require(data, "locked", path),
        locked_at=data.get("locked_at"),
        metadata=data.get("metadata", {}),
        _raw=data,
    )


# ---------------------------------------------------------------------------
# Auto-dispatch loader
# ---------------------------------------------------------------------------

_TYPE_LOADERS = {
    "business_viability_report": load_business_viability_report,
    "path_family_report": load_path_family_report,
    "structure_truth": load_structure_truth,
    "ceiling_report": load_ceiling_report,
    "segmentation_gap_report": load_segmentation_gap_report,
    "setup_truth": load_setup_truth,
    "trigger_truth": load_trigger_truth,
}


def load_artifact(path: Path | str, artifact_type: Optional[str] = None):
    """
    Auto-dispatch loader. Detects artifact type from content if not provided.
    Returns the appropriate typed dataclass.
    """
    path = Path(path)
    data = _load_json(path)

    if artifact_type is None:
        from artifact_validator import detect_artifact_type
        artifact_type = detect_artifact_type(data)

    if artifact_type is None:
        raise ArtifactLoadError(
            f"Cannot detect artifact type for {path}. Pass artifact_type explicitly."
        )

    loader = _TYPE_LOADERS.get(artifact_type)
    if loader is None:
        raise ArtifactLoadError(f"No loader registered for artifact type: {artifact_type!r}")

    return loader(path)


def load_artifact_directory(
    directory: Path | str,
    artifact_type: Optional[str] = None,
) -> List[Any]:
    """Load all .json artifacts from a directory, returning typed records."""
    directory = Path(directory)
    results = []
    for json_file in sorted(directory.glob("*.json")):
        try:
            record = load_artifact(json_file, artifact_type=artifact_type)
            results.append(record)
        except ArtifactLoadError:
            pass  # Non-matching files skipped silently
    return results


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Test-load PC2 artifact files.")
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--type", dest="artifact_type", default=None)
    args = parser.parse_args()

    exit_code = 0
    for p in args.paths:
        try:
            record = load_artifact(p, artifact_type=args.artifact_type)
            print(f"  LOADED  {p}  →  {type(record).__name__}  key={record.key()}")
        except ArtifactLoadError as e:
            print(f"  ERROR   {p}  →  {e}")
            exit_code = 1

    sys.exit(exit_code)
