#!/usr/bin/env python3
"""
Enforcement: Universal Analyzer Scaffolding
=============================================
Scaffolding for the universal analyzer that will consume validated PC2 discovery
artifacts and route them to the appropriate analysis pass.

At this phase (parallel setup trunk), this module:
- Defines the analyzer interface contract
- Defines the AnalysisContext and AnalysisResult shapes
- Defines the analyzer registry
- Provides a no-op runner that accepts valid artifacts and confirms routing

HARD CONSTRAINTS enforced:
- Will NOT analyze artifacts that have not passed validation
- Will NOT judge real candidates — analysis methods return NotImplemented stubs
- Will NOT make archetype conclusions on missing data
- Will NOT run promotion logic (that is the promotion gate's job)

Usage:
    python universal_analyzer.py --artifacts path/to/dir/ --dry-run
"""
from __future__ import annotations

import json
import sys
import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type

# ---------------------------------------------------------------------------
# Analysis context — the unit of work passed to every analyzer
# ---------------------------------------------------------------------------

@dataclass
class AnalysisKey:
    """Canonical discovery key. All analyzers are keyed by this."""
    direction: str        # LONG or SHORT
    target_bucket: float  # pip bucket
    pair: str             # e.g. EUR_USD
    session: str          # e.g. london

    def __str__(self) -> str:
        return f"{self.direction}_{self.pair}_{self.session}_{self.target_bucket}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "direction": self.direction,
            "target_bucket": self.target_bucket,
            "pair": self.pair,
            "session": self.session,
        }


@dataclass
class ArtifactBundle:
    """
    All available discovery artifacts for a single AnalysisKey.
    Fields are None until the corresponding PC2 artifact exists and is valid.
    """
    key: AnalysisKey
    business_viability_report: Optional[Dict[str, Any]] = None
    path_family_report: Optional[Dict[str, Any]] = None
    structure_truth: Optional[Dict[str, Any]] = None
    ceiling_report: Optional[Dict[str, Any]] = None
    segmentation_gap_report: Optional[Dict[str, Any]] = None
    setup_truth: Optional[Dict[str, Any]] = None
    trigger_truth: Optional[Dict[str, Any]] = None

    def available_types(self) -> List[str]:
        result = []
        if self.business_viability_report is not None:
            result.append("business_viability_report")
        if self.path_family_report is not None:
            result.append("path_family_report")
        if self.structure_truth is not None:
            result.append("structure_truth")
        if self.ceiling_report is not None:
            result.append("ceiling_report")
        if self.segmentation_gap_report is not None:
            result.append("segmentation_gap_report")
        if self.setup_truth is not None:
            result.append("setup_truth")
        if self.trigger_truth is not None:
            result.append("trigger_truth")
        return result

    def is_viable(self) -> Optional[bool]:
        """Returns None if viability unknown, True/False if report present."""
        if self.business_viability_report is None:
            return None
        return self.business_viability_report.get("viable")

    def has_family(self) -> bool:
        return self.path_family_report is not None and bool(
            self.path_family_report.get("families")
        )

    def has_structure(self) -> bool:
        return self.structure_truth is not None


# ---------------------------------------------------------------------------
# Analysis result
# ---------------------------------------------------------------------------

@dataclass
class AnalysisResult:
    """
    Output of a single analyzer pass on an ArtifactBundle.
    At scaffolding phase, concrete findings will be empty — only routing is confirmed.
    """
    analyzer_name: str
    key: AnalysisKey
    status: str  # "not_run", "stub", "complete", "blocked", "error"
    findings: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    raw: Optional[Dict[str, Any]] = None

    @property
    def passed(self) -> bool:
        return self.status not in ("error", "blocked")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "analyzer": self.analyzer_name,
            "key": self.key.to_dict(),
            "status": self.status,
            "findings": self.findings,
            "warnings": self.warnings,
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Analyzer base class
# ---------------------------------------------------------------------------

class BaseAnalyzer(ABC):
    """Base class for all PC2 artifact analyzers."""

    name: str = "base"
    description: str = ""

    # Artifact types this analyzer requires to run (at minimum)
    required_artifact_types: List[str] = []

    # Artifact types this analyzer will consume if present
    optional_artifact_types: List[str] = []

    def can_run(self, bundle: ArtifactBundle) -> bool:
        """Returns True if the bundle has the minimum required artifacts."""
        available = set(bundle.available_types())
        return all(t in available for t in self.required_artifact_types)

    def run(self, bundle: ArtifactBundle) -> AnalysisResult:
        """
        Main entry point. Checks prerequisites, then calls analyze().
        """
        if not self.can_run(bundle):
            missing = [t for t in self.required_artifact_types if t not in bundle.available_types()]
            return AnalysisResult(
                analyzer_name=self.name,
                key=bundle.key,
                status="blocked",
                warnings=[f"Missing required artifact(s): {missing}"],
            )
        try:
            return self.analyze(bundle)
        except NotImplementedError:
            return AnalysisResult(
                analyzer_name=self.name,
                key=bundle.key,
                status="stub",
                warnings=[f"Analyzer '{self.name}' is a scaffolding stub — not yet implemented."],
            )

    @abstractmethod
    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        """Perform the analysis. Raise NotImplementedError until implemented."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Stub analyzers — scaffolding stubs registered now, implemented in Trunk 3
# ---------------------------------------------------------------------------

class ViabilityPassAnalyzer(BaseAnalyzer):
    """Confirms that a key passed viability and routes to next phase."""
    name = "viability_pass"
    description = "Confirms business viability and routes viable keys forward."
    required_artifact_types = ["business_viability_report"]

    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        raise NotImplementedError


class FamilyCompletenessAnalyzer(BaseAnalyzer):
    """Checks that path family clustering produced expected family count."""
    name = "family_completeness"
    description = "Validates family clustering output completeness."
    required_artifact_types = ["business_viability_report", "path_family_report"]

    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        raise NotImplementedError


class StructureConsistencyAnalyzer(BaseAnalyzer):
    """Checks structure labels are consistent with family assignment."""
    name = "structure_consistency"
    description = "Validates structure labels against family context."
    required_artifact_types = ["structure_truth"]
    optional_artifact_types = ["path_family_report"]

    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        raise NotImplementedError


class CeilingReachabilityAnalyzer(BaseAnalyzer):
    """Checks that ceiling metrics are within credible bounds."""
    name = "ceiling_reachability"
    description = "Validates ceiling report metrics for credibility."
    required_artifact_types = ["ceiling_report", "business_viability_report"]

    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        raise NotImplementedError


class GapRoutingAnalyzer(BaseAnalyzer):
    """Routes segmentation gaps to appropriate recovery or burial paths."""
    name = "gap_routing"
    description = "Routes segmentation gaps to recovery or permanent block."
    required_artifact_types = ["segmentation_gap_report"]

    def analyze(self, bundle: ArtifactBundle) -> AnalysisResult:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Analyzer registry
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, Type[BaseAnalyzer]] = {}


def register_analyzer(cls: Type[BaseAnalyzer]) -> Type[BaseAnalyzer]:
    """Decorator to register an analyzer class."""
    _REGISTRY[cls.name] = cls
    return cls


# Register all stub analyzers
for _analyzer_cls in [
    ViabilityPassAnalyzer,
    FamilyCompletenessAnalyzer,
    StructureConsistencyAnalyzer,
    CeilingReachabilityAnalyzer,
    GapRoutingAnalyzer,
]:
    register_analyzer(_analyzer_cls)


def get_registered_analyzers() -> List[str]:
    return list(_REGISTRY.keys())


def get_analyzer(name: str) -> BaseAnalyzer:
    if name not in _REGISTRY:
        raise KeyError(f"Analyzer '{name}' not registered. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]()


# ---------------------------------------------------------------------------
# Bundle builder — constructs ArtifactBundle from a directory
# ---------------------------------------------------------------------------

def _detect_type(artifact: Dict[str, Any]) -> Optional[str]:
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


_BUNDLE_FIELD_MAP = {
    "business_viability_report": "business_viability_report",
    "path_family_report": "path_family_report",
    "structure_truth": "structure_truth",
    "setup_truth": "setup_truth",
    "trigger_truth": "trigger_truth",
    "ceiling_report": "ceiling_report",
    "segmentation_gap_report": "segmentation_gap_report",
}


def build_bundles_from_directory(directory: Path) -> List[ArtifactBundle]:
    """Build one ArtifactBundle per discovery key from a directory of artifact files."""
    from collections import defaultdict

    by_key: Dict[tuple, Dict[str, Any]] = defaultdict(dict)

    for json_file in sorted(directory.glob("*.json")):
        try:
            with json_file.open() as f:
                artifact = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        artifact_type = _detect_type(artifact)
        if not artifact_type:
            continue

        key = (
            artifact.get("direction"),
            artifact.get("target_bucket"),
            artifact.get("pair"),
            artifact.get("session"),
        )
        if any(v is None for v in key):
            continue

        by_key[key][artifact_type] = artifact

    bundles = []
    for key, type_map in by_key.items():
        direction, target_bucket, pair, session = key
        analysis_key = AnalysisKey(
            direction=direction,
            target_bucket=target_bucket,
            pair=pair,
            session=session,
        )
        bundle = ArtifactBundle(key=analysis_key)
        for artifact_type, artifact in type_map.items():
            field_name = _BUNDLE_FIELD_MAP.get(artifact_type)
            if field_name:
                setattr(bundle, field_name, artifact)
        bundles.append(bundle)

    return bundles


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class UniversalAnalyzerRunner:
    """
    Runs all registered analyzers over a set of ArtifactBundles.
    At scaffolding phase, this confirms routing only — no real analysis output.
    """

    def __init__(self, analyzers: Optional[List[str]] = None) -> None:
        if analyzers is None:
            self._analyzers = [cls() for cls in _REGISTRY.values()]
        else:
            self._analyzers = [get_analyzer(name) for name in analyzers]

    def run(self, bundles: List[ArtifactBundle]) -> List[AnalysisResult]:
        results = []
        for bundle in bundles:
            for analyzer in self._analyzers:
                result = analyzer.run(bundle)
                results.append(result)
        return results

    def print_routing_report(self, results: List[AnalysisResult]) -> None:
        print(f"\nUniversal Analyzer Routing Report ({len(results)} analysis passes)\n" + "-" * 60)
        for r in results:
            symbol = {"stub": "~", "blocked": "X", "complete": "V", "error": "!!"}.get(r.status, "?")
            print(f"  [{symbol}] {r.analyzer_name:30s} key={r.key}  status={r.status}")
            for w in r.warnings:
                print(f"        WARNING: {w}")
            for e in r.errors:
                print(f"        ERROR: {e}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Universal Analyzer scaffolding — routing confirmation only."
    )
    parser.add_argument(
        "--artifacts", type=Path, required=True,
        help="Directory of validated PC2 artifact JSON files."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print routing report only; no analysis output written."
    )
    parser.add_argument(
        "--list-analyzers", action="store_true",
        help="List registered analyzers and exit."
    )
    args = parser.parse_args()

    if args.list_analyzers:
        print("Registered analyzers:")
        for name in get_registered_analyzers():
            analyzer = get_analyzer(name)
            print(f"  {name:35s} requires={analyzer.required_artifact_types}")
        return 0

    bundles = build_bundles_from_directory(args.artifacts)
    if not bundles:
        print("No valid PC2 artifact bundles found in directory.")
        return 1

    print(f"Built {len(bundles)} artifact bundle(s) from {args.artifacts}")
    runner = UniversalAnalyzerRunner()
    results = runner.run(bundles)
    runner.print_routing_report(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
