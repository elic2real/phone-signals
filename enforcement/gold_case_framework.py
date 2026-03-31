#!/usr/bin/env python3
"""
Enforcement: Gold-Case Framework
==================================
Provides the gold-case manifest and gold-case checker.

A gold case is a known, pre-approved artifact bundle that represents a
reference truth for a specific discovery key. Gold cases are used to:
- Sanity-check the validator pipeline itself
- Prove that the schema + validator chain accept valid artifacts
- Catch schema regressions if schemas are updated

At this phase, gold cases are MANIFEST-only stubs. They define what a valid
artifact SHOULD look like for a given key. Real gold-case artifacts will be
populated after Trunk 2 sync.

Usage:
    python gold_case_framework.py --list
    python gold_case_framework.py --run-manifest manifest/gold_case_manifest.json --artifacts path/to/dir/
    python gold_case_framework.py --emit-stub --artifact-type business_viability_report --output /tmp/
"""
from __future__ import annotations

import json
import sys
import argparse
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"
MANIFEST_DIR = Path(__file__).resolve().parent / "gold_cases"


# ---------------------------------------------------------------------------
# Gold case manifest entry
# ---------------------------------------------------------------------------

@dataclass
class GoldCaseEntry:
    """A single gold-case specification."""
    case_id: str
    description: str
    artifact_type: str
    key: Dict[str, Any]  # direction, target_bucket, pair, session
    expected_fields: Dict[str, Any]  # field: expected_value assertions
    must_pass_schema: bool = True
    must_pass_ownership: bool = True
    must_pass_dependency: bool = True
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GoldCaseManifest:
    """Collection of gold case entries for a test suite."""
    manifest_version: str
    generated_at: str
    description: str
    cases: List[GoldCaseEntry] = field(default_factory=list)

    def add_case(self, case: GoldCaseEntry) -> None:
        self.cases.append(case)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "generated_at": self.generated_at,
            "description": self.description,
            "cases": [c.to_dict() for c in self.cases],
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(self.to_dict(), f, indent=2)
        print(f"Gold case manifest saved: {path}")

    @classmethod
    def load(cls, path: Path) -> "GoldCaseManifest":
        with path.open() as f:
            data = json.load(f)
        cases = [GoldCaseEntry(**c) for c in data.get("cases", [])]
        return cls(
            manifest_version=data["manifest_version"],
            generated_at=data["generated_at"],
            description=data["description"],
            cases=cases,
        )


# ---------------------------------------------------------------------------
# Stub artifact templates (minimum valid structures for each artifact type)
# ---------------------------------------------------------------------------

_NOW = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

STUB_TEMPLATES: Dict[str, Callable[..., Dict[str, Any]]] = {}


def _reg(name: str):
    def decorator(fn):
        STUB_TEMPLATES[name] = fn
        return fn
    return decorator


@_reg("business_viability_report")
def _stub_bvr(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
    viable: bool = True,
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "trade_count": 50,
        "win_rate": 0.62 if viable else 0.21,
        "avg_capture_pips": 2.8 if viable else 0.9,
        "viable": viable,
        "fail_reasons": [] if viable else ["win_rate_below_floor"],
    }


@_reg("path_family_report")
def _stub_pfr(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "family_count": 2,
        "families": [
            {
                "path_family": "impulse_clean",
                "member_count": 31,
                "centroid_features": {"displacement_pips": 1.8, "bar2_ratio": 0.72},
                "win_rate": 0.71,
                "avg_capture_pips": 3.1,
            },
            {
                "path_family": "grind_recovery",
                "member_count": 19,
                "centroid_features": {"displacement_pips": 0.9, "bar2_ratio": 0.45},
                "win_rate": 0.53,
                "avg_capture_pips": 2.6,
            },
        ],
    }


@_reg("structure_truth")
def _stub_st(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
    path_family: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "path_family": path_family,
        "structure_label": "impulse_breakout",
        "label_confidence": 0.88,
        "sample_count": 50,
        "label_source": "detector",
    }


@_reg("ceiling_report")
def _stub_cr(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "path_family": None,
        "ceiling_metrics": {
            "win_rate_ceiling": 0.78,
            "capture_pips_ceiling": 4.2,
            "profit_ceiling": 210.0,
            "pips_per_hour_ceiling": None,
            "equity_per_hour_ceiling": None,
        },
        "population_size": 50,
        "ceiling_method": "mfe_based",
    }


@_reg("segmentation_gap_report")
def _stub_sgr(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "gap_type": "insufficient_population",
        "gap_details": {
            "message": "Only 8 trades found for this key; minimum 30 required.",
            "observed_count": 8,
            "required_count": 30,
        },
        "recoverable": True,
        "downstream_blocked": ["path_family_clustering", "setup_lock", "trigger_lock"],
    }


@_reg("setup_truth")
def _stub_setup(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "setup_id": f"LONG__EUR_USD__london__2.5__impulse_clean__impulse_breakout",
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "path_family": "impulse_clean",
        "structure_label": "impulse_breakout",
        "entry_filter": {
            "conditions": [
                {"field": "displacement_pips", "op": "gte", "value": 1.5},
                {"field": "bar2_ratio", "op": "gte", "value": 0.6},
            ],
            "logic": "AND",
        },
        "population_size": 31,
        "win_rate": 0.71,
        "avg_capture_pips": 3.1,
        "locked": True,
        "locked_at": _NOW(),
        "promoted_from": None,
    }


@_reg("trigger_truth")
def _stub_trigger(
    direction: str = "LONG",
    target_bucket: float = 2.5,
    pair: str = "EUR_USD",
    session: str = "london",
) -> Dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "generated_at": _NOW(),
        "trigger_id": f"TRG__LONG__EUR_USD__london__2.5__impulse_clean",
        "setup_id": f"LONG__EUR_USD__london__2.5__impulse_clean__impulse_breakout",
        "direction": direction,
        "target_bucket": target_bucket,
        "pair": pair,
        "session": session,
        "path_family": "impulse_clean",
        "trigger_conditions": {
            "entry_signals": [
                {"field": "bar1_close_above_midpoint", "op": "eq", "value": True},
            ],
            "kill_conditions": [
                {"field": "spread_pips", "op": "gt", "value": 2.0},
            ],
            "confirmation_window_bars": 3,
            "logic": "AND",
        },
        "locked": True,
        "locked_at": _NOW(),
    }


# Use typing.Callable import
from typing import Callable


# ---------------------------------------------------------------------------
# Built-in baseline manifest
# ---------------------------------------------------------------------------

def build_baseline_manifest() -> GoldCaseManifest:
    """Build the baseline gold-case manifest covering one valid example per artifact type."""
    manifest = GoldCaseManifest(
        manifest_version="1.0.0",
        generated_at=_NOW(),
        description=(
            "Baseline gold-case manifest. One valid stub per artifact type for schema "
            "regression and validator pipeline verification."
        ),
    )
    key = {
        "direction": "LONG",
        "target_bucket": 2.5,
        "pair": "EUR_USD",
        "session": "london",
    }
    for artifact_type in [
        "business_viability_report",
        "path_family_report",
        "structure_truth",
        "ceiling_report",
        "segmentation_gap_report",
        "setup_truth",
        "trigger_truth",
    ]:
        manifest.add_case(GoldCaseEntry(
            case_id=f"baseline_{artifact_type}",
            description=f"Minimum valid stub for {artifact_type}.",
            artifact_type=artifact_type,
            key=key,
            expected_fields={"schema_version": "1.0.0"},
            must_pass_schema=True,
            must_pass_ownership=True,
            must_pass_dependency=False,  # dependency checks need full chain
            notes="Baseline stub — real gold case to be defined after Trunk 2 sync.",
        ))
    return manifest


# ---------------------------------------------------------------------------
# Gold-case runner
# ---------------------------------------------------------------------------

@dataclass
class GoldCaseResult:
    case_id: str
    artifact_type: str
    passed: bool
    failures: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"  [{status}] {self.case_id} ({self.artifact_type})"]
        for f in self.failures:
            lines.append(f"    - {f}")
        return "\n".join(lines)


def run_gold_cases(
    manifest: GoldCaseManifest,
    artifact_dir: Optional[Path] = None,
) -> List[GoldCaseResult]:
    """
    Run all gold cases.
    If artifact_dir is provided, look for actual artifact files. Otherwise, run against stub.
    """
    try:
        from enforcement.artifact_validator import validate_artifact, detect_artifact_type
    except ImportError:
        from artifact_validator import validate_artifact, detect_artifact_type
    import tempfile

    results = []
    for case in manifest.cases:
        if artifact_dir is not None:
            # Look for a matching artifact file in the directory
            # Naming convention: {artifact_type}_{direction}_{pair}_{session}_{bucket}.json
            pattern = f"*{case.artifact_type}*.json"
            candidates = list(artifact_dir.glob(pattern))
            if not candidates:
                results.append(GoldCaseResult(
                    case_id=case.case_id,
                    artifact_type=case.artifact_type,
                    passed=False,
                    failures=[f"No artifact file matching {pattern} found in {artifact_dir}"],
                ))
                continue
            artifact_path = candidates[0]
        else:
            # Emit stub and validate against it
            stub_fn = STUB_TEMPLATES.get(case.artifact_type)
            if stub_fn is None:
                results.append(GoldCaseResult(
                    case_id=case.case_id,
                    artifact_type=case.artifact_type,
                    passed=False,
                    failures=[f"No stub template for artifact type: {case.artifact_type}"],
                ))
                continue
            stub = stub_fn(**{k: v for k, v in case.key.items() if k in ["direction", "target_bucket", "pair", "session"]})
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
                json.dump(stub, tmp)
                tmp_path = Path(tmp.name)
            artifact_path = tmp_path

        validation = validate_artifact(artifact_path, artifact_type=case.artifact_type)
        failures = []
        if case.must_pass_schema and validation.schema_errors:
            failures.extend([f"Schema: {e}" for e in validation.schema_errors])
        if validation.constraint_errors:
            failures.extend([f"Constraint: {e}" for e in validation.constraint_errors])

        # Check expected_fields
        try:
            with artifact_path.open() as f:
                artifact = json.load(f)
            for field_name, expected_val in case.expected_fields.items():
                actual_val = artifact.get(field_name)
                if actual_val != expected_val:
                    failures.append(
                        f"Expected {field_name}={expected_val!r}, got {actual_val!r}"
                    )
        except (json.JSONDecodeError, OSError) as e:
            failures.append(f"Could not read artifact for field check: {e}")

        results.append(GoldCaseResult(
            case_id=case.case_id,
            artifact_type=case.artifact_type,
            passed=len(failures) == 0,
            failures=failures,
        ))

    return results


def print_gold_case_results(results: List[GoldCaseResult]) -> int:
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    print(f"\nGold Case Results: {passed} passed, {failed} failed\n" + "-" * 50)
    for r in results:
        print(str(r))
    print()
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Gold-case framework for PC2 enforcement schemas.")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List built-in gold cases.")

    run_p = sub.add_parser("run", help="Run gold cases.")
    run_p.add_argument("--manifest", type=Path, help="Path to manifest JSON (optional; uses baseline if omitted).")
    run_p.add_argument("--artifacts", type=Path, help="Directory with real artifacts (optional; uses stubs if omitted).")

    emit_p = sub.add_parser("emit-manifest", help="Emit baseline manifest to disk.")
    emit_p.add_argument("--output", type=Path, required=True, help="Output path for manifest JSON.")

    stub_p = sub.add_parser("emit-stub", help="Emit a stub artifact for a given type.")
    stub_p.add_argument("--artifact-type", required=True, choices=list(STUB_TEMPLATES.keys()))
    stub_p.add_argument("--output", type=Path, required=True, help="Output directory.")

    args = parser.parse_args()

    if args.command == "list":
        manifest = build_baseline_manifest()
        for case in manifest.cases:
            print(f"  {case.case_id:45s} {case.artifact_type}")
        return 0

    if args.command == "emit-manifest":
        manifest = build_baseline_manifest()
        manifest.save(args.output)
        return 0

    if args.command == "emit-stub":
        stub_fn = STUB_TEMPLATES[args.artifact_type]
        stub = stub_fn()
        args.output.mkdir(parents=True, exist_ok=True)
        out_path = args.output / f"stub_{args.artifact_type}.json"
        with out_path.open("w") as f:
            json.dump(stub, f, indent=2)
        print(f"Stub emitted: {out_path}")
        return 0

    if args.command == "run":
        if args.manifest:
            manifest = GoldCaseManifest.load(args.manifest)
        else:
            manifest = build_baseline_manifest()
        results = run_gold_cases(manifest, artifact_dir=getattr(args, "artifacts", None))
        return print_gold_case_results(results)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
