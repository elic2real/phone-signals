#!/usr/bin/env python3
"""
Enforcement: Promotion Gate Framework
========================================
Defines the gate checks that a discovery key must pass before being promoted
to a locked setup_truth. At this phase this module enforces only the structural
pre-conditions for promotion — it does NOT run actual promotion logic on
nonexistent PC2 discovery outputs.

Gate hierarchy:
    Gate 0  — Artifact completeness  (required artifacts present)
    Gate 1  — Schema validity         (all artifacts pass schema)
    Gate 2  — Ownership validity      (ownership metadata consistent)
    Gate 3  — Dependency chain        (artifact chain order correct)
    Gate 4  — Viability confirmation  (business_viability_report.viable=true)
    Gate 5  — Family existence        (path_family_report with ≥1 family)
    Gate 6  — Structure label         (structure_truth with non-unknown label)
    Gate 7  — Ceiling floor           (ceiling_report above configured minimums)
    Gate 8  — No active gap block     (no non-recoverable segmentation_gap)
    Gate 9  — Population floor        (minimum trade count satisfied)

Promotion logic (the actual locking of setup_truth) is NOT executed here.
That happens in Trunk 3. This module only returns GATE_PASS or GATE_FAIL
per gate, plus a summary verdict.

Usage:
    python promotion_gate.py --artifacts path/to/dir/ --direction LONG --pair EUR_USD --session london --bucket 2.5
    python promotion_gate.py --artifacts path/to/dir/ --all-keys
"""
from __future__ import annotations

import json
import sys
import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Minimum thresholds (to be locked at Trunk 2 sync; use conservative defaults now)
DEFAULT_MIN_TRADE_COUNT = 30
DEFAULT_MIN_WIN_RATE = 0.50
DEFAULT_MIN_CAPTURE_PIPS = 2.0
DEFAULT_MIN_FAMILY_COUNT = 1

KEY_FIELDS = ("direction", "target_bucket", "pair", "session")


# ---------------------------------------------------------------------------
# Gate result
# ---------------------------------------------------------------------------

@dataclass
class GateResult:
    gate_number: int
    gate_name: str
    passed: bool
    reason: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        line = f"  Gate {self.gate_number} [{status}] {self.gate_name}"
        if self.reason:
            line += f"\n    → {self.reason}"
        return line


@dataclass
class PromotionGateReport:
    """Overall promotion gate result for a single discovery key."""
    direction: str
    target_bucket: float
    pair: str
    session: str
    gate_results: List[GateResult] = field(default_factory=list)

    @property
    def promotion_eligible(self) -> bool:
        return all(g.passed for g in self.gate_results)

    @property
    def first_failing_gate(self) -> Optional[GateResult]:
        for g in self.gate_results:
            if not g.passed:
                return g
        return None

    def key_label(self) -> str:
        return f"direction={self.direction} bucket={self.target_bucket} pair={self.pair} session={self.session}"

    def summary(self) -> str:
        verdict = "ELIGIBLE FOR PROMOTION" if self.promotion_eligible else "NOT ELIGIBLE"
        lines = [f"\nPromotion Gate Report — {self.key_label()}", f"Verdict: {verdict}", "-" * 60]
        for g in self.gate_results:
            lines.append(str(g))
        if not self.promotion_eligible and self.first_failing_gate:
            lines.append(
                f"\nBlocked at Gate {self.first_failing_gate.gate_number}: "
                f"{self.first_failing_gate.gate_name}"
            )
        lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Gate implementations
# ---------------------------------------------------------------------------

def _g0_artifact_completeness(artifacts: Dict[str, Any]) -> GateResult:
    """Gate 0: Required early-phase artifacts are present."""
    required = ["business_viability_report"]
    missing = [t for t in required if t not in artifacts]
    if missing:
        return GateResult(0, "artifact_completeness", False, f"Missing required artifacts: {missing}")
    return GateResult(0, "artifact_completeness", True)


def _g1_schema_validity(artifacts: Dict[str, Any], artifact_dir: Path) -> GateResult:
    """Gate 1: All present artifacts pass schema validation."""
    try:
        from enforcement.artifact_validator import validate_artifact, ARTIFACT_SCHEMA_MAP
    except ImportError:
        from artifact_validator import validate_artifact, ARTIFACT_SCHEMA_MAP

    all_errors = []
    for json_file in sorted(artifact_dir.glob("*.json")):
        result = validate_artifact(json_file)
        if not result.passed:
            all_errors.extend(result.schema_errors + result.constraint_errors)

    if all_errors:
        return GateResult(1, "schema_validity", False, f"{len(all_errors)} schema/constraint error(s) found.")
    return GateResult(1, "schema_validity", True)


def _g2_ownership_validity(artifact_dir: Path) -> GateResult:
    """Gate 2: Ownership metadata is consistent."""
    try:
        from enforcement.ownership_validator import validate_directory
    except ImportError:
        from ownership_validator import validate_directory

    result = validate_directory(artifact_dir)
    if not result.passed:
        errors = [v.message for v in result.violations if v.severity == "ERROR"]
        return GateResult(2, "ownership_validity", False, f"{len(errors)} ownership error(s) found.")
    return GateResult(2, "ownership_validity", True)


def _g3_dependency_chain(artifact_dir: Path) -> GateResult:
    """Gate 3: Artifact dependency chain is valid."""
    try:
        from enforcement.dependency_validator import validate_directory
    except ImportError:
        from dependency_validator import validate_directory

    result = validate_directory(artifact_dir)
    if not result.passed:
        errors = [v.message for v in result.violations if v.severity == "ERROR"]
        return GateResult(3, "dependency_chain", False, f"{len(errors)} dependency error(s) found.")
    return GateResult(3, "dependency_chain", True)


def _g4_viability_confirmation(artifacts: Dict[str, Any]) -> GateResult:
    """Gate 4: business_viability_report.viable must be True."""
    bvr = artifacts.get("business_viability_report")
    if bvr is None:
        return GateResult(4, "viability_confirmation", False, "business_viability_report not present.")
    viable = bvr.get("viable")
    if viable is not True:
        reasons = bvr.get("fail_reasons", [])
        return GateResult(4, "viability_confirmation", False, f"viable=False. Reasons: {reasons}")
    return GateResult(4, "viability_confirmation", True)


def _g5_family_existence(
    artifacts: Dict[str, Any],
    min_family_count: int = DEFAULT_MIN_FAMILY_COUNT,
) -> GateResult:
    """Gate 5: path_family_report must exist with at least one family."""
    pfr = artifacts.get("path_family_report")
    if pfr is None:
        return GateResult(
            5, "family_existence", False,
            "path_family_report not present — family phase has not run."
        )
    families = pfr.get("families", [])
    if len(families) < min_family_count:
        return GateResult(
            5, "family_existence", False,
            f"family_count={len(families)} < required {min_family_count}."
        )
    return GateResult(5, "family_existence", True)


def _g6_structure_label(artifacts: Dict[str, Any]) -> GateResult:
    """Gate 6: structure_truth must have a non-unknown label."""
    st = artifacts.get("structure_truth")
    if st is None:
        return GateResult(
            6, "structure_label", False,
            "structure_truth not present — structure detection has not run."
        )
    label = st.get("structure_label")
    if label == "unknown" or not label:
        return GateResult(
            6, "structure_label", False,
            f"structure_label is {label!r} — structure must be identified before promotion."
        )
    return GateResult(6, "structure_label", True)


def _g7_ceiling_floor(
    artifacts: Dict[str, Any],
    min_win_rate: float = DEFAULT_MIN_WIN_RATE,
    min_capture_pips: float = DEFAULT_MIN_CAPTURE_PIPS,
) -> GateResult:
    """Gate 7: ceiling_report metrics must be above configured minimums."""
    cr = artifacts.get("ceiling_report")
    if cr is None:
        return GateResult(
            7, "ceiling_floor", False,
            "ceiling_report not present — ceiling has not been computed."
        )
    metrics = cr.get("ceiling_metrics", {})
    wr_ceiling = metrics.get("win_rate_ceiling")
    cap_ceiling = metrics.get("capture_pips_ceiling")
    failures = []
    if wr_ceiling is not None and wr_ceiling < min_win_rate:
        failures.append(f"win_rate_ceiling={wr_ceiling:.3f} < floor={min_win_rate}")
    if cap_ceiling is not None and cap_ceiling < min_capture_pips:
        failures.append(f"capture_pips_ceiling={cap_ceiling:.2f} < floor={min_capture_pips}")
    if failures:
        return GateResult(7, "ceiling_floor", False, "; ".join(failures))
    return GateResult(7, "ceiling_floor", True)


def _g8_no_active_gap_block(artifacts: Dict[str, Any]) -> GateResult:
    """Gate 8: No non-recoverable segmentation_gap exists."""
    sgr = artifacts.get("segmentation_gap_report")
    if sgr is None:
        return GateResult(8, "no_active_gap_block", True)  # no gap = no block
    recoverable = sgr.get("recoverable", True)
    downstream_blocked = sgr.get("downstream_blocked", [])
    if not recoverable:
        return GateResult(
            8, "no_active_gap_block", False,
            f"Non-recoverable segmentation_gap: {sgr.get('gap_type')}. "
            f"Blocked: {downstream_blocked}"
        )
    if "setup_lock" in downstream_blocked or "promotion" in downstream_blocked:
        return GateResult(
            8, "no_active_gap_block", False,
            f"segmentation_gap blocks setup_lock/promotion: {downstream_blocked}"
        )
    return GateResult(8, "no_active_gap_block", True)


def _g9_population_floor(
    artifacts: Dict[str, Any],
    min_trade_count: int = DEFAULT_MIN_TRADE_COUNT,
) -> GateResult:
    """Gate 9: Minimum trade count satisfied."""
    bvr = artifacts.get("business_viability_report")
    if bvr is None:
        return GateResult(9, "population_floor", False, "business_viability_report not present.")
    count = bvr.get("trade_count", 0)
    if count < min_trade_count:
        return GateResult(
            9, "population_floor", False,
            f"trade_count={count} < required {min_trade_count}."
        )
    return GateResult(9, "population_floor", True)


# ---------------------------------------------------------------------------
# Gate runner
# ---------------------------------------------------------------------------

def run_promotion_gates(
    artifact_dir: Path,
    direction: str,
    target_bucket: float,
    pair: str,
    session: str,
    min_trade_count: int = DEFAULT_MIN_TRADE_COUNT,
    min_win_rate: float = DEFAULT_MIN_WIN_RATE,
    min_capture_pips: float = DEFAULT_MIN_CAPTURE_PIPS,
) -> PromotionGateReport:
    """Run all promotion gates for a single discovery key."""
    report = PromotionGateReport(
        direction=direction,
        target_bucket=target_bucket,
        pair=pair,
        session=session,
    )

    # Collect artifacts for this key
    artifacts: Dict[str, Any] = {}
    for json_file in sorted(artifact_dir.glob("*.json")):
        try:
            with json_file.open() as f:
                artifact = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        # Filter to matching key
        if (
            artifact.get("direction") == direction
            and artifact.get("target_bucket") == target_bucket
            and artifact.get("pair") == pair
            and artifact.get("session") == session
        ):
            from artifact_validator import detect_artifact_type
            artifact_type = detect_artifact_type(artifact)
            if artifact_type:
                artifacts[artifact_type] = artifact

    # Run gates in order; stop at first failure (cascade model)
    gates = [
        _g0_artifact_completeness(artifacts),
        _g1_schema_validity(artifacts, artifact_dir),
        _g2_ownership_validity(artifact_dir),
        _g3_dependency_chain(artifact_dir),
        _g4_viability_confirmation(artifacts),
        _g5_family_existence(artifacts),
        _g6_structure_label(artifacts),
        _g7_ceiling_floor(artifacts, min_win_rate, min_capture_pips),
        _g8_no_active_gap_block(artifacts),
        _g9_population_floor(artifacts, min_trade_count),
    ]

    for gate in gates:
        report.gate_results.append(gate)
        if not gate.passed:
            break  # Hard stop — remaining gates not run

    return report


def run_all_keys(
    artifact_dir: Path,
    **kwargs: Any,
) -> List[PromotionGateReport]:
    """Discover all keys in directory and run gates for each."""
    keys = set()
    for json_file in sorted(artifact_dir.glob("*.json")):
        try:
            with json_file.open() as f:
                artifact = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        key = (
            artifact.get("direction"),
            artifact.get("target_bucket"),
            artifact.get("pair"),
            artifact.get("session"),
        )
        if all(v is not None for v in key):
            keys.add(key)

    reports = []
    for direction, target_bucket, pair, session in sorted(keys):
        reports.append(run_promotion_gates(
            artifact_dir, direction, target_bucket, pair, session, **kwargs
        ))
    return reports


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run promotion gate checks on PC2 discovery artifacts."
    )
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--direction", choices=["LONG", "SHORT"])
    parser.add_argument("--pair", help="e.g. EUR_USD")
    parser.add_argument("--session", help="e.g. london")
    parser.add_argument("--bucket", type=float, dest="target_bucket")
    parser.add_argument("--all-keys", action="store_true")
    parser.add_argument("--min-trades", type=int, default=DEFAULT_MIN_TRADE_COUNT)
    parser.add_argument("--min-win-rate", type=float, default=DEFAULT_MIN_WIN_RATE)
    parser.add_argument("--min-capture", type=float, default=DEFAULT_MIN_CAPTURE_PIPS)
    args = parser.parse_args()

    gate_kwargs = {
        "min_trade_count": args.min_trades,
        "min_win_rate": args.min_win_rate,
        "min_capture_pips": args.min_capture,
    }

    if args.all_keys:
        reports = run_all_keys(args.artifacts, **gate_kwargs)
    elif all([args.direction, args.target_bucket, args.pair, args.session]):
        reports = [run_promotion_gates(
            args.artifacts,
            args.direction,
            args.target_bucket,
            args.pair,
            args.session,
            **gate_kwargs,
        )]
    else:
        parser.error("Provide --all-keys or all of --direction --pair --session --bucket.")
        return 1

    exit_code = 0
    for report in reports:
        print(report.summary())
        if not report.promotion_eligible:
            exit_code = 1

    eligible = sum(1 for r in reports if r.promotion_eligible)
    print(f"Summary: {eligible}/{len(reports)} keys eligible for promotion.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
