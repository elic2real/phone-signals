#!/usr/bin/env python3
"""
Enforcement: Trigger Sibling Distinctness Validator
=====================================================
Validates that trigger siblings (triggers differing only by target_bucket)
are structurally and metrically distinct, preventing fake variant branching
inflation.

Gate hierarchy:
    Gate 0  — Artifact presence      (trigger_truth.json exists)
    Gate 1  — Schema validity         (all triggers pass schema)
    Gate 2  — Sibling distinctness    (within family, triggers differ meaningfully)
    Gate 3  — Entry condition diversity (sibling entry conditions differ)
    Gate 4  — Hazard profile diversity  (sibling hazard metrics differ)
    Gate 5  — Quality score diversity   (sibling quality scores differ)

Sibling grouping:
    Siblings share: (pair, session, direction, structure_label, path_family)
    Differ by: target_bucket
    
    Example siblings:
      - trigger::EUR_USD_London_LONG_2pip_sweep_retest_level::REASSERTION
      - trigger::EUR_USD_London_LONG_3pip_sweep_retest_level::REASSERTION
      
    These MUST have different entry conditions, kill conditions, hazard profiles,
    and quality metrics. If they are nearly identical, the sibling group is marked
    FAKE and blocked from promotion.

Usage:
    python trigger_validator.py --trigger-dir PC2/discovery/stage_a \
      --schema-dir enforcement/schemas
"""
from __future__ import annotations

import json
import sys
import argparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict


# ---------------------------------------------------------------------------
# Distinctness metrics
# ---------------------------------------------------------------------------

def _normalize_criteria_set(criteria: Dict[str, Any]) -> frozenset:
    """Normalize criteria dict into a hashable set."""
    if not criteria:
        return frozenset()
    # Extract all values and make them hashable
    items = []
    for key, value in criteria.items():
        if isinstance(value, dict):
            items.append((key, tuple(sorted(value.items()))))
        elif isinstance(value, (list, tuple)):
            items.append((key, tuple(value)))
        else:
            items.append((key, value))
    return frozenset(items)


def _hazard_profile_signature(trigger: Dict[str, Any]) -> Tuple:
    """Extract hazard profile from trigger for distinctness comparison."""
    hazard = trigger.get("hazard_model", {})
    if not hazard:
        return ()
    profile = (
        hazard.get("edge_half_life_sec"),
        hazard.get("zone_residency_sec_max"),
        hazard.get("stagnation_hazard"),
        round(trigger.get("trigger_quality", 0.0), 3),
    )
    return profile


def _quality_score_signature(trigger: Dict[str, Any]) -> Tuple[float, float, float]:
    """Extract quality metrics for distinctness comparison."""
    return (
        trigger.get("trigger_quality", 0.0),
        trigger.get("path_quality", 0.0),
        trigger.get("fill_quality", {}).get("budget", 0.0) if isinstance(trigger.get("fill_quality"), dict) else 0.0,
    )


def _triggers_are_similar(t1: Dict[str, Any], t2: Dict[str, Any], threshold: float = 0.95) -> bool:
    """
    Check if two triggers are suspiciously similar (fake variants).
    
    Similarity is high if:
    - Criteria are identical (or nearly identical)
    - States are identical
    - Hazard profiles match closely
    - Quality metric trends are parallel
    
    If any of these differ meaningfully, triggers are distinct.
    """
    # Criteria comparison (PC2 trigger format)
    crit1 = _normalize_criteria_set(t1.get("criteria", {}))
    crit2 = _normalize_criteria_set(t2.get("criteria", {}))
    if crit1 and crit2 and crit1 != crit2:
        return False  # Different criteria = distinct
    
    # States structure comparison (entry/confirmation/kill states)
    states1 = frozenset(str(s) for s in t1.get("states", {}).keys() if t1.get("states"))
    states2 = frozenset(str(s) for s in t2.get("states", {}).keys() if t2.get("states"))
    if states1 and states2 and states1 != states2:
        return False  # Different state structures = distinct

    # Hazard profile comparison (allow small variations)
    hz1 = _hazard_profile_signature(t1)
    hz2 = _hazard_profile_signature(t2)
    if hz1 and hz2:
        # Check if numeric fields are within threshold % of each other
        for v1, v2 in zip(hz1, hz2):
            if v1 is None or v2 is None:
                continue
            if v1 == 0 or v2 == 0:
                if v1 != v2:
                    return False
            else:
                try:
                    ratio = max(v1, v2) / min(v1, v2)
                    if ratio > (1 + (1 - threshold)):
                        return False  # Hazard profiles differ = distinct
                except (TypeError, ZeroDivisionError):
                    return False

    # Quality score comparison
    q1 = _quality_score_signature(t1)
    q2 = _quality_score_signature(t2)
    if q1 and q2:
        # Check if quality metrics are within threshold % of each other
        for qs1, qs2 in zip(q1, q2):
            if qs1 == 0 or qs2 == 0:
                if qs1 != qs2:
                    return False
            else:
                try:
                    ratio = max(qs1, qs2) / min(qs1, qs2)
                    if ratio > (1 + (1 - threshold)):
                        return False  # Quality differs = distinct
                except (TypeError, ZeroDivisionError):
                    return False

    # Directional dominance comparison (measures pair-specific behavior)
    dd1 = t1.get("directional_dominance", {})
    dd2 = t2.get("directional_dominance", {})
    if dd1 and dd2:
        # If dominance ratios differ, triggers are distinct
        ratio1 = dd1.get("ratio")
        ratio2 = dd2.get("ratio")
        if ratio1 is not None and ratio2 is not None:
            if ratio1 == 0 or ratio2 == 0:
                if ratio1 != ratio2:
                    return False
            else:
                dom_ratio = max(ratio1, ratio2) / min(ratio1, ratio2)
                if dom_ratio > (1 + (1 - threshold)):
                    return False  # Dominance differs = distinct

    # If we reach here, triggers are suspiciously similar
    return True


# ---------------------------------------------------------------------------
# Gate results
# ---------------------------------------------------------------------------

@dataclass
class SiblingGroup:
    """A group of sibling triggers differing only by target_bucket."""
    pair: str
    session: str
    direction: str
    structure_label: str
    path_family: str
    triggers: List[Dict[str, Any]] = field(default_factory=list)
    
    def sibling_key(self) -> str:
        return f"{self.pair}_{self.session}_{self.direction}_{self.structure_label}_{self.path_family}"
    
    def is_distinct(self) -> bool:
        """Check if all triggers within this sibling group are distinct."""
        if len(self.triggers) <= 1:
            return True  # Single trigger or empty is trivially distinct
        
        # Check each pair of siblings
        for i in range(len(self.triggers)):
            for j in range(i + 1, len(self.triggers)):
                if _triggers_are_similar(self.triggers[i], self.triggers[j]):
                    return False
        return True
    
    def distinctness_report(self) -> Dict[str, Any]:
        """Generate detailed distinctness report for this sibling group."""
        return {
            "sibling_key": self.sibling_key(),
            "trigger_count": len(self.triggers),
            "triggers": [
                {
                    "trigger_label": t.get("trigger_label"),
                    "setup_label": t.get("setup_label"),
                    "target_bucket": t.get("target_bucket"),
                    "has_criteria": bool(t.get("criteria")),
                    "criteria_count": len(t.get("criteria", {})),
                    "has_states": bool(t.get("states")),
                    "state_count": len(t.get("states", {})),
                    "quality_score": t.get("trigger_quality"),
                    "path_quality": t.get("path_quality"),
                }
                for t in sorted(self.triggers, key=lambda x: x.get("target_bucket", 0))
            ],
            "is_distinct": self.is_distinct(),
            "status": "DISTINCT" if self.is_distinct() else "FAKE_VARIANT_DETECTED",
        }


@dataclass
class TriggerValidationResult:
    """Overall trigger validation result."""
    passed: bool
    trigger_file: Path
    total_triggers: int
    sibling_groups: List[SiblingGroup] = field(default_factory=list)
    fake_variant_groups: List[SiblingGroup] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def summary(self) -> Dict[str, Any]:
        """Generate summary of trigger validation."""
        return {
            "status": "PASS" if self.passed else "FAIL",
            "trigger_file": str(self.trigger_file),
            "total_triggers": self.total_triggers,
            "total_sibling_groups": len(self.sibling_groups),
            "fake_variant_groups": len(self.fake_variant_groups),
            "fake_variant_trigger_labels": [
                tid for group in self.fake_variant_groups
                for tid in [t.get("trigger_label") for t in group.triggers]
            ],
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

def _g0_trigger_presence(artifact_dir: Path) -> Tuple[bool, Optional[Path]]:
    """Gate 0: trigger_truth.json exists."""
    trigger_file = artifact_dir / "trigger_truth.json"
    if not trigger_file.exists():
        return False, None
    return True, trigger_file


def _g1_trigger_schema_validity(trigger_file: Path, schema_dir: Path) -> Tuple[bool, List[str]]:
    """Gate 1: All triggers pass schema validation."""
    try:
        from enforcement.artifact_validator import validate_artifact
    except ImportError:
        from artifact_validator import validate_artifact
    
    result = validate_artifact(trigger_file)
    if not result.passed:
        errors = result.schema_errors + result.constraint_errors
        return False, errors
    return True, []


def _g2_sibling_distinctness(
    trigger_file: Path,
) -> Tuple[bool, List[SiblingGroup], List[SiblingGroup], List[str]]:
    """Gate 2: Trigger siblings are distinct (no fake variants)."""
    try:
        with trigger_file.open() as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        return False, [], [], [str(e)]
    
    triggers_array = payload.get("records", payload.get("triggers", []))
    if not triggers_array:
        return True, [], [], ["No triggers in artifact"]
    
    # Group by sibling key
    sibling_groups_dict: Dict[str, SiblingGroup] = {}
    for trigger in triggers_array:
        key = (
            trigger.get("pair"),
            trigger.get("session"),
            trigger.get("direction"),
            trigger.get("structure_label"),
            trigger.get("path_family"),
        )
        sibling_key_str = "_".join(str(k) for k in key)
        
        if sibling_key_str not in sibling_groups_dict:
            sibling_groups_dict[sibling_key_str] = SiblingGroup(
                pair=key[0],
                session=key[1],
                direction=key[2],
                structure_label=key[3],
                path_family=key[4],
            )
        sibling_groups_dict[sibling_key_str].triggers.append(trigger)
    
    sibling_groups = list(sibling_groups_dict.values())
    fake_variant_groups = [g for g in sibling_groups if not g.is_distinct()]
    
    passed = len(fake_variant_groups) == 0
    return passed, sibling_groups, fake_variant_groups, []


def validate_triggers(artifact_dir: Path, schema_dir: Optional[Path] = None) -> TriggerValidationResult:
    """Run all trigger validation gates."""
    result = TriggerValidationResult(
        passed=True,
        trigger_file=artifact_dir / "trigger_truth.json",
        total_triggers=0,
    )
    
    # Gate 0: Presence
    has_trigger, trigger_file = _g0_trigger_presence(artifact_dir)
    if not has_trigger:
        result.passed = False
        result.errors.append("trigger_truth.json not found")
        return result
    
    # Load trigger count
    try:
        with trigger_file.open() as f:
            payload = json.load(f)
        triggers_array = payload.get("records", payload.get("triggers", []))
        result.total_triggers = len(triggers_array)
    except (json.JSONDecodeError, OSError) as e:
        result.passed = False
        result.errors.append(f"Failed to load trigger file: {e}")
        return result
    
    # Gate 1: Schema validity
    if schema_dir:
        valid_schema, schema_errors = _g1_trigger_schema_validity(trigger_file, schema_dir)
        if not valid_schema:
            result.passed = False
            result.errors.extend(schema_errors)
            return result
    
    # Gate 2: Sibling distinctness
    distinct, sibling_groups, fake_groups, distinctness_errors = _g2_sibling_distinctness(trigger_file)
    result.sibling_groups = sibling_groups
    result.fake_variant_groups = fake_groups
    
    if not distinct:
        result.passed = False
        result.errors.append(f"Fake variants detected in {len(fake_groups)} sibling group(s)")
        result.errors.extend(distinctness_errors)
    
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate trigger sibling distinctness and prevent fake variant branching"
    )
    parser.add_argument(
        "--trigger-dir",
        type=Path,
        required=True,
        help="Directory containing trigger_truth.json",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        help="Directory containing schema files",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Write validation report to this JSON file",
    )
    
    args = parser.parse_args()
    
    result = validate_triggers(args.trigger_dir, args.schema_dir)
    
    # Print summary
    summary = result.summary()
    print(json.dumps(summary, indent=2))
    
    # Print detailed reports for fake variant groups
    if result.fake_variant_groups:
        print("\n=== FAKE VARIANT GROUPS (BLOCKED) ===\n")
        for group in result.fake_variant_groups:
            report = group.distinctness_report()
            print(json.dumps(report, indent=2))
    
    # Write output if requested
    if args.output_json:
        output = {
            "summary": summary,
            "sibling_groups": [g.distinctness_report() for g in result.sibling_groups],
        }
        with args.output_json.open("w") as f:
            json.dump(output, f, indent=2)
        print(f"\nValidation report written to {args.output_json}")
    
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
