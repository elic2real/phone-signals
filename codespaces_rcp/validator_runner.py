from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

from codespaces_rcp.artifact_validator import validate_report_path
from codespaces_rcp.dependency_validator import validate_dependencies
from codespaces_rcp.ownership_validator import validate_ownership
from codespaces_rcp.promotion_gate_framework import evaluate_promotion_gate
from codespaces_rcp.report_loaders import SUPPORTED_ARTIFACTS
from codespaces_rcp.universal_analyzer_scaffold import run_scaffold


def _collect_report_paths(report_dir: Path) -> List[Path]:
    return [report_dir / name for name in sorted(SUPPORTED_ARTIFACTS) if (report_dir / name).exists()]


def run(report_dir: Path, schema_dir: Path) -> Dict:
    report_paths = _collect_report_paths(report_dir)
    artifact_results = []

    for path in report_paths:
        schema_issues = [vars(i) for i in validate_report_path(path, schema_dir)]
        owner_issues = [vars(i) for i in validate_ownership(path)]
        dep_issues = [vars(i) for i in validate_dependencies(path, report_dir)]
        artifact_results.append(
            {
                "artifact": path.name,
                "schema_issues": schema_issues,
                "ownership_issues": owner_issues,
                "dependency_issues": dep_issues,
            }
        )

    analyzer = run_scaffold(report_dir)
    gate = evaluate_promotion_gate(report_dir)

    return {
        "report_dir": str(report_dir),
        "schema_dir": str(schema_dir),
        "artifacts_checked": len(report_paths),
        "results": artifact_results,
        "universal_analyzer_scaffold": {
            "status": analyzer.status,
            "present_artifacts": analyzer.present_artifacts,
            "missing_artifacts": analyzer.missing_artifacts,
            "notes": analyzer.notes,
        },
        "promotion_gate": {
            "status": gate.status,
            "missing_artifacts": gate.missing_artifacts,
            "message": gate.message,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Codespaces RCP enforcement validator runner")
    parser.add_argument("--report-dir", default="control", help="Directory that contains future PC2 outputs")
    parser.add_argument(
        "--schema-dir",
        default="codespaces_rcp/schemas",
        help="Directory containing enforcement JSON schemas",
    )
    parser.add_argument(
        "--out",
        default="control/codespaces_enforcement_validation.json",
        help="Output JSON file path",
    )
    args = parser.parse_args()

    report_dir = Path(args.report_dir)
    schema_dir = Path(args.schema_dir)
    output = Path(args.out)

    result = run(report_dir=report_dir, schema_dir=schema_dir)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
