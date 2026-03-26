"""Artifact availability checks for AEE pipeline assets.

This module guards expensive simulation and runtime flows by verifying that the
core compiled artifacts exist before executing. It prevents noisy runtime
warnings by failing fast with actionable guidance when the artifacts are
missing.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

BASE_DIR = Path(__file__).resolve().parent
_DEFAULT_ENV_FLAG = "SKIP_ARTIFACT_CHECKS"


def _iter_non_hidden(path: Path) -> Iterable[Path]:
    try:
        yield from (p for p in path.iterdir() if not p.name.startswith("."))
    except FileNotFoundError:
        return


@dataclass(frozen=True)
class ArtifactRequirement:
    path: Path
    description: str
    must_exist: bool = True
    require_nonempty: bool = False


REQUIRED_ARTIFACTS: List[ArtifactRequirement] = [
    ArtifactRequirement(
        path=BASE_DIR / "compiled_market_nodes",
        description="Compiled market node datasets (per pair/session)",
        require_nonempty=True,
    ),
    ArtifactRequirement(
        path=BASE_DIR / "compiled_session_templates",
        description="Compiled session templates",
        require_nonempty=True,
    ),
    ArtifactRequirement(
        path=BASE_DIR / "calibration" / "active" / "ACTIVE_ARTIFACTS.json",
        description="Active artifacts manifest",
    ),
]


def check_required_artifacts(requirements: Iterable[ArtifactRequirement] | None = None) -> List[str]:
    """Return a list of human-readable problems for missing artifacts."""
    reqs = list(requirements or REQUIRED_ARTIFACTS)
    problems: List[str] = []
    for req in reqs:
        path = req.path
        if not path.exists():
            if req.must_exist:
                problems.append(f"Missing {req.description}: {path}")
            continue
        if req.require_nonempty and path.is_dir():
            try:
                next(_iter_non_hidden(path))
            except StopIteration:
                problems.append(f"Empty directory for {req.description}: {path}")
    return problems


def ensure_core_artifacts(requirements: Iterable[ArtifactRequirement] | None = None) -> None:
    """Raise RuntimeError if required artifacts are missing (unless skipped)."""
    if os.getenv(_DEFAULT_ENV_FLAG, "0").strip().lower() in {"1", "true", "yes"}:
        return
    problems = check_required_artifacts(requirements=requirements)
    if problems:
        joined = "\n - ".join(problems)
        raise RuntimeError(
            "Required compiled artifacts are missing.\n"
            "Set SKIP_ARTIFACT_CHECKS=1 to bypass (not recommended).\n"
            "Problems detected:\n - " + joined
        )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Verify compiled artifact availability")
    parser.add_argument("--skip-env", action="store_true", help="Ignore SKIP_ARTIFACT_CHECKS flag")
    args = parser.parse_args()

    if not args.skip_env:
        os.environ.pop(_DEFAULT_ENV_FLAG, None)

    problems = check_required_artifacts()
    if problems:
        print("Artifact check FAILED:")
        for prob in problems:
            print(f" - {prob}")
        return 1

    print("Artifact check passed: required assets present")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
