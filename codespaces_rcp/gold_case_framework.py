from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class GoldCaseManifest:
    manifest_id: str
    version: str
    cases: List[Dict]


def empty_manifest() -> GoldCaseManifest:
    return GoldCaseManifest(manifest_id="gold-case-manifest", version="v1", cases=[])


def save_manifest(path: Path, manifest: GoldCaseManifest) -> None:
    payload = {
        "manifest_id": manifest.manifest_id,
        "version": manifest.version,
        "cases": manifest.cases,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_manifest(path: Path) -> GoldCaseManifest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return GoldCaseManifest(
        manifest_id=payload.get("manifest_id", "gold-case-manifest"),
        version=payload.get("version", "v1"),
        cases=payload.get("cases", []),
    )
