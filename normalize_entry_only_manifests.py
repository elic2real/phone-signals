#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path("compiled_market_nodes")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def main() -> None:
    updated: list[str] = []
    skipped: list[str] = []

    for node_dir in sorted(path for path in ROOT.iterdir() if path.is_dir()):
        manifest_path = node_dir / "node_manifest.json"
        calibration_path = node_dir / "session_calibration" / "session_calibration_report.json"
        potential_path = node_dir / "session_potential" / "session_potential_report.json"
        performance_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
        if not all(path.exists() for path in (manifest_path, calibration_path, potential_path, performance_path)):
            continue

        manifest = load_json(manifest_path)
        if manifest.get("pipeline_mode") != "entry-only":
            continue

        calibration = load_json(calibration_path)
        potential = load_json(potential_path)
        performance = load_json(performance_path)

        if (
            calibration.get("status") == "PASS"
            and potential.get("status") == "PASS"
            and performance.get("status") == "PASS"
        ):
            if manifest.get("node_class") in {"accept", "light_delta"} and manifest.get("failure_route") == "none":
                skipped.append(node_dir.name)
                continue
            manifest["timestamp"] = datetime.now(timezone.utc).isoformat()
            manifest["node_class"] = "accept"
            manifest["failure_route"] = "none"
            manifest["reason"] = "performance_pass"
            manifest_path.write_text(json.dumps(manifest, indent=2))
            updated.append(node_dir.name)
        else:
            skipped.append(node_dir.name)

    print(
        json.dumps(
            {
                "updated_count": len(updated),
                "updated_nodes": updated,
                "skipped_count": len(skipped),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
