#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = ROOT / "compiled_market_nodes"
JSON_OUT = ROOT / "market_node_tracker_v2.json"
CSV_OUT = ROOT / "market_node_tracker_v2.csv"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def main() -> None:
    rows = []
    if DEFAULT_ROOT.exists():
        for node_dir in sorted(p for p in DEFAULT_ROOT.iterdir() if p.is_dir()):
            manifest_path = node_dir / "node_manifest.json"
            if not manifest_path.exists():
                continue
            manifest = load_json(manifest_path)
            node = manifest.get("node", {})
            rows.append(
                {
                    "node_key": node_dir.name,
                    "pair": node.get("pair", ""),
                    "weekday": node.get("weekday", ""),
                    "session": node.get("session", ""),
                    "dataset_hash": manifest.get("dataset_hash", ""),
                    "seed_entry_node": manifest.get("seed_entry_node") or "",
                    "seed_aee_node": manifest.get("seed_aee_node") or "",
                    "has_target_entry": int((node_dir / "target_entry_no_timeouts").exists()),
                    "has_aee_stage": int((node_dir / "aee_stage").exists()),
                    "has_aee_target_local": int((node_dir / "aee_target_local_fixedpop").exists()),
                    "has_aee_theoretical_ceiling": int((node_dir / "aee_target_theoretical_ceiling").exists()),
                }
            )

    summary = {
        "compiler": "market_node_tracker_v2",
        "node_count": len(rows),
        "nodes": rows,
    }
    JSON_OUT.write_text(json.dumps(summary, indent=2))
    with CSV_OUT.open("w", newline="") as f:
        fieldnames = [
            "node_key",
            "pair",
            "weekday",
            "session",
            "dataset_hash",
            "seed_entry_node",
            "seed_aee_node",
            "has_target_entry",
            "has_aee_stage",
            "has_aee_target_local",
            "has_aee_theoretical_ceiling",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
