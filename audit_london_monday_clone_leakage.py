#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "compiled_market_nodes"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    groups: dict[str, list[str]] = defaultdict(list)
    rows = []
    for node in sorted(OUT.glob("*__monday__london")):
        truth = node / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"
        if not truth.exists():
            continue
        truth_hash = sha256_file(truth)
        groups[truth_hash].append(node.name)
        rows.append({"node": node.name, "truth_hash": truth_hash, "truth_csv": str(truth)})

    duplicates = [
        {"truth_hash": truth_hash, "nodes": nodes}
        for truth_hash, nodes in sorted(groups.items())
        if len(nodes) > 1
    ]
    report = {
        "session": "london",
        "weekday": "monday",
        "node_count": len(rows),
        "duplicate_groups": duplicates,
        "rows": rows,
    }
    out = ROOT / "audit_london_monday_clone_leakage.json"
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
