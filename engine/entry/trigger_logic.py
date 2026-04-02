from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict


WORKSPACE = Path(__file__).resolve().parents[2]
PHASE4_REPORT = WORKSPACE / "control" / "v2_engine" / "phase4" / "v2_phase4_trigger_report.json"


def load_phase4_trigger_report(path: Path | None = None) -> Dict[str, Any]:
    target = path or PHASE4_REPORT
    return json.loads(target.read_text(encoding="utf-8"))


def summarize_trigger_abort_pressure(path: Path | None = None) -> Dict[str, Any]:
    report = load_phase4_trigger_report(path)
    rows = list(report.get("rows", []))
    abort_counter = Counter(
        str(row.get("reason", "UNKNOWN") or "UNKNOWN")
        for row in rows
        if str(row.get("status", "") or "").upper() == "ABORTED"
    )
    return {
        "candidate_count": int(report.get("candidate_count", 0) or 0),
        "ready_count": int(report.get("ready_count", 0) or 0),
        "aborted_count": int(report.get("aborted_count", 0) or 0),
        "invalid_count": int(report.get("invalid_count", 0) or 0),
        "abort_ratio": float(report.get("abort_ratio", 0.0) or 0.0),
        "abort_reason_counts": dict(abort_counter),
    }

