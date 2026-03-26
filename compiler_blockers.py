from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BLOCKER_PROTOCOL: dict[str, dict[str, Any]] = {
    "zero_fixedpop_trade_rows": {
        "stage": "aee_target_local_fixedpop",
        "severity": "critical",
        "symptom": "Fixed-pop AEE trade rows file exists but contains zero rows.",
        "why_it_matters": "The node appears downstream-complete while the AEE trade-level materialization is empty.",
        "automatic_response": [
            "mark node as blocked instead of pass",
            "record blocker artifact",
            "route to fixed-pop AEE investigation or local rebuild",
        ],
        "verification": [
            "target_local_fixedpop_aee_trade_rows.json length > 0",
            "target_local_fixedpop_aee_report.json aggregate_metrics.trade_count > 0",
        ],
    },
    "missing_aee_trade_rows": {
        "stage": "aee_target_local_fixedpop",
        "severity": "critical",
        "symptom": "Expected AEE trade-level rows are missing for a production/downstream node.",
        "why_it_matters": "The bot cannot rely on full AEE-managed evidence for the node.",
        "automatic_response": [
            "mark node as blocked or fallback-only",
            "record blocker artifact",
        ],
        "verification": [
            "target_local_fixedpop_aee_trade_rows.json exists",
            "target_local_fixedpop_aee_trade_rows.json length > 0",
        ],
    },
}


def write_blocker_report(output_dir: Path, blocker_code: str, details: dict[str, Any]) -> None:
    blocker = dict(BLOCKER_PROTOCOL.get(blocker_code, {}))
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "blocker_code": blocker_code,
        "protocol": blocker,
        "details": details,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "compiler_blocker_report.json").write_text(json.dumps(payload, indent=2))

