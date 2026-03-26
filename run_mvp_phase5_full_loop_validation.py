#!/usr/bin/env python3
"""Build MVP Phase 5 full-loop validation under locked scope.

This validator is measurement-only and consumes existing proof/run artifacts.
It verifies end-to-end loop viability with lock constraints intact.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


PHASE2_PATH = Path("control/mvp_phase2_entry_supply_proof.json")
PHASE3_PATH = Path("control/mvp_phase3_priority_proof.json")
PHASE4_PATH = Path("control/mvp_phase4_allocation_capacity_proof.json")
PAIR_EUR_PATH = Path("control/mvp_phase5_eur_usd.json")
PAIR_GBP_PATH = Path("control/mvp_phase5_gbp_usd.json")
PAIR_EUR_CTX_PATH = Path("control/mvp_phase5_eur_usd_context.json")
PAIR_GBP_CTX_PATH = Path("control/mvp_phase5_gbp_usd_context.json")
OUTPUT_PATH = Path("control/mvp_phase5_full_loop_validation.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _family_counts_by_verdict(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {"KEEP": 0, "TUNE": 0, "KILL": 0}
    for r in rows:
        v = str(r.get("verdict", "")).upper()
        if v in out:
            out[v] += 1
    return out


def main() -> None:
    p2 = _load_json(PHASE2_PATH)
    p3 = _load_json(PHASE3_PATH)
    p4 = _load_json(PHASE4_PATH)
    eur = _load_json(PAIR_EUR_PATH)
    gbp = _load_json(PAIR_GBP_PATH)
    eur_ctx = _load_json(PAIR_EUR_CTX_PATH)
    gbp_ctx = _load_json(PAIR_GBP_CTX_PATH)

    eur_net_pph = float(eur.get("combined_keep_tune_net_pph", 0.0))
    gbp_net_pph = float(gbp.get("combined_keep_tune_net_pph", 0.0))
    eur_trades = int(eur.get("total_accepted_trades", 0))
    gbp_trades = int(gbp.get("total_accepted_trades", 0))
    eur_hours = float(eur.get("total_hours", 0.0))
    gbp_hours = float(gbp.get("total_hours", 0.0))

    weighted_net_pph = (
        (eur_net_pph * eur_hours) + (gbp_net_pph * gbp_hours)
    ) / (eur_hours + gbp_hours) if (eur_hours + gbp_hours) > 0 else 0.0

    eur_skips = eur.get("skipped_by_entry_filter", {})
    gbp_skips = gbp.get("skipped_by_entry_filter", {})

    eur_families = eur.get("ranked_families", [])
    gbp_families = gbp.get("ranked_families", [])

    verdict_counts = {
        "EUR_USD": _family_counts_by_verdict(eur_families),
        "GBP_USD": _family_counts_by_verdict(gbp_families),
    }

    result = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE5_FULL_LOOP_VALIDATION",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
        },
        "dependency_proofs": {
            "phase2_entry_supply": p2.get("status"),
            "phase3_priority_pressure": p3.get("status"),
            "phase4_allocation_capacity": p4.get("status"),
        },
        "full_loop_runs": {
            "EUR_USD": {
                "net_pph_keep_tune": eur_net_pph,
                "accepted_trades": eur_trades,
                "hours": eur_hours,
                "session_lock_skip_count": int(eur_skips.get("ENTRY_SESSION_NOT_INCLUDED", 0)),
                "family_verdict_counts": verdict_counts["EUR_USD"],
                "contexts": sorted({
                    str(v.get("context"))
                    for v in eur_ctx.get("family_x_context", {}).values()
                    if isinstance(v, dict)
                }),
            },
            "GBP_USD": {
                "net_pph_keep_tune": gbp_net_pph,
                "accepted_trades": gbp_trades,
                "hours": gbp_hours,
                "session_lock_skip_count": int(gbp_skips.get("ENTRY_SESSION_NOT_INCLUDED", 0)),
                "family_verdict_counts": verdict_counts["GBP_USD"],
                "contexts": sorted({
                    str(v.get("context"))
                    for v in gbp_ctx.get("family_x_context", {}).values()
                    if isinstance(v, dict)
                }),
            },
            "weighted_net_pph_keep_tune": weighted_net_pph,
            "total_accepted_trades": eur_trades + gbp_trades,
            "total_hours": eur_hours + gbp_hours,
        },
    }

    pass_conditions = {
        "phase2_passed": p2.get("status") == "PASS",
        "phase3_passed": p3.get("status") == "PASS",
        "phase4_passed": p4.get("status") == "PASS",
        "eur_usd_positive_net_pph": eur_net_pph > 0.0,
        "gbp_usd_positive_net_pph": gbp_net_pph > 0.0,
        "weighted_positive_net_pph": weighted_net_pph > 0.0,
        "accepted_trades_exist": (eur_trades + gbp_trades) > 0,
        "session_lock_enforced": (
            int(eur_skips.get("ENTRY_SESSION_NOT_INCLUDED", 0)) > 0
            and int(gbp_skips.get("ENTRY_SESSION_NOT_INCLUDED", 0)) > 0
        ),
        "keep_family_exists_each_pair": (
            verdict_counts["EUR_USD"]["KEEP"] > 0
            and verdict_counts["GBP_USD"]["KEEP"] > 0
        ),
    }

    result["pass_conditions"] = pass_conditions
    result["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": result["status"], "pass_conditions": pass_conditions}, indent=2))


if __name__ == "__main__":
    main()
