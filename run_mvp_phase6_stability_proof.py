#!/usr/bin/env python3
"""Build MVP Phase 6 no-tuning 3-window stability proof under locked scope."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

OUTPUT_PATH = Path("control/mvp_phase6_stability_proof.json")
PHASE5_PATH = Path("control/mvp_phase5_full_loop_validation.json")

WINDOW_CONFIGS = [
    {
        "window": "W1",
        "eur_path": Path("control/mvp_phase6_w1_eur_usd.json"),
        "gbp_path": Path("control/mvp_phase6_w1_gbp_usd.json"),
        "eur_ctx": Path("control/mvp_phase6_w1_eur_usd_context.json"),
        "gbp_ctx": Path("control/mvp_phase6_w1_gbp_usd_context.json"),
    },
    {
        "window": "W2",
        "eur_path": Path("control/mvp_phase6_w2_eur_usd.json"),
        "gbp_path": Path("control/mvp_phase6_w2_gbp_usd.json"),
        "eur_ctx": Path("control/mvp_phase6_w2_eur_usd_context.json"),
        "gbp_ctx": Path("control/mvp_phase6_w2_gbp_usd_context.json"),
    },
    {
        "window": "W3",
        "eur_path": Path("control/mvp_phase6_w3_eur_usd.json"),
        "gbp_path": Path("control/mvp_phase6_w3_gbp_usd.json"),
        "eur_ctx": Path("control/mvp_phase6_w3_eur_usd_context.json"),
        "gbp_ctx": Path("control/mvp_phase6_w3_gbp_usd_context.json"),
    },
]


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _contexts(ctx: Dict[str, Any]) -> List[str]:
    return sorted(
        {
            str(v.get("context"))
            for v in ctx.get("family_x_context", {}).values()
            if isinstance(v, dict)
        }
    )


def _weighted(a: float, ah: float, b: float, bh: float) -> float:
    total_h = ah + bh
    if total_h <= 0:
        return 0.0
    return ((a * ah) + (b * bh)) / total_h


def _all_contexts_london(contexts: List[str]) -> bool:
    return bool(contexts) and all("__london" in c.lower() for c in contexts)


def main() -> None:
    phase5 = _load_json(PHASE5_PATH)

    per_window: List[Dict[str, Any]] = []
    weighted_values: List[float] = []

    for cfg in WINDOW_CONFIGS:
        eur = _load_json(cfg["eur_path"])
        gbp = _load_json(cfg["gbp_path"])
        eur_ctx = _load_json(cfg["eur_ctx"])
        gbp_ctx = _load_json(cfg["gbp_ctx"])

        eur_net = float(eur.get("combined_keep_tune_net_pph", 0.0))
        gbp_net = float(gbp.get("combined_keep_tune_net_pph", 0.0))
        eur_hours = float(eur.get("total_hours", 0.0))
        gbp_hours = float(gbp.get("total_hours", 0.0))
        eur_trades = int(eur.get("total_accepted_trades", 0))
        gbp_trades = int(gbp.get("total_accepted_trades", 0))
        eur_skips = int(eur.get("skipped_by_entry_filter", {}).get("ENTRY_SESSION_NOT_INCLUDED", 0))
        gbp_skips = int(gbp.get("skipped_by_entry_filter", {}).get("ENTRY_SESSION_NOT_INCLUDED", 0))

        eur_contexts = _contexts(eur_ctx)
        gbp_contexts = _contexts(gbp_ctx)

        weighted_net = _weighted(eur_net, eur_hours, gbp_net, gbp_hours)
        weighted_values.append(weighted_net)

        per_window.append(
            {
                "window": cfg["window"],
                "EUR_USD": {
                    "net_pph_keep_tune": eur_net,
                    "hours": eur_hours,
                    "accepted_trades": eur_trades,
                    "entry_session_not_included": eur_skips,
                    "contexts": eur_contexts,
                },
                "GBP_USD": {
                    "net_pph_keep_tune": gbp_net,
                    "hours": gbp_hours,
                    "accepted_trades": gbp_trades,
                    "entry_session_not_included": gbp_skips,
                    "contexts": gbp_contexts,
                },
                "weighted_net_pph_keep_tune": weighted_net,
                "window_status": "PASS"
                if (
                    eur_net > 0.0
                    and gbp_net > 0.0
                    and weighted_net > 0.0
                    and eur_trades > 0
                    and gbp_trades > 0
                    and eur_skips > 0
                    and gbp_skips > 0
                    and _all_contexts_london(eur_contexts)
                    and _all_contexts_london(gbp_contexts)
                )
                else "FAIL",
            }
        )

    weighted_mean = mean(weighted_values) if weighted_values else 0.0
    weighted_min = min(weighted_values) if weighted_values else 0.0
    weighted_max = max(weighted_values) if weighted_values else 0.0
    weighted_relative_spread = (
        (weighted_max - weighted_min) / weighted_mean if weighted_mean > 0.0 else 0.0
    )

    pass_conditions = {
        "phase5_dependency_passed": phase5.get("status") == "PASS",
        "all_windows_positive_weighted_net": all(v > 0.0 for v in weighted_values),
        "all_windows_pair_positive_net": all(
            w["EUR_USD"]["net_pph_keep_tune"] > 0.0 and w["GBP_USD"]["net_pph_keep_tune"] > 0.0
            for w in per_window
        ),
        "all_windows_have_trades": all(
            w["EUR_USD"]["accepted_trades"] > 0 and w["GBP_USD"]["accepted_trades"] > 0
            for w in per_window
        ),
        "session_lock_enforced_all_windows": all(
            w["EUR_USD"]["entry_session_not_included"] > 0
            and w["GBP_USD"]["entry_session_not_included"] > 0
            for w in per_window
        ),
        "london_context_only_all_windows": all(
            _all_contexts_london(w["EUR_USD"]["contexts"])
            and _all_contexts_london(w["GBP_USD"]["contexts"])
            for w in per_window
        ),
        "weighted_net_relative_spread_bounded": weighted_relative_spread <= 0.80,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE6_STABILITY_PROOF_3WINDOW_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "dependency": {
            "phase5_full_loop_validation_status": phase5.get("status"),
            "phase5_path": str(PHASE5_PATH),
        },
        "windows": per_window,
        "aggregates": {
            "weighted_net_pph_values": weighted_values,
            "weighted_net_pph_mean": weighted_mean,
            "weighted_net_pph_min": weighted_min,
            "weighted_net_pph_max": weighted_max,
            "weighted_net_relative_spread": weighted_relative_spread,
        },
        "pass_conditions": pass_conditions,
    }

    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "pass_conditions": pass_conditions}, indent=2))


if __name__ == "__main__":
    main()
