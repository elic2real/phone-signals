#!/usr/bin/env python3
from __future__ import annotations

import itertools
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EVAL = ROOT / "run_aee_band_floor_baseline.py"
OUT_DIR = ROOT / "aee_reverse_engineer"
CAND_DIR = OUT_DIR / "candidates"
RUN_DIR = OUT_DIR / "runs"


@dataclass
class Cand:
    name: str
    cfg: dict


def _load_split(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _candidate_grid() -> list[Cand]:
    # Intentional overfit grid for discovery-only stage.
    near_timeouts = [8, 10, 12, 14]
    never_green_min_r = [0.04, 0.05, 0.06]
    required_z = [2, 3]
    profit_activation = [0.10, 0.14, 0.18]
    stall_timeout = [2.0, 2.5, 3.0]
    fallback_activation = [0.15, 0.20, 0.25]
    fallback_giveback = [0.04, 0.05, 0.06]

    grid = list(itertools.product(
        near_timeouts,
        never_green_min_r,
        required_z,
        profit_activation,
        stall_timeout,
        fallback_activation,
        fallback_giveback,
    ))

    # Keep bounded but diverse subset.
    selected = [grid[i] for i in range(0, len(grid), max(1, len(grid) // 12))][:12]
    cands: list[Cand] = []
    for i, (nt, ngr, rz, pa, st, fa, fg) in enumerate(selected, start=1):
        cfg = {
            "name": f"aee_ped_discovery_overfit_{i:02d}",
            "doctrine": "reverse_engineered_positive_escape",
            "objective": "maximize_net_realized_extraction_per_hour",
            "band_size_r": 0.10,
            "near_entry": {
                "never_green_timeout_sec": nt,
                "never_green_min_r": ngr,
                "required_positive_escape_z": rz,
            },
            "profit_activation_r": pa,
            "stall_timeout_sec": st,
            "fallback_activation_r": fa,
            "fallback_giveback_r": fg,
            "extension_activation_r": max(0.20, fa + 0.05),
            "extension_min_velocity": 0.0,
            "fast_adverse": {"adverse_r": 0.45, "window_sec": 1.5},
            "defensive": {"pre_sl_exit_enabled": True, "panic_exit_enabled": True},
            "branch_set": [
                "AEE_BAND_FAST_FAILURE_EXIT",
                "AEE_BAND_POSITIVE_ESCAPE_TIMEOUT",
                "AEE_BAND_POST_ESCAPE_PROFIT_STALL_EXIT",
                "AEE_BAND_POST_ESCAPE_FALLBACK_EXIT",
                "AEE_PRE_SL_EXIT",
                "AEE_PANIC_EXIT",
                "AEE_BAND_EXTENSION_DECAY_EXIT",
            ],
            "optional_branch": "AEE_BAND_EXTENSION_HOLD",
        }
        cands.append(Cand(cfg["name"], cfg))
    return cands


def _run_eval(config_path: Path, dataset_id: str, streams: list[str], tag: str) -> dict:
    run_out = RUN_DIR / f"{tag}_run.json"
    dist_out = RUN_DIR / f"{tag}_distribution.json"
    runbook_out = RUN_DIR / f"{tag}_runbook.json"
    cand_out = RUN_DIR / f"{tag}_candidate_table.json"
    ci_out = RUN_DIR / f"{tag}_ci_report.json"
    final_out = RUN_DIR / f"{tag}_final_decision.json"

    if run_out.exists():
        return json.loads(run_out.read_text(encoding="utf-8"))

    cmd = [
        "python3",
        str(EVAL),
        "--config",
        str(config_path),
        "--max-streams",
        str(max(1, len(streams))),
        "--dataset-id",
        dataset_id,
        "--spread-pips",
        "0.8",
        "--slippage-pips-per-side",
        "0.15",
        "--commission-pips-roundtrip",
        "0.0",
        "--latency-penalty-pips",
        "0.0",
        "--deep-loss-cap",
        "0.04",
        "--economic-viability-mult",
        "1.10",
        "--epsilon-pips-per-hour",
        "0.02",
        "--ci-bootstrap-samples",
        "400",
        "--ci-seed",
        "1337",
        "--run-out",
        str(run_out),
        "--dist-out",
        str(dist_out),
        "--runbook-out",
        str(runbook_out),
        "--candidate-table-out",
        str(cand_out),
        "--ci-report-out",
        str(ci_out),
        "--final-decision-out",
        str(final_out),
    ]
    for s in streams:
        cmd.extend(["--stream-glob", s])

    subprocess.run(cmd, check=True, cwd=str(ROOT), capture_output=True, text=True)
    return json.loads(run_out.read_text(encoding="utf-8"))


def _minimal_rule_set(best_cfg: dict) -> dict:
    # Trigger-focused extraction: preserve logic shape, avoid hard-coding exact numeric fit.
    return {
        "name": "aee_minimal_rule_set_v1",
        "source": best_cfg.get("name", "unknown"),
        "rules": [
            {
                "id": "rule_probation_positive_escape",
                "trigger": "trade must show early positive excursion before monetization",
                "intent": "eliminate noise churn that never proves positive value",
            },
            {
                "id": "rule_post_escape_stall_harvest",
                "trigger": "after positive escape, close on stall/no-new-high condition",
                "intent": "convert weak continuation into realized positive extraction",
            },
            {
                "id": "rule_post_escape_fallback_protect",
                "trigger": "after meaningful favorable excursion, close on giveback/fallback",
                "intent": "protect realized edge from round-trip decay",
            },
            {
                "id": "rule_fast_adverse_and_defense",
                "trigger": "fast failure, pre-SL, and panic exits remain active throughout",
                "intent": "bound downside while waiting for positive proof",
            },
        ],
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CAND_DIR.mkdir(parents=True, exist_ok=True)
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    split = _load_split(ROOT / "aee_dataset_split_v1.json")
    D = split["D"]
    T = split["T"]
    H = split["H"]

    candidates = _candidate_grid()
    discovery_rows = []

    for c in candidates:
        cfg_path = CAND_DIR / f"{c.name}.json"
        _write_json(cfg_path, c.cfg)
        run = _run_eval(cfg_path, "D", D, c.name)
        discovery_rows.append(
            {
                "candidate": c.name,
                "config_path": str(cfg_path.relative_to(ROOT)),
                "gross_delta_pph": run["baseline_delta"]["delta_realized_pips_per_hour"],
                "net_delta_pph": run["net_delta_realized_pips_per_hour"],
                "avg_pips_per_trade": run["avg_pips_per_trade"],
                "required_min_pips_per_trade": run["economic_viability"]["required_min_pips_per_trade"],
                "economic_viability_ok": run["economic_viability"]["ok"],
                "verdict": run["verdict"],
            }
        )

    discovery_rows.sort(key=lambda x: (x["net_delta_pph"], x["gross_delta_pph"]), reverse=True)
    best = discovery_rows[0]
    best_cfg_path = ROOT / best["config_path"]
    best_cfg = json.loads(best_cfg_path.read_text(encoding="utf-8"))

    _write_json(OUT_DIR / "aee_reverse_engineer_discovery_report.json", {
        "name": "aee_reverse_engineer_discovery_report",
        "dataset_id": "D",
        "candidate_count": len(discovery_rows),
        "best_candidate": best,
        "scoreboard": discovery_rows,
    })

    _write_json(ROOT / "aee_minimal_rule_set.json", _minimal_rule_set(best_cfg))

    tuning_run = _run_eval(best_cfg_path, "T", T, "best_from_D_on_T")
    holdout_run = _run_eval(best_cfg_path, "H", H, "best_from_D_on_H")

    decision = "PROMOTE" if holdout_run["net_delta_realized_pips_per_hour"] > 0.0 and holdout_run["gate_checks"].get("ci_ok", False) else "REJECT"

    _write_json(OUT_DIR / "aee_reverse_engineer_validation_v1.json", {
        "name": "aee_reverse_engineer_validation_v1",
        "best_candidate": best,
        "tuning_metrics": {
            "net_delta_pph": tuning_run["net_delta_realized_pips_per_hour"],
            "gross_delta_pph": tuning_run["baseline_delta"]["delta_realized_pips_per_hour"],
            "economic_viability_ok": tuning_run["economic_viability"]["ok"],
            "ci_ok": tuning_run["gate_checks"].get("ci_ok", False),
        },
        "holdout_metrics": {
            "net_delta_pph": holdout_run["net_delta_realized_pips_per_hour"],
            "gross_delta_pph": holdout_run["baseline_delta"]["delta_realized_pips_per_hour"],
            "economic_viability_ok": holdout_run["economic_viability"]["ok"],
            "ci_ok": holdout_run["gate_checks"].get("ci_ok", False),
        },
        "final_decision": decision,
    })

    print(json.dumps({
        "discovery_candidates": len(discovery_rows),
        "best_candidate": best["candidate"],
        "best_discovery_net_delta_pph": best["net_delta_pph"],
        "tuning_net_delta_pph": tuning_run["net_delta_realized_pips_per_hour"],
        "holdout_net_delta_pph": holdout_run["net_delta_realized_pips_per_hour"],
        "final_decision": decision,
    }, indent=2))


if __name__ == "__main__":
    main()
