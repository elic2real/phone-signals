from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aee_live_doctrine import ACTION_TIGHTEN, LiveDoctrineEngine


def build_forced_tighten_fixture() -> dict[str, Any]:
    # Step 1 primes path-state so the trade is meaningfully green.
    step1 = {
        "trade_key": "forced-tighten-001",
        "mode": "RUNNER",
        "now_s": 12.0,
        "current_r": 0.20,
        "mfe_r": 0.20,
        "mae_r": -0.01,
        "energy": 0.62,
        "force_close": False,
    }

    # Step 2 weakens continuation enough for TIGHTEN, but not for CLOSE.
    step2 = {
        "trade_key": "forced-tighten-001",
        "mode": "RUNNER",
        "now_s": 44.0,
        "current_r": 0.03,
        "mfe_r": 0.20,
        "mae_r": -0.01,
        "energy": 0.34,
        "force_close": False,
    }

    return {"priming_snapshot": step1, "tighten_snapshot": step2}


def run_forced_tighten_handoff_proof() -> dict[str, Any]:
    engine = LiveDoctrineEngine()
    fixture = build_forced_tighten_fixture()

    priming_input = fixture["priming_snapshot"]
    tighten_input = fixture["tighten_snapshot"]

    priming_result = engine.update(**priming_input)
    tighten_result = engine.update(**tighten_input)

    passed = str(tighten_result.get("action")) == ACTION_TIGHTEN

    return {
        "proof_name": "aee_tighten_handoff_proof",
        "type": "synthetic_handoff",
        "goal": "Force deterministic TIGHTEN classification from doctrine path-state inputs.",
        "forced_case": {
            "input_snapshot": tighten_input,
            "engine_received": tighten_input,
            "action_returned": tighten_result.get("action"),
            "result_state": tighten_result.get("state", {}),
            "priming": {
                "input_snapshot": priming_input,
                "action_returned": priming_result.get("action"),
                "result_state": priming_result.get("state", {}),
            },
        },
        "pass_fail": {
            "expected_action": ACTION_TIGHTEN,
            "actual_action": tighten_result.get("action"),
            "passed": passed,
        },
        "notes": [
            "Synthetic handoff proof closes the missing TIGHTEN doctrine class without waiting for rare real-tape runner paths.",
            "This validates doctrine classification compatibility, not end-to-end live order execution.",
        ],
    }


def main() -> None:
    report = run_forced_tighten_handoff_proof()
    out_path = Path("aee_tighten_handoff_proof.json")
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {out_path}")
    print(json.dumps(report["pass_fail"], indent=2))


if __name__ == "__main__":
    main()
