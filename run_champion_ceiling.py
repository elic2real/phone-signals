#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parent


def run(cmd: Sequence[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def jload(path: Path) -> dict:
    return json.loads(path.read_text())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dataset-lock",
        type=Path,
        default=ROOT / "dataset_lock_11_sessions.json",
        help="Locked dataset json for the canonical 11-session EUR_USD Monday London build.",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "compiled_champion_ceiling_11_sessions",
        help="Unified output root for the champion replay.",
    )
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    stage1_6_dir = out_dir / "stage1_6"
    state_dir = out_dir / "session_state"
    trigger_dir = out_dir / "trigger_machine"
    context_dir = out_dir / "energy_context"
    regime_dir = context_dir / "regime_classifier"
    regime_gate_dir = context_dir / "island_regime_gate_replay"
    point_dir = out_dir / "point_energy_trajectory"
    point_gate_dir = point_dir / "island_point_gate_replay"

    # Stage 1-6 deterministic substrate.
    run(
        [
            "python3",
            str(ROOT / "stage1_5_deterministic_compiler.py"),
            "--dataset-lock",
            str(args.dataset_lock),
            "--output-root",
            str(stage1_6_dir),
        ]
    )

    # Continuous state stream.
    run(
        [
            "python3",
            str(ROOT / "build_session_state_stream.py"),
            "--data-root",
            str(ROOT / "london_session_data_11"),
            "--output-dir",
            str(state_dir),
        ]
    )

    # Stream trigger machine.
    run(
        [
            "python3",
            str(ROOT / "build_entry_trigger_state_machine.py"),
            "--state-truth-csv",
            str(state_dir / "state_action_truth_table.csv"),
            "--output-dir",
            str(trigger_dir),
        ]
    )

    # Energy context + island regime compatibility.
    run(
        [
            "python3",
            str(ROOT / "build_energy_context_engine.py"),
            "--stream-csv",
            str(state_dir / "state_action_truth_table.csv"),
            "--rules-json",
            str(trigger_dir / "entry_trigger_state_machine.json"),
            "--output-dir",
            str(context_dir),
        ]
    )
    run(
        [
            "python3",
            str(ROOT / "build_energy_regime_classifier.py"),
            "--context-stream-csv",
            str(context_dir / "session_energy_context_stream.csv"),
            "--rules-json",
            str(trigger_dir / "entry_trigger_state_machine.json"),
            "--output-dir",
            str(regime_dir),
        ]
    )
    run(
        [
            "python3",
            str(ROOT / "apply_island_regime_gate.py"),
            "--context-stream-csv",
            str(regime_dir / "full_stream_regimes.csv"),
            "--rules-json",
            str(trigger_dir / "entry_trigger_state_machine.json"),
            "--regime-report-json",
            str(regime_dir / "energy_regime_report.json"),
            "--output-dir",
            str(regime_gate_dir),
        ]
    )

    # Point-level trajectory + island-specific trajectory compatibility.
    run(
        [
            "python3",
            str(ROOT / "build_point_energy_trajectory.py"),
            "--context-stream-csv",
            str(context_dir / "session_energy_context_stream.csv"),
            "--truth-csv",
            str(state_dir / "state_action_truth_table.csv"),
            "--output-dir",
            str(point_dir),
        ]
    )
    run(
        [
            "python3",
            str(ROOT / "apply_island_point_trajectory_gate.py"),
            "--trajectory-csv",
            str(point_dir / "point_energy_trajectory.csv"),
            "--rules-json",
            str(trigger_dir / "entry_trigger_state_machine.json"),
            "--regime-report-json",
            str(regime_dir / "energy_regime_report.json"),
            "--output-dir",
            str(point_gate_dir),
        ]
    )

    final_report = jload(point_gate_dir / "island_point_trajectory_gate_report.json")
    manifest = {
        "dataset_lock": str(args.dataset_lock),
        "stage1_6_output_dir": str(stage1_6_dir),
        "session_state_output_dir": str(state_dir),
        "trigger_machine_output_dir": str(trigger_dir),
        "energy_context_output_dir": str(context_dir),
        "regime_classifier_output_dir": str(regime_dir),
        "regime_gate_output_dir": str(regime_gate_dir),
        "point_trajectory_output_dir": str(point_dir),
        "point_gate_output_dir": str(point_gate_dir),
        "final_report": str(point_gate_dir / "island_point_trajectory_gate_report.json"),
    }
    (out_dir / "champion_manifest.json").write_text(json.dumps(manifest, indent=2))
    (out_dir / "champion_report.json").write_text(json.dumps(final_report, indent=2))
    print(json.dumps({"status": "PASS", "output_dir": str(out_dir), "after": final_report["after"]}, indent=2))


if __name__ == "__main__":
    main()
