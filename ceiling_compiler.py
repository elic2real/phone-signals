from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compile_current_base() -> dict:
    dataset_lock = load_json(ROOT / "dataset_lock.json")
    entry_unified = load_json(ROOT / "entry_metric_ceiling_report_unified.json")
    aee_rules = load_json(ROOT / "aee_state_machine_rules.json")
    aee_long = load_json(ROOT / "aee_state_machine_replay_long.json")
    aee_short = load_json(ROOT / "aee_state_machine_replay_short.json")
    aee_combined = load_json(ROOT / "aee_state_machine_replay_combined.json")

    inputs = [
        ROOT / "dataset_lock.json",
        ROOT / "entry_metric_ceiling_report_unified.json",
        ROOT / "aee_state_machine_rules.json",
        ROOT / "aee_state_machine_replay_long.json",
        ROOT / "aee_state_machine_replay_short.json",
        ROOT / "aee_state_machine_replay_combined.json",
    ]

    manifest = {
        "compiler_version": "first_pass_inventory_compiler",
        "node": {
            "pair": dataset_lock.get("pair"),
            "weekday": dataset_lock.get("weekday"),
            "session": str(dataset_lock.get("session", "")).upper(),
            "quarter": "SESSION_FULL",
        },
        "inputs": [
            {"path": str(path.relative_to(ROOT)), "sha256": sha256_file(path)}
            for path in inputs
            if path.exists()
        ],
    }

    compiled_entry_thresholds = {
        "source": "entry_metric_ceiling_report_unified.json",
        "available_sections": sorted(entry_unified.keys()) if isinstance(entry_unified, dict) else [],
    }
    compiled_partial_runner_thresholds = {
        "source": "entry_metric_ceiling_report_unified.json",
        "note": "Runner partial payout parameters remain embedded in the unified entry report.",
    }
    compiled_aee_thresholds = {
        "source": "aee_state_machine_rules.json",
        "rules": aee_rules,
    }
    compiled_ceiling_report = {
        "node": manifest["node"],
        "entry_source": "entry_metric_ceiling_report_unified.json",
        "aee_source": "aee_state_machine_replay_combined.json",
        "long": aee_long,
        "short": aee_short,
        "combined": aee_combined,
    }
    threshold_derivation_report = {
        "status": "PARTIAL",
        "manual_tuning_replaced": False,
        "explanation": [
            "This compiler currently composes the solved EUR_USD base node from existing calibrated artifacts.",
            "Distribution-driven first-pass threshold derivation across all nodes is not complete yet.",
        ],
        "sources": {
            "entry": "entry_metric_ceiling_report_unified.json",
            "aee_rules": "aee_state_machine_rules.json",
            "aee_replay": "aee_state_machine_replay_combined.json",
        },
    }

    outputs = {
        "compiled_entry_thresholds.json": compiled_entry_thresholds,
        "compiled_partial_runner_thresholds.json": compiled_partial_runner_thresholds,
        "compiled_aee_thresholds.json": compiled_aee_thresholds,
        "compiled_ceiling_report.json": compiled_ceiling_report,
        "threshold_derivation_report.json": threshold_derivation_report,
    }

    for name, obj in outputs.items():
        (ROOT / name).write_text(json.dumps(obj, indent=2))

    return {"manifest": manifest, "outputs": list(outputs.keys())}


def main() -> None:
    summary = compile_current_base()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
