#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SLICE_PATH = ROOT / "control" / "v2_engine" / "phase5" / "v2_approved_entries_aee_slice.json"
DEFAULT_SIMPLE_CONFIG_PATH = ROOT / "control" / "parallel_tracks" / "simple_aee_dead_trade_only_config.json"
DEFAULT_SUMMARY_PATH = ROOT / "control" / "parallel_tracks" / "v2_approved_entries_aee_summary.json"


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _run(cmd: list[str]) -> None:
    env = dict(**__import__("os").environ)
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            f"cmd={' '.join(cmd)}\n"
            f"stdout_tail={chr(10).join((proc.stdout or '').splitlines()[-30:])}\n"
            f"stderr_tail={chr(10).join((proc.stderr or '').splitlines()[-30:])}"
        )


def _summary_from_run(run_summary: dict[str, Any]) -> dict[str, Any]:
    results = dict(run_summary.get("results") or {})
    baseline_ab = dict(run_summary.get("ab_baseline_comparison") or {})
    candidate_vs = dict(baseline_ab.get("candidate_vs_baselines") or {})
    synthetic = dict(baseline_ab.get("synthetic_account") or {})
    candidate_account = dict((synthetic.get("accounts_by_mode") or {}).get("aee_candidate") or {})
    return {
        "trade_count": int(results.get("trade_count", 0) or 0),
        "realized_pph": float(results.get("realized_pph", 0.0) or 0.0),
        "avg_pips_per_trade": float(results.get("avg_pips_per_trade", 0.0) or 0.0),
        "gap": float(results.get("gap", 0.0) or 0.0),
        "extraction_efficiency": float(results.get("extraction_efficiency", 0.0) or 0.0),
        "candidate_vs_baselines": candidate_vs,
        "synthetic_candidate": candidate_account,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run static vs AEE comparison on the approved V2 entry slice.")
    parser.add_argument("--slice-file", default=str(DEFAULT_SLICE_PATH))
    parser.add_argument("--simple-config", default=str(DEFAULT_SIMPLE_CONFIG_PATH))
    parser.add_argument("--active-config", default="entry_v23_policy_guarded_active.json")
    parser.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_PATH))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    slice_path = Path(args.slice_file)
    if not slice_path.is_absolute():
        slice_path = (ROOT / slice_path).resolve()
    simple_config = Path(args.simple_config)
    if not simple_config.is_absolute():
        simple_config = (ROOT / simple_config).resolve()
    active_config = Path(args.active_config)
    if not active_config.is_absolute():
        active_config = (ROOT / active_config).resolve()
    summary_out = Path(args.summary_out)
    if not summary_out.is_absolute():
        summary_out = (ROOT / summary_out).resolve()

    if not slice_path.exists():
        _run([sys.executable, str((ROOT / "tools" / "build_v2_approved_aee_slice.py").resolve()), "--out", str(slice_path)])

    run_tag = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    slice_payload = _json_load(slice_path)
    all_trades = list(slice_payload.get("trades") or [])
    if not all_trades:
        raise RuntimeError(f"No trades found in slice: {slice_path}")

    target_slices: dict[str, Path] = {}
    slice_root = ROOT / "control" / "parallel_tracks" / f"V2_APPROVED_SLICE_RUNSET_{run_tag}"
    aggregate_slice = slice_root / "ALL_APPROVED.json"
    _json_write(aggregate_slice, slice_payload)
    target_slices["ALL_APPROVED"] = aggregate_slice

    counts_by_strategy = dict(slice_payload.get("counts_by_strategy") or {})
    for strategy_id in sorted(counts_by_strategy):
        strategy_trades = [
            trade for trade in all_trades
            if str((trade.get("meta") or {}).get("strategy_id", "") or "") == strategy_id
        ]
        strategy_slice = {
            **slice_payload,
            "count": len(strategy_trades),
            "counts_by_strategy": {strategy_id: len(strategy_trades)},
            "trades": strategy_trades,
        }
        out_path = slice_root / f"{strategy_id}.json"
        _json_write(out_path, strategy_slice)
        target_slices[strategy_id] = out_path

    run_specs = [
        {
            "name": "simple_aee_dead_trade_only",
            "config": simple_config,
            "result_dir": ROOT / "control" / "parallel_tracks" / f"V2_APPROVED_SIMPLE_AEE_{run_tag}",
            "strategy_form": "simple_aee_dead_trade_only",
        },
        {
            "name": "entry_v23_policy_guarded_active",
            "config": active_config,
            "result_dir": ROOT / "control" / "parallel_tracks" / f"V2_APPROVED_ACTIVE_AEE_{run_tag}",
            "strategy_form": "entry_v23_policy_guarded_active",
        },
    ]

    bucket_summaries: dict[str, Any] = {}
    for bucket_name, bucket_slice_path in target_slices.items():
        bucket_summaries[bucket_name] = {
            "slice_path": str(bucket_slice_path),
            "trade_count": int(_json_load(bucket_slice_path).get("count", 0) or 0),
            "runs": {},
        }
        for spec in run_specs:
            result_dir = Path(
                str(spec["result_dir"]).replace(run_tag, f"{run_tag}_{bucket_name}")
            ).resolve()
            _run(
                [
                    sys.executable,
                    str((ROOT / "run_aee_active_policy_evidencepack.py").resolve()),
                    "--config",
                    str(Path(spec["config"]).resolve()),
                    "--slice-file",
                    str(bucket_slice_path),
                    "--allow-non-entry-truth-contract",
                    "--result-dir",
                    str(result_dir),
                    "--out",
                    str(result_dir / "active_policy_run_report.json"),
                    "--context-out",
                    str(result_dir / "active_policy_run_by_context.json"),
                    "--run-id",
                    f"V2_APPROVED_{bucket_name}_{spec['name'].upper()}_{run_tag}",
                    "--strategy-form",
                    str(spec["strategy_form"]),
                    "--dataset-window-id",
                    f"V2_APPROVED_ENTRIES_SLICE::{bucket_name}",
                    "--pair",
                    "EUR_USD",
                ]
            )
            run_summary = _json_load(result_dir / "run_summary_active.json")
            bucket_summaries[bucket_name]["runs"][spec["name"]] = {
                "result_dir": str(result_dir),
                "summary": _summary_from_run(run_summary),
            }

    summary = {
        "artifact": "v2_approved_entries_aee_summary",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "slice_path": str(slice_path),
        "slice_trade_count": int(slice_payload.get("count", 0) or 0),
        "counts_by_strategy": dict(slice_payload.get("counts_by_strategy") or {}),
        "buckets": bucket_summaries,
    }
    _json_write(summary_out, summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
