#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iso_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _discover_contexts(root: Path, pair: str, session: str) -> list[str]:
    pair_norm = pair.upper().replace("/", "_")
    session_lc = session.strip().lower()
    glob_pat = f"compiled_market_nodes/{pair_norm}__*/aee_stage/aee_state_stream/aee_state_stream.csv"
    contexts: set[str] = set()
    for path in root.glob(glob_pat):
        try:
            context = path.parts[-4]
        except Exception:
            continue
        parts = context.split("__")
        if len(parts) >= 3 and parts[-1].lower() == session_lc:
            contexts.add(context.lower())
    return sorted(contexts)


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run constrained MVP proof loop and emit one verdict report")
    ap.add_argument("--profile", default="control/mvp_profile.json")
    ap.add_argument("--run-tag", default="")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    profile_path = Path(args.profile)
    if not profile_path.is_absolute():
        profile_path = (root / profile_path).resolve()
    if not profile_path.exists():
        raise SystemExit(f"ERROR: profile not found: {profile_path}")

    profile = _load_json(profile_path)
    run_tag = str(args.run_tag or _iso_tag())

    baseline_config = str(profile.get("baseline_config", "entry_v23_policy_guarded_active.json"))
    variant_spec = str(profile.get("variant_spec", "control/variant_spec_template.json"))
    aee_version = str(profile.get("aee_version", "v3"))
    session = str(profile.get("session", "new_york"))
    pairs = [str(x).upper().replace("/", "_") for x in (profile.get("pairs") or [])]
    max_trades = int(profile.get("max_trades_per_pair", 120) or 120)
    workers = int(profile.get("matrix_workers", 2) or 2)
    matrix_slice = str(profile.get("run_matrix_on_slice", "fake_runner_slice"))

    if not pairs:
        raise SystemExit("ERROR: profile has no pairs")

    rows: list[dict[str, Any]] = []
    for pair in pairs:
        contexts = _discover_contexts(root, pair, session)
        include_contexts = ",".join(contexts)

        pair_run_dir = root / "control" / "mvp_runs" / run_tag / pair / "baseline"
        pair_run_dir.mkdir(parents=True, exist_ok=True)

        run_id = f"AEE_MVP_{pair}_{run_tag}"[:120]
        run_cmd = [
            sys.executable,
            str(root / "run_aee_active_policy_evidencepack.py"),
            "--config",
            baseline_config,
            "--pair",
            pair,
            "--aee-version",
            aee_version,
            "--run-id",
            run_id,
            "--result-dir",
            str(pair_run_dir),
            "--out",
            str(pair_run_dir / "main_report.json"),
            "--context-out",
            str(pair_run_dir / "context_report.json"),
            "--max-trades",
            str(max_trades),
        ]
        if include_contexts:
            run_cmd += ["--include-contexts", include_contexts]

        proc = _run(run_cmd, root)
        row: dict[str, Any] = {
            "pair": pair,
            "contexts": contexts,
            "baseline_exit_code": int(proc.returncode),
            "baseline_stdout_tail": "\n".join((proc.stdout or "").splitlines()[-20:]),
            "baseline_stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
            "baseline_result_dir": str(pair_run_dir),
        }
        if proc.returncode != 0:
            rows.append(row)
            continue

        rs = _load_json(pair_run_dir / "run_summary_active.json")
        mc = _load_json(pair_run_dir / "aee_module_collision_audit_active.json")
        sc = _load_json(pair_run_dir / "aee_simplicity_reality_check_active.json")

        results = rs.get("results", {}) if isinstance(rs, dict) else {}
        metrics = mc.get("metrics", {}) if isinstance(mc, dict) else {}
        row["metrics"] = {
            "realized_pph": float(results.get("realized_pph", 0.0) or 0.0),
            "gap": float(results.get("gap", 0.0) or 0.0),
            "extraction_efficiency": float(results.get("extraction_efficiency", 0.0) or 0.0),
            "objective_collision_detected": bool(mc.get("objective_collision_detected", False)),
            "fake_runner_count": int(metrics.get("fake_runner_count", 0) or 0),
            "overheld_winner_count": int(metrics.get("overheld_winner_count", 0) or 0),
            "bankable_green_loss_red_rate": float(metrics.get("bankable_green_loss_red_rate", 0.0) or 0.0),
            "complexity_adds_value": bool(sc.get("complexity_adds_value", False)),
        }

        # Build per-pair failure slices.
        slices_dir = root / "control" / "failure_slices" / run_tag / pair
        slices_dir.mkdir(parents=True, exist_ok=True)
        slice_cmd = [
            sys.executable,
            str(root / "tools" / "build_failure_slices.py"),
            "--green-loss-audit",
            str(pair_run_dir / "aee_green_loss_audit_active.json"),
            "--trade-sample",
            str(pair_run_dir / "trade_evidence_sample_active.json"),
            "--out-dir",
            str(slices_dir),
            "--max-per-slice",
            str(max_trades),
        ]
        sproc = _run(slice_cmd, root)
        row["slice_exit_code"] = int(sproc.returncode)
        row["slice_index"] = str(slices_dir / "index.json")

        # Optional matrix run on a target slice.
        target_slice_path = slices_dir / f"{matrix_slice}.json"
        if sproc.returncode == 0 and target_slice_path.exists():
            mtag = f"MVP_{pair}_{run_tag}"
            matrix_cmd = [
                sys.executable,
                str(root / "tools" / "run_aee_variant_matrix.py"),
                "--base-config",
                baseline_config,
                "--variant-spec",
                variant_spec,
                "--pair",
                pair,
                "--aee-version",
                aee_version,
                "--slice-file",
                str(target_slice_path),
                "--max-trades",
                str(max_trades),
                "--workers",
                str(workers),
                "--run-tag",
                mtag,
            ]
            mproc = _run(matrix_cmd, root)
            row["matrix_exit_code"] = int(mproc.returncode)
            row["matrix_run_tag"] = mtag
            row["matrix_summary"] = str(root / "control" / "variant_runs" / mtag / "variant_matrix_summary.json")

        rows.append(row)

    # Aggregate proof verdict.
    require_positive_net = bool(profile.get("require_positive_net", True))
    require_positive_realized = bool(profile.get("require_positive_realized_pph", True))
    require_collision_clear = bool(profile.get("require_collision_clear", True))
    max_fake_runner_count = int(profile.get("max_fake_runner_count", 25) or 25)
    max_overheld_winner_count = int(profile.get("max_overheld_winner_count", 2) or 2)
    require_complexity_adds_value = bool(profile.get("require_complexity_adds_value", False))

    passed_pairs = 0
    for row in rows:
        m = row.get("metrics") or {}
        if not m:
            row["mvp_pass"] = False
            continue
        pass_net = (float(m.get("realized_pph", 0.0)) > 0.0) if require_positive_net else True
        pass_realized = (float(m.get("realized_pph", 0.0)) > 0.0) if require_positive_realized else True
        pass_collision = (not bool(m.get("objective_collision_detected", True))) if require_collision_clear else True
        pass_fake_runner = int(m.get("fake_runner_count", 10**9)) <= max_fake_runner_count
        pass_overhold = int(m.get("overheld_winner_count", 10**9)) <= max_overheld_winner_count
        pass_complexity = (bool(m.get("complexity_adds_value", False)) == require_complexity_adds_value)
        row["gate_results"] = {
            "pass_net": bool(pass_net),
            "pass_realized": bool(pass_realized),
            "pass_collision": bool(pass_collision),
            "pass_fake_runner": bool(pass_fake_runner),
            "pass_overhold": bool(pass_overhold),
            "pass_complexity": bool(pass_complexity),
        }
        row["mvp_pass"] = bool(
            pass_net
            and pass_realized
            and pass_collision
            and pass_fake_runner
            and pass_overhold
            and pass_complexity
        )
        if row["mvp_pass"]:
            passed_pairs += 1

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_tag": run_tag,
        "profile": profile,
        "pairs_tested": len(rows),
        "pairs_passed": passed_pairs,
        "overall_pass": (passed_pairs == len(rows)) and len(rows) > 0,
        "rows": rows,
    }

    out_path = root / "control" / "mvp_proof_report.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
