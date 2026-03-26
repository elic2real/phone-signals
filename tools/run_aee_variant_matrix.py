#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _deep_merge(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            _deep_merge(dst[key], value)
        else:
            dst[key] = value
    return dst


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _variant_configs(
    root: Path,
    base_config_path: Path,
    variants_spec: list[dict[str, Any]],
    config_out_dir: Path,
) -> list[dict[str, Any]]:
    base_cfg = _load_json(base_config_path)
    rows: list[dict[str, Any]] = []
    for spec in variants_spec:
        name = str(spec.get("name", "")).strip()
        if not name:
            raise SystemExit("ERROR: variant missing non-empty 'name'")

        config_value = str(spec.get("config", "")).strip()
        if config_value:
            cfg_path = Path(config_value)
            if not cfg_path.is_absolute():
                cfg_path = (root / cfg_path).resolve()
            if not cfg_path.exists():
                raise SystemExit(f"ERROR: variant config not found for {name}: {cfg_path}")
            rows.append({"name": name, "config_path": cfg_path, "source": "config"})
            continue

        overrides = spec.get("overrides") or {}
        cfg = deepcopy(base_cfg)
        if overrides:
            _deep_merge(cfg, overrides)
        out_path = config_out_dir / f"{name}.json"
        out_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        rows.append({"name": name, "config_path": out_path, "source": "overrides"})
    return rows


def _run_variant(
    root: Path,
    runner_path: Path,
    run_tag: str,
    variant_name: str,
    config_path: Path,
    pair: str,
    aee_version: str,
    slice_file: str,
    include_contexts: str,
    include_trade_ids: str,
    max_trades: int,
) -> dict[str, Any]:
    slug = variant_name.lower().replace(" ", "_")
    result_dir = (root / "control" / "variant_runs" / run_tag / slug).resolve()
    result_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"AEE_MATRIX_{run_tag}_{slug}"[:120]
    cmd = [
        sys.executable,
        str(runner_path),
        "--config",
        str(config_path),
        "--pair",
        pair,
        "--aee-version",
        aee_version,
        "--run-id",
        run_id,
        "--result-dir",
        str(result_dir),
        "--out",
        str(result_dir / "main_report.json"),
        "--context-out",
        str(result_dir / "context_report.json"),
    ]
    if slice_file:
        cmd += ["--slice-file", slice_file]
    if include_contexts:
        cmd += ["--include-contexts", include_contexts]
    if include_trade_ids:
        cmd += ["--include-trade-ids", include_trade_ids]
    if max_trades > 0:
        cmd += ["--max-trades", str(max_trades)]

    proc = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)

    out: dict[str, Any] = {
        "name": variant_name,
        "run_id": run_id,
        "result_dir": str(result_dir),
        "exit_code": int(proc.returncode),
        "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
        "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-30:]),
    }
    if proc.returncode != 0:
        return out

    rs = _load_json(result_dir / "run_summary_active.json")
    mc = _load_json(result_dir / "aee_module_collision_audit_active.json")
    sc = _load_json(result_dir / "aee_simplicity_reality_check_active.json")

    results = rs.get("results", {}) if isinstance(rs, dict) else {}
    metrics = mc.get("metrics", {}) if isinstance(mc, dict) else {}

    out["metrics"] = {
        "realized_pph": float(results.get("realized_pph", 0.0) or 0.0),
        "gap": float(results.get("gap", 0.0) or 0.0),
        "extraction_efficiency": float(results.get("extraction_efficiency", 0.0) or 0.0),
        "objective_collision_detected": bool(mc.get("objective_collision_detected", False)),
        "bankable_green_loss_red_rate": float(metrics.get("bankable_green_loss_red_rate", 0.0) or 0.0),
        "fake_runner_count": int(metrics.get("fake_runner_count", 0) or 0),
        "overheld_winner_count": int(metrics.get("overheld_winner_count", 0) or 0),
        "complexity_adds_value": bool(sc.get("complexity_adds_value", False)),
    }
    return out


def _to_markdown(rows: list[dict[str, Any]], baseline_name: str) -> str:
    if not rows:
        return "# Variant Matrix\n\nNo rows.\n"

    baseline = next((r for r in rows if r.get("name") == baseline_name), rows[0])
    b = baseline.get("metrics", {})

    header = [
        "| Variant | Exit | realized_pph | gap | extraction_eff | collision | fake_runner | overheld | bankable_red | complexity |",
        "|---|---:|---:|---:|---:|---|---:|---:|---:|---|",
    ]
    body: list[str] = []
    for r in rows:
        m = r.get("metrics", {})
        if not m:
            body.append(
                f"| {r.get('name')} | {r.get('exit_code')} | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |"
            )
            continue
        body.append(
            "| {name} | {code} | {pph:.6f} | {gap:.6f} | {eff:.6f} | {coll} | {fr} | {oh} | {br:.6f} | {simp} |".format(
                name=r.get("name"),
                code=r.get("exit_code"),
                pph=float(m.get("realized_pph", 0.0)),
                gap=float(m.get("gap", 0.0)),
                eff=float(m.get("extraction_efficiency", 0.0)),
                coll="true" if bool(m.get("objective_collision_detected", False)) else "false",
                fr=int(m.get("fake_runner_count", 0)),
                oh=int(m.get("overheld_winner_count", 0)),
                br=float(m.get("bankable_green_loss_red_rate", 0.0)),
                simp="true" if bool(m.get("complexity_adds_value", False)) else "false",
            )
        )

    notes = [
        "",
        "## Baseline",
        f"- {baseline.get('name')}",
        f"- realized_pph={float(b.get('realized_pph', 0.0)):.6f}",
        f"- gap={float(b.get('gap', 0.0)):.6f}",
        f"- extraction_efficiency={float(b.get('extraction_efficiency', 0.0)):.6f}",
    ]
    return "\n".join(["# Variant Matrix", "", *header, *body, *notes]) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="Run AEE variants in parallel on the same baseline/slice")
    ap.add_argument("--base-config", default="entry_v23_policy_guarded_active.json")
    ap.add_argument("--variant-spec", required=True, help="Path to JSON with {variants:[...]}")
    ap.add_argument("--pair", default="EUR_USD")
    ap.add_argument("--aee-version", default="v3")
    ap.add_argument("--slice-file", default="")
    ap.add_argument("--include-contexts", default="")
    ap.add_argument("--include-trade-ids", default="")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--baseline-name", default="baseline")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    runner = (root / "run_aee_active_policy_evidencepack.py").resolve()

    base_config = Path(args.base_config)
    if not base_config.is_absolute():
        base_config = (root / base_config).resolve()
    if not base_config.exists():
        raise SystemExit(f"ERROR: base config not found: {base_config}")

    spec_path = Path(args.variant_spec)
    if not spec_path.is_absolute():
        spec_path = (root / spec_path).resolve()
    if not spec_path.exists():
        raise SystemExit(f"ERROR: variant spec not found: {spec_path}")

    spec = _load_json(spec_path)
    variants_spec = list(spec.get("variants") or [])
    if not variants_spec:
        raise SystemExit("ERROR: variant spec requires non-empty 'variants' list")

    run_tag = str(args.run_tag or _iso_tag())
    run_root = (root / "control" / "variant_runs" / run_tag).resolve()
    cfg_dir = run_root / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)

    baseline_spec = {"name": args.baseline_name, "config": str(base_config)}
    full_specs = [baseline_spec] + variants_spec
    variant_rows = _variant_configs(root, base_config, full_specs, cfg_dir)

    rows: list[dict[str, Any]] = []
    workers = max(1, int(args.workers))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(
                _run_variant,
                root,
                runner,
                run_tag,
                row["name"],
                Path(row["config_path"]),
                args.pair,
                args.aee_version,
                args.slice_file,
                args.include_contexts,
                args.include_trade_ids,
                int(args.max_trades),
            )
            for row in variant_rows
        ]
        for fut in as_completed(futures):
            rows.append(fut.result())

    rows.sort(key=lambda r: str(r.get("name", "")))

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_tag": run_tag,
        "pair": args.pair,
        "aee_version": args.aee_version,
        "slice": {
            "slice_file": args.slice_file,
            "include_contexts": args.include_contexts,
            "include_trade_ids": args.include_trade_ids,
            "max_trades": int(args.max_trades),
        },
        "rows": rows,
    }

    summary_path = run_root / "variant_matrix_summary.json"
    table_path = run_root / "variant_matrix_table.md"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    table_path.write_text(_to_markdown(rows, args.baseline_name), encoding="utf-8")

    print(f"Wrote: {summary_path}")
    print(f"Wrote: {table_path}")


if __name__ == "__main__":
    main()
