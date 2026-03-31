"""
stability_test.py
-----------------
Tests whether a discovery domain survives sample-size pressure.

Runs the full pipeline at sample sizes 100, 200, 300 and compares:
  - strongest setup expectancy
  - strongest trigger quality score
  - setup count
  - trigger count
  - viable business count
  - discovery/promotion stage pass/fail

Kill logic (configurable):
  - expectancy drops more than EXPECTANCY_DRIFT_THRESHOLD (default 20%) from s100 to s300
  - trigger quality drops more than TRIGGER_QUALITY_DRIFT_THRESHOLD (default 15%) from s100 to s300
  - setup count reaches 0 at s300
  - trigger count reaches 0 at s300

Results written to control/stability_tests/<batch_domain>_stability.json

Usage
-----
python tools/stability_test.py \\
    --pair EUR_USD \\
    --direction SHORT \\
    --session London \\
    --weekday Friday

Optional:
    --sample-sizes 100 200 300     (override default)
    --expectancy-drift-threshold 0.20
    --quality-drift-threshold 0.15
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent

SAMPLE_SIZES_DEFAULT = [100, 200, 300]
EXPECTANCY_DRIFT_THRESHOLD_DEFAULT = 0.20
QUALITY_DRIFT_THRESHOLD_DEFAULT = 0.15


# ─── helpers ─────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[stability_test] {msg}", flush=True)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_json_if_exists(path: Path) -> dict | None:
    if not path.exists():
        return None
    return load_json(path)


def pct_change(base: float | None, comparison: float | None) -> float | None:
    if base is None or comparison is None or base == 0:
        return None
    return (comparison - base) / abs(base)


def derive_batch_name(pair: str, session: str, direction: str, sample_size: int) -> str:
    return f"{pair.lower()}_{session.lower()}_{direction.lower()}_s{sample_size}"


# ─── batch runner ─────────────────────────────────────────────────────────────

def run_batch(pair: str, direction: str, session: str, weekday: str, sample_size: int) -> None:
    batch_name = derive_batch_name(pair, session, direction, sample_size)
    art_dir = WORKSPACE / "PC2" / "discovery" / "scale_batches" / batch_name
    val_dir = WORKSPACE / "control" / "scale_batch_validation" / batch_name
    core_artifacts = [
        "business_viability_report.json",
        "path_family_report.json",
        "structure_truth.json",
        "setup_truth.json",
        "trigger_truth.json",
        "ceiling_report.json",
    ]
    required_validation_reports = [
        "setup_phase_reports_discovery/validation_report.json",
        "setup_phase_reports_promotion/validation_report.json",
    ]
    artifacts_ready = all((art_dir / fn).exists() for fn in core_artifacts)
    validations_ready = all((val_dir / fn).exists() for fn in required_validation_reports)
    if artifacts_ready and validations_ready:
        log(f"  sample={sample_size}: artifacts + validations exist, skipping batch run")
        return

    if artifacts_ready:
        log(f"  sample={sample_size}: artifacts exist, re-running validation chain")

    log(f"  Launching run_scale_batch for sample={sample_size}")
    result = subprocess.run(
        [
            sys.executable,
            "tools/run_scale_batch.py",
            "--pair", pair,
            "--direction", direction,
            "--session", session,
            "--weekday", weekday,
            "--sample-size", str(sample_size),
            "--skip-if-exists",
        ],
        capture_output=False,
    )
    if result.returncode != 0:
        artifacts_ready = all((art_dir / fn).exists() for fn in core_artifacts)
        validations_ready = all((val_dir / fn).exists() for fn in required_validation_reports)
        if artifacts_ready and validations_ready:
            log(
                f"  WARN: run_scale_batch exited {result.returncode} but required artifacts/"
                f"validations exist; continuing stability evaluation"
            )
            return
        log(f"  FAIL: run_scale_batch for sample={sample_size} exited {result.returncode}")
        sys.exit(result.returncode)


# ─── summarize one batch ──────────────────────────────────────────────────────

def summarize_batch(pair: str, session: str, direction: str, sample_size: int) -> dict:
    batch_name = derive_batch_name(pair, session, direction, sample_size)
    art_dir = WORKSPACE / "PC2" / "discovery" / "scale_batches" / batch_name
    val_dir = WORKSPACE / "control" / "scale_batch_validation" / batch_name

    setup = load_json(art_dir / "setup_truth.json")
    trigger = load_json(art_dir / "trigger_truth.json")
    viability = load_json(art_dir / "business_viability_report.json")
    discovery_val = load_json(val_dir / "setup_phase_reports_discovery" / "validation_report.json")
    promotion_val = load_json(val_dir / "setup_phase_reports_promotion" / "validation_report.json")
    trigger_val = load_json_if_exists(
        val_dir / "trigger_validation_reports" / "trigger_validation_report.json"
    )

    setup_records = setup.get("records", [])
    trigger_records = trigger.get("records", [])
    viable_records = [r for r in viability.get("records", []) if r.get("viable")]

    best_expectancy = max(
        (r.get("expectancy", float("-inf")) for r in setup_records), default=None
    )
    best_trigger_quality = max(
        (
            r.get("trigger_quality", {}).get("trigger_quality_score", float("-inf"))
            for r in trigger_records
        ),
        default=None,
    )

    return {
        "sample_size": sample_size,
        "viable_business_count": len(viable_records),
        "setup_count": len(setup_records),
        "trigger_count": len(trigger_records),
        "best_setup_expectancy": best_expectancy,
        "best_trigger_quality_score": best_trigger_quality,
        "discovery_status": discovery_val.get("status"),
        "promotion_status": promotion_val.get("status"),
        "trigger_distinctness": (
            trigger_val.get("sibling_distinctness", {}).get("status")
            if trigger_val is not None else "UNKNOWN"
        ),
    }


# ─── stability classification ─────────────────────────────────────────────────

def classify_stability(
    summaries: list[dict],
    expectancy_threshold: float,
    quality_threshold: float,
) -> dict:
    by_size = {s["sample_size"]: s for s in summaries}
    sizes = sorted(by_size)
    if len(sizes) < 2:
        return {"stable": True, "reasons": [], "drifts": {}}

    base = by_size[sizes[0]]
    end = by_size[sizes[-1]]

    e_drift = pct_change(base["best_setup_expectancy"], end["best_setup_expectancy"])
    q_drift = pct_change(base["best_trigger_quality_score"], end["best_trigger_quality_score"])

    stable = True
    reasons = []

    if e_drift is not None and e_drift < -expectancy_threshold:
        stable = False
        reasons.append(
            f"expectancy dropped {abs(e_drift)*100:.1f}% "
            f"(threshold {expectancy_threshold*100:.0f}%)"
        )
    if q_drift is not None and q_drift < -quality_threshold:
        stable = False
        reasons.append(
            f"trigger quality dropped {abs(q_drift)*100:.1f}% "
            f"(threshold {quality_threshold*100:.0f}%)"
        )
    if base.get("setup_count", 0) == 0:
        stable = False
        reasons.append("no setups at smallest sample size")
    if base.get("trigger_count", 0) == 0:
        stable = False
        reasons.append("no triggers at smallest sample size")
    if end.get("setup_count", 0) == 0:
        stable = False
        reasons.append("no setups remain at largest sample size")
    if end.get("trigger_count", 0) == 0:
        stable = False
        reasons.append("no triggers remain at largest sample size")

    return {
        "stable": stable,
        "reasons": reasons,
        "drifts": {
            f"expectancy_{sizes[0]}_to_{sizes[-1]}": e_drift,
            f"trigger_quality_{sizes[0]}_to_{sizes[-1]}": q_drift,
        },
    }


# ─── main ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stability test a discovery domain across sample sizes.")
    p.add_argument("--pair", required=True)
    p.add_argument("--direction", required=True, choices=["LONG", "SHORT"])
    p.add_argument("--session", required=True)
    p.add_argument("--weekday", default="Friday")
    p.add_argument("--sample-sizes", type=int, nargs="+", default=SAMPLE_SIZES_DEFAULT)
    p.add_argument("--expectancy-drift-threshold", type=float, default=EXPECTANCY_DRIFT_THRESHOLD_DEFAULT)
    p.add_argument("--quality-drift-threshold", type=float, default=QUALITY_DRIFT_THRESHOLD_DEFAULT)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    domain_key = (
        f"{args.pair.lower()}_{args.session.lower()}_{args.direction.lower()}"
    )
    log(f"=== Stability test: {domain_key} at samples {args.sample_sizes} ===")

    summaries = []
    for size in args.sample_sizes:
        log(f"--- sample={size} ---")
        run_batch(args.pair, args.direction, args.session, args.weekday, size)
        summary = summarize_batch(args.pair, args.session, args.direction, size)
        summaries.append(summary)
        log(
            f"  expectancy={summary['best_setup_expectancy']} "
            f"trigger_quality={summary['best_trigger_quality_score']} "
            f"setups={summary['setup_count']} triggers={summary['trigger_count']}"
        )

    stability = classify_stability(
        summaries,
        args.expectancy_drift_threshold,
        args.quality_drift_threshold,
    )

    verdict = "STABLE" if stability["stable"] else "UNSTABLE"
    log(f"=== Stability verdict: {verdict} ===")
    if not stability["stable"]:
        for reason in stability["reasons"]:
            log(f"  kill reason: {reason}")

    out = {
        "domain": {
            "pair": args.pair,
            "session": args.session,
            "direction": args.direction,
            "weekday": args.weekday,
        },
        "sample_sizes_tested": args.sample_sizes,
        "thresholds": {
            "expectancy_drift": args.expectancy_drift_threshold,
            "trigger_quality_drift": args.quality_drift_threshold,
        },
        "summaries": summaries,
        "stability": stability,
    }

    out_dir = WORKSPACE / "control" / "stability_tests"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{domain_key}_stability.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    log(f"Report written to: {out_path}")


if __name__ == "__main__":
    main()
