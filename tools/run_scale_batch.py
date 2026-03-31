"""
run_scale_batch.py
------------------
Proper reusable batch runner for PC2 discovery scaling.

Replaces all inline python -c "..." one-liners.
Each step is a discrete, logged call. Fail fast on first error.

Usage
-----
python tools/run_scale_batch.py \\
    --pair EUR_USD \\
    --direction SHORT \\
    --session London \\
    --weekday Friday \\
    --sample-size 200

Optional:
    --batch-name <override>   default: <pair_lower>_<session_lower>_<dir_lower>_s<size>
    --skip-if-exists          skip Stage A if all 6 core artifacts already exist
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

import pandas as pd

WORKSPACE = Path(__file__).resolve().parent.parent

CORE_ARTIFACTS = [
    "business_viability_report.json",
    "path_family_report.json",
    "structure_truth.json",
    "setup_truth.json",
    "trigger_truth.json",
    "ceiling_report.json",
]

VALIDATION_OUTPUTS = [
    "codespaces_enforcement_validation.json",
    "setup_phase_reports_discovery/validation_report.json",
    "setup_phase_reports_promotion/validation_report.json",
    "trigger_validation_reports/trigger_validation_report.json",
]


# ─── helpers ─────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[run_scale_batch] {msg}", flush=True)


def run_step(label: str, cmd: list[str]) -> None:
    log(f"  running: {label}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        log(f"  FAIL: {label} exited {result.returncode}")
        sys.exit(result.returncode)
    log(f"  OK: {label}")


def all_artifacts_exist(art_dir: Path) -> bool:
    return all((art_dir / fn).exists() for fn in CORE_ARTIFACTS)


# ─── stage A ─────────────────────────────────────────────────────────────────

def run_stage_a(pair: str, direction: str, session: str, weekday: str,
                sample_size: int, art_dir: Path) -> None:
    """Load pc2_stage_a_runner, patch all config globals, call run()."""
    log(f"  Stage A: {pair} / {session} / {direction} / {weekday} / sample={sample_size}")
    spec = importlib.util.spec_from_file_location(
        "pc2_stage_a_runner",
        WORKSPACE / "tools" / "pc2_stage_a_runner.py",
    )
    r = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(r)

    # Patch all config globals before calling run()
    r.PAIRS = [pair]
    r.DIRECTIONS = [direction]
    r.SESSION = session
    r.WEEKDAY = weekday
    r.SAMPLE_SIZE = sample_size
    r.COMPILED_NODES = WORKSPACE / "PC2" / "mapping_minimal" / "compiled_friday_refactor"
    r.OUTPUT_DIR = art_dir
    r.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Override sample_hits so it honours the patched SAMPLE_SIZE at call time
    r.sample_hits = lambda hits, n=None: hits.head(r.SAMPLE_SIZE)

    # Extend SPREADS to cover all expansion-grid pairs (conservative pips).
    # Pairs not listed here will use the default 1.5 pip fallback.
    _FULL_SPREADS: dict[str, float] = {
        "EUR_USD": 0.8,
        "GBP_USD": 1.2,
        "AUD_USD": 1.5,
        "USD_JPY": 0.8,
        "NZD_USD": 1.8,
        "USD_CAD": 1.5,
        "USD_CHF": 1.5,
        "EUR_GBP": 1.2,
        "EUR_JPY": 1.2,
        "EUR_CHF": 1.5,
        "GBP_JPY": 2.0,
        "GBP_CHF": 2.0,
        "AUD_JPY": 2.0,
        "AUD_CAD": 2.0,
        "CHF_JPY": 2.0,
        "NZD_JPY": 2.0,
    }
    r.SPREADS = {**r.SPREADS, **_FULL_SPREADS}
    # Ensure the specific pair has an entry; fall back to 1.5 if still missing
    if pair not in r.SPREADS:
        r.SPREADS[pair] = 1.5
        log(f"  WARNING: no known spread for {pair}, using 1.5 pips fallback")

    # Some cohorts (e.g., JPY) carry nanosecond ISO timestamps that can fail
    # strict parse_dates in the original loader. Use robust ISO parsing here.
    def _robust_load_phase1(pair_name: str, weekday_name: str, session_name: str):
        node_dir = r.COMPILED_NODES / f"{pair_name}__{weekday_name}__{session_name}"
        csv_path = node_dir / "phase1" / "opportunity_map_raw.csv"
        df = pd.read_csv(csv_path)
        if "timestamp" not in df.columns:
            raise KeyError(f"Missing 'timestamp' column in {csv_path}")
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
        return df.sort_values("timestamp").reset_index(drop=True)

    r.load_phase1 = _robust_load_phase1

    r.run()
    log(f"  Stage A complete. Artifacts in: {art_dir}")


# ─── main ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one PC2 discovery scale batch end-to-end.")
    p.add_argument("--pair", required=True, help="e.g. EUR_USD")
    p.add_argument("--direction", required=True, choices=["LONG", "SHORT"])
    p.add_argument("--session", required=True, help="e.g. London, NewYork")
    p.add_argument("--weekday", default="Friday", help="e.g. Friday")
    p.add_argument("--sample-size", type=int, default=200)
    p.add_argument(
        "--batch-name",
        default=None,
        help="Override output folder name (default: auto-derived)",
    )
    p.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip Stage A if all 6 core artifacts already exist",
    )
    return p.parse_args()


def derive_batch_name(pair: str, session: str, direction: str, sample_size: int) -> str:
    return f"{pair.lower()}_{session.lower()}_{direction.lower()}_s{sample_size}"


def main() -> None:
    args = parse_args()
    batch_name = args.batch_name or derive_batch_name(
        args.pair, args.session, args.direction, args.sample_size
    )

    art_dir = WORKSPACE / "PC2" / "discovery" / "scale_batches" / batch_name
    val_dir = WORKSPACE / "control" / "scale_batch_validation" / batch_name

    log(f"=== Batch: {batch_name} ===")
    log(f"  Artifacts  → {art_dir}")
    log(f"  Validation → {val_dir}")

    # ── Stage A ───────────────────────────────────────────────────────────────
    if args.skip_if_exists and all_artifacts_exist(art_dir):
        log("  Stage A: skipped (all 6 core artifacts already exist)")
    else:
        run_stage_a(args.pair, args.direction, args.session, args.weekday,
                    args.sample_size, art_dir)

    py = sys.executable

    # ── Phase 3: setup discovery ──────────────────────────────────────────────
    run_step("phase3 setup discovery", [
        py, "tools/pc2_phase3_setup_discovery.py",
        "--input-dir", str(art_dir),
        "--out", str(art_dir / "setup_truth.json"),
    ])

    # ── Phase 4: trigger discovery ────────────────────────────────────────────
    run_step("phase4 trigger discovery", [
        py, "tools/pc2_phase4_trigger_discovery.py",
        "--input", str(art_dir / "setup_truth.json"),
        "--out", str(art_dir / "trigger_truth.json"),
    ])

    # ── Phase 6: ceiling discovery ────────────────────────────────────────────
    run_step("phase6 ceiling discovery", [
        py, "tools/pc2_phase6_ceiling_discovery.py",
        "--setup", str(art_dir / "setup_truth.json"),
        "--trigger", str(art_dir / "trigger_truth.json"),
        "--out", str(art_dir / "ceiling_report.json"),
    ])

    val_dir.mkdir(parents=True, exist_ok=True)

    # ── Enforcement validation ────────────────────────────────────────────────
    run_step("codespaces enforcement validation", [
        py, "-m", "codespaces_rcp.validator_runner",
        "--report-dir", str(art_dir),
        "--schema-dir", "codespaces_rcp/schemas",
        "--out", str(val_dir / "codespaces_enforcement_validation.json"),
    ])

    # ── Setup-phase validation: discovery stage ───────────────────────────────
    run_step("setup phase validation (discovery)", [
        py, "enforcement/setup_phase_validation.py",
        "--artifact-dir", str(art_dir),
        "--output-dir", str(val_dir / "setup_phase_reports_discovery"),
        "--stage", "discovery",
        "--min-sample-size", "30",
        "--discovery-sample-floor", "15",
    ])

    # ── Setup-phase validation: promotion stage ───────────────────────────────
    run_step("setup phase validation (promotion)", [
        py, "enforcement/setup_phase_validation.py",
        "--artifact-dir", str(art_dir),
        "--output-dir", str(val_dir / "setup_phase_reports_promotion"),
        "--stage", "promotion",
        "--min-sample-size", "30",
        "--discovery-sample-floor", "15",
    ])

    # ── Trigger validation ────────────────────────────────────────────────────
    run_step("trigger validation", [
        py, "enforcement/trigger_validation_runner.py",
        "--trigger-dir", str(art_dir),
        "--schema-dir", "enforcement/schemas",
        "--output-dir", str(val_dir / "trigger_validation_reports"),
    ])

    log(f"=== {batch_name} DONE ===")


if __name__ == "__main__":
    main()
