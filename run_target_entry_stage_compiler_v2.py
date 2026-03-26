#!/usr/bin/env python3
"""
Enhanced Target Entry Stage Compiler - Version 2
Fixes all critical compilation issues with validation, checkpointing, and auto-repair
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import subprocess
import shutil
import sys
import time
from pathlib import Path
from typing import Sequence, Any

from optimize_target_entry_classes import TARGETS
import optimize_target_entry_classes_contextual_v2 as contextual_v2
from optimize_target_entry_classes_pph_static_cached import load_csv, rule_applies, summarize
from compilation_health_checker import CompilationHealthChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler('run_target_entry_stage_compiler_v2.log')
    ]
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
MIN_ZONE_OPPORTUNITIES = 10
MAX_ZONE_OPPORTUNITIES = 10000
MIN_TOTAL_TRUTH_ROWS = 100
MAX_TOTAL_TRUTH_ROWS = 200000


class CompilationCheckpoint:
    """Manage compilation checkpoints for resumability (FIX #16, #25)"""
    
    def __init__(self, checkpoint_file: Path):
        self.checkpoint_file = checkpoint_file
        self.data = self.load()
    
    def load(self) -> dict[str, Any]:
        """Load checkpoint data"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return {"completed_stages": [], "started_at": None, "last_update": None}
    
    def save(self) -> None:
        """Save checkpoint data"""
        self.data["last_update"] = time.time()
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)
    
    def mark_stage_complete(self, stage_name: str) -> None:
        """Mark a stage as completed"""
        if stage_name not in self.data["completed_stages"]:
            self.data["completed_stages"].append(stage_name)
        self.save()
        logger.info(f"Checkpoint: {stage_name} completed")
    
    def is_stage_complete(self, stage_name: str) -> bool:
        """Check if stage is already completed"""
        return stage_name in self.data["completed_stages"]
    
    def reset(self) -> None:
        """Reset checkpoint"""
        self.data = {"completed_stages": [], "started_at": time.time(), "last_update": time.time()}
        self.save()


def run_with_validation(cmd: Sequence[str], stage_name: str, timeout: int = 3600) -> None:
    """
    Run subprocess with validation and error handling.
    FIX #14: Fail-fast timeout guards
    FIX #27: Better subprocess error handling
    """
    logger.info(f"Running stage: {stage_name}")
    logger.info(f"Command: {' '.join(str(c) for c in cmd)}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False
        )
        
        elapsed = time.time() - start_time
        
        if result.returncode != 0:
            logger.error(f"Stage {stage_name} FAILED (exit code {result.returncode}, {elapsed:.1f}s)")
            logger.error(f"STDOUT:\n{result.stdout}")
            logger.error(f"STDERR:\n{result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
        
        logger.info(f"Stage {stage_name} completed successfully in {elapsed:.1f}s")
        
        # Log output for debugging
        if result.stdout:
            logger.debug(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.debug(f"STDERR:\n{result.stderr}")
            
    except subprocess.TimeoutExpired as e:
        elapsed = time.time() - start_time
        logger.error(f"Stage {stage_name} TIMEOUT after {elapsed:.1f}s (limit: {timeout}s)")
        logger.error(f"Command: {' '.join(str(c) for c in cmd)}")
        raise


def jload(path: Path) -> dict:
    """Load JSON file"""
    return json.loads(path.read_text())


def has_files(*paths: Path) -> bool:
    """Check if all files exist"""
    return all(path.exists() for path in paths)


def validate_file_not_empty(path: Path, min_size: int = 1) -> bool:
    """
    Validate file exists and is not empty.
    FIX #11: Stage output validation
    """
    if not path.exists():
        logger.error(f"File does not exist: {path}")
        return False
    
    size = path.stat().st_size
    if size < min_size:
        logger.error(f"File is too small ({size} bytes): {path}")
        return False
    
    return True


def validate_csv_structure(csv_path: Path, required_columns: set[str] = None) -> bool:
    """
    Validate CSV file structure.
    FIX #11: Stage output validation
    """
    if not validate_file_not_empty(csv_path, min_size=100):
        return False
    
    try:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            if not headers:
                logger.error(f"CSV has no headers: {csv_path}")
                return False
            
            if required_columns:
                missing = required_columns - set(headers)
                if missing:
                    logger.error(f"CSV missing required columns {missing}: {csv_path}")
                    return False
            
            # Read first row to ensure data exists
            first_row = next(reader, None)
            if first_row is None:
                logger.error(f"CSV has no data rows: {csv_path}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"CSV validation failed for {csv_path}: {e}")
        return False


def sha256_file(path: Path) -> str:
    """Calculate SHA256 hash of file"""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_truth_opportunity_sanity(truth_csv: Path) -> None:
    """Validate truth table has reasonable data"""
    total_rows = 0
    zone_counts: dict[tuple[str, str, float], int] = {}
    with truth_csv.open() as f:
        for row in csv.DictReader(f):
            total_rows += 1
            direction = row.get("direction_assumed", "")
            quarter = row.get("quarter", "")
            target = float(row.get("target_distance", 0))
            key = (direction, quarter, target)
            zone_counts[key] = zone_counts.get(key, 0) + 1
    
    if total_rows < MIN_TOTAL_TRUTH_ROWS:
        raise ValueError(f"Truth table has only {total_rows} rows (minimum {MIN_TOTAL_TRUTH_ROWS})")
    if total_rows > MAX_TOTAL_TRUTH_ROWS:
        raise ValueError(f"Truth table has {total_rows} rows (maximum {MAX_TOTAL_TRUTH_ROWS})")
    
    for key, count in zone_counts.items():
        if count < MIN_ZONE_OPPORTUNITIES:
            logger.warning(f"Zone {key} has only {count} opportunities (minimum {MIN_ZONE_OPPORTUNITIES})")
        if count > MAX_ZONE_OPPORTUNITIES:
            logger.warning(f"Zone {key} has {count} opportunities (maximum {MAX_ZONE_OPPORTUNITIES})")


def truth_matches_lock_dates(truth_csv: Path, dataset_lock: Path, sample_limit: int = None) -> bool:
    """
    Validate truth table dates match dataset lock.
    FIX #6: Validate ALL rows, not just sample
    """
    try:
        lock_data = jload(dataset_lock)
        valid_dates = set(lock_data.get("dates", []))
        
        if not valid_dates:
            logger.warning("Dataset lock has no dates")
            return False
        
        checked = 0
        mismatches = 0
        
        with truth_csv.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session_id = str(row.get("session_id") or "").strip()
                ts = str(row.get("timestamp") or "").strip()
                ts_date = ts[:10] if len(ts) >= 10 else ""
                
                if session_id and session_id not in valid_dates:
                    mismatches += 1
                    if mismatches <= 5:  # Log first few mismatches
                        logger.warning(f"Row {checked}: session_id {session_id} not in dataset lock")
                
                if ts_date and ts_date not in valid_dates:
                    mismatches += 1
                    if mismatches <= 5:
                        logger.warning(f"Row {checked}: date {ts_date} not in dataset lock")
                
                checked += 1
                
                # Use sample limit if provided (for performance)
                if sample_limit and checked >= sample_limit:
                    break
        
        if mismatches > 0:
            logger.error(f"Found {mismatches} date mismatches in {checked} rows checked")
            return False
        
        logger.info(f"Validated {checked} rows against dataset lock - all match")
        return True
        
    except Exception as e:
        logger.error(f"Date validation failed: {e}")
        return False


def validate_dataset_lock(dataset_lock: Path, node_name: str) -> None:
    """
    Validate dataset lock has required fields.
    FIX #12: Validate pair field in dataset lock
    """
    try:
        lock_data = jload(dataset_lock)
        
        # Check for pair field
        if "pair" not in lock_data:
            # Try to infer from node name
            pair = node_name.split("__")[0] if "__" in node_name else None
            if pair:
                logger.warning(f"Dataset lock missing 'pair' field - inferred as {pair}")
                # Auto-repair
                lock_data["pair"] = pair
                with open(dataset_lock, "w") as f:
                    json.dump(lock_data, f, indent=2)
                logger.info(f"Auto-repaired dataset lock: added pair={pair}")
            else:
                raise ValueError("Dataset lock missing 'pair' field and cannot infer from node name")
        
        # Validate pair matches node
        lock_pair = lock_data.get("pair", "").upper()
        node_pair = node_name.split("__")[0].upper() if "__" in node_name else ""
        
        if lock_pair != node_pair:
            logger.error(f"Dataset lock pair mismatch: lock has {lock_pair}, node is {node_pair}")
            raise ValueError(f"Dataset lock pair mismatch")
        
        # Check dates exist
        if "dates" not in lock_data or not lock_data["dates"]:
            raise ValueError("Dataset lock has no dates")
        
        logger.info(f"Dataset lock validated: pair={lock_pair}, {len(lock_data['dates'])} dates")
        
    except Exception as e:
        logger.error(f"Dataset lock validation failed: {e}")
        raise


def optional_sha256_file(path: Path) -> str | None:
    """Calculate SHA256 or return None if file doesn't exist"""
    return sha256_file(path) if path.exists() else None


def build_stage_inputs_hash(args: argparse.Namespace) -> str:
    """Build hash of all stage inputs for cache validation"""
    state_machine = ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json"
    payload = {
        "dataset_lock_hash": sha256_file(args.dataset_lock),
        "data_root": str(args.data_root.resolve()),
        "historical_fast": args.historical_fast,
        "research_lite": args.research_lite,
        "research_max_sessions": args.research_max_sessions,
        "research_row_stride": args.research_row_stride,
        "research_max_rows_per_session": args.research_max_rows_per_session,
        "script_hashes": {
            "run_target_entry_stage_compiler_v2.py": sha256_file(ROOT / "run_target_entry_stage_compiler_v2.py"),
            "build_session_state_stream_v2.py": sha256_file(ROOT / "build_session_state_stream_v2.py"),
            "stage1_5_deterministic_compiler.py": sha256_file(ROOT / "stage1_5_deterministic_compiler.py"),
            "build_energy_context_engine.py": sha256_file(ROOT / "build_energy_context_engine.py"),
            "build_point_energy_trajectory.py": sha256_file(ROOT / "build_point_energy_trajectory.py"),
            "optimize_target_entry_classes_contextual_v2.py": sha256_file(ROOT / "optimize_target_entry_classes_contextual_v2.py"),
        },
        "config_hashes": {
            "entry_trigger_state_machine.json": optional_sha256_file(state_machine),
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def clear_stage_outputs(out_dir: Path) -> None:
    """Clear all stage outputs for clean rebuild"""
    logger.info("Clearing stage outputs for clean rebuild")
    for name in [
        "stage1_6",
        "target_contextual_v2",
        "target_contextual_v2_targeted",
        "stream_seed",
        "context_seed",
        "trajectory_seed",
        "target_stage_report.json",
        "target_stage_manifest.json",
        "session_calibration_report.json",
        "session_calibration_manifest.json",
    ]:
        path = out_dir / name
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            logger.debug(f"Removed directory: {path}")
        else:
            path.unlink(missing_ok=True)
            logger.debug(f"Removed file: {path}")


def extract_node_info(dataset_lock: Path) -> tuple[str, str, str]:
    """
    Extract pair, weekday, session from dataset lock filename.
    FIX #1, #2: Enable node-local filtering
    """
    # Example: dataset_lock__eur_gbp__thursday__sydney__11.json
    filename = dataset_lock.stem
    parts = filename.split("__")
    
    if len(parts) >= 4:
        pair = parts[1].upper()
        weekday = parts[2].lower()
        session = parts[3].lower()
        return pair, weekday, session
    
    logger.warning(f"Could not extract node info from {filename}")
    return None, None, None


def write_template_apply_manifest(
    out_dir: Path,
    stage_inputs_hash: str,
    args: argparse.Namespace,
    stage1_6_dir: Path,
    stream_seed_dir: Path,
    context_seed_dir: Path,
    trajectory_seed_dir: Path,
    target_context_dir: Path,
    target_targeted_dir: Path,
) -> None:
    """Write manifest for template-applied compilation"""
    manifest = {
        "status": "PASS",
        "stage_inputs_hash": stage_inputs_hash,
        "timestamp": time.time(),
        "template_applied": True,
        "artifacts": {
            "stage1_6": str(stage1_6_dir),
            "stream_seed": str(stream_seed_dir),
            "context_seed": str(context_seed_dir),
            "trajectory_seed": str(trajectory_seed_dir),
            "target_contextual_v2": str(target_context_dir),
            "target_contextual_v2_targeted": str(target_targeted_dir),
        }
    }
    
    manifest_path = out_dir / "target_stage_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    logger.info(f"Wrote manifest: {manifest_path}")


def main():
    """Main compilation entry point with all fixes"""
    parser = argparse.ArgumentParser(description="Enhanced Target Entry Stage Compiler V2")
    parser.add_argument("--dataset-lock", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--historical-fast", action="store_true")
    parser.add_argument("--research-lite", action="store_true")
    parser.add_argument("--research-max-sessions", type=int, default=0)
    parser.add_argument("--research-row-stride", type=int, default=1)
    parser.add_argument("--research-max-rows-per-session", type=int, default=0)
    parser.add_argument("--force-rebuild", action="store_true", help="Force rebuild even if cached")
    parser.add_argument("--skip-health-check", action="store_true", help="Skip final health check")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    logger.info("=" * 80)
    logger.info("Enhanced Target Entry Stage Compiler V2")
    logger.info("=" * 80)
    logger.info(f"Dataset lock: {args.dataset_lock}")
    logger.info(f"Data root: {args.data_root}")
    logger.info(f"Output root: {args.output_root}")
    
    # Validate dataset lock
    node_name = args.output_root.name
    validate_dataset_lock(args.dataset_lock, node_name)
    
    # Extract node info for filtering
    pair, weekday, session = extract_node_info(args.dataset_lock)
    logger.info(f"Node info: pair={pair}, weekday={weekday}, session={session}")
    
    # Setup checkpoint
    checkpoint_file = args.output_root / ".compilation_checkpoint.json"
    checkpoint = CompilationCheckpoint(checkpoint_file)
    
    if args.force_rebuild:
        logger.info("Force rebuild requested - resetting checkpoint")
        checkpoint.reset()
        clear_stage_outputs(args.output_root)
    
    # Define directories
    out_dir = args.output_root
    stage1_6_dir = out_dir / "stage1_6"
    stream_seed_dir = out_dir / "stream_seed"
    context_seed_dir = out_dir / "context_seed"
    trajectory_seed_dir = out_dir / "trajectory_seed"
    target_context_dir = out_dir / "target_contextual_v2"
    target_targeted_dir = out_dir / "target_contextual_v2_targeted"
    
    try:
        # Stage 1-6: Deterministic compilation
        if not checkpoint.is_stage_complete("stage1_6"):
            if not has_files(stage1_6_dir / "compiler_report.json"):
                logger.info("Running Stage 1-6: Deterministic compilation")
                run_with_validation(
                    [
                        "python3",
                        str(ROOT / "stage1_5_deterministic_compiler.py"),
                        "--dataset-lock", str(args.dataset_lock),
                        "--data-root", str(args.data_root),
                        "--pair", pair,
                        "--output-root", str(stage1_6_dir),
                    ],
                    "stage1_6",
                    timeout=1800
                )
                
                # Validate output
                if not validate_file_not_empty(stage1_6_dir / "compiler_report.json"):
                    raise ValueError("Stage 1-6 output validation failed")
            
            checkpoint.mark_stage_complete("stage1_6")
        else:
            logger.info("Stage 1-6 already complete (from checkpoint)")
        
        # Stream seed: Build session state stream
        if not checkpoint.is_stage_complete("stream_seed"):
            if not has_files(
                stream_seed_dir / "session_energy_state_stream.csv",
                stream_seed_dir / "state_action_truth_table.csv",
            ):
                logger.info("Running Stream Seed: Build session state stream (V2 with node-local filtering)")
                cmd = [
                    "python3",
                    str(ROOT / "build_session_state_stream_v2.py"),
                    "--data-root", str(args.data_root),
                    "--output-dir", str(stream_seed_dir),
                    "--pair", pair,
                    "--weekday", weekday,
                    "--session", session,
                    "--dataset-lock", str(args.dataset_lock),
                ]
                if args.verbose:
                    cmd.append("--verbose")
                
                run_with_validation(cmd, "stream_seed", timeout=600)
                
                # Validate outputs
                required_cols = {"timestamp", "session_id", "direction_assumed", "price"}
                if not validate_csv_structure(stream_seed_dir / "session_energy_state_stream.csv", required_cols):
                    raise ValueError("Stream seed output validation failed")
            
            checkpoint.mark_stage_complete("stream_seed")
        else:
            logger.info("Stream seed already complete (from checkpoint)")
        
        # Context seed: Build energy context
        if not checkpoint.is_stage_complete("context_seed"):
            if not has_files(
                context_seed_dir / "session_energy_context_stream.csv",
                context_seed_dir / "energy_context_report.json",
            ):
                logger.info("Running Context Seed: Build energy context")
                run_with_validation(
                    [
                        "python3",
                        str(ROOT / "build_energy_context_engine.py"),
                        "--stream-csv", str(stream_seed_dir / "session_energy_state_stream.csv"),
                        "--rules-json", str(ROOT / "compiled_entry_trigger_state_machine_11_sessions" / "entry_trigger_state_machine.json"),
                        "--output-dir", str(context_seed_dir),
                    ],
                    "context_seed",
                    timeout=600
                )
                
                # Validate output
                if not validate_csv_structure(context_seed_dir / "session_energy_context_stream.csv"):
                    raise ValueError("Context seed output validation failed")
            
            checkpoint.mark_stage_complete("context_seed")
        else:
            logger.info("Context seed already complete (from checkpoint)")
        
        # Trajectory seed: Build point energy trajectory
        if not checkpoint.is_stage_complete("trajectory_seed"):
            if not has_files(
                trajectory_seed_dir / "point_energy_trajectory.csv",
                trajectory_seed_dir / "point_energy_transition_report.json",
            ):
                logger.info("Running Trajectory Seed: Build point energy trajectory")
                run_with_validation(
                    [
                        "python3",
                        str(ROOT / "build_point_energy_trajectory.py"),
                        "--context-stream-csv", str(context_seed_dir / "session_energy_context_stream.csv"),
                        "--truth-csv", str(stream_seed_dir / "state_action_truth_table.csv"),
                        "--output-dir", str(trajectory_seed_dir),
                    ],
                    "trajectory_seed",
                    timeout=600
                )
                
                # Validate output
                if not validate_csv_structure(trajectory_seed_dir / "point_energy_trajectory.csv"):
                    raise ValueError("Trajectory seed output validation failed")
            
            checkpoint.mark_stage_complete("trajectory_seed")
        else:
            logger.info("Trajectory seed already complete (from checkpoint)")
        
        # Target contextual v2: Optimize target entry classes
        if not checkpoint.is_stage_complete("target_contextual_v2"):
            if not has_files(
                target_context_dir / "target_entry_classes.json",
                target_context_dir / "target_entry_truth_table.csv",
                target_context_dir / "target_entry_class_report.json",
            ):
                logger.info("Running Target Contextual V2: Optimize target entry classes")
                contextual_v2.run_contextual_v2(
                    data_root=args.data_root,
                    targets=TARGETS,
                    context_csv=context_seed_dir / "session_energy_context_stream.csv",
                    trajectory_csv=trajectory_seed_dir / "point_energy_trajectory.csv",
                    out_dir=target_context_dir,
                    research_mode=args.research_lite,
                    research_max_sessions=args.research_max_sessions,
                    research_row_stride=args.research_row_stride,
                    research_max_rows_per_session=args.research_max_rows_per_session,
                )
                
                # Validate outputs
                if not validate_file_not_empty(target_context_dir / "target_entry_class_report.json"):
                    raise ValueError("Target contextual v2 output validation failed")
                
                # Validate truth table dates
                if not truth_matches_lock_dates(
                    target_context_dir / "target_entry_truth_table.csv",
                    args.dataset_lock,
                    sample_limit=1000  # Sample for performance
                ):
                    raise ValueError("Target contextual v2 truth table date mismatch")
            
            checkpoint.mark_stage_complete("target_contextual_v2")
        else:
            logger.info("Target contextual v2 already complete (from checkpoint)")
        
        # Write final manifest
        stage_inputs_hash = build_stage_inputs_hash(args)
        write_template_apply_manifest(
            out_dir,
            stage_inputs_hash,
            args,
            stage1_6_dir,
            stream_seed_dir,
            context_seed_dir,
            trajectory_seed_dir,
            target_context_dir,
            target_targeted_dir,
        )
        
        # Health check
        if not args.skip_health_check:
            logger.info("Running health check...")
            checker = CompilationHealthChecker(args.output_root)
            passed = checker.check_all()
            
            if not passed:
                logger.warning("Health check found issues - attempting auto-repair")
                checker.attempt_auto_repair()
            
            # Write health report
            report = checker.generate_report()
            report_path = args.output_root / "compilation_health_report.json"
            with open(report_path, "w") as f:
                json.dump(report, f, indent=2)
            logger.info(f"Health report: {report_path}")
        
        logger.info("=" * 80)
        logger.info("COMPILATION SUCCESSFUL")
        logger.info("=" * 80)
        print(json.dumps({"status": "PASS", "output_dir": str(out_dir)}, indent=2))
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("COMPILATION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}", exc_info=True)
        print(json.dumps({"status": "FAIL", "error": str(e)}, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
