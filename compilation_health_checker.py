#!/usr/bin/env python3
"""
Compilation Health Checker and Auto-Repair Module
Validates compilation outputs and attempts automatic repairs
"""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthCheckResult:
    """Result of a health check"""
    def __init__(self, passed: bool, message: str, severity: str = "INFO", auto_repair_attempted: bool = False):
        self.passed = passed
        self.message = message
        self.severity = severity
        self.auto_repair_attempted = auto_repair_attempted


class CompilationHealthChecker:
    """Health checker for compilation outputs"""
    
    def __init__(self, node_dir: Path):
        self.node_dir = node_dir
        self.results: list[HealthCheckResult] = []
    
    def check_all(self) -> bool:
        """Run all health checks"""
        logger.info(f"Running health checks for {self.node_dir.name}")
        
        self.check_stage1_6()
        self.check_stream_seed()
        self.check_context_seed()
        self.check_trajectory_seed()
        self.check_target_contextual_v2()
        self.check_target_stage_report()
        self.check_dataset_lock()
        
        passed = all(r.passed for r in self.results)
        failed = [r for r in self.results if not r.passed]
        
        if failed:
            logger.warning(f"Health check FAILED: {len(failed)} issues found")
            for r in failed:
                logger.warning(f"  [{r.severity}] {r.message}")
        else:
            logger.info("Health check PASSED: All checks successful")
        
        return passed
    
    def check_stage1_6(self) -> None:
        """Check Stage 1-6 outputs"""
        stage_dir = self.node_dir / "target_entry_stage" / "stage1_6"
        
        if not stage_dir.exists():
            self.results.append(HealthCheckResult(
                False, "Stage 1-6 directory missing", "ERROR"
            ))
            return
        
        # Check compiler report
        report_path = stage_dir / "compiler_report.json"
        if not report_path.exists():
            self.results.append(HealthCheckResult(
                False, "Stage 1-6 compiler_report.json missing", "ERROR"
            ))
            return
        
        # Validate report content
        try:
            with open(report_path) as f:
                report = json.load(f)
            
            if report.get("status") != "PASS":
                self.results.append(HealthCheckResult(
                    False, f"Stage 1-6 status is {report.get('status')}, expected PASS", "ERROR"
                ))
                return
        except Exception as e:
            self.results.append(HealthCheckResult(
                False, f"Stage 1-6 report invalid: {e}", "ERROR"
            ))
            return
        
        self.results.append(HealthCheckResult(True, "Stage 1-6 OK"))
    
    def check_stream_seed(self) -> None:
        """Check stream_seed outputs with auto-repair"""
        stream_dir = self.node_dir / "target_entry_stage" / "stream_seed"
        
        if not stream_dir.exists():
            self.results.append(HealthCheckResult(
                False, "stream_seed directory missing", "ERROR"
            ))
            return
        
        # Check required files
        stream_csv = stream_dir / "session_energy_state_stream.csv"
        truth_csv = stream_dir / "state_action_truth_table.csv"
        
        for csv_path in [stream_csv, truth_csv]:
            if not csv_path.exists():
                self.results.append(HealthCheckResult(
                    False, f"{csv_path.name} missing", "ERROR"
                ))
                continue
            
            # Check file size
            if csv_path.stat().st_size == 0:
                self.results.append(HealthCheckResult(
                    False, f"{csv_path.name} is empty (0 bytes)", "ERROR"
                ))
                continue
            
            # Validate CSV structure
            try:
                df = pd.read_csv(csv_path, nrows=10)
                if len(df) == 0:
                    self.results.append(HealthCheckResult(
                        False, f"{csv_path.name} has no data rows", "ERROR"
                    ))
                    continue
                
                # Check required columns
                required_cols = {"timestamp", "session_id", "direction", "price"}
                missing_cols = required_cols - set(df.columns)
                if missing_cols:
                    self.results.append(HealthCheckResult(
                        False, f"{csv_path.name} missing columns: {missing_cols}", "ERROR"
                    ))
                    continue
                
            except Exception as e:
                self.results.append(HealthCheckResult(
                    False, f"{csv_path.name} invalid CSV: {e}", "ERROR"
                ))
                continue
        
        self.results.append(HealthCheckResult(True, "stream_seed OK"))
    
    def check_context_seed(self) -> None:
        """Check context_seed outputs"""
        context_dir = self.node_dir / "target_entry_stage" / "context_seed"
        
        if not context_dir.exists():
            self.results.append(HealthCheckResult(
                False, "context_seed directory missing", "ERROR"
            ))
            return
        
        # Check required files
        context_csv = context_dir / "session_energy_context_stream.csv"
        report_json = context_dir / "energy_context_report.json"
        
        if not context_csv.exists():
            self.results.append(HealthCheckResult(
                False, "session_energy_context_stream.csv missing", "ERROR"
            ))
            return
        
        if context_csv.stat().st_size == 0:
            self.results.append(HealthCheckResult(
                False, "session_energy_context_stream.csv is empty", "ERROR"
            ))
            return
        
        self.results.append(HealthCheckResult(True, "context_seed OK"))
    
    def check_trajectory_seed(self) -> None:
        """Check trajectory_seed outputs"""
        traj_dir = self.node_dir / "target_entry_stage" / "trajectory_seed"
        
        if not traj_dir.exists():
            self.results.append(HealthCheckResult(
                False, "trajectory_seed directory missing", "ERROR"
            ))
            return
        
        # Check required files
        traj_csv = traj_dir / "point_energy_trajectory.csv"
        
        if not traj_csv.exists():
            self.results.append(HealthCheckResult(
                False, "point_energy_trajectory.csv missing", "ERROR"
            ))
            return
        
        if traj_csv.stat().st_size == 0:
            self.results.append(HealthCheckResult(
                False, "point_energy_trajectory.csv is empty", "ERROR"
            ))
            return
        
        self.results.append(HealthCheckResult(True, "trajectory_seed OK"))
    
    def check_target_contextual_v2(self) -> None:
        """Check target_contextual_v2 outputs"""
        target_dir = self.node_dir / "target_entry_stage" / "target_contextual_v2"
        
        if not target_dir.exists():
            self.results.append(HealthCheckResult(
                False, "target_contextual_v2 directory missing", "ERROR"
            ))
            return
        
        # Check required files
        required_files = [
            "target_entry_classes.json",
            "target_entry_truth_table.csv",
            "target_entry_class_report.json"
        ]
        
        for filename in required_files:
            file_path = target_dir / filename
            if not file_path.exists():
                self.results.append(HealthCheckResult(
                    False, f"target_contextual_v2/{filename} missing", "ERROR"
                ))
                return
            
            if file_path.stat().st_size == 0:
                self.results.append(HealthCheckResult(
                    False, f"target_contextual_v2/{filename} is empty", "ERROR"
                ))
                return
        
        self.results.append(HealthCheckResult(True, "target_contextual_v2 OK"))
    
    def check_target_stage_report(self) -> None:
        """Check target_stage_report.json with validation"""
        report_path = self.node_dir / "target_entry_stage" / "target_stage_report.json"
        
        if not report_path.exists():
            self.results.append(HealthCheckResult(
                False, "target_stage_report.json missing - Stage 7 incomplete", "ERROR"
            ))
            return
        
        if report_path.stat().st_size == 0:
            self.results.append(HealthCheckResult(
                False, "target_stage_report.json is empty", "ERROR"
            ))
            return
        
        # Validate report content
        try:
            with open(report_path) as f:
                report = json.load(f)
            
            summary = report.get("summary", [])
            if not summary:
                self.results.append(HealthCheckResult(
                    False, "target_stage_report.json has no summary entries", "WARNING"
                ))
                return
            
            total_trades = sum(s.get("trade_count", 0) for s in summary)
            
            # Check for EUR_GBP Mon-Wed anomaly (FIX #3)
            node_name = self.node_dir.name
            if "EUR_GBP" in node_name and any(day in node_name.lower() for day in ["monday", "tuesday", "wednesday"]):
                if total_trades == 14:
                    self.results.append(HealthCheckResult(
                        False, f"EUR_GBP Mon-Wed anomaly detected: exactly 14 trades (likely corrupted)", "ERROR"
                    ))
                    return
            
            logger.info(f"target_stage_report.json: {total_trades} total trades")
            
        except Exception as e:
            self.results.append(HealthCheckResult(
                False, f"target_stage_report.json invalid: {e}", "ERROR"
            ))
            return
        
        self.results.append(HealthCheckResult(True, "target_stage_report OK"))
    
    def check_dataset_lock(self) -> None:
        """Check dataset lock validation (FIX #6, #12)"""
        # Find dataset lock file
        lock_files = list(self.node_dir.glob("dataset_lock*.json"))
        
        if not lock_files:
            self.results.append(HealthCheckResult(
                False, "No dataset lock file found", "WARNING"
            ))
            return
        
        lock_path = lock_files[0]
        
        try:
            with open(lock_path) as f:
                lock_data = json.load(f)
            
            # Check for pair field (FIX #12)
            if "pair" not in lock_data:
                # Try to infer from filename
                node_name = self.node_dir.name
                pair = node_name.split("__")[0] if "__" in node_name else None
                
                if pair:
                    # Auto-repair: add pair field
                    lock_data["pair"] = pair
                    with open(lock_path, "w") as f:
                        json.dump(lock_data, f, indent=2)
                    
                    self.results.append(HealthCheckResult(
                        True, f"Dataset lock missing 'pair' field - auto-repaired to {pair}", "WARNING", auto_repair_attempted=True
                    ))
                else:
                    self.results.append(HealthCheckResult(
                        False, "Dataset lock missing 'pair' field and cannot infer", "WARNING"
                    ))
            
            # Check dates exist
            if "dates" not in lock_data or not lock_data["dates"]:
                self.results.append(HealthCheckResult(
                    False, "Dataset lock has no dates", "ERROR"
                ))
                return
            
            logger.info(f"Dataset lock: {len(lock_data['dates'])} dates")
            
        except Exception as e:
            self.results.append(HealthCheckResult(
                False, f"Dataset lock invalid: {e}", "ERROR"
            ))
            return
        
        self.results.append(HealthCheckResult(True, "Dataset lock OK"))
    
    def attempt_auto_repair(self) -> bool:
        """Attempt automatic repairs for common issues"""
        logger.info("Attempting auto-repair...")
        
        repaired = False
        
        # Check for empty stream_seed and attempt rebuild
        stream_dir = self.node_dir / "target_entry_stage" / "stream_seed"
        if stream_dir.exists():
            stream_csv = stream_dir / "session_energy_state_stream.csv"
            if not stream_csv.exists() or stream_csv.stat().st_size == 0:
                logger.warning("stream_seed is empty or missing - needs rebuild")
                # Mark for rebuild but don't attempt here (requires full recompilation)
        
        return repaired
    
    def generate_report(self) -> dict[str, Any]:
        """Generate health check report"""
        passed = all(r.passed for r in self.results)
        failed = [r for r in self.results if not r.passed]
        warnings = [r for r in self.results if r.severity == "WARNING"]
        errors = [r for r in self.results if r.severity == "ERROR"]
        auto_repairs = [r for r in self.results if r.auto_repair_attempted]
        
        return {
            "node": self.node_dir.name,
            "overall_status": "PASS" if passed else "FAIL",
            "total_checks": len(self.results),
            "passed": len([r for r in self.results if r.passed]),
            "failed": len(failed),
            "warnings": len(warnings),
            "errors": len(errors),
            "auto_repairs_attempted": len(auto_repairs),
            "results": [
                {
                    "passed": r.passed,
                    "message": r.message,
                    "severity": r.severity,
                    "auto_repair": r.auto_repair_attempted
                }
                for r in self.results
            ]
        }


def main():
    """Main entry point for standalone health checking"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check compilation health")
    parser.add_argument("--node-dir", type=Path, required=True, help="Node directory to check")
    parser.add_argument("--auto-repair", action="store_true", help="Attempt automatic repairs")
    parser.add_argument("--output", type=Path, help="Output JSON report path")
    
    args = parser.parse_args()
    
    checker = CompilationHealthChecker(args.node_dir)
    passed = checker.check_all()
    
    if args.auto_repair and not passed:
        checker.attempt_auto_repair()
    
    report = checker.generate_report()
    
    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Report written to {args.output}")
    
    print(json.dumps(report, indent=2))
    
    return 0 if passed else 1


if __name__ == "__main__":
    exit(main())
