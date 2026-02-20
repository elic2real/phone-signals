#!/usr/bin/env python3
"""
Log-based Artifact Collector
Separates artifact collection from production code
"""
import os
import json
import time
import threading
import queue
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

class ArtifactCollector:
    """Non-blocking artifact collector that writes from log data"""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.artifact_dir = base_dir / "proof_artifacts"
        self.log_dir = base_dir / "logs"
        self.queue = queue.Queue()
        self.running = False
        self.thread = None
        self.current_run_dir = None
        
    def start(self):
        """Start the artifact collector thread"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()
            
    def stop(self):
        """Stop the artifact collector thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
            
    def _worker(self):
        """Background thread for writing artifacts"""
        while self.running:
            try:
                # Get item from queue with timeout
                item = self.queue.get(timeout=0.1)
                self._write_artifact(item)
                self.queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                # Don't let artifact errors stop production
                print(f"ArtifactCollector error: {e}")
                
    def ensure_run_dir(self) -> Path:
        """Ensure we have a run directory for this session"""
        if not self.current_run_dir:
            ts = datetime.utcnow().isoformat().replace(":", "-")
            self.current_run_dir = self.artifact_dir / ts
            self.current_run_dir.mkdir(parents=True, exist_ok=True)
        return self.current_run_dir
        
    def _write_artifact(self, item: Dict[str, Any]):
        """Write an artifact item to disk"""
        try:
            run_dir = self.ensure_run_dir()
            artifact_type = item.get("type")
            data = item.get("data")
            
            if artifact_type == "candle_samples":
                pair = data.get("pair")
                gran = data.get("gran")
                samples = data.get("samples", [])
                
                # Write samples
                samples_file = run_dir / f"time_parse_samples_{pair}_{gran}.jsonl"
                with open(samples_file, "a") as f:
                    for sample in samples:
                        f.write(json.dumps(sample) + "\n")
                        
            elif artifact_type == "candle_report":
                pair = data.get("pair")
                gran = data.get("gran")
                report = data.get("report")
                
                report_file = run_dir / f"time_parse_report_{pair}_{gran}.json"
                with open(report_file, "w") as f:
                    json.dump(report, f, indent=2)
                    
            elif artifact_type == "spread_report":
                report = data.get("report")
                report_file = run_dir / "spread_report.json"
                with open(report_file, "w") as f:
                    json.dump(report, f, indent=2)
                    
            elif artifact_type == "state_transition":
                pair = data.get("pair")
                transition = data.get("transition")
                
                transitions_file = run_dir / "state_transitions.jsonl"
                with open(transitions_file, "a") as f:
                    f.write(json.dumps(transition) + "\n")
                    
            # Add more artifact types as needed
            
        except Exception as e:
            # Silently fail - don't impact production
            print(f"Artifact write failed: {e}")
            
    # Public API - non-blocking queue methods
    
    def collect_candle_samples(self, pair: str, gran: str, samples: List[Dict]):
        """Queue candle samples for collection"""
        self.queue.put({
            "type": "candle_samples",
            "data": {
                "pair": pair,
                "gran": gran,
                "samples": samples
            }
        })
        
    def collect_candle_report(self, pair: str, gran: str, report: Dict):
        """Queue candle report for collection"""
        self.queue.put({
            "type": "candle_report",
            "data": {
                "pair": pair,
                "gran": gran,
                "report": report
            }
        })
        
    def collect_spread_report(self, report: Dict):
        """Queue spread report for collection"""
        self.queue.put({
            "type": "spread_report",
            "data": {
                "report": report
            }
        })
        
    def collect_state_transition(self, pair: str, transition: Dict):
        """Queue state transition for collection"""
        self.queue.put({
            "type": "state_transition",
            "data": {
                "pair": pair,
                "transition": transition
            }
        })
        
    def collect_atr_report(self, pair: str, period: int, report: Dict):
        """Queue ATR report for collection"""
        self.queue.put({
            "type": "atr_report",
            "data": {
                "pair": pair,
                "period": period,
                "report": report
            }
        })
        
    def collect_order_event(self, event: Dict):
        """Queue order event for collection"""
        self.queue.put({
            "type": "order_event",
            "data": {
                "event": event
            }
        })
        
    def collect_trade_event(self, event: Dict):
        """Queue trade event for collection"""
        self.queue.put({
            "type": "trade_event",
            "data": {
                "event": event
            }
        })
        
    def generate_manifest(self):
        """Generate SHA256 manifest of all artifacts"""
        try:
            run_dir = self.ensure_run_dir()
            import hashlib
            
            manifest = {}
            
            for file_path in run_dir.rglob("*"):
                if file_path.is_file():
                    sha = hashlib.sha256()
                    with open(file_path, "rb") as f:
                        for chunk in iter(lambda: f.read(8192), b""):
                            sha.update(chunk)
                    
                    rel_path = file_path.relative_to(run_dir)
                    manifest[str(rel_path)] = sha.hexdigest()
                    
            # Write manifest
            manifest_file = run_dir / "manifest.sha256"
            with open(manifest_file, "w") as f:
                json.dump(manifest, f, indent=2)
                
        except Exception as e:
            print(f"Manifest generation failed: {e}")


# Global instance
_collector: Optional[ArtifactCollector] = None

def get_collector() -> Optional[ArtifactCollector]:
    """Get the global artifact collector instance"""
    global _collector
    return _collector

def init_collector(base_dir: Path) -> ArtifactCollector:
    """Initialize the global artifact collector"""
    global _collector
    _collector = ArtifactCollector(base_dir)
    _collector.start()
    return _collector

def shutdown_collector():
    """Shutdown the artifact collector"""
    global _collector
    if _collector:
        _collector.generate_manifest()
        _collector.stop()
        _collector = None
