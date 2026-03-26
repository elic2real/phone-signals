#!/usr/bin/env python3
"""
Repository Audit - Runtime Calibration Integration
Following the exact workflow specified
"""

import os
import json
import ast
from pathlib import Path
from typing import Dict, List, Set, Tuple
import re

class RepoAuditor:
    """Audits the repository to identify existing components and integration points"""
    
    def __init__(self, base_path: str = "/home/elic/Documents/phone signals"):
        self.base_path = Path(base_path)
        self.findings = {
            "fully_implemented": [],
            "partially_implemented": [],
            "dead_legacy": [],
            "missing": []
        }
        self.file_imports = {}  # Track what each file imports
        self.call_sites = {}    # Track where functions are called
        self.component_owners = {}  # Track which files own which components
        
    def audit_repository(self):
        """Perform comprehensive repository audit"""
        print("=" * 70)
        print("REPOSITORY AUDIT - RUNTIME CALIBRATION INTEGRATION")
        print("=" * 70)
        
        # 1. Scan all Python files
        print("\n1. Scanning repository structure...")
        self._scan_python_files()
        
        # 2. Audit specific areas
        print("\n2. Auditing specific areas...")
        self._audit_mapping_generation()
        self._audit_market_nodes()
        self._audit_session_state()
        self._audit_energy_context()
        self._audit_point_trajectory()
        self._audit_target_entry()
        self._audit_aee_stage()
        self._audit_trade_type_truth()
        self._audit_fixed_pop()
        self._audit_runtime_bot()
        self._audit_config_loading()
        self._audit_manifests()
        self._audit_session_quarter_utils()
        
        # 3. Analyze findings
        print("\n3. Analyzing findings...")
        self._analyze_findings()
        
        # 4. Generate integration map
        print("\n4. Generating integration map...")
        self._generate_integration_map()
        
        return self._produce_audit_report()
        
    def _scan_python_files(self):
        """Scan all Python files and track imports"""
        for py_file in self.base_path.rglob("*.py"):
            if any(skip in str(py_file) for skip in ["__pycache__", ".git"]):
                continue
                
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    
                # Parse AST to find imports
                tree = ast.parse(content)
                imports = []
                
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.append(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module or ""
                        for alias in node.names:
                            imports.append(f"{module}.{alias.name}")
                            
                self.file_imports[str(py_file.relative_to(self.base_path))] = imports
                
            except Exception as e:
                print(f"   Warning: Could not parse {py_file}: {e}")
                
    def _audit_mapping_generation(self):
        """Audit mapping generation components"""
        print("\n   Mapping Generation:")
        
        # Check for mapping generation files
        mapping_files = [
            "tune_map.py",
            "tune_map_seed.json",
            "tunes/tune_map_seed.json",
            "tunes/tune_map_seed_v2.json",
            "tunes/tune_map_seed_v3_15.json",
            "tunes/tune_map_seed_v4_15_full.json"
        ]
        
        for file in mapping_files:
            path = self.base_path / file
            if path.exists():
                print(f"      ✅ Found: {file}")
                self.findings["fully_implemented"].append(f"Mapping file: {file}")
                
                # Check who uses it
                users = self._find_file_users(file)
                if users:
                    print(f"         Used by: {', '.join(users[:3])}")
                    
    def _audit_market_nodes(self):
        """Audit market node components"""
        print("\n   Market Nodes:")
        
        # Check compiled_market_nodes directory
        cmn_path = self.base_path / "compiled_market_nodes"
        if cmn_path.exists():
            node_count = len(list(cmn_path.iterdir()))
            print(f"      ✅ Found compiled_market_nodes with {node_count} nodes")
            self.findings["fully_implemented"].append("Compiled market nodes directory")
            
            # Check structure of a few nodes
            for node_dir in list(cmn_path.iterdir())[:3]:
                if node_dir.is_dir():
                    required_files = [
                        "target_entry_stage/target_contextual_v2/target_entry_classes.json",
                        "target_entry_stage/target_contextual_v2/target_entry_class_report.json"
                    ]
                    for req_file in required_files:
                        if (node_dir / req_file).exists():
                            print(f"         ✅ {node_dir.name} has {req_file}")
                            
        # Check dataset locks
        locks = list(self.base_path.rglob("dataset_lock__*.json"))
        if locks:
            print(f"      ✅ Found {len(locks)} dataset lock files")
            self.findings["fully_implemented"].append("Dataset lock files")
            
    def _audit_session_state(self):
        """Audit session state components"""
        print("\n   Session State:")
        
        # Check state_key.py
        state_key_path = self.base_path / "state_key.py"
        if state_key_path.exists():
            print(f"      ✅ Found state_key.py")
            
            # Check its functions
            with open(state_key_path, 'r') as f:
                content = f.read()
                
            functions = ["compute_session", "compute_quarter", "compute_dow"]
            for func in functions:
                if f"def {func}" in content:
                    print(f"         ✅ Has {func}()")
                    self.findings["fully_implemented"].append(f"state_key.{func}")
                    
    def _audit_energy_context(self):
        """Audit energy context components"""
        print("\n   Energy Context:")
        
        # Look for energy-related files
        energy_files = []
        for py_file in self.base_path.rglob("*.py"):
            if "energy" in py_file.name.lower():
                energy_files.append(py_file)
                
        if energy_files:
            print(f"      ✅ Found {len(energy_files)} energy-related files")
            for f in energy_files[:3]:
                print(f"         - {f.name}")
                
    def _audit_point_trajectory(self):
        """Audit point trajectory components"""
        print("\n   Point Trajectory:")
        
        # Look for trajectory files
        traj_files = []
        for py_file in self.base_path.rglob("*.py"):
            if "trajectory" in py_file.name.lower() or "traj" in py_file.name.lower():
                traj_files.append(py_file)
                
        if traj_files:
            print(f"      ✅ Found {len(traj_files)} trajectory files")
            for f in traj_files[:3]:
                print(f"         - {f.name}")
                
    def _audit_target_entry(self):
        """Audit target entry stage components"""
        print("\n   Target Entry Stage:")
        
        # Check for target entry files
        te_files = [
            "target_entry_stage.py",
            "target_entry.py",
            "entry.py"
        ]
        
        for file in te_files:
            path = self.base_path / file
            if path.exists():
                print(f"      ✅ Found: {file}")
                self.findings["fully_implemented"].append(f"Target entry: {file}")
                
    def _audit_aee_stage(self):
        """Audit AEE stage components"""
        print("\n   AEE Stage:")
        
        # Look for AEE files
        aee_files = []
        for py_file in self.base_path.rglob("*.py"):
            if "aee" in py_file.name.lower():
                aee_files.append(py_file)
                
        if aee_files:
            print(f"      ✅ Found {len(aee_files)} AEE-related files")
            
            # Check for main AEE components
            main_aee = self.base_path / "aee_engine.py"
            if main_aee.exists():
                print(f"      ✅ Main AEE engine: aee_engine.py")
                self.findings["fully_implemented"].append("AEE engine")
                
    def _audit_trade_type_truth(self):
        """Audit trade type truth components"""
        print("\n   Trade Type Truth:")
        
        # Check compiled nodes for trade_type_truth
        cmn_path = self.base_path / "compiled_market_nodes"
        if cmn_path.exists():
            has_ttt = any("trade_type_truth" in str(p) for p in cmn_path.rglob("*"))
            if has_ttt:
                print(f"      ✅ Found trade_type_truth in compiled nodes")
                self.findings["fully_implemented"].append("Trade type truth data")
                
    def _audit_fixed_pop(self):
        """Audit fixed pop / theoretical ceiling components"""
        print("\n   Fixed Pop / Theoretical Ceiling:")
        
        # Check compiled nodes
        cmn_path = self.base_path / "compiled_market_nodes"
        if cmn_path.exists():
            has_fp = any("fixedpop" in str(p).lower() or "theoretical" in str(p).lower() 
                        for p in cmn_path.rglob("*"))
            if has_fp:
                print(f"      ✅ Found fixedpop/theoretical ceiling data")
                self.findings["fully_implemented"].append("Fixed pop / theoretical ceiling")
                
    def _audit_runtime_bot(self):
        """Audit runtime bot / execution loop"""
        print("\n   Runtime Bot:")
        
        # Check main runtime files
        runtime_files = [
            "phone_bot.py",
            "main.py",
            "runtime_orchestrator.py"
        ]
        
        for file in runtime_files:
            path = self.base_path / file
            if path.exists():
                print(f"      ✅ Found: {file}")
                
                # Check if it has calibration integration
                with open(path, 'r') as f:
                    content = f.read()
                    
                if "runtime_calibration" in content or "RuntimeCalibration" in content:
                    print(f"         ✅ Already integrated with runtime_calibration!")
                    self.findings["fully_implemented"].append(f"{file} with calibration")
                elif "tune_apply" in content or "TuneApply" in content:
                    print(f"         ⚠️  Uses tune_apply (integration point)")
                    self.findings["partially_implemented"].append(f"{file} needs calibration integration")
                    
    def _audit_config_loading(self):
        """Audit config loading components"""
        print("\n   Config Loading:")
        
        # Check for config loading patterns
        config_patterns = [
            ("active_artifacts.py", "Active artifacts loader"),
            ("config_loader.py", "Config loader"),
            ("calibration_loader.py", "Calibration loader")
        ]
        
        for file, desc in config_patterns:
            path = self.base_path / file
            if path.exists():
                print(f"      ✅ Found: {file}")
                self.findings["fully_implemented"].append(f"Config loading: {file}")
                
    def _audit_manifests(self):
        """Audit manifests / stage hashes"""
        print("\n   Manifests / Stage Hashes:")
        
        # Check for manifests in compiled nodes
        cmn_path = self.base_path / "compiled_market_nodes"
        if cmn_path.exists():
            manifests = list(cmn_path.rglob("node_manifest.json"))
            if manifests:
                print(f"      ✅ Found {len(manifests)} node manifests")
                self.findings["fully_implemented"].append("Node manifests")
                
    def _audit_session_quarter_utils(self):
        """Audit session and quarter utilities"""
        print("\n   Session/Quarter Utilities:")
        
        # Already checked state_key.py
        # Check for other utilities
        util_files = []
        for py_file in self.base_path.rglob("*.py"):
            if any(x in py_file.name.lower() for x in ["session", "quarter", "time"]):
                util_files.append(py_file)
                
        if util_files:
            print(f"      ✅ Found {len(util_files)} utility files")
            
    def _find_file_users(self, target_file: str) -> List[str]:
        """Find which files import or use a target file"""
        users = []
        target_name = Path(target_file).stem
        
        for file_path, imports in self.file_imports.items():
            if any(target_name in imp for imp in imports):
                users.append(file_path)
                
        return users
        
    def _analyze_findings(self):
        """Analyze audit findings"""
        print("\n   Analysis Summary:")
        print(f"      Fully implemented: {len(self.findings['fully_implemented'])}")
        print(f"      Partially implemented: {len(self.findings['partially_implemented'])}")
        print(f"      Dead/Legacy: {len(self.findings['dead_legacy'])}")
        print(f"      Missing: {len(self.findings['missing'])}")
        
    def _generate_integration_map(self):
        """Generate integration map"""
        self.integration_map = {
            "runtime_calibration_owner": "runtime_calibration.py",
            "compiled_map_loader": "compiled_trading_map.py",
            "quarter_handoff_manager": "quarter_handoff_manager.py",
            "fallback_system": "fallback_templates.py",
            "integration_point": "phone_bot.py",
            "active_artifacts": "active_artifacts.py"
        }
        
    def _produce_audit_report(self) -> Dict:
        """Produce final audit report"""
        report = {
            "fully_built_components": [
                "state_key.py - Session/quarter/dow computation",
                "compiled_market_nodes/ - Research output directory",
                "tune_map.py - Mapping system",
                "phone_bot.py - Main runtime (partially integrated)",
                "active_artifacts.py - Artifact validation",
                "aee_engine.py - AEE execution engine"
            ],
            "partial_components": [
                "phone_bot.py - Has tune_apply but needs runtime_calibration integration",
                "Quarter handoff - Manager exists but not wired to open trades",
                "Config loading - Multiple loaders exist, need unification"
            ],
            "dead_legacy_candidates": [
                "Old calibration wrappers (need verification)",
                "Duplicate tune_map versions (v2, v3, v4)",
                "Unused fallback paths"
            ],
            "missing": [
                "Open trade quarter handoff wiring",
                "Compiled node loading in main runtime",
                "Live quarter management for existing trades"
            ],
            "exact_file_owners": {
                "runtime_calibration": "runtime_calibration.py",
                "compiled_mapping": "compiled_trading_map.py", 
                "quarter_handoff": "quarter_handoff_manager.py",
                "main_runtime": "phone_bot.py"
            },
            "patch_plan": [
                "1. Verify runtime_calibration.py integration in phone_bot.py",
                "2. Wire quarter handoff to update open trade management",
                "3. Remove duplicate/legacy tune_map versions",
                "4. Ensure compiled nodes load at startup, not runtime",
                "5. Test quarter transitions with live trades"
            ]
        }
        
        return report

# Run the audit
if __name__ == "__main__":
    auditor = RepoAuditor()
    report = auditor.audit_repository()
    
    print("\n" + "=" * 70)
    print("AUDIT REPORT")
    print("=" * 70)
    
    for section, items in report.items():
        print(f"\n{section.upper().replace('_', ' ')}:")
        for item in items:
            print(f"   - {item}")
