#!/usr/bin/env python3
"""
Sizing Logic Audit - Find All Position Sizing Related Code
"""

import ast
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple
import re

class SizingLogicAuditor:
    """Find all sizing logic in the project"""
    
    def __init__(self, base_path: str = "/home/elic/Documents/phone signals"):
        self.base_path = Path(base_path)
        self.sizing_references = []
        self.sizing_functions = []
        self.sizing_configs = []
        
    def audit_sizing_logic(self):
        """Find all sizing-related code"""
        print("=" * 70)
        print("SIZING LOGIC AUDIT")
        print("=" * 70)
        
        # Search terms related to sizing
        sizing_terms = [
            "position_size", "positionsize", "position.sizing",
            "risk_percent", "riskpercent", "risk.percent",
            "max_risk", "maxrisk", "risk_per_trade",
            "lot_size", "lotsize", "lots",
            "units", "volume", "trade_size",
            "sizing", "size", "calculate_size"
        ]
        
        # 1. Search Python files for sizing terms
        print("\n1. Searching Python files for sizing logic...")
        
        for py_file in self.base_path.rglob("*.py"):
            if any(skip in str(py_file) for skip in ["__pycache__", ".git", ".venv"]):
                continue
                
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    
                for i, line in enumerate(lines):
                    line_lower = line.lower()
                    
                    # Check for sizing terms
                    for term in sizing_terms:
                        if term in line_lower:
                            # Store the reference
                            self.sizing_references.append({
                                'file': str(py_file.relative_to(self.base_path)),
                                'line': i + 1,
                                'content': line.strip(),
                                'term': term
                            })
                            
                            # Check if it's a function definition
                            if 'def ' in line and any(t in line_lower for t in ['size', 'risk', 'position']):
                                self.sizing_functions.append({
                                    'file': str(py_file.relative_to(self.base_path)),
                                    'line': i + 1,
                                    'signature': line.strip()
                                })
                                
            except Exception as e:
                print(f"   Warning: Could not read {py_file}: {e}")
                
        # 2. Search JSON/config files
        print("\n2. Searching config files for sizing parameters...")
        
        for json_file in self.base_path.rglob("*.json"):
            if any(skip in str(json_file) for skip in ["__pycache__", ".git"]):
                continue
                
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Look for sizing-related keys
                for term in sizing_terms:
                    if term in content.lower():
                        # Try to parse as JSON to get context
                        try:
                            data = json.loads(content)
                            sizing_keys = self._find_sizing_keys(data, term)
                            if sizing_keys:
                                self.sizing_configs.append({
                                    'file': str(json_file.relative_to(self.base_path)),
                                    'keys': sizing_keys
                                })
                        except:
                            # Not valid JSON, just note the file
                            self.sizing_configs.append({
                                'file': str(json_file.relative_to(self.base_path)),
                                'keys': [f"Contains '{term}'"]
                            })
                            
            except Exception as e:
                pass
                
        # 3. Look for specific sizing patterns
        print("\n3. Looking for specific sizing patterns...")
        
        patterns = {
            "Account percentage": r"(\d+\.?\d*)\s*%.*account",
            "Risk per trade": r"risk.*per.*trade",
            "Position calculation": r"calculate.*position|position.*calculate",
            "Lot calculation": r"calculate.*lot|lot.*calculate",
            "Unit calculation": r"calculate.*unit|unit.*calculate"
        }
        
        for pattern_name, pattern in patterns.items():
            matches = self._search_pattern(pattern)
            if matches:
                print(f"\n   {pattern_name}:")
                for match in matches[:3]:  # Show first 3
                    print(f"      {match['file']}: {match['line']} - {match['content'][:80]}...")
                    
        # 4. Analyze findings
        self._analyze_sizing_findings()
        
        return self._generate_sizing_plan()
        
    def _find_sizing_keys(self, data: dict, term: str, path: str = "") -> List[str]:
        """Recursively find sizing-related keys in JSON"""
        keys = []
        term_lower = term.lower()
        
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                key_lower = key.lower()
                
                if term_lower in key_lower:
                    keys.append(current_path)
                    
                if isinstance(value, (dict, list)):
                    keys.extend(self._find_sizing_keys(value, term, current_path))
                    
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    keys.extend(self._find_sizing_keys(item, term, f"{path}[{i}]"))
                    
        return keys
        
    def _search_pattern(self, pattern: str) -> List[Dict]:
        """Search for regex pattern in files"""
        matches = []
        regex = re.compile(pattern, re.IGNORECASE)
        
        for py_file in self.base_path.rglob("*.py"):
            if any(skip in str(py_file) for skip in ["__pycache__", ".git", ".venv"]):
                continue
                
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                for i, line in enumerate(lines):
                    if regex.search(line):
                        matches.append({
                            'file': str(py_file.relative_to(self.base_path)),
                            'line': i + 1,
                            'content': line.strip()
                        })
                        
            except:
                pass
                
        return matches
        
    def _analyze_sizing_findings(self):
        """Analyze the sizing findings"""
        print("\n" + "=" * 70)
        print("SIZING ANALYSIS SUMMARY")
        print("=" * 70)
        
        print(f"\nFound {len(self.sizing_references)} sizing references")
        print(f"Found {len(self.sizing_functions)} sizing functions")
        print(f"Found {len(self.sizing_configs)} config files with sizing")
        
        # Group by file
        by_file = {}
        for ref in self.sizing_references:
            file = ref['file']
            if file not in by_file:
                by_file[file] = []
            by_file[file].append(ref)
            
        print("\nFiles with most sizing logic:")
        sorted_files = sorted(by_file.items(), key=lambda x: len(x[1]), reverse=True)
        for file, refs in sorted_files[:5]:
            print(f"   {file}: {len(refs)} references")
            
    def _generate_sizing_plan(self) -> Dict:
        """Generate a plan for sizing logic integration"""
        print("\n" + "=" * 70)
        print("SIZING INTEGRATION PLAN")
        print("=" * 70)
        
        plan = {
            "current_state": {
                "sizing_locations": [],
                "sizing_methods": [],
                "config_sources": []
            },
            "integration_points": [],
            "required_changes": [],
            "implementation_order": []
        }
        
        # Identify key files
        key_files = set()
        for ref in self.sizing_references:
            key_files.add(ref['file'])
            
        plan["current_state"]["sizing_locations"] = list(key_files)[:10]
        
        # Check for specific sizing implementations
        print("\n1. Current Sizing Implementation:")
        
        # Look for main sizing logic
        main_sizing_files = [
            "phone_bot.py",
            "position_sizer.py",
            "risk_manager.py",
            "trade_executor.py"
        ]
        
        for file in main_sizing_files:
            if (self.base_path / file).exists():
                print(f"   ✅ Found: {file}")
                plan["current_state"]["sizing_methods"].append(file)
            else:
                print(f"   ❌ Missing: {file}")
                
        # Check config sources
        print("\n2. Configuration Sources:")
        config_sources = set()
        for config in self.sizing_configs:
            config_sources.add(config['file'])
            
        for source in sorted(config_sources)[:5]:
            print(f"   - {source}")
            plan["current_state"]["config_sources"].append(source)
            
        # Generate integration plan
        print("\n3. Integration Plan:")
        
        plan["integration_points"] = [
            "Runtime calibration provides max_risk_percent",
            "Position sizer uses this to calculate trade size",
            "AEE management respects sizing limits",
            "Emergency fallback forces 0.5% max risk"
        ]
        
        plan["required_changes"] = [
            "Ensure position sizer reads from runtime calibration",
            "Add size validation before trade entry",
            "Wire quarter handoff to update sizing limits",
            "Add size monitoring in trade management"
        ]
        
        plan["implementation_order"] = [
            "1. Find/verify position sizing implementation",
            "2. Connect runtime calibration to position sizer",
            "3. Add sizing validation in entry logic",
            "4. Test sizing across all fallback scenarios",
            "5. Monitor sizing in live trading"
        ]
        
        for item in plan["integration_points"]:
            print(f"   - {item}")
            
        print("\n4. Implementation Order:")
        for item in plan["implementation_order"]:
            print(f"   {item}")
            
        return plan

# Import json for parsing
import json

if __name__ == "__main__":
    auditor = SizingLogicAuditor()
    plan = auditor.audit_sizing_logic()
    
    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("\n1. Examine the files with sizing logic")
    print("2. Identify the main position sizing function")
    print("3. Trace how risk_percent is currently used")
    print("4. Plan integration with runtime calibration")
