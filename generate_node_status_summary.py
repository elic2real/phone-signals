#!/usr/bin/env python3
"""
Generate node_status_summary.json for all compiled nodes.

This script consolidates Stage 1-6 (opportunity/structure) and Stage 7+ (entry selection)
status into a single summary file per node, eliminating confusion between:
  - Stage 6 session_potential (pre-selection opportunity metrics)
  - Stage 7+ target_entry_stage (actual selected trades)

Output: node_status_summary.json in each compiled_market_nodes/<node>/ directory
"""

import json
import glob
from pathlib import Path
from typing import Dict, Any, Optional


def load_json_safe(path: str) -> Optional[Dict[str, Any]]:
    """Load JSON file, return None if missing or invalid."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def extract_stage6_status(node_dir: Path) -> Dict[str, Any]:
    """Extract Stage 1-6 compilation status from session_potential report."""
    report_path = node_dir / "session_potential" / "session_potential_report.json"
    data = load_json_safe(str(report_path))
    
    if not data:
        return {
            "stage6_complete": False,
            "status": "MISSING",
            "error": "session_potential_report.json not found"
        }
    
    pr = data.get('pair_rollup', {})
    zones = data.get('zones', [])
    
    return {
        "stage6_complete": data.get('status') == 'PASS',
        "status": data.get('status'),
        "timestamp": data.get('timestamp'),
        "expected_long_opp_per_hour": pr.get('expected_long_opportunities_per_hour'),
        "expected_short_opp_per_hour": pr.get('expected_short_opportunities_per_hour'),
        "zone_count": len(zones),
        "warning": "Stage 6 metrics are PRE-SELECTION only. See stage7_status for actual trades."
    }


def extract_stage7_status(node_dir: Path) -> Dict[str, Any]:
    """Extract Stage 7+ entry selection status from target_entry_stage."""
    report_path = node_dir / "target_entry_stage" / "target_stage_report.json"
    data = load_json_safe(str(report_path))
    
    if not data:
        return {
            "stage7_complete": False,
            "status": "NOT_RUN",
            "error": "target_stage_report.json not found - entry selection has not run"
        }
    
    summary = data.get('summary', [])
    
    # Aggregate across all targets
    total_trades = sum(s.get('trade_count', 0) for s in summary)
    total_wins = sum(s.get('wins', 0) for s in summary)
    
    long_summary = [s for s in summary if s.get('direction') == 'LONG']
    short_summary = [s for s in summary if s.get('direction') == 'SHORT']
    
    long_trades = sum(s.get('trade_count', 0) for s in long_summary)
    short_trades = sum(s.get('trade_count', 0) for s in short_summary)
    
    # Find best performing targets
    best_long = max(long_summary, key=lambda s: s.get('pips_per_hour', 0)) if long_summary else None
    best_short = max(short_summary, key=lambda s: s.get('pips_per_hour', 0)) if short_summary else None
    
    overall_win_rate = (total_wins / total_trades) if total_trades > 0 else 0
    
    return {
        "stage7_complete": True,
        "status": "PASS",
        "total_trades": total_trades,
        "long_trades": long_trades,
        "short_trades": short_trades,
        "overall_win_rate": round(overall_win_rate, 4),
        "best_long_target": {
            "target_distance": best_long.get('target_distance'),
            "trade_count": best_long.get('trade_count'),
            "win_rate": round(best_long.get('win_rate', 0), 4),
            "pips_per_hour": round(best_long.get('pips_per_hour', 0), 2)
        } if best_long else None,
        "best_short_target": {
            "target_distance": best_short.get('target_distance'),
            "trade_count": best_short.get('trade_count'),
            "win_rate": round(best_short.get('win_rate', 0), 4),
            "pips_per_hour": round(best_short.get('pips_per_hour', 0), 2)
        } if best_short else None
    }


def generate_node_summary(node_dir: Path) -> Dict[str, Any]:
    """Generate complete node status summary."""
    node_name = node_dir.name
    parts = node_name.split('__')
    
    if len(parts) >= 3:
        pair, weekday, session = parts[0], parts[1], parts[2]
    else:
        pair, weekday, session = node_name, "unknown", "unknown"
    
    stage6 = extract_stage6_status(node_dir)
    stage7 = extract_stage7_status(node_dir)
    
    # Determine overall node health
    if not stage6.get('stage6_complete'):
        health = "INCOMPLETE_STAGE6"
    elif not stage7.get('stage7_complete'):
        health = "INCOMPLETE_STAGE7"
    elif stage7.get('total_trades', 0) == 0:
        health = "NO_TRADES_SELECTED"
    elif stage7.get('total_trades', 0) < 10:
        health = "LOW_TRADE_COUNT"
    else:
        health = "OPERATIONAL"
    
    return {
        "node": {
            "name": node_name,
            "pair": pair,
            "weekday": weekday,
            "session": session
        },
        "health": health,
        "stage6_opportunity_layer": stage6,
        "stage7_entry_selection": stage7,
        "interpretation_guide": {
            "stage6": "Shows opportunity POTENTIAL before entry selection. actual_selected_count=0 is normal.",
            "stage7": "Shows ACTUAL trades after entry logic. This is the true trade activity.",
            "health_operational": "Node has completed both stages and has sufficient trade activity.",
            "health_incomplete": "Node compilation or entry selection has not finished."
        }
    }


def main():
    """Generate node_status_summary.json for all compiled nodes."""
    node_dirs = sorted(Path('compiled_market_nodes').glob('*'))
    node_dirs = [d for d in node_dirs if d.is_dir()]
    
    print(f"Generating node status summaries for {len(node_dirs)} nodes...\n")
    
    success_count = 0
    error_count = 0
    
    for node_dir in node_dirs:
        try:
            summary = generate_node_summary(node_dir)
            output_path = node_dir / "node_status_summary.json"
            
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            health = summary['health']
            stage7_trades = summary['stage7_entry_selection'].get('total_trades', 0)
            
            print(f"✓ {node_dir.name}")
            print(f"  Health: {health}")
            print(f"  Stage 7 trades: {stage7_trades}")
            
            success_count += 1
            
        except Exception as e:
            print(f"✗ {node_dir.name}: {e}")
            error_count += 1
    
    print(f"\n{'='*80}")
    print(f"Summary generation complete:")
    print(f"  Success: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"\nOutput: compiled_market_nodes/*/node_status_summary.json")


if __name__ == '__main__':
    main()
