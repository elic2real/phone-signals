#!/usr/bin/env python3
"""
Opportunity Supply Stability Audit

Diagnoses instability in opportunity counts across pipeline layers:
1. Raw observations (inflated top-layer count)
2. Cluster-resolved impulses (distinct movement events)
3. Valid entry-window states (usable entry points)
4. Selected trades (Stage 7 output)

For comparable contexts (same pair/weekday/session, different locks),
computes mean, std dev, and coefficient of variation to identify
the first unstable layer.
"""

import json
import csv
from pathlib import Path
from collections import defaultdict
import statistics

ROOT = Path(__file__).parent
COMPILED_DIR = ROOT / "compiled_market_nodes"


def load_json(path):
    """Load JSON file, return None if missing."""
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def analyze_node_supply_layers(node_dir):
    """
    Extract supply counts from all available pipeline stages.
    
    Returns dict with:
        raw_observation_count
        cluster_count
        avg_cluster_size
        max_cluster_size
        valid_entry_state_count
        selected_trade_count
        raw_pph (if available)
        cluster_pph (if available)
        selected_trade_pph (if available)
    """
    result = {
        "raw_observation_count": None,
        "cluster_count": None,
        "avg_cluster_size": None,
        "max_cluster_size": None,
        "valid_entry_state_count": None,
        "selected_trade_count": None,
        "raw_pph": None,
        "cluster_pph": None,
        "selected_trade_pph": None,
    }
    
    # Try to load target_entry_stage report (raw opportunity counts)
    target_report_path = node_dir / "target_entry_stage" / "target_stage_report.json"
    if target_report_path.exists():
        target_report = load_json(target_report_path)
        if target_report:
            # Sum all opportunity counts across classes
            total_trades = sum(item.get("trade_count", 0) for item in target_report.get("summary", []))
            result["raw_observation_count"] = total_trades
            
            # Best PPH as proxy for raw performance
            best_pph = max((item.get("pips_per_hour", 0) for item in target_report.get("summary", [])), default=0)
            result["raw_pph"] = best_pph
    
    # Try to load session_energy_state_stream.csv for cluster analysis
    stream_csv = node_dir / "target_entry_stage" / "session_energy_state_stream.csv"
    if stream_csv.exists():
        try:
            import pandas as pd
            df = pd.read_csv(stream_csv)
            
            # Count unique clusters if cluster_id exists
            if "cluster_id" in df.columns:
                cluster_counts = df.groupby("cluster_id").size()
                result["cluster_count"] = len(cluster_counts)
                result["avg_cluster_size"] = cluster_counts.mean()
                result["max_cluster_size"] = cluster_counts.max()
            
            # Count valid entry states (unique timestamps with valid direction_assumed)
            if "direction_assumed" in df.columns and "timestamp" in df.columns:
                valid_entries = df[df["direction_assumed"].notna()]
                result["valid_entry_state_count"] = len(valid_entries)
        except Exception as e:
            pass
    
    # Try to load target_entry_no_timeouts for selected trade count
    no_timeout_report = node_dir / "target_entry_no_timeouts" / "target_entry_class_report.json"
    if no_timeout_report.exists():
        no_timeout_data = load_json(no_timeout_report)
        if no_timeout_data:
            best_class = no_timeout_data.get("best_class", {})
            result["selected_trade_count"] = best_class.get("trade_count", 0)
            result["selected_trade_pph"] = best_class.get("pips_per_hour", 0)
    
    # Try to load AEE stage for optimized performance
    aee_report = node_dir / "aee_stage" / "aee_stage_report.json"
    if aee_report.exists():
        aee_data = load_json(aee_report)
        if aee_data:
            result["selected_trade_count"] = aee_data.get("trade_count", result["selected_trade_count"])
            result["cluster_pph"] = aee_data.get("aee_pph", 0)
    
    return result


def compute_stability_stats(values):
    """
    Compute mean, std dev, and coefficient of variation.
    Returns dict with mean, std_dev, cv.
    """
    if not values or len(values) < 2:
        return {"mean": None, "std_dev": None, "cv": None, "count": len(values)}
    
    valid_values = [v for v in values if v is not None and v > 0]
    if len(valid_values) < 2:
        return {"mean": None, "std_dev": None, "cv": None, "count": len(valid_values)}
    
    mean = statistics.mean(valid_values)
    std_dev = statistics.stdev(valid_values)
    cv = std_dev / mean if mean > 0 else None
    
    return {
        "mean": mean,
        "std_dev": std_dev,
        "cv": cv,
        "count": len(valid_values),
        "min": min(valid_values),
        "max": max(valid_values)
    }


def analyze_layer_collapse_ratios(supply_layers):
    """
    Compute collapse ratios between pipeline layers.
    
    Returns dict with ratios showing how much supply collapses at each stage.
    """
    ratios = {}
    
    raw = supply_layers.get("raw_observation_count")
    cluster = supply_layers.get("cluster_count")
    entry = supply_layers.get("valid_entry_state_count")
    selected = supply_layers.get("selected_trade_count")
    
    # Cluster collapse ratio (how much raw observations collapse into clusters)
    if raw and cluster and raw > 0:
        ratios["cluster_collapse_ratio"] = cluster / raw
        ratios["avg_observations_per_cluster"] = raw / cluster
    
    # Entry window collapse (how many clusters become valid entry states)
    if cluster and entry and cluster > 0:
        ratios["entry_from_cluster_ratio"] = entry / cluster
    
    # Selection ratio (how many entry states become selected trades)
    if entry and selected and entry > 0:
        ratios["selection_ratio"] = selected / entry
    
    # End-to-end collapse (raw to selected)
    if raw and selected and raw > 0:
        ratios["end_to_end_collapse"] = selected / raw
    
    # Identify problematic collapse patterns
    issues = []
    
    # If cluster collapse is extreme (< 0.01 means 100+ observations per cluster)
    if ratios.get("cluster_collapse_ratio", 1) < 0.01:
        issues.append("EXTREME_CLUSTER_COLLAPSE")
    
    # If cluster collapse is too weak (> 0.9 means almost no clustering)
    if ratios.get("cluster_collapse_ratio", 0) > 0.9:
        issues.append("WEAK_CLUSTERING")
    
    # If entry window collapse is extreme
    if ratios.get("entry_from_cluster_ratio", 1) < 0.05:
        issues.append("EXTREME_ENTRY_COLLAPSE")
    
    # If selection is too aggressive
    if ratios.get("selection_ratio", 1) < 0.01:
        issues.append("EXTREME_SELECTION_COLLAPSE")
    
    # If end-to-end collapse is absurd (< 0.001 means 1000:1 collapse)
    if ratios.get("end_to_end_collapse", 1) < 0.001:
        issues.append("ABSURD_END_TO_END_COLLAPSE")
    
    ratios["issues"] = issues
    
    return ratios


def audit_context_stability(pair, weekday, session):
    """
    Audit supply layer collapse for a specific context.
    
    Since each node has only 1 lock, we analyze within-node layer ratios
    instead of cross-lock variance.
    
    Returns dict with:
        context: {pair, weekday, session}
        node_data: supply layers and collapse ratios
        collapse_issues: list of identified problems
    """
    context_key = f"{pair}__{weekday}__{session}"
    
    # Find all nodes matching this context
    matching_nodes = list(COMPILED_DIR.glob(f"{context_key}*"))
    
    if not matching_nodes:
        return None
    
    # Analyze the node (should only be 1 for Thursday)
    node_dir = matching_nodes[0]
    supply_layers = analyze_node_supply_layers(node_dir)
    supply_layers["node_name"] = node_dir.name
    
    # Compute collapse ratios
    collapse_ratios = analyze_layer_collapse_ratios(supply_layers)
    
    return {
        "context": {"pair": pair, "weekday": weekday, "session": session},
        "node_name": node_dir.name,
        "supply_layers": supply_layers,
        "collapse_ratios": collapse_ratios,
        "collapse_issues": collapse_ratios.get("issues", [])
    }


def main():
    """Run stability audit on selected contexts."""
    
    # Define contexts to audit (worst offenders from Thursday data)
    contexts_to_audit = [
        # GBP_CHF showed extreme variance (0 to 1628 trades)
        ("GBP_CHF", "thursday", "sydney"),
        ("GBP_CHF", "thursday", "asia"),
        ("GBP_CHF", "thursday", "london"),
        ("GBP_CHF", "thursday", "new_york"),
        
        # EUR_GBP for comparison
        ("EUR_GBP", "thursday", "sydney"),
        ("EUR_GBP", "thursday", "asia"),
        ("EUR_GBP", "thursday", "london"),
        ("EUR_GBP", "thursday", "new_york"),
        
        # USD_JPY for comparison
        ("USD_JPY", "thursday", "sydney"),
        ("USD_JPY", "thursday", "asia"),
        ("USD_JPY", "thursday", "london"),
        ("USD_JPY", "thursday", "new_york"),
        
        # NZD_USD for comparison
        ("NZD_USD", "thursday", "sydney"),
        ("NZD_USD", "thursday", "asia"),
        ("NZD_USD", "thursday", "london"),
        ("NZD_USD", "thursday", "new_york"),
    ]
    
    results = []
    
    print("=" * 120)
    print("OPPORTUNITY SUPPLY STABILITY AUDIT")
    print("=" * 120)
    print()
    
    for pair, weekday, session in contexts_to_audit:
        print(f"Auditing {pair} {weekday} {session}...")
        audit_result = audit_context_stability(pair, weekday, session)
        
        if audit_result:
            results.append(audit_result)
            
            # Print summary
            ctx = audit_result["context"]
            supply = audit_result["supply_layers"]
            ratios = audit_result["collapse_ratios"]
            issues = audit_result["collapse_issues"]
            
            print(f"\n{ctx['pair']} {ctx['weekday']} {ctx['session']}:")
            print(f"  Node: {audit_result['node_name']}")
            
            print(f"\n  Supply Layer Counts:")
            raw_obs = supply.get('raw_observation_count')
            clusters = supply.get('cluster_count')
            entry_states = supply.get('valid_entry_state_count')
            selected = supply.get('selected_trade_count')
            
            print(f"    Raw observations:     {raw_obs if raw_obs is not None else 'N/A':>10}")
            print(f"    Clusters:             {clusters if clusters is not None else 'N/A':>10}")
            print(f"    Valid entry states:   {entry_states if entry_states is not None else 'N/A':>10}")
            print(f"    Selected trades:      {selected if selected is not None else 'N/A':>10}")
            
            if supply.get('avg_cluster_size'):
                print(f"    Avg cluster size:     {supply.get('avg_cluster_size'):>10.1f}")
            
            print(f"\n  Collapse Ratios:")
            if ratios.get("cluster_collapse_ratio"):
                print(f"    Raw → Cluster:        {ratios['cluster_collapse_ratio']:>10.4f}  ({ratios.get('avg_observations_per_cluster', 0):.1f} obs/cluster)")
            if ratios.get("entry_from_cluster_ratio"):
                print(f"    Cluster → Entry:      {ratios['entry_from_cluster_ratio']:>10.4f}")
            if ratios.get("selection_ratio"):
                print(f"    Entry → Selected:     {ratios['selection_ratio']:>10.4f}")
            if ratios.get("end_to_end_collapse"):
                print(f"    Raw → Selected:       {ratios['end_to_end_collapse']:>10.4f}  (1:{1/ratios['end_to_end_collapse']:.0f} collapse)")
            
            if issues:
                print(f"\n  ⚠️  ISSUES DETECTED:")
                for issue in issues:
                    print(f"      - {issue}")
            else:
                print(f"\n  ✅ No collapse issues detected")
            
            print()
    
    # Write JSON output
    output_json = ROOT / "opportunity_supply_stability_audit.json"
    with open(output_json, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Full results written to: {output_json}")
    
    # Write CSV output
    output_csv = ROOT / "opportunity_supply_stability_audit.csv"
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Pair", "Weekday", "Session", "Node Name",
            "Raw Obs", "Clusters", "Entry States", "Selected Trades",
            "Cluster Collapse Ratio", "Entry From Cluster Ratio", 
            "Selection Ratio", "End-to-End Collapse",
            "Issues"
        ])
        
        for result in results:
            ctx = result["context"]
            supply = result["supply_layers"]
            ratios = result["collapse_ratios"]
            issues = "; ".join(result["collapse_issues"]) if result["collapse_issues"] else "None"
            
            row = [
                ctx["pair"], ctx["weekday"], ctx["session"], result["node_name"],
                supply.get("raw_observation_count"),
                supply.get("cluster_count"),
                supply.get("valid_entry_state_count"),
                supply.get("selected_trade_count"),
                ratios.get("cluster_collapse_ratio"),
                ratios.get("entry_from_cluster_ratio"),
                ratios.get("selection_ratio"),
                ratios.get("end_to_end_collapse"),
                issues
            ]
            writer.writerow(row)
    
    print(f"✅ CSV summary written to: {output_csv}")
    
    # Summary statistics
    print("\n" + "=" * 120)
    print("SUMMARY")
    print("=" * 120)
    
    contexts_with_issues = [r for r in results if r["collapse_issues"]]
    print(f"\nContexts analyzed: {len(results)}")
    print(f"Contexts with collapse issues: {len(contexts_with_issues)}")
    print(f"Contexts without issues: {len(results) - len(contexts_with_issues)}")
    
    if contexts_with_issues:
        print(f"\nIssue breakdown:")
        issue_counts = defaultdict(int)
        for r in contexts_with_issues:
            for issue in r["collapse_issues"]:
                issue_counts[issue] += 1
        
        for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  {issue}: {count} contexts")
    
    print("\n" + "=" * 120)


if __name__ == "__main__":
    main()
