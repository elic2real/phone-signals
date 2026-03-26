import os
import csv
import json
def list_csv_columns(path):
    try:
        with open(path, 'r') as f:
            reader = csv.reader(f)
            return next(reader)
    except Exception:
        return []
def list_json_keys(path):
    try:
        with open(path, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return list(data.keys())
            return []
    except Exception:
        return []
def audit_node(node_dir, required_cols):
    result = {
        'node': node_dir,
        'missing': [],
        'old_files': [],
        'candidates': {}
    }
    # Find all CSV and JSON files in stage1_6
    old_files = []
    for root, dirs, files in os.walk(os.path.join(node_dir, 'stage1_6')):
        for f in files:
            if f.endswith('.csv') or f.endswith('.json'):
                old_files.append(os.path.relpath(os.path.join(root, f), node_dir))
    result['old_files'] = old_files
    # Gather all columns from all CSVs
    all_csv_cols = {}
    for f in old_files:
        if f.endswith('.csv'):
            cols = list_csv_columns(os.path.join(node_dir, f))
            all_csv_cols[f] = cols
    # Gather all keys from all JSONs
    all_json_keys = {}
    for f in old_files:
        if f.endswith('.json'):
            keys = list_json_keys(os.path.join(node_dir, f))
            all_json_keys[f] = keys
    # Check session_energy_state_stream.csv for missing columns
    stream_path = os.path.join(node_dir, 'session_energy_state_stream.csv')
    present_cols = list_csv_columns(stream_path) if os.path.exists(stream_path) else []
    missing = [col for col in required_cols if col not in present_cols]
    result['missing'] = missing
    # For each missing, look for candidate legacy columns
    for m in missing:
        candidates = []
        for f, cols in all_csv_cols.items():
            for c in cols:
                if m.lower() in c.lower() or c.lower() in m.lower():
                    candidates.append((f, c))
        result['candidates'][m] = candidates
    return result

def main():
    required_cols = [
        'speed_3', 'speed_10', 'bias_20', 'compression', 'pullback_depth_10',
        'distance_from_extreme_10', 'reclaim_state', 'swing_break_state', 'quarter_phase'
    ]
    root = 'compiled_market_nodes'
    monday_nodes = [os.path.join(root, d) for d in os.listdir(root) if '__monday__' in d]
    audits = []
    for node in monday_nodes:
        if os.path.exists(os.path.join(node, 'session_energy_state_stream.csv')):
            audits.append(audit_node(node, required_cols))
    # Group by missing pattern
    pattern_groups = {}
    for audit in audits:
        pattern = tuple(sorted(audit['missing']))
        if pattern not in pattern_groups:
            pattern_groups[pattern] = []
        pattern_groups[pattern].append(audit)
    # Write report
    with open('monday_missing_fields_report.json', 'w') as f:
        json.dump({'audits': audits, 'pattern_groups': {str(k): [a['node'] for a in v] for k, v in pattern_groups.items()}}, f, indent=2)
    print('Monday missing-fields audit complete. See monday_missing_fields_report.json')

if __name__ == '__main__':
    main()
