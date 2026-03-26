import os
import json
import csv
import glob

REQUIRED_IDENTITY_FIELDS = ["pair", "weekday", "session"]

# Helper to safely load JSON
def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None

def find_first_file(pattern):
    files = glob.glob(pattern)
    return files[0] if files else None

def build_node_identity(node_dir):
    manifest_path = os.path.join(node_dir, "node_manifest.json")
    manifest = load_json(manifest_path)
    if manifest and "node" in manifest:
        ident = {k: manifest["node"].get(k, None) for k in REQUIRED_IDENTITY_FIELDS}
        ident["node_path"] = node_dir
        return ident
    return None

def build_session_state_build_report(node_dir):
    calib_path = os.path.join(node_dir, "session_calibration", "session_calibration_report.json")
    opp_path = os.path.join(node_dir, "session_opportunity_map", "session_opportunity_map_report.json")
    calib = load_json(calib_path)
    opp = load_json(opp_path)
    report = {}
    if calib:
        report["status"] = calib.get("status", "unknown")
        report["raw_observation_count"] = len(calib.get("pair_summary", []))
    if opp:
        report["cluster_count"] = opp.get("pair_rollup", {}).get("distinct_session_ids", 0)
        report["entry_window_count"] = opp.get("pair_rollup", {}).get("total_opportunities", 0)
    return report if report else None

def build_session_energy_state_stream(node_dir):
    # Try phase3 entry_window_states.csv first
    ew_path = os.path.join(node_dir, "stage1_6", "phase3", "entry_window_states.csv")
    if os.path.exists(ew_path):
        return ew_path
    # Fallback: look for any CSV with entry/row-level state
    phase6_dir = os.path.join(node_dir, "stage1_6", "phase6")
    for fname in os.listdir(phase6_dir):
        if fname.endswith(".csv"):
            return os.path.join(phase6_dir, fname)
    return None

def main():
    root = "compiled_market_nodes"
    monday_nodes = [os.path.join(root, d) for d in os.listdir(root) if "__monday__" in d]
    for node in monday_nodes:
        print(f"Processing {node}")
        identity = build_node_identity(node)
        state_report = build_session_state_build_report(node)
        state_stream_path = build_session_energy_state_stream(node)
        missing = []
        if not identity:
            missing.append("node_identity.json")
        if not state_report:
            missing.append("session_state_build_report.json")
        if not state_stream_path:
            missing.append("session_energy_state_stream.csv")
        if missing:
            print(f"{node} missing_required_stage6_fields: {missing}")
            continue
        # Write node_identity.json
        with open(os.path.join(node, "node_identity.json"), "w") as f:
            json.dump(identity, f, indent=2)
        # Write session_state_build_report.json
        with open(os.path.join(node, "session_state_build_report.json"), "w") as f:
            json.dump(state_report, f, indent=2)
        # Patch: If using opportunity_zones_labeled.csv, map 'speed' to both 'speed_3' and 'speed_10'
        out_csv = os.path.join(node, "session_energy_state_stream.csv")
        if state_stream_path.endswith("opportunity_zones_labeled.csv"):
            with open(state_stream_path, "r", newline='') as src:
                reader = csv.DictReader(src)
                fieldnames = list(reader.fieldnames)
                # Add speed_3 and speed_10 if not present
                patched_fields = fieldnames.copy()
                if "speed_3" not in patched_fields:
                    patched_fields.append("speed_3")
                if "speed_10" not in patched_fields:
                    patched_fields.append("speed_10")
                rows = []
                for row in reader:
                    speed_val = row.get("speed", "")
                    row["speed_3"] = speed_val
                    row["speed_10"] = speed_val
                    rows.append(row)
            with open(out_csv, "w", newline='') as dst:
                writer = csv.DictWriter(dst, fieldnames=patched_fields)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
        else:
            if state_stream_path != out_csv:
                with open(state_stream_path, "r") as src, open(out_csv, "w") as dst:
                    dst.write(src.read())
        print(f"{node} normalized Stage-6 handoff generated.")

if __name__ == "__main__":
    main()
