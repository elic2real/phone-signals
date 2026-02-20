
import os
import glob
import subprocess
import json
import sys

SCENARIO_DIR = "scenarios/golden/v1.0"
OUTPUT_DIR = "sim_results"
HARNESS_SCRIPT = "sim_harness.py"

def get_instrument_from_csv(csv_path):
    with open(csv_path, 'r') as f:
        header = f.readline()
        first_line = f.readline()
        if not first_line:
            return "EUR_USD"
        return first_line.split(',')[0]

def run_simulation(scenario_path, direction):
    base_name = os.path.basename(scenario_path).replace(".csv", "")
    # Use double underscore to matching sim_extraction_audit.py expectation for scenario extraction,
    # but encode direction in the suffix so we can parse it if needed, or group by prefix.
    # However, sim_extraction_audit.py takes parts[-1] as scenario name.
    # If we want to group LONG/SHORT under "scenario", we need a different strategy or accept them as different "scenarios" in the report.
    # Let's use sim__<scenario>_<direction>.json for now and see if the audit script handles it.
    # Actually, better: modify sim_extraction_audit.py to handle <scenario>_<direction> if needed, 
    # OR: just accept that "scenario_LONG" is the scenario name in the report.
    
    # Let's try to match the naming convention that allows grouping if possible.
    # But for now, let's just make it compatible with the splitter.
    out_file = os.path.join(OUTPUT_DIR, f"run_1__{base_name}_{direction}.json")
    
    pair = get_instrument_from_csv(scenario_path)

    cmd = [
        sys.executable,
        HARNESS_SCRIPT,
        "--ticks", scenario_path,
        "--pair", pair,
        "--direction", direction,
        "--atr-pips", "10",
        "--tp-atr", "100.0",
        "--bucket-sec", "0.1",
        "--out", out_file
    ]
    
    try:
        # Run and capture output to avoid cluttering specific output
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"FAIL: {base_name} {direction} - Return code {result.returncode}")
            print(result.stderr)
            return None
            
        with open(out_file, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"ERROR: {base_name} {direction} - {e}")
        return None

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    csv_files = glob.glob(os.path.join(SCENARIO_DIR, "*.csv"))
    csv_files.sort()
    
    results = []
    
    print(f"{'Scenario':<30} | {'Dir':<5} | {'Pips':<8} | {'Reason':<40} | {'Ticks':<6}")
    print("-" * 100)
    
    for csv_file in csv_files:
        for direction in ["LONG", "SHORT"]:
            res = run_simulation(csv_file, direction)
            if res:
                pips = res.get("weighted_pips", 0.0)
                exit_info = res.get("exit", {})
                reason = exit_info.get("reason", "N/A") if exit_info else "NO_EXIT"
                ticks = res.get("n_ticks", 0)
                
                print(f"{os.path.basename(csv_file):<30} | {direction:<5} | {pips:8.2f} | {reason:<40} | {ticks:<6}")
                
                results.append({
                    "scenario": os.path.basename(csv_file),
                    "direction": direction,
                    "pips": pips,
                    "reason": reason,
                    "ticks": ticks
                })
            else:
                print(f"{os.path.basename(csv_file):<30} | {direction:<5} | {'ERR':>8} | {'ERROR':<40} | {'-':<6}")

if __name__ == "__main__":
    main()
