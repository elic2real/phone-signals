#!/usr/bin/env python3
import subprocess
import json
import os
import sys

SCENARIOS = [
    {
        "name": "Trend Continuation",
        "file": "scenarios/golden/v1.0/trend_continuation.csv",
        "pair": "EUR_USD",
        "tp_atr": 100
    },
    {
        "name": "Panic Reversal",
        "file": "scenarios/golden/v1.0/panic_reversal.csv",
        "pair": "EUR_USD",
        "tp_atr": 100
    },
    {
        "name": "Whipsaw Spike Fade",
        "file": "scenarios/golden/v1.0/whipsaw_spike_fade.csv",
        "pair": "EUR_USD",
        "tp_atr": 100
    },
    {
        "name": "Energy Depletion",
        "file": "scenarios/golden/v1.0/energy_depletion.csv",
        "pair": "EUR_USD",
        "tp_atr": 100
    },
    {
        "name": "High Energy Trend",
        "file": "scenarios/golden/v1.0/high_energy_trend.csv",
        "pair": "EUR_USD",
        "tp_atr": 100
    }
]

def run_scenario(scenario):
    out_file = f"temp_res_{scenario['name'].replace(' ', '_')}.json"
    cmd = [
        "python3", "sim_harness.py",
        "--ticks", scenario['file'],
        "--pair", scenario['pair'],
        "--tp-atr", str(scenario['tp_atr']),
        "--out", out_file
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        with open(out_file, 'r') as f:
            data = json.load(f)
        
        # Calculate duration
        start_ts = data['tick_records'][0]['ts']
        end_ts = data['exit']['ts']
        duration_sec = end_ts - start_ts
        pips = data['exit'].get('total_weighted_pips', 0.0)
        
        # Pips/Hr calculation
        pips_per_hr = (pips / duration_sec) * 3600 if duration_sec > 0 else 0
        
        return {
            "name": scenario['name'],
            "pips": pips,
            "duration": duration_sec,
            "pips_hr": pips_per_hr,
            "exit_reason": data['exit']['reason']
        }
    except Exception as e:
        return {
            "name": scenario['name'],
            "error": str(e)
        }
    finally:
        if os.path.exists(out_file):
            os.remove(out_file)

def main():
    print(f"{'Scenario':<25} | {'Pips':<10} | {'Duration':<10} | {'Pips/Hr':<10} | {'Exit Reason':<30}")
    print("-" * 95)
    
    for s in SCENARIOS:
        res = run_scenario(s)
        if "error" in res:
            print(f"{res['name']:<25} | ERROR: {res['error']}")
        else:
            print(f"{res['name']:<25} | {res['pips']:<10.1f} | {res['duration']:<10.1f} | {res['pips_hr']:<10.0f} | {res['exit_reason']:<30}")

if __name__ == "__main__":
    main()
