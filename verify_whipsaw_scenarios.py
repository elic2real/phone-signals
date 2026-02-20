import csv
import json
import subprocess
import os
import time

# Constants
ATR = 0.0010  # 10 pips
TP_ATR = 1.2
TP_DIST = ATR * TP_ATR  # 12 pips
ANCHOR_PRICE = 1.0850 + TP_DIST  # 1.0862
ENTRY_PRICE = 1.0850
PAIR = "EUR_USD"

def generate_ticks(filename, scenario):
    ticks = []
    base_time = 1640000000.0
    
    # Warmup
    price = ENTRY_PRICE
    for i in range(180):
        t = base_time + i
        p = price + (0.0001 if i % 2 == 0 else -0.0001)
        ticks.append([PAIR, t, p-0.0001, p+0.0001])
    
    current_time = base_time + 180
    
    if scenario == "OVERSHOOT":
        # Burst: 20 pips in 10 seconds (Speed ~2.0)
        # Consistent high speed
        for i in range(1, 11):
            current_time += 1.0
            price = ENTRY_PRICE + (i * 0.0002) 
            ticks.append([PAIR, current_time, price-0.0001, price+0.0001])
            
        peak = price # 1.0870 (+20 pips)
        
        # Hover high for metrics to catch up
        for i in range(20):
            current_time += 1.0
            price = peak
            ticks.append([PAIR, current_time, price-0.0001, price+0.0001])
            
        # Reversal
        for i in range(30):
            current_time += 1.0
            # Drop 3 pips
            price = peak - (0.0001 * i)
            ticks.append([PAIR, current_time, price-0.0001, price+0.0001])

    elif scenario == "STALL":
        # Slow climb: 10 pips in 40 seconds (Speed ~0.25)
        for i in range(1, 41):
            current_time += 1.0
            price = ENTRY_PRICE + (i * 0.000025)
            ticks.append([PAIR, current_time, price-0.0001, price+0.0001])
            
        peak = price # 1.0860 (+10 pips)
        
        # Stall for 60 seconds (trigger time decay)
        for i in range(60):
            current_time += 1.0
            price = peak - 0.0001 # constant slight pullback
            ticks.append([PAIR, current_time, price-0.0001, price+0.0001])

    # Extension to cleanup
    for i in range(60):
        current_time += 1.0
        ticks.append([PAIR, current_time, price-0.0001, price+0.0001])

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["instrument", "ts", "bid", "ask"])
        for inst, t, b, a in ticks:
            writer.writerow([inst, t, b, a])

def run_test(scenario, filename):
    outfile = f"result_{scenario}.json"
    if os.path.exists(outfile):
        os.remove(outfile)
        
    cmd = [
        "python3", "sim_harness.py",
        "--ticks", filename,
        "--pair", "EUR_USD",
        "--direction", "LONG",
        "--atr-pips", "10",
        "--tp-atr", "1.2",
        "--bucket-sec", "1",
        "--out", outfile
    ]
    
    result = subprocess.run(cmd, capture_output=True)
    if os.path.exists(outfile):
        try:
            with open(outfile, 'r') as f:
                data = json.load(f)
                exit_info = data.get("exit", {})
                return exit_info.get("reason", "NO_EXIT"), float(exit_info.get("price", 0.0))
        except:
             pass
    return "FAIL_OR_TIMEOUT", 0.0

generate_ticks("whipsaw_overshoot.csv", "OVERSHOOT")
generate_ticks("whipsaw_stall.csv", "STALL")

r1, p1 = run_test("OVERSHOOT", "whipsaw_overshoot.csv")
r2, p2 = run_test("STALL", "whipsaw_stall.csv")

print(f"OVERSHOOT: {r1} (Price: {p1}) -> {'SUCCESS' if 'OVERSHOOT' in r1 else 'FAIL'}")
print(f"STALL: {r2} (Price: {p2}) -> {'SUCCESS' if 'REJECTION' in r2 else 'FAIL'}")
