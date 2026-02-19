import sys
import json
import os
import math

def read_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def fail(msg, mismatches):
    print("FAIL: " + msg)
    for m in mismatches[:5]:
        print("  MISMATCH:", m)
    sys.exit(1)

def pass_msg():
    print("PASS: All accounting invariants satisfied.")
    sys.exit(0)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--audit', required=True)
    parser.add_argument('--dir', required=True)
    args = parser.parse_args()

    audit = read_json(args.audit)
    runs = audit.get('runs', [])
    mismatches = []
    epsilon = 1e-6

    # 1. weighted_pips == core_weight*core_pips + runner_weight*runner_pips
    for i, r in enumerate(runs):
        core_weight = r.get('core_weight', 0.8)
        runner_weight = r.get('runner_weight', 0.2)
        expected = core_weight * r['core_pips'] + runner_weight * r['runner_pips']
        if abs(r['weighted_pips'] - expected) > epsilon:
            mismatches.append({'trade_id': r.get('file'), 'weighted_pips': r['weighted_pips'], 'expected': expected})
    if mismatches:
        fail('weighted_pips != core_weight*core_pips + runner_weight*runner_pips', mismatches)

    # 2. pips from entry/exit prices consistent with direction
    for r in runs:
        entry = r['entry_price']
        exit = r['exit_price']
        direction = r['direction']
        pip = 0.01 if r['instrument'].endswith('_JPY') else 0.0001
        if direction == 'LONG':
            expected_pips = (exit - entry) / pip
        else:
            expected_pips = (entry - exit) / pip
        if abs(r['exit_pips'] - expected_pips) > 1e-3:
            mismatches.append({'trade_id': r.get('file'), 'exit_pips': r['exit_pips'], 'expected': expected_pips})
    if mismatches:
        fail('exit_pips inconsistent with entry/exit prices', mismatches)

    # 3. total_market_seconds_all_legs must match sum of replay wall times
    total_core = sum(r['core_hold_sec'] for r in runs)
    total_runner = sum(r['runner_hold_sec'] for r in runs)
    total_all = sum(r['hold_sec'] for r in runs)
    market_time = sum(r.get('replay_wall_time_sec', 0.0) for r in runs)
    reported_market_time = audit.get('total_market_seconds_all_legs')
    if abs(reported_market_time - market_time) > 1e-3:
        mismatches.append({'reported_market_time': reported_market_time, 'expected_market_time': market_time})
    # For transparency, print both sums (not an invariant)
    print(f"[INFO] total_hold_sec (sum of all legs): {total_all}")
    print(f"[INFO] total_market_seconds_all_legs (true market time): {reported_market_time}")
    if mismatches:
        fail('total_market_seconds_all_legs != market_time', mismatches)

    # 4. pips_per_hour uses honest denominator (sum of replay wall times)
    scorecard = audit.get('AEE_SCORECARD', {})
    pph = scorecard.get('weighted_pips_per_hour')
    if market_time > 0:
        expected_pph = sum(r['weighted_pips'] for r in runs) / (market_time / 3600.0)
    else:
        expected_pph = 0.0
    if abs(pph - expected_pph) > 1e-3:
        mismatches.append({'scorecard_pph': pph, 'expected_pph': expected_pph})
    if mismatches:
        fail('pips_per_hour denominator not honest', mismatches)

    pass_msg()

if __name__ == '__main__':
    main()
