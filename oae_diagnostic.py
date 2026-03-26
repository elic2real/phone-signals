#!/usr/bin/env python3
"""
OAE Diagnostic: Check zone classification sanity

Count zones where MFE ≥ 2.5 AND MAE ≤ 2.5
This should produce hundreds of GOOD zones if discovery engine is correct.
"""

import json

def main():
    # Load phase 2 analysis
    with open('phase2_analysis.json', 'r') as f:
        data = json.load(f)

    opportunities = data['opportunities']
    print(f"Total opportunities: {len(opportunities)}")

    # Count basic sanity check: MFE ≥ 2.5 AND MAE ≤ 2.5
    sanity_good = 0
    current_good = 0
    current_bad = 0
    current_noise = 0

    for opp in opportunities:
        mfe = opp['mfe_pips']
        mae = opp['mae_pips']
        bucket = opp['bucket']

        # Sanity check: pure opportunity definition
        if mfe >= 2.5 and mae <= 2.5:
            sanity_good += 1

        # Current classification
        if bucket == 'good':
            current_good += 1
        elif bucket == 'bad':
            current_bad += 1
        elif bucket == 'noise':
            current_noise += 1

    print("""
CURRENT CLASSIFICATION:""")
    print(f"GOOD: {current_good} ({current_good/len(opportunities)*100:.1f}%)")
    print(f"BAD: {current_bad} ({current_bad/len(opportunities)*100:.1f}%)")
    print(f"NOISE: {current_noise} ({current_noise/len(opportunities)*100:.1f}%)")

    print("""
SANITY CHECK (MFE ≥ 2.5 AND MAE ≤ 2.5):""")
    print(f"Pure GOOD zones: {sanity_good} ({sanity_good/len(opportunities)*100:.1f}%)")

    if sanity_good > 100:
        print("✅ DISCOVERY ENGINE WORKING: Produces reasonable GOOD zone count")
    else:
        print("❌ DISCOVERY ENGINE PROBLEM: Too few basic opportunities")

    target_good_range = (len(opportunities) * 0.20, len(opportunities) * 0.40)
    if sanity_good >= target_good_range[0] and sanity_good <= target_good_range[1]:
        print(f"✅ BALANCED: {sanity_good} GOOD zones in target range {target_good_range[0]:.0f}-{target_good_range[1]:.0f}")
    else:
        print(f"⚠️  IMBALANCED: Need {target_good_range[0]:.0f}-{target_good_range[1]:.0f} GOOD zones, got {sanity_good}")

if __name__ == "__main__":
    main()
