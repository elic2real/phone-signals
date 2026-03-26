#!/usr/bin/env python3
"""
Quick proof test to verify pricing stream is working
"""

import sys
import os
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from phone_bot import main

if __name__ == "__main__":
    # Run for 2 minutes to verify pricing stream works
    run_time = 120  # 2 minutes
    
    print(f"🧪 Running 2-minute proof test for pricing stream...")
    print(f"⏰ Runtime: {run_time} seconds")
    print(f"📊 Looking for: PRICING_STREAM_STARTED, PRICE_UPDATE events")
    print("=" * 60)
    
    # Run with live trading enabled for real test
    main(run_for_sec=run_time, dry_run=False)
