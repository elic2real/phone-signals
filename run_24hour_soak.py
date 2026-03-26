#!/usr/bin/env python3
"""
Wrapper script to run phone_bot in 24-hour soak mode
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from phone_bot import main

if __name__ == "__main__":
    # 24 hours = 24 * 60 * 60 = 86400 seconds
    run_for_24_hours = 86400
    
    print(f"🚀 Starting phone_bot in 24-hour soak mode...")
    print(f"⏰ Runtime: {run_for_24_hours} seconds (24 hours)")
    print(f"📊 Trading mode: LIVE (actual trades will be executed)")
    print("=" * 60)
    
    # Run for 24 hours with live trading
    main(run_for_sec=run_for_24_hours, dry_run=False)
