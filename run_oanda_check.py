#!/usr/bin/env python3
"""
Run the bot to check if OANDA data updates over time
"""

import sys
import os
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from phone_bot import main

if __name__ == "__main__":
    # Run for 10 minutes to check for data updates
    run_time = 600  # 10 minutes
    
    print(f"🤖 Running bot for 10 minutes to check OANDA data updates...")
    print(f"⏰ Runtime: {run_time} seconds (10 minutes)")
    print(f"📊 Will check if pricing timestamps update")
    print("=" * 60)
    
    # Run with dry_run=True to avoid trades while testing
    main(run_for_sec=run_time, dry_run=True)
