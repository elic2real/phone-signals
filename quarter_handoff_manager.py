#!/usr/bin/env python3
"""
Quarter Handoff Manager
Detects quarter changes and ensures all trades use current quarter rules
"""

import logging
from typing import Dict, Tuple
from datetime import datetime, timezone

from state_key import compute_dow, compute_session, compute_quarter

logger = logging.getLogger(__name__)

# Day name mapping
DAY_MAPPING = {
    "Mon": "monday",
    "Tue": "tuesday", 
    "Wed": "wednesday",
    "Thu": "thursday",
    "Fri": "friday",
    "Sat": "saturday",
    "Sun": "sunday"
}

class QuarterHandoffManager:
    """Simple quarter change detection and logging"""
    
    def __init__(self):
        # Track last seen (weekday, session, quarter) per pair
        self._last_state: Dict[str, Tuple[str, str, str]] = {}
        
    def check_handoff(self, pair: str, ts: float) -> bool:
        """
        Check if quarter/session changed for this pair
        Returns True if this is a new quarter/session
        """
        weekday = DAY_MAPPING.get(compute_dow(ts), compute_dow(ts).lower())
        session = compute_session(ts)
        quarter = compute_quarter(ts, session)
        
        current_state = (weekday, session, quarter)
        last_state = self._last_state.get(pair)
        
        if last_state != current_state:
            self._last_state[pair] = current_state
            
            if last_state:
                # This is a handoff
                logger.info(f"QUARTER HANDOFF: {pair} {last_state} -> {current_state}")
                logger.info(f"All {pair} trades now using {session} {quarter} rules")
                return True
            else:
                # First time seeing this pair
                logger.debug(f"Initial state for {pair}: {current_state}")
                
        return False
        
    def get_current_state(self, pair: str, ts: float) -> Tuple[str, str, str]:
        """Get current (weekday, session, quarter) for a pair"""
        weekday = DAY_MAPPING.get(compute_dow(ts), compute_dow(ts).lower())
        session = compute_session(ts)
        quarter = compute_quarter(ts, session)
        return (weekday, session, quarter)
        
    def is_in_new_session(self, pair: str, ts: float) -> bool:
        """Check if we're in a new session (not just quarter)"""
        current_state = self.get_current_state(pair, ts)
        last_state = self._last_state.get(pair)
        
        if not last_state:
            return True
            
        # Check if session changed (not just quarter)
        return current_state[1] != last_state[1]
