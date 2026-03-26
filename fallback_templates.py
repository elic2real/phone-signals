#!/usr/bin/env python3
"""
Fallback Templates
Provides targeted configurations from research mapping when compiled data is unavailable
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from state_key import compute_session, compute_quarter
from quarter_mapping_extractor import QuarterMappingExtractor

logger = logging.getLogger(__name__)

class FallbackTemplates:
    """Provides fallback configurations using real research mapping data"""
    
    def __init__(self):
        self.extractor = QuarterMappingExtractor()
        
    def get_quarter_fallback(self, pair: str, ts: float) -> Dict[str, Any]:
        """Get quarter-specific configuration from research mapping"""
        # Try to get real quarter config from research mapping
        config = self.extractor.get_quarter_config(pair, ts)
        
        if config:
            logger.info(f"Using research mapping fallback for {pair}")
            return config
            
        # If no mapping found, use conservative fallback
        logger.warning(f"No research mapping found for {pair}, using conservative fallback")
        return self.get_conservative_config()
        
    @classmethod
    def get_conservative_config(cls) -> Dict[str, Any]:
        """Get the most conservative configuration (emergency fallback)"""
        return {
            "entry_filters": {
                "max_spread_pips": 1.0,
                "min_volatility": 0.7,
                "max_atr_distance": 1.5
            },
            "management": {
                "panic_multiplier": 0.4,
                "stall_timeout_minutes": 5,
                "runner_extension": False,
                "trailing_stop_enabled": False
            },
            "position_sizing": {
                "max_risk_percent": 0.5,
                "max_positions_per_pair": 1
            },
            "targets": {
                "default_target_atr": 1.2,
                "partial_targets_enabled": False
            },
            "source": "emergency_conservative",
            "description": "Emergency conservative configuration"
        }
