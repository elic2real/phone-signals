#!/usr/bin/env python3
"""
Runtime Calibration System
Integrates compiled research, adaptive market calibration, and fallbacks
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from compiled_trading_map import CompiledTradingMap
from quarter_handoff_manager import QuarterHandoffManager
from fallback_templates import FallbackTemplates
from adaptive_market_calibration import AdaptiveMarketCalibration

logger = logging.getLogger(__name__)

class RuntimeCalibration:
    """Main interface for runtime calibration with adaptive capabilities"""
    
    def __init__(self):
        self.compiled_map = CompiledTradingMap()
        self.handoff_manager = QuarterHandoffManager()
        self.fallback = FallbackTemplates()
        self.adaptive = AdaptiveMarketCalibration()
        
        # Statistics
        self.stats = {
            "config_requests": 0,
            "compiled_hits": 0,
            "adaptive_hits": 0,
            "research_fallbacks": 0,
            "conservative_fallbacks": 0,
            "handoffs_detected": 0
        }
        
        logger.info("RuntimeCalibration initialized with adaptive capabilities")
        
    def update_market_data(self, pair: str, tick_data: Dict[str, Any]):
        """Update market data for adaptive learning"""
        self.adaptive.update_market_data(pair, tick_data)
        
    def get_current_config(self, pair: str, ts: Optional[float] = None) -> Dict[str, Any]:
        """
        Get the current configuration for a pair
        Priority: Compiled > Adaptive > Research Fallback > Emergency
        """
        if ts is None:
            ts = datetime.now(timezone.utc).timestamp()
            
        self.stats["config_requests"] += 1
        
        # Check for quarter handoff
        if self.handoff_manager.check_handoff(pair, ts):
            self.stats["handoffs_detected"] += 1
            
        # 1. Try compiled map first
        config = self.compiled_map.get_config(pair, ts)
        if config:
            self.stats["compiled_hits"] += 1
            config["source"] = "compiled_map"
            config["resolved_at"] = ts
            return config
            
        # 2. Try adaptive market calibration
        config = self.adaptive.generate_adaptive_config(pair, ts)
        if config and config.get('adaptive', {}).get('confidence', 0) > 0.3:
            self.stats["adaptive_hits"] += 1
            logger.info(f"Using adaptive market calibration for {pair}")
            return config
            
        # 3. Fallback to research mapping
        quarter_config = self.fallback.get_quarter_fallback(pair, ts)
        if quarter_config:
            self.stats["research_fallbacks"] += 1
            logger.warning(f"Using research fallback for {pair}")
            return quarter_config
            
        # 4. Final emergency fallback
        self.stats["conservative_fallbacks"] += 1
        logger.error(f"Using EMERGENCY fallback for {pair} - no configuration available")
        return self.fallback.get_conservative_config()
        
    def is_pair_supported(self, pair: str, ts: Optional[float] = None) -> bool:
        """Check if a pair has compiled calibration available"""
        if ts is None:
            ts = datetime.now(timezone.utc).timestamp()
        return self.compiled_map.is_node_available(pair, ts)
        
    def get_entry_filters(self, pair: str, ts: Optional[float] = None) -> Dict[str, Any]:
        """Get entry filters for current time"""
        config = self.get_current_config(pair, ts)
        return config.get("entry_filters", {})
        
    def get_management_rules(self, pair: str, ts: Optional[float] = None) -> Dict[str, Any]:
        """Get management rules for current time"""
        config = self.get_current_config(pair, ts)
        return config.get("management", {})
        
    def log_current_state(self, pair: str, ts: Optional[float] = None):
        """Log the current calibration state for debugging"""
        if ts is None:
            ts = datetime.now(timezone.utc).timestamp()
            
        weekday = compute_dow(ts)
        session = compute_session(ts)
        quarter = compute_quarter(ts, session)
        
        config = self.get_current_config(pair, ts)
        source = config.get("source", "unknown")
        
        logger.info(f"Calibration state for {pair}:")
        logger.info(f"  Time: {datetime.fromtimestamp(ts, timezone.utc)}")
        logger.info(f"  Session: {weekday} {session} {quarter}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Available: {self.is_pair_supported(pair, ts)}")
        
    def get_stats(self) -> Dict[str, int]:
        """Get calibration usage statistics"""
        return self.stats.copy()
        
    def reset_stats(self):
        """Reset statistics counters"""
        for key in self.stats:
            self.stats[key] = 0
