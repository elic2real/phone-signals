#!/usr/bin/env python3
"""Runtime Calibration System with runtime-safe optional dependencies."""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

try:
    from compiled_trading_map import CompiledTradingMap
except Exception:
    CompiledTradingMap = None

try:
    from quarter_handoff_manager import QuarterHandoffManager
except Exception:
    QuarterHandoffManager = None

try:
    from fallback_templates import FallbackTemplates
except Exception:
    FallbackTemplates = None

try:
    from adaptive_market_calibration import AdaptiveMarketCalibration
except Exception:
    AdaptiveMarketCalibration = None

logger = logging.getLogger(__name__)

class RuntimeCalibration:
    """Main interface for runtime calibration with adaptive capabilities"""
    
    def __init__(self):
        self.compiled_map = CompiledTradingMap() if CompiledTradingMap else None
        self.handoff_manager = QuarterHandoffManager() if QuarterHandoffManager else None
        self.fallback = FallbackTemplates() if FallbackTemplates else None
        self.adaptive = AdaptiveMarketCalibration() if AdaptiveMarketCalibration else None
        
        # Statistics
        self.stats = {
            "config_requests": 0,
            "compiled_hits": 0,
            "adaptive_hits": 0,
            "research_fallbacks": 0,
            "conservative_fallbacks": 0,
            "handoffs_detected": 0
        }
        
        logger.info(
            "RuntimeCalibration initialized",
            extra={
                "compiled_map": self.compiled_map is not None,
                "handoff_manager": self.handoff_manager is not None,
                "fallback": self.fallback is not None,
                "adaptive": self.adaptive is not None,
            },
        )
        
    def update_market_data(self, pair: str, tick_data: Dict[str, Any]):
        """Update market data for adaptive learning"""
        if self.adaptive is not None:
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
        if self.handoff_manager is not None and self.handoff_manager.check_handoff(pair, ts):
            self.stats["handoffs_detected"] += 1
            
        # 1. Try compiled map first
        config = self.compiled_map.get_config(pair, ts) if self.compiled_map is not None else None
        if config:
            self.stats["compiled_hits"] += 1
            config["source"] = "compiled_map"
            config["resolved_at"] = ts
            return config
            
        # 2. Try adaptive market calibration
        config = self.adaptive.generate_adaptive_config(pair, ts) if self.adaptive is not None else None
        if config and config.get('adaptive', {}).get('confidence', 0) > 0.3:
            self.stats["adaptive_hits"] += 1
            logger.info(f"Using adaptive market calibration for {pair}")
            return config
            
        # 3. Fallback to research mapping
        quarter_config = self.fallback.get_quarter_fallback(pair, ts) if self.fallback is not None else None
        if quarter_config:
            self.stats["research_fallbacks"] += 1
            logger.warning(f"Using research fallback for {pair}")
            return quarter_config
            
        # 4. Final emergency fallback
        self.stats["conservative_fallbacks"] += 1
        logger.error(f"Using EMERGENCY fallback for {pair} - no configuration available")
        if self.fallback is not None:
            return self.fallback.get_conservative_config()
        return {
            "source": "emergency_fallback_builtin",
            "entry_filters": {},
            "management": {},
            "adaptive": {"confidence": 0.0},
        }
        
    def is_pair_supported(self, pair: str, ts: Optional[float] = None) -> bool:
        """Check if a pair has compiled calibration available"""
        if ts is None:
            ts = datetime.now(timezone.utc).timestamp()
        if self.compiled_map is None:
            return False
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
            
        dt = datetime.fromtimestamp(ts, timezone.utc)
        weekday = dt.strftime("%A")
        hour = dt.hour
        if 8 <= hour < 16:
            session = "LONDON"
        elif 13 <= hour < 21:
            session = "NEW_YORK"
        else:
            session = "ASIA"
        quarter = f"Q{min(4, max(1, (hour // 6) + 1))}"
        
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
