#!/usr/bin/env python3
"""
Adaptive Market Calibration
Generates quality specifications from live market data when research data unavailable
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from collections import deque
import logging

from state_key import compute_session, compute_quarter, compute_dow

logger = logging.getLogger(__name__)

class AdaptiveMarketCalibration:
    """Adaptive calibration that learns from market behavior in real-time"""
    
    def __init__(self, learning_window_minutes: int = 60):
        self.learning_window = learning_window_minutes
        self.market_memory: Dict[str, Dict] = {}
        self.min_samples = 20  # Minimum samples to generate specs
        
        # Adaptive parameters by session/quarter
        self.base_params = {
            "ASIA": {
                "Q1": {"volatility_threshold": 0.4, "target_multiplier": 1.2, "risk_factor": 0.8},
                "Q2": {"volatility_threshold": 0.5, "target_multiplier": 1.3, "risk_factor": 0.9},
                "Q3": {"volatility_threshold": 0.5, "target_multiplier": 1.4, "risk_factor": 1.0},
                "Q4": {"volatility_threshold": 0.6, "target_multiplier": 1.3, "risk_factor": 0.8}
            },
            "LONDON": {
                "Q1": {"volatility_threshold": 0.5, "target_multiplier": 1.4, "risk_factor": 0.9},
                "Q2": {"volatility_threshold": 0.6, "target_multiplier": 1.5, "risk_factor": 1.0},
                "Q3": {"volatility_threshold": 0.6, "target_multiplier": 1.6, "risk_factor": 1.1},
                "Q4": {"volatility_threshold": 0.7, "target_multiplier": 1.5, "risk_factor": 0.9}
            },
            "NY": {
                "Q1": {"volatility_threshold": 0.5, "target_multiplier": 1.3, "risk_factor": 0.9},
                "Q2": {"volatility_threshold": 0.6, "target_multiplier": 1.4, "risk_factor": 1.0},
                "Q3": {"volatility_threshold": 0.6, "target_multiplier": 1.5, "risk_factor": 1.1},
                "Q4": {"volatility_threshold": 0.7, "target_multiplier": 1.4, "risk_factor": 0.9}
            }
        }
        
    def update_market_data(self, pair: str, tick_data: Dict[str, Any]):
        """Update market data for adaptive learning"""
        ts = tick_data.get('timestamp', datetime.now(timezone.utc).timestamp())
        session = compute_session(ts)
        quarter = compute_quarter(ts, session)
        
        key = f"{pair}_{session}_{quarter}"
        
        if key not in self.market_memory:
            self.market_memory[key] = {
                'prices': deque(maxlen=self.min_samples * 2),
                'spreads': deque(maxlen=self.min_samples * 2),
                'volumes': deque(maxlen=self.min_samples * 2),
                'movements': deque(maxlen=self.min_samples * 2),
                'last_update': ts
            }
            
        memory = self.market_memory[key]
        
        # Store market data
        if 'price' in tick_data:
            memory['prices'].append(tick_data['price'])
        if 'spread' in tick_data:
            memory['spreads'].append(tick_data['spread'])
        if 'volume' in tick_data:
            memory['volumes'].append(tick_data['volume'])
            
        # Calculate price movements
        if len(memory['prices']) >= 2:
            movement = (memory['prices'][-1] - memory['prices'][-2]) / memory['prices'][-2]
            memory['movements'].append(movement)
            
        memory['last_update'] = ts
        
    def generate_adaptive_config(self, pair: str, ts: float) -> Optional[Dict[str, Any]]:
        """Generate adaptive configuration based on recent market behavior"""
        session = compute_session(ts)
        quarter = compute_quarter(ts, session)
        key = f"{pair}_{session}_{quarter}"
        
        if key not in self.market_memory:
            return self._get_base_adaptive_config(session, quarter)
            
        memory = self.market_memory[key]
        
        # Check if we have enough data
        if len(memory['prices']) < self.min_samples:
            logger.info(f"Insufficient data for {pair} {session} {quarter}, using base adaptive")
            return self._get_base_adaptive_config(session, quarter)
            
        # Analyze market conditions
        market_analysis = self._analyze_market_conditions(memory)
        
        # Generate adaptive specifications
        config = self._build_adaptive_config(pair, session, quarter, market_analysis)
        
        config['source'] = 'adaptive_market_calibration'
        config['adaptive'] = {
            'samples_used': len(memory['prices']),
            'market_regime': market_analysis['regime'],
            'volatility_state': market_analysis['volatility_state'],
            'confidence': market_analysis['confidence']
        }
        
        logger.info(f"Generated adaptive config for {pair} {session} {quarter} - {market_analysis['regime']} regime")
        
        return config
        
    def _analyze_market_conditions(self, memory: Dict) -> Dict[str, Any]:
        """Analyze current market conditions from stored data"""
        prices = list(memory['prices'])
        spreads = list(memory['spreads'])
        movements = list(memory['movements'])
        
        # Calculate volatility
        if len(movements) >= 10:
            volatility = np.std(movements) * np.sqrt(1440)  # Daily volatility
        else:
            volatility = 0.001
            
        # Determine market regime
        if len(prices) >= 20:
            # Simple trend detection
            recent_prices = prices[-10:]
            earlier_prices = prices[-20:-10]
            
            if np.mean(recent_prices) > np.mean(earlier_prices) * 1.001:
                regime = "trending_up"
            elif np.mean(recent_prices) < np.mean(earlier_prices) * 0.999:
                regime = "trending_down"
            else:
                regime = "ranging"
        else:
            regime = "insufficient_data"
            
        # Volatility state
        if volatility < 0.005:
            volatility_state = "low"
        elif volatility < 0.015:
            volatility_state = "normal"
        else:
            volatility_state = "high"
            
        # Confidence based on sample size and stability
        confidence = min(len(prices) / 50, 1.0)  # More samples = more confidence
        if volatility_state == "high":
            confidence *= 0.8  # Less confident in high volatility
            
        return {
            'volatility': volatility,
            'avg_spread': np.mean(spreads) if spreads else 0,
            'regime': regime,
            'volatility_state': volatility_state,
            'confidence': confidence
        }
        
    def _build_adaptive_config(self, pair: str, session: str, quarter: str, analysis: Dict) -> Dict[str, Any]:
        """Build configuration based on market analysis"""
        base = self.base_params.get(session, self.base_params["LONDON"]).get(quarter, {})
        
        # Adapt based on market conditions
        volatility_mult = 1.0
        if analysis['volatility_state'] == "low":
            volatility_mult = 0.8
        elif analysis['volatility_state'] == "high":
            volatility_mult = 1.3
            
        # Adjust targets based on regime
        target_adjustment = 1.0
        if analysis['regime'] == "trending_up":
            target_adjustment = 1.1
        elif analysis['regime'] == "trending_down":
            target_adjustment = 0.9
            
        # Build config
        config = {
            "pair": pair,
            "session": session.lower(),
            "quarter": quarter,
            
            "entry_filters": {
                "max_spread_pips": base.get("risk_factor", 1.0) * 2.0 * volatility_mult,
                "min_volatility": base.get("volatility_threshold", 0.5) * volatility_mult,
                "max_atr_distance": 2.5 * volatility_mult,
                "regime_filter": analysis['regime']
            },
            
            "management": {
                "panic_multiplier": base.get("risk_factor", 1.0) * (0.8 if analysis['volatility_state'] == "high" else 1.0),
                "stall_timeout_minutes": int(10 * (1.5 if analysis['volatility_state'] == "low" else 1.0)),
                "runner_extension": analysis['regime'] in ["trending_up", "trending_down"],
                "trailing_stop_enabled": analysis['regime'] == "trending_up" or quarter == "Q4",
                "adaptive_scaling": True
            },
            
            "position_sizing": {
                "max_risk_percent": base.get("risk_factor", 1.0) * (0.8 if analysis['volatility_state'] == "high" else 1.0),
                "max_positions_per_pair": 1,
                "volatility_adjusted": True
            },
            
            "targets": {
                "default_target_atr": base.get("target_multiplier", 1.5) * target_adjustment,
                "partial_targets_enabled": quarter == "Q4" or analysis['volatility_state'] == "high",
                "adaptive_targets": True
            },
            
            "aee": {
                "aee.strictness_mult": 0.9 if analysis['volatility_state'] == "high" else 1.0,
                "promote_mfe_atr": 0.2 if analysis['regime'] == "trending_up" else 0.1,
                "extension_allow_energy_min": 0.8 if analysis['volatility_state'] == "low" else 0.9
            }
        }
        
        return config
        
    def _get_base_adaptive_config(self, session: str, quarter: str) -> Dict[str, Any]:
        """Get base adaptive config when no market data available"""
        base = self.base_params.get(session, self.base_params["LONDON"]).get(quarter, {})
        
        return {
            "pair": "unknown",
            "session": session.lower(),
            "quarter": quarter,
            
            "entry_filters": {
                "max_spread_pips": 2.0,
                "min_volatility": base.get("volatility_threshold", 0.5),
                "max_atr_distance": 2.0
            },
            
            "management": {
                "panic_multiplier": base.get("risk_factor", 1.0),
                "stall_timeout_minutes": 10,
                "runner_extension": False,
                "trailing_stop_enabled": quarter == "Q4"
            },
            
            "position_sizing": {
                "max_risk_percent": base.get("risk_factor", 1.0),
                "max_positions_per_pair": 1
            },
            
            "targets": {
                "default_target_atr": base.get("target_multiplier", 1.5),
                "partial_targets_enabled": False
            },
            
            "source": "adaptive_base_calibration",
            "adaptive": {
                "samples_used": 0,
                "market_regime": "unknown",
                "confidence": 0.5
            }
        }
