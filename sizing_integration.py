#!/usr/bin/env python3
"""
Integrate Simple Sizing Model with Runtime Calibration
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class SizingIntegration:
    """Integrates simple sizing model with runtime calibration"""
    
    def __init__(self, runtime_calibration):
        self.runtime_calibration = runtime_calibration
        from simple_sizing_model import SimpleSizingModel
        self.sizing_model = SimpleSizingModel()
        
    def get_position_size(self, pair: str, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get position size using integrated system"""
        
        # Get base configuration from runtime calibration
        config = self.runtime_calibration.get_current_config(pair, datetime.now(timezone.utc).timestamp())
        
        # Extract risk limits from calibration
        max_risk = config.get('position_sizing', {}).get('max_risk_percent', 2.0)
        
        # Create trade opportunity
        from simple_sizing_model import TradeOpportunity
        
        opportunity = TradeOpportunity(
            pair=pair,
            direction=signal_data.get('direction', 'LONG'),
            signal_strength=signal_data.get('signal_strength', 0.5),
            bias_alignment=signal_data.get('bias_alignment', 0.5),
            trend_strength=signal_data.get('trend_strength', 0.5),
            regime_fit=signal_data.get('regime_fit', 0.5),
            session=signal_data.get('session', 'London'),
            quarter=signal_data.get('quarter', 'Q2'),
            estimated_trade_life=signal_data.get('estimated_hours', 4.0),
            spread_pips=signal_data.get('spread_pips', 2.0),
            expected_move=signal_data.get('expected_move_pips', 30)
        )
        
        # Get sizing from simple model
        sizing = self.sizing_model.size_trade(opportunity)
        
        # Apply calibration cap
        initial_size = min(sizing['initial_size_percent'], max_risk)
        
        # If capped, adjust add-on accordingly
        if initial_size < sizing['initial_size_percent']:
            # Cap reached, no add-on
            add_allowed = False
            max_total = initial_size
        else:
            add_allowed = sizing['add_allowed']
            max_total = min(sizing['max_total_size'], max_risk + 0.5)  # Allow slight exceed for add-on
            
        return {
            'grade': sizing['grade'],
            'priority_score': sizing['priority_score'],
            'initial_size_percent': initial_size,
            'max_risk_percent': max_risk,
            'add_allowed': add_allowed,
            'add_on_size_percent': 0.5 if add_allowed else 0,
            'max_total_size': max_total,
            'calibration_source': config.get('source', 'unknown'),
            'sizing_model': 'SimpleSizingModel'
        }
        
    def check_add_on_conditions(self, trade_id: str, current_state: Dict[str, Any]) -> bool:
        """Check if add-on should be triggered"""
        return self.sizing_model.should_add_on(current_state)
        
    def check_weak_reduction(self, trade_id: str, current_state: Dict[str, Any]) -> Optional[float]:
        """Check if weak trade should be reduced"""
        return self.sizing_model.should_reduce_weak(current_state)

def create_integration_demo():
    """Demonstrate the integration"""
    print("=" * 70)
    print("SIZING INTEGRATION DEMO")
    print("=" * 70)
    
    # Mock runtime calibration
    class MockRuntimeCalibration:
        def get_current_config(self, pair, ts):
            # Simulate different sources
            if pair == "EUR_USD":
                return {
                    'source': 'compiled_map',
                    'position_sizing': {'max_risk_percent': 2.0}
                }
            elif pair == "GBP_USD":
                return {
                    'source': 'adaptive_base_calibration',
                    'position_sizing': {'max_risk_percent': 0.9}
                }
            else:
                return {
                    'source': 'emergency_conservative',
                    'position_sizing': {'max_risk_percent': 0.5}
                }
    
    # Create integration
    integration = SizingIntegration(MockRuntimeCalibration())
    
    # Test signals
    signals = [
        {
            'pair': 'EUR_USD',
            'direction': 'LONG',
            'signal_strength': 0.9,
            'bias_alignment': 0.8,
            'trend_strength': 0.85,
            'regime_fit': 0.9,
            'spread_pips': 1.2,
            'expected_move_pips': 50
        },
        {
            'pair': 'GBP_USD',
            'direction': 'SHORT',
            'signal_strength': 0.6,
            'bias_alignment': 0.7,
            'trend_strength': 0.5,
            'regime_fit': 0.6,
            'spread_pips': 2.0,
            'expected_move_pips': 30
        },
        {
            'pair': 'AUD_USD',
            'direction': 'LONG',
            'signal_strength': 0.3,
            'bias_alignment': 0.4,
            'trend_strength': 0.3,
            'regime_fit': 0.3,
            'spread_pips': 1.5,
            'expected_move_pips': 20
        }
    ]
    
    print("\nIntegrated Sizing Results:")
    print("-" * 70)
    
    for signal in signals:
        result = integration.get_position_size(signal['pair'], signal)
        
        print(f"\n{signal['pair']} {signal['direction']}:")
        print(f"   Grade: {result['grade']}")
        print(f"   Priority: {result['priority_score']:.3f}")
        print(f"   Initial Size: {result['initial_size_percent']}% NAV")
        print(f"   Max Risk (from cal): {result['max_risk_percent']}%")
        print(f"   Add-on Allowed: {result['add_allowed']}")
        print(f"   Source: {result['calibration_source']}")
        
        if result['initial_size_percent'] < result['max_risk_percent']:
            print(f"   ✅ Using model sizing (under cap)")
        else:
            print(f"   ⚠️  Capped by calibration limit")
            
    print("\n" + "=" * 70)
    print("INTEGRATION BENEFITS")
    print("=" * 70)
    print("\n1. Runtime calibration provides safety caps")
    print("2. Simple model provides clean grading and sizing")
    print("3. Priority scoring prevents weak trades from stealing capital")
    print("4. Add-on logic is simple and mechanical")
    print("5. Weak trade reduction protects capital")
    
    print("\nIntegration is ready for phone_bot.py!")

if __name__ == "__main__":
    create_integration_demo()
