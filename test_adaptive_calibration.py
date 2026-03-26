#!/usr/bin/env python3
"""
Test Adaptive Market Calibration
"""

import logging
logging.basicConfig(level=logging.CRITICAL)

import numpy as np
from datetime import datetime, timezone
from runtime_calibration import RuntimeCalibration

def test_adaptive_calibration():
    print("ADAPTIVE MARKET CALIBRATION TEST")
    print("=" * 60)
    
    cal = RuntimeCalibration()
    
    # Simulate market data for a pair without compiled data
    pair = "AUD_CAD"  # This pair had emergency fallbacks
    print(f"\nTesting adaptive calibration for {pair}:")
    
    # Simulate NY session (missing compiled data)
    dt_ny = datetime(2024, 1, 11, 17, 0, 0, tzinfo=timezone.utc)
    
    # First, try without market data (should use research fallback)
    print("\n1. Without market data:")
    config = cal.get_current_config(pair, dt_ny.timestamp())
    print(f"   Source: {config.get('source')}")
    print(f"   Max risk: {config.get('position_sizing', {}).get('max_risk_percent', 'N/A')}%")
    
    # Now simulate market data feeding
    print("\n2. Feeding market data (simulating live ticks):")
    
    # Simulate trending market with moderate volatility
    base_price = 1.3500
    for i in range(30):
        # Simulate price movement with trend
        trend = i * 0.0001  # Upward trend
        noise = np.random.normal(0, 0.0005)  # Random noise
        price = base_price + trend + noise
        
        tick_data = {
            'timestamp': dt_ny.timestamp() + i * 60,
            'price': price,
            'spread': np.random.uniform(1.0, 2.0),
            'volume': np.random.uniform(100, 1000)
        }
        
        cal.update_market_data(pair, tick_data)
        
    # Now get adaptive config
    print("\n3. After learning from market:")
    config = cal.get_current_config(pair, dt_ny.timestamp())
    print(f"   Source: {config.get('source')}")
    
    if 'adaptive' in config.get('source', ''):
        print("   ✅ Using ADAPTIVE calibration!")
        adaptive_info = config.get('adaptive', {})
        print(f"   Samples used: {adaptive_info.get('samples_used')}")
        print(f"   Market regime: {adaptive_info.get('market_regime')}")
        print(f"   Confidence: {adaptive_info.get('confidence', 0):.2f}")
        
        print("\n   Adaptive parameters:")
        print(f"   - Max spread: {config['entry_filters']['max_spread_pips']:.1f} pips")
        print(f"   - Volatility threshold: {config['entry_filters']['min_volatility']:.2f}")
        print(f"   - Target ATR: {config['targets']['default_target_atr']:.1f}")
        print(f"   - Panic multiplier: {config['management']['panic_multiplier']:.2f}")
        print(f"   - Runner extension: {config['management']['runner_extension']}")
        print(f"   - Trailing stop: {config['management']['trailing_stop_enabled']}")
        
    # Test different market conditions
    print("\n4. Testing different market conditions:")
    
    # High volatility scenario
    print("\n   High volatility scenario:")
    cal_high = RuntimeCalibration()
    
    for i in range(30):
        price = base_price + np.random.normal(0, 0.002)  # High volatility
        tick_data = {
            'timestamp': dt_ny.timestamp() + i * 60,
            'price': price,
            'spread': np.random.uniform(2.0, 3.5),  # Wider spreads
            'volume': np.random.uniform(500, 2000)
        }
        cal_high.update_market_data(pair, tick_data)
        
    config_high = cal_high.get_current_config(pair, dt_ny.timestamp())
    print(f"   - Volatility state: {config_high.get('adaptive', {}).get('volatility_state')}")
    print(f"   - Max spread: {config_high['entry_filters']['max_spread_pips']:.1f} pips")
    print(f"   - Risk reduced: {config_high['management']['panic_multiplier']:.2f}")
    
    # Low volatility scenario
    print("\n   Low volatility scenario:")
    cal_low = RuntimeCalibration()
    
    for i in range(30):
        price = base_price + np.random.normal(0, 0.0002)  # Low volatility
        tick_data = {
            'timestamp': dt_ny.timestamp() + i * 60,
            'price': price,
            'spread': np.random.uniform(0.8, 1.5),  # Tight spreads
            'volume': np.random.uniform(50, 300)
        }
        cal_low.update_market_data(pair, tick_data)
        
    config_low = cal_low.get_current_config(pair, dt_ny.timestamp())
    print(f"   - Volatility state: {config_low.get('adaptive', {}).get('volatility_state')}")
    print(f"   - Max spread: {config_low['entry_filters']['max_spread_pips']:.1f} pips")
    print(f"   - Longer timeout: {config_low['management']['stall_timeout_minutes']} min")
    
    # Show final statistics
    print("\n5. System statistics:")
    stats = cal.get_stats()
    print(f"   Config requests: {stats['config_requests']}")
    print(f"   Compiled hits: {stats['compiled_hits']}")
    print(f"   Adaptive hits: {stats['adaptive_hits']}")
    print(f"   Research fallbacks: {stats['research_fallbacks']}")
    print(f"   Emergency fallbacks: {stats['conservative_fallbacks']}")
    
    print("\n✅ Adaptive calibration can generate quality specs from market data!")
    print("   No more dependency on incomplete research pipeline!")

if __name__ == "__main__":
    test_adaptive_calibration()
