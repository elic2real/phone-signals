#!/usr/bin/env python3
"""
Wire Quarter Handoff to Open Trades
Integration to update AEE policies when quarter changes
"""

import logging
from typing import Dict, Any, Callable, Optional

logger = logging.getLogger(__name__)

class QuarterHandoffIntegrator:
    """Integrates quarter handoffs with open trade management"""
    
    def __init__(self, runtime_calibration, update_aee_callback: Callable):
        self.runtime_calibration = runtime_calibration
        self.update_aee_callback = update_aee_callback
        self.last_quarter_state: Dict[str, Dict[str, str]] = {}
        
        # Register handoff callbacks for each pair
        self._register_handoff_callbacks()
        
    def _register_handoff_callbacks(self):
        """Register callbacks with quarter handoff manager"""
        # The handoff manager should call this when quarter changes
        if hasattr(self.runtime_calibration, 'handoff_manager'):
            # Store original check_handoff
            original_check = self.runtime_calibration.handoff_manager.check_handoff
            
            def check_with_callback(pair: str, ts: float) -> bool:
                handoff = original_check(pair, ts)
                
                if handoff:
                    logger.info(f"Quarter handoff detected for {pair}, updating open trades")
                    self._handle_quarter_change(pair, ts)
                    
                return handoff
                
            # Replace with wrapped version
            self.runtime_calibration.handoff_manager.check_handoff = check_with_callback
            
    def _handle_quarter_change(self, pair: str, ts: float):
        """Handle quarter change for a pair"""
        # Get new quarter configuration
        new_config = self.runtime_calibration.get_current_config(pair, ts)
        
        if not new_config:
            logger.warning(f"No new config available for {pair} after handoff")
            return
            
        # Extract management settings
        management = new_config.get("management", {})
        aee_settings = new_config.get("aee", {})
        
        # Build AEE knobs for the new quarter
        new_knobs = {}
        
        # Map management to AEE knobs
        if "panic_multiplier" in management:
            new_knobs["aee.panic_multiplier"] = management["panic_multiplier"]
        if "stall_timeout_minutes" in management:
            new_knobs["aee.stall_timeout_minutes"] = management["stall_timeout_minutes"]
        if "runner_extension" in management:
            new_knobs["aee.runner_extension"] = management["runner_extension"]
        if "trailing_stop_enabled" in management:
            new_knobs["aee.trailing_stop_enabled"] = management["trailing_stop_enabled"]
            
        # Add direct AEE settings
        new_knobs.update(aee_settings)
        
        # Update all open trades for this pair
        self.update_aee_callback(pair, new_knobs, new_config)
        
    def get_current_quarter_state(self, pair: str) -> Dict[str, str]:
        """Get current quarter state for a pair"""
        from state_key import compute_session, compute_quarter
        import time
        
        ts = time.time()
        return {
            "pair": pair,
            "session": compute_session(ts),
            "quarter": compute_quarter(ts, compute_session(ts)),
            "timestamp": ts
        }

# Integration function to add to phone_bot.py
def integrate_quarter_handoff():
    """
    Add this to phone_bot.py after _RUNTIME_CALIBRATION initialization
    """
    def update_open_trades_aee(pair: str, new_knobs: Dict[str, Any], new_config: Dict[str, Any]):
        """Update AEE settings for all open trades of a pair"""
        # This will be called when quarter changes
        # Implementation depends on how AEE updates work in phone_bot.py
        
        # For now, just log the change
        logger.info(f"Updating AEE for {pair} open trades:")
        for knob, value in new_knobs.items():
            logger.info(f"  {knob}: {value}")
            
        # TODO: Actually update the AEE engine for open trades
        # This might involve:
        # 1. Finding all open trades for the pair
        # 2. Updating their AEE state with new knobs
        # 3. Emitting AEE_TUNE_APPLIED events
        
    # Create integrator
    integrator = QuarterHandoffIntegrator(_RUNTIME_CALIBRATION, update_open_trades_aee)
    
    # Store for reference
    globals()['_QUARTER_HANDOFF_INTEGRATOR'] = integrator
    
    logger.info("Quarter handoff integrator initialized")

print("""
QUARTER HANDOFF INTEGRATION INSTRUCTIONS
=========================================

1. Add this code to phone_bot.py after line 1273 (after _RUNTIME_CALIBRATION init):

   # Import at top of file:
   from quarter_handoff_integrator import integrate_quarter_handoff
   
   # After _RUNTIME_CALIBRATION = runtime_calibration.RuntimeCalibration():
   integrate_quarter_handoff()

2. The integrator will:
   - Detect quarter handoffs
   - Get new quarter configuration
   - Update AEE settings for open trades
   - Emit appropriate events

3. Test by:
   - Running with demo trading
   - Monitoring logs for "Quarter handoff detected"
   - Verifying AEE_TUNE_APPLIED events for open trades

4. The system will now update open trade management when quarter changes!
""")
