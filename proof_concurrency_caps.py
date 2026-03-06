#!/usr/bin/env python3
"""
Proof validation for concurrency caps implementation.
Simulates trading scenarios to verify caps are enforced correctly.
"""

import os
import sys
import time
import json
from collections import defaultdict
from unittest.mock import MagicMock, patch

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from phone_bot import (
    compute_open_trade_counts,
    _place_order_with_guards,
    _ORDER_TIMESTAMPS,
    _ENTRY_ID_TIMESTAMPS,
    MAX_OPEN_TRADES_GLOBAL,
    MAX_OPEN_TRADES_PER_PAIR,
    MAX_ORDERS_PER_MIN,
    ENTRY_DEDUP_TTL_SEC,
    normalize_pair
)


def setup_mock_broker(open_positions=None, pending_orders=None):
    """Setup a mock broker with specified positions and orders"""
    if open_positions is None:
        open_positions = []
    if pending_orders is None:
        pending_orders = []
    
    mock_oanda = MagicMock()
    mock_oanda.open_positions.return_value = {"positions": open_positions}
    mock_oanda.pending_orders.return_value = {"orders": pending_orders}
    mock_oanda.place_market.return_value = {
        "orderFillTransaction": {
            "orderID": f"order_{int(time.time())}",
            "tradeOpened": {"tradeID": f"trade_{int(time.time())}"},
            "units": "1000",
            "price": "1.1000"
        }
    }
    
    return mock_oanda


def test_global_cap_enforcement():
    """Test that global cap is enforced"""
    print("\n=== Testing Global Cap Enforcement ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    # Setup broker at global cap - 1
    open_positions = []
    for i in range(MAX_OPEN_TRADES_GLOBAL - 1):
        open_positions.append({
            "instrument": f"EUR_USD_{i}",
            "longUnits": "1000",
            "shortUnits": "0"
        })
    
    mock_oanda = setup_mock_broker(open_positions)
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        # First order should succeed
        result1 = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="test_global_1"
        )
        print(f"Order 1 result: {'SUCCESS' if not result1.get('error') else 'BLOCKED - ' + result1.get('reason')}")
        
        # Now simulate the broker having one more position (at global cap)
        open_positions.append({
            "instrument": "GBP_JPY",
            "longUnits": "1000",
            "shortUnits": "0"
        })
        mock_oanda.open_positions.return_value = {"positions": open_positions}
        
        # Second order should be blocked (at global cap)
        result2 = _place_order_with_guards(
            pair="GBP_JPY",
            units=1000,
            order_type="MARKET",
            entry_id="test_global_2"
        )
        print(f"Order 2 result: {'SUCCESS' if not result2.get('error') else 'BLOCKED - ' + result2.get('reason')}")
        
        # Verify
        assert not result1.get("error"), "First order should succeed"
        assert result2.get("error") is True, "Second order should have error=True"
        assert result2.get("reason") == "GLOBAL_CONCURRENCY_CAP", f"Second order should be blocked by global cap, but got reason: {result2.get('reason')}"
        
        print("✓ Global cap enforcement verified")


def test_pair_cap_enforcement():
    """Test that per-pair cap is enforced"""
    print("\n=== Testing Per-Pair Cap Enforcement ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    # Setup broker with EUR_USD at pair cap
    open_positions = []
    for i in range(MAX_OPEN_TRADES_PER_PAIR):
        open_positions.append({
            "instrument": "EUR_USD",
            "longUnits": "1000",
            "shortUnits": "0"
        })
    
    mock_oanda = setup_mock_broker(open_positions)
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        counts = compute_open_trade_counts()
        print(f"Initial EUR_USD trades: {counts['open_by_pair']['EUR_USD']}/{MAX_OPEN_TRADES_PER_PAIR}")
        
        # EUR_USD order should be blocked
        result1 = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="test_pair_eur"
        )
        print(f"EUR_USD order result: {'SUCCESS' if not result1.get('error') else 'BLOCKED - ' + result1.get('reason')}")
        
        # GBP_JPY order should succeed
        result2 = _place_order_with_guards(
            pair="GBP_JPY",
            units=1000,
            order_type="MARKET",
            entry_id="test_pair_gbp"
        )
        print(f"GBP_JPY order result: {'SUCCESS' if not result2.get('error') else 'BLOCKED - ' + result2.get('reason')}")
        
        # Verify
        assert result1.get("error") is True, "EUR_USD order should have error=True"
        assert result1.get("reason") == "PAIR_CONCURRENCY_CAP", "EUR_USD order should be blocked by pair cap"
        assert not result2.get("error"), "GBP_JPY order should succeed"
        
        print("✓ Per-pair cap enforcement verified")


def test_rate_limiting():
    """Test order rate limiting"""
    print("\n=== Testing Order Rate Limiting ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    mock_oanda = setup_mock_broker()
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        # Fill rate limit
        now = time.time()
        for i in range(MAX_ORDERS_PER_MIN):
            result = _place_order_with_guards(
                pair=f"PAIR_{i}",
                units=1000,
                order_type="MARKET",
                entry_id=f"rate_test_{i}"
            )
            if i < MAX_ORDERS_PER_MIN:
                assert not result.get("error"), f"Order {i} should succeed"
        
        print(f"Placed {MAX_ORDERS_PER_MIN} orders successfully")
        
        # Next order should be rate limited
        result = _place_order_with_guards(
            pair="RATE_LIMITED_PAIR",
            units=1000,
            order_type="MARKET",
            entry_id="rate_limited"
        )
        print(f"Rate limited order result: {'SUCCESS' if not result.get('error') else 'BLOCKED - ' + result.get('reason')}")
        
        # Verify
        assert result.get("error") is True, "Order should have error=True"
        assert result.get("reason") == "ORDER_RATE_LIMIT", "Order should be rate limited"
        
        print("✓ Order rate limiting verified")


def test_entry_deduplication():
    """Test entry deduplication"""
    print("\n=== Testing Entry Deduplication ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    mock_oanda = setup_mock_broker()
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        # First entry should succeed
        result1 = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="dedup_test_entry"
        )
        print(f"First entry result: {'SUCCESS' if not result1.get('error') else 'BLOCKED - ' + result1.get('reason')}")
        
        # Duplicate entry should be blocked
        result2 = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="dedup_test_entry"
        )
        print(f"Duplicate entry result: {'SUCCESS' if not result2.get('error') else 'BLOCKED - ' + result2.get('reason')}")
        
        # Different entry should succeed
        result3 = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="dedup_test_entry_2"
        )
        print(f"Different entry result: {'SUCCESS' if not result3.get('error') else 'BLOCKED - ' + result3.get('reason')}")
        
        # Verify
        assert not result1.get("error"), "First entry should succeed"
        assert result2.get("error") is True, "Duplicate entry should have error=True"
        assert result2.get("reason") == "DUPLICATE_ENTRY_ID", "Duplicate entry should be blocked"
        assert not result3.get("error"), "Different entry should succeed"
        
        print("✓ Entry deduplication verified")


def test_pending_orders_counted():
    """Test that pending orders are counted towards caps"""
    print("\n=== Testing Pending Orders Counted ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    # Setup broker with pending orders at pair cap
    pending_orders = []
    for i in range(MAX_OPEN_TRADES_PER_PAIR):
        pending_orders.append({
            "instrument": "EUR_USD",
            "state": "PENDING"
        })
    
    mock_oanda = setup_mock_broker(pending_orders=pending_orders)
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        counts = compute_open_trade_counts()
        print(f"EUR_USD pending orders: {counts['pending_by_pair']['EUR_USD']}")
        print(f"EUR_USD total (open + pending): {counts['open_by_pair']['EUR_USD'] + counts['pending_by_pair']['EUR_USD']}")
        
        # Should be blocked due to pending orders
        result = _place_order_with_guards(
            pair="EUR_USD",
            units=1000,
            order_type="MARKET",
            entry_id="pending_test"
        )
        print(f"Order with pending result: {'SUCCESS' if not result.get('error') else 'BLOCKED - ' + result.get('reason')}")
        
        # Verify
        assert result.get("error") is True, "Order should have error=True"
        assert result.get("reason") == "PAIR_CONCURRENCY_CAP", "Order should be blocked due to pending orders"
        
        print("✓ Pending orders counted towards caps")


def test_logging_output():
    """Test that proper logging is generated"""
    print("\n=== Testing Logging Output ===")
    
    # Reset state
    _ORDER_TIMESTAMPS.clear()
    _ENTRY_ID_TIMESTAMPS.clear()
    
    # Capture logs
    logs = []
    
    def capture_log(msg, data=None):
        if isinstance(msg, str) and "ORDER_BLOCKED" in msg:
            logs.append((msg, data))
    
    mock_oanda = setup_mock_broker()
    
    with patch('phone_bot._require_runtime_oanda', return_value=mock_oanda):
        with patch('phone_bot.log', side_effect=capture_log):
            # Trigger each block type
            # 1. Duplicate entry
            _place_order_with_guards(
                pair="EUR_USD",
                units=1000,
                order_type="MARKET",
                entry_id="log_test_dup"
            )
            _place_order_with_guards(
                pair="EUR_USD",
                units=1000,
                order_type="MARKET",
                entry_id="log_test_dup"
            )
            
            # 2. Rate limit
            for i in range(MAX_ORDERS_PER_MIN + 1):
                _place_order_with_guards(
                    pair=f"PAIR_{i}",
                    units=1000,
                    order_type="MARKET",
                    entry_id=f"log_rate_{i}"
                )
    
    print(f"Captured {len(logs)} ORDER_BLOCKED logs")
    
    # Verify log content
    log_reasons = [data.get("reason") for _, data in logs if data]
    assert "DUPLICATE_ENTRY_ID" in log_reasons, "Should log duplicate entry blocks"
    assert "ORDER_RATE_LIMIT" in log_reasons, "Should log rate limit blocks"
    
    print("✓ Proper logging verified")
    
    # Show sample log
    if logs:
        print("\nSample ORDER_BLOCKED log:")
        print(json.dumps(logs[0][1], indent=2))


def run_all_proofs():
    """Run all proof validation tests"""
    print("="*60)
    print("CONCURRENCY CAPS PROOF VALIDATION")
    print("="*60)
    
    print(f"\nConfiguration:")
    print(f"  MAX_OPEN_TRADES_GLOBAL: {MAX_OPEN_TRADES_GLOBAL}")
    print(f"  MAX_OPEN_TRADES_PER_PAIR: {MAX_OPEN_TRADES_PER_PAIR}")
    print(f"  MAX_ORDERS_PER_MIN: {MAX_ORDERS_PER_MIN}")
    print(f"  ENTRY_DEDUP_TTL_SEC: {ENTRY_DEDUP_TTL_SEC}")
    
    try:
        test_global_cap_enforcement()
        test_pair_cap_enforcement()
        test_rate_limiting()
        test_entry_deduplication()
        test_pending_orders_counted()
        test_logging_output()
        
        print("\n" + "="*60)
        print("✅ ALL PROOF VALIDATIONS PASSED")
        print("="*60)
        print("\nThe concurrency caps implementation is working correctly:")
        print("• Global trade cap is enforced")
        print("• Per-pair trade cap is enforced")
        print("• Order rate limiting prevents bursts")
        print("• Entry deduplication blocks duplicates")
        print("• Pending orders are counted towards caps")
        print("• Detailed logging is generated for blocks")
        
        return 0
        
    except AssertionError as e:
        print(f"\n❌ PROOF VALIDATION FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(run_all_proofs())
