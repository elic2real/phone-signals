#!/usr/bin/env python3
"""
Unit tests for concurrency caps implementation.
Tests the _place_order_with_guards choke point and related functions.
"""

import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch
from collections import defaultdict
from typing import Dict

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


class TestConcurrencyCaps(unittest.TestCase):
    """Test concurrency caps enforcement"""
    
    def setUp(self):
        """Reset global state before each test"""
        _ORDER_TIMESTAMPS.clear()
        _ENTRY_ID_TIMESTAMPS.clear()
    
    def test_compute_open_trade_counts_empty(self):
        """Test counting with no open trades"""
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            mock_oanda.return_value.open_positions.return_value = {
                "positions": []
            }
            mock_oanda.return_value.pending_orders.return_value = {
                "orders": []
            }
            
            counts = compute_open_trade_counts()
            
            self.assertEqual(counts["open_global"], 0)
            self.assertEqual(counts["pending_global"], 0)
            self.assertEqual(len(counts["open_by_pair"]), 0)
            self.assertEqual(len(counts["pending_by_pair"]), 0)
    
    def test_compute_open_trade_counts_with_positions(self):
        """Test counting with open positions"""
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            mock_oanda.return_value.open_positions.return_value = {
                "positions": [
                    {"instrument": "EUR_USD", "longUnits": "1000", "shortUnits": "0"},
                    {"instrument": "GBP_JPY", "longUnits": "0", "shortUnits": "500"},
                    {"instrument": "EUR_USD", "longUnits": "0", "shortUnits": "2000"},
                    {"instrument": "USD_CAD", "longUnits": "0", "shortUnits": "0"}  # Empty position
                ]
            }
            mock_oanda.return_value.pending_orders.return_value = {
                "orders": []
            }
            
            counts = compute_open_trade_counts()
            
            self.assertEqual(counts["open_global"], 3)
            self.assertEqual(counts["open_by_pair"]["EUR_USD"], 2)
            self.assertEqual(counts["open_by_pair"]["GBP_JPY"], 1)
            self.assertEqual(counts["open_by_pair"]["USD_CAD"], 0)
    
    def test_compute_open_trade_counts_with_pending(self):
        """Test counting with pending orders"""
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            mock_oanda.return_value.open_positions.return_value = {
                "positions": [
                    {"instrument": "EUR_USD", "longUnits": "1000", "shortUnits": "0"}
                ]
            }
            mock_oanda.return_value.pending_orders.return_value = {
                "orders": [
                    {"instrument": "EUR_USD", "state": "PENDING"},
                    {"instrument": "GBP_JPY", "state": "PENDING"},
                    {"instrument": "USD_CAD", "state": "CANCELLED"}  # Not pending
                ]
            }
            
            counts = compute_open_trade_counts()
            
            self.assertEqual(counts["open_global"], 1)
            self.assertEqual(counts["pending_global"], 2)
            self.assertEqual(counts["open_by_pair"]["EUR_USD"], 1)
            self.assertEqual(counts["pending_by_pair"]["EUR_USD"], 1)
            self.assertEqual(counts["pending_by_pair"]["GBP_JPY"], 1)
    
    def test_compute_open_trade_counts_fail_closed(self):
        """Test fail-closed behavior on error"""
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            mock_oanda.side_effect = Exception("API error")
            
            counts = compute_open_trade_counts()
            
            # Should return high counts to block on uncertainty
            self.assertEqual(counts["open_global"], 999)
            self.assertEqual(counts["open_by_pair"]["EUR_USD"], 999)
    
    def test_place_order_with_guards_duplicate_entry(self):
        """Test duplicate entry deduplication"""
        entry_id = "test_entry"
        now = time.time()
        
        # Simulate recent entry
        _ENTRY_ID_TIMESTAMPS[entry_id] = now - 60  # 60 seconds ago
        
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            result = _place_order_with_guards(
                pair="EUR_USD",
                units=1000,
                order_type="MARKET",
                entry_id=entry_id
            )
            
            self.assertTrue(result.get("error"))
            self.assertEqual(result.get("reason"), "DUPLICATE_ENTRY_ID")
            mock_oanda.assert_not_called()
    
    def test_place_order_with_guards_rate_limit(self):
        """Test order rate limiting"""
        # Fill rate limit tracker
        now = time.time()
        for i in range(MAX_ORDERS_PER_MIN):
            _ORDER_TIMESTAMPS.append(now - i)  # Spread over last minute
        
        with patch('phone_bot._require_runtime_oanda') as mock_oanda:
            result = _place_order_with_guards(
                pair="EUR_USD",
                units=1000,
                order_type="MARKET"
            )
            
            self.assertTrue(result.get("error"))
            self.assertEqual(result.get("reason"), "ORDER_RATE_LIMIT")
            mock_oanda.assert_not_called()
    
    def test_place_order_with_guards_global_cap(self):
        """Test global concurrency cap"""
        # Mock broker snapshot at global cap
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": MAX_OPEN_TRADES_GLOBAL,
                "pending_global": 0,
                "open_by_pair": defaultdict(int),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                result = _place_order_with_guards(
                    pair="EUR_USD",
                    units=1000,
                    order_type="MARKET"
                )
                
                self.assertTrue(result.get("error"))
                self.assertEqual(result.get("reason"), "GLOBAL_CONCURRENCY_CAP")
                mock_oanda.assert_not_called()
    
    def test_place_order_with_guards_pair_cap(self):
        """Test per-pair concurrency cap"""
        # Mock broker snapshot at pair cap
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": 10,
                "pending_global": 0,
                "open_by_pair": defaultdict(int, {"EUR_USD": MAX_OPEN_TRADES_PER_PAIR}),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                result = _place_order_with_guards(
                    pair="EUR_USD",
                    units=1000,
                    order_type="MARKET"
                )
                
                self.assertTrue(result.get("error"))
                self.assertEqual(result.get("reason"), "PAIR_CONCURRENCY_CAP")
                mock_oanda.assert_not_called()
    
    def test_place_order_with_guards_success(self):
        """Test successful order placement through choke point"""
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": 10,
                "pending_global": 0,
                "open_by_pair": defaultdict(int, {"EUR_USD": 2}),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                mock_oanda.return_value.place_market.return_value = {
                    "orderFillTransaction": {
                        "orderID": "123",
                        "tradeOpened": {"tradeID": "456"},
                        "units": "1000",
                        "price": "1.1000"
                    }
                }
                
                result = _place_order_with_guards(
                    pair="EUR_USD",
                    units=1000,
                    order_type="MARKET",
                    stop_loss=1.0900,
                    take_profit=1.1100,
                    client_id="test_client",
                    reason="test_order",
                    entry_id="test_entry_unique"
                )
                
                self.assertFalse(result.get("error"))
                self.assertIn("orderFillTransaction", result)
                mock_oanda.return_value.place_market.assert_called_once()
                
                # Verify tracking updated
                self.assertEqual(len(_ORDER_TIMESTAMPS), 1)
                self.assertIn("test_entry_unique", _ENTRY_ID_TIMESTAMPS)
    
    def test_place_order_with_guards_limit_order(self):
        """Test limit order placement through choke point"""
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": 5,
                "pending_global": 0,
                "open_by_pair": defaultdict(int),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                mock_oanda.return_value.place_limit.return_value = {
                    "orderCreateTransaction": {"orderID": "789"}
                }
                
                result = _place_order_with_guards(
                    pair="GBP_JPY",
                    units=500,
                    order_type="LIMIT",
                    price=150.00,
                    stop_loss=145.00,
                    take_profit=155.00
                )
                
                self.assertFalse(result.get("error"))
                mock_oanda.return_value.place_limit.assert_called_once_with(
                    "GBP_JPY", 500, 150.00, 145.00, 155.00, 
                    client_id=None, allow_error_dict=True
                )
    
    def test_entry_dedup_ttl_expiry(self):
        """Test that duplicate entries are allowed after TTL expires"""
        entry_id = "test_entry_ttl"
        now = time.time()
        
        # Simulate old entry beyond TTL
        _ENTRY_ID_TIMESTAMPS[entry_id] = now - ENTRY_DEDUP_TTL_SEC - 10
        
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": 0,
                "pending_global": 0,
                "open_by_pair": defaultdict(int),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                mock_oanda.return_value.place_market.return_value = {
                    "orderFillTransaction": {"orderID": "999"}
                }
                
                result = _place_order_with_guards(
                    pair="EUR_USD",
                    units=1000,
                    order_type="MARKET",
                    entry_id=entry_id
                )
                
                self.assertFalse(result.get("error"))
                mock_oanda.assert_called_once()
    
    def test_order_rate_limit_cleanup(self):
        """Test that old order timestamps are cleaned up"""
        now = time.time()
        
        # Add old timestamps (should be cleaned)
        _ORDER_TIMESTAMPS.extend([
            now - 61,  # 61 seconds ago
            now - 120, # 2 minutes ago
        ])
        
        # Add recent timestamps (should remain)
        recent = [now - 30, now - 10, now - 5]
        _ORDER_TIMESTAMPS.extend(recent)
        
        with patch('phone_bot.compute_open_trade_counts') as mock_counts:
            mock_counts.return_value = {
                "open_global": 0,
                "pending_global": 0,
                "open_by_pair": defaultdict(int),
                "pending_by_pair": defaultdict(int)
            }
            
            with patch('phone_bot._require_runtime_oanda') as mock_oanda:
                mock_oanda.return_value.place_market.return_value = {
                    "orderFillTransaction": {"orderID": "111"}
                }
                
                _place_order_with_guards(
                    pair="EUR_USD",
                    units=1000,
                    order_type="MARKET"
                )
                
                # Old timestamps should be cleaned, recent + new should remain
                expected_count = len(recent) + 1  # recent + new order
                self.assertEqual(len(_ORDER_TIMESTAMPS), expected_count)
                # Check that all recent timestamps are present (allowing for small time differences)
                for ts in recent:
                    found = any(abs(ots - ts) < 1.0 for ots in _ORDER_TIMESTAMPS)
                    self.assertTrue(found, f"Recent timestamp {ts} should be present")


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)
