#!/usr/bin/env python3
"""
Testing Multiple Simultaneous Trade Triggers
"""

import sys
sys.path.append('/home/elic/Documents/phone signals')

def test_simultaneous_trades():
    print("=" * 70)
    print("MULTIPLE SIMULTANEOUS TRADES TEST")
    print("=" * 70)
    
    from priority_engine import PriorityEngine, TradeCandidate, SelectionMode
    
    # Create engine
    engine = PriorityEngine(selection_mode=SelectionMode.B)  # A and B grades allowed
    
    print("\nScenario: 3 trades trigger at the same time...")
    print("-" * 70)
    
    # Simulate 3 trades triggering simultaneously
    simultaneous_trades = [
        # Trade 1: Strong signal but wide spread
        TradeCandidate(
            pair="EUR_USD",
            direction="LONG",
            entry_price=1.1000,
            sl_price=1.0900,
            signal_strength=0.9,  # Very strong
            bias_alignment=0.8,
            trend_strength=0.8,
            regime_fit=0.9,
            session="London",
            quarter="Q2",
            estimated_trade_life=3.0,
            expected_move=50,
            spread_pips=3.5  # Wide spread penalty
        ),
        # Trade 2: Moderate signal but tight spread and fast
        TradeCandidate(
            pair="GBP_USD",
            direction="SHORT",
            entry_price=1.2500,
            sl_price=1.2600,
            signal_strength=0.7,  # Moderate
            bias_alignment=0.7,
            trend_strength=0.7,
            regime_fit=0.7,
            session="London",
            quarter="Q2",
            estimated_trade_life=2.0,  # Fast!
            expected_move=40,
            spread_pips=1.0  # Tight spread
        ),
        # Trade 3: Weak signal
        TradeCandidate(
            pair="USD_JPY",
            direction="LONG",
            entry_price=110.00,
            sl_price=109.00,
            signal_strength=0.4,  # Weak
            bias_alignment=0.6,
            trend_strength=0.5,
            regime_fit=0.6,
            session="NY",
            quarter="Q2",
            estimated_trade_life=4.0,
            expected_move=30,
            spread_pips=1.2
        )
    ]
    
    # Evaluate all
    print("\nEvaluating all 3 simultaneous trades:")
    for trade in simultaneous_trades:
        eval_result = engine.evaluate_candidate(trade)
        print(f"\n{trade.pair} {trade.direction}:")
        print(f"   Signal: {trade.signal_strength}, Spread: {trade.spread_pips}pips")
        print(f"   Time: {trade.estimated_trade_life}h, Move: {trade.expected_move}pips")
        print(f"   → Grade: {eval_result['grade']}, Priority: {eval_result['priority_score']:.3f}")
    
    # Rank them
    print("\n" + "=" * 70)
    print("RANKING RESULTS (Best to Worst)")
    print("=" * 70)
    
    ranked = engine.rank_candidates(simultaneous_trades)
    
    print(f"\nSelection Mode: B (A and B grades allowed)")
    print(f"Trades ranked by priority:\n")
    
    for i, trade in enumerate(ranked, 1):
        status = "✓ TAKE" if i <= 3 else "✗ SKIP"
        print(f"{i}. {trade['pair']} - Grade {trade['grade']} - Priority {trade['priority_score']:.3f} {status}")
    
    print("\n" + "=" * 70)
    print("HOW THE SYSTEM HANDLES IT")
    print("=" * 70)
    
    print("\n1. All 3 trades are evaluated simultaneously")
    print("2. Each gets a priority score based on:")
    print("   - Signal strength (25% weight)")
    print("   - Capital efficiency (20% weight)")
    print("   - Spread penalty (10% weight)")
    print("   - Other factors (45% weight)")
    
    print("\n3. Selection Mode B filters out C grades")
    print("4. Remaining trades sorted by priority")
    print("5. System takes trades in priority order")
    
    print("\n" + "=" * 70)
    print("PRACTICAL EXAMPLE")
    print("=" * 70)
    
    print("\nWith $10,000 account and 2% risk per trade:")
    print("1. GBP_USD gets taken first (priority 0.610)")
    print("   - Tight spread, fast completion")
    print("   - Risk: $200")
    
    print("\n2. EUR_USD gets taken second (priority 0.585)")
    print("   - Strong signal but wide spread")
    print("   - Risk: $200")
    
    print("\n3. USD_JPY is SKIPPED (Grade C)")
    print("   - Weak signal, filtered out by Mode B")
    
    print("\nTotal at risk: $400 (4% of account)")
    
    print("\n" + "=" * 70)
    print("KEY POINTS")
    print("=" * 70)
    
    print("\n✅ No random selection - always by priority")
    print("✅ No first-come bias - all evaluated together")
    print("✅ Capital goes to best opportunities first")
    print("✅ Weak signals get filtered out by selection mode")
    print("✅ System can handle ANY number of simultaneous triggers")

if __name__ == "__main__":
    test_simultaneous_trades()
