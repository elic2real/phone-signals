#!/usr/bin/env python3
"""
Priority Engine with Selection Modes
Clean trade selection and ordering system
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class TradeGrade(Enum):
    """Trade quality grades"""
    A = "A"  # Elite/Exceptional
    B = "B"  # Strong/Normal
    C = "C"  # Broad/Weak but valid

class SelectionMode(Enum):
    """Bot strictness/selection modes"""
    A = "A"  # Elite only - only A grades
    B = "B"  # Strong + elite - A and B grades
    C = "C"  # Broad - A, B, and C grades

@dataclass
class TradeCandidate:
    """Trade candidate for priority evaluation"""
    # Basic trade info
    pair: str
    direction: str
    entry_price: float
    sl_price: float
    
    # Signal quality metrics (0-1 scale)
    signal_strength: float
    bias_alignment: float
    trend_strength: float
    regime_fit: float
    
    # Context
    session: str
    quarter: str
    
    # Trade characteristics
    estimated_trade_life: float  # hours
    expected_move: float  # pips
    spread_pips: float
    
    # Calculated fields
    grade: Optional[TradeGrade] = None
    priority_score: Optional[float] = None
    capital_efficiency: Optional[float] = None

class PriorityEngine:
    """Clean priority engine for trade selection and ordering"""
    
    def __init__(self, selection_mode: SelectionMode = SelectionMode.B):
        self.selection_mode = selection_mode
        
        # Grade thresholds
        self.grade_thresholds = {
            TradeGrade.A: 0.8,  # Elite threshold
            TradeGrade.B: 0.5,  # Strong threshold
            TradeGrade.C: 0.0   # Minimum valid (anything above 0)
        }
        
        # Priority scoring weights
        self.weights = {
            'signal_strength': 0.25,
            'bias_alignment': 0.15,
            'trend_strength': 0.15,
            'regime_fit': 0.15,
            'capital_efficiency': 0.20,
            'spread_penalty': 0.10
        }
        
    def evaluate_candidate(self, candidate: TradeCandidate) -> Dict[str, Any]:
        """Evaluate a trade candidate"""
        # 1. Assign grade
        candidate.grade = self._assign_grade(candidate)
        
        # 2. Calculate capital efficiency
        candidate.capital_efficiency = self._calculate_capital_efficiency(candidate)
        
        # 3. Calculate priority score
        candidate.priority_score = self._calculate_priority_score(candidate)
        
        return {
            'pair': candidate.pair,
            'direction': candidate.direction,
            'grade': candidate.grade.value,
            'priority_score': candidate.priority_score,
            'capital_efficiency': candidate.capital_efficiency,
            'selection_mode': self.selection_mode.value
        }
        
    def _assign_grade(self, candidate: TradeCandidate) -> TradeGrade:
        """Assign grade based on signal quality"""
        # Base score from signal strength
        base_score = candidate.signal_strength
        
        # Adjust for context factors
        if candidate.regime_fit < 0.5:
            base_score *= 0.8  # Penalize poor regime fit
            
        if candidate.bias_alignment < 0.6:
            base_score *= 0.9  # Penalize weak bias alignment
            
        if candidate.spread_pips > 3.0:
            base_score *= 0.9  # Penalize wide spreads
            
        # Determine grade
        if base_score >= self.grade_thresholds[TradeGrade.A]:
            return TradeGrade.A
        elif base_score >= self.grade_thresholds[TradeGrade.B]:
            return TradeGrade.B
        else:
            return TradeGrade.C
            
    def _calculate_capital_efficiency(self, candidate: TradeCandidate) -> float:
        """Calculate capital recycling value"""
        if candidate.estimated_trade_life <= 0:
            return 0.0
            
        # Efficiency = expected move / time to completion
        # Higher is better (faster capital recycling)
        efficiency = candidate.expected_move / candidate.estimated_trade_life
        
        # Normalize to 0-1 scale (assuming 100 pips/hour is excellent)
        normalized = min(efficiency / 100.0, 1.0)
        
        return normalized
        
    def _calculate_priority_score(self, candidate: TradeCandidate) -> float:
        """Calculate overall priority score"""
        # Base components
        signal_score = candidate.signal_strength * self.weights['signal_strength']
        bias_score = candidate.bias_alignment * self.weights['bias_alignment']
        trend_score = candidate.trend_strength * self.weights['trend_strength']
        regime_score = candidate.regime_fit * self.weights['regime_fit']
        
        # Capital efficiency bonus
        efficiency_score = candidate.capital_efficiency * self.weights['capital_efficiency']
        
        # Spread penalty (lower spread = higher score)
        spread_penalty = (1.0 - min(candidate.spread_pips / 5.0, 1.0)) * self.weights['spread_penalty']
        
        # Sum all components
        total_score = (
            signal_score + bias_score + trend_score + 
            regime_score + efficiency_score + spread_penalty
        )
        
        return round(total_score, 3)
        
    def rank_candidates(self, candidates: List[TradeCandidate]) -> List[Dict[str, Any]]:
        """Rank trades by priority with selection mode filter"""
        results = []
        
        # 1. Evaluate all candidates
        for candidate in candidates:
            evaluation = self.evaluate_candidate(candidate)
            results.append(evaluation)
            
        # 2. Apply selection mode filter
        filtered = self._apply_selection_filter(results)
        
        # 3. Sort by priority score (highest first)
        ranked = sorted(filtered, key=lambda x: x['priority_score'], reverse=True)
        
        return ranked
        
    def _apply_selection_filter(self, evaluations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter based on selection mode"""
        allowed_grades = {
            SelectionMode.A: [TradeGrade.A],
            SelectionMode.B: [TradeGrade.A, TradeGrade.B],
            SelectionMode.C: [TradeGrade.A, TradeGrade.B, TradeGrade.C]
        }
        
        allowed = allowed_grades[self.selection_mode]
        filtered = []
        
        for eval in evaluations:
            grade = TradeGrade(eval['grade'])
            if grade in allowed:
                filtered.append(eval)
                
        return filtered
        
    def set_selection_mode(self, mode: SelectionMode):
        """Update selection mode"""
        self.selection_mode = mode
        logger.info(f"Priority engine selection mode set to {mode.value}")

def demo_priority_engine():
    """Demonstrate the priority engine"""
    print("=" * 70)
    print("PRIORITY ENGINE WITH SELECTION MODES")
    print("=" * 70)
    
    # Create engine
    engine = PriorityEngine()
    
    # Generate candidates
    candidates = [
        TradeCandidate(
            pair="EUR_USD",
            direction="LONG",
            entry_price=1.1000,
            sl_price=1.0900,
            signal_strength=0.9,
            bias_alignment=0.85,
            trend_strength=0.8,
            regime_fit=0.9,
            session="London",
            quarter="Q2",
            estimated_trade_life=3.0,
            expected_move=60,
            spread_pips=1.2
        ),
        TradeCandidate(
            pair="GBP_USD",
            direction="SHORT",
            entry_price=1.2500,
            sl_price=1.2600,
            signal_strength=0.7,
            bias_alignment=0.8,
            trend_strength=0.6,
            regime_fit=0.7,
            session="London",
            quarter="Q2",
            estimated_trade_life=4.0,
            expected_move=40,
            spread_pips=2.0
        ),
        TradeCandidate(
            pair="USD_JPY",
            direction="LONG",
            entry_price=110.00,
            sl_price=109.00,
            signal_strength=0.6,
            bias_alignment=0.7,
            trend_strength=0.5,
            regime_fit=0.6,
            session="NY",
            quarter="Q2",
            estimated_trade_life=5.0,
            expected_move=30,
            spread_pips=1.0
        ),
        TradeCandidate(
            pair="AUD_USD",
            direction="SHORT",
            entry_price=0.7500,
            sl_price=0.7600,
            signal_strength=0.4,
            bias_alignment=0.5,
            trend_strength=0.4,
            regime_fit=0.4,
            session="Asia",
            quarter="Q2",
            estimated_trade_life=6.0,
            expected_move=25,
            spread_pips=1.5
        )
    ]
    
    print("\n1. All Candidates (unfiltered):")
    print("-" * 70)
    
    for candidate in candidates:
        eval = engine.evaluate_candidate(candidate)
        print(f"{candidate.pair} {candidate.direction}:")
        print(f"   Grade: {eval['grade']}")
        print(f"   Priority: {eval['priority_score']}")
        print(f"   Efficiency: {eval['capital_efficiency']:.3f}")
        
    print("\n2. Selection Mode A (Elite only):")
    print("-" * 70)
    engine.set_selection_mode(SelectionMode.A)
    ranked_a = engine.rank_candidates(candidates)
    
    for trade in ranked_a:
        print(f"{trade['pair']} - Grade {trade['grade']} - Priority {trade['priority_score']}")
        
    print("\n3. Selection Mode B (Strong + Elite):")
    print("-" * 70)
    engine.set_selection_mode(SelectionMode.B)
    ranked_b = engine.rank_candidates(candidates)
    
    for i, trade in enumerate(ranked_b, 1):
        print(f"{i}. {trade['pair']} - Grade {trade['grade']} - Priority {trade['priority_score']}")
        
    print("\n4. Selection Mode C (Broad):")
    print("-" * 70)
    engine.set_selection_mode(SelectionMode.C)
    ranked_c = engine.rank_candidates(candidates)
    
    for i, trade in enumerate(ranked_c, 1):
        print(f"{i}. {trade['pair']} - Grade {trade['grade']} - Priority {trade['priority_score']}")
        
    print("\n5. All Ranked Trades (Mode B):")
    print("-" * 70)
    engine.set_selection_mode(SelectionMode.B)
    ranked_trades = engine.rank_candidates(candidates)
    
    for i, trade in enumerate(ranked_trades, 1):
        print(f"{i}. {trade['pair']} - Grade {trade['grade']} - Priority {trade['priority_score']}")
        
    print("\n" + "=" * 70)
    print("PRIORITY ENGINE BENEFITS")
    print("=" * 70)
    print("\n✅ Prevents random entry order")
    print("✅ Prevents first-come first-served behavior")
    print("✅ Prevents weak trades from consuming capital first")
    print("✅ Configurable strictness (A/B/C modes)")
    print("✅ Always ranks by priority within allowed grades")
    
    print("\nFlow: Score → Grade → Filter → Rank → Select")

if __name__ == "__main__":
    demo_priority_engine()
