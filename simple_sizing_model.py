#!/usr/bin/env python3
"""
Simple Mechanical Sizing Model
Clean extraction-focused sizing without overfitting
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class TradeGrade(Enum):
    """Trade quality grades"""
    A = "A"  # Exceptional
    B = "B"  # Normal/Default
    C = "C"  # Weak but valid
    D = "D"  # Historically bad but still fillable
    E = "E"  # Noise/none - exposed but not fillable in production

@dataclass
class TradeOpportunity:
    """Trade opportunity with sizing parameters"""
    pair: str
    direction: str
    signal_strength: float  # 0-1
    bias_alignment: float  # 0-1
    trend_strength: float  # 0-1
    regime_fit: float  # 0-1
    session: str
    quarter: str
    estimated_trade_life: float  # hours
    spread_pips: float
    expected_move: float  # pips
    historical_grade: Optional[str] = None
    
    # Calculated fields
    grade: Optional[TradeGrade] = None
    priority_score: Optional[float] = None
    initial_size: Optional[float] = None  # % of NAV
    add_allowed: bool = False
    
class SimpleSizingModel:
    """Clean, mechanical sizing model for extraction"""
    
    def __init__(self):
        # Grade risk percentages (NAV)
        self.grade_risk = {
            TradeGrade.A: 3.0,  # Exceptional
            TradeGrade.B: 2.0,  # Normal/Default
            TradeGrade.C: 1.5,  # Weak but valid
            TradeGrade.D: 0.5,  # Bad, lowest fillable rank
            TradeGrade.E: 0.0,  # Noise/none, do not fill in production
        }
        
        # Add-on parameters
        self.add_on_risk = 1.0  # +1.0% NAV once
        
        # Weak trade reduction
        self.weak_reduction = 1.5  # Reduce to 1.5% if stalling
        
    def grade_trade(self, opportunity: TradeOpportunity) -> TradeGrade:
        """Grade the trade based on signal quality"""
        override = str(getattr(opportunity, "historical_grade", "") or "").upper()
        if override in TradeGrade.__members__:
            return TradeGrade[override]

        score = opportunity.signal_strength
        
        # Adjust for regime fit and bias alignment
        if opportunity.regime_fit < 0.5:
            score *= 0.8  # Penalize poor regime fit
            
        if opportunity.bias_alignment < 0.6:
            score *= 0.9  # Penalize weak bias alignment
            
        # Simple grading thresholds
        if score >= 0.8:
            return TradeGrade.A
        elif score >= 0.5:
            return TradeGrade.B
        elif score >= 0.25:
            return TradeGrade.C
        elif score > 0.0:
            return TradeGrade.D
        return TradeGrade.E
            
    def calculate_priority_score(self, opportunity: TradeOpportunity) -> float:
        """Calculate priority score for trade ranking"""
        # Base score from signal and alignment
        base_score = (
            opportunity.signal_strength * 0.3 +
            opportunity.bias_alignment * 0.2 +
            opportunity.trend_strength * 0.2 +
            opportunity.regime_fit * 0.2 +
            (1.0 - min(opportunity.spread_pips / 5.0, 1.0)) * 0.1  # Spread penalty
        )
        
        # Capital efficiency bonus
        if opportunity.estimated_trade_life > 0:
            capital_efficiency = opportunity.expected_move / opportunity.estimated_trade_life
            efficiency_bonus = min(capital_efficiency / 100.0, 0.2)  # Cap at 0.2
            base_score += efficiency_bonus
            
        return base_score
        
    def size_trade(self, opportunity: TradeOpportunity, runtime_settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Calculate initial size and parameters for a trade"""
        # Grade the trade
        opportunity.grade = self.grade_trade(opportunity)
        
        # Calculate priority
        opportunity.priority_score = self.calculate_priority_score(opportunity)
        
        risk_by_grade = dict(self.grade_risk)
        add_on_risk = self.add_on_risk
        if isinstance(runtime_settings, dict):
            sizing_cfg = dict(runtime_settings.get("sizing", {}) or {})
            custom_risk = sizing_cfg.get("risk_by_grade")
            if isinstance(custom_risk, dict):
                for grade_name, pct in custom_risk.items():
                    try:
                        enum_key = TradeGrade[str(grade_name).upper()]
                        risk_by_grade[enum_key] = float(pct)
                    except Exception:
                        continue
            try:
                add_on_risk = float(sizing_cfg.get("add_on_once_risk_percent", add_on_risk) or add_on_risk)
            except Exception:
                add_on_risk = self.add_on_risk

        # Set initial size based on grade
        opportunity.initial_size = risk_by_grade[opportunity.grade]
        
        # Determine if add-on is allowed (A and B grades only)
        opportunity.add_allowed = opportunity.grade in [TradeGrade.A, TradeGrade.B]
        
        return {
            "grade": opportunity.grade.value,
            "priority_score": opportunity.priority_score,
            "initial_size_percent": opportunity.initial_size,
            "add_allowed": opportunity.add_allowed,
            "add_on_size_percent": add_on_risk if opportunity.add_allowed else 0,
            "max_total_size": opportunity.initial_size + (add_on_risk if opportunity.add_allowed else 0)
        }
        
    def should_add_on(self, current_trade: Dict[str, Any]) -> bool:
        """Determine if add-on should be triggered"""
        if not current_trade.get("add_allowed", False):
            return False
            
        # Condition 1: Trade in profit
        pnl_pips = current_trade.get("pnl_pips", 0)
        if pnl_pips <= 0:
            return False
            
        # Condition 2: Trade still has continuation energy
        # This would come from AEE engine
        continuation_strength = current_trade.get("continuation_strength", 0)
        if continuation_strength < 0.6:  # Threshold for strong continuation
            return False
            
        return True
        
    def should_reduce_weak(self, current_trade: Dict[str, Any]) -> Optional[float]:
        """Check if weak trade should be reduced"""
        current_size = current_trade.get("size_percent", 2.0)
        
        # Only reduce B-grade trades that stall
        if current_trade.get("grade") != "B":
            return None
            
        # Check if stalling (no progress for too long)
        stall_time = current_trade.get("minutes_without_progress", 0)
        if stall_time > 30 and current_size > self.weak_reduction:
            return self.weak_reduction
            
        return None
        
    def rank_trades(self, opportunities: List[TradeOpportunity]) -> List[TradeOpportunity]:
        """Rank trades by priority score"""
        # Calculate scores for all
        for opp in opportunities:
            opp.priority_score = self.calculate_priority_score(opp)
            
        # Sort by priority (highest first)
        return sorted(opportunities, key=lambda x: x.priority_score or 0, reverse=True)
        
    def get_sizing_summary(self) -> Dict[str, Any]:
        """Get summary of sizing model"""
        return {
            "model_type": "Simple Mechanical Sizing",
            "grades": {
                "A": f"{self.grade_risk[TradeGrade.A]}% NAV (exceptional)",
                "B": f"{self.grade_risk[TradeGrade.B]}% NAV (normal)",
                "C": f"{self.grade_risk[TradeGrade.C]}% NAV (weak)",
                "D": f"{self.grade_risk[TradeGrade.D]}% NAV (bad)",
                "E": f"{self.grade_risk[TradeGrade.E]}% NAV (noise/none)",
            },
            "add_on": f"+{self.add_on_risk}% NAV once when in profit with continuation",
            "weak_reduction": f"Reduce to {self.weak_reduction}% if stalling",
            "philosophy": "Let winners run, reduce weak trades, no early partialing"
        }

# Example usage
def demo_sizing_model():
    """Demonstrate the sizing model"""
    model = SimpleSizingModel()
    
    print("=" * 70)
    print("SIMPLE MECHANICAL SIZING MODEL")
    print("=" * 70)
    print(model.get_sizing_summary())
    
    # Example trades
    opportunities = [
        TradeOpportunity(
            pair="EUR_USD",
            direction="LONG",
            signal_strength=0.9,
            bias_alignment=0.8,
            trend_strength=0.85,
            regime_fit=0.9,
            session="London",
            quarter="Q2",
            estimated_trade_life=4.0,
            spread_pips=1.2,
            expected_move=50
        ),
        TradeOpportunity(
            pair="GBP_USD",
            direction="SHORT", 
            signal_strength=0.6,
            bias_alignment=0.7,
            trend_strength=0.5,
            regime_fit=0.6,
            session="NY",
            quarter="Q2",
            estimated_trade_life=6.0,
            spread_pips=2.0,
            expected_move=30
        ),
        TradeOpportunity(
            pair="AUD_USD",
            direction="LONG",
            signal_strength=0.3,
            bias_alignment=0.4,
            trend_strength=0.3,
            regime_fit=0.3,
            session="Asia",
            quarter="Q2",
            estimated_trade_life=8.0,
            spread_pips=1.5,
            expected_move=20
        )
    ]
    
    print("\n" + "=" * 70)
    print("TRADE GRADING AND SIZING")
    print("=" * 70)
    
    # Size each trade
    for opp in opportunities:
        sizing = model.size_trade(opp)
        
        print(f"\n{opp.pair} {opp.direction}:")
        print(f"   Signal: {opp.signal_strength:.2f}")
        print(f"   Grade: {sizing['grade']}")
        print(f"   Priority: {sizing['priority_score']:.3f}")
        print(f"   Initial Size: {sizing['initial_size_percent']}% NAV")
        print(f"   Add-on Allowed: {sizing['add_allowed']}")
        if sizing['add_allowed']:
            print(f"   Max Total: {sizing['max_total_size']}% NAV")
            
    # Rank trades
    print("\n" + "=" * 70)
    print("TRADE RANKING")
    print("=" * 70)
    
    ranked = model.rank_trades(opportunities)
    print("\nTrades ranked by priority:")
    for i, opp in enumerate(ranked, 1):
        print(f"{i}. {opp.pair}: {opp.grade.value} grade (score: {opp.priority_score:.3f})")
        
    print("\n" + "=" * 70)
    print("SIMPLE RULES SUMMARY")
    print("=" * 70)
    print("\n1. Grade sizing:")
    print("   A = 2.5% NAV (exceptional)")
    print("   B = 2.0% NAV (normal)")
    print("   C = 1.25% NAV (weak)")
    
    print("\n2. Add-on:")
    print("   +0.5% NAV once when:")
    print("   - Trade in profit")
    print("   - Continuation strength > 0.6")
    
    print("\n3. Weak trade handling:")
    print("   Reduce B-grade to 1.5% if stalling > 30 min")
    
    print("\n4. Winners:")
    print("   No early partialing")
    print("   Let AEE manage exit")

if __name__ == "__main__":
    demo_sizing_model()
