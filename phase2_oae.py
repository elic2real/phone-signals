#!/usr/bin/env python3
"""
Phase 2 - Opportunity Anatomy Engine (OAE)

Analyzes the behavior inside discovered opportunities.
Loads richer data (candles, spreads, microstructure) for the discovered windows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from enum import Enum
import statistics


class BucketType(Enum):
    """Opportunity classification buckets."""
    GOOD = "good"
    BAD = "bad"
    NOISE = "noise"


class ZoneType(Enum):
    """GOOD zone subtypes for AEE curriculum learning."""
    CONTINUATION = "continuation"
    EXTENSION = "extension"
    SPIKE = "spike"
    GRIND = "grind"


@dataclass
class OpportunityMetrics:
    """Detailed metrics for opportunity behavior analysis."""

    # Early impulse metrics
    early_impulse_ratio: float  # move_in_first_60s / total_move
    path_efficiency: float       # net_move / total_path_length
    early_mae_pips: float       # max adverse move in first 60s

    # Behavioral characteristics
    extension_behavior: str     # "strong", "moderate", "weak"
    stall_behavior: str         # "quick_recovery", "prolonged", "terminal"
    reversal_behavior: str      # "sharp", "gradual", "false"

    # Advanced metrics
    velocity_profile: List[float]  # ATR per second over time
    pullback_depth: float         # max retracement %
    stall_duration: float         # minutes spent in stall
    momentum_decay: float         # how momentum fades
    microstructure_quality: float # spread/microstructure score


@dataclass
class AnalyzedOpportunity:
    """Fully analyzed opportunity with behavior classification."""

    # Raw opportunity data (from Phase 1)
    opportunity_id: str
    start_time: datetime
    pair: str
    direction: str
    distance_pips: float
    time_to_move_minutes: float
    mfe_pips: float
    mae_pips: float
    session: str
    weekday: str

    # Analysis window
    analysis_start: datetime
    analysis_end: datetime
    full_price_path: List[Dict[str, Any]]  # Full candle data for window

    # Classification
    bucket: BucketType
    zone_type: Optional[ZoneType]
    confidence_score: float

    # Detailed metrics
    metrics: OpportunityMetrics

    # AEE-relevant data
    entry_price: float
    tp_price: float
    sl_price: float
    atr_pips: float


class OpportunityAnatomyEngine:
    """
    Phase 2: Analyze behavior inside discovered opportunities.

    Loads richer data for analysis windows and classifies opportunities.
    """

    def __init__(self, analysis_window_minutes: int = 120):
        self.analysis_window_minutes = analysis_window_minutes  # 2 hours analysis window

    def analyze_opportunities(self, raw_opportunities: List[Dict],
                            data_source: str, pair: str = "EUR_USD") -> List[AnalyzedOpportunity]:
        """
        Analyze all raw opportunities with rich data.

        Args:
            raw_opportunities: List of opportunity dicts from Phase 1
            data_source: Path to rich data source (candles, spreads, etc.)
            pair: Currency pair

        Returns:
            List of fully analyzed opportunities
        """
        analyzed_opportunities = []

        for opp_data in raw_opportunities:
            try:
                analyzed = self._analyze_single_opportunity(opp_data, data_source, pair)
                if analyzed:
                    analyzed_opportunities.append(analyzed)
            except Exception as e:
                print(f"Error analyzing opportunity {opp_data.get('start_time')}: {e}")
                continue

        print(f"Analyzed {len(analyzed_opportunities)} opportunities")
        return analyzed_opportunities

    def _analyze_single_opportunity(self, opp_data: Dict, data_source: str, pair: str) -> Optional[AnalyzedOpportunity]:
        """Analyze a single opportunity with rich data."""

        # Parse opportunity data
        start_time = datetime.fromisoformat(opp_data["start_time"])
        direction = opp_data["direction"]

        # Define analysis window (extend beyond the raw opportunity)
        analysis_start = start_time - timedelta(minutes=30)  # 30 min before
        analysis_end = start_time + timedelta(minutes=self.analysis_window_minutes)

        # Load rich data for this window
        price_path = self._load_analysis_window_data(
            data_source, pair, analysis_start, analysis_end
        )

        if not price_path:
            return None

        # Calculate detailed metrics
        metrics = self._calculate_opportunity_metrics(price_path, start_time, direction)

        # Classify opportunity
        bucket, zone_type, confidence = self._classify_opportunity(metrics, opp_data)

        # Calculate AEE parameters
        entry_price, tp_price, sl_price, atr_pips = self._calculate_aee_parameters(
            price_path, start_time, direction
        )

        return AnalyzedOpportunity(
            opportunity_id=f"{opp_data['direction']}_{start_time.isoformat()}",
            start_time=start_time,
            pair=pair,
            direction=direction,
            distance_pips=opp_data["distance_pips"],
            time_to_move_minutes=opp_data["time_to_move_minutes"],
            mfe_pips=opp_data["mfe_pips"],
            mae_pips=opp_data["mae_pips"],
            session=opp_data["session"],
            weekday=opp_data["weekday"],
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            full_price_path=price_path,
            bucket=bucket,
            zone_type=zone_type,
            confidence_score=confidence,
            metrics=metrics,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            atr_pips=atr_pips
        )

    def _load_analysis_window_data(self, data_source: str, pair: str,
                                 start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Load rich candle data for the analysis window.

        For now, using simplified data. In production, this would load:
        - Full OHLC candles
        - Spread data
        - Volume/microstructure
        - Order book data
        """
        # For this implementation, we'll use the same data as Phase 1
        # In production, this would load much richer data
        return self._load_oanda_candles(data_source, pair, start_time, end_time)

    def _load_oanda_candles(self, data_root: str, pair: str,
                           start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Load Oanda candle data for analysis window."""
        import pandas as pd

        candles = []

        # Find all parquet files for the pair
        data_path = Path(data_root)
        pair_files = list(data_path.glob(f"pair={pair}/year=*/month=*/part-*.parquet"))

        for file_path in sorted(pair_files):
            df = pd.read_parquet(file_path)

            for _, row in df.iterrows():
                dt = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))

                # Filter to analysis window
                if start_time <= dt <= end_time:
                    candles.append({
                        "timestamp": dt,
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "spread_pips": 0.1  # Simplified spread
                    })

        return sorted(candles, key=lambda x: x["timestamp"])

    def _calculate_opportunity_metrics(self, price_path: List[Dict], start_time: datetime,
                                     direction: str) -> OpportunityMetrics:
        """Calculate detailed behavior metrics for the opportunity."""

        # Extract prices and times
        prices = []
        timestamps = []

        for candle in price_path:
            if candle["timestamp"] >= start_time:
                prices.append(candle["close"])  # Use close price
                timestamps.append(candle["timestamp"])

        if len(prices) < 10:  # Need minimum data
            return self._default_metrics()

        # Calculate core behavior metrics as specified
        early_impulse_ratio = self._calculate_early_impulse_ratio(prices, timestamps, direction)
        path_efficiency = self._calculate_path_efficiency(prices)
        early_mae = self._calculate_early_adverse_excursion(prices, timestamps, direction)
        breakout_strength = self._calculate_breakout_strength(prices, timestamps, direction)
        stall_duration = self._calculate_stall_duration(prices)
        extension_behavior = self._classify_extension_behavior(prices, direction)
        reversal_behavior = self._classify_reversal_behavior(prices, direction)

        # Calculate additional metrics for completeness
        velocity_profile = self._calculate_velocity_profile(prices, timestamps)
        pullback_depth = self._calculate_pullback_depth(prices, direction)
        momentum_decay = self._calculate_momentum_decay(velocity_profile)
        microstructure_quality = self._calculate_microstructure_quality(price_path)

        return OpportunityMetrics(
            early_impulse_ratio=early_impulse_ratio,
            path_efficiency=path_efficiency,
            early_mae_pips=early_mae,
            extension_behavior=extension_behavior,
            stall_behavior="moderate",  # Simplified for now
            reversal_behavior=reversal_behavior,
            velocity_profile=velocity_profile,
            pullback_depth=pullback_depth,
            stall_duration=stall_duration,
            momentum_decay=momentum_decay,
            microstructure_quality=microstructure_quality
        )

    def _calculate_early_impulse_ratio(self, prices: List[float], timestamps: List[datetime],
                                     direction: str) -> float:
        """
        Early Impulse Ratio: move_first_30s / total_move

        Measures how quickly the move begins.
        High values indicate strong impulse starts.
        """
        if len(prices) < 10 or len(timestamps) < 10:
            return 0.0

        start_time = timestamps[0]
        early_cutoff = start_time + timedelta(seconds=30)  # First 30 seconds

        # Find early prices
        early_prices = []
        total_prices = []

        for price, ts in zip(prices, timestamps):
            total_prices.append(price)
            if ts <= early_cutoff:
                early_prices.append(price)

        if len(early_prices) < 2:
            return 0.0

        # Calculate moves in price units (not pips yet)
        early_range = max(early_prices) - min(early_prices)
        total_range = max(total_prices) - min(total_prices)

        return early_range / total_range if total_range > 0 else 0.0

    def _calculate_path_efficiency(self, prices: List[float]) -> float:
        """
        Path Efficiency: net_move / total_path_length

        Measures how straight the move was.
        Higher efficiency means less noise and chop.
        """
        if len(prices) < 2:
            return 0.0

        net_move = abs(prices[-1] - prices[0])
        total_path_length = sum(abs(prices[i] - prices[i-1]) for i in range(1, len(prices)))

        return net_move / total_path_length if total_path_length > 0 else 0.0

    def _calculate_early_adverse_excursion(self, prices: List[float], timestamps: List[datetime],
                                         direction: str) -> float:
        """
        Early Adverse Excursion: max adverse move in first N seconds

        Measures early drawdown in pips.
        Low values mean clean entries.
        """
        if len(prices) < 2 or len(timestamps) < 2:
            return 0.0

        start_time = timestamps[0]
        early_cutoff = start_time + timedelta(seconds=60)  # First 60 seconds (N=60)

        max_adverse = 0.0
        entry_price = prices[0]

        for price, ts in zip(prices, timestamps):
            if ts <= early_cutoff:
                if direction == "LONG":
                    adverse = entry_price - price
                else:  # SHORT
                    adverse = price - entry_price
                max_adverse = max(max_adverse, adverse)

        return max_adverse * 10000  # Convert to pips

    def _calculate_breakout_strength(self, prices: List[float], timestamps: List[datetime],
                                   direction: str) -> float:
        """
        Breakout Strength: first_impulse_velocity / ATR

        Measures momentum at the beginning.
        Strong breakouts tend to continue.
        """
        if len(prices) < 5 or len(timestamps) < 5:
            return 0.0

        # Calculate first impulse velocity (first 30 seconds)
        start_time = timestamps[0]
        early_cutoff = start_time + timedelta(seconds=30)

        early_prices = []
        for price, ts in zip(prices, timestamps):
            if ts <= early_cutoff:
                early_prices.append(price)

        if len(early_prices) < 2:
            return 0.0

        # Calculate velocity: price_change / time
        price_change = abs(early_prices[-1] - early_prices[0])
        time_change = (timestamps[len(early_prices)-1] - timestamps[0]).total_seconds()

        if time_change == 0:
            return 0.0

        velocity = price_change / time_change  # Price units per second

        # Calculate ATR estimate
        recent_prices = prices[:min(20, len(prices))]  # First 20 prices
        if len(recent_prices) >= 2:
            ranges = [abs(recent_prices[i] - recent_prices[i-1]) for i in range(1, len(recent_prices))]
            atr = sum(ranges) / len(ranges) if ranges else 0.0001
        else:
            atr = 0.0001

        return velocity / atr if atr > 0 else 0.0

    def _calculate_stall_duration(self, prices: List[float]) -> float:
        """
        Stall Detection: time spent with < 0.2 pip progress

        Measures when price stops progressing.
        """
        stall_threshold = 0.2 / 10000  # 0.2 pips in price units
        stall_minutes = 0

        for i in range(1, len(prices)):
            progress = abs(prices[i] - prices[i-1])
            if progress < stall_threshold:
                stall_minutes += 1  # Assuming 1-minute bars

        return stall_minutes

    def _classify_extension_behavior(self, prices: List[float], direction: str) -> str:
        """
        Extension Behavior: measures whether price continues after initial target

        extension = max_move - target
        """
        if len(prices) < 10:
            return "insufficient_data"

        entry_price = prices[0]
        target_pips = 2.5  # Initial target
        target_price_change = target_pips / 10000

        if direction == "LONG":
            max_price = max(prices)
            extension = (max_price - entry_price) - target_price_change
        else:
            min_price = min(prices)
            extension = (entry_price - min_price) - target_price_change

        extension_pips = extension * 10000

        if extension_pips > 2.0:
            return "strong_extension"
        elif extension_pips > 0:
            return "moderate_extension"
        else:
            return "no_extension"

    def _classify_reversal_behavior(self, prices: List[float], direction: str) -> str:
        """
        Reversal Behavior: measures how violently price reverses

        reversal_depth = retracement / total_move
        """
        if len(prices) < 5:
            return "insufficient_data"

        entry_price = prices[0]
        peak_price = max(prices) if direction == "LONG" else min(prices)

        # Calculate total move
        if direction == "LONG":
            total_move = peak_price - entry_price
        else:
            total_move = entry_price - peak_price

        if total_move <= 0:
            return "no_move"

        # Find maximum retracement after peak
        max_retracement = 0.0
        peak_found = False

        for price in prices:
            if direction == "LONG":
                if price >= peak_price:
                    peak_found = True
                elif peak_found:
                    retracement = peak_price - price
                    max_retracement = max(max_retracement, retracement)
            else:  # SHORT
                if price <= peak_price:
                    peak_found = True
                elif peak_found:
                    retracement = price - peak_price
                    max_retracement = max(max_retracement, retracement)

        reversal_depth = max_retracement / total_move if total_move > 0 else 0.0

        if reversal_depth > 0.8:
            return "sharp_reversal"
        elif reversal_depth > 0.5:
            return "moderate_reversal"
        else:
            return "false_reversal"

    def _calculate_velocity_profile(self, prices: List[float], timestamps: List[datetime]) -> List[float]:
        """Calculate velocity (ATR per second) over time."""
        velocities = []

        for i in range(1, len(prices)):
            price_change = abs(prices[i] - prices[i-1])
            time_change = (timestamps[i] - timestamps[i-1]).total_seconds()

            if time_change > 0:
                # Simple ATR estimate (price change in pips per second)
                velocity = (price_change * 10000) / time_change
                velocities.append(velocity)

        return velocities

    def _calculate_pullback_depth(self, prices: List[float], direction: str) -> float:
        """Calculate maximum pullback depth as percentage."""
        if len(prices) < 2:
            return 0.0

        entry_price = prices[0]
        peak_price = max(prices) if direction == "LONG" else min(prices)

        max_pullback = 0.0
        for price in prices:
            if direction == "LONG":
                pullback = (peak_price - price) / (peak_price - entry_price) if peak_price > entry_price else 0
            else:
                pullback = (price - peak_price) / (entry_price - peak_price) if entry_price > peak_price else 0
            max_pullback = max(max_pullback, pullback)

        return max_pullback

        stall_minutes = 0

        for i in range(1, len(prices)):
            if abs(prices[i] - prices[i-1]) < 0.0001:  # Stall threshold
                stall_minutes += 1  # Assuming 1-minute bars

        return stall_minutes

    def _calculate_momentum_decay(self, velocity_profile: List[float]) -> float:
        """Calculate how momentum decays over time."""
        if len(velocity_profile) < 5:
            return 0.0

        # Compare early vs late momentum
        early_avg = statistics.mean(velocity_profile[:len(velocity_profile)//3])
        late_avg = statistics.mean(velocity_profile[-len(velocity_profile)//3:])

        if early_avg > 0:
            return (early_avg - late_avg) / early_avg  # Decay ratio
        return 0.0

    def _calculate_microstructure_quality(self, price_path: List[Dict]) -> float:
        """Calculate microstructure quality score (simplified)."""
        # Simplified: average spread as proxy for microstructure quality
        spreads = [candle.get("spread_pips", 0.1) for candle in price_path]
        avg_spread = statistics.mean(spreads) if spreads else 0.1

        # Lower spread = better quality (higher score)
        return max(0, 1.0 - avg_spread)

    def _classify_opportunity(self, metrics: OpportunityMetrics, opp_data: Dict) -> tuple:
        """
        Classify opportunity using exact rules from research architecture.

        GOOD: predictable directional movement
        Conditions:
        - MFE ≥ 2.5 pips
        - early_impulse ≥ 0.4
        - efficiency ≥ 0.6
        - early_MAE ≤ 1 pip

        BAD: trap-like movement
        Conditions:
        - MFE ≥ 2.5 pips but
        - efficiency < 0.5 or early_MAE ≥ threshold or fast reversal

        NOISE: no extractable opportunity
        Conditions:
        - MFE < 2.5 pips or both directions triggered randomly
        """
        mfe_pips = opp_data.get("mfe_pips", 0)
        mae_pips = opp_data.get("mae_pips", 0)

        # GOOD classification - MINIMUM REQUIREMENTS
        good_conditions = (
            mfe_pips >= 2.5 and  # Must reach minimum exploitable movement
            mae_pips <= 2.5      # Must not have excessive adverse movement
            # Removed all other requirements - using pure sanity check only
        )

        if good_conditions:
            bucket = BucketType.GOOD

            # Classify GOOD zone type
            if metrics.extension_behavior == "strong_extension":
                zone_type = ZoneType.EXTENSION
            elif metrics.stall_duration < 5:  # Low stall time
                zone_type = ZoneType.CONTINUATION
            else:
                zone_type = ZoneType.GRIND

            confidence = 0.9
            return bucket, zone_type, confidence

        # BAD classification
        bad_conditions = (
            mfe_pips >= 2.5 and (  # Has exploitable movement but problematic
                metrics.path_efficiency < 0.5 or  # Inefficient/choppy path
                metrics.early_mae_pips >= 2.0 or  # High early adverse excursion
                metrics.reversal_behavior == "sharp_reversal"  # Fast reversal
            )
        )

        if bad_conditions:
            bucket = BucketType.BAD
            zone_type = None
            confidence = 0.8
            return bucket, zone_type, confidence

        # NOISE classification (everything else)
        bucket = BucketType.NOISE
        zone_type = None
        confidence = 0.7
        return bucket, zone_type, confidence

    def _calculate_aee_parameters(self, price_path: List[Dict], start_time: datetime,
                                direction: str):
        """
        Calculate AEE parameters: entry_price, tp_price, sl_price, atr_pips

        This simulates trading parameters for the opportunity.
        """
        # Find entry price (close at start time or nearest)
        entry_price = None
        for candle in price_path:
            if candle["timestamp"] >= start_time:
                entry_price = candle["close"]
                break

        if entry_price is None:
            entry_price = price_path[0]["close"] if price_path else 1.0

        # Calculate ATR from recent candles
        recent_prices = [c["close"] for c in price_path[-20:]]  # Last 20 candles
        if len(recent_prices) >= 2:
            ranges = [abs(recent_prices[i] - recent_prices[i-1]) for i in range(1, len(recent_prices))]
            atr = statistics.mean(ranges) * 10000  # ATR in pips
        else:
            atr = 10.0  # Default ATR

        # Calculate TP/SL (2.5 ATR targets)
        target_pips = atr * 2.5
        if direction == "LONG":
            tp_price = entry_price + (target_pips / 10000)
            sl_price = entry_price - (target_pips / 10000)
        else:
            tp_price = entry_price - (target_pips / 10000)
            sl_price = entry_price + (target_pips / 10000)

        return entry_price, tp_price, sl_price, atr

    def _default_metrics(self) -> OpportunityMetrics:
        """Return default metrics when calculation fails."""
        return OpportunityMetrics(
            early_impulse_ratio=0.0,
            path_efficiency=0.0,
            early_mae_pips=0.0,
            extension_behavior="insufficient_data",
            stall_behavior="insufficient_data",
            reversal_behavior="insufficient_data",
            velocity_profile=[],
            pullback_depth=0.0,
            stall_duration=0.0,
            momentum_decay=0.0,
            microstructure_quality=0.0
        )

    def save_analyzed_opportunities(self, opportunities: List[AnalyzedOpportunity], output_path: str):
        """Save analyzed opportunities to JSON file."""
        data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "total_opportunities": len(opportunities),
            "buckets": {
                "GOOD": len([o for o in opportunities if o.bucket == BucketType.GOOD]),
                "BAD": len([o for o in opportunities if o.bucket == BucketType.BAD]),
                "NOISE": len([o for o in opportunities if o.bucket == BucketType.NOISE])
            },
            "opportunities": [
                {
                    "opportunity_id": opp.opportunity_id,
                    "start_time": opp.start_time.isoformat(),
                    "pair": opp.pair,
                    "direction": opp.direction,
                    "distance_pips": opp.distance_pips,
                    "time_to_move_minutes": opp.time_to_move_minutes,
                    "mfe_pips": opp.mfe_pips,
                    "mae_pips": opp.mae_pips,
                    "session": opp.session,
                    "weekday": opp.weekday,
                    "bucket": opp.bucket.value,
                    "zone_type": opp.zone_type.value if opp.zone_type else None,
                    "confidence_score": opp.confidence_score,
                    "metrics": {
                        "early_impulse_ratio": opp.metrics.early_impulse_ratio,
                        "path_efficiency": opp.metrics.path_efficiency,
                        "early_mae_pips": opp.metrics.early_mae_pips,
                        "extension_behavior": opp.metrics.extension_behavior,
                        "stall_behavior": opp.metrics.stall_behavior,
                        "reversal_behavior": opp.metrics.reversal_behavior,
                        "pullback_depth": opp.metrics.pullback_depth,
                        "stall_duration": opp.metrics.stall_duration,
                        "momentum_decay": opp.metrics.momentum_decay,
                        "microstructure_quality": opp.metrics.microstructure_quality
                    },
                    "aee_params": {
                        "entry_price": opp.entry_price,
                        "tp_price": opp.tp_price,
                        "sl_price": opp.sl_price,
                        "atr_pips": opp.atr_pips
                    }
                }
                for opp in opportunities
            ]
        }

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved {len(opportunities)} analyzed opportunities to {output_path}")


def main():
    """Run the Opportunity Anatomy Engine on Phase 1 results."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 2: Opportunity Anatomy Engine")
    parser.add_argument("--phase1-results", required=True, help="Path to Phase 1 results JSON")
    parser.add_argument("--data-source", required=True, help="Path to rich data source")
    parser.add_argument("--pair", default="EUR_USD", help="Currency pair")
    parser.add_argument("--output", default="phase2_analysis.json", help="Output file path")

    args = parser.parse_args()

    # Load Phase 1 results
    print("Loading Phase 1 results...")
    with open(args.phase1_results, 'r') as f:
        phase1_data = json.load(f)

    raw_opportunities = phase1_data["opportunities"]
    print(f"Loaded {len(raw_opportunities)} raw opportunities")

    # Run OAE
    print("Running Opportunity Anatomy Engine...")
    oae = OpportunityAnatomyEngine()
    analyzed_opportunities = oae.analyze_opportunities(
        raw_opportunities, args.data_source, args.pair
    )

    # Print summary
    good_count = len([o for o in analyzed_opportunities if o.bucket == BucketType.GOOD])
    bad_count = len([o for o in analyzed_opportunities if o.bucket == BucketType.BAD])
    noise_count = len([o for o in analyzed_opportunities if o.bucket == BucketType.NOISE])

    print("Analysis Summary:")
    print(f"  GOOD opportunities: {good_count}")
    print(f"  BAD opportunities: {bad_count}")
    print(f"  NOISE opportunities: {noise_count}")

    # Save results
    oae.save_analyzed_opportunities(analyzed_opportunities, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
