#!/usr/bin/env python3
"""
Phase 1 - Opportunity Discovery Engine (ODE)

Purpose: Find every directional movement that exists in the market without trying to predict it.
This creates the raw opportunity dataset.

Input: Minimal dataset (timestamp, price, pair, session, weekday)
Output: Raw opportunity records with MFE, MAE, tau, distance, session, weekday
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from enum import Enum


class Session(Enum):
    """Trading session."""
    LONDON = "london"
    NEW_YORK = "new_york"
    ASIA = "asia"


@dataclass
class RawOpportunity:
    """Raw opportunity discovered by ODE."""
    start_time: datetime
    pair: str
    direction: str  # "LONG" or "SHORT"
    distance_pips: float  # Movement size in pips (MFE)
    time_to_move_minutes: float  # tau - Time to reach target
    mfe_pips: float  # Max Favorable Excursion
    mae_pips: float  # Max Adverse Excursion
    session: Session
    weekday: str


class OpportunityDiscoveryEngine:
    """
    Phase 1: Find all exploitable moves in raw price data.

    Core Discovery Rules:
    - For every minute t, treat price P[t] as potential start
    - Look forward H = 100 minutes
    - LONG opportunity: MFE_up = max(P[t+k] - P[t]) ≥ 2.5 pips
    - SHORT opportunity: MFE_down = max(P[t] - P[t+k]) ≥ 2.5 pips
    - tau = first k where movement reaches 2.5 pips
    """

    def __init__(self, min_movement_pips: float = 2.5, max_time_minutes: int = 100):
        self.min_movement_pips = min_movement_pips  # Minimum exploitable movement
        self.max_time_minutes = max_time_minutes    # Maximum look-ahead window

    def discover_opportunities(self, price_data: List[Dict[str, Any]]) -> List[RawOpportunity]:
        """
        Scan price series and find all movements ≥ min_movement_pips within max_time_minutes.

        Args:
            price_data: List of dicts with keys: timestamp, price, pair, session, weekday

        Returns:
            List of RawOpportunity objects
        """
        opportunities = []

        # Convert timestamps to datetime objects
        processed_data = []
        for row in price_data:
            dt = self._parse_timestamp(row["timestamp"])
            processed_data.append({
                "datetime": dt,
                "price": row["price"],
                "pair": row["pair"],
                "session": Session(row.get("session", "london").lower()),
                "weekday": row.get("weekday", "monday")
            })

        # Scan every timestamp as potential origin (no overlapping removal)
        for i, origin in enumerate(processed_data):
            # Check LONG opportunities
            long_opp = self._check_direction_opportunity(
                processed_data, i, "LONG", origin["session"], origin["weekday"]
            )
            if long_opp:
                opportunities.append(long_opp)

            # Check SHORT opportunities
            short_opp = self._check_direction_opportunity(
                processed_data, i, "SHORT", origin["session"], origin["weekday"]
            )
            if short_opp:
                opportunities.append(short_opp)

        return opportunities

    def _check_direction_opportunity(self, data: List[Dict], start_idx: int,
                                   direction: str, session: Session, weekday: str) -> Optional[RawOpportunity]:
        """
        Check if a directional opportunity exists from start_idx.

        LONG opportunity:
        MFE_up(t) = max(P[t+k] - P[t]) for k = 1..100
        If MFE_up ≥ 2.5 pips, then LONG opportunity exists

        SHORT opportunity:
        MFE_down(t) = max(P[t] - P[t+k]) for k = 1..100
        If MFE_down ≥ 2.5 pips, then SHORT opportunity exists

        tau = first k where movement reaches 2.5 pips
        """
        origin = data[start_idx]
        origin_price = origin["price"]
        origin_time = origin["datetime"]

        # Look forward up to max_time_minutes
        max_lookahead = self.max_time_minutes  # minutes = data points (1-minute bars)

        # Track movement metrics
        max_favorable_excursion = 0.0  # MFE
        max_adverse_excursion = 0.0    # MAE
        time_to_move = None            # tau
        peak_price = origin_price
        trough_price = origin_price

        for k in range(1, min(max_lookahead + 1, len(data) - start_idx)):
            current = data[start_idx + k]
            current_price = current["price"]

            # Update MFE and MAE
            if direction == "LONG":
                # LONG: favorable = higher prices, adverse = lower prices
                favorable_move = current_price - origin_price
                adverse_move = origin_price - min(trough_price, current_price)
                trough_price = min(trough_price, current_price)
            else:  # SHORT
                # SHORT: favorable = lower prices, adverse = higher prices
                favorable_move = origin_price - current_price
                adverse_move = max(peak_price, current_price) - origin_price
                peak_price = max(peak_price, current_price)

            max_favorable_excursion = max(max_favorable_excursion, favorable_move)
            max_adverse_excursion = max(max_adverse_excursion, adverse_move)

            # Check if we've reached the minimum movement (set tau if not set)
            if time_to_move is None and max_favorable_excursion >= self.min_movement_pips / 10000:
                time_to_move = k  # tau = k minutes

        # Convert to pips
        mfe_pips = max_favorable_excursion * 10000
        mae_pips = max_adverse_excursion * 10000
        distance_pips = mfe_pips  # Distance = MFE

        # Only create opportunity if minimum movement achieved
        if mfe_pips >= self.min_movement_pips and time_to_move is not None:
            return RawOpportunity(
                start_time=origin_time,
                pair=origin["pair"],
                direction=direction,
                distance_pips=distance_pips,
                time_to_move_minutes=time_to_move,
                mfe_pips=mfe_pips,
                mae_pips=mae_pips,
                session=session,
                weekday=weekday
            )

        return None

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(timestamp_str)
        except ValueError:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    def save_opportunities(self, opportunities: List[RawOpportunity], output_path: str):
        """Save discovered opportunities to JSON file."""
        data = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "min_movement_pips": self.min_movement_pips,
            "max_time_minutes": self.max_time_minutes,
            "total_opportunities": len(opportunities),
            "opportunities": [
                {
                    "start_time": opp.start_time.isoformat(),
                    "pair": opp.pair,
                    "direction": opp.direction,
                    "distance_pips": opp.distance_pips,
                    "time_to_move_minutes": opp.time_to_move_minutes,
                    "mfe_pips": opp.mfe_pips,
                    "mae_pips": opp.mae_pips,
                    "session": opp.session.value,
                    "weekday": opp.weekday
                }
                for opp in opportunities
            ]
        }

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved {len(opportunities)} opportunities to {output_path}")


def load_oanda_data(data_root: str, pair: str = "EUR_USD") -> List[Dict[str, Any]]:
    """
    Load Oanda candle data and convert to ODE format.

    Expected format: data_root/pair=PAIR/year=YYYY/month=MM/part-000.parquet
    """
    import pandas as pd

    data = []

    # Find all parquet files for the pair
    data_path = Path(data_root)
    pair_files = list(data_path.glob(f"pair={pair}/year=*/month=*/part-*.parquet"))

    for file_path in sorted(pair_files):
        df = pd.read_parquet(file_path)

        for _, row in df.iterrows():
            # Determine session based on hour (simplified)
            dt = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            hour = dt.hour

            if 8 <= hour < 16:  # 8 AM to 4 PM UTC = London session
                session = "london"
            elif 13 <= hour < 21:  # 1 PM to 9 PM UTC = New York session
                session = "new_york"
            else:
                session = "asia"

            data.append({
                "timestamp": str(row["timestamp"]),
                "price": row["close"],  # Use close price
                "pair": pair,
                "session": session,
                "weekday": dt.strftime("%A").lower()
            })

    print(f"Loaded {len(data)} price points for {pair}")
    return data


def main():
    """Run the Opportunity Discovery Engine on Oanda data."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 1: Opportunity Discovery Engine")
    parser.add_argument("--data-root", required=True, help="Path to Oanda data directory")
    parser.add_argument("--pair", default="EUR_USD", help="Currency pair")
    parser.add_argument("--min-movement", type=float, default=2.5, help="Minimum movement in pips")
    parser.add_argument("--max-time", type=int, default=100, help="Maximum time in minutes")
    parser.add_argument("--output", default="phase1_opportunities.json", help="Output file path")

    args = parser.parse_args()

    # Load data
    print("Loading Oanda data...")
    price_data = load_oanda_data(args.data_root, args.pair)

    if not price_data:
        print("No data loaded!")
        return 1

    # Run ODE
    print(f"Running Opportunity Discovery Engine...")
    print(f"  Min movement: {args.min_movement} pips")
    print(f"  Max time: {args.max_time} minutes")

    ode = OpportunityDiscoveryEngine(args.min_movement, args.max_time)
    opportunities = ode.discover_opportunities(price_data)

    print(f"Discovered {len(opportunities)} opportunities")

    # Save results
    ode.save_opportunities(opportunities, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
