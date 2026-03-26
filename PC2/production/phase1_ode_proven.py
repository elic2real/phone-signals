#!/usr/bin/env python3
"""
Phase 1 - Opportunity Discovery Engine (ODE) - Proof-First Implementation

PURPOSE: Find every move of 2.5 pips or more in under 100 minutes from every 1-minute starting point.

REQUIRED FORMULAS:
For each minute t:
- MFE_up(t) = max(P[t+k] - P[t]) for k = 1..H
- MFE_down(t) = max(P[t] - P[t+k]) for k = 1..H
- tau_up(t) = smallest k such that P[t+k] - P[t] >= 2.5 pips
- tau_down(t) = smallest k such that P[t] - P[t+k] >= 2.5 pips
- MAE_up(t) = max(P[t] - P[t+j]) for j = 1..tau_up(t)
- MAE_down(t) = max(P[t+j] - P[t]) for j = 1..tau_down(t)

REQUIRED OUTPUT FILES:
- opportunity_map_raw.csv (exact columns)
- opportunity_map_summary.json (exact counts/stats)
- opportunity_map_audit.json (exact proofs/validations)

FAIL CONDITIONS:
- Missing timestamps in processed range
- Negative taus
- up_exists=1 but mfe_up_pips < 2.5
- down_exists=1 but mfe_down_pips < 2.5
"""

from __future__ import annotations
import json
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict

import pandas as pd


class OpportunityDiscoveryEngine:
    """
    Phase 1: Find every exploitable move using exact formulas.
    """

    def __init__(self, min_movement_pips: float = 2.5, max_time_minutes: int = 100):
        self.min_movement_pips = min_movement_pips  # Δ = 2.5 pips
        self.max_time_minutes = max_time_minutes    # H = 100 minutes
        self.pip_multiplier = 10000  # For EURUSD (4 decimal places)

    def discover_opportunities(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Scan every minute t and calculate exact metrics.

        Args:
            price_data: List of dicts with timestamp, price, pair, session, weekday

        Returns:
            List of opportunity records with exact calculated metrics
        """
        opportunities = []

        # Process each timestamp as potential origin
        for i, origin in enumerate(price_data):
            opp = self._calculate_opportunity_metrics(price_data, i)
            opportunities.append(opp)

        return opportunities

    def _calculate_opportunity_metrics(self, data: List[Dict], start_idx: int) -> Dict[str, Any]:
        """
        Calculate exact opportunity metrics for timestamp at start_idx.

        Formulas:
        - MFE_up(t) = max(P[t+k] - P[t]) for k = 1..H
        - MFE_down(t) = max(P[t] - P[t+k]) for k = 1..H
        - tau_up(t) = min k such that P[t+k] - P[t] >= Δ
        - tau_down(t) = min k such that P[t] - P[t+k] >= Δ
        - MAE_up(t) = max(P[t] - P[t+j]) for j = 1..tau_up(t)
        - MAE_down(t) = max(P[t+j] - P[t]) for j = 1..tau_down(t)
        """
        origin = data[start_idx]
        origin_price = origin["price"]

        # Initialize metrics
        mfe_up = 0.0
        mfe_down = 0.0
        tau_up = None
        tau_down = None
        mae_up = 0.0
        mae_down = 0.0

        # Look forward H minutes
        max_lookahead = min(self.max_time_minutes, len(data) - start_idx - 1)

        for k in range(1, max_lookahead + 1):
            current_price = data[start_idx + k]["price"]

            # Calculate MFE (running maximum)
            price_diff_up = current_price - origin_price
            price_diff_down = origin_price - current_price

            mfe_up = max(mfe_up, price_diff_up)
            mfe_down = max(mfe_down, price_diff_down)

            # Check for tau (first time reaching Δ)
            if tau_up is None and price_diff_up >= self.min_movement_pips / self.pip_multiplier:
                tau_up = k
                # Calculate MAE_up up to tau
                for j in range(1, tau_up + 1):
                    adverse_up = origin_price - data[start_idx + j]["price"]
                    mae_up = max(mae_up, adverse_up)

            if tau_down is None and price_diff_down >= self.min_movement_pips / self.pip_multiplier:
                tau_down = k
                # Calculate MAE_down up to tau
                for j in range(1, tau_down + 1):
                    adverse_down = data[start_idx + j]["price"] - origin_price
                    mae_down = max(mae_down, adverse_down)

        # Convert to pips
        mfe_up_pips = mfe_up * self.pip_multiplier
        mfe_down_pips = mfe_down * self.pip_multiplier
        mae_up_pips = mae_up * self.pip_multiplier
        mae_down_pips = mae_down * self.pip_multiplier

        # Determine if opportunities exist
        up_exists = 1 if tau_up is not None else 0
        down_exists = 1 if tau_down is not None else 0

        return {
            "timestamp": origin["timestamp"],
            "price": origin["price"],
            "session": origin["session"],
            "weekday": origin["weekday"],
            "mfe_up_pips": round(mfe_up_pips, 4),
            "mfe_down_pips": round(mfe_down_pips, 4),
            "tau_up_min": tau_up,
            "tau_down_min": tau_down,
            "mae_up_pips": round(mae_up_pips, 4),
            "mae_down_pips": round(mae_down_pips, 4),
            "up_exists": up_exists,
            "down_exists": down_exists
        }

    def save_opportunity_map_raw(self, opportunities: List[Dict], output_path: str):
        """Save opportunity_map_raw.csv with exact required columns."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "timestamp", "price", "session", "weekday",
                "mfe_up_pips", "mfe_down_pips", "tau_up_min", "tau_down_min",
                "mae_up_pips", "mae_down_pips", "up_exists", "down_exists"
            ])
            writer.writeheader()
            writer.writerows(opportunities)

        print(f"Created opportunity_map_raw.csv: {len(opportunities)} rows")

    def generate_summary_report(self, opportunities: List[Dict]) -> Dict[str, Any]:
        """Generate opportunity_map_summary.json with exact required metrics."""
        total_rows = len(opportunities)

        # Count opportunity types
        long_only = 0
        short_only = 0
        both = 0
        none = 0

        for opp in opportunities:
            up_exists = opp["up_exists"]
            down_exists = opp["down_exists"]

            if up_exists and down_exists:
                both += 1
            elif up_exists:
                long_only += 1
            elif down_exists:
                short_only += 1
            else:
                none += 1

        # Opportunities per hour (assuming 9-hour London session)
        london_session_hours = 9
        opps_per_hour = {
            "long_only": long_only / london_session_hours,
            "short_only": short_only / london_session_hours,
            "both": both / london_session_hours,
            "none": none / london_session_hours
        }

        # Opportunities by weekday
        weekday_counts = defaultdict(lambda: {"long_only": 0, "short_only": 0, "both": 0, "none": 0})
        for opp in opportunities:
            weekday = opp["weekday"]
            up_exists = opp["up_exists"]
            down_exists = opp["down_exists"]

            if up_exists and down_exists:
                weekday_counts[weekday]["both"] += 1
            elif up_exists:
                weekday_counts[weekday]["long_only"] += 1
            elif down_exists:
                weekday_counts[weekday]["short_only"] += 1
            else:
                weekday_counts[weekday]["none"] += 1

        # Opportunities by session (all should be london)
        session_counts = defaultdict(lambda: {"long_only": 0, "short_only": 0, "both": 0, "none": 0})
        for opp in opportunities:
            session = opp["session"]
            up_exists = opp["up_exists"]
            down_exists = opp["down_exists"]

            if up_exists and down_exists:
                session_counts[session]["both"] += 1
            elif up_exists:
                session_counts[session]["long_only"] += 1
            elif down_exists:
                session_counts[session]["short_only"] += 1
            else:
                session_counts[session]["none"] += 1

        return {
            "total_rows_processed": total_rows,
            "total_LONG_opportunities": long_only,
            "total_SHORT_opportunities": short_only,
            "total_BOTH_opportunities": both,
            "total_NONE_opportunities": none,
            "opportunities_per_hour": opps_per_hour,
            "opportunities_by_weekday": dict(weekday_counts),
            "opportunities_by_session": dict(session_counts)
        }

    def generate_audit_report(self, opportunities: List[Dict], original_data: List[Dict]) -> Dict[str, Any]:
        """Generate opportunity_map_audit.json with exact validation proofs."""
        audit_results = {
            "timestamp_range_check": self._audit_timestamp_range(original_data),
            "no_negative_taus": self._audit_no_negative_taus(opportunities),
            "up_exists_validation": self._audit_up_exists_validation(opportunities),
            "down_exists_validation": self._audit_down_exists_validation(opportunities)
        }

        # Determine overall pass/fail
        all_passed = all(result["passed"] for result in audit_results.values())
        audit_results["overall_phase1_status"] = "PHASE1_PASS" if all_passed else "PHASE1_FAIL"

        return audit_results

    def _audit_timestamp_range(self, original_data: List[Dict]) -> Dict[str, Any]:
        """Audit: no missing timestamps in the processed range."""
        timestamps = [self._parse_timestamp(row["timestamp"]) for row in original_data]
        timestamps.sort()

        # Check for gaps (assuming 1-minute intervals)
        gaps_found = 0
        for i in range(1, len(timestamps)):
            gap_minutes = (timestamps[i] - timestamps[i-1]).total_seconds() / 60
            if gap_minutes > 1.1:  # Allow small tolerance
                gaps_found += 1

        passed = gaps_found == 0
        return {
            "check": "no_missing_timestamps",
            "passed": passed,
            "details": f"Found {gaps_found} gaps > 1 minute",
            "total_timestamps": len(timestamps)
        }

    def _audit_no_negative_taus(self, opportunities: List[Dict]) -> Dict[str, Any]:
        """Audit: no negative taus."""
        negative_taus = 0
        for opp in opportunities:
            if opp["tau_up_min"] is not None and opp["tau_up_min"] <= 0:
                negative_taus += 1
            if opp["tau_down_min"] is not None and opp["tau_down_min"] <= 0:
                negative_taus += 1

        passed = negative_taus == 0
        return {
            "check": "no_negative_taus",
            "passed": passed,
            "details": f"Found {negative_taus} negative tau values"
        }

    def _audit_up_exists_validation(self, opportunities: List[Dict]) -> Dict[str, Any]:
        """Audit: every up_exists=1 has mfe_up_pips >= 2.5."""
        violations = 0
        for opp in opportunities:
            if opp["up_exists"] == 1 and opp["mfe_up_pips"] < 2.5:
                violations += 1

        passed = violations == 0
        return {
            "check": "up_exists_mfe_validation",
            "passed": passed,
            "details": f"Found {violations} up_exists=1 with mfe_up_pips < 2.5"
        }

    def _audit_down_exists_validation(self, opportunities: List[Dict]) -> Dict[str, Any]:
        """Audit: every down_exists=1 has mfe_down_pips >= 2.5."""
        violations = 0
        for opp in opportunities:
            if opp["down_exists"] == 1 and opp["mfe_down_pips"] < 2.5:
                violations += 1

        passed = violations == 0
        return {
            "check": "down_exists_mfe_validation",
            "passed": passed,
            "details": f"Found {violations} down_exists=1 with mfe_down_pips < 2.5"
        }

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


def load_oanda_data(data_root: str, pair: str = "EUR_USD") -> List[Dict[str, Any]]:
    """Load Oanda candle data for ODE processing."""
    import pandas as pd

    data = []
    data_path = Path(data_root)
    pair_files = list(data_path.glob(f"pair={pair}/year=*/month=*/part-*.parquet"))

    # Schema mapping (tight fallback set)
    # Confirmed schemas in this repo:
    # - raw ohlc: timestamp + close
    # - engineered m1: ts_utc + m1_close
    ts_candidates = ["timestamp", "time", "ts_utc"]
    close_candidates = ["close", "mid_c", "c", "m1_close"]

    for file_path in sorted(pair_files):
        # Read full schema first so we can fail fast with a clear message.
        df_full = pd.read_parquet(file_path)
        cols = set(df_full.columns)

        ts_col = next((c for c in ts_candidates if c in cols), None)
        close_col = next((c for c in close_candidates if c in cols), None)
        if ts_col is None or close_col is None:
            raise ValueError(
                "PHASE1_SCHEMA_ERROR: Parquet missing required fields. "
                f"expected timestamp in {ts_candidates}, close in {close_candidates}. "
                f"found_columns={sorted(cols)} file={file_path}"
            )

        # Reduce to only required columns.
        df = df_full[[ts_col, close_col]].rename(columns={ts_col: "timestamp", close_col: "close"})

        for _, row in df.iterrows():
            dt = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            hour = dt.hour

            # Determine session
            if 8 <= hour < 16:  # London session
                session = "london"
            else:
                session = "other"

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
    """Run Phase 1 - ODE with proof-first requirements."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 1: Opportunity Discovery Engine")
    parser.add_argument("--data-root", required=True, help="Path to Oanda data directory")
    parser.add_argument("--pair", default="EUR_USD", help="Currency pair")
    parser.add_argument("--output-dir", default="phase1_outputs", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    print("Phase 1: Loading Oanda data...")
    price_data = load_oanda_data(args.data_root, args.pair)

    if not price_data:
        print("ERROR: No data loaded!")
        return 1

    # Run ODE
    print("Phase 1: Running Opportunity Discovery Engine...")
    ode = OpportunityDiscoveryEngine(min_movement_pips=2.5, max_time_minutes=100)
    opportunities = ode.discover_opportunities(price_data)

    print(f"Phase 1: Processed {len(opportunities)} timestamps")

    # Save required outputs
    print("Phase 1: Generating required output files...")

    # 1. opportunity_map_raw.csv
    csv_path = output_dir / "opportunity_map_raw.csv"
    ode.save_opportunity_map_raw(opportunities, str(csv_path))

    # 2. opportunity_map_summary.json
    summary = ode.generate_summary_report(opportunities)
    summary_path = output_dir / "opportunity_map_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    # 3. opportunity_map_audit.json
    audit = ode.generate_audit_report(opportunities, price_data)
    audit_path = output_dir / "opportunity_map_audit.json"
    with open(audit_path, 'w') as f:
        json.dump(audit, f, indent=2)

    # Print phase completion status
    phase_status = audit["overall_phase1_status"]
    print(f"\nPhase 1 Status: {phase_status}")

    if phase_status == "PHASE1_PASS":
        print("✅ Phase 1 COMPLETED: All required files and proofs generated")
        print(f"   - opportunity_map_raw.csv: {len(opportunities)} rows")
        print(f"   - opportunity_map_summary.json: {summary['total_rows_processed']} processed")
        print(f"   - opportunity_map_audit.json: All validations passed")
    else:
        print("❌ Phase 1 FAILED: See opportunity_map_audit.json for details")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
