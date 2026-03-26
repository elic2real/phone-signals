"""Debug version of AEE vs Static TP/SL Analysis with detailed logging."""

import json
from typing import List, Dict, Any
from dataclasses import dataclass
from unified_pipeline import UnifiedReverseEngineeringPipeline, BucketType, generate_sample_historical_data


@dataclass
class StaticExitResult:
    """Results from static TP/SL simulation."""
    target_r: float
    avg_r: float
    win_rate: float
    total_trades: int
    profitable_trades: int
    avg_win_r: float
    avg_loss_r: float
    max_r: float
    min_r: float


@dataclass
class AEEExitResult:
    """Results from AEE simulation."""
    avg_r: float
    win_rate: float
    total_trades: int
    profitable_trades: int
    avg_win_r: float
    avg_loss_r: float
    max_r: float
    min_r: float
    exit_reason_distribution: Dict[str, int]


def run_static_baseline_simulation(zones: List[Any], target_r: float) -> StaticExitResult:
    """Run static TP/SL simulation at given target size."""

    results = []
    profitable_trades = 0

    for zone in zones:
        # Calculate TP/SL prices based on target R
        entry_price = zone.entry_price
        atr_risk = zone.atr_pips / 10000  # Convert to price units

        if zone.direction == "LONG":
            tp_price = entry_price + (target_r * atr_risk)
            sl_price = entry_price - (1.0 * atr_risk)  # 1R stop
        else:  # SHORT
            tp_price = entry_price - (target_r * atr_risk)
            sl_price = entry_price + (1.0 * atr_risk)  # 1R stop

        # Simulate static exit using zone's forward path
        exit_r = simulate_static_exit(zone.forward_path, entry_price, tp_price, sl_price, zone.direction)

        results.append(exit_r)
        if exit_r > 0:
            profitable_trades += 1

    # Calculate metrics
    total_trades = len(results)
    avg_r = sum(results) / len(results) if results else 0
    win_rate = profitable_trades / total_trades if total_trades > 0 else 0

    profitable_results = [r for r in results if r > 0]
    losing_results = [r for r in results if r < 0]

    avg_win_r = sum(profitable_results) / len(profitable_results) if profitable_results else 0
    avg_loss_r = sum(losing_results) / len(losing_results) if losing_results else 0
    max_r = max(results) if results else 0
    min_r = min(results) if results else 0

    return StaticExitResult(
        target_r=target_r,
        avg_r=avg_r,
        win_rate=win_rate,
        total_trades=total_trades,
        profitable_trades=profitable_trades,
        avg_win_r=avg_win_r,
        avg_loss_r=avg_loss_r,
        max_r=max_r,
        min_r=min_r
    )


def simulate_static_exit(forward_path: List[tuple], entry_price: float, tp_price: float, sl_price: float, direction: str) -> float:
    """Simulate static TP/SL exit on forward path."""

    for timestamp, price in forward_path:
        if direction == "LONG":
            if price >= tp_price:
                return (price - entry_price) / (entry_price - sl_price)  # R calculation
            elif price <= sl_price:
                return -1.0  # SL hit
        else:  # SHORT
            if price <= tp_price:
                return (entry_price - price) / (sl_price - entry_price)  # R calculation
            elif price >= sl_price:
                return -1.0  # SL hit

    # Timeout - use final price
    final_price = forward_path[-1][1] if forward_path else entry_price
    if direction == "LONG":
        return (final_price - entry_price) / (entry_price - sl_price)
    else:
        return (entry_price - final_price) / (sl_price - entry_price)


def run_aee_simulation(zones: List[Any]) -> AEEExitResult:
    """Run AEE simulation on zones with debug logging."""

    from aee_synthetic_evaluator import AEEEvaluator, AEEKnobs

    # Use fitted AEE parameters from unified pipeline
    aee_knobs = AEEKnobs(
        profit_capture_min_atr=0.35,
        allowed_giveback_atr_mult=0.45
    )

    evaluator = AEEEvaluator(aee_knobs)

    results = []
    exit_reasons = {}
    profitable_trades = 0

    print("\n🔍 DEBUG: Starting AEE evaluation with parameters:")
    print(f"  profit_capture_min_atr: {aee_knobs.profit_capture_min_atr}")
    print(f"  allowed_giveback_atr_mult: {aee_knobs.allowed_giveback_atr_mult}")
    print(f"  panic_velocity: {aee_knobs.panic_velocity}")
    print(f"  decay_min_hold_sec: {aee_knobs.decay_min_hold_sec}")
    print(f"  Total zones to evaluate: {len(zones)}")

    for i, zone in enumerate(zones):
        print(f"\n📊 DEBUG: Evaluating zone {i+1}/{len(zones)} - {zone.direction} bucket: {zone.bucket}")

        # Log zone characteristics
        duration = zone.forward_path[-1][0] - zone.forward_path[0][0] if zone.forward_path else 0
        max_price = max(p for _, p in zone.forward_path) if zone.forward_path else zone.entry_price
        min_price = min(p for _, p in zone.forward_path) if zone.forward_path else zone.entry_price
        price_range = max_price - min_price
        atr_range = price_range / (zone.atr_pips / 10000) if zone.atr_pips > 0 else 0

        print(".1f")
        print(".3f")
        print(f"  Duration: {duration:.1f} seconds")
        print(f"  Data points: {len(zone.forward_path)}")

        # Convert zone to synthetic path format for AEE evaluation
        path = zone_to_synthetic_path(zone)

        # Run AEE evaluation
        aee_result = evaluator.evaluate_path(path)

        r_result = aee_result["actual_r"]
        results.append(r_result)

        if r_result > 0:
            profitable_trades += 1

        # Track exit reasons
        exit_reason = aee_result["exit_reason"]
        exit_reasons[exit_reason] = exit_reasons.get(exit_reason, 0) + 1

        print(f"  Result: {exit_reason} at R={r_result:.3f}")

        # Log detailed exit analysis
        if exit_reason == "TIMEOUT":
            print("  ❌ TIMEOUT: No exit conditions triggered during entire path")
            print(f"     Peak progress: {aee_result['mfe_r']:.3f} R")
            print("     Possible reasons:")
            print("     - Profit never reached 0.35 ATR threshold")
            print("     - Velocity/pullback conditions never met")
            print("     - No panic/decay/giveback triggers")
        else:
            print(f"  ✅ {exit_reason}: Exit condition met")

    # Summary statistics
    timeout_count = exit_reasons.get("TIMEOUT", 0)
    timeout_rate = timeout_count / len(zones) * 100 if zones else 0

    print("\n📈 DEBUG: AEE Evaluation Summary:")
    print(f"  Total zones: {len(zones)}")
    print(".1f")
    print(f"  Exit reasons: {exit_reasons}")
    print(".3f")
    print(".3f")
    print(".3f")
    print(".3f")

    # Calculate metrics
    total_trades = len(results)
    avg_r = sum(results) / len(results) if results else 0
    win_rate = profitable_trades / total_trades if total_trades > 0 else 0

    profitable_results = [r for r in results if r > 0]
    losing_results = [r for r in results if r < 0]

    avg_win_r = sum(profitable_results) / len(profitable_results) if profitable_results else 0
    avg_loss_r = sum(losing_results) / len(losing_results) if losing_results else 0
    max_r = max(results) if results else 0
    min_r = min(results) if results else 0

    return AEEExitResult(
        avg_r=avg_r,
        win_rate=win_rate,
        total_trades=total_trades,
        profitable_trades=profitable_trades,
        avg_win_r=avg_win_r,
        avg_loss_r=avg_loss_r,
        max_r=max_r,
        min_r=min_r,
        exit_reason_distribution=exit_reasons
    )


def zone_to_synthetic_path(zone):
    """Convert unified zone to synthetic path format."""
    from synthetic_path_generator import SyntheticPath

    timestamps = [ts for ts, _ in zone.forward_path]
    mid_prices = [price for _, price in zone.forward_path]
    spreads = [0.0001] * len(mid_prices)  # 1 pip spread

    # Calculate TP/SL (2R target, 1R stop)
    atr_risk = zone.atr_pips / 10000
    if zone.direction == "LONG":
        tp_price = zone.entry_price + (2.0 * atr_risk)
        sl_price = zone.entry_price - (1.0 * atr_risk)
    else:
        tp_price = zone.entry_price - (2.0 * atr_risk)
        sl_price = zone.entry_price + (1.0 * atr_risk)

    return SyntheticPath(
        path_class="UNKNOWN",  # Not used for AEE evaluation
        direction=zone.direction,
        entry_price=zone.entry_price,
        entry_spread=1.5,
        atr_pips=zone.atr_pips,
        tp_price=tp_price,
        sl_price=sl_price,
        timestamps=timestamps,
        mid_prices=mid_prices,
        spreads=spreads,
        exit_reason="UNKNOWN",
        exit_time=timestamps[-1] if timestamps else 0,
        final_r=zone.static_final_r
    )


def analyze_bucket_performance(bucket_name: str, zones: List[Any]):
    """Analyze AEE vs static TP/SL performance for a bucket with debug logging."""

    if not zones:
        print(f"\n⚠️ {bucket_name}: No zones to analyze")
        return

    print(f"\n{'='*60}")
    print(f"{bucket_name} BUCKET ANALYSIS (DEBUG MODE)")
    print(f"{'='*60}")

    # Run static baselines at different target sizes
    static_results = {}
    for target_r in [2, 4, 8]:
        static_results[target_r] = run_static_baseline_simulation(zones, target_r)

    # Run AEE with debug logging
    print(f"\n🔍 DEBUG: Running AEE evaluation for {bucket_name} bucket...")
    aee_result = run_aee_simulation(zones)

    # Print results
    print(f"\nSTATIC TP/SL BASELINES:")
    print("Target | Avg R | Win Rate | Profitable | Avg Win R | Avg Loss R | Max R | Min R")
    print("-------|-------|----------|------------|-----------|------------|-------|-------")

    for target_r in [2, 4, 8]:
        result = static_results[target_r]
        print("3.0f")

    print(f"\nAEE PERFORMANCE:")
    print(".3f")
    print(f"Avg Win R: {aee_result.avg_win_r:.3f}")
    print(f"Avg Loss R: {aee_result.avg_loss_r:.3f}")
    print(f"Max R: {aee_result.max_r:.3f}")
    print(f"Min R: {aee_result.min_r:.3f}")

    print("\nExit Reason Distribution:")
    for reason, count in sorted(aee_result.exit_reason_distribution.items(), key=lambda x: x[1], reverse=True):
        pct = count / aee_result.total_trades * 100
        print("12")

    # Comparative analysis
    print("\nCOMPARATIVE ANALYSIS:")
    best_static = max(static_results.values(), key=lambda x: x.avg_r)
    aee_vs_best_static = aee_result.avg_r - best_static.avg_r

    print(".3f")
    print(".3f")
    print(".3f")

    if aee_result.avg_r > best_static.avg_r:
        print("  ✅ AEE outperforms best static baseline")
    else:
        print("  ❌ AEE underperforms best static baseline")

    # Bucket-specific insights
    if bucket_name == "GOOD":
        print("\nGOOD ZONE INSIGHTS:")
        print("  - Should show AEE extending profits beyond static TP")
        print("  - Exit reasons should favor PROFIT_CAPTURE and POST_TP")
        good_extension_rate = sum(1 for r, count in aee_result.exit_reason_distribution.items()
                                 if r in ["PROFIT_CAPTURE", "POST_TP"] for _ in range(count)) / aee_result.total_trades
        print(".1%")

    elif bucket_name == "BAD":
        print("\nBAD ZONE INSIGHTS:")
        print("  - Should show AEE cutting losses better than static SL")
        print("  - Exit reasons should favor PANIC and DECAY")
        bad_loss_prevention = sum(1 for r, count in aee_result.exit_reason_distribution.items()
                                  if r in ["PANIC", "DECAY"] for _ in range(count)) / aee_result.total_trades
        print(".1%")

    elif bucket_name == "NOISE":
        print("\nNOISE ZONE INSIGHTS:")
        print("  - Should show AEE avoiding overreaction")
        print("  - Exit reasons should be balanced, not extreme")


def main():
    """Main analysis function."""

    print("AEE vs STATIC TP/SL ANALYSIS (DEBUG MODE)")
    print("=" * 60)

    # Generate historical data and run unified pipeline
    historical_data = generate_sample_historical_data()
    pipeline = UnifiedReverseEngineeringPipeline()
    results = pipeline.run_unified_pipeline(historical_data)

    zones = results["zones"]

    # Separate by bucket
    good_zones = [z for z in zones if z.bucket == BucketType.GOOD]
    bad_zones = [z for z in zones if z.bucket == BucketType.BAD]
    noise_zones = [z for z in zones if z.bucket == BucketType.NOISE]

    print("\nZone Distribution:")
    print(f"GOOD zones: {len(good_zones)}")
    print(f"BAD zones: {len(bad_zones)}")
    print(f"NOISE zones: {len(noise_zones)}")

    # Analyze each bucket with debug logging
    analyze_bucket_performance("GOOD", good_zones)
    analyze_bucket_performance("BAD", bad_zones)
    analyze_bucket_performance("NOISE", noise_zones)

    print(f"\n{'='*60}")
    print("DEBUG ANALYSIS COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
