#!/usr/bin/env python3
"""
Phase 4 - AEE Fitter: Optimize Exit Logic Using GOOD Opportunities

PURPOSE: Derive AEE settings from path behavior inside GOOD opportunities.
Compare AEE-managed exits vs static exits on the same triggered trades.

APPROACH:
1. Load GOOD opportunities (98) with their price paths
2. For each opportunity, simulate static exits (fixed TP/SL/timeout)
3. For each opportunity, simulate AEE state machine exits
4. Calculate DeltaR = R_aee - R_static for each trade
5. Optimize AEE parameters to maximize AEEScore

REQUIRED OUTPUTS:
- aee_fit_long.json: Best config, static/AEE metrics, top 10 configs
- aee_fit_short.json: Same for SHORT
- aee_failure_catalog.json: Examples of clipped GOOD, uncaught BAD, timeouts
"""

from __future__ import annotations
import json
import csv
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
import statistics
import math


@dataclass
class AEEState:
    """Live AEE state during trade execution."""
    profit: float = 0.0
    mfe: float = 0.0
    giveback: float = 0.0
    velocity: float = 0.0
    vel_decay: float = 0.0
    bars_since_high: int = 0
    bars_since_low: int = 0
    entry_price: float = 0.0
    direction: str = "LONG"


@dataclass
class AEEResult:
    """Result of AEE execution on a trade."""
    exit_price: float
    exit_time: int
    exit_reason: str
    final_r: float
    max_profit: float
    max_giveback: float
    total_bars: int


@dataclass
class AEEConfig:
    """AEE parameter configuration."""
    profit_capture_min_atr: float = 0.5
    allowed_giveback_atr_mult: float = 0.5
    panic_threshold: float = -1.0
    decay_threshold: float = -0.5
    extension_hold_bars: int = 5
    timeout_bars: int = 60


class AEEFitter:
    """
    Phase 4: Fit AEE parameters using GOOD opportunities.
    """

    def __init__(self, target_pips: float = 2.5, pip_multiplier: float = 10000):
        self.target_pips = target_pips
        self.pip_multiplier = pip_multiplier
        self.target_r = target_pips  # 2.5 pips target

    def load_good_opportunities(self, csv_path: str) -> List[Dict[str, Any]]:
        """Load GOOD opportunities from CSV."""
        opportunities = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert numeric fields
                for key in ['price_start', 'time_to_target', 'max_mfe_pips', 'max_mae_pips',
                           'speed', 'efficiency', 'drawdown_ratio', 'extension', 'composite_score', 'final_price']:
                    if row[key]:
                        row[key] = float(row[key])
                row['price_path'] = [float(p) for p in row['price_path'].strip('[]').split(',')]
                opportunities.append(row)
        return opportunities

    def compute_path_energy_metrics(self, opportunity: Dict) -> Dict[str, float]:
        """Compute all path energy metrics as defined in the specification."""
        price_path = opportunity['price_path']
        direction = opportunity['direction']
        start_price = opportunity['price_start']

        # Favorable movement F(k)
        if direction == 'LONG':
            F = lambda k: (price_path[k] - start_price) * self.pip_multiplier
        else:  # SHORT
            F = lambda k: (start_price - price_path[k]) * self.pip_multiplier

        # 3.1 Time to target tau
        tau = None
        for k in range(1, len(price_path)):
            if F(k) >= self.target_r:
                tau = k
                break
        if tau is None:
            return {'valid': False}

        # 3.2 Maximum favorable excursion MFE
        mfe = max(F(k) for k in range(len(price_path)))

        # 3.3 Maximum adverse excursion MAE (up to tau)
        mae = 0.0
        for k in range(tau + 1):
            if direction == 'LONG':
                adverse = start_price - price_path[k]
            else:
                adverse = price_path[k] - start_price
            mae = max(mae, adverse * self.pip_multiplier)

        # 3.4 Speed
        speed = self.target_r / tau if tau > 0 else 0.0

        # 3.5 Early impulse ratio (Te = 5 bars)
        te = min(5, len(price_path) - 1)
        early_move = F(te)
        early_impulse = early_move / mfe if mfe > 0 else 0.0

        # 3.6 Path efficiency
        path_length = sum(abs(price_path[k] - price_path[k-1]) for k in range(1, len(price_path)))
        path_length_pips = path_length * self.pip_multiplier
        efficiency = mfe / path_length_pips if path_length_pips > 0 else 0.0

        # 3.7 Risk ratio
        risk_ratio = mfe / (mae + 0.01)  # ε = 0.01

        # 3.8 Extension
        extension = mfe / self.target_r

        return {
            'valid': True,
            'tau': tau,
            'mfe': mfe,
            'mae': mae,
            'speed': speed,
            'early_impulse': early_impulse,
            'efficiency': efficiency,
            'risk_ratio': risk_ratio,
            'extension': extension,
            'direction': direction,
            'price_path': price_path,
            'start_price': start_price
        }

    def simulate_static_exit(self, metrics: Dict) -> Dict[str, float]:
        """Simulate static exits: fixed TP, SL, timeout."""
        price_path = metrics['price_path']
        direction = metrics['direction']
        start_price = metrics['start_price']

        tp_pips = 5.0  # 5 pips TP
        sl_pips = -2.5  # 2.5 pips SL
        timeout_bars = 60

        if direction == 'LONG':
            tp_price = start_price + (tp_pips / self.pip_multiplier)
            sl_price = start_price + (sl_pips / self.pip_multiplier)
        else:
            tp_price = start_price - (tp_pips / self.pip_multiplier)
            sl_price = start_price - (sl_pips / self.pip_multiplier)

        exit_price = None
        exit_reason = None

        # Check each bar
        for k in range(1, min(len(price_path), timeout_bars + 1)):
            current_price = price_path[k]

            if direction == 'LONG':
                if current_price >= tp_price:
                    exit_price = tp_price
                    exit_reason = 'tp_hit'
                    break
                elif current_price <= sl_price:
                    exit_price = sl_price
                    exit_reason = 'sl_hit'
                    break
            else:  # SHORT
                if current_price <= tp_price:
                    exit_price = tp_price
                    exit_reason = 'tp_hit'
                    break
                elif current_price >= sl_price:
                    exit_price = sl_price
                    exit_reason = 'sl_hit'
                    break

        # Timeout
        if exit_price is None:
            exit_price = price_path[-1] if len(price_path) <= timeout_bars else price_path[timeout_bars]
            exit_reason = 'timeout'

        # Calculate R
        if direction == 'LONG':
            r = (exit_price - start_price) * self.pip_multiplier
        else:
            r = (start_price - exit_price) * self.pip_multiplier

        return {
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'r_static': r,
            'bars_held': min(len(price_path) - 1, timeout_bars)
        }

    def simulate_aee_exit(self, metrics: Dict, config: AEEConfig) -> Dict[str, float]:
        """Simulate AEE state machine exit."""
        price_path = metrics['price_path']
        direction = metrics['direction']
        start_price = metrics['start_price']

        state = AEEState(entry_price=start_price, direction=direction)
        max_bars = len(price_path) - 1
        timeout_bars = 60

        # Convert config to pips
        profit_capture_min = config.profit_capture_min_atr * self.target_r  # Scale to target
        allowed_giveback = config.allowed_giveback_atr_mult * self.target_r
        panic_threshold = config.panic_threshold
        decay_threshold = config.decay_threshold

        exit_price = None
        exit_reason = None
        max_profit_seen = 0.0
        max_giveback_seen = 0.0

        # Run AEE state machine
        for k in range(1, min(max_bars + 1, timeout_bars + 1)):
            current_price = price_path[k]

            # Update profit
            if direction == 'LONG':
                state.profit = (current_price - start_price) * self.pip_multiplier
            else:
                state.profit = (start_price - current_price) * self.pip_multiplier

            # Update MFE and giveback
            state.mfe = max(state.mfe, state.profit)
            state.giveback = state.mfe - state.profit
            max_profit_seen = max(max_profit_seen, state.profit)
            max_giveback_seen = max(max_giveback_seen, state.giveback)

            # Update velocity and decay
            prev_profit = state.profit - (price_path[k] - price_path[k-1]) * self.pip_multiplier * (1 if direction == 'LONG' else -1)
            state.velocity = state.profit - prev_profit
            state.vel_decay = state.velocity - getattr(state, 'prev_velocity', state.velocity)
            state.prev_velocity = state.velocity

            # Update bars since high/low
            if direction == 'LONG':
                if current_price >= state.entry_price + (max_profit_seen / self.pip_multiplier):
                    state.bars_since_high = 0
                else:
                    state.bars_since_high += 1
            else:
                if current_price <= state.entry_price - (max_profit_seen / self.pip_multiplier):
                    state.bars_since_low = 0
                else:
                    state.bars_since_low += 1

            # AEE Exit Conditions
            should_exit = False

            # 1. Profit capture: Take profit when reached minimum and velocity slowing
            if (state.profit >= profit_capture_min and
                state.vel_decay < decay_threshold and
                state.bars_since_high >= config.extension_hold_bars):
                exit_price = current_price
                exit_reason = 'profit_capture'
                should_exit = True

            # 2. Panic exit: Giveback too large and velocity negative
            elif state.giveback >= allowed_giveback and state.velocity < panic_threshold:
                exit_price = current_price
                exit_reason = 'panic_exit'
                should_exit = True

            # 3. Giveback decay: Too much giveback over time
            elif state.giveback >= allowed_giveback and state.vel_decay < decay_threshold:
                exit_price = current_price
                exit_reason = 'giveback_decay'
                should_exit = True

            if should_exit:
                break

        # Timeout exit
        if exit_price is None:
            exit_price = price_path[-1] if max_bars < timeout_bars else price_path[timeout_bars]
            exit_reason = 'timeout'

        # Calculate R
        if direction == 'LONG':
            r = (exit_price - start_price) * self.pip_multiplier
        else:
            r = (start_price - exit_price) * self.pip_multiplier

        return {
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'r_aee': r,
            'bars_held': k if 'k' in locals() else timeout_bars,
            'max_profit': max_profit_seen,
            'max_giveback': max_giveback_seen
        }

    def evaluate_aee_config(self, config: AEEConfig, opportunities: List[Dict]) -> Dict[str, float]:
        """Evaluate AEE config on GOOD opportunities."""
        static_results = []
        aee_results = []

        for opp in opportunities:
            metrics = self.compute_path_energy_metrics(opp)
            if not metrics['valid']:
                continue

            static = self.simulate_static_exit(metrics)
            aee = self.simulate_aee_exit(metrics, config)

            static_results.append(static)
            aee_results.append(aee)

        if not static_results or not aee_results:
            return {'valid': False}

        # Calculate metrics
        static_rs = [r['r_static'] for r in static_results]
        aee_rs = [r['r_aee'] for r in aee_results]

        mean_delta_r = statistics.mean(a - s for a, s in zip(aee_rs, static_rs))

        # Saved loss: improvement on losing trades
        losing_trades = [(s, a) for s, a in zip(static_rs, aee_rs) if s < 0]
        saved_loss = 0.0
        if losing_trades:
            saved_loss = statistics.mean(max(0, a - s) for s, a in losing_trades)

        # Clip rate: AEE closed winners too early
        winner_trades = [(s, a) for s, a in zip(static_rs, aee_rs) if s > 0]
        clip_count = sum(1 for s, a in winner_trades if a < s * 0.8)  # AEE got < 80% of static profit
        clip_rate = clip_count / len(winner_trades) if winner_trades else 0.0

        # Held to loss rate: BAD trades held to full loss
        held_to_loss = sum(1 for s, a in zip(static_rs, aee_rs) if s >= 0 and a < -1.0)
        held_to_loss_rate = held_to_loss / len([r for r in static_rs if r >= 0]) if static_rs else 0.0

        # AEE Score: μ1=0.5, μ2=0.3, μ3=0.2
        μ1, μ2, μ3 = 0.5, 0.3, 0.2
        aee_score = mean_delta_r + μ1 * saved_loss - μ2 * clip_rate - μ3 * held_to_loss_rate

        return {
            'valid': True,
            'trade_count': len(static_results),
            'static_avg_r': statistics.mean(static_rs),
            'aee_avg_r': statistics.mean(aee_rs),
            'mean_delta_r': mean_delta_r,
            'saved_loss': saved_loss,
            'clip_rate': clip_rate,
            'held_to_loss_rate': held_to_loss_rate,
            'aee_score': aee_score,
            'config': {
                'profit_capture_min_atr': config.profit_capture_min_atr,
                'allowed_giveback_atr_mult': config.allowed_giveback_atr_mult,
                'panic_threshold': config.panic_threshold,
                'decay_threshold': config.decay_threshold,
                'extension_hold_bars': config.extension_hold_bars,
                'timeout_bars': config.timeout_bars
            }
        }

    def optimize_aee_configs(self, opportunities: List[Dict], direction: str) -> Dict[str, Any]:
        """Optimize AEE configs for specific direction."""
        dir_opps = [opp for opp in opportunities if opp['direction'] == direction]
        if not dir_opps:
            return {'error': f'No {direction} opportunities'}

        print(f"Optimizing AEE for {direction}: {len(dir_opps)} opportunities")

        # Generate config combinations
        configs = []
        param_ranges = {
            'profit_capture_min_atr': [0.3, 0.5, 0.7],
            'allowed_giveback_atr_mult': [0.3, 0.5, 0.7],
            'panic_threshold': [-2.0, -1.0, -0.5],
            'decay_threshold': [-1.0, -0.5, -0.2],
            'extension_hold_bars': [3, 5, 8],
            'timeout_bars': [60]  # Fixed
        }

        # Sample configs (not full grid search for efficiency)
        for pc_min in param_ranges['profit_capture_min_atr']:
            for giveback in param_ranges['allowed_giveback_atr_mult']:
                for panic in param_ranges['panic_threshold']:
                    for decay in param_ranges['decay_threshold']:
                        for hold in param_ranges['extension_hold_bars']:
                            config = AEEConfig(
                                profit_capture_min_atr=pc_min,
                                allowed_giveback_atr_mult=giveback,
                                panic_threshold=panic,
                                decay_threshold=decay,
                                extension_hold_bars=hold,
                                timeout_bars=60
                            )
                            result = self.evaluate_aee_config(config, dir_opps)
                            if result['valid']:
                                configs.append(result)

        if not configs:
            return {'error': 'No valid configs found'}

        # Sort by AEE score
        configs.sort(key=lambda x: x['aee_score'], reverse=True)

        best_config = configs[0]
        top_10_configs = configs[:10]

        return {
            'best_config': best_config,
            'top_10_configs': top_10_configs,
            'total_configs_tested': len(configs),
            'direction': direction,
            'opportunities_used': len(dir_opps)
        }

    def generate_failure_catalog(self, opportunities: List[Dict], best_configs: Dict) -> Dict[str, Any]:
        """Generate catalog of AEE failures and edge cases."""
        catalog = {
            'clipped_good_examples': [],
            'uncaught_bad_examples': [],
            'timeout_only_examples': [],
            'perfect_exits': []
        }

        for opp in opportunities:
            metrics = self.compute_path_energy_metrics(opp)
            if not metrics['valid']:
                continue

            direction = opp['direction']
            config = best_configs.get(direction)
            if not config:
                continue

            aee_config = AEEConfig(**config['best_config']['config'])
            static = self.simulate_static_exit(metrics)
            aee = self.simulate_aee_exit(metrics, aee_config)

            delta_r = aee['r_aee'] - static['r_static']

            # Clipped GOOD: AEE got much less than static on a winner
            if static['r_static'] > 1.0 and aee['r_aee'] < static['r_static'] * 0.7:
                catalog['clipped_good_examples'].append({
                    'opportunity_id': f"{opp['timestamp_start']}_{direction}",
                    'static_r': static['r_static'],
                    'aee_r': aee['r_aee'],
                    'delta_r': delta_r,
                    'exit_reason': aee['exit_reason']
                })

            # Uncaught BAD: AEE held losing trade too long
            elif static['r_static'] >= 0 and aee['r_aee'] < -1.0:
                catalog['uncaught_bad_examples'].append({
                    'opportunity_id': f"{opp['timestamp_start']}_{direction}",
                    'static_r': static['r_static'],
                    'aee_r': aee['r_aee'],
                    'delta_r': delta_r,
                    'exit_reason': aee['exit_reason']
                })

            # Timeout only: AEE never found an exit condition
            elif aee['exit_reason'] == 'timeout':
                catalog['timeout_only_examples'].append({
                    'opportunity_id': f"{opp['timestamp_start']}_{direction}",
                    'static_r': static['r_static'],
                    'aee_r': aee['r_aee'],
                    'delta_r': delta_r,
                    'bars_held': aee['bars_held']
                })

            # Perfect exits: AEE significantly improved over static
            elif delta_r > 2.0:
                catalog['perfect_exits'].append({
                    'opportunity_id': f"{opp['timestamp_start']}_{direction}",
                    'static_r': static['r_static'],
                    'aee_r': aee['r_aee'],
                    'delta_r': delta_r,
                    'exit_reason': aee['exit_reason']
                })

        return catalog


def main():
    """Run Phase 4 - AEE Fitter."""
    import argparse

    parser = argparse.ArgumentParser(description="Phase 4: AEE Fitter")
    parser.add_argument("--good-opportunities-csv", required=True, help="Path to good_opportunities.csv")
    parser.add_argument("--output-dir", default="phase4_aee_fit_outputs", help="Output directory")

    args = parser.parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load GOOD opportunities
    print("Phase 4: Loading GOOD opportunities...")
    fitter = AEEFitter(target_pips=2.5, pip_multiplier=10000)
    opportunities = fitter.load_good_opportunities(args.good_opportunities_csv)

    print(f"Phase 4: Loaded {len(opportunities)} GOOD opportunities")

    # Separate by direction
    long_opps = [opp for opp in opportunities if opp['direction'] == 'LONG']
    short_opps = [opp for opp in opportunities if opp['direction'] == 'SHORT']

    print(f"LONG opportunities: {len(long_opps)}")
    print(f"SHORT opportunities: {len(short_opps)}")

    # Optimize AEE for LONG
    print("Phase 4: Optimizing AEE for LONG...")
    long_results = fitter.optimize_aee_configs(opportunities, 'LONG')

    # Optimize AEE for SHORT
    print("Phase 4: Optimizing AEE for SHORT...")
    short_results = fitter.optimize_aee_configs(opportunities, 'SHORT')

    # Save results
    if 'best_config' in long_results:
        long_path = output_dir / "aee_fit_long.json"
        with open(long_path, 'w') as f:
            json.dump(long_results, f, indent=2, default=str)

    if 'best_config' in short_results:
        short_path = output_dir / "aee_fit_short.json"
        with open(short_path, 'w') as f:
            json.dump(short_results, f, indent=2, default=str)

    # Generate failure catalog
    best_configs = {}
    if 'best_config' in long_results:
        best_configs['LONG'] = long_results
    if 'best_config' in short_results:
        best_configs['SHORT'] = short_results

    failure_catalog = fitter.generate_failure_catalog(opportunities, best_configs)
    catalog_path = output_dir / "aee_failure_catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(failure_catalog, f, indent=2)

    # Print summary
    print("""
PHASE 4 RESULTS:""")
    print(f"Total GOOD opportunities: {len(opportunities)}")
    print(f"LONG opportunities: {len(long_opps)}")
    print(f"SHORT opportunities: {len(short_opps)}")

    if 'best_config' in long_results:
        bc = long_results['best_config']
        print(f"\nLONG AEE - Best Config Score: {bc['aee_score']:.3f}")
        print(f"LONG AEE - Static Avg R: {bc['static_avg_r']:.2f} pips")
        print(f"LONG AEE - AEE Avg R: {bc['aee_avg_r']:.2f} pips")
        print(f"LONG AEE - Mean Delta R: {bc['mean_delta_r']:.2f} pips")
        print(f"LONG AEE - Saved Loss: {bc['saved_loss']:.2f} pips")
        print(f"LONG AEE - Clip Rate: {bc['clip_rate']:.2f}")
        print(f"LONG AEE - Held to Loss Rate: {bc['held_to_loss_rate']:.2f}")

    if 'best_config' in short_results:
        bc = short_results['best_config']
        print(f"\nSHORT AEE - Best Config Score: {bc['aee_score']:.3f}")
        print(f"SHORT AEE - Static Avg R: {bc['static_avg_r']:.2f} pips")
        print(f"SHORT AEE - AEE Avg R: {bc['aee_avg_r']:.2f} pips")
        print(f"SHORT AEE - Mean Delta R: {bc['mean_delta_r']:.2f} pips")
        print(f"SHORT AEE - Saved Loss: {bc['saved_loss']:.2f} pips")
        print(f"SHORT AEE - Clip Rate: {bc['clip_rate']:.2f}")
        print(f"SHORT AEE - Held to Loss Rate: {bc['held_to_loss_rate']:.2f}")

    # Check for AEE_TEST_INVALID
    timeout_examples = failure_catalog.get('timeout_only_examples', [])
    all_timeout = all(
        example.get('exit_reason') == 'timeout'
        for example in timeout_examples
    ) if timeout_examples else False

    if all_timeout and len(timeout_examples) > len(opportunities) * 0.8:
        print("\nAEE_TEST_INVALID: 100% timeout exits - AEE not functioning")
    else:
        print("""
✅ Phase 4 COMPLETED: AEE settings derived from GOOD opportunity paths""")
        print(f"   - aee_fit_long.json: LONG AEE optimization results")
        print(f"   - aee_fit_short.json: SHORT AEE optimization results")
        print(f"   - aee_failure_catalog.json: AEE failure analysis")

    return 0


if __name__ == "__main__":
    exit(main())
