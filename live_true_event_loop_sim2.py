import pandas as pd
import numpy as np
import os
from pathlib import Path

class LiveTrueEventLoopSimulatorOptimized:
    def __init__(self, start_balance=100.0, risk_pct=0.02, 
                 threshold_harvester=0.70, threshold_runner=0.90): # Bumped threshold to 0.70
        self.balance = start_balance
        self.risk_pct = risk_pct
        self.threshold_harvester = threshold_harvester
        self.threshold_runner = threshold_runner
        
        self.margin_used = 0.0
        self.active_trades = {}  
        self.trade_history = []
        self.rejected_history = []
        self.next_trade_id = 1
        
        self.margin_map = self._fetch_margins()
        self.price_tapes = {}
        
    def _fetch_margins(self):
        try:
            df = pd.read_csv('oanda_margin_requirements.csv')
            margin_map = {}
            for row in df.itertuples():
                try: margin_map[row.pair] = float(row.margin_rate.replace('%','')) / 100.0
                except: pass
            return margin_map
        except: return {}
        
    def load_tapes(self):
        print("Loading stitched M5 price tapes into memory...")
        root = Path("data_tape_oanda_m5_15_stitched")
        pairs = [d.name.split('=')[1] for d in root.iterdir() if d.is_dir()]
        for pair in pairs:
            pfiles = list((root / f"pair={pair}").glob("*.parquet"))
            if not pfiles: continue
            try:
                df = pd.read_parquet(pfiles[0], columns=['timestamp', 'open', 'high', 'low', 'close'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp').sort_index()
                
                prev_close = df['close'].shift(1)
                tr1 = df['high'] - df['low']
                tr2 = (df['high'] - prev_close).abs()
                tr3 = (df['low'] - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                df['atr'] = tr.rolling(14).mean()
                pip_size = 0.01 if 'JPY' in pair else 0.0001
                df['atr'] = df['atr'].clip(lower=pip_size)
                self.price_tapes[pair] = df
            except Exception as e: pass
        print(f"Loaded {len(self.price_tapes)} pairs.")

    def run_simulation(self):
        candidates = pd.read_parquet("global_true_candidate_stream.parquet")
        
        # Filter strictly
        candidates['timestamp'] = pd.to_datetime(candidates['timestamp'])
        candidates['hour'] = candidates['timestamp'].dt.hour
        
        # Apply regime isolation & score bump
        candidates = candidates[candidates['composite_score'] >= self.threshold_harvester]
        candidates = candidates[candidates['hour'].isin([0, 11, 12, 15, 23])]
        
        candidates = candidates.sort_values(by=['timestamp', 'composite_score'], ascending=[True, False]).reset_index(drop=True)
        
        print(f"Simulating strictly optimized {len(candidates)} raw entry candidates...")
        
        all_timestamps = set(candidates['timestamp'])
        cand_dict = {}
        for row in candidates.itertuples(index=False):
            ts = row.timestamp
            if ts not in cand_dict: cand_dict[ts] = []
            cand_dict[ts].append(row)
            
        sorted_times = sorted(list(all_timestamps))
        
        static_spread_r = 0.05
        # The true mathematically positive bounding structure found:
        HARVESTER_TARGET = 0.55
        HARVESTER_CUT = -0.35
        RUNNER_TARGET = 1.50
        RUNNER_CUT = -0.50

        last_time = None
        max_active_trades = 6 # Limit absolute max concurrent trades slightly to prevent blowout on margin
        
        # Track maximum balance for drawdown math
        max_recorded_balance = self.balance
        
        for curr_time in sorted_times:
            closed_this_tick = []
            
            if last_time is not None and self.active_trades:
                for t_id, t in list(self.active_trades.items()):
                    pair = t['pair']
                    if pair not in self.price_tapes: continue
                    df_tape = self.price_tapes[pair]
                    
                    mask = (df_tape.index > last_time) & (df_tape.index <= curr_time)
                    bars = df_tape.loc[mask]
                    if bars.empty: continue
                    
                    for bar_ts, bar in bars.iterrows():
                        H, L = bar['high'], bar['low']
                        mfe_r, mae_r = 0, 0
                        if t['direction'] == 'LONG':
                            mfe_r = (H - t['entry_price']) / t['entry_atr']
                            mae_r = (t['entry_price'] - L) / t['entry_atr']
                        else:
                            mfe_r = (t['entry_price'] - L) / t['entry_atr']
                            mae_r = (H - t['entry_price']) / t['entry_atr']
                            
                        if mfe_r > t['max_mfe']: t['max_mfe'] = mfe_r
                        if mae_r > t['max_mae']: t['max_mae'] = mae_r
                        
                        resolved = False
                        action = None
                        
                        if t['mode'] == 'HARVESTER':
                            if mae_r >= abs(HARVESTER_CUT):
                                resolved = True; action = 'CUT'; gross_r = HARVESTER_CUT
                            elif mfe_r >= HARVESTER_TARGET:
                                resolved = True; action = 'TARGET'; gross_r = HARVESTER_TARGET
                        else:
                            if mae_r >= abs(RUNNER_CUT):
                                resolved = True; action = 'CUT'; gross_r = RUNNER_CUT
                            elif mfe_r >= RUNNER_TARGET:
                                resolved = True; action = 'TARGET'; gross_r = RUNNER_TARGET
                                
                        if resolved:
                            t['exit_time'] = bar_ts
                            t['gross_r'] = gross_r
                            t['net_r'] = gross_r - static_spread_r
                            t['exit_type'] = action
                            t['friction'] = static_spread_r
                            
                            pnl = t['risk_usd'] * t['net_r']
                            self.balance += pnl
                            if self.balance > max_recorded_balance:
                                max_recorded_balance = self.balance
                            self.margin_used -= t['margin']
                            t['final_balance'] = self.balance
                            t['drawdown_pct'] = ((max_recorded_balance - self.balance) / max_recorded_balance) * 100
                            
                            self.trade_history.append(t)
                            closed_this_tick.append(t_id)
                            break
                            
                for t_id in closed_this_tick:
                    del self.active_trades[t_id]
            
            competing = cand_dict[curr_time]
            for cand in competing:
                if len(self.active_trades) >= max_active_trades:
                    self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'MAX_TRADES_HIT'})
                    continue
                    
                pair = cand.node.split('__')[0]
                
                # Deduplicate exact pair
                if any(t['pair'] == pair for t in self.active_trades.values()):
                    self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'PAIR_ALREADY_ACTIVE'})
                    continue
                
                margin_req = self.margin_map.get(pair, 0.05)
                risk_amount = self.balance * self.risk_pct
                required_margin = risk_amount * (1.0 / margin_req)
                available_buying_power = self.balance - self.margin_used
                
                if available_buying_power >= required_margin:
                    mode = 'RUNNER' if cand.composite_score >= self.threshold_runner else 'HARVESTER'
                    if pair in self.price_tapes:
                        tape_slice = self.price_tapes[pair].loc[:curr_time]
                        if not tape_slice.empty:
                            last_bar = tape_slice.iloc[-1]
                            entry_atr = last_bar['atr']
                            pip_sz = 0.01 if 'JPY' in pair else 0.0001
                            if pd.isna(entry_atr) or entry_atr < pip_sz: entry_atr = pip_sz
                            
                            trad_obj = {
                                'trade_id': self.next_trade_id,
                                'entry_time': curr_time,
                                'node': cand.node,
                                'pair': pair,
                                'direction': cand.direction_assumed,
                                'score': cand.composite_score,
                                'mode': mode,
                                'entry_price': cand.price,
                                'entry_atr': entry_atr,
                                'margin': required_margin,
                                'risk_usd': risk_amount,
                                'max_mfe': 0.0,
                                'max_mae': 0.0
                            }
                            self.active_trades[self.next_trade_id] = trad_obj
                            self.margin_used += required_margin
                            self.next_trade_id += 1
                        else: self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'NO_PRICE_TAPE'})
                    else: self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'PAIR_NOT_LOADED'})
                else: self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'MARGIN_EXHAUSTED'})
                    
            last_time = curr_time
            
        self._finalize()
        
    def _finalize(self):
        for t_id, t in list(self.active_trades.items()):
            t['exit_time'] = "END_OF_DATA"
            t['gross_r'] = 0.0
            t['net_r'] = -0.05
            t['exit_type'] = "TIMEOUT"
            t['friction'] = 0.05
            pnl = t['risk_usd'] * t['net_r']
            self.balance += pnl
            self.margin_used -= t['margin']
            t['final_balance'] = self.balance
            t['drawdown_pct'] = 0
            self.trade_history.append(t)
            
        print("Saving true physics OPZ output logs...")
        tdf = pd.DataFrame(self.trade_history)
        if not tdf.empty: tdf.to_csv('true_physics_trades_opz.csv', index=False)
        rdf = pd.DataFrame(self.rejected_history)
        if not rdf.empty: rdf.to_csv('true_physics_rejected_opz.csv', index=False)
        print(f"Final Balance: ${self.balance:.2f} (Start: $100.00)")
        print(f"Total Taken: {len(tdf)}, Total Rejected: {len(rdf)}")

if __name__ == "__main__":
    sim = LiveTrueEventLoopSimulatorOptimized(threshold_harvester=0.70, risk_pct=0.02)
    sim.load_tapes()
    sim.run_simulation()
