import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor

class LiveTrueEventLoopSimulator:
    def __init__(self, start_balance=100.0, risk_pct=0.02, 
                 threshold_harvester=0.60, threshold_runner=0.85):
        self.balance = start_balance
        self.risk_pct = risk_pct
        self.threshold_harvester = threshold_harvester
        self.threshold_runner = threshold_runner
        
        self.margin_used = 0.0
        self.active_trades = {}  # trade_id -> trade_state
        self.trade_history = []
        self.rejected_history = []
        self.next_trade_id = 1
        
        # Load necessary static data
        self.margin_map = self._fetch_margins()
        
        # For intra-trade evolution, we need quick access to price paths per pair. 
        # We will map pair -> dataframe of M5 path.
        self.price_tapes = {}
        
    def _fetch_margins(self):
        try:
            df = pd.read_csv('oanda_margin_requirements.csv')
            margin_map = {}
            for row in df.itertuples():
                try:
                    margin_map[row.pair] = float(row.margin_rate.replace('%','')) / 100.0
                except: pass
            return margin_map
        except: return {}
        
    def load_tapes(self):
        print("Loading stitched M5 price tapes into memory...")
        root = Path("data_tape_oanda_m5_15_stitched")
        pairs = [d.name.split('=')[1] for d in root.iterdir() if d.is_dir()]
        
        for pair in pairs:
            # We strictly need open/high/low/close by timestamp.
            pfiles = list((root / f"pair={pair}").glob("*.parquet"))
            if not pfiles: continue
            
            try:
                # Store strictly needed columns
                df = pd.read_parquet(pfiles[0], columns=['timestamp', 'open', 'high', 'low', 'close'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                # Set index to timestamp for very fast lookup via slicing
                df = df.set_index('timestamp').sort_index()
                
                # compute 14M5 ATR for dynamic 'R' mapping (equivalent to 70 mins)
                # Instead of computing for every row, we do it in bulk.
                prev_close = df['close'].shift(1)
                tr1 = df['high'] - df['low']
                tr2 = (df['high'] - prev_close).abs()
                tr3 = (df['low'] - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                df['atr'] = tr.rolling(14).mean()
                
                pip_size = 0.01 if 'JPY' in pair else 0.0001
                df['atr'] = df['atr'].clip(lower=pip_size)
                
                self.price_tapes[pair] = df
            except Exception as e:
                print(f"Skipped tape load for {pair}: {e}")
        print(f"Loaded {len(self.price_tapes)} pairs.")

    def run_simulation(self):
        print("Loading global true candidate stream...")
        if not os.path.exists("global_true_candidate_stream.parquet"):
            print("No true candidate stream found. Abort.")
            return
            
        candidates = pd.read_parquet("global_true_candidate_stream.parquet")
        # Step 3: Entry Decision is independent. Score controls filtering blind to outcome.
        candidates = candidates[candidates['composite_score'] >= self.threshold_harvester].copy()
        
        # Sort candidates chronologically 
        candidates['timestamp'] = pd.to_datetime(candidates['timestamp'])
        # To simulate Step 4 (Real Priority), we sort exactly by:
        # timestamp ASC, score DESC
        candidates = candidates.sort_values(by=['timestamp', 'composite_score'], ascending=[True, False]).reset_index(drop=True)
        
        # We group by exact minute timestamp to run event ticks
        
        # For extreme performance over 350k events, we iterate over unique exact timestamps.
        # But wait! We also need to process active trades.
        # Active trades evolve along the M5 tape. The timeline jumps to whichever is next:
        # either a new candidate, or the next M5 bar for open trades.
        
        all_timestamps = set(candidates['timestamp'])
        
        print(f"Simulating {len(candidates)} raw entry candidates over {len(all_timestamps)} unique timestamps...")
        
        cand_dict = {}
        for row in candidates.itertuples(index=False):
            ts = row.timestamp
            if ts not in cand_dict: cand_dict[ts] = []
            cand_dict[ts].append(row)
            
        sorted_times = sorted(list(all_timestamps))
        
        # Standard friction
        static_spread_r = 0.05
        # Standard Exits
        HARVESTER_TARGET = 0.25
        HARVESTER_CUT = -0.35
        RUNNER_TARGET = 1.50
        RUNNER_CUT = -0.50

        # We will jump from candidate timestamp to candidate timestamp. 
        # IF there are active trades, we MUST pull forward their specific pair M5 paths to the current timestamp
        # and evaluate them BEFORE executing new entries.
        last_time = None
        
        for curr_time in sorted_times:
            # 1. Update existing trades (Intra-Trade Evolution - Step 5)
            # We look at the M5 paths mapped between last_time and curr_time for each open trade's pair.
            
            closed_this_tick = []
            
            if last_time is not None and self.active_trades:
                for t_id, t in list(self.active_trades.items()):
                    pair = t['pair']
                    if pair not in self.price_tapes: continue
                    
                    df_tape = self.price_tapes[pair]
                    
                    # Get the price bars that occurred strictly after the last evaluation, up to curr_time
                    # (Exclusive of last_time, inclusive of curr_time)
                    mask = (df_tape.index > last_time) & (df_tape.index <= curr_time)
                    bars = df_tape.loc[mask]
                    
                    if bars.empty: continue
                    
                    # Evaluate path chronologically
                    for bar_ts, bar in bars.iterrows():
                        H = bar['high']
                        L = bar['low']
                        
                        mfe_r = 0
                        mae_r = 0
                        
                        if t['direction'] == 'LONG':
                            mfe_r = (H - t['entry_price']) / t['entry_atr']
                            mae_r = (t['entry_price'] - L) / t['entry_atr']
                        else:
                            mfe_r = (t['entry_price'] - L) / t['entry_atr']
                            mae_r = (H - t['entry_price']) / t['entry_atr']
                            
                        # Keep track of absolute extremes
                        if mfe_r > t['max_mfe']: t['max_mfe'] = mfe_r
                        if mae_r > t['max_mae']: t['max_mae'] = mae_r
                        
                        # Evaluate AEE
                        resolved = False
                        action = None
                        
                        if t['mode'] == 'HARVESTER':
                            if mae_r >= abs(HARVESTER_CUT):
                                resolved = True
                                action = 'CUT'
                                gross_r = HARVESTER_CUT
                            elif mfe_r >= HARVESTER_TARGET:
                                resolved = True
                                action = 'TARGET'
                                gross_r = HARVESTER_TARGET
                        else: # RUNNER
                            if mae_r >= abs(RUNNER_CUT):
                                resolved = True
                                action = 'CUT'
                                gross_r = RUNNER_CUT
                            elif mfe_r >= RUNNER_TARGET:
                                resolved = True
                                action = 'TARGET'
                                gross_r = RUNNER_TARGET
                                
                        if resolved:
                            t['exit_time'] = bar_ts
                            t['gross_r'] = gross_r
                            t['net_r'] = gross_r - static_spread_r
                            t['exit_type'] = action
                            t['friction'] = static_spread_r
                            
                            pnl = t['risk_usd'] * t['net_r']
                            self.balance += pnl
                            self.margin_used -= t['margin']
                            t['final_balance'] = self.balance
                            
                            self.trade_history.append(t)
                            closed_this_tick.append(t_id)
                            break # stop processing bars for this trade
                            
                for t_id in closed_this_tick:
                    del self.active_trades[t_id]
            
            # 2. Process new candidates (Step 4: Real Priority Competition)
            competing = cand_dict[curr_time]
            
            for cand in competing:
                # Rank logic is innate to the sorting (already sorted by score DESC in cand_dict grouping)
                
                # Check mode isolation (can only have 1 trade per node per direction active ideally, or just pure margin physics)
                pair = cand.node.split('__')[0]
                
                # Check Account Physics (Step 8)
                margin_req = self.margin_map.get(pair, 0.05)
                risk_amount = self.balance * self.risk_pct
                required_margin = risk_amount * (1.0 / margin_req)
                
                available_buying_power = self.balance - self.margin_used
                
                if available_buying_power >= required_margin:
                    # Accept trade
                    mode = 'RUNNER' if cand.composite_score >= self.threshold_runner else 'HARVESTER'
                    
                    # Need ATR at entry moment
                    if pair in self.price_tapes:
                        # get state at exact moment or nearest previous
                        tape_slice = self.price_tapes[pair].loc[:curr_time]
                        if not tape_slice.empty:
                            last_bar = tape_slice.iloc[-1]
                            entry_atr = last_bar['atr']
                            # Enforce clipping just in case
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
                        else:
                             self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'NO_PRICE_TAPE'})
                    else:
                        self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'PAIR_NOT_LOADED'})
                else:
                    # Reject trade due to competition physics
                    self.rejected_history.append({'timestamp': curr_time, 'node': cand.node, 'reason': 'MARGIN_EXHAUSTED', 'req': required_margin, 'avail': available_buying_power})
                    
            last_time = curr_time
            
        self._finalize()
        
    def _finalize(self):
        # Force close open trades at end of time loop flatly
        for t_id, t in list(self.active_trades.items()):
            # We assume it stalled out at 0 R if it never hit target/cut
            # Or realistically, evaluate current MFE/MAE
            gross_r = t['max_mfe'] - t['max_mae'] # crude end state
            
            t['exit_time'] = "END_OF_DATA"
            t['gross_r'] = gross_r
            t['net_r'] = gross_r - 0.05
            t['exit_type'] = "TIMEOUT"
            t['friction'] = 0.05
            
            pnl = t['risk_usd'] * t['net_r']
            self.balance += pnl
            self.margin_used -= t['margin']
            t['final_balance'] = self.balance
            
            self.trade_history.append(t)
            
        print("Saving true physics output logs...")
        tdf = pd.DataFrame(self.trade_history)
        if not tdf.empty:
            tdf.to_csv('true_physics_trades.csv', index=False)
            
        rdf = pd.DataFrame(self.rejected_history)
        if not rdf.empty:
            rdf.to_csv('true_physics_rejected.csv', index=False)
            
        print(f"Final Balance: ${self.balance:.2f} (Start: $100.00)")
        print(f"Total Taken: {len(tdf)}, Total Rejected: {len(rdf)}")

if __name__ == "__main__":
    sim = LiveTrueEventLoopSimulator(threshold_harvester=0.60, risk_pct=0.02)
    sim.load_tapes()
    sim.run_simulation()
