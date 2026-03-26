import os
import pandas as pd
import glob
import numpy as np
from datetime import datetime

class GlobalMondaySimulator:
    def __init__(self, start_balance=100.0, risk_pct=0.02, threshold=0.72, slippage_r=0.1, margin_req=1.0):
        self.start_balance = start_balance
        self.risk_pct = risk_pct
        self.threshold = threshold
        self.slippage_r = slippage_r
        self.margin_req = margin_req # Percentage of trade size reserved as margin
        
        # Account State
        self.balance = start_balance
        self.equity = start_balance
        self.margin_used = 0.0
        self.open_trades = []
        
        # Logs
        self.trade_log = []
        self.rejected_log = []
        self.equity_curve = []
        
    def load_candidates(self):
        all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
        score_cols = ['macro_dir_score', 'micro_dir_score', 'compression_score', 'release_quality_score', 'remaining_budget_score']
        
        all_dfs = []
        for node_path in all_monday_nodes:
            pop_file = f"{node_path}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
            if not os.path.exists(pop_file): continue
            
            df = pd.read_csv(pop_file)
            if df.empty: continue
            
            df['node'] = os.path.basename(node_path)
            df['composite_score'] = df[score_cols].mean(axis=1)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # For simplicity in this 'lifecycle' version, we estimate an exit time.
            # Real exit logic requires price paths, but for this 'Account Physics' baseline, 
            # we use 'trade_duration_bars' proxy (e.g. 2 hours / 24 bars of 5m)
            # or we could use the next candle where 'is_exit' is true if available.
            # Here 우리는 assume a 4-hour hold (48 bars of 5m) as a physics proxy for margin lock.
            df['exit_timestamp'] = df['timestamp'] + pd.Timedelta(hours=4)
            
            all_dfs.append(df)
            
        if not all_dfs: return pd.DataFrame()
        
        master_df = pd.concat(all_dfs)
        # Rule 1 & 9: Strict global timestamp ordering and deterministic secondary sort
        master_df = master_df.sort_values(by=['timestamp', 'composite_score', 'node'], ascending=[True, False, True])
        return master_df

    def run(self):
        master_df = self.load_candidates()
        if master_df.empty:
            print("No candidates found.")
            return

        # Timeline Events: (timestamp, type, data)
        # Types: 'ENTRY', 'EXIT'
        events = []
        for i, row in master_df.iterrows():
            if row['composite_score'] >= self.threshold:
                events.append((row['timestamp'], 'ENTRY', row))
        
        # Sort events by time
        events.sort(key=lambda x: x[0])
        
        # Main Event Loop (Rule 13)
        for current_time, event_type, data in events:
            # 1. Process EXITS first to free margin (Rule 8)
            self._process_exits(current_time)
            
            if event_type == 'ENTRY':
                # Rule 2: Shared balance/margin check
                available_balance = self.balance - self.margin_used
                
                # Rule 5 & 7: Competitive execution
                risk_amount = self.balance * self.risk_pct
                # Simple margin model: margin used is 5x the risk amount (leveraged proxy)
                trade_margin = risk_amount * 5 
                
                if available_balance >= trade_margin:
                    # OPEN TRADE (Rule 3)
                    self.margin_used += trade_margin
                    
                    trade_obj = {
                        'entry_time': current_time,
                        'exit_time': data['exit_timestamp'],
                        'node': data['node'],
                        'side': 'LONG' if 'long' in data['node'] else 'SHORT',
                        'risk_usd': risk_amount,
                        'margin': trade_margin,
                        'static_R': data['static_R'],
                        'score': data['composite_score']
                    }
                    self.open_trades.append(trade_obj)
                    
                    # Log entry
                    self.equity_curve.append({
                        'timestamp': current_time,
                        'balance': self.balance,
                        'margin_used': self.margin_used,
                        'open_positions': len(self.open_trades)
                    })
                else:
                    self.rejected_log.append({
                        'timestamp': current_time,
                        'node': data['node'],
                        'reason': 'Insufficient Margin/Balance',
                        'available': available_balance,
                        'required': trade_margin
                    })

        # Final close out
        self._process_exits(master_df['timestamp'].max() + pd.Timedelta(days=1))
        
        self._generate_reports()

    def _process_exits(self, current_time):
        # Rule 3, 6, 8: Evolution and Exit
        active = []
        for trade in self.open_trades:
            if current_time >= trade['exit_time']:
                # CLOSE TRADE
                realized_r = trade['static_R'] - self.slippage_r
                pnl = trade['risk_usd'] * realized_r
                
                self.balance += pnl
                self.margin_used -= trade['margin']
                
                trade['closed_at'] = current_time
                trade['pnl'] = pnl
                trade['realized_R'] = realized_r
                self.trade_log.append(trade)
            else:
                active.append(trade)
        self.open_trades = active

    def _generate_reports(self):
        if not self.trade_log:
            print("No trades closed.")
            return

        t_df = pd.DataFrame(self.trade_log)
        e_df = pd.DataFrame(self.equity_curve)
        r_df = pd.DataFrame(self.rejected_log)
        
        t_df.to_csv('global_monday_trade_log.csv', index=False)
        e_df.to_csv('global_monday_equity_curve.csv', index=False)
        r_df.to_csv('global_monday_rejected_log.csv', index=False)
        
        # Rule 10 & 12 analysis
        print(f"\n--- GLOBAL MONDAY PHYSICS SIMULATION ---")
        print(f"Final Balance: ${self.balance:,.2f}")
        print(f"Total Trades:  {len(t_df)}")
        print(f"Rejected:      {len(r_df)}")
        print(f"Win Rate:      {(t_df['realized_R']>0).mean()*100:.2f}%")
        print(f"Max Concurrent Positions: {e_df['open_positions'].max() if not e_df.empty else 0}")
        
        # NODE DIAGNOSTICS
        node_stats = []
        for node, group in t_df.groupby('node'):
            avg_r = group['realized_R'].mean()
            neg_r_count = (group['realized_R'] < 0).sum()
            node_stats.append({
                'node': node,
                'trades': len(group),
                'avg_R': avg_r,
                'net_pnl': group['pnl'].sum(),
                'failure_signals': 'Side Bias' if abs(group['realized_R'].sum()) < 2 and len(group) > 5 else 'Structural'
            })
        
        n_df = pd.DataFrame(node_stats).sort_values('net_pnl')
        n_df.to_csv('monday_node_diagnostics.csv', index=False)
        print("\n--- WORST PERFORMING MONDAY NODES ---")
        print(n_df.head(10).to_string(index=False))

if __name__ == "__main__":
    sim = GlobalMondaySimulator(start_balance=100.0, risk_pct=0.01) # Low risk for first physics pass
    sim.run()
