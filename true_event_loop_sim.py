import os
import pandas as pd
import numpy as np

class EventDrivenMondaySimulator:
    def __init__(self, start_balance=100.0, risk_pct=0.02, threshold_harvester=0.60, threshold_runner=0.85, slippage_r=0.05):
        self.start_balance = start_balance
        self.risk_pct = risk_pct 
        self.threshold_harvester = threshold_harvester
        self.threshold_runner = threshold_runner
        self.slippage_r = slippage_r

        self.runner_partial_tp = 1.5
        self.runner_partial_fraction = 0.9
        self.balance = start_balance
        self.locked_margin = 0.0
        self.active_trades = []
        
    def fetch_margins(self):
        try:
            df = pd.read_csv('oanda_margin_requirements.csv')
            margin_map = {}
            for row in df.itertuples():
                try:
                    margin_map[row.pair] = float(row.margin_rate.replace('%','')) / 100.0
                except: pass
            return margin_map
        except: return {}

    def simulate_harvester_logic(self, raw_r):
        if raw_r >= 0.25:
            gross = 0.25
            net = gross - self.slippage_r
            return net, gross, 'TARGET', self.slippage_r
        elif raw_r > 0:
            gross = raw_r
            net = gross - self.slippage_r
            return net, gross, 'STALL', self.slippage_r
        else:
            gross = max(-0.35, float(raw_r))
            net = gross - self.slippage_r
            return net, gross, 'CUT', self.slippage_r

    def simulate_runner_logic(self, raw_r):
        if raw_r >= self.runner_partial_tp:
            gross = (self.runner_partial_tp * self.runner_partial_fraction) + (raw_r * (1.0 - self.runner_partial_fraction))
            net = gross - self.slippage_r
            return net, gross, 'TARGET', self.slippage_r
        else:
            gross = max(-0.5, float(raw_r))
            net = gross - self.slippage_r
            return net, gross, 'CUT', self.slippage_r

    def load_and_prepare_events(self):
        margin_map = self.fetch_margins()
        
        df_ex = pd.read_csv('global_monday_trade_log.csv') if os.path.exists('global_monday_trade_log.csv') else pd.DataFrame()
        
        records = []
        for row in df_ex.itertuples():
            score = float(getattr(row, 'score', getattr(row, 'setup_quality', 0)))
            if score < self.threshold_harvester: continue
                
            ts_start = pd.to_datetime(getattr(row, 'entry_time', getattr(row, 'timestamp', None)))
            ts_end = pd.to_datetime(getattr(row, 'exit_time', getattr(row, 'closed_at', ts_start + pd.Timedelta(minutes=55))))
            
            # calculate hold time in minutes
            hold_time_mins = (ts_end - ts_start).total_seconds() / 60.0

            base_pair = row.node.split('__')[0]
            margin_req = margin_map.get(base_pair, 0.05) 
            mode = "RUNNER" if score >= self.threshold_runner else "HARVESTER"
                
            records.append({
                'timestamp': ts_start, 'type': 'ENTRY_SIGNAL', 'node': row.node,
                'data': {'timestamp': ts_start, 'composite_score': score, 'mode': mode, 'margin_req': margin_req, 'hold_time_mins': hold_time_mins}
            })
            
            raw_r = getattr(row, 'realized_R', getattr(row, 'static_R', 0))
            if pd.isna(raw_r): raw_r = 0
            
            records.append({
                'timestamp': ts_end, 'type': 'TRADE_EXIT', 'node': row.node,
                'data': {'timestamp': ts_start, 'state_machine_R': raw_r, 'hold_time_mins': hold_time_mins, 'mode': mode}
            })

        df_events = pd.DataFrame(records)
        if not df_events.empty:
            df_events = df_events.sort_values('timestamp').reset_index(drop=True)
        return df_events

    def run_simulation(self):
        events = self.load_and_prepare_events()
        if events.empty: return

        active_positions = {} 
        self.trade_log = []
        self.margin_used = 0

        for row in events.itertuples():
            curr_time = row.timestamp
            etype = row.type
            node = row.node
            data = row.data
            
            if etype == 'TRADE_EXIT':
                key = (node, data['timestamp'])
                if key in active_positions:
                    trade = active_positions.pop(key)
                    
                    if trade['mode'] == "RUNNER":
                        net_r, gross_r, exit_type, friction = self.simulate_runner_logic(data['state_machine_R'])
                    else:
                        net_r, gross_r, exit_type, friction = self.simulate_harvester_logic(data['state_machine_R'])
                    
                    pnl = trade['risk_usd'] * net_r
                    self.balance += pnl
                    self.margin_used -= trade['margin']
                    
                    self.trade_log.append({
                        **trade, 'exit_time': curr_time, 'hold_time_mins': data.get('hold_time_mins', 0),
                        'pnl': pnl, 'net_r': net_r, 'gross_r': gross_r, 'friction': friction, 'exit_type': exit_type,
                        'final_balance': self.balance
                    })
                
            elif etype == 'ENTRY_SIGNAL':
                if any(t['node'] == node for t in active_positions.values()): continue

                risk_amount = self.balance * self.risk_pct
                margin_multiplier = 1.0 / data['margin_req'] 
                required_margin = risk_amount * margin_multiplier
                
                if (self.balance - self.margin_used) >= required_margin:
                    trade_obj = {
                        'entry_time': curr_time, 'node': node, 'risk_usd': risk_amount,
                        'margin': required_margin, 'score': data['composite_score'], 'mode': data['mode']
                    }
                    self.margin_used += required_margin
                    active_positions[(node, data['timestamp'])] = trade_obj
                    
        self._finalize_reports()
        
    def _finalize_reports(self):
        trades_df = pd.DataFrame(self.trade_log)
        if trades_df.empty: return
        trades_df.to_csv('micro_harvest_enriched_monday_trades.csv', index=False)

if __name__ == "__main__":
    sim = EventDrivenMondaySimulator()
    sim.run_simulation()
