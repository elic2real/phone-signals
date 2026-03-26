import os
import pandas as pd
import glob
import numpy as np

def run_independent_decision_simulation(start_balance=100.0, risk_pct=0.02, threshold=0.65):
    # Search for raw contextual populations (full candidate pool)
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    
    session_results = []
    
    # State-based decision pillars
    score_cols = [
        'macro_dir_score', 
        'micro_dir_score', 
        'compression_score', 
        'release_quality_score', 
        'remaining_budget_score'
    ]
    
    for node_path in all_monday_nodes:
        node_name = os.path.basename(node_path)
        pop_file = f"{node_path}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
        
        if not os.path.exists(pop_file):
            continue
            
        df = pd.read_csv(pop_file)
        if df.empty:
            continue
            
        # 1. CANDIDATE POOL (All rows before filtering by action_truth)
        # PROOF: We are using EVERY row as a potential candidate, ignoring DO_NOT_ENTER vs ENTER
        candidates = df.copy()
        
        # 2. INDEPENDENT DECISION MODEL
        # Ignoring action_truth. Calculating our own composite signal.
        candidates['my_signal_score'] = candidates[score_cols].mean(axis=1)
        
        # 3. STRICT REPLAY ORDER
        candidates['timestamp'] = pd.to_datetime(candidates['timestamp'])
        candidates = candidates.sort_values('timestamp')
        
        # Group by Monday session
        for session_id, session_df in candidates.groupby('session_id'):
            balance = start_balance
            trades_taken = 0
            wins = 0
            losses = 0
            pnl_accum = 0.0
            max_drawdown = 0.0
            peak = balance
            R_list = []
            
            # 4. CAPACITY/CONCURRENCY: 1 concurrent trade per node-session
            last_entry_time = None
            trade_duration_offset = pd.Timedelta(minutes=20) # Assume 20min lockout 
            
            for i, trade in session_df.iterrows():
                # RULE: Only enter if above threshold AND not currently in a trade
                if trade['my_signal_score'] >= threshold:
                    if last_entry_time is not None and trade['timestamp'] <= last_entry_time + trade_duration_offset:
                        continue # Concurrency lockout
                        
                    # 5. INDEPENDENT ENTRY
                    last_entry_time = trade['timestamp']
                    
                    # 6. PnL ACCOUNTING (From realized outcome fields only)
                    risk_amount = start_balance * risk_pct
                    r_multiple = trade['static_R']
                    trade_pnl = risk_amount * r_multiple
                    
                    balance += trade_pnl
                    pnl_accum += trade_pnl
                    trades_taken += 1
                    
                    if r_multiple > 0:
                        wins += 1
                    elif r_multiple < 0:
                        losses += 1
                    
                    R_list.append(r_multiple)
                    
                    # Drawdown tracking
                    if balance > peak: peak = balance
                    dd = (peak - balance) / peak
                    if dd > max_drawdown: max_drawdown = dd
            
            if trades_taken > 0:
                session_results.append({
                    'node': node_name,
                    'session_id': session_id,
                    'pnl': pnl_accum,
                    'trades': trades_taken,
                    'win_rate': wins / trades_taken,
                    'max_dd': max_drawdown,
                    'avg_R': np.mean(R_list)
                })
                
    if not session_results:
        print("No trades triggered by independent decision logic.")
        return
        
    res_df = pd.DataFrame(session_results)
    daily_agg = res_df.groupby('session_id').agg({
        'pnl': 'sum',
        'trades': 'sum',
        'win_rate': 'mean',
        'max_dd': 'max',
        'avg_R': 'mean'
    }).reset_index()
    
    print(f"\n--- INDEPENDENT DECISION SIMULATION (THRESH={threshold}) ---")
    print(daily_agg.to_string(index=False))
    
    print("\n--- PERFORMANCE PROOF ---")
    total_wins = res_df['win_rate'].mean() * 100
    print(f"Mean Win Rate:   {total_wins:.2f}%")
    print(f"Total Net PnL:   ${daily_agg['pnl'].sum():,.2f}")
    print(f"Profit Factor:   {res_df[res_df['pnl']>0]['pnl'].sum() / abs(res_df[res_df['pnl']<0]['pnl'].sum()) if res_df['pnl'].min() < 0 else 'Infinite'}")
    
if __name__ == "__main__":
    # Test with a moderate threshold (0.65) to see if we can still find winners while allowing losses
    run_independent_decision_simulation(start_balance=100.0, risk_pct=0.02, threshold=0.65)
