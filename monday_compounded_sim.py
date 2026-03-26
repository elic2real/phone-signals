import os
import pandas as pd
import glob
import numpy as np

def run_compounded_simulation(start_balance=100.0, risk_pct=0.02, threshold=0.72):
    """
    Compounded multi-day simulation.
    Risk is calculated as 2% of the dynamic current_balance.
    The balance carries over between Monday sessions.
    """
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    
    score_cols = [
        'macro_dir_score', 
        'micro_dir_score', 
        'compression_score', 
        'release_quality_score', 
        'remaining_budget_score'
    ]
    
    # Collect all candidates across all nodes
    all_candidates = []
    
    for node_path in all_monday_nodes:
        node_name = os.path.basename(node_path)
        pop_file = f"{node_path}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
        
        if not os.path.exists(pop_file):
            continue
            
        df = pd.read_csv(pop_file)
        if df.empty:
            continue
            
        df['node'] = node_name
        df['my_signal_score'] = df[score_cols].mean(axis=1)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        all_candidates.append(df)
        
    if not all_candidates:
        print("No candidates found.")
        return
        
    full_df = pd.concat(all_candidates)
    
    # GLOBAL TIMELINE REPLAY
    # We sort strictly by timestamp to ensure no lookahead across nodes
    full_df = full_df.sort_values(['timestamp', 'node'])
    
    current_balance = start_balance
    peak_balance = start_balance
    max_drawdown = 0.0
    
    trade_history = []
    # Concurrency tracker: Last entry time PER NODE to prevent overlap on same instrument
    node_last_entry = {node: None for node in full_df['node'].unique()}
    lockout = pd.Timedelta(minutes=20)
    
    for _, trade in full_df.iterrows():
        # ENTRY FILTER
        if trade['my_signal_score'] >= threshold:
            node = trade['node']
            
            # Lockout check for the specific node
            if node_last_entry[node] is not None and trade['timestamp'] <= node_last_entry[node] + lockout:
                continue
                
            # COMPONENT EXECUTION
            risk_amount = current_balance * risk_pct
            
            # 6. SLIPPAGE & TRANSACTION COST (PROXY: -0.1 R per trade, assuming 1.0 pips cost)
            # 7. RISK CAPS (Never risk more than $2000 per trade to model size liquidity constraint)
            risk_amount = min(risk_amount, 2000.0) 
            
            if risk_amount > current_balance: risk_amount = current_balance
            
            r_multiple = trade['static_R']
            # Slippage deduction
            pnl = risk_amount * (r_multiple - 0.1) 
            
            old_balance = current_balance
            current_balance += pnl
            node_last_entry[node] = trade['timestamp']
            
            # Record trade
            trade_history.append({
                'timestamp': trade['timestamp'],
                'node': node,
                'score': trade['my_signal_score'],
                'R': r_multiple,
                'pnl': pnl,
                'balance': current_balance
            })
            
            # Drawdown
            if current_balance > peak_balance:
                peak_balance = current_balance
            dd = (peak_balance - current_balance) / peak_balance
            if dd > max_drawdown:
                max_drawdown = dd
                
    if not trade_history:
        print("No trades executed.")
        return
        
    history_df = pd.DataFrame(trade_history)
    
    # 8. EXPORT PER-NODE RESULTS (Requested for table)
    per_node = history_df.groupby('node').agg({
        'R': ['count', lambda x: (x > 0).mean()],
        'pnl': 'sum',
        'R': 'mean' # Re-calculate for avg_R
    }).reset_index()
    
    # Simple formatting for table output
    node_stats = []
    for node, group in history_df.groupby('node'):
        wins = (group['R'] > 0).sum()
        trades = len(group)
        peak = group['balance'].max()
        # Local DD for the node
        node_stats.append({
            'node': node,
            'trades': trades,
            'win_rate': wins / trades,
            'avg_R': group['R'].mean(),
            'net_pnl': group['pnl'].sum(),
            'max_dd': 0.0 # DD is tricky per-node in compounded, omit for now or use session
        })
    
    node_df = pd.DataFrame(node_stats)
    node_df.to_csv('monday_per_node_performance.csv', index=False)
    
    print(f"\n--- COMPOUNDED MONDAY SIMULATION (THRESH={threshold}) ---")
    print(f"Start Balance: ${start_balance:,.2f}")
    print(f"Final Balance: ${current_balance:,.2f}")
    print(f"Total Return:  {((current_balance/start_balance)-1)*100:,.2f}%")
    print(f"Max Drawdown:  {max_drawdown*100:.2f}%")
    print(f"Total Trades:  {len(history_df)}")
    print(f"Win Rate:      {(history_df['R']>0).mean()*100:.2f}%")
    print(f"Average R:     {history_df['R'].mean():.4f}")
    
    # Daily aggregation for equity curve feel
    history_df['day'] = history_df['timestamp'].dt.date
    daily = history_df.groupby('day').agg({'pnl': 'sum', 'balance': 'last'}).reset_index()
    print("\n--- DAILY PROGRESSION ---")
    print(daily.to_string(index=False))

if __name__ == "__main__":
    run_compounded_simulation(start_balance=100.0, risk_pct=0.02, threshold=0.72)
