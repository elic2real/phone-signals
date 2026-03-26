import os
import pandas as pd
import glob
import json
import numpy as np

def run_simulation(start_balance=10000.0, risk_pct=0.01):
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    
    session_results = []
    
    for node_path in all_monday_nodes:
        node_name = os.path.basename(node_path)
        pop_file = f"{node_path}/target_entry_no_timeouts/target_entry_population.csv"
        
        if not os.path.exists(pop_file):
            continue
            
        df = pd.read_csv(pop_file)
        if df.empty:
            continue
            
        # Filter for entry signals only
        entries = df[df['action_truth'].isin(['ENTER_LONG', 'ENTER_SHORT'])].copy()
        if entries.empty:
            continue
            
        # Composite score for priority simulation (since priority_rank wasn't in CSV directly)
        score_cols = ['macro_dir_score', 'micro_dir_score', 'compression_score', 'release_quality_score', 'remaining_budget_score']
        entries['composite_score'] = entries[score_cols].mean(axis=1)
        
        # Sort by timestamp to prevent future leakage
        entries = entries.sort_values('timestamp')
        
        # Group by session_id (which maps to specific Monday dates)
        for session_id, session_df in entries.groupby('session_id'):
            # Trial starts with fixed balance
            balance = start_balance
            equity_curve = [balance]
            trades_taken = 0
            wins = 0
            losses = 0
            pnl_accum = 0.0
            max_drawdown = 0.0
            peak = balance
            R_list = []
            
            # Simple capacity: 1 trade at a time per node session (most restrictive/clean for trial)
            # Replay each trade in order
            for _, trade in session_df.iterrows():
                risk_amount = start_balance * risk_pct # Fixed risk per trial rule
                
                # static_R is +1 for win, -1 for loss
                # Note: target_distance is used to scale pips, but static_R is the normalized R
                r_multiple = trade['static_R']
                trade_pnl = risk_amount * r_multiple
                
                balance += trade_pnl
                equity_curve.append(balance)
                
                pnl_accum += trade_pnl
                trades_taken += 1
                if r_multiple > 0:
                    wins += 1
                else:
                    losses += 1
                
                R_list.append(r_multiple)
                
                # Update peak for drawdown
                if balance > peak:
                    peak = balance
                dd = (peak - balance) / peak
                if dd > max_drawdown:
                    max_drawdown = dd
            
            avg_R = np.mean(R_list) if R_list else 0
            win_rate = wins / trades_taken if trades_taken > 0 else 0
            
            session_results.append({
                'node': node_name,
                'session_id': session_id,
                'start_balance': start_balance,
                'end_balance': balance,
                'pnl': pnl_accum,
                'trades': trades_taken,
                'win_rate': win_rate,
                'max_dd': max_drawdown,
                'avg_R': avg_R
            })
            
    if not session_results:
        print("No simulation results generated.")
        return
        
    res_df = pd.DataFrame(session_results)
    
    # 1. Per-session Independent Results (Aggregation by session_id involves multiple nodes)
    # But user wants "Each Monday is its own unit first"
    # Let's aggregate by session_id across ALL nodes to see total portfolio performance per Monday
    
    daily_agg = res_df.groupby('session_id').agg({
        'pnl': 'sum',
        'trades': 'sum',
        'win_rate': 'mean',
        'max_dd': 'max',
        'avg_R': 'mean'
    }).reset_index()
    
    print("\n--- INDEPENDENT MONDAY SESSION RESULTS (FIXED RISK) ---")
    print(daily_agg.to_string(index=False))
    
    # 2. Global Aggregates
    print("\n--- GLOBAL AGGREGATE STATS ---")
    stats = {
        'Mean Monday PnL': daily_agg['pnl'].mean(),
        'Median Monday PnL': daily_agg['pnl'].median(),
        'Worst Monday': daily_agg['pnl'].min(),
        'Best Monday': daily_agg['pnl'].max(),
        'Profitable Mondays %': (daily_agg['pnl'] > 0).mean() * 100,
        'Std Dev PnL': daily_agg['pnl'].std()
    }
    for k, v in stats.items():
        print(f"{k}: {v:,.2f}")
        
    # 3. Compounded Growth (Second Pass)
    compounded_balance = start_balance
    compounded_history = [compounded_balance]
    
    # We sort by session_id to ensure chronological chaining
    for s_pnl in daily_agg.sort_values('session_id')['pnl']:
        # Note: daily_pnl is sum of fixed risk pnl based on start_balance
        # We need to scale it to the CURRENT compounded balance
        # percentage_return = daily_pnl / start_balance
        pct_return = s_pnl / start_balance
        actual_gain = compounded_balance * pct_return
        compounded_balance += actual_gain
        compounded_history.append(compounded_balance)
        
    print("\n--- COMPOUNDED GROWTH SIMULATION ---")
    print(f"Starting: ${start_balance:,.2f}")
    print(f"Final:    ${compounded_balance:,.2f}")
    print(f"Total Return: {((compounded_balance/start_balance)-1)*100:.2f}%")

if __name__ == "__main__":
    run_simulation(start_balance=100.0, risk_pct=0.02)
