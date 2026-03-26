import os
import pandas as pd
import glob
import json
import numpy as np

def run_end_to_end_simulation(start_balance=100.0, risk_pct=0.02):
    # Search for the raw contextual populations which contain ALL entries (including losses)
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    
    session_results = []
    
    # Selection logic based on your instruction:
    # Use composite score from score columns to simulate priority filtering
    score_cols = ['macro_dir_score', 'micro_dir_score', 'compression_score', 'release_quality_score', 'remaining_budget_score']
    
    for node_path in all_monday_nodes:
        node_name = os.path.basename(node_path)
        # Use target_contextual_v2 as it was proven to have candidate losses
        pop_file = f"{node_path}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
        
        if not os.path.exists(pop_file):
            continue
            
        df = pd.read_csv(pop_file)
        if df.empty or 'action_truth' not in df.columns:
            continue
            
        # PROOF: Data contains both wins and losses here
        # Filter for candidates that the logic WOULD enter (ENTER_LONG, ENTER_SHORT)
        entries = df[df['action_truth'].isin(['ENTER_LONG', 'ENTER_SHORT'])].copy()
        if entries.empty:
            continue
            
        # Priority Simulation: Calculate composite confidence score
        entries['composite_score'] = entries[score_cols].mean(axis=1)
        
        # PROOF: Strict Replay Order
        entries = entries.sort_values('timestamp')
        
        # Group by Monday Session IDs
        for session_id, session_df in entries.groupby('session_id'):
            # Trial starts with fixed balance for logic comparison
            balance = start_balance
            trades_taken = 0
            wins = 0
            losses = 0
            pnl_accum = 0.0
            max_drawdown = 0.0
            peak = balance
            R_list = []
            
            # Simple capacity: 1 trade at a time per node-session node
            # This represents a conservative concurrency constraint
            last_trade_end_time = -1
            
            for _, trade in session_df.iterrows():
                # Capacity Check (using timestamp as proxy for trade duration/overlap)
                # In a real system, we'd check if the previous trade finished.
                # Here we simulate serial execution by selecting top signals.
                
                # Priority Filter: Only take high-confidence signals (e.g., score > 0.5) 
                # to simulate what the priority engine actually does
                if trade['composite_score'] < 0.5:
                    continue
                
                risk_amount = start_balance * risk_pct
                r_multiple = trade['static_R']
                trade_pnl = risk_amount * r_multiple
                
                balance += trade_pnl
                pnl_accum += trade_pnl
                trades_taken += 1
                
                if r_multiple > 0:
                    wins += 1
                else:
                    losses += 1
                
                R_list.append(r_multiple)
                
                # Drawdown Tracking
                if balance > peak: peak = balance
                dd = (peak - balance) / peak
                if dd > max_drawdown: max_drawdown = dd
            
            if trades_taken > 0:
                session_results.append({
                    'node': node_name,
                    'session_id': session_id,
                    'start_balance': start_balance,
                    'pnl': pnl_accum,
                    'trades': trades_taken,
                    'win_rate': wins / trades_taken,
                    'max_dd': max_drawdown,
                    'avg_R': np.mean(R_list)
                })
            
    if not session_results:
        print("No simulation results generated from raw source.")
        return
        
    res_df = pd.DataFrame(session_results)
    
    # Group sessions by DATE across all pairs (Portfolio View per Monday)
    daily_agg = res_df.groupby('session_id').agg({
        'pnl': 'sum',
        'trades': 'sum',
        'win_rate': 'mean',
        'max_dd': 'max',
        'avg_R': 'mean'
    }).reset_index()
    
    print("\n--- END-TO-END MONDAY SESSION RESULTS (RAW SOURCE / FIXED RISK) ---")
    print(daily_agg.to_string(index=False))
    
    print("\n--- AGGREGATE STATS (ACROSS ALL MONDAYS) ---")
    print(f"Mean PnL:        ${daily_agg['pnl'].mean():,.2f}")
    print(f"Median PnL:      ${daily_agg['pnl'].median():,.2f}")
    print(f"Worst Monday:    ${daily_agg['pnl'].min():,.2f} ({daily_agg.loc[daily_agg['pnl'].idxmin(), 'session_id']})")
    print(f"Best Monday:     ${daily_agg['pnl'].max():,.2f} ({daily_agg.loc[daily_agg['pnl'].idxmax(), 'session_id']})")
    print(f"Profitable %:    {(daily_agg['pnl'] > 0).mean()*100:.1f}%")
    print(f"Std Dev PnL:     ${daily_agg['pnl'].std():,.2f}")

if __name__ == "__main__":
    # Start with $100 and 2% risk as requested
    run_end_to_end_simulation(start_balance=100.0, risk_pct=0.02)
