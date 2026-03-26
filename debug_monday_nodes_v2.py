import os
import pandas as pd
import glob
import numpy as np

def debug_node_diagnostics():
    """
    Analyzes Monday nodes individually using realistic physics penalties.
    """
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    score_cols = [
        'macro_dir_score', 
        'micro_dir_score', 
        'compression_score', 
        'release_quality_score', 
        'remaining_budget_score'
    ]
    
    diagnostic_results = []
    
    for node_path in all_monday_nodes:
        node_name = os.path.basename(node_path)
        pop_file = f"{node_path}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
        
        if not os.path.exists(pop_file):
            continue
            
        df = pd.read_csv(pop_file)
        if df.empty:
            continue
            
        # Physics Simulator subtracts -0.1 R for slippage
        # Calculate composite score
        df['composite_score'] = df[score_cols].mean(axis=1)
        
        # Filter for our entry threshold
        high_score_df = df[df['composite_score'] >= 0.72].copy()
        
        if len(high_score_df) > 0:
            # Apply physics penalty
            high_score_df['realized_R'] = high_score_df['static_R'] - 0.1
            
            win_rate = (high_score_df['realized_R'] > 0).mean()
            avg_r = high_score_df['realized_R'].mean()
            pnl_sum = high_score_df['realized_R'].sum()
            
            diagnostic_results.append({
                'node': node_name,
                'trades': len(high_score_df),
                'win_rate': win_rate,
                'avg_R': avg_r,
                'pnl_sum': pnl_sum
            })
            
    res_df = pd.DataFrame(diagnostic_results).sort_values('avg_R')
    
    print("\n--- MONDAY NODE DEBUGGING: REALIZED QUALITY (AVG R - 0.1) ---")
    print(res_df.head(20).to_string(index=False))
    
    res_df.to_csv('monday_debug_diagnostics_v2.csv', index=False)

if __name__ == "__main__":
    debug_node_diagnostics()
