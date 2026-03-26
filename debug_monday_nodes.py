import os
import pandas as pd
import glob
import numpy as np

def debug_node_diagnostics():
    """
    Analyzes Monday nodes individually to identify side bias, 
    threshold mismatch, and structural weakness.
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
            
        # Calculate composite score for debugging
        df['composite_score'] = df[score_cols].mean(axis=1)
        
        # 1. TOTAL CANDIDATES
        total_candidates = len(df)
        
        # 2. SEPARATE LONG/SHORT POPULATION FOR BIAS CHECK
        # Instead of using a 'node' column that isn't in the CSV, we use the local variable node_name
        df['is_long'] = 'long' in node_name.lower()
        
        # 3. HIGH SCORE ANALYSIS (Above 0.72 threshold)
        high_score_df = df[df['composite_score'] >= 0.72].copy()
        
        if len(high_score_df) > 0:
            win_rate = (high_score_df['static_R'] > 0).mean()
            avg_r = high_score_df['static_R'].mean()
            median_score = high_score_df['composite_score'].median()
            
            # SIDE BIAS: Check if one side is significantly worse
            # (In this architecture, nodes are already split by side, so we check the node's performance)
            
            # STRUCTURAL WEAKNESS: High score but negative/low R
            is_structural = avg_r < 0.1
            
            # THRESHOLD MISMATCH: If lowering threshold improves R (Rare) or if only extreme scores win
            extreme_df = df[df['composite_score'] >= 0.85]
            extreme_r = extreme_df['static_R'].mean() if len(extreme_df) > 0 else 0
            
            diagnostic_results.append({
                'node': node_name,
                'candidates': total_candidates,
                'signaled_trades': len(high_score_df),
                'win_rate': win_rate,
                'avg_R': avg_r,
                'extreme_R': extreme_r,
                'status': 'FAIL' if avg_r < 0 else 'PASS',
                'pnl_sum': high_score_df['static_R'].sum() # Simple sum of R
            })
            
    res_df = pd.DataFrame(diagnostic_results).sort_values('avg_R') # Sort by average R to find worst quality
    
    print("\n--- MONDAY NODE DEBUGGING: WORST QUALITY (AVG R) ---")
    print(res_df.head(15).to_string(index=False))
    
    res_df.to_csv('monday_debug_diagnostics.csv', index=False)
    
    # Analyze the absolute worst node specifically
    if not res_df.empty:
        worst_node = res_df.iloc[0]['node']
        print(f"\n--- DEEP DIVE: {worst_node} ---")
        node_df = pd.read_csv(f"compiled_market_nodes/{worst_node}/target_entry_stage/target_contextual_v2/target_entry_population.csv")
        node_df['comp'] = node_df[score_cols].mean(axis=1)
        
        # Look at the distribution of static_R relative to scores
        print("Score Quartiles vs Avg R:")
        node_df['q'] = pd.qcut(node_df['comp'], 4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
        print(node_df.groupby('q')['static_R'].mean())

if __name__ == "__main__":
    debug_node_diagnostics()
