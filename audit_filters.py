import pandas as pd
import glob
import os

def audit_filters():
    all_monday_nodes = glob.glob('compiled_market_nodes/*__monday__*')
    
    counts = {
        'total_raw': 0,
        'above_0.72_raw_mean': 0,
        'above_0.72_weighted': 0,
        'above_0.72_weighted_with_budget': 0
    }
    
    score_cols = ['macro_dir_score', 'micro_dir_score', 'compression_score', 'release_quality_score', 'remaining_budget_score']

    for node in all_monday_nodes:
        pop_file = f"{node}/target_entry_stage/target_contextual_v2/target_entry_population.csv"
        if not os.path.exists(pop_file): continue
        
        df = pd.read_csv(pop_file)
        counts['total_raw'] += len(df)
        
        # 1. Old Logic (Simple Mean)
        df['old_comp'] = df[score_cols].mean(axis=1)
        counts['above_0.72_raw_mean'] += len(df[df['old_comp'] >= 0.72])
        
        # 2. New Weighted Logic (Macro/Micro Bias 60%)
        df['weighted'] = (0.3*df['macro_dir_score'] + 0.3*df['micro_dir_score'] + 0.2*df['compression_score'] + 0.2*df['release_quality_score'])
        counts['above_0.72_weighted'] += len(df[df['weighted'] >= 0.72])
        
        # 3. Full Logic (Weighted * Remaining Budget Score)
        df['full'] = df['weighted'] * df['remaining_budget_score']
        counts['above_0.72_weighted_with_budget'] += len(df[df['full'] >= 0.72])
        
    print("\n--- SIGNAL DROPOFF AUDIT ---")
    for k, v in counts.items():
        print(f"{k}: {v:,}")

if __name__ == "__main__":
    audit_filters()
