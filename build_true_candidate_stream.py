import pandas as pd
import glob
from pathlib import Path

def main():
    root = Path("compiled_market_nodes")
    files = list(root.glob("*__monday__*/target_entry_stage/target_contextual_v2/target_entry_population.csv"))
    
    if not files:
        print("No target entry populations found.")
        return
        
    print(f"Aggregating {len(files)} candidate streams...")
    dfs = []
    
    safe_columns = [
        'timestamp', 'direction_assumed', 'price', 
        'macro_dir_score', 'micro_dir_score', 'compression_score', 
        'release_quality_score', 'exhaustion_score', 'noise_score', 
        'remaining_budget_score', 'energy_state', 'energy_regime'
    ]
    
    for f in files:
        node = f.parents[2].name
        try:
            df = pd.read_csv(f, engine='pyarrow')
            keep_cols = [c for c in safe_columns if c in df.columns]
            df = df[keep_cols].copy()
            df['node'] = node
            
            score_cols = ['macro_dir_score', 'micro_dir_score', 'compression_score', 'release_quality_score', 'remaining_budget_score']
            df['composite_score'] = df[[c for c in score_cols if c in df.columns]].mean(axis=1)
            
            dfs.append(df)
        except Exception as e:
            pass

    combined = pd.concat(dfs, ignore_index=True)
    combined['timestamp'] = pd.to_datetime(combined['timestamp'])
    combined = combined.sort_values(by=['timestamp', 'composite_score'], ascending=[True, False]).reset_index(drop=True)
    
    out_path = "global_true_candidate_stream.parquet"
    combined.to_parquet(out_path, index=False)
    
    print(f"Stream built: {len(combined)} raw candidates.")
    print("NO future knowledge is contained in this file. Outcomes are explicitly stripped.")
    print("\nSample:")
    print(combined[['timestamp', 'node', 'direction_assumed', 'composite_score']].head())

if __name__ == "__main__":
    main()
