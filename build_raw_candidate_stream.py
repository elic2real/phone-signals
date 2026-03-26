import pandas as pd
import glob
from pathlib import Path
import os

def main():
    root = Path("compiled_market_nodes")
    if not root.exists():
        print("Error: compiled_market_nodes directory missing.")
        return
        
    all_files = list(root.glob("*/session_energy_state_stream.csv"))
    print(f"Found {len(all_files)} node candidate files.")
    
    if not all_files:
        print("No files to merge.")
        return
        
    merged_dfs = []
    
    # We will use pandas with memory mapping to avoid crash or use PyArrow engine. 
    for f in all_files:
        node_name = f.parent.name
        try:
            # use pyarrow engine for fast load
            df = pd.read_csv(f, engine='pyarrow')
            df['node'] = node_name
            merged_dfs.append(df)
        except Exception as e:
            print(f"Failed reading {f.parent.name}: {e}")
            
    print("Combining logic states...")
    combined = pd.concat(merged_dfs, ignore_index=True)
    
    print("Sorting globally by timestamp...")
    combined['timestamp'] = pd.to_datetime(combined['timestamp'])
    combined = combined.sort_values('timestamp').reset_index(drop=True)
    
    bad_columns = [
        'realized_R', 'static_R', 'action_truth', 'future_mfe', 
        'future_mae', 'exit_time', 'pnl', 'target_r', 'profit_now'
    ]
    to_drop = [c for c in bad_columns if c in combined.columns]
    if to_drop:
        combined.drop(columns=to_drop, inplace=True)
        print(f"Dropped leaked outcome columns: {to_drop}")
        
    output_path = "global_raw_candidate_stream.parquet"
    combined.to_parquet(output_path, index=False)
    
    print(f"\nSaved {len(combined)} raw candidates across {len(all_files)} nodes to {output_path}.")
    print("\nSneak peek of the first 5 events (NO OUTCOMES):")
    cols_to_show = ['timestamp', 'node', 'direction', 'price', 'speed_3', 'compression']
    available_cols = [c for c in cols_to_show if c in combined.columns]
    print(combined[available_cols].head().to_string(index=False))

if __name__ == "__main__":
    main()
