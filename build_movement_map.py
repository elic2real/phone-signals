import pandas as pd
import numpy as np
from pathlib import Path
import time

def get_session(hour):
    if 21 <= hour or hour < 6:
        return 'sydney_asia'
    elif 6 <= hour < 14:
        return 'london'
    else:
        return 'new_york'

def scan_parquet_pandas(file_path, pair, lookforward_bars=24):
    try:
        df = pd.read_parquet(file_path)
    except:
        return None
        
    df = df.sort_values('timestamp').reset_index(drop=True)
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    df['session'] = df['hour'].apply(get_session)
    df['node'] = pair + "__" + df['session']
    
    # compute ATR length 14
    prev_close = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - prev_close).abs()
    df['tr3'] = (df['low'] - prev_close).abs()
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=14).mean()
    
    df = df.dropna(subset=['atr']).copy().reset_index(drop=True)
    pip_size = 0.01 if 'JPY' in pair else 0.0001
    df['atr'] = df['atr'].clip(lower=pip_size)
    df['sim_spread_r'] = pip_size / df['atr']
    
    # We create future arrays. 
    # For a row i, the *next* 24 high/low prices determine MFE/MAE.
    
    # Forward Highs [n x 24]
    high_seqs = [df['high'].shift(-j) for j in range(1, lookforward_bars + 1)]
    low_seqs = [df['low'].shift(-j) for j in range(1, lookforward_bars + 1)]
    
    H = np.column_stack(high_seqs) # [N, 24]
    L = np.column_stack(low_seqs)  # [N, 24]
    
    O = df['open'].values[:, None] # [N, 1]
    ATR = df['atr'].values[:, None] # [N, 1]
    
    # MFE / MAE fields
    # Long: 
    #   MFE at step j: (H[:, j] - O) / ATR
    #   MAE at step j: (O - L[:, j]) / ATR
    long_mfe_steps = (H - O) / ATR
    long_mae_steps = (O - L) / ATR
    
    # Short:
    short_mfe_steps = (O - L) / ATR
    short_mae_steps = (H - O) / ATR
    
    # To drop the last `lookforward_bars` where shift created NaNs
    valid = ~(np.isnan(H).any(axis=1))
    
    # To compute hit statistics:
    t_levels = [0.05, 0.10, 0.25, 0.50, 1.00]
    
    def calc_stats(mfe_steps, mae_steps, valid_mask):
        # We process boolean mask for whether threshold was hit
        res_dict = {}
        # cumulative mae up to step j
        cum_mae = np.fmax.accumulate(mae_steps, axis=1) 
        
        for t in t_levels:
            hits = (mfe_steps >= t)
            # Find first True index. argmax returns 0 for all-False, so we must check any()
            hit_any = hits.any(axis=1)
            first_idx = np.argmax(hits, axis=1)
            
            # Bars to hit (1-based)
            bars = np.where(hit_any, first_idx + 1, np.nan)
            
            # MAE at the first hit
            # Advanced indexing to get cum_mae[i, first_idx[i]]
            row_idx = np.arange(len(hits))
            mae_at_hit = np.where(hit_any, cum_mae[row_idx, first_idx], np.nan)
            
            res_dict[f'hit_{t}R'] = hit_any[valid_mask].astype(float)
            res_dict[f'bars_to_{t}R'] = bars[valid_mask]
            res_dict[f'mae_at_{t}R'] = mae_at_hit[valid_mask]
            
        return res_dict

    l_res = calc_stats(long_mfe_steps, long_mae_steps, valid)
    s_res = calc_stats(short_mfe_steps, short_mae_steps, valid)
    
    n_valid = valid.sum()
    nodes_valid = df['node'].values[valid]
    spread_valid = df['sim_spread_r'].values[valid]
    
    l_df = pd.DataFrame(l_res)
    l_df['node'] = nodes_valid
    l_df['direction'] = 'LONG'
    l_df['spread_r'] = spread_valid
    
    s_df = pd.DataFrame(s_res)
    s_df['node'] = nodes_valid
    s_df['direction'] = 'SHORT'
    s_df['spread_r'] = spread_valid
    
    return pd.concat([l_df, s_df], ignore_index=True)

def main():
    root = Path("data_tape_oanda_m5_15_stitched")
    pairs = sorted([d.name.split('=')[1] for d in root.iterdir() if d.is_dir() and 'pair=' in d.name])
    
    # Just run EUR_USD, USD_JPY, GBP_USD first to avoid huge processing time unless we need all
    core_pairs = [p for p in pairs if p in ['EUR_USD', 'GBP_JPY', 'NZD_JPY']]
    if not core_pairs:
        core_pairs = pairs[:3]
        
    print(f"Mapping ceiling across pairs: {core_pairs}")
    
    all_res = []
    
    for pair in core_pairs:
        t0 = time.time()
        print(f"Processing {pair}...")
        pfiles = list((root / f"pair={pair}").glob("*.parquet"))
        if not pfiles: continue
        
        df_scan = scan_parquet_pandas(pfiles[0], pair, lookforward_bars=48) # 4 hours
        if df_scan is not None:
             agg_dict = {'direction': 'count', 'spread_r': 'mean'}
             for t in [0.05, 0.10, 0.25, 0.50, 1.00]:
                 agg_dict[f'hit_{t}R'] = 'mean'
                 agg_dict[f'bars_to_{t}R'] = 'mean'
                 agg_dict[f'mae_at_{t}R'] = 'mean'
                 
             summary = df_scan.groupby(['node', 'direction']).agg(agg_dict).rename(columns={'direction': 'total_candidates'}).reset_index()
             all_res.append(summary)
             print(f"  -> Finished {pair} in {time.time()-t0:.2f}s")
             
    if all_res:
        final_df = pd.concat(all_res, ignore_index=True)
        final_df.to_csv("movement_map_ceiling.csv", index=False)
        print("\nSaved movement_map_ceiling.csv.")
        
        cols = ['node', 'direction', 'total_candidates', 'spread_r', 'hit_0.10R', 'hit_0.25R', 'hit_0.50R', 'hit_1.00R']
        print("\n=== SYSTEM OPPORTUNITY DENSITY MAP ===")
        print(final_df.sort_values(['node', 'direction'])[cols].to_string(index=False))

if __name__ == "__main__":
    main()
