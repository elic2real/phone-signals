import pandas as pd
df = pd.read_csv('true_physics_trades.csv')

df['entry_time'] = pd.to_datetime(df['entry_time'])
df['hour'] = df['entry_time'].dt.hour
df['day'] = df['entry_time'].dt.dayofweek

print("Wait, let's look at the days and hours strictly together.")
active = df[(df['hour'].isin([11, 12, 15, 23, 0]))]

def score_df(pdf):
    if len(pdf) == 0: return 0
    wins, losses = 0, 0
    tgt, cut = 0.55, -0.35
    net_tgt, net_cut = tgt - 0.05, cut - 0.05
    for _, row in pdf.iterrows():
        hit_tgt = row['max_mfe'] >= tgt
        hit_cut = row['max_mae'] >= abs(cut)
        if hit_tgt and not hit_cut: wins += 1
        elif hit_cut and not hit_tgt: losses += 1
        else: losses += 1 # pessimistic
    
    total = wins + losses
    wr = wins / total if total > 0 else 0
    exp = (wr * net_tgt) + ((1 - wr) * net_cut)
    return len(pdf), wr, exp

c, w, e = score_df(active)
print(f"Isolated Best Hours (0, 11, 12, 15, 23): {c} trades, WR: {w:.1%}, Net Ex: {e:+.3f} R")

# Wait, we have the true raw candidates `global_true_candidate_stream.parquet`.
# Let's filter the stream to those hours, bump the minimum score to 0.70 instead of 0.60, 
# and rerun the exact physical test using our optimized bounds +0.55 / -0.35.

