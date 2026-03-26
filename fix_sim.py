import pandas as pd
df = pd.read_csv('true_physics_trades.csv')
print('\nNumber of Harvester trades per specific score decile:')
df['score_bin'] = pd.cut(df['score'], bins=[0.60, 0.70, 0.80, 0.90, 1.00])
groups = df.groupby('score_bin')
for name, group in groups:
    if len(group) == 0: continue
    tg = (group['exit_type']=='TARGET').sum()
    ct = (group['exit_type']=='CUT').sum()
    wr = tg / (tg + ct) if (tg+ct)>0 else 0
    mean_nr = group['net_r'].mean()
    print(f"Score {name}: {len(group)} trades, WR: {wr:.1%}, Net R: {mean_nr:.3f}")
