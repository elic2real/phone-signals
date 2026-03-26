import pandas as pd
df = pd.read_csv("movement_map_ceiling.csv")
cols = ['node', 'direction', 'total_candidates', 'spread_r', 'hit_0.1R', 'hit_0.25R', 'hit_0.5R', 'hit_1.0R']
print(df.sort_values(by=['node', 'direction'])[cols].to_string(index=False))
