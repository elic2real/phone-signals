import pandas as pd
df = pd.read_csv("movement_map_ceiling.csv")
cols = ['node', 'direction', 'mae_at_0.25R', 'mae_at_0.5R', 'mae_at_1.0R']
print(df.sort_values(by=['node', 'direction'])[cols].to_string(index=False))
