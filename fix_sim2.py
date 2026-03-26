import pandas as pd
df = pd.read_csv('true_physics_trades_opz.csv')
print("\nRejected Log:")
dfr = pd.read_csv('true_physics_rejected_opz.csv')
print(dfr['reason'].value_counts())
