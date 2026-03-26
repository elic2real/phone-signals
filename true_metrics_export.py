import pandas as pd
import json

df = pd.read_csv('true_physics_trades_opz.csv')
dfr = pd.read_csv('true_physics_rejected_opz.csv')

out = {
    'Total_Trades_Taken': len(df),
    'Total_Trades_Rejected': len(dfr),
    'Win_Rate': float((df['exit_type']=='TARGET').sum() / len(df)),
    'Net_R_Per_Trade': float(df['net_r'].mean()),
    'Cumulative_R': float(df['net_r'].sum()),
    'Starting_Balance': 100.0,
    'Ending_Balance': float(df['final_balance'].iloc[-1]),
    'Max_Drawdown_Pct': float(df['drawdown_pct'].max())
}

with open('TRUE_PHYSICS_REPORT.json', 'w') as f:
    json.dump(out, f, indent=4)
print("Finished True Physics Pipeline Validation")
