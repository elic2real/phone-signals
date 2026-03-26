import pandas as pd
from derive_entry_logic import derive_entry_logic
from compute_priority_scores import compute_priority_scores
from classify_aee_path import classify_aee_path


# Load Monday CSV research table
RESEARCH_TABLE_PATH = 'compiled_market_nodes/EUR_USD__monday__new_york/stage1_6/phase4/opportunity_zones_labeled.csv'
df = pd.read_csv(RESEARCH_TABLE_PATH)

# Stage 1: Entry logic only
entry_df = derive_entry_logic(df)
entry_trades = entry_df[entry_df['entry_selected']].copy()

# Stage 2: Entry + Priority (top N)
priority_df = compute_priority_scores(entry_df)
N = 20  # Set to your risk capacity or desired number of trades
priority_trades = priority_df[priority_df['entry_selected']].sort_values('priority_score', ascending=False).head(N).copy()


# Stage 3: Entry + Priority + AEE (only if replay columns are present)
has_replay = all(col in priority_trades.columns for col in ['mfe_r', 'mae_r', 'max_band', 'realized_r'])
if has_replay:
    aee_df = classify_aee_path(priority_trades)
    harvester = aee_df[aee_df['aee_action'].isin(['PARTIAL', 'HOLD'])].copy()
    runner = aee_df.copy()

def summarize(trades, label):
    d = {'Stage': label, 'Trade Count': len(trades)}
    if 'realized_r' in trades.columns:
        d['Win Rate'] = float((trades['realized_r'] > 0).mean())
        d['Avg MFE (R)'] = float(trades['mfe_r'].mean()) if 'mfe_r' in trades.columns else None
        d['Avg MAE (R)'] = float(trades['mae_r'].mean()) if 'mae_r' in trades.columns else None
        d['Expectancy (R)'] = float(trades['realized_r'].mean())
        d['Total Profit (R)'] = float(trades['realized_r'].sum())
    return d

# Build summary tables
summary = []
summary.append(summarize(entry_trades, 'Entry Only - All'))
summary.append(summarize(priority_trades, 'Entry+Priority - All'))
if has_replay:
    summary.append(summarize(harvester, 'Entry+Priority+AEE - Harvester'))
    summary.append(summarize(runner, 'Entry+Priority+AEE - Runner'))

# Convert to DataFrame for pretty printing
summary_df = pd.DataFrame(summary)
print(summary_df.to_markdown(index=False))

# Optionally, save the summary table
summary_df.to_csv('logic_stage_performance_summary.csv', index=False)
