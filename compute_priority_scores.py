import pandas as pd

# Example: stat_lookup built from Stage-6 outcome frequencies
stat_lookup = {0: 0.41, 1: 0.44, 2: 0.53, 3: 0.50}

def compute_priority_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds priority_score and rank columns to the DataFrame for all entry_selected rows.
    Uses available columns: speed, efficiency, extension, risk_ratio, composite_score.
    Columns added: priority_score, rank
    """
    required_cols = ['speed_3', 'speed_10', 'bias_20', 'quarter_phase', 'entry_selected']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required Stage-6 columns for priority: {missing}")
    df = df.copy()
    df['accel'] = (df['speed_3'] - df['speed_10']).clip(lower=0)
    df['trend_align'] = df['bias_20'].clip(lower=0)
    stat_lookup = {0: 0.41, 1: 0.44, 2: 0.53, 3: 0.50}  # Replace with actual frequencies
    df['stat_edge'] = df['quarter_phase'].map(stat_lookup)
    df['priority_score'] = (
        0.45 * df['stat_edge'] +
        0.35 * df['accel'] +
        0.20 * df['trend_align']
    )
    # Only rank selected entries
    df_selected = df[df['entry_selected']].copy()
    df_selected = df_selected.sort_values('priority_score', ascending=False)
    df_selected['rank'] = range(1, len(df_selected) + 1)
    # Merge rank back into main df (NaN for non-selected)
    df = df.merge(df_selected[['rank']], left_index=True, right_index=True, how='left')
    return df

