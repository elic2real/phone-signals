import pandas as pd

# Example: stat_lookup built from Stage-6 outcome frequencies
stat_lookup = {0: 0.41, 1: 0.44, 2: 0.53, 3: 0.50}

def derive_entry_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Implements Stage-6 entry logic as specified:
    - accel = speed_3 - speed_10
    - energy_ok: accel > 0.10 AND bias_20 > 0.05 AND compression < 0.40 AND pullback_depth_10 < 0.50
    - mechanical_ok: swing_break_state == 1 AND reclaim_state == 1 AND distance_from_extreme_10 < 0.30
    - statistical_ok: stat_lookup[quarter_phase] >= 0.50
    - entry if votes >= 2
    """
    required_cols = [
        'speed_3', 'speed_10', 'bias_20', 'compression', 'pullback_depth_10',
        'distance_from_extreme_10', 'reclaim_state', 'swing_break_state', 'quarter_phase'
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required Stage-6 columns: {missing}")
    df = df.copy()
    df['accel'] = df['speed_3'] - df['speed_10']
    df['energy_ok'] = (
        (df['accel'] > 0.10)
        & (df['bias_20'] > 0.05)
        & (df['compression'] < 0.40)
        & (df['pullback_depth_10'] < 0.50)
    )
    df['mechanical_ok'] = (
        (df['swing_break_state'] == 1)
        & (df['reclaim_state'] == 1)
        & (df['distance_from_extreme_10'] < 0.30)
    )
    # stat_lookup must be defined from Stage-6 outcome frequencies; placeholder below
    stat_lookup = {0: 0.41, 1: 0.44, 2: 0.53, 3: 0.50}
    df['stat_edge'] = df['quarter_phase'].map(stat_lookup)
    df['statistical_ok'] = df['stat_edge'] >= 0.50
    df['entry_votes'] = df[['energy_ok', 'mechanical_ok', 'statistical_ok']].sum(axis=1)
    df['entry_selected'] = df['entry_votes'] >= 2
    return df

# Example usage:
# df = pd.read_parquet('research_table.parquet')
# df = derive_entry_logic(df)
# df[df['entry_selected']]  # These are your valid entries
