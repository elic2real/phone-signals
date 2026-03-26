import pandas as pd

def classify_aee_path(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds path_class and aee_action columns to the DataFrame based on mfe_r, mae_r, max_band.
    Columns added: path_class, aee_action
    """
    df = df.copy()
    def classify(row):
        if row['max_band'] >= 3 and row['mae_r'] < 0.5:
            return 'CLEAN_CONTINUATION'
        elif row['max_band'] >= 2 and row['mae_r'] >= 0.5:
            return 'STALL_CONTINUE'
        elif row['max_band'] <= 1 and row['mae_r'] >= 1:
            return 'FAIL_FAST'
        else:
            return 'WHIPSAW'
    df['path_class'] = df.apply(classify, axis=1)
    action_map = {
        'CLEAN_CONTINUATION': 'HOLD',
        'STALL_CONTINUE': 'PARTIAL',
        'WHIPSAW': 'TIGHTEN',
        'FAIL_FAST': 'CLOSE'
    }
    df['aee_action'] = df['path_class'].map(action_map)
    return df

# Example usage:
# df = classify_aee_path(df)
# df[['path_class', 'aee_action']].value_counts()
