import pandas as pd

print("=== PARQUET TAPE ===")
try:
    df = pd.read_parquet("data_tape_oanda_m5_15_stitched/pair=EUR_USD/stitched.parquet")
    print(df.head(2))
    print(df.columns.tolist())
except Exception as e:
    print(e)
    
print("\n=== GLOBAL MONDAY ALL (TRADE + REJECTED) ===")
try:
    df1 = pd.read_csv("global_monday_trade_log.csv", nrows=1)
    df2 = pd.read_csv("global_monday_rejected_log.csv", nrows=1)
    print("Trade cols:", df1.columns.tolist())
    print("Rejected cols:", df2.columns.tolist())
except Exception as e:
    print(e)

print("\n=== ENTRY OUTCOMES ===")
try:
    df = pd.read_csv("entry_outcomes.csv", nrows=2)
    print(df.head(2).to_dict('records'))
except Exception as e:
    print(e)
