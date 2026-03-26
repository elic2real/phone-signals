import pandas as pd
import json

def derive_permissions():
    df = pd.read_csv('true_event_loop_monday_trades.csv')
    
    stats = df.groupby('node').agg(
        trade_count=('entry_time', 'count'),
        total_pnl=('pnl', 'sum'),
        avg_R=('realized_R', 'mean')
    ).reset_index()

    permissions = {}
    for _, row in stats.iterrows():
        node = row['node']
        avg_r = row['avg_R']
        trades = int(row['trade_count'])
        avg_r_val = float(avg_r)
        
        # Derivative logic assignment
        # Using a more strict requirement for statistical significance and pure edge
        if avg_r_val <= 0 or trades < 4:
            perm = "DISABLED"
        elif avg_r_val <= 0.15:
            perm = "LIMITED"
        else:
            perm = "ACTIVE"
            
        permissions[node] = {
            "status": perm,
            "avg_R": round(avg_r_val, 3),
            "trades": trades
        }
        
    with open('node_permissions.json', 'w') as f:
        json.dump(permissions, f, indent=4)
        
    active = sum(1 for v in permissions.values() if v['status'] == 'ACTIVE')
    limited = sum(1 for v in permissions.values() if v['status'] == 'LIMITED')
    disabled = sum(1 for v in permissions.values() if v['status'] == 'DISABLED')
    
    print(f"Total Nodes Processed: {len(permissions)}")
    print(f"ACTIVE Nodes (Clear Edge): {active}")
    print(f"LIMITED Nodes (Weak Edge): {limited}")
    print(f"DISABLED Nodes (No Edge/Noise): {disabled}")
    print("\n--- ACTIVE NODES ---")
    for k, v in permissions.items():
        if v['status'] == 'ACTIVE': print(f"{k}: {v['avg_R']} R ({v['trades']} trades)")

if __name__ == "__main__":
    derive_permissions()
