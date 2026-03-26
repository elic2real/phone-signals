import pandas as pd
df = pd.read_csv('true_physics_trades.csv')

print("We are failing mathematically because we take trades at ALL hours.")
print("We know from the initial domain knowledge that London/NY overlap is the pure volatility regime.")

df['entry_time'] = pd.to_datetime(df['entry_time'])
df['hour'] = df['entry_time'].dt.hour

def score_hour(h):
    # Rough approximation of London/NY (08:00 UTC to 16:00 UTC)
    pdf = df[df['hour'] == h]
    if len(pdf) == 0: return
    # Use the proven +0.55 / -0.35 profile which had max expectancy across the board
    wins = 0
    losses = 0
    tgt, cut = 0.55, -0.35
    net_tgt, net_cut = tgt - 0.05, cut - 0.05
    for _, row in pdf.iterrows():
        mfe, mae = row['max_mfe'], row['max_mae']
        hit_tgt = mfe >= tgt
        hit_cut = mae >= abs(cut)
        if hit_tgt and not hit_cut: wins += 1
        elif hit_cut and not hit_tgt: losses += 1
        else: losses += 1 # pessimistic
    
    total = wins + losses
    wr = wins / total if total > 0 else 0
    exp = (wr * net_tgt) + ((1 - wr) * net_cut)
    print(f"Hour {h:02d} UTC: {len(pdf):5d} trades => WR: {wr:.1%}, Net EX: {exp:+.3f} R")

for i in range(24):
    score_hour(i)
