import pandas as pd

df = pd.read_csv('true_physics_trades.csv')

print("What if we gave Harvester more breathing room for MAE?")
# Notice that at [-0.35 Cut], we have 0.05 net expectancy max at 0.55 Gross Target.
def simulate_new_bounds(df, tgt, cut, spread=0.05):
    net_tgt = tgt - spread
    net_cut = cut - spread
    wins = 0
    losses = 0
    import random
    random.seed(42) # determinism
    for idx, row in df.iterrows():
        mfe = row['max_mfe']
        mae = row['max_mae']
        hit_tgt = mfe >= tgt
        hit_cut = mae >= abs(cut)
        if hit_tgt and not hit_cut: wins += 1
        elif hit_cut and not hit_tgt: losses += 1
        elif hit_tgt and hit_cut:
            if random.random() > 0.5: wins += 1
            else: losses += 1
        else: losses += 1

    total = wins + losses
    wr = wins / total if total > 0 else 0
    exp = (wr * net_tgt) + ((1 - wr) * net_cut)
    print(f"[{tgt} / {cut}] WR: {wr:.2%}, Net Exp: {exp:.3f} R, Gross Expectancy: {(wr*tgt)+((1-wr)*cut):.3f} R")

print("Cut: -0.50")
simulate_new_bounds(df, 0.45, -0.50)
simulate_new_bounds(df, 0.65, -0.50)
simulate_new_bounds(df, 0.85, -0.50)
simulate_new_bounds(df, 1.05, -0.50)

print("\Cut: -0.75")
simulate_new_bounds(df, 0.45, -0.75)
simulate_new_bounds(df, 0.65, -0.75)
simulate_new_bounds(df, 0.85, -0.75)
simulate_new_bounds(df, 1.05, -0.75)
