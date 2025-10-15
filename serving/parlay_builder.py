# serving/parlay_builder.py — parlay con SOLO prob del modelo; sin odds
from pathlib import Path
import pandas as pd
import numpy as np

REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

MAX_LEGS = 5          # hasta 5
TARGET_DEC_ODDS = 2.5 # cuota objetivo (decimal) del parlay
MARGIN = 0.95         # conservadurismo (reduce la cuota justa)

def load_picks():
    f = REPORTS/"picks.csv"
    if not f.exists() or f.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(f)

def parlay_metrics(probs):
    p_parlay = float(np.prod(probs))
    dec_fair  = 1.0 / max(p_parlay, 1e-6)
    dec_cons  = dec_fair * MARGIN
    return p_parlay, dec_cons

def main():
    picks = load_picks()
    if picks.empty:
        print("parlay ok – 0 (sin picks)")
        (REPORTS/"parlay.csv").write_text("")
        return

    # Greedy: añade de mayor a menor prob hasta alcanzar cuota objetivo o 5 legs
    pool = picks.sort_values("prob", ascending=False).copy()
    legs = []
    probs = []

    for _, row in pool.iterrows():
        if len(legs) >= MAX_LEGS:
            break
        legs.append(row)
        probs.append(float(row["prob"]))
        _, dec = parlay_metrics(probs)
        if dec >= TARGET_DEC_ODDS and len(legs) >= 2:
            break

    legs = pd.DataFrame(legs)
    p, dec = parlay_metrics(legs["prob"].astype(float).values)
    legs["parlay_prob"] = round(p, 4)
    legs["parlay_decimal_odds"] = round(dec, 3)
    legs["note"] = "Parlay segurito (modelo puro, sin odds)."

    out = REPORTS/"parlay.csv"
    legs.to_csv(out, index=False)
    print(f"parlay ok – {len(legs)} -> {out}")

if __name__ == "__main__":
    main()