# models/backtest.py
import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime

OUT = Path("reports"); OUT.mkdir(parents=True, exist_ok=True)
HIST = Path("data/historical")

def acc(df): 
    return float(df["result_home_win"].mean()) if not df.empty else 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--publish", type=str, default="reports/")
    args = ap.parse_args()

    rows=[]
    for nm,fn in [("americano_NFL","nfl_games.csv"),("tenis","tennis_matches.csv"),("futbol","soccer_matches_incremental.csv"),("NBA","nba_games.csv"),("MLB","mlb_games.csv"),("NHL","nhl_games.csv")]:
        p=HIST/fn
        if p.exists():
            d=pd.read_csv(p)
            rows.append((f"{nm}_homeWinRate", acc(d)))
    out = Path(args.publish)/"backtest_summary.csv"
    pd.DataFrame(rows, columns=["metric","value"]).to_csv(out, index=False)
    print("backtest ok – wrote", out)

if __name__=="__main__": main()
