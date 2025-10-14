# models/train.py
import argparse, json
from pathlib import Path
import pandas as pd
from datetime import datetime

STORE = Path("models_store"); STORE.mkdir(parents=True, exist_ok=True)
HIST = Path("data/historical")

def load_csv(p): 
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

def train_baselines():
    model = {"trained_at": datetime.utcnow().isoformat()+"Z", "version": "v1-baselines"}
    # NFL
    nfl = load_csv(HIST/"nfl_games.csv")
    if not nfl.empty:
        rate = nfl["result_home_win"].mean()
        model.setdefault("americano", {})["NFL"] = {"home_win_rate": float(rate)}
    # TENIS (por surface)
    ten = load_csv(HIST/"tennis_matches.csv")
    if not ten.empty:
        grp = ten.groupby(ten["surface"].fillna("Unknown"))["result_home_win"].mean().to_dict()
        model["tenis"] = {"by_surface": {k: float(v) for k,v in grp.items()}}
    # FUTBOL (global y por liga si hay volumen)
    soc_i = load_csv(HIST/"soccer_matches_incremental.csv")
    if not soc_i.empty:
        global_rate = soc_i["result_home_win"].mean()
        by_league = soc_i.groupby("league")["result_home_win"].mean()
        model["futbol"] = {
            "global_home_win_rate": float(global_rate),
            "by_league": {k: float(v) for k,v in by_league.items() if by_league.count()[k] >= 50}
        }
    # NBA / MLB / NHL (global home win rates)
    nba = load_csv(HIST/"nba_games.csv")
    if not nba.empty:
        model["baloncesto"] = {"NBA":{"home_win_rate": float(nba["result_home_win"].mean())}}
    mlb = load_csv(HIST/"mlb_games.csv")
    if not mlb.empty:
        model["beisbol"] = {"MLB":{"home_win_rate": float(mlb["result_home_win"].mean())}}
    nhl = load_csv(HIST/"nhl_games.csv")
    if not nhl.empty:
        model["hockey"] = {"NHL":{"home_win_rate": float(nhl["result_home_win"].mean())}}
    return model

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--calibrate", type=str, default="none")
    ap.add_argument("--ensemble", action="store_true")
    args = ap.parse_args()

    model = train_baselines()
    model["meta"] = {"years": args.years, "calibration": args.calibrate, "ensemble": bool(args.ensemble)}
    with open(STORE/"active_model.json","w",encoding="utf-8") as f:
        json.dump(model, f, ensure_ascii=False, indent=2)
    print("train ok – wrote", STORE/"active_model.json")

if __name__=="__main__": main()
