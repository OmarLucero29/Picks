# pipelines/historical_nfl.py
import pandas as pd, requests, io
from pathlib import Path
from datetime import datetime

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)
URL = "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data/games.csv.gz"

def main():
    r = requests.get(URL, timeout=90); r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), compression="gzip")
    df = df[["season","game_type","game_date","home_team","away_team","result"]]
    current = datetime.utcnow().year
    df = df[(df["season"]>=current-5) & (df["game_type"].isin(["REG","POST","SB"]))]
    df["date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["sport"]="americano"; df["league"]="NFL"
    df["result_home_win"] = (df["result"]>0).astype(int)
    df.rename(columns={"home_team":"home","away_team":"away"}, inplace=True)
    out = OUT/"nfl_games.csv"
    df[["sport","league","date","home","away","result_home_win","season","game_type"]].to_csv(out, index=False)
    print("historical nfl ok:", len(df))
if __name__=="__main__": main()
