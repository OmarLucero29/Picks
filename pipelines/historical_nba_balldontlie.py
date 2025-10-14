# pipelines/historical_nba_balldontlie.py
import requests, pandas as pd
from pathlib import Path
from datetime import datetime

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)
API = "https://www.balldontlie.io/api/v1/games"

def fetch_season(season: int):
    rows=[]; page=1
    while True:
        r=requests.get(API, params={"seasons[]":season, "per_page":100, "page":page}, timeout=30)
        if r.status_code!=200: break
        data=r.json(); games=data.get("data",[])
        if not games: break
        for g in games:
            if g.get("status")!="Final": 
                continue
            date = g.get("date")
            home = g.get("home_team",{}).get("full_name")
            away = g.get("visitor_team",{}).get("full_name")
            hs = g.get("home_team_score",0); as_ = g.get("visitor_team_score",0)
            rows.append(dict(date=date[:10], home=home, away=away, result_home_win=int(hs>as_)))
        page+=1
        if page>40: break
    return rows

def main():
    current=datetime.utcnow().year
    seasons=list(range(current-5, current))
    rows=[]
    for s in seasons:
        rows+=fetch_season(s)
    if not rows:
        print("no nba data"); return
    df=pd.DataFrame(rows)
    df["sport"]="baloncesto"; df["league"]="NBA"
    df.to_csv(OUT/"nba_games.csv", index=False)
    print("historical nba ok:", len(df))
if __name__=='__main__': main()
