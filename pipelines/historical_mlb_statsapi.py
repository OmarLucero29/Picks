# pipelines/historical_mlb_statsapi.py
import requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)

def fetch_range(start_date, end_date):
    url=f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}&gameType=R,F,D,L,W"
    r=requests.get(url, timeout=30)
    if r.status_code!=200: return []
    data=r.json()
    rows=[]
    for day in data.get("dates",[]):
        for g in day.get("games",[]):
            if g.get("status",{}).get("abstractGameState")!="Final": continue
            home=g.get("teams",{}).get("home",{}).get("team",{}).get("name")
            away=g.get("teams",{}).get("away",{}).get("team",{}).get("name")
            hs=g.get("teams",{}).get("home",{}).get("score",0)
            as_=g.get("teams",{}).get("away",{}).get("score",0)
            rows.append(dict(date=day.get("date"), home=home, away=away, result_home_win=int(hs>as_)))
    return rows

def main():
    current=datetime.utcnow().date()
    start=(current.replace(month=1, day=1) - timedelta(days=365*5))
    rows=[]
    cursor=start
    while cursor<=current:
        rng_end=min(cursor+timedelta(days=29), current)
        rows+=fetch_range(cursor.isoformat(), rng_end.isoformat())
        cursor=rng_end+timedelta(days=1)
    if not rows:
        print("no mlb data"); return
    df=pd.DataFrame(rows)
    df["sport"]="beisbol"; df["league"]="MLB"
    df.to_csv(OUT/"mlb_games.csv", index=False)
    print("historical mlb ok:", len(df))
if __name__=='__main__': main()
