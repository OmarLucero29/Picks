# pipelines/historical_nhl_statsapi.py
import requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)

def fetch_range(start_date, end_date):
    url=f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start_date}&endDate={end_date}"
    r=requests.get(url, timeout=30)
    if r.status_code!=200: return []
    data=r.json()
    rows=[]
    for day in data.get("dates",[]):
        for g in day.get("games",[]):
            if g.get("status",{}).get("statusCode")!="7": continue  # 7 = Final
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
        print("no nhl data"); return
    df=pd.DataFrame(rows)
    df["sport"]="hockey"; df["league"]="NHL"
    df.to_csv(OUT/"nhl_games.csv", index=False)
    print("historical nhl ok:", len(df))
if __name__=='__main__': main()
