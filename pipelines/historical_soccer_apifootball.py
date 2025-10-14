# pipelines/historical_soccer_apifootball.py
import os, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import parser

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)
API_BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": os.environ["APIFOOTBALL_KEY"]}

def day_range(days=180):
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)
    cur = start
    while cur <= end:
        yield cur.isoformat()
        cur += timedelta(days=1)

def main():
    rows=[]
    for d in day_range(180):
        try:
            r = requests.get(f"{API_BASE}/fixtures", params={"date": d, "timezone":"UTC"}, headers=HEADERS, timeout=30)
            if r.status_code!=200: continue
            for fx in r.json().get("response", []):
                st = (fx["fixture"]["status"]["short"] or "").upper()
                if st not in ("FT","AET","PEN"):  # solo finalizados
                    continue
                home = fx["teams"]["home"]["name"]; away = fx["teams"]["away"]["name"]
                gh = fx["goals"]["home"] or 0; ga = fx["goals"]["away"] or 0
                date = parser.isoparse(fx["fixture"]["date"]).date().isoformat()
                league = fx["league"]["name"]
                rows.append(dict(sport="futbol", league=league, date=date, home=home, away=away, result_home_win=int(gh>ga)))
        except Exception:
            continue
    if not rows:
        print("no soccer data"); return
    df = pd.DataFrame(rows)
    out = OUT/"soccer_matches_incremental.csv"
    if out.exists():
        cur = pd.read_csv(out)
        df = pd.concat([cur, df], ignore_index=True).drop_duplicates(subset=["date","home","away"])
    df.to_csv(out, index=False)
    print("historical soccer recent ok:", len(df))
if __name__=="__main__": main()
