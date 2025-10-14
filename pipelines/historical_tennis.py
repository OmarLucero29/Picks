# pipelines/historical_tennis.py
import io, requests, pandas as pd
from pathlib import Path
from datetime import datetime

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)

def fetch_csv(url):
    r = requests.get(url, timeout=60); r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def main():
    years = list(range(datetime.utcnow().year-4, datetime.utcnow().year+1))
    frames=[]
    for tour in ["atp", "wta"]:
        for y in years:
            url=f"https://raw.githubusercontent.com/JeffSackmann/tennis_{tour}/master/{tour}_matches_{y}.csv"
            try:
                df=fetch_csv(url)
                df["tour"]=tour
                frames.append(df[["tourney_date","surface","winner_name","loser_name","best_of","tourney_name","score","tour"]])
            except Exception:
                continue
    if not frames:
        print("no tennis data"); return
    allm=pd.concat(frames, ignore_index=True)
    allm["date"]=pd.to_datetime(allm["tourney_date"], format="%Y%m%d", errors="coerce")
    allm=allm.dropna(subset=["date"])
    allm.rename(columns={"winner_name":"home","loser_name":"away"}, inplace=True)
    allm["result_home_win"]=1
    allm["sport"]="tenis"; allm["league"]=allm["tourney_name"]
    allm[["sport","league","date","surface","home","away","result_home_win","best_of"]].to_csv(OUT/"tennis_matches.csv", index=False)
    print("historical tennis ok:", len(allm))
if __name__=="__main__": main()
