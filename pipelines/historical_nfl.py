# pipelines/historical_nfl.py
import io, requests, pandas as pd
from pathlib import Path
from datetime import datetime

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)

CANDIDATES = [
    # rama main
    "https://raw.githubusercontent.com/nflverse/nflfastR-data/main/data/games.csv.gz",
    "https://github.com/nflverse/nflfastR-data/raw/main/data/games.csv.gz",
    # rama master (repos antiguos/espelhos)
    "https://raw.githubusercontent.com/nflverse/nflfastR-data/master/data/games.csv.gz",
    "https://github.com/nflverse/nflfastR-data/raw/master/data/games.csv.gz",
]

HEADERS = {"User-Agent": "multisport-starter/1.0"}

def fetch_games_bytes():
    last_err = None
    for url in CANDIDATES:
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code == 200 and r.content:
                return r.content
            else:
                last_err = f"HTTP {r.status_code} for {url}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e} @ {url}"
    print(f"WARNING: no se pudo descargar games.csv.gz – {last_err}")
    return None

def main():
    content = fetch_games_bytes()
    if content is None:
        # no fallar el workflow completo; sólo avisar
        print("historical nfl skipped (no data)")
        return

    df = pd.read_csv(io.BytesIO(content), compression="gzip")
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
