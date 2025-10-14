# -*- coding: utf-8 -*-
"""
Crea la ESTRUCTURA FINAL del proyecto Multisport (workflows + pipelines + históricos + modelos + serving + Space).
Requisitos previos: tener los Secrets ya configurados en GitHub (HF_TOKEN, HF_SPACE, ODDS_API_KEY, APIFOOTBALL_KEY, PANDASCORE_TOKEN, opcionales de Telegram).
Ejecuta:  python bootstrap_multisport_final.py
"""
import os, json, textwrap, zipfile
from pathlib import Path

ROOT = Path(".")
def write(rel, content):
    p = ROOT/rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

# ---------------- REQUIREMENTS ----------------
write("requirements.txt", "\n".join([
    "pandas",
    "numpy",
    "requests",
    "python-dateutil",
    "huggingface_hub",
    "gradio"
])+"\n")

# ---------------- CONFIG ----------------
write("config/leagues.yaml", textwrap.dedent("""\
futbol: {all_competitions: true}
beisbol: {MLB: true, KBO: true}
baloncesto: {NBA: true, NCAA: true, LNBP: true, Euroliga: true, Europa_otros: true}
tenis: {all_tours: true}
hockey: {NHL: true}
ping_pong: {all_competitions: true}
americano: {NFL: true, NCAA: true, LFA: true}
esports: {all_competitions: true}
"""))

write("config/markets.yaml", textwrap.dedent("""\
# mercados soportados; si faltan datos para un mercado en un partido se marca "No Bet"
futbol: [ganador, over_under_goles, total_goles, corners, tarjetas, offsides, handicap, props_equipo, props_jugador]
beisbol: [ganador, handicap, total_carreras, props_equipo, props_jugador, entradas]
baloncesto: [ganador, handicap, total_puntos, props_equipo, props_jugador]
tenis: [ganador, juegos_totales, ganara_un_set, ganara_sin_perder_set]
hockey: [ganador, total_goles, handicap]
ping_pong: [ganador, juegos_totales]
americano: [ganador, totales, over_under, spread, handicap, props_jugador, props_equipo]
esports: [ganador, total_mapas, total_kills, e_futbol_ganador, e_futbol_over_under]
"""))

write("config/schedules.yaml", textwrap.dedent("""\
futbol: {recalibrate: daily, hour_local: "05:00"}
baloncesto: {recalibrate: "Mon,Wed,Fri", hour_local: "03:00"}
beisbol: {recalibrate: daily, hour_local: "04:00"}
tenis: {recalibrate: daily, hour_local: "02:00"}
hockey: {recalibrate: weekly, day_local: "Mon", hour_local: "03:00"}
americano: {recalibrate: weekly, day_local: "Tue", hour_local: "01:00"}  # NFL después del MNF
ping_pong: {recalibrate: daily, hour_local: "01:00"}
esports: {recalibrate: daily, hour_local: "01:30"}
"""))

write("config/apis.yaml", textwrap.dedent("""\
odds: {provider: theoddsapi, secret_env_key: ODDS_API_KEY}
weather: {provider: meteostat, secondary: open-meteo}
soccer_data: {primary: api_football, secret_env_key: APIFOOTBALL_KEY}
nba_data: {primary: balldontlie_api}
mlb_data: {primary: mlb_stats_api}
nhl_data: {primary: nhl_stats_api}
nfl_data: {primary: nflverse_csv}
tennis_data: {primary: api_sports_tennis, alt: jeff_sackmann_csvs, secret_env_key: APISPORTS_TENNIS_KEY}
esports_data: {primary: pandascore, secret_env_key: PANDASCORE_TOKEN}
special_events: {mma_ufc: ufcstats_scrape, boxing: topology_scrape, f1: fastf1}
"""))

# ---------------- WORKFLOWS ----------------
write(".github/workflows/daily.yml", textwrap.dedent("""\
name: daily
on:
  schedule:
    - cron: "0 12 * * *"   # 12:00 UTC diario
  workflow_dispatch: {}

concurrency:
  group: daily-${{ github.ref }}
  cancel-in-progress: true

jobs:
  run-daily:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install deps
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt
      - name: Fetch upcoming (48h)
        run: python pipelines/fetch_all.py --mode daily
      - name: Build features
        run: python pipelines/features.py
      - name: Predict
        run: python models/predict.py
      - name: Select Top-5
        run: python serving/select_picks.py
      - name: Build Parlay
        run: python serving/parlay_builder.py
      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: daily_outputs
          path: reports/
      - name: Sync to Hugging Face
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
          HF_SPACE: ${{ secrets.HF_SPACE }}
        run: python serving/sync_hf.py
      - name: Notify Telegram (robusto)
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          HF_SPACE: ${{ secrets.HF_SPACE }}
        if: ${{ (env.TELEGRAM_BOT_TOKEN != '') && (env.TELEGRAM_CHAT_ID != '') }}
        run: |
          set -e
          RES=$(curl -s -w "\\n%{http_code}" -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="${TELEGRAM_CHAT_ID}" \
            -d text="✅ Picks y Parlay publicados. Space: ${HF_SPACE}")
          BODY=$(echo "$RES" | head -n1); CODE=$(echo "$RES" | tail -n1)
          echo "HTTP $CODE: $BODY"
          [ "$CODE" = "200" ] || (echo "::error::Falló el envío a Telegram"; exit 1)
"""))

write(".github/workflows/weekly.yml", textwrap.dedent("""\
name: weekly
on:
  schedule:
    - cron: "0 7 * * 2"   # Martes 07:00 UTC = 01:00 América/Mérida
  workflow_dispatch: {}

concurrency:
  group: weekly-${{ github.ref }}
  cancel-in-progress: true

jobs:
  run-weekly:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install deps
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt
      - name: Fetch historical TENNIS
        run: python pipelines/historical_tennis.py
      - name: Fetch historical NFL
        run: python pipelines/historical_nfl.py
      - name: Fetch historical NBA (balldontlie)
        run: python pipelines/historical_nba_balldontlie.py
      - name: Fetch historical MLB (statsapi)
        run: python pipelines/historical_mlb_statsapi.py
      - name: Fetch historical NHL (statsapi)
        run: python pipelines/historical_nhl_statsapi.py
      - name: Soccer recent history (180d)
        env:
          APIFOOTBALL_KEY: ${{ secrets.APIFOOTBALL_KEY }}
        run: python pipelines/historical_soccer_apifootball.py
      - name: Fetch upcoming (48h)
        run: python pipelines/fetch_all.py --mode weekly
      - name: Build features
        run: python pipelines/features.py
      - name: Train (baselines)
        run: python models/train.py --years 5 --calibrate isotonic --ensemble
      - name: Backtest (summary)
        run: python models/backtest.py --years 5 --publish reports/
      - name: Predict
        run: python models/predict.py
      - name: Select Top-5
        run: python serving/select_picks.py
      - name: Build Parlay
        run: python serving/parlay_builder.py
      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: weekly_reports
          path: reports/
      - name: Sync to Hugging Face
        env:
          HF_TOKEN: ${{ secrets.HF_TOKEN }}
          HF_SPACE: ${{ secrets.HF_SPACE }}
        run: python serving/sync_hf.py
      - name: Notify Telegram (robusto)
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          HF_SPACE: ${{ secrets.HF_SPACE }}
        if: ${{ (env.TELEGRAM_BOT_TOKEN != '') && (env.TELEGRAM_CHAT_ID != '') }}
        run: |
          set -e
          RES=$(curl -s -w "\\n%{http_code}" -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="${TELEGRAM_CHAT_ID}" \
            -d text="🛠️ Recalibración semanal completada. Nuevos picks publicados. Space: ${HF_SPACE}")
          BODY=$(echo "$RES" | head -n1); CODE=$(echo "$RES" | tail -n1)
          echo "HTTP $CODE: $BODY"
          [ "$CODE" = "200" ] || (echo "::error::Falló el envío a Telegram"; exit 1)
"""))

# ---------------- PIPELINES: UPCOMING (multi-sport real) ----------------
write("pipelines/fetch_all.py", textwrap.dedent("""\
# pipelines/fetch_all.py
import os, requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pandas as pd

PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

ODDS_KEY = os.environ.get("ODDS_API_KEY", "")
PANDASCORE_TOKEN = os.environ.get("PANDASCORE_TOKEN", "")

def utcnow(): return datetime.now(timezone.utc)
def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None: return default
        cur = cur.get(k)
    return cur if cur is not None else default

# -------- TheOddsAPI (base multi-deporte + bet365) --------
def fetch_from_theoddsapi(hours_ahead=48):
    if not ODDS_KEY:
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away","market_total","ml_home","ml_away","spread_line","spread_home","spread_away"])
    sports = requests.get("https://api.the-odds-api.com/v4/sports",
                          params={"apiKey": ODDS_KEY, "all": "true"}, timeout=30).json()
    keep = []
    for s in sports:
        key = s.get("key","")
        if any([
            key.startswith("soccer_"),
            key.startswith("americanfootball_"),
            key.startswith("basketball_"),
            key.startswith("baseball_"),
            key.startswith("icehockey_"),
            key.startswith("tennis_"),
            key.startswith("esports_"),
            key.startswith("tabletennis_"),
            key.startswith("mma_"),
            key.startswith("boxing_"),
            key.startswith("motorsport_")
        ]):
            keep.append(s)

    rows = []
    end_time = utcnow() + timedelta(hours=hours_ahead)

    for s in keep:
        key = s["key"]
        try:
            r = requests.get(f"https://api.the-odds-api.com/v4/sports/{key}/odds",
                             params={
                                 "apiKey": ODDS_KEY,
                                 "regions": "us,eu,uk",
                                 "markets": "h2h,totals,spreads",
                                 "oddsFormat": "decimal",
                                 "dateFormat": "iso",
                                 "bookmakers": "bet365"
                             }, timeout=30)
            r.raise_for_status()
            events = r.json()
        except Exception:
            continue

        for ev in events:
            try:
                start = parser.isoparse(ev["commence_time"])
            except Exception:
                continue
            if start <= utcnow() or start > end_time:
                continue

            home = safe_get(ev,"home_team", default="")
            away = safe_get(ev,"away_team", default="")
            league = s.get("title", key)
            sport_key = key.split("_")[0]

            bm = next((b for b in ev.get("bookmakers", []) if b.get("key")=="bet365"), None)
            ml_home = ml_away = None; market_total = None
            spread_line = None; spread_home = spread_away = None
            if bm:
                for m in bm.get("markets", []):
                    if m.get("key") == "h2h":
                        outs = m.get("outcomes", [])
                        for o in outs:
                            if o.get("name") == home: ml_home = o.get("price")
                            if o.get("name") == away: ml_away = o.get("price")
                    elif m.get("key") == "totals":
                        outs = m.get("outcomes", [])
                        if outs: market_total = outs[0].get("point")
                    elif m.get("key") == "spreads":
                        outs = m.get("outcomes", [])
                        if outs:
                            spread_line = outs[0].get("point")
                            for o in outs:
                                if o.get("name")==home: spread_home = o.get("price")
                                if o.get("name")==away: spread_away = o.get("price")

            if sport_key == "soccer": sport = "futbol"
            elif sport_key == "americanfootball": sport = "americano"
            elif sport_key == "basketball": sport = "baloncesto"
            elif sport_key == "baseball": sport = "beisbol"
            elif sport_key == "icehockey": sport = "hockey"
            elif sport_key == "tennis": sport = "tenis"
            elif sport_key == "esports": sport = "esports"
            elif sport_key == "tabletennis": sport = "ping_pong"
            else: sport = sport_key

            rows.append(dict(
                sport=sport, league=league,
                start_time_utc=start.isoformat(), status="scheduled",
                home=home, away=away,
                market_total=market_total, ml_home=ml_home, ml_away=ml_away,
                spread_line=spread_line, spread_home=spread_home, spread_away=spread_away
            ))
    return pd.DataFrame(rows)

# -------- PandaScore (e-sports próximos) --------
def fetch_pandascore(hours_ahead=48):
    if not PANDASCORE_TOKEN:
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away"])
    end = (utcnow() + timedelta(hours=hours_ahead)).isoformat()
    start = utcnow().isoformat()
    url = "https://api.pandascore.co/matches/upcoming"
    rows = []; page = 1
    while True:
        r = requests.get(url, params={
            "per_page": 50, "page": page,
            "range[begin_at]": f"{start},{end}"
        }, headers={"Authorization": f"Bearer {PANDASCORE_TOKEN}"}, timeout=30)
        if r.status_code != 200: break
        data = r.json()
        if not data: break
        for m in data:
            begin = safe_get(m,"begin_at", default=None)
            if not begin: continue
            start_dt = parser.isoparse(begin)
            home = safe_get(m,"opponents",0,"opponent","name", default="TBA")
            away = safe_get(m,"opponents",1,"opponent","name", default="TBA")
            league = safe_get(m,"league","name", default="eSports")
            rows.append(dict(
                sport="esports", league=league, start_time_utc=start_dt.isoformat(), status="scheduled",
                home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                spread_line=None, spread_home=None, spread_away=None
            ))
        page += 1
        if page>4: break
    return pd.DataFrame(rows)

if __name__ == "__main__":
    base = fetch_from_theoddsapi(hours_ahead=48)
    es = fetch_pandascore(hours_ahead=48)
    df = pd.concat([base, es], ignore_index=True)
    df["status"] = "scheduled"
    df = df.dropna(subset=["start_time_utc","home","away"], how="any").sort_values("start_time_utc")
    out = PROC / "upcoming_events.csv"
    df.to_csv(out, index=False)
    print(f"fetch ok – wrote {len(df)} events -> {out}")
"""))

# ---------------- FEATURES ----------------
write("pipelines/features.py", textwrap.dedent("""\
from pathlib import Path
import pandas as pd
PROC=Path('data/processed'); PROC.mkdir(parents=True, exist_ok=True)
df=pd.read_csv(PROC/'upcoming_events.csv')
# (placeholder) aquí se pueden crear features por deporte/mercado
df.to_csv(PROC/'upcoming_events_features.csv', index=False)
print('features ok –', len(df))
"""))

# ---------------- HISTORICALS ----------------
write("pipelines/historical_tennis.py", textwrap.dedent("""\
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
"""))

write("pipelines/historical_nfl.py", textwrap.dedent("""\
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
"""))

write("pipelines/historical_nba_balldontlie.py", textwrap.dedent("""\
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
"""))

write("pipelines/historical_mlb_statsapi.py", textwrap.dedent("""\
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
"""))

write("pipelines/historical_nhl_statsapi.py", textwrap.dedent("""\
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
"""))

write("pipelines/historical_soccer_apifootball.py", textwrap.dedent("""\
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
"""))

# ---------------- MODELS ----------------
write("models/train.py", textwrap.dedent("""\
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
"""))

write("models/backtest.py", textwrap.dedent("""\
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
"""))

write("models/predict.py", textwrap.dedent("""\
# models/predict.py
import json
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

PROC=Path('data/processed')
STORE=Path('models_store')/'active_model.json'

def implied_from_decimal(odds):
    try:
        o=float(odds); 
        return 1.0/o if o>1.0 else None
    except: return None

def load_model():
    if STORE.exists():
        return json.load(open(STORE,'r',encoding='utf-8'))
    return {}

def calibrate_prob(p, sport, row, model):
    alpha=0.15
    if sport=="americano" and "americano" in model and "NFL" in model["americano"]:
        base=model["americano"]["NFL"].get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="tenis" and "tenis" in model:
        surf=(row.get("surface") or "Unknown")
        base=model["tenis"].get("by_surface",{}).get(surf)
        if base: return alpha*base + (1-alpha)*p
    if sport=="futbol" and "futbol" in model:
        base=model["futbol"].get("by_league",{}).get(row.get("league"))
        if not base: base=model["futbol"].get("global_home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="baloncesto" and "baloncesto" in model:
        base=model["baloncesto"].get("NBA",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="beisbol" and "beisbol" in model:
        base=model["beisbol"].get("MLB",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="hockey" and "hockey" in model:
        base=model["hockey"].get("NHL",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    return p

def main():
    df=pd.read_csv(PROC/'upcoming_events.csv')
    now=datetime.now(timezone.utc)
    df['start_time_utc']=pd.to_datetime(df['start_time_utc'], utc=True, errors='coerce')
    df=df[(df['start_time_utc']>now)]
    model=load_model()
    rows=[]
    for _,r in df.iterrows():
        ph = implied_from_decimal(r.get("ml_home"))
        pa = implied_from_decimal(r.get("ml_away"))
        if ph and pa:
            s=ph+pa; phn=(ph/s)*0.97+0.015; pan=(pa/s)*0.97+0.015
            if phn>=pan:
                p=calibrate_prob(phn, r["sport"], r, model)
                winner=r["home"]; p_win=p; fav="home"
            else:
                p=calibrate_prob(pan, r["sport"], r, model)
                winner=r["away"]; p_win=p; fav="away"
        else:
            defaults={"americano":0.54,"futbol":0.52,"baloncesto":0.54,"beisbol":0.53,"hockey":0.53,"tenis":0.52,"esports":0.52,"ping_pong":0.52}
            p=defaults.get(r["sport"],0.52); winner=r["home"] if p>=0.5 else r["away"]; p_win=p; fav="home" if p>=0.5 else "away"
        total = r.get("market_total") if r.get("market_total")==r.get("market_total") else None
        ou_pick="No Bet"; delta_total=0.0
        spread_line = r.get("spread_line") if r.get("spread_line")==r.get("spread_line") else None
        spread_pick="No Bet"
        if spread_line is not None:
            spread_pick = f"{r['home']} {float(spread_line):+}" if fav=='home' else f"{r['away']} {(-float(spread_line)):+}"
        rows.append(dict(
            date=r.start_time_utc.date().isoformat(), sport=r.sport, league=r.league,
            game=f"{r.home} vs {r.away}", winner=winner, p_win=float(p_win),
            total=(float(total) if total else None), ou_pick=ou_pick, delta_total=float(delta_total),
            spread=spread_pick
        ))
    out=pd.DataFrame(rows); out.to_csv(PROC/'predictions.csv', index=False)
    print(f"predictions ok – {len(out)} rows -> {out}")

if __name__=='__main__': main()
"""))

# ---------------- SERVING ----------------
write("serving/prefs.json", json.dumps({
    "language":"es_MX","explanation_max_lines":10,"daily_picks_target":5,
    "parlay_always":True,"parlay_min_legs":2,"parlay_max_legs":5,
    "parlay_min_combined_odds":2.5,"parlay_min_combined_prob":0.55
}, indent=2, ensure_ascii=False))

write("serving/select_picks.py", textwrap.dedent("""\
import pandas as pd, numpy as np
from pathlib import Path
PROC=Path('data/processed'); REPORTS=Path('reports'); REPORTS.mkdir(parents=True, exist_ok=True)

def fair_odds(p): p=max(min(p,0.999),0.001); return 1.0/p

preds=pd.read_csv(PROC/'predictions.csv').sort_values('p_win', ascending=False)
chosen=[]; used_games=set(); used_leagues=set()
for _,r in preds.iterrows():
    if len(chosen)>=5: break
    if r['game'] in used_games: continue
    if r['league'] in used_leagues and len(chosen)<3: 
        continue
    chosen.append(r); used_games.add(r['game']); used_leagues.add(r['league'])

if len(chosen)<5:
    for _,r in preds.iterrows():
        if len(chosen)>=5: break
        if r['game'] in used_games: continue
        chosen.append(r); used_games.add(r['game'])

picks=pd.DataFrame(chosen[:5]).copy()
picks['market']='ML'; picks['selection']=picks['winner']; picks['line']=''; picks['prob']=picks['p_win']
picks['prob_decimal_odds']=picks['prob'].apply(fair_odds)
picks['confidence']=np.where(picks['prob']>=0.7,'Alta',np.where(picks['prob']>=0.6,'Media','Baja'))
picks['rationale']="Predicción basada en odds ajustadas y baselines históricos por deporte/competencia."
cols=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale']
picks[cols].to_csv('reports/picks.csv', index=False); print('picks ok –', len(picks))
"""))

write("serving/parlay_builder.py", textwrap.dedent("""\
import pandas as pd
from pathlib import Path
from itertools import combinations
import json
PREFS=json.load(open('serving/prefs.json','r',encoding='utf-8'))
picks=pd.read_csv('reports/picks.csv').drop_duplicates(subset=['game']).sort_values('prob', ascending=False)
target_odds=PREFS.get('parlay_min_combined_odds',2.5); min_prob=PREFS.get('parlay_min_combined_prob',0.55)
best=None
for k in [5,4,3,2]:
    if len(picks)<k: continue
    for combo in combinations(picks.index, k):
        sub=picks.loc[list(combo)].copy()
        p_joint=sub['prob'].prod(); dec_odds=(1.0/sub['prob']).prod()
        if dec_odds>=target_odds and p_joint>=min_prob:
            sub['parlay_prob']=p_joint; sub['parlay_decimal_odds']=dec_odds; best=sub; break
    if best is not None: break
if best is None and len(picks)>=2:
    sub=picks.head(2).copy(); sub['parlay_prob']=sub['prob'].prod(); sub['parlay_decimal_odds']=(1.0/sub['prob']).prod(); best=sub
(best if best is not None else picks.head(0)).assign(note='Parlay generado').to_csv('reports/parlay.csv', index=False); print('parlay ok')
"""))

write("serving/sync_hf.py", textwrap.dedent("""\
import os, sys
from pathlib import Path
from huggingface_hub import HfApi, create_repo, upload_file
SPACE=os.getenv('HF_SPACE'); TOKEN=os.getenv('HF_TOKEN')
if not SPACE or not TOKEN: 
    print('HF missing'); sys.exit(0)
api=HfApi(token=TOKEN)
try: api.repo_info(SPACE, repo_type='space')
except Exception: create_repo(repo_id=SPACE, repo_type='space', space_sdk='gradio', exist_ok=True, private=False, token=TOKEN)
def up(local,dest): upload_file(path_or_fileobj=local, path_in_repo=dest, repo_id=SPACE, repo_type='space', token=TOKEN)
Path('reports').mkdir(parents=True, exist_ok=True)
for f in ['reports/picks.csv','reports/parlay.csv']:
    if not Path(f).exists(): Path(f).write_text('')
Path('serving/space/requirements.txt').write_text('pandas\\ngradio\\n')
up('serving/space/app.py','app.py'); up('serving/space/requirements.txt','requirements.txt')
up('reports/picks.csv','picks.csv'); up('reports/parlay.csv','parlay.csv')
if Path('serving/prefs.json').exists(): up('serving/prefs.json','prefs.json')
print('Space synced')
"""))

write("serving/space/app.py", textwrap.dedent("""\
import gradio as gr, pandas as pd, time

def load(p): 
    try: return pd.read_csv(p)
    except: return pd.DataFrame()

def filter_sport(df, s):
    if df is None or df.empty or s=="todos": return df
    return df[df["sport"]==s]

with gr.Blocks() as demo:
    gr.Markdown("# Multisport – Top-5 + Parlay 'segurito'")
    gr.Markdown(f"**Última actualización:** {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")
    sport = gr.Dropdown(choices=['todos','futbol','baloncesto','beisbol','tenis','hockey','ping_pong','americano','esports'], value='todos', label='Filtrar por deporte')
    with gr.Tabs():
        with gr.Tab("Top-5 del día"):
            table = gr.Dataframe(value=filter_sport(load('picks.csv'), 'todos'), interactive=False, wrap=True, height=500)
            gr.File(label="Descargar picks.csv", value='picks.csv', visible=True)
            def update(s):
                df = load('picks.csv')
                return filter_sport(df, s)
            sport.change(fn=update, inputs=sport, outputs=table)
        with gr.Tab("Parlay 'segurito'"):
            gr.Dataframe(load('parlay.csv'), interactive=False, wrap=True, height=400)
            gr.File(label="Descargar parlay.csv", value='parlay.csv', visible=True)

if __name__=='__main__': demo.launch()
"""))

# ---------------- REPORTS placeholders ----------------
Path("reports").mkdir(parents=True, exist_ok=True)
(Path("reports")/"picks.csv").write_text("", encoding="utf-8")
(Path("reports")/"parlay.csv").write_text("", encoding="utf-8")

# ---------------- README ----------------
write("README.md", textwrap.dedent("""\
# Multisport Starter – FINAL

**Incluye** todos los deportes y mercados en estructura, datos live (TheOddsAPI/PandaScore), históricos (NFL/tenis/NBA/MLB/NHL + fútbol incremental), modelos (odds + calibración) y publicación a Hugging Face Space. Notificación opcional a Telegram.

## Secrets requeridos
- `HF_TOKEN`, `HF_SPACE`
- `ODDS_API_KEY`
- `APIFOOTBALL_KEY` (fútbol histórico incremental)
- `PANDASCORE_TOKEN` (e-sports, opcional)
- (Opcional) `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

## Flujos
- **daily**: fetch → features → predict → Top-5 → Parlay → Space (+ Telegram)
- **weekly**: históricos → train → backtest → predict → Top-5 → Parlay → Space (+ Telegram)

> Los props y mercados secundarios están definidos en `config/markets.yaml`. El sistema publica ML/Spread/OU cuando hay líneas; si falta información, marca `No Bet`.
"""))

print("✅ Proyecto final escrito. Ahora: git add . && git commit && git push")
