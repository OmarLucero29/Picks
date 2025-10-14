# pipelines/fetch_all.py
import os, requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pandas as pd

PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

ODDS_KEY = os.environ.get("ODDS_API_KEY", "")
APIFOOTBALL_KEY = os.environ.get("APIFOOTBALL_KEY", "")
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
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away","market_total","ml_home","ml_away"])
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
            key.startswith("tabletennis_"),   # por si está disponible
            key.startswith("mma_"),           # eventos especiales si aparecen
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
                                 "markets": "h2h,totals",
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

            # odds bet365
            bm = next((b for b in ev.get("bookmakers", []) if b.get("key")=="bet365"), None)
            ml_home = ml_away = None; market_total = None
            if bm:
                for m in bm.get("markets", []):
                    if m.get("key") == "h2h":
                        for o in m.get("outcomes", []):
                            if o.get("name") == home: ml_home = o.get("price")
                            if o.get("name") == away: ml_away = o.get("price")
                    elif m.get("key") == "totals":
                        outs = m.get("outcomes", [])
                        if outs: market_total = outs[0].get("point")

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
            ))
    return pd.DataFrame(rows)

# -------- Enriquecimiento fútbol (últimos 5) con API-FOOTBALL --------
def enrich_soccer_last5(df: pd.DataFrame) -> pd.DataFrame:
    if not APIFOOTBALL_KEY: 
        return df
    API_BASE = "https://v3.football.api-sports.io"
    HEADERS = {"x-apisports-key": APIFOOTBALL_KEY}

    teams = set(df.loc[df["sport"]=="futbol","home"]).union(set(df.loc[df["sport"]=="futbol","away"]))
    stats = {}
    for name in teams:
        try:
            sr = requests.get(f"{API_BASE}/teams", params={"search": name}, headers=HEADERS, timeout=20).json()
            tid = safe_get(sr,"response",0,"team","id", default=None)
            if not tid: continue
            fr = requests.get(f"{API_BASE}/fixtures", params={"team": tid, "last": 5}, headers=HEADERS, timeout=30).json()
            resp = fr.get("response", [])
            gf = ga = 0.0; totals=[]
            for g in resp:
                gh = safe_get(g,"goals","home", default=0) or 0
                ga_ = safe_get(g,"goals","away", default=0) or 0
                hid = safe_get(g,"teams","home","id", default=None)
                if hid == tid: gf += gh; ga += ga_
                else: gf += ga_; ga += gh
                totals.append(gh+ga_)
            n = max(1,len(resp))
            stats[name] = dict(ppg_for=gf/n, ppg_against=ga/n, tot5=(sum(totals)/n if totals else 2.5))
        except Exception:
            continue

    def map_stat(team, key, fallback):
        d = stats.get(team); 
        return d.get(key, fallback) if d else fallback

    if "home_ppg_for" not in df.columns:
        df["home_ppg_for"] = df.apply(lambda r: map_stat(r["home"],"ppg_for",1.5), axis=1)
        df["home_ppg_against"] = df.apply(lambda r: map_stat(r["home"],"ppg_against",1.0), axis=1)
        df["home_recent_totals_5"] = df.apply(lambda r: map_stat(r["home"],"tot5",2.5), axis=1)
        df["away_ppg_for"] = df.apply(lambda r: map_stat(r["away"],"ppg_for",1.3), axis=1)
        df["away_ppg_against"] = df.apply(lambda r: map_stat(r["away"],"ppg_against",1.2), axis=1)
        df["away_recent_totals_5"] = df.apply(lambda r: map_stat(r["away"],"tot5",2.5), axis=1)
    return df

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
                home=home, away=away, market_total=None, ml_home=None, ml_away=None
            ))
        page += 1
        if page>4: break
    return pd.DataFrame(rows)

if __name__ == "__main__":
    base = fetch_from_theoddsapi(hours_ahead=48)
    es = fetch_pandascore(hours_ahead=48)
    df = pd.concat([base, es], ignore_index=True)
    if not df.empty and (df["sport"]=="futbol").any():
        df = enrich_soccer_last5(df)
    df["status"] = "scheduled"
    df = df.dropna(subset=["start_time_utc","home","away"], how="any").sort_values("start_time_utc")
    out = PROC / "upcoming_events.csv"
    df.to_csv(out, index=False)
    print(f"fetch ok – wrote {len(df)} events -> {out}")
