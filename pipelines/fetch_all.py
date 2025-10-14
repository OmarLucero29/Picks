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
