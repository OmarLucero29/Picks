# pipelines/fetch_all.py  — multisource upcoming (TheOddsAPI + API-FOOTBALL + NBA/MLB/NHL + ESPN NFL + PandaScore)
import os, requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pandas as pd

PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

ODDS_KEY = os.environ.get("ODDS_API_KEY", "")
PANDASCORE_TOKEN = os.environ.get("PANDASCORE_TOKEN", "")
APIFOOTBALL_KEY = os.environ.get("APIFOOTBALL_KEY", "")

def utcnow():
    return datetime.now(timezone.utc)

def iso(dt): return dt.isoformat()

def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None: return default
        if isinstance(k, int):
            if isinstance(cur, (list, tuple)) and -len(cur) <= k < len(cur):
                cur = cur[k]
            else:
                return default
        else:
            if isinstance(cur, dict):
                cur = cur.get(k)
            else:
                return default
    return default if cur is None else cur

# ---------- 1) TheOddsAPI (más abierto: sin forzar bet365) ----------
def fetch_theoddsapi(hours_ahead=72):
    if not ODDS_KEY:
        print("theoddsapi: no key")
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away","market_total","ml_home","ml_away","spread_line","spread_home","spread_away"])
    try:
        sports = requests.get(
            "https://api.the-odds-api.com/v4/sports",
            params={"apiKey": ODDS_KEY, "all": "true"},
            timeout=30
        ).json()
    except Exception as e:
        print(f"theoddsapi: error list sports: {e}")
        sports = []

    keep = []
    for s in sports:
        key = s.get("key","")
        if key.startswith(("soccer_","americanfootball_","basketball_","baseball_","icehockey_","tennis_","esports_","tabletennis_","mma_","boxing_","motorsport_")):
            keep.append(s)

    rows=[]; end_time = utcnow() + timedelta(hours=hours_ahead)
    for s in keep:
        skey = s["key"]
        try:
            r = requests.get(
                f"https://api.the-odds-api.com/v4/sports/{skey}/odds",
                params={
                    "apiKey": ODDS_KEY,
                    "regions": "us,eu,uk,au",
                    "markets": "h2h,totals,spreads",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                    # ← SIN “bookmakers” para no filtrar; si hay bet365 lo tomaremos abajo
                },
                timeout=45
            )
            r.raise_for_status()
            events = r.json()
        except Exception as e:
            print(f"theoddsapi: {skey} fail {e}")
            continue

        for ev in events:
            try:
                start = parser.isoparse(ev["commence_time"])
            except Exception:
                continue
            if start <= utcnow() or start > end_time:  # sólo próximos
                continue

            home = ev.get("home_team",""); away = ev.get("away_team","")
            league = s.get("title", skey)
            sport_key = skey.split("_")[0]
            sport_map = {
                "soccer":"futbol","americanfootball":"americano","basketball":"baloncesto",
                "baseball":"beisbol","icehockey":"hockey","tennis":"tenis","esports":"esports","tabletennis":"ping_pong"
            }
            sport = sport_map.get(sport_key, sport_key)

            # Preferimos bet365 si existe; si no, tomamos el primer book disponible
            books = ev.get("bookmakers", [])
            b365 = next((b for b in books if b.get("key")=="bet365"), None)
            bm = b365 or (books[0] if books else None)

            ml_home=ml_away=market_total=None
            spread_line=spread_home=spread_away=None
            if bm:
                for m in bm.get("markets", []):
                    if m.get("key")=="h2h":
                        for o in m.get("outcomes", []):
                            if o.get("name")==home: ml_home = o.get("price")
                            if o.get("name")==away: ml_away = o.get("price")
                    elif m.get("key")=="totals":
                        outs = m.get("outcomes", [])
                        if outs: market_total = outs[0].get("point")
                    elif m.get("key")=="spreads":
                        outs = m.get("outcomes", [])
                        if outs:
                            spread_line = outs[0].get("point")
                            for o in outs:
                                if o.get("name")==home: spread_home = o.get("price")
                                if o.get("name")==away: spread_away = o.get("price")

            rows.append(dict(
                sport=sport, league=league, start_time_utc=iso(start), status="scheduled",
                home=home, away=away, market_total=market_total, ml_home=ml_home, ml_away=ml_away,
                spread_line=spread_line, spread_home=spread_home, spread_away=spread_away
            ))
    print(f"theoddsapi: events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- 2) API-FOOTBALL (fixtures próximos sin odds) ----------
def fetch_soccer_apifootball(hours_ahead=72):
    if not APIFOOTBALL_KEY:
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away","market_total","ml_home","ml_away","spread_line","spread_home","spread_away"])
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    start = utcnow()
    end = start + timedelta(hours=hours_ahead)
    rows=[]
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            params={"from": start.date().isoformat(), "to": end.date().isoformat(), "timezone":"UTC"},
            headers=headers, timeout=45
        )
        data = r.json().get("response", [])
    except Exception as e:
        print(f"apifootball fail: {e}")
        data = []
    for fx in data:
        st = (safe_get(fx,"fixture","status","short", default="") or "").upper()
        if st in ("FT","AET","PEN","PST","CANC"):  # sólo próximos/pendientes
            continue
        date = safe_get(fx,"fixture","date", default=None)
        if not date: continue
        try:
            dt = parser.isoparse(date)
        except Exception:
            continue
        if dt <= utcnow(): 
            continue
        league = safe_get(fx,"league","name", default="Soccer")
        home = safe_get(fx,"teams","home","name", default="TBA")
        away = safe_get(fx,"teams","away","name", default="TBA")
        rows.append(dict(
            sport="futbol", league=league, start_time_utc=iso(dt), status="scheduled",
            home=home, away=away, market_total=None, ml_home=None, ml_away=None,
            spread_line=None, spread_home=None, spread_away=None
        ))
    print(f"apifootball: events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- 3) NBA (balldontlie) ----------
def fetch_nba_balldontlie(hours_ahead=72):
    start = utcnow().date()
    end = (utcnow()+timedelta(hours=hours_ahead)).date()
    rows=[]; page=1
    while True:
        try:
            r = requests.get(
                "https://www.balldontlie.io/api/v1/games",
                params={"start_date": start.isoformat(), "end_date": end.isoformat(), "per_page":100, "page":page},
                timeout=30
            )
            if r.status_code!=200: break
            data=r.json().get("data",[])
        except Exception:
            break
        if not data: break
        for g in data:
            status = g.get("status","")
            if status in ("Final","Postponed","Canceled"): 
                continue
            date = g.get("date")
            try:
                dt = parser.isoparse(date)
            except Exception:
                continue
            home = safe_get(g,"home_team","full_name", default="Home")
            away = safe_get(g,"visitor_team","full_name", default="Away")
            rows.append(dict(
                sport="baloncesto", league="NBA", start_time_utc=iso(dt), status="scheduled",
                home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                spread_line=None, spread_home=None, spread_away=None
            ))
        page+=1
        if page>12: break
    print(f"nba(bdl): events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- 4) MLB/NHL (StatsAPI schedules) ----------
def fetch_mlb_statsapi(hours_ahead=72):
    start = utcnow().date().isoformat()
    end = (utcnow()+timedelta(hours=hours_ahead)).date().isoformat()
    url=f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start}&endDate={end}"
    rows=[]
    try:
        data = requests.get(url, timeout=30).json()
        for day in data.get("dates",[]):
            for g in day.get("games",[]):
                if g.get("status",{}).get("abstractGameState")=="Final": 
                    continue
                home=g.get("teams",{}).get("home",{}).get("team",{}).get("name")
                away=g.get("teams",{}).get("away",{}).get("team",{}).get("name")
                dt = parser.isoparse(g.get("gameDate"))
                rows.append(dict(sport="beisbol", league="MLB", start_time_utc=iso(dt), status="scheduled",
                                 home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                                 spread_line=None, spread_home=None, spread_away=None))
    except Exception as e:
        print(f"mlb schedule fail: {e}")
    print(f"mlb(schedule): events {len(rows)}")
    return pd.DataFrame(rows)

def fetch_nhl_statsapi(hours_ahead=72):
    start = utcnow().date().isoformat()
    end = (utcnow()+timedelta(hours=hours_ahead)).date().isoformat()
    url=f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start}&endDate={end}"
    rows=[]
    try:
        data = requests.get(url, timeout=30).json()
        for day in data.get("dates",[]):
            for g in day.get("games",[]):
                # statusCode 7=Final; nos quedamos con no finalizados
                if g.get("status",{}).get("statusCode")=="7": 
                    continue
                home=g.get("teams",{}).get("home",{}).get("team",{}).get("name")
                away=g.get("teams",{}).get("away",{}).get("team",{}).get("name")
                dt = parser.isoparse(g.get("gameDate"))
                rows.append(dict(sport="hockey", league="NHL", start_time_utc=iso(dt), status="scheduled",
                                 home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                                 spread_line=None, spread_home=None, spread_away=None))
    except Exception as e:
        print(f"nhl schedule fail: {e}")
    print(f"nhl(schedule): events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- 5) NFL (ESPN scoreboard — sin odds) ----------
def fetch_nfl_espn(hours_ahead=96):
    # ESPN entrega por fecha; consultamos un rango corto
    rows=[]
    for off in range(0, int(hours_ahead/24)+2):
        day = (utcnow()+timedelta(days=off)).date().isoformat()
        try:
            r = requests.get("https://site.api.espn.com/apis/v2/sports/football/nfl/scoreboard",
                             params={"dates": day}, timeout=30)
            if r.status_code!=200: 
                continue
            for ev in r.json().get("events", []):
                comps = safe_get(ev,"competitions",0, default={})
                status = safe_get(comps,"status","type","name", default="")
                if status.lower() in ("status_final","final","postponed","canceled"):
                    continue
                dt_txt = safe_get(comps,"date", default=None)
                if not dt_txt: continue
                try:
                    dt = parser.isoparse(dt_txt)
                except Exception:
                    continue
                if dt <= utcnow(): 
                    continue
                competitors = safe_get(comps,"competitors", default=[]) or []
                home = next((c.get("team",{}).get("displayName") for c in competitors if c.get("homeAway")=="home"), "Home")
                away = next((c.get("team",{}).get("displayName") for c in competitors if c.get("homeAway")=="away"), "Away")
                rows.append(dict(sport="americano", league="NFL", start_time_utc=iso(dt), status="scheduled",
                                 home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                                 spread_line=None, spread_home=None, spread_away=None))
        except Exception as e:
            print(f"espn nfl fail: {e}")
    print(f"nfl(espn): events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- 6) PandaScore (e-sports) ----------
def fetch_pandascore(hours_ahead=72):
    if not PANDASCORE_TOKEN:
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away"])
    end = (utcnow() + timedelta(hours=hours_ahead)).isoformat()
    start = utcnow().isoformat()
    url = "https://api.pandascore.co/matches/upcoming"
    rows = []; page = 1
    while True:
        try:
            r = requests.get(url, params={"per_page": 50, "page": page, "range[begin_at]": f"{start},{end}"},
                             headers={"Authorization": f"Bearer {PANDASCORE_TOKEN}"}, timeout=30)
            if r.status_code != 200: break
            data = r.json()
        except Exception:
            break
        if not data: break
        for m in data:
            dt = None
            for k in ("begin_at","scheduled_at","start_at"):
                if m.get(k):
                    try: dt = parser.isoparse(m[k]); break
                    except Exception: pass
            if not dt or dt <= utcnow(): 
                continue
            opps = m.get("opponents") or []
            def name_at(i):
                if i >= len(opps) or not isinstance(opps[i], dict): return "TBA"
                op = opps[i].get("opponent") or opps[i].get("team") or {}
                return op.get("name") or op.get("slug") or op.get("acronym") or "TBA"
            home, away = name_at(0), name_at(1)
            league = safe_get(m,"league","name", default="eSports")
            rows.append(dict(sport="esports", league=league, start_time_utc=iso(dt), status="scheduled",
                             home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                             spread_line=None, spread_home=None, spread_away=None))
        page += 1
        if page > 4: break
    print(f"pandascore: events {len(rows)}")
    return pd.DataFrame(rows)

# ---------- MAIN ----------
if __name__ == "__main__":
    parts = [
        fetch_theoddsapi(72),
        fetch_soccer_apifootball(72),
        fetch_nba_balldontlie(72),
        fetch_mlb_statsapi(96),
        fetch_nhl_statsapi(96),
        fetch_nfl_espn(120),
        fetch_pandascore(96),
    ]
    frames = [p for p in parts if isinstance(p, pd.DataFrame)]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[
        "sport","league","start_time_utc","status","home","away",
        "market_total","ml_home","ml_away","spread_line","spread_home","spread_away"
    ])
    if not df.empty:
        df = df.dropna(subset=["start_time_utc","home","away"], how="any")
        df["start_time_utc"] = pd.to_datetime(df["start_time_utc"], utc=True, errors="coerce")
        df = df[df["start_time_utc"] > utcnow()].sort_values("start_time_utc")
    out = PROC/"upcoming_events.csv"
    df.to_csv(out, index=False)
    # Conteo por deporte para depurar
    if not df.empty:
        print("upcoming by sport:", df["sport"].value_counts().to_dict())
    print(f"fetch_all: wrote {len(df)} events -> {out}")
