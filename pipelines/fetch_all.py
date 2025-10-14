# pipelines/fetch_all.py  (VERSION r2-ps-fix)
import os, requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import parser
import pandas as pd

PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

ODDS_KEY = os.environ.get("ODDS_API_KEY", "")
PANDASCORE_TOKEN = os.environ.get("PANDASCORE_TOKEN", "")

def utcnow():
    return datetime.now(timezone.utc)

def safe_get(cur, *keys, default=None):
    """
    Acceso seguro para mezclas dict/list con índices.
    safe_get(d, 'a', 0, 'b') -> d['a'][0]['b'] si existe; si no, default.
    """
    for k in keys:
        if cur is None:
            return default
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

# -------------------- TheOddsAPI (cuotas bet365) --------------------
def fetch_from_theoddsapi(hours_ahead=48):
    if not ODDS_KEY:
        return pd.DataFrame(columns=[
            "sport","league","start_time_utc","status","home","away",
            "market_total","ml_home","ml_away","spread_line","spread_home","spread_away"
        ])

    try:
        sports = requests.get(
            "https://api.the-odds-api.com/v4/sports",
            params={"apiKey": ODDS_KEY, "all": "true"}, timeout=30
        ).json()
    except Exception:
        sports = []

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
            key.startswith("motorsport_"),
        ]):
            keep.append(s)

    rows = []
    end_time = utcnow() + timedelta(hours=hours_ahead)

    for s in keep:
        key = s["key"]
        try:
            r = requests.get(
                f"https://api.the-odds-api.com/v4/sports/{key}/odds",
                params={
                    "apiKey": ODDS_KEY,
                    "regions": "us,eu,uk",
                    "markets": "h2h,totals,spreads",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                    "bookmakers": "bet365",
                }, timeout=30
            )
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

            home = ev.get("home_team","")
            away = ev.get("away_team","")
            league = s.get("title", key)
            sport_key = key.split("_")[0]

            bm = next((b for b in ev.get("bookmakers", []) if b.get("key")=="bet365"), None)
            ml_home = ml_away = None
            market_total = None
            spread_line = None; spread_home = spread_away = None
            if bm:
                for m in bm.get("markets", []):
                    if m.get("key") == "h2h":
                        for o in m.get("outcomes", []):
                            if o.get("name") == home: ml_home = o.get("price")
                            if o.get("name") == away: ml_away = o.get("price")
                    elif m.get("key") == "totals":
                        outs = m.get("outcomes", [])
                        if outs:
                            market_total = outs[0].get("point")
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

# -------------------- PandaScore (e-sports) --------------------
def ps_normalize_team(entry):
    """entry puede ser {'opponent': {...}}, {'team': {...}} o directamente {'name': ...}."""
    if not isinstance(entry, dict): 
        return None
    # Formato usual
    if isinstance(entry.get("opponent"), dict):
        return entry["opponent"].get("name") or entry["opponent"].get("slug") or entry["opponent"].get("acronym")
    # Otras variantes
    if isinstance(entry.get("team"), dict):
        return entry["team"].get("name") or entry["team"].get("slug")
    # Flat
    for k in ("name","slug","acronym","short_name","display_name"):
        if entry.get(k): return entry[k]
    return None

def ps_extract_teams(m):
    opps = m.get("opponents") or []
    home = ps_normalize_team(opps[0]) if len(opps)>0 else None
    away = ps_normalize_team(opps[1]) if len(opps)>1 else None
    if not home: home = "TBA"
    if not away: away = "TBA"
    return home, away

def ps_extract_begin(m):
    for k in ("begin_at","scheduled_at","start_at"):
        val = m.get(k)
        if val:
            try:
                return parser.isoparse(val)
            except Exception:
                pass
    return None

def fetch_pandascore(hours_ahead=48):
    if not PANDASCORE_TOKEN:
        return pd.DataFrame(columns=["sport","league","start_time_utc","status","home","away"])

    end = (utcnow() + timedelta(hours=hours_ahead)).isoformat()
    start = utcnow().isoformat()
    url = "https://api.pandascore.co/matches/upcoming"
    rows = []; page = 1

    while True:
        try:
            r = requests.get(
                url,
                params={"per_page": 50, "page": page, "range[begin_at]": f"{start},{end}"},
                headers={"Authorization": f"Bearer {PANDASCORE_TOKEN}"},
                timeout=30
            )
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break

        if not data:
            break

        for m in data:
            start_dt = ps_extract_begin(m)
            if not start_dt:
                continue
            home, away = ps_extract_teams(m)
            league = safe_get(m, "league", "name", default="eSports")

            rows.append(dict(
                sport="esports", league=league, start_time_utc=start_dt.isoformat(), status="scheduled",
                home=home, away=away, market_total=None, ml_home=None, ml_away=None,
                spread_line=None, spread_home=None, spread_away=None
            ))
        page += 1
        if page > 4:
            break

    return pd.DataFrame(rows)

# -------------------- MAIN --------------------
if __name__ == "__main__":
    base = fetch_from_theoddsapi(hours_ahead=48)
    es = fetch_pandascore(hours_ahead=48)
    df = pd.concat([base, es], ignore_index=True)

    if not df.empty:
        df["status"] = "scheduled"
        df = df.dropna(subset=["start_time_utc","home","away"], how="any").sort_values("start_time_utc")

    out = PROC / "upcoming_events.csv"
    df.to_csv(out, index=False)
    print(f"[fetch_all r2] fetch ok – wrote {len(df)} events -> {out}")
