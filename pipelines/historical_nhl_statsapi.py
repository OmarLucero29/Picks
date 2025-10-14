# pipelines/historical_nhl_statsapi.py
import requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from requests.exceptions import RequestException

OUT = Path("data/historical"); OUT.mkdir(parents=True, exist_ok=True)

# ---------- Fuente 1: StatsAPI (oficial) ----------
def fetch_range_statsapi(start_date, end_date):
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={start_date}&endDate={end_date}"
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent":"multisport-starter/1.0"})
        r.raise_for_status()
        data = r.json()
    except RequestException:
        return []
    rows = []
    for day in data.get("dates",[]):
        for g in day.get("games",[]):
            if g.get("status",{}).get("statusCode") != "7":  # 7 = Final
                continue
            home=g.get("teams",{}).get("home",{}).get("team",{}).get("name")
            away=g.get("teams",{}).get("away",{}).get("team",{}).get("name")
            hs=g.get("teams",{}).get("home",{}).get("score",0)
            as_=g.get("teams",{}).get("away",{}).get("score",0)
            rows.append(dict(date=day.get("date"), home=home, away=away, result_home_win=int(hs>as_)))
    return rows

def fetch_statsapi_last5y():
    current=datetime.utcnow().date()
    start=(current.replace(month=1, day=1) - timedelta(days=365*5))
    rows=[]; cur=start
    while cur<=current:
        end=min(cur+timedelta(days=29), current)
        rows += fetch_range_statsapi(cur.isoformat(), end.isoformat())
        cur=end+timedelta(days=1)
    return rows

# ---------- Fuente 2 (fallback): Hockey-Reference ----------
def fetch_hr_table(url):
    try:
        tables = pd.read_html(url)
    except Exception:
        return pd.DataFrame()
    if not tables: 
        return pd.DataFrame()
    df = tables[0].copy()
    # Normaliza nombres de columnas típicos: Date, Visitor, G, Home, G
    df.columns = [str(c).strip() for c in df.columns]
    # Drop filas de encabezado repetido
    df = df[df["Date"].astype(str).str.contains(r"\d", regex=True)]
    # Identifica columnas de goles (suelen llamarse 'G' y 'G.1')
    vis_g_col = None; home_g_col = None
    if "G" in df.columns and "G.1" in df.columns:
        vis_g_col, home_g_col = "G", "G.1"
    else:
        # fallback: busca numéricas
        num_cols = [c for c in df.columns if c.lower().startswith("g")]
        if len(num_cols)>=2:
            vis_g_col, home_g_col = num_cols[0], num_cols[1]
    if not {"Date","Visitor","Home"}.issubset(df.columns) or vis_g_col is None or home_g_col is None:
        return pd.DataFrame()
    # Convierte tipos
    df["date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["vh"] = pd.to_numeric(df[vis_g_col], errors="coerce")
    df["hh"] = pd.to_numeric(df[home_g_col], errors="coerce")
    df = df.dropna(subset=["date","vh","hh","Visitor","Home"])
    out = df[["date","Home","Visitor","hh","vh"]].rename(columns={"Home":"home","Visitor":"away"})
    out["result_home_win"] = (out["hh"]>out["vh"]).astype(int)
    out["date"] = out["date"].astype(str)
    return out[["date","home","away","result_home_win"]]

def fetch_hockeyref_last5y():
    # En HR, el año es el de cierre de temporada (p.ej., 2024 para 2023-24)
    this_year = datetime.utcnow().year
    end_years = list(range(this_year-4, this_year+1))
    rows=[]
    for y in end_years:
        urls = [
            f"https://www.hockey-reference.com/leagues/NHL_{y}_games.html",
            f"https://www.hockey-reference.com/leagues/NHL_{y}_games-playoffs.html",
        ]
        for u in urls:
            df = fetch_hr_table(u)
            if not df.empty:
                rows += df.to_dict("records")
    return rows

def main():
    # 1) Intenta StatsAPI
    rows = fetch_statsapi_last5y()
    source = "statsapi"
    # 2) Si falla o queda vacío, usa fallback
    if not rows:
        rows = fetch_hockeyref_last5y()
        source = "hockey-reference"
    if not rows:
        print("no nhl data"); return
    df = pd.DataFrame(rows)
    df["sport"]="hockey"; df["league"]="NHL"
    (OUT/"nhl_games.csv").write_text("")  # asegúrate de crear el archivo
    df.to_csv(OUT/"nhl_games.csv", index=False)
    print(f"historical nhl ok ({source}):", len(df))

if __name__=='__main__': main()
