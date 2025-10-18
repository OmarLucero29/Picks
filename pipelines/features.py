# pipelines/features.py
import os, sys, json, hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def safe_get(d, *path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def first_of(d, candidates, default=None):
    """Prueba varias rutas y devuelve el primer valor encontrado."""
    for path in candidates:
        v = safe_get(d, *path, default=None)
        if v not in (None, "", []):
            return v
    return default

def norm_dt(s):
    if not s:
        return ""
    try:
        # APISports suele dar ISO8601 con Z
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(s)

def make_id(sport, raw_id, home, away, dt_iso):
    base = f"{sport}|{raw_id}|{home}|{away}|{dt_iso}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def parse_item(sport, item):
    # Fecha (varía por deporte)
    date_iso = first_of(
        item,
        [
            ("fixture","date"),
            ("game","date"),
            ("date",),
            ("events","date"),  # fallback muy raro
        ],
        default="",
    )
    date_iso = norm_dt(date_iso)

    # League / competition
    league = first_of(item, [("league","name"), ("league","id"), ("tournament","name")], default="")

    # Equipos (home/away) – APISports usa "teams" o "teams.home/away" o "teams.name"
    home = first_of(
        item,
        [("teams","home","name"), ("home","name"), ("home","team","name"), ("teams","home")],
        default="",
    )
    away = first_of(
        item,
        [("teams","away","name"), ("away","name"), ("away","team","name"), ("teams","away")],
        default="",
    )

    # Venue
    venue = first_of(
        item,
        [("fixture","venue","name"), ("game","venue","name"), ("venue","name")],
        default="",
    )

    # Raw ID si existe
    raw_id = first_of(item, [("fixture","id"), ("game","id"), ("id",)], default="")

    # Status (opcional)
    status = first_of(item, [("fixture","status","short"), ("status","short"), ("status","long")], default="")

    # Construir ID estable
    eid = make_id(sport, raw_id, home or "", away or "", date_iso or "")

    # Fecha solo (YYYY-MM-DD)
    date_only = ""
    try:
        if date_iso:
            date_only = datetime.fromisoformat(date_iso.replace("Z","+00:00")).date().isoformat()
    except Exception:
        date_only = ""

    return {
        "ID": eid,
        "date": date_only,
        "date_time_utc": date_iso,
        "sport": sport,
        "league": str(league or ""),
        "home": str(home or ""),
        "away": str(away or ""),
        "venue": str(venue or ""),
        "status": str(status or ""),
    }

def collect_events():
    rows = []
    if not RAW_DIR.exists():
        return pd.DataFrame(columns=["ID","date","date_time_utc","sport","league","home","away","venue","status"])

    for p in sorted(RAW_DIR.glob("apisports_*.json")):
        # nombre: apisports_{sport}_{YYYY-MM-DD}.json
        try:
            sport = p.name.split("_")[1]
        except Exception:
            sport = "unknown"
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

        resp = obj.get("response", [])
        if not isinstance(resp, list):
            continue

        for item in resp:
            try:
                row = parse_item(sport, item)
                rows.append(row)
            except Exception:
                # no rompemos por un evento mal formado
                pass

    cols = ["ID","date","date_time_utc","sport","league","home","away","venue","status"]
    df = pd.DataFrame(rows, columns=cols).drop_duplicates(subset=["ID"], keep="first")
    return df

def build_features(df_events: pd.DataFrame):
    """Aquí puedes crear columnas extra que tu modelo use; por ahora básicas."""
    if df_events.empty:
        return df_events.assign(
            home_form=0.0,
            away_form=0.0,
            days_to_kickoff=0.0,
        )

    # Ejemplos de features sencillas
    now = datetime.utcnow()
    def days_to(dt_iso):
        try:
            d = datetime.fromisoformat(dt_iso.replace("Z","+00:00"))
            return (d - now).total_seconds()/86400.0
        except Exception:
            return 0.0

    feats = df_events.copy()
    feats["days_to_kickoff"] = feats["date_time_utc"].apply(days_to)
    # placeholders de forma (luego podremos rellenar con históricos reales)
    feats["home_form"] = 0.0
    feats["away_form"] = 0.0
    return feats

def main():
    df_events = collect_events()

    # Siempre escribir ambos CSV (aunque vacíos) para no romper pasos siguientes
    upc_path = OUT_DIR / "upcoming_events.csv"
    feat_path = OUT_DIR / "features.csv"

    df_events_out = df_events.copy()
    df_events_out.to_csv(upc_path, index=False, encoding="utf-8")
    print(f"upcoming_events ok – {len(df_events_out)} rows -> {upc_path}")

    df_feats = build_features(df_events)
    df_feats.to_csv(feat_path, index=False, encoding="utf-8")
    print(f"features ok – {len(df_feats)} rows -> {feat_path}")

if __name__ == "__main__":
    main()
