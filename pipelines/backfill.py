# pipelines/backfill.py
"""
Backfill histórico (seguro si no hay datos o la API no responde).
Uso:
  python pipelines/backfill.py --sport mlb --years 2021-2024
  python pipelines/backfill.py --sport nfl --years 2020-2024

Hace:
- No rompe si la API no está disponible.
- Crea carpeta data/historical/<sport>/ y deja un índice con lo descargado.
- Por ahora es minimalista: descarga por día el endpoint base si existe, o hace NO-OP.
"""

from __future__ import annotations
import os, sys, json
from pathlib import Path
from datetime import date, timedelta
import argparse
import pandas as pd

# Import robusto del cliente APISports
try:
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from integrations.apisports_client import get as api_get  # type: ignore
except Exception:
    # Fallback que nunca rompe
    def api_get(*args, **kwargs):
        raise RuntimeError("apisports_client no disponible (fallback).")

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "historical"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SPORT_MAP = {
    "mlb": ("baseball", "/games"),
    "nfl": ("american_football", "/games"),
    "nba": ("basketball", "/games"),
    "nhl": ("hockey", "/games"),
    "soccer": ("football", "/fixtures"),
    "tenis": (None, None),  # placeholder para integrar otra fuente si se desea
}

def parse_years(s: str) -> list[int]:
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s)]

def daterange(y: int):
    d = date(y, 1, 1)
    end = date(y, 12, 31)
    while d <= end:
        yield d
        d += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", required=True, help="mlb|nfl|nba|nhl|soccer|tenis")
    ap.add_argument("--years", required=True, help="2021-2024 o 2024")
    args = ap.parse_args()

    sport_key = args.sport.lower()
    years = parse_years(args.years)
    out_sport_dir = RAW_DIR / sport_key
    out_sport_dir.mkdir(parents=True, exist_ok=True)

    api_sport, path = SPORT_MAP.get(sport_key, (None, None))

    index_rows = []
    for y in years:
        for d in daterange(y):
            fname = out_sport_dir / f"{d.isoformat()}.json"
            # Si ya existe, no volvemos a pedir
            if fname.exists():
                index_rows.append({"sport": sport_key, "date": d.isoformat(), "items": None, "status": "cached"})
                continue

            if api_sport and path:
                try:
                    obj = api_get(sport=api_sport, path=path, params={"date": d.isoformat()})
                    items = len(obj.get("response", [])) if isinstance(obj, dict) else 0
                    fname.write_text(json.dumps(obj, ensure_ascii=False))
                    index_rows.append({"sport": sport_key, "date": d.isoformat(), "items": items, "status": "ok"})
                except Exception as e:
                    index_rows.append({"sport": sport_key, "date": d.isoformat(), "items": 0, "status": f"err:{e}"})
            else:
                # NO-OP para deportes aún no cableados
                index_rows.append({"sport": sport_key, "date": d.isoformat(), "items": 0, "status": "noop"})

    idx = pd.DataFrame(index_rows)
    idx_path = out_sport_dir / "index.csv"
    idx.to_csv(idx_path, index=False, encoding="utf-8")
    print(f"[backfill] {sport_key} años={years} -> {idx_path} ({len(idx)} días)")
    print("[backfill] listo (no rompe si falta la API; es ampliable).")

if __name__ == "__main__":
    main()
