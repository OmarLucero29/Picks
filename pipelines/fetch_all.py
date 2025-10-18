# pipelines/fetch_all.py
import os, sys, json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import argparse
import pandas as pd

# --- Import robusto del cliente APISports ---
try:
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from integrations.apisports_client import get as api_get  # type: ignore
except Exception:
    # Fallback inline si no existe el paquete 'integrations'
    import requests
    APISPORTS_KEY = os.getenv("APISPORTS_KEY", "")
    BASES = {
        "football": "https://v3.football.api-sports.io",
        "basketball": "https://v1.basketball.api-sports.io",
        "hockey": "https://v1.hockey.api-sports.io",
        "baseball": "https://v1.baseball.api-sports.io",
        "american_football": "https://v1.american-football.api-sports.io",
    }
    def api_get(session=None, sport="football", path="/status", params=None, timeout=30):
        if not APISPORTS_KEY:
            raise RuntimeError("APISPORTS_KEY faltante")
        if sport not in BASES:
            raise ValueError(f"Sport no soportado: {sport}")
        url = f"{BASES[sport]}{path}"
        s = session or requests.Session()
        r = s.get(url, headers={"x-apisports-key": APISPORTS_KEY}, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()

OUT = Path("data/raw"); OUT.mkdir(parents=True, exist_ok=True)

def date_range(mode: str):
    now = datetime.now(timezone.utc)
    if mode == "daily":
        start = now
        end = now + timedelta(days=3)
    else:
        start = now
        end = now + timedelta(days=7)
    return start.date().isoformat(), end.date().isoformat()

def fetch_apisports_block(sport_key: str, path: str, date_str: str):
    try:
        j = api_get(sport=sport_key, path=path, params={"date": date_str})
        return j
    except Exception as e:
        print(f"[APISPORTS {sport_key} {date_str}] {e}")
        return {"errors": str(e), "response": []}

def save_json(obj, fname: str):
    # ✅ usar json.dumps (no pandas.io.json)
    (OUT / fname).write_text(json.dumps(obj, indent=2, ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="daily", choices=["daily","weekly"])
    args = parser.parse_args()

    d0, d1 = date_range(args.mode)
    days = pd.date_range(d0, d1, freq="D")

    mapping = [
        ("football", "/fixtures"),
        ("basketball", "/games"),
        ("hockey", "/games"),
        ("baseball", "/games"),
        ("american_football", "/games"),
    ]
    total = 0
    for d in days:
        ds = d.date().isoformat()
        for sport_key, path in mapping:
            obj = fetch_apisports_block(sport_key, path, ds)
            save_json(obj, f"apisports_{sport_key}_{ds}.json")
            n = len(obj.get("response", [])) if isinstance(obj, dict) else 0
            print(f"saved apisports_{sport_key}_{ds}.json  items={n}")
            total += n

    print(f"fetch_all: OK  total_items={total}")

if __name__ == "__main__":
    main()
