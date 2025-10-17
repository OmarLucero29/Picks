# pipelines/fetch_all.py
import os
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

from integrations.apisports_client import get as api_get

OUT = Path("data/raw"); OUT.mkdir(parents=True, exist_ok=True)

def date_range(mode):
    now = datetime.now(timezone.utc)
    if mode == "daily":
        start = now
        end = now + timedelta(days=3)
    else:
        start = now
        end = now + timedelta(days=7)
    return start.date().isoformat(), end.date().isoformat()

def fetch_apisports_block(sport_key, path, date_str):
    try:
        j = api_get(sport=sport_key, path=path, params={"date": date_str})
        return j
    except Exception as e:
        print(f"[APISPORTS {sport_key}] {e}")
        return {"errors": str(e), "response": []}

def save_json(obj, fname):
    (OUT/fname).write_text(pd.io.json.dumps(obj, indent=2, ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="daily", choices=["daily","weekly"])
    args = parser.parse_args()

    d0, d1 = date_range(args.mode)
    days = pd.date_range(d0, d1, freq="D")

    # === Football / Basketball / Hockey / Baseball / American Football (fixtures of day) ===
    mapping = [
        ("football", "/fixtures"),
        ("basketball", "/games"),
        ("hockey", "/games"),
        ("baseball", "/games"),
        ("american_football", "/games"),
    ]
    for d in days:
        ds = d.date().isoformat()
        for sport_key, path in mapping:
            obj = fetch_apisports_block(sport_key, path, ds)
            save_json(obj, f"apisports_{sport_key}_{ds}.json")

    # === Tus otros fetchers (no requieren key) — placeholder ===
    # MLB StatsAPI, NHL StatsAPI, Ergast F1, Jeff Sackmann Tenis, nflverse, etc.
    # if needed: call other modules here.

    print("fetch_all: OK")

if __name__ == "__main__":
    main()
