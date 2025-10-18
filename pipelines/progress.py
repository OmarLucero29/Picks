# pipelines/progress.py
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import date, datetime
import pandas as pd

HIST_DIR = Path("data/historical")
REPORTS = Path("reports")
REPORTS.mkdir(parents=True, exist_ok=True)
SPORTS = ["soccer", "mlb", "nfl", "nba", "nhl"]

def month_iter(start: date, end: date):
    y, m = start.year, start.month
    while True:
        first = date(y, m, 1)
        if first > end:
            break
        yield first
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years-start", required=True)
    ap.add_argument("--years-end", required=True)
    args = ap.parse_args()

    y0, y1 = int(args.years_start), int(args.years_end)
    start, end = date(y0,1,1), date(y1,12,31)

    soccer_total = sum(1 for _ in month_iter(start, end))
    rows = []

    for sport in SPORTS:
        idx_path = HIST_DIR / sport / "index.csv"
        if not idx_path.exists():
            total = soccer_total if sport == "soccer" else (end - start).days + 1
            rows.append([sport, total, 0, 0.0]); continue

        idx = pd.read_csv(idx_path)
        if sport == "soccer":
            done = 0
            seen = set()
            for s in idx.get("scope", []):
                if isinstance(s, str) and "_" in s:
                    left = s.split("_",1)[0]
                    try:
                        dt = datetime.strptime(left, "%Y-%m-%d").date()
                        if start <= dt <= end:
                            seen.add((dt.year, dt.month))
                        done = len(seen)
                    except: pass
            total = soccer_total
        else:
            seen = set()
            for s in idx.get("scope", []):
                if isinstance(s, str) and len(s)==10 and s[4]=="-" and s[7]=="-":
                    try:
                        dt = datetime.strptime(s, "%Y-%m-%d").date()
                        if start <= dt <= end: seen.add(dt)
                    except: pass
            done = len(seen)
            total = (end - start).days + 1

        pct = round(100.0 * done / total, 2) if total>0 else 0.0
        rows.append([sport, total, done, pct])

    out = pd.DataFrame(rows, columns=["sport","total_scopes","done_scopes","percent"])
    out.to_csv(REPORTS / "historical_progress.csv", index=False)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()