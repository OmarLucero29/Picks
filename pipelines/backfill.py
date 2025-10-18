# pipelines/backfill.py
"""
Backfill histórico ultrarrápido + comprimido:
- Async + httpx (concurrencia controlada)
- Rate limiter global (throttle)
- Ventanas from/to para soccer (1 request mensual + paginación)
- Día a día paralelizado para mlb/nfl/nba/nhl
- Salida comprimida: *.json.gz (menor I/O)
- Reanudable: no reescribe scopes ya guardados
- Índice por deporte: data/historical/<sport>/index.csv
"""

from __future__ import annotations
import os, sys, json, math, argparse, asyncio, time, gzip
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Tuple

import pandas as pd
import httpx

APISPORTS_KEY = os.getenv("APISPORTS_KEY", "")
DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "historical"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# sport interno -> (apisport_key, base_url, endpoint)
SPORT_MAP = {
    "soccer": ("football", "https://v3.football.api-sports.io", "/fixtures"),  # soporta from/to
    "mlb":    ("baseball", "https://v1.baseball.api-sports.io", "/games"),
    "nfl":    ("american_football","https://v1.american-football.api-sports.io","/games"),
    "nba":    ("basketball","https://v1.basketball.api-sports.io","/games"),
    "nhl":    ("hockey","https://v1.hockey.api-sports.io","/games"),
}

DEFAULT_THROTTLE_MS   = int(os.getenv("APISPORTS_THROTTLE_MS", "250"))  # ~4 rps
DEFAULT_CONCURRENCY   = int(os.getenv("APISPORTS_CONCURRENCY", "8"))
DEFAULT_WINDOW_DAYS   = 30  # soccer from/to

# ---------- fechas ----------
def iter_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def parse_years(s: str) -> List[int]:
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s)]

def years_to_range(years: List[int]) -> Tuple[date, date]:
    return date(min(years), 1, 1), date(max(years), 12, 31)

def month_windows(start: date, end: date, window_days: int = DEFAULT_WINDOW_DAYS) -> List[Tuple[date, date]]:
    windows = []
    cur = start
    while cur <= end:
        to = min(end, cur + timedelta(days=window_days - 1))
        windows.append((cur, to))
        cur = to + timedelta(days=1)
    return windows

# ---------- rate limiter ----------
class RateLimiter:
    def __init__(self, min_interval_ms: int):
        self.min_interval = max(0.0, min_interval_ms / 1000.0)
        self._last = 0.0
        self._lock = asyncio.Lock()
    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                await asyncio.sleep(self.min_interval - delta)
            self._last = time.monotonic()

# ---------- almacenamiento (.json.gz) ----------
def _write_json_gz(path: Path, obj: Dict[str, Any]):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def save_pages(out_dir: Path, scope: str, pages: List[Dict[str, Any]]) -> Tuple[int,int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    total_items, total_pages = 0, 0
    for i, obj in enumerate(pages, start=1):
        items = len(obj.get("response", [])) if isinstance(obj, dict) else 0
        total_items += items
        total_pages += 1
        _write_json_gz(out_dir / f"{scope}_p{i}.json.gz", obj)
    return total_items, total_pages

def scope_exists(out_dir: Path, scope: str) -> bool:
    return any(out_dir.glob(f"{scope}_p*.json.gz"))

# ---------- core HTTP ----------
async def api_get(client: httpx.AsyncClient, base: str, path: str,
                  params: Dict[str, Any], limiter: RateLimiter, retries: int = 3) -> Dict[str, Any]:
    if not APISPORTS_KEY:
        raise RuntimeError("APISPORTS_KEY faltante")
    headers = {"x-apisports-key": APISPORTS_KEY}
    url = f"{base}{path}"
    for attempt in range(retries):
        await limiter.wait()
        try:
            r = await client.get(url, headers=headers, params=params, timeout=30.0)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                return {"errors": str(e), "response": []}
            await asyncio.sleep(0.75 * (2 ** attempt))

# ---------- window (soccer) ----------
async def fetch_window(client: httpx.AsyncClient, base: str, path: str,
                       start_iso: str, end_iso: str, limiter: RateLimiter) -> List[Dict[str, Any]]:
    pages, cur = [], 1
    while True:
        obj = await api_get(client, base, path,
                            {"from": start_iso, "to": end_iso, "page": cur},
                            limiter)
        pages.append(obj)
        try:
            paging = obj.get("paging") or {}
            total = int(paging.get("total") or 1)
            current = int(paging.get("current") or cur)
        except Exception:
            total, current = 1, cur
        if current >= total:
            break
        cur += 1
    return pages

# ---------- day (mlb/nfl/nba/nhl) ----------
async def fetch_day(client: httpx.AsyncClient, base: str, path: str,
                    day_iso: str, limiter: RateLimiter) -> List[Dict[str, Any]]:
    pages, cur = [], 1
    while True:
        obj = await api_get(client, base, path, {"date": day_iso, "page": cur}, limiter)
        pages.append(obj)
        try:
            paging = obj.get("paging") or {}
            total = int(paging.get("total") or 1)
            current = int(paging.get("current") or cur)
        except Exception:
            total, current = 1, cur
        if current >= total:
            break
        cur += 1
    return pages

# ---------- ejecución ----------
async def run_backfill(sports: List[str], start: date, end: date,
                       window_days: int, concurrency: int, throttle_ms: int,
                       overwrite: bool):
    limiter = RateLimiter(throttle_ms)
    async with httpx.AsyncClient(timeout=30.0) as client:
        sem = asyncio.Semaphore(concurrency)
        tasks, all_rows = [], []

        async def _worker(sport_key: str, base: str, path: str,
                          out_dir: Path, scope: str, coro):
            async with sem:
                if not overwrite and scope_exists(out_dir, scope):
                    all_rows.append({"sport": sport_key, "scope": scope,
                                     "pages": None, "items": None, "status": "cached"})
                    return
                pages = await coro
                items, pages_n = save_pages(out_dir, scope, pages)
                all_rows.append({"sport": sport_key, "scope": scope,
                                 "pages": pages_n, "items": items, "status": "ok"})

        for s in sports:
            apikey, base, path = SPORT_MAP[s]
            out_dir = OUT_DIR / s
            out_dir.mkdir(parents=True, exist_ok=True)

            if s == "soccer":
                for a, b in month_windows(start, end, window_days):
                    scope = f"{a.isoformat()}_{b.isoformat()}"
                    coro = fetch_window(client, base, path, a.isoformat(), b.isoformat(), limiter)
                    tasks.append(_worker(s, base, path, out_dir, scope, coro))
            else:
                for d in iter_dates(start, end):
                    scope = f"{d.isoformat()}"
                    coro = fetch_day(client, base, path, d.isoformat(), limiter)
                    tasks.append(_worker(s, base, path, out_dir, scope, coro))

        await asyncio.gather(*tasks)

        df = pd.DataFrame(all_rows)
        for s in sports:
            sub = df[df["sport"] == s].copy()
            if not sub.empty:
                (OUT_DIR / s / "index.csv").write_text(sub.to_csv(index=False), encoding="utf-8")
                print(f"[{s}] {len(sub)} scopes -> {OUT_DIR / s / 'index.csv'}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", required=True, help="soccer|mlb|nfl|nba|nhl|all o lista (mlb,nfl)")
    ap.add_argument("--years", help="2021-2024 o 2024")
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end", help="YYYY-MM-DD")
    ap.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    ap.add_argument("--throttle-ms", type=int, default=DEFAULT_THROTTLE_MS)
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    sports_raw = [s.strip().lower() for s in args.sport.split(",")]
    sports = list(SPORT_MAP.keys()) if "all" in sports_raw else [s for s in sports_raw if s in SPORT_MAP]

    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end   = datetime.strptime(args.end, "%Y-%m-%d").date()
    elif args.years:
        yrs = parse_years(args.years)
        start, end = years_to_range(yrs)
    else:
        raise SystemExit("Debes indicar --years o --start/--end")

    print(f"[backfill] sports={sports} range={start}..{end} "
          f"win={args.window_days}d conc={args.concurrency} throttle={args.throttle_ms}ms")

    asyncio.run(run_backfill(
        sports=sports,
        start=start, end=end,
        window_days=args.window_days,
        concurrency=args.concurrency,
        throttle_ms=args.throttle_ms,
        overwrite=args.overwrite
    ))
    print("[backfill] terminado.")

if __name__ == "__main__":
    main()