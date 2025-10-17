import os
import requests

APISPORTS_KEY = os.getenv("APISPORTS_KEY", "")

BASES = {
    "football": "https://v3.football.api-sports.io",
    "basketball": "https://v1.basketball.api-sports.io",
    "hockey": "https://v1.hockey.api-sports.io",
    "baseball": "https://v1.baseball.api-sports.io",
    "american_football": "https://v1.american-football.api-sports.io",
}

def get(session=None, sport="football", path="/status", params=None, timeout=30):
    """
    Wrapper unificado para APISports.
    sport: football | basketball | hockey | baseball | american_football
    path:  "/fixtures" | "/games" | "/status" ...
    """
    if not APISPORTS_KEY:
        raise RuntimeError("APISPORTS_KEY faltante")
    base = BASES.get(sport)
    if not base:
        raise ValueError(f"Sport no soportado: {sport}")
    url = f"{base}{path}"
    s = session or requests.Session()
    r = s.get(url, headers={"x-apisports-key": APISPORTS_KEY}, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()
