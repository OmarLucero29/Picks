import os
from datetime import datetime
from integrations.apisports_client import get

def test():
    today = datetime.utcnow().date().isoformat()
    checks = [
        ("football", "/status", {}),
        ("football", "/fixtures", {"date": today}),
        ("basketball", "/games", {"date": today}),
        ("hockey", "/games", {"date": today}),
        ("baseball", "/games", {"date": today}),
        ("american_football", "/games", {"date": today}),
    ]
    for sport, path, params in checks:
        try:
            j = get(sport=sport, path=path, params=params)
            n = len(j.get("response", [])) if isinstance(j, dict) else 0
            print(f"[{sport} {path}] OK — items={n}")
        except Exception as e:
            print(f"[{sport} {path}] ERROR — {e}")

if __name__ == "__main__":
    if not os.getenv("APISPORTS_KEY"):
        raise SystemExit("APISPORTS_KEY faltante en entorno")
    test()
