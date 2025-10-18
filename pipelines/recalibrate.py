# pipelines/recalibrate.py
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

FEATS = Path("data/processed/features.csv")
CALIB = Path("models/calibration.json")
CALIB.parent.mkdir(parents=True, exist_ok=True)

def main():
    n_rows = 0
    if FEATS.exists():
        try:
            df = pd.read_csv(FEATS)
            n_rows = len(df)
        except Exception:
            pass

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_rows": n_rows,
        "isotonic": False,
        "temperature": 1.0,
        "notes": "Recalibración placeholder: sin outcomes, mantiene defaults.",
    }
    CALIB.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"[recalibrate] escrito -> {CALIB} (rows={n_rows})")

if __name__ == "__main__":
    main()
