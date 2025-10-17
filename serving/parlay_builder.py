# serving/parlay_builder.py — Segurito + Soñadora con columnas finales
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import datetime
import hashlib

REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

# Config
MAX_LEGS_SEG = int(os.environ.get("PARLAY_LEGS","5"))
MAX_LEGS_DRM = int(os.environ.get("PARLAY_LEGS_DREAM","5"))
TARGET_DEC_ODDS_SEG = float(os.environ.get("TARGET_DEC_ODDS","2.5"))
TARGET_DEC_ODDS_DRM = float(os.environ.get("TARGET_DEC_ODDS_DREAM","10.0"))  # soñadora ≥10 por defecto
MARGIN = float(os.environ.get("MARGIN","0.95"))

STAKE_SEG = os.environ.get("STAKE_SEGURITO","5%")
STAKE_DRM = os.environ.get("STAKE_SONADORA","2%")

def load_pool():
    f = REPORTS/"all_picks.csv"
    if not f.exists() or f.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(f)

def parlay_metrics(probs):
    p = float(np.prod(probs))
    dec = (1.0 / max(p, 1e-6)) * MARGIN
    return p, dec

def build_parlay(pool, max_legs, target_dec):
    legs = []
    probs = []
    for _, row in pool.iterrows():
        if len(legs) >= max_legs: break
        legs.append(row)
        # CUOTA (PROB %) viene como "x.xx (yy%)" → extraer prob
        try:
            prob_pct = int(str(row["CUOTA (PROB %)"]).split("(")[1].split("%")[0])
            p = prob_pct/100.0
        except Exception:
            p = 0.55
        probs.append(p)
        _, dec = parlay_metrics(probs)
        if dec >= target_dec and len(legs) >= 2:
            break
    legs = pd.DataFrame(legs)
    p, dec = parlay_metrics([int(str(x).split("(")[1].split("%")[0])/100.0 for x in legs["CUOTA (PROB %)"]]) if not legs.empty else (0.0, 0.0)
    return legs, round(p,4), round(dec,2)

def make_parlay_id(tipo):
    ymd = datetime.now().strftime("%Y%m%d")
    salt = hashlib.sha1(f"{tipo}|{ymd}|{datetime.now().isoformat()}".encode()).hexdigest()[:5].upper()
    return f"PR{ymd}-{salt}"

def format_parlay_rows(legs_df, tipo, parlay_id, stake):
    # columnas objetivo
    legs_df = legs_df.copy()
    legs_df["ID"] = parlay_id
    legs_df["TIPO"] = "🔒 Segurito" if tipo=="segurito" else "🌙 Soñadora"
    # Renombrar desde all_picks:
    # FECHA, DEPORTE, PARTIDO, MERCADO, PICK, CUOTA (PROB %), STAKE
    # Para parlays, STAKE del parlay (igual en todas las legs)
    legs_df["STAKE"] = stake
    cols = ["ID","TIPO","FECHA","DEPORTE","PARTIDO","MERCADO","PICK","CUOTA (PROB %)","STAKE"]
    return legs_df[cols]

def main():
    pool = load_pool()
    if pool.empty:
        (REPORTS/"parlay.csv").write_text("")
        print("parlay ok – 0 (sin picks)")
        return
    pool = pool.sort_values("CUOTA (PROB %)", ascending=False)  # ya viene ordenado por prob, pero no estorba

    # Segurito
    seg_legs, seg_p, seg_dec = build_parlay(pool.sort_values("CUOTA (PROB %)", ascending=True).iloc[::-1], MAX_LEGS_SEG, TARGET_DEC_ODDS_SEG)
    seg_id = make_parlay_id("segurito") if not seg_legs.empty else None
    seg_rows = format_parlay_rows(seg_legs, "segurito", seg_id, STAKE_SEG) if seg_id else pd.DataFrame()

    # Soñadora (prioriza cuota objetivo ≥ 10)
    drm_legs, drm_p, drm_dec = build_parlay(pool, MAX_LEGS_DRM, TARGET_DEC_ODDS_DRM)
    drm_id = make_parlay_id("sonadora") if not drm_legs.empty else None
    drm_rows = format_parlay_rows(drm_legs, "sonadora", drm_id, STAKE_DRM) if drm_id else pd.DataFrame()

    out = REPORTS/"parlay.csv"
    if not seg_rows.empty or not drm_rows.empty:
        pd.concat([seg_rows, drm_rows], ignore_index=True).to_csv(out, index=False)
        print(f"parlay ok – seg={len(seg_rows)} legs, dream={len(drm_rows)} legs -> {out}")
    else:
        (REPORTS/"parlay.csv").write_text("")
        print("parlay ok – 0")

if __name__ == "__main__":
    main()