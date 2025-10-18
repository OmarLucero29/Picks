# models/predict.py
"""
Genera data/processed/predictions.csv a partir de data/processed/features.csv
- Acepta columnas: date, date_time_utc (o start_time_utc), sport, league, home, away, venue,
  y opcionalmente: home_form, away_form, days_to_kickoff.
- Produce columnas: date, sport, league, game, market, selection, line, prob,
  prob_decimal, confidence, rationale.
"""

from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
import numpy as np

IN_FEATS = Path("data/processed/features.csv")
OUT_PRED = Path("data/processed/predictions.csv")
OUT_PRED.parent.mkdir(parents=True, exist_ok=True)

def _bucket_confidence(p: float) -> str:
    if p >= 0.63:   # ~ -170 american
        return "Alta"
    if p >= 0.56:   # ~ -127 american
        return "Media"
    return "Baja"

def _clip01(x: np.ndarray | float) -> np.ndarray | float:
    return np.clip(x, 0.01, 0.99)

def main():
    if not IN_FEATS.exists():
        raise FileNotFoundError(f"No existe {IN_FEATS}")

    df = pd.read_csv(IN_FEATS)
    # Normalización de columnas esperadas
    if "start_time_utc" not in df.columns:
        if "date_time_utc" in df.columns:
            df["start_time_utc"] = df["date_time_utc"]
        else:
            df["start_time_utc"] = ""

    for col in ["date","sport","league","home","away","venue"]:
        if col not in df.columns:
            df[col] = ""

    # Features opcionales con defaults
    for col, default in [("home_form", 0.0), ("away_form", 0.0), ("days_to_kickoff", 0.0)]:
        if col not in df.columns:
            df[col] = default

    if df.empty:
        # escribir CSV vacío con cabecera correcta para no romper downstream
        empty = pd.DataFrame(columns=[
            "date","sport","league","game","market","selection","line",
            "prob","prob_decimal","confidence","rationale"
        ])
        empty.to_csv(OUT_PRED, index=False)
        print(f"predictions ok – 0 rows -> {OUT_PRED}")
        return

    # ======= “modelo” base (placeholder) =======
    # Score simple a partir de diferencias de forma y una leve anticipación al kickoff.
    # Si más adelante cargas modelos verdaderos, reemplaza esta sección por tu .pkl.
    home_adv = df["home_form"].astype(float) - df["away_form"].astype(float)
    # penaliza muy levemente eventos muy lejanos
    time_term = -0.01 * df["days_to_kickoff"].astype(float)
    score = 0.5 + 0.15 * home_adv + time_term
    score = _clip01(score)

    # Decisión
    pick_home_mask = score >= 0.5
    pick_team = np.where(pick_home_mask, df["home"], df["away"])
    prob = np.where(pick_home_mask, score, 1.0 - score)
    prob = _clip01(prob)

    # Ensamble de la tabla de predicciones
    game = df["home"].fillna("").astype(str) + " vs " + df["away"].fillna("").astype(str)
    out = pd.DataFrame({
        "date": df["date"].astype(str),
        "sport": df["sport"].astype(str),
        "league": df["league"].astype(str),
        "game": game,
        "market": "ML",            # Moneyline como mercado base
        "selection": pick_team.astype(str),
        "line": "",                # sin línea para ML
        "prob": np.round(prob.astype(float), 4),
    })

    out["prob_decimal"] = np.round(1.0 / out["prob"].astype(float), 6)
    out["confidence"] = out["prob"].apply(_bucket_confidence)
    out["rationale"] = (
        "Probabilidad del modelo basada en forma relativa y proximidad al evento."
    )

    # Orden y salida
    cols = ["date","sport","league","game","market","selection","line",
            "prob","prob_decimal","confidence","rationale"]
    out = out[cols].sort_values(["date","sport","league","game"], ignore_index=True)

    OUT_PRED.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PRED, index=False, encoding="utf-8")
    print(f"predictions ok – {len(out)} rows -> {OUT_PRED}")

if __name__ == "__main__":
    main()
