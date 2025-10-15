# serving/select_picks.py — usa SOLO la prob del modelo, sin filtros
from pathlib import Path
import pandas as pd
import numpy as np

DATA = Path("data/processed")
REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

MAX_PICKS = 5  # siempre 5 picks

def load_preds():
    f = DATA / "predictions.csv"
    if not f.exists() or f.stat().st_size == 0:
        print("picks ok – 0 (no hay predictions.csv)")
        (REPORTS/"picks.csv").write_text("")
        return pd.DataFrame()
    df = pd.read_csv(f)

    # Columna de probabilidad del MODELO (sin odds). Prioriza 'prob_model' si existe.
    if "prob_model" in df.columns:
        df["p"] = df["prob_model"].astype(float)
    elif "prob" in df.columns:
        df["p"] = df["prob"].astype(float)
    else:
        df["p"] = np.nan

    # Normaliza columnas mínimas
    for c in ["date","sport","league","game","market","selection","line"]:
        if c not in df.columns:
            df[c] = ""
    df = df.dropna(subset=["p"])
    return df

def main():
    df = load_preds()
    if df.empty:
        return

    # NO filtros: ordenar por prob desc y tomar TOP-5 (evitando duplicar el mismo juego)
    df = df.sort_values("p", ascending=False).copy()
    if "game" in df.columns:
        df = df.drop_duplicates(subset=["game","market","selection"], keep="first")

    top = df.head(MAX_PICKS).copy()

    # Campos de salida
    top["prob"] = top["p"].round(2)
    top["prob_decimal_odds"] = (1.0 / top["p"].clip(1e-6, 1-1e-6)).round(3)
    top["confidence"] = np.where(top["prob"]>=0.64,"Alta",np.where(top["prob"]>=0.58,"Media","Baja"))
    top["rationale"] = (
        "Predicción basada en el modelo (datos recientes, forma y ajustes)."
    )

    cols = ["date","sport","league","game","market","selection","line",
            "prob","prob_decimal_odds","confidence","rationale"]
    for c in cols:
        if c not in top.columns: top[c] = ""
    top = top[cols]

    out = REPORTS/"picks.csv"
    top.to_csv(out, index=False)
    print(f"picks ok – {len(top)} -> {out}")

if __name__ == "__main__":
    main()