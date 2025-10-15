# serving/select_picks.py — robusto, usa SOLO prob del MODELO y sin filtros
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timezone

DATA = Path("data/processed")
REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

MAX_PICKS = 5  # siempre intentamos 5

# Candidatos de columnas de prob. del MODELO (0..1)
PROB_CANDIDATES_SINGLE = [
    "prob_model","model_prob","pred_prob","win_prob","prob","p","prediction","yhat_proba"
]
# Candidatos cuando hay una fila por partido y prob para cada lado:
HOME_PROB_CANDS = ["p_home","prob_home","home_prob","home_win_prob","ph","win_home"]
AWAY_PROB_CANDS = ["p_away","prob_away","away_prob","away_win_prob","pa","win_away"]

def _read_preds():
    f = DATA / "predictions.csv"
    if not f.exists() or f.stat().st_size == 0:
        print("select_picks: predictions.csv no existe o vacío")
        return pd.DataFrame()
    df = pd.read_csv(f)
    # Limpieza básica
    df = df.replace([np.inf, -np.inf], np.nan)
    return df

def _clamp_prob(x):
    try:
        x = float(x)
    except Exception:
        return np.nan
    if x <= 0 or x >= 1:
        return np.nan
    return x

def _detect_single_prob_col(df):
    for c in PROB_CANDIDATES_SINGLE:
        if c in df.columns:
            p = df[c].apply(_clamp_prob)
            if p.notna().any():
                print(f"select_picks: usando prob col = '{c}'")
                return c
    return None

def _detect_dual_prob_cols(df):
    h = next((c for c in HOME_PROB_CANDS if c in df.columns), None)
    a = next((c for c in AWAY_PROB_CANDS if c in df.columns), None)
    if h and a:
        hp = df[h].apply(_clamp_prob)
        ap = df[a].apply(_clamp_prob)
        if hp.notna().any() or ap.notna().any():
            print(f"select_picks: usando prob dual = ('{h}','{a}')")
            return h, a
    return None, None

def _ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def _mk_game(home, away):
    home = home if isinstance(home,str) and home else "Home"
    away = away if isinstance(away,str) and away else "Away"
    return f"{away} @ {home}"

def _mk_date(dt_iso):
    # produce DD/MM/YYYY (MX) a partir de start_time_utc o fecha
    if pd.isna(dt_iso):
        return datetime.now(timezone.utc).strftime("%d/%m/%Y")
    try:
        ts = pd.to_datetime(dt_iso, utc=True)
        return ts.strftime("%d/%m/%Y")
    except Exception:
        return datetime.now(timezone.utc).strftime("%d/%m/%Y")

def build_long_from_dual(df, hcol, acol):
    # Requiere columnas 'home' y 'away' o similares
    home_name = None
    away_name = None
    for cand in ["home","home_team","team_home","local"]:
        if cand in df.columns: home_name = cand; break
    for cand in ["away","away_team","team_away","visitante","visitor"]:
        if cand in df.columns: away_name = cand; break

    # Si no hay columnas claras, intenta usar 'selection_home/selection_away' inexistentes → fallback
    if home_name is None or away_name is None:
        # Fallback: usa selection si existiera (no ideal)
        df["game"] = df.get("game", "")
        df["selection"] = df.get("selection", "")
        df["p"] = df[hcol].apply(_clamp_prob).fillna(df[acol].apply(_clamp_prob))
        return df

    # Unpivot a dos filas por partido (home y away)
    base_cols = [c for c in df.columns if c not in [hcol, acol]]
    home_rows = df[base_cols].copy()
    home_rows["selection"] = df[home_name]
    home_rows["p"] = df[hcol].apply(_clamp_prob)

    away_rows = df[base_cols].copy()
    away_rows["selection"] = df[away_name]
    away_rows["p"] = df[acol].apply(_clamp_prob)

    long_df = pd.concat([home_rows, away_rows], ignore_index=True)
    # game
    if "game" not in long_df.columns or long_df["game"].eq("").all():
        long_df["game"] = _mk_game(df.get(home_name), df.get(away_name))
        # _mk_game con Series no concatena bien; si quedó raro, recomputamos fila a fila
        if not isinstance(long_df["game"].iloc[0], str):
            long_df["game"] = [ _mk_game(h, a) for h,a in zip(df.get(home_name,""), df.get(away_name,"")) ] * 2
    return long_df

def main():
    raw = _read_preds()
    if raw.empty:
        (REPORTS/"picks.csv").write_text("")
        print("picks ok – 0 (predictions vacío)")
        return

    # Detecta formato
    single = _detect_single_prob_col(raw)
    if single:
        df = raw.copy()
        df["p"] = df[single].apply(_clamp_prob)

        # Si existe una única fila por partido sin selection, intenta inferir selección ganadora si hay 'winner' o 'pick'
        if "selection" not in df.columns or df["selection"].replace("", np.nan).isna().all():
            # intenta usar 'pick' o 'team'
            sel_cand = next((c for c in ["pick","team","pred_team","winner","side"] if c in df.columns), None)
            if sel_cand:
                df["selection"] = df[sel_cand].fillna("")
            # si aún falta y existen home/away + prob home/away separadas, reconvertimos
            hcol, acol = _detect_dual_prob_cols(raw)
            if hcol and acol:
                df = build_long_from_dual(raw, hcol, acol)

    else:
        # No hay prob simple; buscamos dual home/away
        hcol, acol = _detect_dual_prob_cols(raw)
        if not (hcol and acol):
            print("select_picks: no se detectó columna de probabilidad válida")
            (REPORTS/"picks.csv").write_text("")
            print("picks ok – 0")
            return
        df = build_long_from_dual(raw, hcol, acol)

    # Limpiar y asegurar columnas base
    df = df.dropna(subset=["p"])
    df = df[(df["p"] > 0) & (df["p"] < 1)].copy()
    df = _ensure_cols(df, ["date","sport","league","game","market","selection","line"])

    # market por defecto
    if df["market"].replace("", np.nan).isna().all():
        df["market"] = "ML"

    # game si faltó
    if df["game"].replace("", np.nan).isna().any():
        # intenta construir con home/away
        home = None; away = None
        for cand in ["home","home_team","team_home","local"]:
            if cand in df.columns: home = cand; break
        for cand in ["away","away_team","team_away","visitante","visitor"]:
            if cand in df.columns: away = cand; break
        if home and away:
            df.loc[:, "game"] = [ _mk_game(h, a) for h, a in zip(df[home], df[away]) ]
        else:
            df.loc[:, "game"] = df.get("game", "").fillna("")

    # date desde start_time_utc si existe
    if "date" not in df.columns or df["date"].replace("", np.nan).isna().any():
        src = "start_time_utc" if "start_time_utc" in df.columns else None
        if src:
            df["date"] = df[src].apply(_mk_date)
        else:
            df["date"] = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    # Ordenar por prob del modelo y tomar top-5 (sin filtros)
    df = df.sort_values("p", ascending=False).drop_duplicates(subset=["game","market","selection"], keep="first")
    top = df.head(MAX_PICKS).copy()

    if top.empty:
        (REPORTS/"picks.csv").write_text("")
        print("picks ok – 0")
        return

    top["prob"] = top["p"].round(2)
    top["prob_decimal_odds"] = (1.0 / top["p"].clip(1e-6, 1-1e-6)).round(3)
    top["confidence"] = np.where(top["prob"]>=0.64,"Alta", np.where(top["prob"]>=0.58,"Media","Baja"))
    top["rationale"] = "Predicción 100% modelo (sin odds), con forma y ajustes."

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
