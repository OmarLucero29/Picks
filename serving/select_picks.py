# serving/select_picks.py — genera TODOS los picks (ordenados) y TOP-M
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os

DATA = Path("data/processed")
REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

MAX_PICKS = int(os.environ.get("MAX_PICKS", "5"))  # Top-M configurable

# Heurísticas de nombres de prob del MODELO
PROB_SINGLE = ["prob_model","model_prob","pred_prob","win_prob","prob","p","prediction","yhat_proba","proba"]
HOME_PROB  = ["p_home","prob_home","home_prob","home_win_prob","ph","win_home","proba_home"]
AWAY_PROB  = ["p_away","prob_away","away_prob","away_win_prob","pa","win_away","proba_away"]

def _read_preds():
    f = DATA / "predictions.csv"
    if not f.exists() or f.stat().st_size == 0:
        print("select_picks: predictions.csv no existe o vacío")
        return pd.DataFrame()
    df = pd.read_csv(f).replace([np.inf,-np.inf], np.nan)
    print(f"select_picks: cols => {list(df.columns)}")
    return df

def _clamp01(x):
    try: x = float(x)
    except Exception: return np.nan
    return x if 0 < x < 1 else np.nan

def _detect_single_prob(df):
    for c in PROB_SINGLE:
        if c in df.columns:
            p = df[c].apply(_clamp01)
            if p.notna().sum() >= 1:
                print(f"select_picks: usando prob única = '{c}' (válidas={int(p.notna().sum())})")
                return c
    return None

def _detect_dual_prob(df):
    h = next((c for c in HOME_PROB if c in df.columns), None)
    a = next((c for c in AWAY_PROB if c in df.columns), None)
    if h and a:
        hp = df[h].apply(_clamp01); ap = df[a].apply(_clamp01)
        if max(hp.notna().sum(), ap.notna().sum()) >= 1:
            print(f"select_picks: usando prob dual = ('{h}','{a}')")
            return h, a
    return None, None

def _best_numeric_prob(df):
    # Busca cualquier columna numérica en (0,1) con más cobertura
    best, best_cnt = None, 0
    for c in df.columns:
        if df[c].dtype.kind in "fc":
            p = df[c].apply(_clamp01)
            cnt = p.notna().sum()
            if cnt > best_cnt:
                best, best_cnt = c, cnt
    if best:
        print(f"select_picks: mejor numérica en (0,1) => '{best}' (válidas={best_cnt})")
    return best

def _normalize_from_score(df):
    if "score" in df.columns and df["score"].notna().sum() >= 1:
        s = df["score"].astype(float)
        mn, mx = s.min(), s.max()
        if mx > mn:
            p = 0.05 + 0.9 * (s - mn) / (mx - mn)
            print("select_picks: usando 'score' normalizado como prob (fallback)")
            return p
    return pd.Series(dtype=float)

def _ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns: df[c] = ""
    return df

def _mk_game(home, away):
    home = home if isinstance(home,str) and home else "Home"
    away = away if isinstance(away,str) and away else "Away"
    return f"{away} @ {home}"

def _mk_date(dt_iso):
    try: ts = pd.to_datetime(dt_iso, utc=True)
    except Exception: ts = datetime.now(timezone.utc)
    return ts.strftime("%d/%m/%Y")

def _build_long_from_dual(raw, hcol, acol):
    hname = next((c for c in ["home","home_team","team_home","local"] if c in raw.columns), None)
    aname = next((c for c in ["away","away_team","team_away","visitante","visitor"] if c in raw.columns), None)
    base = [c for c in raw.columns if c not in [hcol, acol]]
    home_rows = raw[base].copy(); away_rows = raw[base].copy()

    if hname and aname:
        home_rows["selection"] = raw[hname]; away_rows["selection"] = raw[aname]
        game_series = [ _mk_game(h,a) for h,a in zip(raw[hname], raw[aname]) ]
    else:
        home_rows["selection"] = "Home"; away_rows["selection"] = "Away"
        game_series = raw.get("game", pd.Series([""]*len(raw))).tolist()

    home_rows["p"] = raw[hcol].apply(_clamp01)
    away_rows["p"] = raw[acol].apply(_clamp01)

    long_df = pd.concat([home_rows, away_rows], ignore_index=True)
    if "game" not in long_df.columns or long_df["game"].replace("", np.nan).isna().any():
        long_df["game"] = game_series + game_series
    return long_df

def build_all_picks():
    raw = _read_preds()
    if raw.empty: return pd.DataFrame()

    # 1) single?
    single = _detect_single_prob(raw)
    if single:
        df = raw.copy(); df["p"] = df[single].apply(_clamp01)
    else:
        # 2) dual?
        hcol, acol = _detect_dual_prob(raw)
        if hcol and acol:
            df = _build_long_from_dual(raw, hcol, acol)
        else:
            # 3) mejor numérica (0,1)
            best = _best_numeric_prob(raw)
            if best:
                df = raw.copy(); df["p"] = df[best].apply(_clamp01)
            else:
                # 4) fallback: score
                pscore = _normalize_from_score(raw)
                if pscore.empty or pscore.notna().sum() < 1:
                    print("select_picks: no hay columna de prob ni score util → 0")
                    return pd.DataFrame()
                df = raw.copy(); df["p"] = pscore

    # limpiar
    df = df.dropna(subset=["p"])
    df = df[(df["p"] > 0) & (df["p"] < 1)].copy()

    # columnas base
    df = _ensure_cols(df, ["date","sport","league","game","market","selection","line"])
    if df["market"].replace("", np.nan).isna().all():
        df["market"] = "ML"

    # game si falta y tenemos home/away
    if df["game"].replace("", np.nan).isna().any():
        hname = next((c for c in ["home","home_team","team_home","local"] if c in df.columns), None)
        aname = next((c for c in ["away","away_team","team_away","visitante","visitor"] if c in df.columns), None)
        if hname and aname:
            df["game"] = [ _mk_game(h,a) for h,a in zip(df[hname], df[aname]) ]
        else:
            df["game"] = df["game"].fillna("")

    # fecha preferente desde start_time_utc
    if "start_time_utc" in df.columns:
        df["date"] = df["start_time_utc"].apply(_mk_date)
    else:
        df["date"] = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    # métricas derivadas
    df["prob"] = df["p"].round(2)
    df["prob_decimal_odds"] = (1.0 / df["p"].clip(1e-6, 1-1e-6)).round(3)
    df["confidence"] = np.where(df["prob"]>=0.64,"Alta", np.where(df["prob"]>=0.58,"Media","Baja"))
    df["rationale"] = "Predicción 100% del modelo (sin odds)."

    # ordenar por prob desc y deduplicar selecciones idénticas del mismo juego
    df = df.sort_values("p", ascending=False).drop_duplicates(subset=["game","market","selection"], keep="first")

    # columnas finales
    cols = ["date","sport","league","game","market","selection","line",
            "prob","prob_decimal_odds","confidence","rationale"]
    for c in cols:
        if c not in df.columns: df[c] = ""
    return df[cols]

def main():
    all_df = build_all_picks()
    if all_df.empty:
        (REPORTS/"all_picks.csv").write_text("")
        (REPORTS/"picks.csv").write_text("")
        print("picks ok – 0 (no hay candidatos)")
        return

    # escribe TODOS
    all_out = REPORTS/"all_picks.csv"
    all_df.to_csv(all_out, index=False)
    print(f"all_picks ok – {len(all_df)} -> {all_out}")

    # TOP-M
    top = all_df.head(MAX_PICKS).copy()
    top_out = REPORTS/"picks.csv"
    top.to_csv(top_out, index=False)
    print(f"picks ok – {len(top)} -> {top_out}")

if __name__ == "__main__":
    main()
