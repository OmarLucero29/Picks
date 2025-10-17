# serving/select_picks.py — genera all_picks + picks (Top-M) con campos normalizados
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import re
import hashlib

DATA = Path("data/processed")
REPORTS = Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

MAX_PICKS = int(os.environ.get("MAX_PICKS", "5"))  # Top-M configurable
DEFAULT_STAKE = os.environ.get("STAKE_DEFAULT", "5%")  # para picks

PROB_SINGLE = ["prob_model","model_prob","pred_prob","win_prob","prob","p","prediction","yhat_proba","proba"]
HOME_PROB  = ["p_home","prob_home","home_prob","home_win_prob","ph","win_home","proba_home"]
AWAY_PROB  = ["p_away","prob_away","away_prob","away_win_prob","pa","win_away","proba_away"]

HOME_NAMES = ["home","home_team","team_home","local"]
AWAY_NAMES = ["away","away_team","team_away","visitante","visitor"]

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

def _get_col(df, cands): return next((c for c in cands if c in df.columns), None)

def _mk_game(home, away):
    home = home if isinstance(home,str) and home else "Home"
    away = away if isinstance(away,str) and away else "Away"
    return f"{away} @ {home}"

def _split_from_game(game):
    if not isinstance(game,str): return None, None
    g = game.strip()
    if "@" in g:
        parts = [p.strip() for p in g.split("@", 1)]
        if len(parts)==2: return parts[0], parts[1]
    if " vs " in g.lower():
        m = re.split(r"\s+vs\s+", g, flags=re.IGNORECASE)
        if len(m)==2: return m[0].strip(), m[1].strip()
    return None, None

def _ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns: df[c] = ""
    return df

def _build_long_from_dual(raw, hcol, acol):
    hname = _get_col(raw, HOME_NAMES)
    aname = _get_col(raw, AWAY_NAMES)
    base = [c for c in raw.columns if c not in [hcol, acol]]

    home_rows = raw[base].copy(); away_rows = raw[base].copy()
    if hname and aname:
        home_rows["selection"] = raw[hname]
        away_rows["selection"] = raw[aname]
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

def _infer_selection_single(df):
    if "selection" in df.columns and df["selection"].replace("",np.nan).notna().any():
        return df
    hname = _get_col(df, HOME_NAMES); aname = _get_col(df, AWAY_NAMES)
    if hname and aname:
        choice_is_home = df["p"] >= 0.5
        df.loc[choice_is_home, "selection"] = df.loc[choice_is_home, hname]
        df.loc[~choice_is_home, "selection"] = df.loc[~choice_is_home, aname]
        return df
    if "game" in df.columns:
        away_list, home_list = [], []
        for g in df["game"].fillna(""):
            a, h = _split_from_game(g); away_list.append(a); home_list.append(h)
        away_s = pd.Series(away_list); home_s = pd.Series(home_list)
        mask = away_s.notna() & home_s.notna()
        if mask.any():
            choice_is_home = df["p"] >= 0.5
            sel = np.where(choice_is_home, home_s, away_s)
            df.loc[mask, "selection"] = sel[mask]
    if "selection" not in df.columns or df["selection"].replace("",np.nan).isna().any():
        df["selection"] = df.get("selection","")
        df.loc[df["selection"].replace("",np.nan).isna(), "selection"] = np.where(df["p"]>=0.5, "Home", "Away")
    return df

def _date_local_from(df):
    # preferente start_time_utc → FECHA local (sin hora)
    if "start_time_utc" in df.columns:
        try:
            dt = pd.to_datetime(df["start_time_utc"], utc=True, errors="coerce")
            return dt.dt.tz_convert("America/Mexico_City").dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    return datetime.now(timezone.utc).astimezone().strftime("%d/%m/%Y")

def _sport_from(df):
    return df.get("sport","").fillna("")

def _league_from(df):
    return df.get("league","").fillna("")

def build_all_picks():
    raw = _read_preds()
    if raw.empty: return pd.DataFrame()

    single = _detect_single_prob(raw)
    if single:
        df = raw.copy(); df["p"] = df[single].apply(_clamp01)
        df = df.dropna(subset=["p"])
        df = _infer_selection_single(df)
    else:
        hcol, acol = _detect_dual_prob(raw)
        if hcol and acol:
            df = _build_long_from_dual(raw, hcol, acol)
        else:
            best = _best_numeric_prob(raw)
            if best:
                df = raw.copy(); df["p"] = df[best].apply(_clamp01)
                df = df.dropna(subset=["p"])
                df = _infer_selection_single(df)
            else:
                pscore = _normalize_from_score(raw)
                if pscore.empty or pscore.notna().sum() < 1:
                    print("select_picks: no hay prob/score util → 0")
                    return pd.DataFrame()
                df = raw.copy(); df["p"] = pscore
                df = _infer_selection_single(df)

    df = df.dropna(subset=["p"])
    df = df[(df["p"] > 0) & (df["p"] < 1)].copy()

    # columnas base
    for c in ["sport","league","game","market","selection"]:
        if c not in df.columns: df[c] = ""

    # FECHA (solo fecha local)
    date_local = _date_local_from(df)
    df["FECHA"] = date_local if isinstance(date_local,str) else date_local

    # DEPORTE
    df["DEPORTE"] = _sport_from(df)
    # PARTIDO
    if df["game"].replace("", np.nan).isna().any():
        hname = _get_col(df, HOME_NAMES); aname = _get_col(df, AWAY_NAMES)
        if hname and aname:
            df["game"] = [ _mk_game(h,a) for h,a in zip(df[hname], df[aname]) ]
    df["PARTIDO"] = df["game"]
    # MERCADO
    df["MERCADO"] = df["market"].replace({"Moneyline":"ML","moneyline":"ML"}).fillna("ML")
    # PICK
    df["PICK"] = df["selection"].fillna("")

    # CUOTA (PROB %) — cuota derivada del modelo: 1/p
    dec = (1.0 / df["p"].clip(1e-6, 1-1e-6)).round(2)
    perc = (df["p"]*100).round(0).astype(int)
    df["CUOTA (PROB %)"] = [f"{d:.2f} ({pp}%)" for d,pp in zip(dec, perc)]

    # STAKE (por defecto)
    df["STAKE"] = DEFAULT_STAKE

    # ID estable (día + hash corto del partido/mercado/pick)
    def _mk_id(row):
        base = f"{row.get('FECHA','')}|{row.get('DEPORTE','')}|{row.get('PARTIDO','')}|{row.get('MERCADO','')}|{row.get('PICK','')}"
        h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:6].upper()
        # prefijo P + yyyymmdd
        try:
            ymd = datetime.strptime(row.get("FECHA",""), "%d/%m/%Y").strftime("%Y%m%d")
        except Exception:
            ymd = datetime.now().strftime("%Y%m%d")
        return f"P{ymd}-{h}"
    df["ID"] = df.apply(_mk_id, axis=1)

    # ordenar por prob desc y deduplicar
    df = df.sort_values("p", ascending=False).drop_duplicates(subset=["ID"], keep="first")

    # columnas finales (para Sheets)
    final_cols = ["ID","FECHA","DEPORTE","PARTIDO","MERCADO","PICK","CUOTA (PROB %)","STAKE"]
    return df[final_cols], df

def main():
    all_df, _raw = build_all_picks()
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