# serving/sheets_append.py
import os, json, base64
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

REPORTS = Path("reports")

SHEET_ID = os.environ.get("GSHEET_ID", "")
TAB_PICKS = os.environ.get("GSHEET_PICKS_TAB", "Picks")
TAB_PARLAY = os.environ.get("GSHEET_PARLAY_TAB", "Parlay")
SA_JSON = os.environ.get("GCP_SA_JSON", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def load_service_account():
    if not SA_JSON:
        raise RuntimeError("GCP_SA_JSON faltante")
    # Acepta JSON plano o base64
    try:
        data = json.loads(SA_JSON)
    except json.JSONDecodeError:
        data = json.loads(base64.b64decode(SA_JSON).decode("utf-8"))
    creds = Credentials.from_service_account_info(data, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc

def append_df(ws, df: pd.DataFrame, dedup_cols=None):
    """
    Anexa df al final. Si la hoja está vacía, escribe encabezados.
    Si dedup_cols, hace merge con datos existentes y elimina duplicados conservando lo más reciente.
    """
    # Lee existente (si hay)
    try:
        existing = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    except Exception:
        existing = pd.DataFrame()

    # Limpia columnas vacías adicionales
    if isinstance(existing, pd.DataFrame):
        existing = existing.dropna(how="all")
        # Alinea columnas si existen encabezados
        if not existing.empty:
            # normaliza tipos a str para evitar problemas
            existing.columns = [str(c) for c in existing.columns]

    if existing is None or existing.empty:
        # Hoja vacía: escribe encabezados + df
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, allow_formulas=False)
        return

    # Alinea columnas
    for c in df.columns:
        if c not in existing.columns:
            existing[c] = pd.NA
    for c in existing.columns:
        if c not in df.columns:
            df[c] = pd.NA

    # Concat y dedup
    merged = pd.concat([existing[existing.columns], df[existing.columns]], ignore_index=True)
    if dedup_cols:
        merged = merged.drop_duplicates(subset=dedup_cols, keep="last")
    # Reescribe todo (simple y robusto)
    ws.clear()
    set_with_dataframe(ws, merged, include_index=False, include_column_header=True, allow_formulas=False)

def load_csv(path: Path, required_cols=None):
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=required_cols or [])
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=required_cols or [])
    if required_cols:
        # Asegura todas las columnas
        for c in required_cols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[required_cols]
    return df

def main():
    # Definir columnas estándar
    cols_picks = ["date","sport","league","game","market","selection","line","prob","prob_decimal_odds","confidence","rationale"]
    cols_parlay = ["date","sport","league","game","market","selection","line","prob","prob_decimal_odds","confidence","rationale","parlay_prob","parlay_decimal_odds","note"]

    picks_df = load_csv(REPORTS/"picks.csv", required_cols=cols_picks)
    parlay_df = load_csv(REPORTS/"parlay.csv", required_cols=cols_parlay)

    # Si no hay nada, no hacemos nada
    if picks_df.empty and parlay_df.empty:
        print("sheets: nada que enviar (picks y parlay vacíos)")
        return

    gc = load_service_account()
    sh = gc.open_by_key(SHEET_ID)

    # Picks
    if not picks_df.empty:
        try:
            ws = sh.worksheet(TAB_PICKS)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=TAB_PICKS, rows=1000, cols=max(10, len(cols_picks)))
        # deduplicar por combinación clave (fecha+partido+mercado+selección)
        dedup_cols = ["date","game","market","selection"]
        append_df(ws, picks_df, dedup_cols=dedup_cols)
        print(f"sheets: picks enviados ({len(picks_df)} filas) -> {TAB_PICKS}")

    # Parlay
    if not parlay_df.empty:
        try:
            ws2 = sh.worksheet(TAB_PARLAY)
        except gspread.WorksheetNotFound:
            ws2 = sh.add_worksheet(title=TAB_PARLAY, rows=1000, cols=max(10, len(cols_parlay)))
        # dedup simple por (date, selection, market) para no repetir
        dedup_cols2 = ["date","selection","market"]
        append_df(ws2, parlay_df, dedup_cols=dedup_cols2)
        print(f"sheets: parlay enviado ({len(parlay_df)} filas) -> {TAB_PARLAY}")

if __name__ == "__main__":
    main()