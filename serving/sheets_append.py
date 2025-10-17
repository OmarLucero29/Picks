# serving/sheets_append.py — upsert a Sheets con nuevas columnas sencillas
import os, json, base64, hashlib
from pathlib import Path
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

REPORTS = Path("reports")

SHEET_ID = os.getenv("GSHEET_ID", "")
TAB_PICKS = os.getenv("GSHEET_PICKS_TAB", "PICKS")
TAB_PARLAY = os.getenv("GSHEET_PARLAY_TAB", "PARLAYS")
TAB_SAVED  = os.getenv("GSHEET_SAVED_TAB", "PICKS_GUARDADOS")  # para el botón "Avísame"

SA_JSON = os.getenv("GCP_SA_JSON", "")

# Columnas estándar pedidas
PICKS_COLS  = ["ID","FECHA","DEPORTE","PARTIDO","MERCADO","PICK","CUOTA (PROB %)","STAKE"]
PARLAY_COLS = ["ID","TIPO","FECHA","DEPORTE","PARTIDO","MERCADO","PICK","CUOTA (PROB %)","STAKE"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def gclient():
    if not SA_JSON: raise RuntimeError("GCP_SA_JSON faltante")
    try:
        data = json.loads(SA_JSON)
    except json.JSONDecodeError:
        data = json.loads(base64.b64decode(SA_JSON).decode("utf-8"))
    creds = Credentials.from_service_account_info(data, scopes=SCOPES)
    return gspread.authorize(creds)

def row_hash(row, cols):
    s = "|".join(str(row.get(c,"")) for c in cols)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns: df[c] = pd.NA
    return df[cols]

def upsert(ws, df: pd.DataFrame, key_cols):
    try:
        existing = get_as_dataframe(ws, header=0).dropna(how="all")
    except Exception:
        existing = pd.DataFrame(columns=df.columns)
    df = df.copy()
    df["_row_key"] = df.apply(lambda r: row_hash(r, key_cols), axis=1)
    if "_row_key" not in existing.columns:
        existing["_row_key"] = pd.NA
    merged = pd.concat([existing, df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["_row_key"], keep="last")
    # Mostrar columnas visibles + _row_key al final
    vis = [c for c in df.columns if c != "_row_key"] + ["_row_key"]
    ws.clear()
    set_with_dataframe(ws, merged[vis], include_index=False, include_column_header=True, allow_formulas=False)

def load_csv(path: Path, cols):
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=cols)
    df = pd.read_csv(path)
    return ensure_cols(df, cols)

def main():
    gc = gclient()
    sh = gc.open_by_key(SHEET_ID)

    # PICKS (todos)
    picks_df = load_csv(REPORTS/"all_picks.csv", PICKS_COLS)
    if not picks_df.empty:
        try:
            ws = sh.worksheet(TAB_PICKS)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=TAB_PICKS, rows=5000, cols=max(10,len(PICKS_COLS)+1))
        upsert(ws, picks_df, key_cols=["ID"])
        print(f"sheets: upsert {len(picks_df)} filas -> {TAB_PICKS}")
    else:
        print("sheets: no hay all_picks.csv")

    # PARLAYS (Segurito + Soñadora)
    parlay_df = load_csv(REPORTS/"parlay.csv", PARLAY_COLS)
    if not parlay_df.empty:
        try:
            ws2 = sh.worksheet(TAB_PARLAY)
        except gspread.WorksheetNotFound:
            ws2 = sh.add_worksheet(title=TAB_PARLAY, rows=5000, cols=max(10,len(PARLAY_COLS)+1))
        # Dedupe por (ID + PICK) para no duplicar legs del mismo parlay
        upsert(ws2, parlay_df, key_cols=["ID","PICK","PARTIDO","MERCADO"])
        print(f"sheets: upsert {len(parlay_df)} filas -> {TAB_PARLAY}")
    else:
        print("sheets: no hay parlay.csv")

    # NOTA: TAB_SAVED (PICKS_GUARDADOS) se llenará desde el bot con el botón "Avísame".

if __name__ == "__main__":
    main()