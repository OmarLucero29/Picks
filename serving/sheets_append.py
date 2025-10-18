# serving/sheets_append.py
import os
import io
import json
import base64
import hashlib
from pathlib import Path
from typing import List, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
GSHEET_ID = os.getenv("GSHEET_ID", "")
TAB_PICKS = os.getenv("GSHEET_PICKS_TAB", "PICKS")
TAB_PARLAYS = os.getenv("GSHEET_PARLAY_TAB", "PARLAYS")

PICKS_CSV = Path("reports/picks.csv")
PARLAY_CSV = Path("reports/parlay.csv")

# ---------- Auth ----------
def _load_sa_credentials():
    raw = os.getenv("GCP_SA_JSON", "")
    if not raw:
        raise RuntimeError("GCP_SA_JSON faltante (Service Account JSON)")

    # Permitir que el secreto venga como JSON plano o base64
    txt = raw
    if not raw.strip().startswith("{"):
        try:
            txt = base64.b64decode(raw).decode("utf-8")
        except Exception:
            # si no es base64 válido, asumimos que ya es texto JSON
            txt = raw

    data = json.loads(txt)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    return creds

def _open_sheet():
    creds = _load_sa_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)
    return sh

# ---------- Utilidades ----------
def _uid_from_row(row: List[str]) -> str:
    # Hash determinista de toda la fila (para dedup si no hay ID)
    h = hashlib.sha1(("||".join([str(x) for x in row])).encode("utf-8")).hexdigest()
    return h

def _ensure_columns_order(df: pd.DataFrame, existing_header: Optional[List[str]]) -> pd.DataFrame:
    if existing_header:
        # Reordena columnas según la cabecera existente y agrega nuevas al final
        ordered = [c for c in existing_header if c in df.columns]
        tail = [c for c in df.columns if c not in ordered]
        df = df[ordered + tail]
    return df

def _sheet_to_df(worksheet) -> pd.DataFrame:
    try:
        values = worksheet.get_all_values()
    except gspread.exceptions.APIError:
        return pd.DataFrame()

    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(rows, columns=header)

def _df_to_sheet(worksheet, df: pd.DataFrame):
    # Limpia hoja y escribe todo de nuevo (garantiza no duplicados)
    worksheet.clear()
    if df.empty:
        worksheet.update([[]])  # deja hoja vacía
        return

    # Convierte todo a strings (Google Sheets-friendly)
    df_str = df.fillna("").astype(str)
    data = [list(df_str.columns)] + df_str.values.tolist()
    worksheet.update(data, value_input_option="RAW")

def _dedup_df(df_existing: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    if df_existing is None or df_existing.empty:
        base = df_new.copy()
    else:
        base = pd.concat([df_existing, df_new], ignore_index=True)

    cols = [c.lower() for c in base.columns]
    has_id = "id" in cols
    if has_id:
        # Normaliza por si viene 'ID' o 'id'
        idcol = base.columns[cols.index("id")]
        base = base.drop_duplicates(subset=[idcol], keep="first")
    else:
        # Genera UID por fila
        uid = base.apply(lambda r: _uid_from_row([str(x) for x in r.values]), axis=1)
        base = base.assign(__uid=uid).drop_duplicates(subset=["__uid"], keep="first").drop(columns="__uid")

    base.reset_index(drop=True, inplace=True)
    return base

# ---------- Normalización opcional (si tus CSV no traen ID) ----------
def _add_id_if_missing(df: pd.DataFrame, key_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if df.empty:
        return df
    cols = [c.lower() for c in df.columns]
    if "id" in cols:
        return df

    # Crea un ID a partir de columnas relevantes o de toda la fila
    if key_cols and all(k in df.columns for k in key_cols):
        base = df[key_cols].astype(str).agg("||".join, axis=1)
    else:
        base = df.astype(str).agg("||".join, axis=1)
    df = df.copy()
    df.insert(0, "ID", base.apply(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()))
    return df

# ---------- Flujo principal ----------
def _process_tab(sh, tab_name: str, csv_path: Path):
    # Si no hay archivo CSV (p.ej., parlay sin filas), salimos sin error
    if not csv_path.exists():
        print(f"{tab_name}: archivo no encontrado -> {csv_path}")
        return

    df_new = pd.read_csv(csv_path)
    if df_new.empty:
        print(f"{tab_name}: CSV vacío; nada que enviar")
        return

    # Asegura ID si hace falta
    df_new = _add_id_if_missing(df_new)

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        # Crea la pestaña si no existe
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=30)
        ws.update([list(df_new.columns)])  # cabecera inicial

    df_existing = _sheet_to_df(ws)
    header_existing = list(df_existing.columns) if not df_existing.empty else list(df_new.columns)

    # Ordena df_new según cabecera existente (si la hay)
    df_new = _ensure_columns_order(df_new, header_existing)

    # Deduplicar y escribir
    df_final = _dedup_df(df_existing, df_new)
    df_final = _ensure_columns_order(df_final, header_existing)
    _df_to_sheet(ws, df_final)

    print(f"{tab_name}: subidos {len(df_new)} nuevos, total {len(df_final)} (sin duplicados)")

def main():
    if not GSHEET_ID:
        print("GSHEET_ID faltante; omitiendo envío a Google Sheets.")
        return
    sh = _open_sheet()

    # Pestaña de picks
    _process_tab(sh, TAB_PICKS, PICKS_CSV)

    # Pestaña de parlays
    _process_tab(sh, TAB_PARLAYS, PARLAY_CSV)

if __name__ == "__main__":
    main()
