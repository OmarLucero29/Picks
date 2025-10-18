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
from gspread.exceptions import WorksheetNotFound, APIError

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

    txt = raw
    if not raw.strip().startswith("{"):
        try:
            txt = base64.b64decode(raw).decode("utf-8")
        except Exception:
            txt = raw

    data = json.loads(txt)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(data, scopes=scopes)

def _open_sheet():
    creds = _load_sa_credentials()
    gc = gspread.authorize(creds)
    return gc.open_by_key(GSHEET_ID)

# ---------- Utilidades ----------
def _uid_from_row(row: List[str]) -> str:
    h = hashlib.sha1(("||".join([str(x) for x in row])).encode("utf-8")).hexdigest()
    return h

def _ensure_columns_order(df: pd.DataFrame, existing_header: Optional[List[str]]) -> pd.DataFrame:
    if existing_header:
        ordered = [c for c in existing_header if c in df.columns]
        tail = [c for c in df.columns if c not in ordered]
        df = df[ordered + tail]
    return df

def _sheet_to_df(ws) -> pd.DataFrame:
    try:
        values = ws.get_all_values()
    except gspread.exceptions.APIError:
        return pd.DataFrame()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(rows, columns=header)

def _df_to_sheet(ws, df: pd.DataFrame):
    ws.clear()
    if df.empty:
        ws.update([[]])
        return
    df_str = df.fillna("").astype(str)
    data = [list(df_str.columns)] + df_str.values.tolist()
    ws.update(data, value_input_option="RAW")

def _dedup_df(df_existing: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    base = pd.concat([df_existing, df_new], ignore_index=True) if not df_existing.empty else df_new.copy()
    cols = [c.lower() for c in base.columns]
    if "id" in cols:
        idcol = base.columns[cols.index("id")]
        base = base.drop_duplicates(subset=[idcol], keep="first")
    else:
        uid = base.apply(lambda r: _uid_from_row([str(x) for x in r.values]), axis=1)
        base = base.assign(__uid=uid).drop_duplicates(subset=["__uid"], keep="first").drop(columns="__uid")
    return base.reset_index(drop=True)

def _add_id_if_missing(df: pd.DataFrame, key_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if df.empty:
        return df
    cols = [c.lower() for c in df.columns]
    if "id" in cols:
        return df
    if key_cols and all(k in df.columns for k in key_cols):
        base = df[key_cols].astype(str).agg("||".join, axis=1)
    else:
        base = df.astype(str).agg("||".join, axis=1)
    df = df.copy()
    df.insert(0, "ID", base.apply(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest()))
    return df

# ---------- Obtener/crear worksheet de forma robusta ----------
def _get_or_create_ws(sh, tab_name: str):
    target = tab_name.strip()
    # 1) Busca case-insensitive entre todas las hojas
    for ws in sh.worksheets():
        if ws.title.strip().lower() == target.lower():
            return ws
    # 2) Intenta crear; si API dice 'already exists', vuelve a buscar por nombre exacto
    try:
        return sh.add_worksheet(title=target, rows=1000, cols=30)
    except APIError as e:
        msg = str(e).lower()
        if "already exists" in msg or "already exists" in getattr(e, "response", {}).get("message", "").lower():
            try:
                return sh.worksheet(target)
            except WorksheetNotFound:
                # Último recurso: buscar por coincidencia parcial
                for ws in sh.worksheets():
                    if ws.title.strip().lower() == target.lower():
                        return ws
        raise

# ---------- Flujo por pestaña ----------
def _process_tab(sh, tab_name: str, csv_path: Path):
    if not csv_path.exists():
        print(f"{tab_name}: archivo no encontrado -> {csv_path}")
        return

    df_new = pd.read_csv(csv_path)
    if df_new.empty:
        print(f"{tab_name}: CSV vacío; nada que enviar")
        return

    df_new = _add_id_if_missing(df_new)

    ws = _get_or_create_ws(sh, tab_name)
    df_existing = _sheet_to_df(ws)
    header_existing = list(df_existing.columns) if not df_existing.empty else list(df_new.columns)

    df_new = _ensure_columns_order(df_new, header_existing)
    df_final = _dedup_df(df_existing, df_new)
    df_final = _ensure_columns_order(df_final, header_existing)

    _df_to_sheet(ws, df_final)
    print(f"{tab_name}: subidos {len(df_new)} nuevos, total {len(df_final)} (sin duplicados)")

def main():
    if not GSHEET_ID:
        print("GSHEET_ID faltante; omitiendo envío a Google Sheets.")
        return
    sh = _open_sheet()

    _process_tab(sh, TAB_PICKS, PICKS_CSV)
    _process_tab(sh, TAB_PARLAYS, PARLAY_CSV)

if __name__ == "__main__":
    main()
