# -*- coding: utf-8 -*-
"""
Append a Google Sheet with new rows from CSVs, avoiding duplicates by ID (col A).
Secrets (repo/org secrets):
  - GSHEET_ID
  - GCP_SA_JSON
  - GSHEET_PICKS_TAB      (default: PICKS)
  - GSHEET_PARLAY_TAB     (default: PARLAYS)
  - GSHEET_GUARDADOS      (default: GUARDADOS)
CLI:
  python serving/sheets_append.py
  python serving/sheets_append.py --overwrite   # borra todo y reescribe
  python serving/sheets_append.py --reset       # alias de --overwrite
"""

import os
import sys
import csv
import json
import argparse
from pathlib import Path
from typing import List, Set, Tuple

# ---------- Config por defecto ----------
DEFAULT_TABS = {
    "picks": os.getenv("GSHEET_PICKS_TAB", "PICKS"),
    "parlays": os.getenv("GSHEET_PARLAY_TAB", "PARLAYS"),
    "guardados": os.getenv("GSHEET_GUARDADOS", "GUARDADOS"),
}
REPORTS_DIR = Path("reports")
CSV_MAP = {
    "picks": REPORTS_DIR / "picks.csv",
    "parlays": REPORTS_DIR / "parlay.csv",
    "guardados": REPORTS_DIR / "guardados.csv",
}
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------- Helpers de Google Sheets ----------
def _ws_connect(spreadsheet_id: str):
    import gspread
    from google.oauth2.service_account import Credentials
    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Falta GCP_SA_JSON en variables de entorno.")
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPE)
    return gspread.authorize(creds).open_by_key(spreadsheet_id)

def _get_or_create_ws(sh, title: str):
    import gspread
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"[create] creando pestaña '{title}'…")
        return sh.add_worksheet(title=title, rows=2000, cols=50)
    except gspread.exceptions.APIError as e:
        if "already exists" in str(e):
            print(f"[info] pestaña '{title}' ya existía; usando la existente")
            return sh.worksheet(title)
        raise

def _ensure_headers(ws, headers: List[str]):
    # Si la fila 1 está vacía o diferente, escribe encabezados
    current = ws.row_values(1)
    if not current:
        ws.update("A1", [headers])
        return headers
    # Alinear número de columnas si hacen falta
    if len(current) < len(headers):
        current = current + [""] * (len(headers) - len(current))
        ws.update("A1", [current])
    return current[: len(headers)]

def _existing_ids(ws, id_col: int = 1) -> Set[str]:
    try:
        col = ws.col_values(id_col)
        return set(x for x in col[1:] if x)  # sin encabezado
    except Exception:
        return set()

def _read_csv_rows(csv_path: Path, has_header: bool = True) -> Tuple[List[str], List[List[str]]]:
    if not csv_path.exists():
        print(f"[skip] no existe {csv_path}")
        return [], []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        print(f"[skip] {csv_path} vacío")
        return [], []
    headers = rows[0] if has_header else None
    data = rows[1:] if has_header else rows
    return (headers or []), data

def _append_dedup(ws, headers: List[str], rows: List[List[str]], overwrite: bool = False) -> int:
    if overwrite:
        # borra todas las filas excepto encabezados
        nrows = ws.row_count
        if nrows > 1:
            ws.delete_rows(2, nrows)
        _ensure_headers(ws, headers)
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        return len(rows)

    # Anti-duplicados por ID (col A)
    existing = _existing_ids(ws)
    new_rows = [r for r in rows if r and r[0] and r[0] not in existing]
    if not new_rows:
        print("[ok] Sin nuevos registros (todos duplicados).")
        return 0
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    return len(new_rows)

# ---------- Pipeline ----------
PICKS_HEADERS   = ["id","fecha","deporte","evento","mercado","seleccion","cuota_prob"]
PARLAYS_HEADERS = ["id","tipo","fecha","deporte","evento","mercado","seleccion","cuota_prob"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--overwrite", action="store_true", help="borra y recarga completamente cada pestaña")
    parser.add_argument("--reset", action="store_true", help="alias de --overwrite")
    args = parser.parse_args()
    overwrite = bool(args.overwrite or args.reset)

    spreadsheet_id = os.getenv("GSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Falta GSHEET_ID en variables de entorno.")

    sh = _ws_connect(spreadsheet_id)

    total = 0

    # ---------- PICKS ----------
    ws_picks = _get_or_create_ws(sh, DEFAULT_TABS["picks"])
    headers, data = _read_csv_rows(CSV_MAP["picks"])
    # Si el CSV no trae encabezados, usamos los esperados
    headers = headers or PICKS_HEADERS
    _ensure_headers(ws_picks, headers)
    total += _append_dedup(ws_picks, headers, data, overwrite=overwrite)
    print(f"[picks] agregadas {total} filas acumuladas")

    # ---------- PARLAYS ----------
    ws_parlays = _get_or_create_ws(sh, DEFAULT_TABS["parlays"])
    headers_p, data_p = _read_csv_rows(CSV_MAP["parlays"])
    headers_p = headers_p or PARLAYS_HEADERS
    _ensure_headers(ws_parlays, headers_p)
    added_p = _append_dedup(ws_parlays, headers_p, data_p, overwrite=overwrite)
    total += added_p
    print(f"[parlays] +{added_p} (total {total})")

    # ---------- GUARDADOS (opcional) ----------
    ws_guard = _get_or_create_ws(sh, DEFAULT_TABS["guardados"])
    headers_g, data_g = _read_csv_rows(CSV_MAP["guardados"])
    if headers_g or data_g:
        # si no hay archivo/filas, simplemente no hacemos nada
        headers_g = headers_g or PICKS_HEADERS
        _ensure_headers(ws_guard, headers_g)
        added_g = _append_dedup(ws_guard, headers_g, data_g, overwrite=overwrite)
        total += added_g
        print(f"[guardados] +{added_g} (total {total})")

    print(f"[done] total filas nuevas: {total}")

if __name__ == "__main__":
    sys.exit(main() or 0)
