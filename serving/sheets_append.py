# serving/sheets_append.py
# Anexa filas a Google Sheets usando tus secrets:
# GSHEET_ID, GCP_SA_JSON, GSHEET_PICKS_TAB, GSHEET_PARLAY_TAB, GSHEET_GUARDADOS

import os
import csv
import json
from pathlib import Path

def _get_ws(spreadsheet_id: str, title: str):
    import gspread
    from google.oauth2.service_account import Credentials

    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Falta GCP_SA_JSON en variables de entorno.")

    creds = Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(title)
    except Exception:
        # crea si no existe
        ws = sh.add_worksheet(title=title, rows=2000, cols=50)
    return ws

def _append_csv(ws, csv_path: Path, has_header=True):
    if not csv_path.exists():
        print(f"[skip] no existe {csv_path}")
        return 0
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if has_header and rows:
            rows = rows[1:]
        if not rows:
            print(f"[skip] {csv_path} vacío")
            return 0
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"[ok] {csv_path} -> {len(rows)} filas")
        return len(rows)

def main():
    spreadsheet_id = os.getenv("GSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Falta GSHEET_ID en variables de entorno.")

    tab_picks = os.getenv("GSHEET_PICKS_TAB", "PICKS")
    tab_parlay = os.getenv("GSHEET_PARLAY_TAB", "PARLAYS")
    tab_guardados = os.getenv("GSHEET_GUARDADOS", "GUARDADOS")

    # rutas de ejemplo; ajusta si tus jobs generan otros paths
    reports_dir = Path("reports")
    picks_csv = reports_dir / "picks.csv"
    parlays_csv = reports_dir / "parlay.csv"
    guardados_csv = reports_dir / "guardados.csv"

    total = 0

    # Picks
    ws_picks = _get_ws(spreadsheet_id, tab_picks)
    total += _append_csv(ws_picks, picks_csv)

    # Parlays
    ws_parlay = _get_ws(spreadsheet_id, tab_parlay)
    total += _append_csv(ws_parlay, parlays_csv)

    # Guardados / favoritos (opcional)
    ws_guard = _get_ws(spreadsheet_id, tab_guardados)
    total += _append_csv(ws_guard, guardados_csv)

    print(f"[done] total filas anexadas: {total}")

if __name__ == "__main__":
    main()