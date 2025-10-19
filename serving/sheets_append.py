from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ======================================================
# CONFIGURACIÓN
# ======================================================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "TU_SHEET_ID"  # <-- cambia esto por tu ID real
RANGE_PARLAYS = "PARLAYS!A:H"   # ajusta si tienes más columnas
SERVICE_FILE = "service_account.json"  # ruta a tu archivo de credenciales

# ======================================================
# FUNCIONES AUXILIARES
# ======================================================

def make_key(tipo, fecha, deporte, partido, mercado, pick):
    """Crea una clave única combinando campos relevantes."""
    parts = [str(x).strip().lower() for x in [tipo, fecha, deporte, partido, mercado, pick]]
    return "|".join(parts)

def get_existing_keys(service):
    """Lee las claves existentes desde la hoja PARLAYS."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_PARLAYS
    ).execute()

    rows = result.get("values", [])[1:]  # saltar encabezado
    keys = set()

    for r in rows:
        if len(r) >= 7:
            key = make_key(r[1], r[2], r[3], r[4], r[5], r[6])
            keys.add(key)
    return keys

# ======================================================
# FUNCIÓN PRINCIPAL PARA AGREGAR PARLAY
# ======================================================

def append_parlay(row):
    """
    row = [
      ID, TIPO, FECHA, DEPORTE, PARTIDO, MERCADO, PICK, CUOTA (PROB %), STAKE
    ]
    """
    creds = Credentials.from_service_account_file(SERVICE_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    new_key = make_key(row[1], row[2], row[3], row[4], row[5], row[6])
    existing_keys = get_existing_keys(service)

    if new_key in existing_keys:
        print(f"[SKIP] Duplicado detectado: {new_key}")
        return

    body = {"values": [row]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="PARLAYS!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

    print(f"[OK] Parlay agregado correctamente: {row[0]}")

# ======================================================
# EJEMPLO DE USO
# ======================================================
if __name__ == "__main__":
    ejemplo_row = [
        "PR20251019-TEST01",   # ID
        "🔒 Segurito",          # TIPO
        "19/10/2025",          # FECHA
        "american",            # DEP
        "Tennessee Titans vs New England Patriots",  # PARTIDO
        "ML",                  # MERCADO
        "Tennessee Titans ML", # PICK
        "1.85 (54%)",          # CUOTA (PROB %)
        "5%"                   # STAKE
    ]

    append_parlay(ejemplo_row)
