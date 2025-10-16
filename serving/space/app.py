# app.py (lee de Google Sheets en lugar de CSV locales)
import os, json, base64
import gradio as gr
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe

SHEET_ID = os.getenv("GSHEET_ID", "")
TAB_PICKS = os.getenv("GSHEET_PICKS_TAB", "Picks")
TAB_PARLAY = os.getenv("GSHEET_PARLAY_TAB", "Parlay")
SA_JSON = os.getenv("GCP_SA_JSON", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def gclient():
    if not SA_JSON:
        raise RuntimeError("Falta GCP_SA_JSON en secrets del Space.")
    try:
        data = json.loads(SA_JSON)
    except json.JSONDecodeError:
        data = json.loads(base64.b64decode(SA_JSON).decode("utf-8"))
    creds = Credentials.from_service_account_info(data, scopes=SCOPES)
    return gspread.authorize(creds)

def read_sheet(tab):
    gc = gclient()
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    df = get_as_dataframe(ws, header=0)
    return df.dropna(how="all")

def refresh(sport):
    picks = read_sheet(TAB_PICKS)
    parlay = read_sheet(TAB_PARLAY)
    if isinstance(picks, pd.DataFrame) and "sport" in picks.columns and sport != "todos":
        picks = picks[picks["sport"] == sport]
    return picks, parlay

SPORTS = ["todos","futbol","baloncesto","beisbol","hockey","tenis","americano","ping_pong","esports"]

with gr.Blocks(title="Picks & Parlay") as demo:
    gr.Markdown("## Picks (Top-M) y Parlay — fuente: Google Sheets")
    with gr.Row():
        sport_dd = gr.Dropdown(SPORTS, value="todos", label="Filtrar deporte")
        btn = gr.Button("Actualizar", scale=0)
    picks_df = gr.Dataframe(value=pd.DataFrame(), label="Picks", interactive=False)
    parlay_df = gr.Dataframe(value=pd.DataFrame(), label="Parlay", interactive=False)
    btn.click(refresh, inputs=[sport_dd], outputs=[picks_df, parlay_df])
    sport_dd.change(refresh, inputs=[sport_dd], outputs=[picks_df, parlay_df])

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
