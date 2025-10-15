# app.py — UI simple y compatible con Gradio 4.x / 3.x
import gradio as gr
import pandas as pd
from pathlib import Path

def _load_csv(relpath):
    # En Space los archivos terminan en /app; local es raíz del repo
    for base in [Path("."), Path("/app")]:
        p = (base / relpath)
        if p.exists() and p.stat().st_size > 0:
            try:
                return pd.read_csv(p)
            except Exception:
                pass
    # columnas por defecto para que Gradio no falle
    if "parlay" in relpath:
        cols = ["date","sport","league","game","market","selection","line",
                "prob","prob_decimal_odds","confidence","rationale",
                "parlay_prob","parlay_decimal_odds","note"]
    else:
        cols = ["date","sport","league","game","market","selection","line",
                "prob","prob_decimal_odds","confidence","rationale"]
    return pd.DataFrame(columns=cols)

SPORTS = ["todos","futbol","baloncesto","beisbol","hockey","tenis","americano",
          "ping_pong","esports"]

def _filter(df, sport):
    if df.empty or sport == "todos":
        return df
    if "sport" in df.columns:
        return df[df["sport"] == sport]
    return df

def refresh(sport):
    picks = _filter(_load_csv("reports/picks.csv"), sport)
    parlay = _load_csv("reports/parlay.csv")
    return picks, parlay

with gr.Blocks(title="Picks & Parlay") as demo:
    gr.Markdown("## Picks (Top-5) y Parlay")
    with gr.Row():
        sport_dd = gr.Dropdown(SPORTS, value="todos", label="Filtrar deporte")
        btn = gr.Button("Actualizar", scale=0)
    picks_df = gr.Dataframe(
        value=_filter(_load_csv("reports/picks.csv"), "todos"),
        label="Top-5",
        interactive=False
    )
    parlay_df = gr.Dataframe(
        value=_load_csv("reports/parlay.csv"),
        label="Parlay",
        interactive=False
    )
    btn.click(fn=refresh, inputs=[sport_dd], outputs=[picks_df, parlay_df])
    sport_dd.change(fn=refresh, inputs=[sport_dd], outputs=[picks_df, parlay_df])

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
