"""
App de despliegue gratuito en Hugging Face Spaces (SDK: Gradio)
- Expone un endpoint de webhook para Telegram usando FastAPI
- Monta una UI mínima de estado con Gradio en /
- Respeta los nombres de variables en "claves" y crea alias para el bot existente

Requisitos del Space (Settings → Variables and secrets):
  TELEGRAM_BOT_TOKEN        (obligatorio)
  HF_SPACE                  (https://<org>-<space>.hf.space)
  # opcional: definir WEBHOOK_SECRET_PATH (si no, se deriva del token)

  # Google Sheets (opcionales)
  GSHEET_ID
  GCP_SA_JSON               # o GOOGLE_APPLICATION_CREDENTIALS

Nota: No es necesario modificar tu bot.py. Este app.py exporta alias de env
para que bot.py siga funcionando sin cambios.
"""
from __future__ import annotations
import os
import logging
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import gradio as gr

# =============================
#  Aliases de variables (ajuste a "claves")
# =============================
# Hacemos alias antes de importar bot.py
if os.getenv("GSHEET_ID") and not os.getenv("GOOGLE_SHEETS_ID"):
    os.environ["GOOGLE_SHEETS_ID"] = os.getenv("GSHEET_ID")  # alias
if os.getenv("GCP_SA_JSON") and not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = os.getenv("GCP_SA_JSON")  # alias

# (otros alias posibles en el futuro)

# =============================
#  Importar el bot existente (del canvas)
# =============================
from bot import dp as tg_dp, bot as tg_bot, BOT_COMMANDS  # type: ignore
from aiogram.types import Update

# =============================
#  Lectura de secrets
# =============================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
HF_SPACE = os.getenv("HF_SPACE")  # ej. https://org-space.hf.space
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en secrets del Space.")
if not HF_SPACE:
    raise RuntimeError("Falta HF_SPACE (URL público del Space) en secrets.")

# Derivar secret si no está seteado
if not WEBHOOK_SECRET_PATH:
    # usa los últimos 32 chars del token para formar un secret reproducible
    WEBHOOK_SECRET_PATH = (TELEGRAM_BOT_TOKEN[-32:]).replace(":", "_")

HF_SPACE = HF_SPACE.rstrip("/")
WEBHOOK_URL = f"{HF_SPACE}/webhook/{WEBHOOK_SECRET_PATH}"

# =============================
#  FastAPI app + Gradio UI
# =============================
app = FastAPI()

# UI mínima en Gradio para ver estado
with gr.Blocks(title="Bot Americano — Status") as demo:
    gr.Markdown("""
    # 🤖 Bot Americano — Webhook activo
    - **Webhook URL:** `{WEBHOOK_URL}`
    - **Space:** `{HF_SPACE}`
    - **Vars Sheets:** `GSHEET_ID` → `{gs}` / `GCP_SA_JSON` → `{sa}`
    """.format(WEBHOOK_URL=WEBHOOK_URL, HF_SPACE=HF_SPACE,
                gs=bool(os.getenv("GSHEET_ID")), sa=bool(os.getenv("GCP_SA_JSON"))))

app = gr.mount_gradio_app(app, demo, path="/")

@app.get("/health")
async def health():
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    # fija /help, /menu, etc.
    try:
        await tg_bot.set_my_commands(BOT_COMMANDS)
    except Exception as e:
        logging.warning("No se pudieron fijar comandos: %s", e)
    # registra webhook
    try:
        await tg_bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, allowed_updates=["message","callback_query"])
        logging.info("Webhook fijado: %s", WEBHOOK_URL)
    except Exception as e:
        logging.error("set_webhook falló: %s", e)
        raise

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await tg_bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET_PATH:
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    try:
        update = Update.model_validate(data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Update invalid: {e}")
    await tg_dp.feed_webhook_update(tg_bot, update)
    return PlainTextResponse("OK")
