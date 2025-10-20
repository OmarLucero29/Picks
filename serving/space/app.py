# app.py — Webhook FastAPI + Gradio (Hugging Face Space con SDK Gradio)
# - Registra el webhook de Telegram al arrancar
# - Monta una UI mínima en "/" para ver estado
# - Respeta tus claves: TELEGRAM_BOT_TOKEN, HF_SPACE, GSHEET_ID, GCP_SA_JSON
#   (y crea alias hacia GOOGLE_* si tu bot.py los usa internamente)

from __future__ import annotations
import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import gradio as gr

# ---- Aliases de variables (claves -> GOOGLE_*) antes de importar bot.py ----
if os.getenv("GSHEET_ID") and not os.getenv("GOOGLE_SHEETS_ID"):
    os.environ["GOOGLE_SHEETS_ID"] = os.getenv("GSHEET_ID")
if os.getenv("GCP_SA_JSON") and not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = os.getenv("GCP_SA_JSON")

# ---- Importa tu bot (handlers, dispatcher y bot) ----
from bot import dp as tg_dp, bot as tg_bot, BOT_COMMANDS  # type: ignore
from aiogram.types import Update

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
HF_SPACE = os.getenv("HF_SPACE")  # p.ej. https://org-mi-bot.hf.space
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH")  # opcional

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en 'claves' del Space.")
if not HF_SPACE:
    raise RuntimeError("Falta HF_SPACE (URL público del Space).")

# Secret derivado si no está definido
if not WEBHOOK_SECRET_PATH:
    WEBHOOK_SECRET_PATH = (TELEGRAM_BOT_TOKEN[-32:]).replace(":", "_")

HF_SPACE = HF_SPACE.rstrip("/")
WEBHOOK_URL = f"{HF_SPACE}/webhook/{WEBHOOK_SECRET_PATH}"

# --------- FastAPI + Gradio UI ----------
app = FastAPI()

with gr.Blocks(title="Bot Americano — Status") as demo:
    gr.Markdown(
        f"""
# 🤖 Bot Americano — Webhook activo
- **Webhook URL:** `{WEBHOOK_URL}`
- **Space:** `{HF_SPACE}`
- **Sheets activos:** GSHEET_ID={'✅' if os.getenv('GSHEET_ID') else '❌'} · GCP_SA_JSON={'✅' if os.getenv('GCP_SA_JSON') else '❌'}
        """
    )

app = gr.mount_gradio_app(app, demo, path="/")

@app.get("/health")
async def health():
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    try:
        await tg_bot.set_my_commands(BOT_COMMANDS)
    except Exception as e:
        logging.warning("No se pudieron fijar comandos: %s", e)
    try:
        await tg_bot.set_webhook(
            WEBHOOK_URL,
            drop_pending_updates=True,
            allowed_updates=["message","callback_query"]
        )
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