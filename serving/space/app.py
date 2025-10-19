# app.py — FastAPI + Telegram Webhook para Hugging Face Spaces (gratis)

import os
import json
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

# Importa tu bot completo (del canvas). ¡MUY IMPORTANTE!
# Asegúrate que el archivo del canvas esté como bot.py en el repositorio del Space.
from bot import (
    bot as tg_bot,
    dp as tg_dp,
    BOT_COMMANDS,
    render_config_text,
    build_config_keyboard,
    load_profile,
    save_profile,
    DEPORTES,
)

from aiogram.types import Update
from aiogram import Bot
from aiogram.enums import ParseMode

# === Variables requeridas ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", "super-secure-path")  # pon algo largo y único
SPACE_URL = os.getenv("SPACE_URL")  # ej: https://<org>-<space>.hf.space

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Falta TELEGRAM_BOT_TOKEN en variables del Space.")

if not SPACE_URL:
    # Puedes setearlo en los Secrets del Space (Settings → Variables and secrets)
    raise RuntimeError("Falta SPACE_URL (URL público del Space).")

WEBHOOK_URL = f"{SPACE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET_PATH}"

# Asegura parse_mode
try:
    tg_bot._default.parse_mode = ParseMode.HTML  # type: ignore
except Exception:
    pass

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "status": "Bot Americano webhook up"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.on_event("startup")
async def on_startup():
    # Fija /actualiza comandos del bot
    try:
        await tg_bot.set_my_commands(BOT_COMMANDS)
    except Exception as e:
        logging.warning("No se pudieron fijar comandos: %s", e)

    # Registra el webhook en Telegram
    try:
        await tg_bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True, allowed_updates=["message","callback_query"])
        logging.info("Webhook fijado: %s", WEBHOOK_URL)
    except Exception as e:
        logging.error("No se pudo fijar webhook: %s", e)
        raise

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await tg_bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass

@app.post(f"/webhook/{{secret}}")
async def telegram_webhook(secret: str, request: Request):
    # Verifica “ruta secreta” para evitar probes
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

    # Entrega el update a Aiogram Dispatcher
    await tg_dp.feed_webhook_update(tg_bot, update)
    return PlainTextResponse("OK")
