"""
Bot Americano — Menú principal estilo BotFather
Checkpoint: robot

⚠️ Nota de compatibilidad SSL
Este script detecta si el intérprete de Python carece del módulo estándar `ssl`.
- Si `ssl` está disponible ➜ ejecuta el bot con **aiogram** (long polling) y toda la UI.
- Si `ssl` NO está disponible ➜ entra en **modo OFFLINE** (sin red) y sólo habilita pruebas/validaciones.

Requisitos (runtime con red):
  - Python 3.10+
  - pip install aiogram==3.13.1 python-dotenv==1.0.1
  - (opcional) pip install gspread==6.1.4 google-auth==2.35.0
Variables de entorno:
  - TELEGRAM_BOT_TOKEN=<tu_token_de_BotFather>
  - GOOGLE_SHEETS_ID=<id del spreadsheet>            # opcional para sync
  - GOOGLE_SERVICE_ACCOUNT_JSON=<JSON completo>      # o
  - GOOGLE_APPLICATION_CREDENTIALS=<ruta al .json>   # credenciales SA

Ejecución local (bot real):
  - python bot.py

Ejecución de pruebas (sin red/SSL o para CI):
  - python bot.py --test
"""

from __future__ import annotations
import asyncio
import logging
import os
import sys
import json
from dataclasses import dataclass
from typing import Any, List, Optional
from pathlib import Path
from datetime import datetime

# =============================
#  Detección de SSL y selección de modo
# =============================
try:
    import ssl  # noqa: F401
    SSL_OK = True
except Exception:  # pragma: no cover
    SSL_OK = False

OFFLINE_MODE = not SSL_OK

# =============================
#  Imports condicionales (aiogram solo si hay SSL)
# =============================
if not OFFLINE_MODE:
    from aiogram import Bot, Dispatcher, F
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode
    from aiogram.filters import Command
    from aiogram.types import (
        Message,
        ReplyKeyboardMarkup,
        KeyboardButton,
        InlineKeyboardMarkup,
        InlineKeyboardButton,
        CallbackQuery,
        BotCommand,
        ForceReply,
    )
    from aiogram.fsm.state import StatesGroup, State
    from aiogram.fsm.context import FSMContext
else:
    # ---- Stubs mínimos para pruebas offline (sin red ni aiogram) ----
    class KeyboardButton:  # type: ignore
        def __init__(self, text: str):
            self.text = text

    class ReplyKeyboardMarkup:  # type: ignore
        def __init__(self, keyboard: List[List[KeyboardButton]], resize_keyboard: bool = True, input_field_placeholder: str | None = None):
            self.keyboard = keyboard
            self.resize_keyboard = resize_keyboard
            self.input_field_placeholder = input_field_placeholder

    class InlineKeyboardButton:  # type: ignore
        def __init__(self, text: str, callback_data: str):
            self.text = text
            self.callback_data = callback_data

    class InlineKeyboardMarkup:  # type: ignore
        def __init__(self, inline_keyboard: List[List[InlineKeyboardButton]]):
            self.inline_keyboard = inline_keyboard

    @dataclass
    class BotCommand:  # type: ignore
        command: str
        description: str

    F = type("F", (), {})  # noqa: N806

from dotenv import load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# =============================
#  Configuración de MENÚS (acordado en checkpoint "robot")
# =============================

# Prefijos para callbacks (Configuración estilo BotFather)
CB_INPUT = "input::"       # solicita un valor vía textbox
CB_TOGGLE = "toggle::"     # alterna boolean / opción
CB_ACTION = "action::"     # dispara una acción (cargar históricos, volver, etc.)

# Botones principales
BTN_SONADORA = "Soñadora"
BTN_PARLAY_SEGURITO = "Parlay 🔒 Segurito"
BTN_TOP_PICKS = "Top PICS"
BTN_DEPORTES = "Deportes"
BTN_CONFIG = "Configuración"
BTN_AVISAME = "🔔 Avísame"
BTN_VOLVER = "⬅️ Volver"

# Submenú Configuración (teclado de acceso) — solo Notificaciones + Volver
BTN_CFG_NOTIF = "Notificaciones"

# Deportes (emojis acordados)
DEPORTES = [
    "⚽ Fútbol",
    "⚾ Béisbol",
    "🏀 Baloncesto",
    "🎾 Tenis",
    "🏒 Hockey",
    "🏓 Ping Pong",
    "🏈 Americano",
    "🎮 e‑Sports",
    "🥋 MMA/UFC",
    "🥊 Boxeo",
    "🏎️ F1",
]

@dataclass
class Menus:
    principal: Any
    configuracion: Any
    deportes: Any


def build_principal_menu() -> Any:
    kb = [
        [KeyboardButton(text=BTN_SONADORA), KeyboardButton(text=BTN_PARLAY_SEGURITO)],
        [KeyboardButton(text=BTN_TOP_PICKS), KeyboardButton(text=BTN_DEPORTES)],
        [KeyboardButton(text=BTN_CONFIG), KeyboardButton(text=BTN_AVISAME)],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Elige una opción…")


def build_config_menu() -> Any:
    kb = [
        [KeyboardButton(text=BTN_CFG_NOTIF)],
        [KeyboardButton(text=BTN_VOLVER)],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Config…")


def build_deportes_menu() -> Any:
    filas: List[List[KeyboardButton]] = []
    fila: List[KeyboardButton] = []
    for i, dep in enumerate(DEPORTES, start=1):
        fila.append(KeyboardButton(text=dep))
        if i % 2 == 0:
            filas.append(fila)
            fila = []
    if fila:
        filas.append(fila)
    filas.append([KeyboardButton(text=BTN_VOLVER)])
    return ReplyKeyboardMarkup(keyboard=filas, resize_keyboard=True, input_field_placeholder="Deportes…")


MENUS = Menus(
    principal=build_principal_menu(),
    configuracion=build_config_menu(),
    deportes=build_deportes_menu(),
)

# =============================
#  Persistencia y perfil de usuario + Google Sheets Sync
# =============================
DATA_DIR = Path(os.getenv("BOT_DATA_DIR", "data"))
USERS_DIR = DATA_DIR / "users"
USERS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PROFILE = {
    "language": "es-MX",
    "bankroll_mxn": 500,
    "stake_mode": "fijo",    # fijo | auto
    "stake_pct": 5.0,
    "leagues_priority": ["NFL", "College", "Liga Mexicana"],
    "markets_priority": ["Winner", "Handicap", "Totals", "Over/Under", "Spread", "Team Props", "Player Props"],
    "sports_enabled": {dep: True for dep in [
        "⚽ Fútbol","⚾ Béisbol","🏀 Baloncesto","🎾 Tenis","🏒 Hockey","🏓 Ping Pong",
        "🏈 Americano","🎮 e‑Sports","🥋 MMA/UFC","🥊 Boxeo","🏎️ F1"]},
    "alerts": {dep: False for dep in [
        "⚽ Fútbol","⚾ Béisbol","🏀 Baloncesto","🎾 Tenis","🏒 Hockey","🏓 Ping Pong",
        "🏈 Americano","🎮 e‑Sports","🥋 MMA/UFC","🥊 Boxeo","🏎️ F1"]},
    "parlay_segurito": {"max_legs": 3, "min_odds": 1.8, "reuse_picks": False},
    "parlay_sonadora": {"max_legs": 8, "min_odds": 10.0, "reuse_picks": True},
    "notifications": {"enabled": True, "start": True, "end": True, "progress_50": False, "progress_75": True, "result": True},
    "next_events_hours": 48,
    "historical_load": {"periods": 3, "current_season": True, "progress": {"⚽ Fútbol": 100, "🏈 Americano": 65, "🎾 Tenis": 40}},
    "google_sheets_sync": True,
}

SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SHEET_TAB = "CONFIG_USUARIOS"


def _user_path(user_id: int) -> Path:
    return USERS_DIR / f"{user_id}.json"


def load_profile(user_id: int) -> dict:
    p = _user_path(user_id)
    if not p.exists():
        return json.loads(json.dumps(DEFAULT_PROFILE))
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(json.dumps(DEFAULT_PROFILE))

# ---- Google Sheets helpers (opcionales) ----

def _get_gspread_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:  # libs no instaladas
        logging.warning("gspread/google-auth no disponibles: %s", e)
        return None
    creds = None
    try:
        if GOOGLE_SERVICE_ACCOUNT_JSON:
            data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = Credentials.from_service_account_info(data, scopes=["https://www.googleapis.com/auth/spreadsheets"]) 
        elif GOOGLE_APPLICATION_CREDENTIALS and Path(GOOGLE_APPLICATION_CREDENTIALS).exists():
            creds = Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS, scopes=["https://www.googleapis.com/auth/spreadsheets"]) 
        if not creds:
            logging.warning("Credenciales de Google no configuradas")
            return None
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        logging.warning("No se pudo inicializar gspread: %s", e)
        return None


def _ensure_config_sheet(gc) -> Optional[object]:
    if not gc or not SHEETS_ID:
        return None
    try:
        sh = gc.open_by_key(SHEETS_ID)
        try:
            ws = sh.worksheet(SHEET_TAB)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_TAB, rows=2000, cols=20)
            headers = [
                "user_id","username","language","bankroll_mxn","stake_mode","stake_pct",
                "leagues_priority","markets_priority","sports_enabled","alerts_per_sport",
                "parlay_segurito","parlay_sonadora","notifications","next_events_hours",
                "historical_periods","historical_progress","google_sheets_sync","updated_at"
            ]
            ws.update('A1', [headers])
        return ws
    except Exception as e:
        logging.warning("No se pudo abrir hoja de Sheets: %s", e)
        return None


def _flatten_profile(user_id: int, username: str, profile: dict) -> dict:
    row = {
        "user_id": user_id,
        "username": username or "",
        "language": profile.get("language","es-MX"),
        "bankroll_mxn": profile.get("bankroll_mxn",0),
        "stake_mode": profile.get("stake_mode","fijo"),
        "stake_pct": profile.get("stake_pct",0),
        "leagues_priority": ",".join(profile.get("leagues_priority",[])),
        "markets_priority": ",".join(profile.get("markets_priority",[])),
        "sports_enabled": ",".join([k for k,v in profile.get("sports_enabled",{}).items() if v]),
        "alerts_per_sport": json.dumps(profile.get("alerts",{}), ensure_ascii=False),
        "parlay_segurito": json.dumps(profile.get("parlay_segurito",{}), ensure_ascii=False),
        "parlay_sonadora": json.dumps(profile.get("parlay_sonadora",{}), ensure_ascii=False),
        "notifications": json.dumps(profile.get("notifications",{}), ensure_ascii=False),
        "next_events_hours": profile.get("next_events_hours",48),
        "historical_periods": profile.get("historical_load",{}).get("periods",0),
        "historical_progress": json.dumps(profile.get("historical_load",{}).get("progress",{}), ensure_ascii=False),
        "google_sheets_sync": profile.get("google_sheets_sync", True),
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    return row


def _upsert_row(ws, data: dict) -> None:
    try:
        headers = ws.row_values(1)
        # Map data to row
        row_values = [str(data.get(h, "")) for h in headers]
        # Buscar user_id
        cell = None
        try:
            cell = ws.find(str(data["user_id"]))
        except Exception:
            cell = None
        if cell and cell.row > 1:
            rng = f"A{cell.row}:{chr(64+len(headers))}{cell.row}"
            ws.update(rng, [row_values])
        else:
            ws.append_row(row_values)
    except Exception as e:
        logging.warning("Error upsert Sheets: %s", e)


def sync_to_sheets(user_id: int, username: str, profile: dict) -> None:
    if not profile.get("google_sheets_sync", True):
        return
    gc = _get_gspread_client()
    ws = _ensure_config_sheet(gc)
    if not ws:
        return
    data = _flatten_profile(user_id, username, profile)
    _upsert_row(ws, data)


def save_profile(user_id: int, profile: dict, username: str = "") -> None:
    p = _user_path(user_id)
    p.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    # Sync opcional a Google Sheets
    try:
        sync_to_sheets(user_id, username, profile)
    except Exception as e:
        logging.warning("Sync Sheets falló: %s", e)

# =============================
#  Helpers de UI
# =============================

def _fmt_bool(v: bool) -> str:
    return "✅" if v else "❌"


def alerts_inline_kb() -> Any:
    # Inline para activar/desactivar alertas globales
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Activar alertas", callback_data="alert_on")],
            [InlineKeyboardButton(text="Desactivar alertas", callback_data="alert_off")],
        ]
    )


def sport_inline_kb(nombre: str) -> Any:
    # Inline contextual por deporte
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Top PICS {nombre}", callback_data=f"sport_top::{nombre}")],
            [InlineKeyboardButton(text="Mercados", callback_data=f"sport_markets::{nombre}")],
            [InlineKeyboardButton(text="Avísame", callback_data=f"sport_alert::{nombre}")],
        ]
    )


def render_config_text(profile: dict) -> str:
    ps = profile.get("parlay_segurito", {})
    pd = profile.get("parlay_sonadora", {})
    notif = profile.get("notifications", {})
    hours = profile.get("next_events_hours", 48)
    hist = profile.get("historical_load", {})
    progress = hist.get("progress", {})

    lines = [
        "<b>⚙️ Configuración del Bot Americano</b>",
        "",
        "<b>💰 Bank & Stake</b>",
        f"Bank inicial: <code>{profile.get('bankroll_mxn', 0)} MXN</code>",
        f"Stake: <code>{'Fijo ('+str(profile.get('stake_pct', 0))+'%)' if profile.get('stake_mode','fijo')=='fijo' else 'Automático'}</code>",
        "",
        "<b>🔒 Parlay Segurito</b>",
        f"Max Legs: <code>{ps.get('max_legs', 3)}</code> · Min cuota: <code>{ps.get('min_odds', 1.8)}</code> · Reutilizar: {_fmt_bool(ps.get('reuse_picks', False))}",
        "",
        "<b>🌙 Parlay Soñador</b>",
        f"Max Legs: <code>{pd.get('max_legs', 8)}</code> · Min cuota: <code>{pd.get('min_odds', 10.0)}</code> · Reutilizar: {_fmt_bool(pd.get('reuse_picks', True))}",
        "",
        "<b>🔔 Notificaciones</b>",
        f"Global: {_fmt_bool(notif.get('enabled', True))} · Inicio: {_fmt_bool(notif.get('start', True))} · Fin: {_fmt_bool(notif.get('end', True))}",
        f"50%: {_fmt_bool(notif.get('progress_50', False))} · 75%: {_fmt_bool(notif.get('progress_75', True))} · Resultado: {_fmt_bool(notif.get('result', True))}",
        "",
        "<b>🗓️ Próximas Fechas</b>",
        f"Mostrar próximos: <code>{hours}h</code>",
        "",
        "<b>⚽ Deportes</b>",
        ", ".join([f"{_fmt_bool(profile['sports_enabled'].get(dep, False))} {dep}" for dep in DEPORTES]),
        "",
        "<b>📊 Carga de Históricos</b>",
        f"Periodo: <code>{hist.get('periods', 3)}</code> temporadas + actual",
    ]
    if progress:
        for dep in DEPORTES:
            if dep in progress:
                lines.append(f"{dep}: {progress[dep]}%")
    lines += [
        "",
        "<b>📄 Google Sheets</b>",
        f"Escritura activa: {_fmt_bool(profile.get('google_sheets_sync', True))}",
    ]
    return "
".join(lines)


def build_config_keyboard(profile: dict) -> Any:
    kb: list[list[InlineKeyboardButton]] = []
    # Bank & Stake
    kb.append([InlineKeyboardButton(text=f"Bank: {profile.get('bankroll_mxn',0)} MXN", callback_data=CB_INPUT+"bank")])
    stake_mode = profile.get("stake_mode", "fijo")
    kb.append([
        InlineKeyboardButton(text=f"Stake: {'Fijo '+str(profile.get('stake_pct',0))+'%' if stake_mode=='fijo' else 'Automático'}", callback_data=CB_INPUT+("stake_pct" if stake_mode=='fijo' else "stake_mode")),
        InlineKeyboardButton(text=("Cambiar a Automático" if stake_mode=="fijo" else "Cambiar a Fijo"), callback_data=CB_TOGGLE+"stake_mode"),
    ])
    # Parlay Segurito
    ps = profile.get("parlay_segurito", {})
    kb.append([
        InlineKeyboardButton(text=f"Segurito Legs: {ps.get('max_legs',3)}", callback_data=CB_INPUT+"ps_legs"),
        InlineKeyboardButton(text=f"Min cuota: {ps.get('min_odds',1.8)}", callback_data=CB_INPUT+"ps_odds"),
    ])
    kb.append([
        InlineKeyboardButton(text=f"Reutilizar: {_fmt_bool(ps.get('reuse_picks',False))}", callback_data=CB_TOGGLE+"ps_reuse")
    ])
    # Parlay Soñador
    pd = profile.get("parlay_sonadora", {})
    kb.append([
        InlineKeyboardButton(text=f"Soñador Legs: {pd.get('max_legs',8)}", callback_data=CB_INPUT+"pd_legs"),
        InlineKeyboardButton(text=f"Min cuota: {pd.get('min_odds',10.0)}", callback_data=CB_INPUT+"pd_odds"),
    ])
    kb.append([
        InlineKeyboardButton(text=f"Reutilizar: {_fmt_bool(pd.get('reuse_picks',True))}", callback_data=CB_TOGGLE+"pd_reuse")
    ])
    # Notificaciones
    notif = profile.get("notifications", {})
    kb.append([
        InlineKeyboardButton(text=f"Global {_fmt_bool(notif.get('enabled',True))}", callback_data=CB_TOGGLE+"n_enabled"),
        InlineKeyboardButton(text=f"Inicio {_fmt_bool(notif.get('start',True))}", callback_data=CB_TOGGLE+"n_start"),
        InlineKeyboardButton(text=f"Fin {_fmt_bool(notif.get('end',True))}", callback_data=CB_TOGGLE+"n_end"),
    ])
    kb.append([
        InlineKeyboardButton(text=f"50% {_fmt_bool(notif.get('progress_50',False))}", callback_data=CB_TOGGLE+"n_50"),
        InlineKeyboardButton(text=f"75% {_fmt_bool(notif.get('progress_75',True))}", callback_data=CB_TOGGLE+"n_75"),
        InlineKeyboardButton(text=f"Resultado {_fmt_bool(notif.get('result',True))}", callback_data=CB_TOGGLE+"n_result"),
    ])
    # Próximas Fechas
    kb.append([
        InlineKeyboardButton(text=f"Próximos: {profile.get('next_events_hours',48)}h", callback_data=CB_INPUT+"next_hours")
    ])
    # Deportes (una línea por 3-4 deportes)
    row: list[InlineKeyboardButton] = []
    for i, dep in enumerate(DEPORTES, 1):
        on = profile["sports_enabled"].get(dep, False)
        row.append(InlineKeyboardButton(text=f"{('✅' if on else '❌')} {dep}", callback_data=CB_TOGGLE+f"sport::{dep}"))
        if i % 3 == 0:
            kb.append(row); row = []
    if row:
        kb.append(row)
    # Históricos
    hist = profile.get("historical_load", {})
    kb.append([
        InlineKeyboardButton(text=f"Periodo: {hist.get('periods',3)}", callback_data=CB_INPUT+"hist_periods")
    ])
    for dep in list(hist.get("progress", {}).keys())[:6]:
        kb.append([InlineKeyboardButton(text=f"🔄 Actualizar {dep}", callback_data=CB_ACTION+f"hist_load::{dep}")])
    # Sheets
    kb.append([
        InlineKeyboardButton(text=f"Google Sheets {_fmt_bool(profile.get('google_sheets_sync', True))}", callback_data=CB_TOGGLE+"sheets")
    ])
    # Volver
    kb.append([InlineKeyboardButton(text="« Volver", callback_data=CB_ACTION+"back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# =============================
#  Comandos estilo BotFather
# =============================
BOT_COMMANDS = [
    BotCommand(command="start", description="Abrir menú principal"),
    BotCommand(command="help", description="Ayuda y comandos"),
    BotCommand(command="menu", description="Reenviar menú principal"),
    BotCommand(command="deportes", description="Abrir submenú de deportes"),
    BotCommand(command="config", description="Abrir configuración"),
]

# =============================
#  Modo online (aiogram) — Handlers y arranque
# =============================
if not OFFLINE_MODE:
    bot = Bot(TOKEN or "", default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    def _uid(message: Message) -> int:
        return message.from_user.id if message.from_user else 0

    def _uname(message: Message) -> str:
        u = getattr(message, 'from_user', None)
        if not u:
            return ""
        return (u.username and f"@{u.username}") or f"{u.first_name or ''} {u.last_name or ''}".strip()

    async def _show_profile(message: Message) -> None:
        uid = _uid(message)
        prof = load_profile(uid)
        txt = [
            "<b>Tu perfil</b>",
            f"• Idioma: {prof['language']}",
            f"• Bankroll: {prof['bankroll_mxn']} MXN",
            f"• Stake: {'Fijo '+str(prof['stake_pct'])+'%' if prof['stake_mode']=='fijo' else 'Automático'}",
        ]
        await message.answer("
".join(txt))

    @dp.message(Command("start"))
    async def cmd_start(message: Message):
        if not TOKEN:
            await message.answer("⚠️ Falta TELEGRAM_BOT_TOKEN en tus variables de entorno.")
            return
        await message.answer(
            (
                "<b>Bot Americano</b>
"
                "Selecciona una opción del menú.

"
                "Consejo: escribe <code>/help</code> para ver los comandos."
            ),
            reply_markup=MENUS.principal,
        )

    @dp.message(Command("help"))
    async def cmd_help(message: Message):
        text = [
            "<b>Ayuda</b>",
            "• /start – abre el menú principal",
            "• /menu – vuelve a mostrar el menú",
            "• /deportes – abre el submenú de deportes",
            "• /config – abre configuración",
        ]
        await message.answer("
".join(text))

    @dp.message(Command("menu"))
    async def cmd_menu(message: Message):
        await message.answer("Menú principal:", reply_markup=MENUS.principal)

    @dp.message(Command("deportes"))
    async def cmd_deportes(message: Message):
        await message.answer("Elige un deporte:", reply_markup=MENUS.deportes)

    @dp.message(Command("config"))
    async def cmd_config(message: Message):
        uid = _uid(message)
        prof = load_profile(uid)
        await message.answer(render_config_text(prof), reply_markup=build_config_keyboard(prof))

    @dp.message(F.text == BTN_VOLVER)
    async def go_back(message: Message):
        await message.answer("Volviendo al menú principal…", reply_markup=MENUS.principal)

    @dp.message(F.text == BTN_CONFIG)
    async def open_config(message: Message):
        uid = _uid(message)
        prof = load_profile(uid)
        await message.answer(render_config_text(prof), reply_markup=build_config_keyboard(prof))

    @dp.message(F.text == BTN_DEPORTES)
    async def open_deportes(message: Message):
        await message.answer("Elige un deporte:", reply_markup=MENUS.deportes)

    @dp.message(F.text == BTN_SONADORA)
    async def handle_sonadora(message: Message):
        await message.answer(
            "Soñadora (parlays altos) – próximamente conectamos al generador de combinaciones.",
            reply_markup=alerts_inline_kb(),
        )

    @dp.message(F.text == BTN_PARLAY_SEGURITO)
    async def handle_parlay_segurito(message: Message):
        await message.answer(
            "Parlay 🔒 Segurito – generaremos combinaciones con mayor probabilidad (placeholder).",
            reply_markup=alerts_inline_kb(),
        )

    @dp.message(F.text == BTN_TOP_PICKS)
    async def handle_top_picks(message: Message):
        await message.answer(
            "Top PICS del día (placeholder). Usa ‘Avísame’ para alertas.",
            reply_markup=alerts_inline_kb(),
        )

    @dp.message(F.text == BTN_AVISAME)
    async def handle_avisame(message: Message):
        await _show_profile(message)
        await message.answer("Activa/Desactiva alertas globales. Para por-deporte, entra a Deportes y usa \"Avísame\".", reply_markup=alerts_inline_kb())

    @dp.message(F.text.in_(DEPORTES))
    async def handle_deporte(message: Message):
        deporte = message.text
        uid = _uid(message)
        prof = load_profile(uid)
        is_on = prof["alerts"].get(deporte, False)
        status = "🔔 ON" if is_on else "🔕 OFF"
        await message.answer(
            f"Has elegido <b>{deporte}</b> · Alertas: {status}
¿Qué deseas hacer?",
            reply_markup=sport_inline_kb(deporte),
        )

    # --------- Inline callbacks (Alertas globales legacy) ---------
    @dp.callback_query(F.data == "alert_on")
    async def cb_alert_on(cb: CallbackQuery):
        await cb.answer()
        uid = cb.from_user.id if cb.from_user else 0
        prof = load_profile(uid)
        for k in prof["alerts"].keys():
            prof["alerts"][k] = True
        save_profile(uid, prof, username=(cb.from_user.username and f"@{cb.from_user.username}") or cb.from_user.full_name)
        await cb.message.answer("✅ Alertas activadas (todas las disciplinas).")

    @dp.callback_query(F.data == "alert_off")
    async def cb_alert_off(cb: CallbackQuery):
        await cb.answer()
        uid = cb.from_user.id if cb.from_user else 0
        prof = load_profile(uid)
        for k in prof["alerts"].keys():
            prof["alerts"][k] = False
        save_profile(uid, prof, username=(cb.from_user.username and f"@{cb.from_user.username}") or cb.from_user.full_name)
        await cb.message.answer("🔕 Alertas desactivadas (todas las disciplinas).")

    # ===== Config: FSM =====
    class Cfg(StatesGroup):
        waiting_value = State()

    @dp.callback_query(F.data.startswith(CB_TOGGLE))
    async def cfg_toggle(cb: CallbackQuery):
        await cb.answer()
        uid = cb.from_user.id if cb.from_user else 0
        prof = load_profile(uid)
        key = cb.data[len(CB_TOGGLE):]
        if key == "stake_mode":
            prof["stake_mode"] = "auto" if prof.get("stake_mode","fijo")=="fijo" else "fijo"
        elif key == "ps_reuse":
            prof.setdefault("parlay_segurito", {}).setdefault("reuse_picks", False)
            prof["parlay_segurito"]["reuse_picks"] = not prof["parlay_segurito"]["reuse_picks"]
        elif key == "pd_reuse":
            prof.setdefault("parlay_sonadora", {}).setdefault("reuse_picks", True)
            prof["parlay_sonadora"]["reuse_picks"] = not prof["parlay_sonadora"]["reuse_picks"]
        elif key == "n_enabled":
            prof.setdefault("notifications", {}).setdefault("enabled", True)
            prof["notifications"]["enabled"] = not prof["notifications"]["enabled"]
        elif key == "n_start":
            prof.setdefault("notifications", {}).setdefault("start", True)
            prof["notifications"]["start"] = not prof["notifications"]["start"]
        elif key == "n_end":
            prof.setdefault("notifications", {}).setdefault("end", True)
            prof["notifications"]["end"] = not prof["notifications"]["end"]
        elif key == "n_50":
            prof.setdefault("notifications", {}).setdefault("progress_50", False)
            prof["notifications"]["progress_50"] = not prof["notifications"]["progress_50"]
        elif key == "n_75":
            prof.setdefault("notifications", {}).setdefault("progress_75", True)
            prof["notifications"]["progress_75"] = not prof["notifications"]["progress_75"]
        elif key == "n_result":
            prof.setdefault("notifications", {}).setdefault("result", True)
            prof["notifications"]["result"] = not prof["notifications"]["result"]
        elif key.startswith("sport::"):
            dep = key.split("::",1)[1]
            cur = prof["sports_enabled"].get(dep, False)
            prof["sports_enabled"][dep] = not cur
        elif key == "sheets":
            prof["google_sheets_sync"] = not prof.get("google_sheets_sync", True)
        save_profile(uid, prof, username=(cb.from_user.username and f"@{cb.from_user.username}") or cb.from_user.full_name)
        await cb.message.edit_text(render_config_text(prof), reply_markup=build_config_keyboard(prof))

    @dp.callback_query(F.data.startswith(CB_INPUT))
    async def cfg_input_request(cb: CallbackQuery, state: FSMContext):
        await cb.answer()
        field = cb.data[len(CB_INPUT):]
        await state.update_data(field=field)
        prompt_map = {
            "bank": "💰 Ingresa nuevo bank (MXN, entero):",
            "stake_pct": "% de stake fijo (ej. 5.0):",
            "ps_legs": "Max Legs para Parlay Segurito (entero):",
            "ps_odds": "Cuota mínima para Parlay Segurito (decimal):",
            "pd_legs": "Max Legs para Parlay Soñador (entero):",
            "pd_odds": "Cuota mínima para Parlay Soñador (decimal):",
            "next_hours": "Horas para próximos eventos (24/48/72):",
            "hist_periods": "Número de temporadas históricas (entero):",
        }
        await state.set_state(Cfg.waiting_value)
        await cb.message.reply(prompt_map.get(field, "Ingresa valor:"), reply_markup=ForceReply(selective=True))

    @dp.message(Cfg.waiting_value)
    async def cfg_input_receive(message: Message, state: FSMContext):
        data = await state.get_data()
        field = data.get("field")
        uid = message.from_user.id if message.from_user else 0
        prof = load_profile(uid)
        text = (message.text or "").strip()
        ok = True
        try:
            if field == "bank":
                val = int(text); assert val >= 0
                prof["bankroll_mxn"] = val
            elif field == "stake_pct":
                val = float(text); assert 0 < val <= 100
                prof["stake_pct"] = val; prof["stake_mode"] = "fijo"
            elif field == "ps_legs":
                val = int(text); assert 1 <= val <= 20
                prof.setdefault("parlay_segurito", {})["max_legs"] = val
            elif field == "ps_odds":
                val = float(text); assert val >= 1.01
                prof.setdefault("parlay_segurito", {})["min_odds"] = val
            elif field == "pd_legs":
                val = int(text); assert 1 <= val <= 20
                prof.setdefault("parlay_sonadora", {})["max_legs"] = val
            elif field == "pd_odds":
                val = float(text); assert val >= 1.01
                prof.setdefault("parlay_sonadora", {})["min_odds"] = val
            elif field == "next_hours":
                val = int(text); assert val in (24,48,72)
                prof["next_events_hours"] = val
            elif field == "hist_periods":
                val = int(text); assert 0 <= val <= 15
                prof.setdefault("historical_load", {})["periods"] = val
            else:
                ok = False
        except Exception:
            ok = False
        if ok:
            save_profile(uid, prof, username=((message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name))
            await message.answer("✅ Guardado.")
            await state.clear()
            await message.answer(render_config_text(prof), reply_markup=build_config_keyboard(prof))
        else:
            await message.answer("❌ Valor inválido. Toca el campo otra vez e ingresa un valor correcto.")

    @dp.callback_query(F.data.startswith(CB_ACTION))
    async def cfg_action(cb: CallbackQuery):
        await cb.answer()
        uid = cb.from_user.id if cb.from_user else 0
        prof = load_profile(uid)
        key = cb.data[len(CB_ACTION):]
        if key == "back":
            await cb.message.answer("Volviendo al menú principal…", reply_markup=MENUS.principal)
            return
        if key.startswith("hist_load::"):
            dep = key.split("::",1)[1]
            prof.setdefault("historical_load", {}).setdefault("progress", {})
            cur = prof["historical_load"]["progress"].get(dep, 0)
            prof["historical_load"]["progress"][dep] = min(100, cur + 5)  # simulación
            save_profile(uid, prof, username=(cb.from_user.username and f"@{cb.from_user.username}") or cb.from_user.full_name)
        await cb.message.edit_text(render_config_text(prof), reply_markup=build_config_keyboard(prof))

# =============================
#  Pruebas/validaciones (unit tests)
# =============================
import unittest

class MenuTests(unittest.TestCase):
    def test_principal_buttons(self):
        labels = [btn.text for row in MENUS.principal.keyboard for btn in row]
        expected = {BTN_SONADORA, BTN_PARLAY_SEGURITO, BTN_TOP_PICKS, BTN_DEPORTES, BTN_CONFIG, BTN_AVISAME}
        self.assertTrue(expected.issubset(set(labels)))

    def test_deportes_len_and_back(self):
        all_labels = [btn.text for row in MENUS.deportes.keyboard for btn in row]
        count_deportes = sum(1 for x in all_labels if x in DEPORTES)
        self.assertEqual(count_deportes, 11)
        self.assertIn(BTN_VOLVER, all_labels)

    def test_commands(self):
        cmds = {c.command for c in BOT_COMMANDS}
        self.assertEqual(cmds, {"start", "help", "menu", "deportes", "config"})

    def test_inline_alerts(self):
        kb = alerts_inline_kb().inline_keyboard
        flat = [b.text for row in kb for b in row]
        self.assertIn("Activar alertas", flat)
        self.assertIn("Desactivar alertas", flat)

    def test_config_keyboard_has_core_items(self):
        uid = 777
        prof = load_profile(uid)
        kb = build_config_keyboard(prof).inline_keyboard
        cbdata = []
        for row in kb:
            for b in row:
                cbdata.append(getattr(b, 'callback_data', ''))
        self.assertTrue(any(x.startswith(CB_INPUT+"bank") for x in cbdata))
        self.assertIn(CB_TOGGLE+"stake_mode", cbdata)
        self.assertIn(CB_TOGGLE+"ps_reuse", cbdata)
        self.assertIn(CB_TOGGLE+"pd_reuse", cbdata)
        self.assertIn(CB_TOGGLE+"n_enabled", cbdata)
        self.assertIn(CB_TOGGLE+"sheets", cbdata)

# =============================
#  Entrypoint
# =============================
async def _run_bot() -> None:
    if OFFLINE_MODE:
        logging.warning("Modo OFFLINE por ausencia de ssl. Ejecuta con --test para validar o instala Python con SSL.")
        return
    if not TOKEN:
        logging.error("Falta TELEGRAM_BOT_TOKEN en el entorno. Exporta la variable y reintenta.")
        return
    await bot.set_my_commands(BOT_COMMANDS)
    logging.info("Comandos fijados.")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


def _run_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(MenuTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    if "--test" in sys.argv:
        sys.exit(_run_tests())
    try:
        asyncio.run(_run_bot())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot detenido.")
