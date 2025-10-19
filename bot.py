""" Bot Americano — Menú principal estilo BotFather Checkpoint: robot

⚠️ Nota de compatibilidad SSL Este script detecta si el intérprete de Python carece del módulo estándar ssl.

Si ssl está disponible ➜ ejecuta el bot con aiogram (long polling) y toda la UI.

Si ssl NO está disponible ➜ entra en modo OFFLINE (sin red) y sólo habilita pruebas/validaciones.


Requisitos (runtime con red):

Python 3.10+

pip install aiogram==3.13.1 python-dotenv==1.0.1 Variables de entorno:

TELEGRAM_BOT_TOKEN=<tu_token_de_BotFather>


Ejecución local (bot real):

python bot.py


Ejecución de pruebas (sin red/SSL o para CI):

python bot.py --test """


from future import annotations import asyncio import logging import os import sys import json from dataclasses import dataclass import datetime as dt from typing import Any, List from pathlib import Path

=============================

Detección de SSL y selección de modo

=============================

try: import ssl  # noqa: F401 SSL_OK = True except Exception:  # pragma: no cover SSL_OK = False

OFFLINE_MODE = not SSL_OK

=============================

Imports condicionales (aiogram solo si hay SSL)

=============================

if not OFFLINE_MODE: from aiogram import Bot, Dispatcher, F from aiogram.client.default import DefaultBotProperties from aiogram.enums import ParseMode from aiogram.filters import Command from aiogram.types import ( Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BotCommand, ForceReply, ) from aiogram.fsm.state import StatesGroup, State from aiogram.fsm.context import FSMContext else: # ---- Stubs mínimos para pruebas offline (sin red ni aiogram) ---- class KeyboardButton:  # type: ignore def init(self, text: str): self.text = text

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

============ Google Sheets Integración ==========

Usa variables de entorno:

- GOOGLE_SHEETS_CREDENTIALS_JSON  (contenido JSON de service account)  o

- GOOGLE_APPLICATION_CREDENTIALS   (ruta a archivo .json de service account)

- GOOGLE_SHEETS_SPREADSHEET_ID     (ID del spreadsheet)

- GOOGLE_SHEETS_CONFIG_SHEET       (nombre de pestaña, default: CONFIG_USUARIOS)

Carga/Sincroniza preferencias de usuario cuando profile['google_sheets_sync'] es True.

try: import gspread  # type: ignore GSPREAD_OK = True except Exception: GSPREAD_OK = False load_dotenv() logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

=============================

Configuración de MENÚS (acordado en checkpoint "robot")

=============================

Prefijos para callbacks (Configuración estilo BotFather)

CB_INPUT = "input::"       # solicita un valor vía textbox CB_TOGGLE = "toggle::"     # alterna boolean / opción CB_ACTION = "action::"     # dispara una acción (cargar históricos, volver, etc.)

Botones principales

BTN_SONADORA = "Soñadora" BTN_PARLAY_SEGURITO = "Parlay 🔒 Segurito" BTN_TOP_PICKS = "Top PICS" BTN_DEPORTES = "Deportes" BTN_CONFIG = "Configuración" BTN_AVISAME = "🔔 Avísame" BTN_VOLVER = "⬅️ Volver"

Submenú Configuración (según checkpoint robot): SOLO Notificaciones + Volver (teclado de acceso)

BTN_CFG_NOTIF = "Notificaciones"

Deportes (emojis acordados)

DEPORTES = [ "⚽ Fútbol", "⚾ Béisbol", "🏀 Baloncesto", "🎾 Tenis", "🏒 Hockey", "🏓 Ping Pong", "🏈 Americano", "🎮 e‑Sports", "🥋 MMA/UFC", "🥊 Boxeo", "🏎️ F1", ]

@dataclass class Menus: principal: Any configuracion: Any deportes: Any

def build_principal_menu() -> Any: kb = [ [KeyboardButton(text=BTN_SONADORA), KeyboardButton(text=BTN_PARLAY_SEGURITO)], [KeyboardButton(text=BTN_TOP_PICKS), KeyboardButton(text=BTN_DEPORTES)], [KeyboardButton(text=BTN_CONFIG), KeyboardButton(text=BTN_AVISAME)], ] return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Elige una opción…")

def build_config_menu() -> Any: # Submenú Configuración (solo entrada al panel inline + Volver) kb = [ [KeyboardButton(text=BTN_CFG_NOTIF)], [KeyboardButton(text=BTN_VOLVER)], ] return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Config…")

def build_deportes_menu() -> Any: filas: List[List[KeyboardButton]] = [] fila: List[KeyboardButton] = [] for i, dep in enumerate(DEPORTES, start=1): fila.append(KeyboardButton(text=dep)) if i % 2 == 0: filas.append(fila) fila = [] if fila: filas.append(fila) filas.append([KeyboardButton(text=BTN_VOLVER)]) return ReplyKeyboardMarkup(keyboard=filas, resize_keyboard=True, input_field_placeholder="Deportes…")

MENUS = Menus( principal=build_principal_menu(), configuracion=build_config_menu(), deportes=build_deportes_menu(), )

=============================

Persistencia y perfil de usuario + Sync con Google Sheets

=============================

DATA_DIR = Path(os.getenv("BOT_DATA_DIR", "data")) USERS_DIR = DATA_DIR / "users" USERS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PROFILE = { # ====== Campos mínimos que sincronizaremos a Sheets ====== # ver mapping en profile_to_row()/row_to_profile()

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

def _user_path(user_id: int) -> Path: return USERS_DIR / f"{user_id}.json"

def load_profile(user_id: int) -> dict: p = _user_path(user_id) if not p.exists(): return json.loads(json.dumps(DEFAULT_PROFILE)) try: return json.loads(p.read_text(encoding="utf-8")) except Exception: return json.loads(json.dumps(DEFAULT_PROFILE))

def _sheets_client(): if OFFLINE_MODE or not GSPREAD_OK: return None # creds por JSON embebido o por archivo creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON") creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") if creds_json: import json as _json from google.oauth2.service_account import Credentials  # type: ignore info = _json.loads(creds_json) scopes = [ "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly", ] creds = Credentials.from_service_account_info(info, scopes=scopes) return gspread.authorize(creds) if creds_path and os.path.exists(creds_path): from google.oauth2.service_account import Credentials  # type: ignore scopes = [ "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly", ] creds = Credentials.from_service_account_file(creds_path, scopes=scopes) return gspread.authorize(creds) return None

SHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "") CONFIG_SHEET = os.getenv("GOOGLE_SHEETS_CONFIG_SHEET", "CONFIG_USUARIOS")

Mapeo: perfil ➜ fila plana para Sheets

CONFIG_COLUMNS = [ "user_id","username","language","bankroll_mxn","stake_mode","stake_pct", "leagues_priority","markets_priority","sports_enabled","alerts_per_sport", "parlay_segurito.max_legs","parlay_segurito.min_odds","parlay_segurito.reuse_picks", "parlay_sonadora.max_legs","parlay_sonadora.min_odds","parlay_sonadora.reuse_picks", "notif.enabled","notif.start","notif.end","notif.p50","notif.p75","notif.result", "next_events_hours","hist_periods","google_sheets_sync","updated_at" ]

def profile_to_row(user_id: int, username: str | None, profile: dict) -> list[str]: lp = ",".join(profile.get("leagues_priority", [])) mp = ",".join(profile.get("markets_priority", [])) sports_on = [k for k,v in profile.get("sports_enabled", {}).items() if v] sports_csv = ",".join(sports_on) alerts_json = json.dumps(profile.get("alerts", {}), ensure_ascii=False) ps = profile.get("parlay_segurito", {}) pd = profile.get("parlay_sonadora", {}) n = profile.get("notifications", {}) hist = profile.get("historical_load", {}) return [ str(user_id), username or "", profile.get("language","es-MX"), str(profile.get("bankroll_mxn",0)), profile.get("stake_mode","fijo"), str(profile.get("stake_pct",0)), lp, mp, sports_csv, alerts_json, str(ps.get("max_legs",3)), str(ps.get("min_odds",1.8)), str(ps.get("reuse_picks",False)), str(pd.get("max_legs",8)), str(pd.get("min_odds",10.0)), str(pd.get("reuse_picks",True)), str(n.get("enabled",True)), str(n.get("start",True)), str(n.get("end",True)), str(n.get("progress_50",False)), str(n.get("progress_75",True)), str(n.get("result",True)), str(profile.get("next_events_hours",48)), str(hist.get("periods",3)), str(profile.get("google_sheets_sync",True)), dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") ]

def upsert_config_row(user_id: int, username: str | None, profile: dict) -> None: if OFFLINE_MODE or not GSPREAD_OK or not SHEET_ID or not profile.get("google_sheets_sync", True): return gc = _sheets_client() if not gc: return sh = gc.open_by_key(SHEET_ID) try: ws = sh.worksheet(CONFIG_SHEET) except Exception: ws = sh.add_worksheet(title=CONFIG_SHEET, rows=1000, cols=len(CONFIG_COLUMNS)) ws.update("1:1", [CONFIG_COLUMNS]) # buscar por user_id en columna A try: cell = ws.find(str(user_id)) row_idx = cell.row ws.update(f"A{row_idx}:{chr(64+len(CONFIG_COLUMNS))}{row_idx}", [profile_to_row(user_id, username, profile)]) except Exception: # append ws.append_row(profile_to_row(user_id, username, profile))

def save_profile(user_id: int, profile: dict) -> None: p = _user_path(user_id) p.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8") # sync opcional a Sheets try: upsert_config_row(user_id, None, profile) except Exception as e: logging.warning(f"Sheets sync falló: {e}")

=============================

Helpers de UI

=============================

def _fmt_bool(v: bool) -> str: return "✅" if v else "❌"

def alerts_inline_kb() -> Any: # Inline para activar/desactivar alertas globales return InlineKeyboardMarkup( inline_keyboard=[ [InlineKeyboardButton(text="Activar alertas", callback_data="alert_on")], [InlineKeyboardButton(text="Desactivar alertas", callback_data="alert_off")], ] )

def sport_inline_kb(nombre: str) -> Any: # Inline contextual por deporte return InlineKeyboardMarkup( inline_keyboard=[ [InlineKeyboardButton(text=f"Top PICS {nombre}", callback_data=f"sport_top::{nombre}")], [InlineKeyboardButton(text="Mercados", callback_data=f"sport_markets::{nombre}")], [InlineKeyboardButton(text="Avísame", callback_data=f"sport_alert::{nombre}")], ] )

def render_config_text(profile: dict) -> str: ps = profile.get("parlay_segurito", {}) pd = profile.get("parlay_sonadora", {}) notif = profile.get("notifications", {}) hours = profile.get("next_events_hours", 48) hist = profile.get("historical_load", {}) progress = hist.get("progress", {})

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

def build_config_keyboard(profile: dict) -> Any: kb: list[list[InlineKeyboardButton]] = [] # Bank & Stake kb.append([InlineKeyboardButton(text=f"Bank: {profile.get('bankroll_mxn',0)} MXN", callback_data=CB_INPUT+"bank")]) stake_mode = profile.get("stake_mode", "fijo") kb.append([ InlineKeyboardButton(text=f"Stake: {'Fijo '+str(profile.get('stake_pct',0))+'%' if stake_mode=='fijo' else 'Automático'}", callback_data=CB_INPUT+("stake_pct" if stake_mode=='fijo' else "stake_mode")), InlineKeyboardButton(text=("Cambiar a Automático" if stake_mode=="fijo" else "Cambiar a Fijo"), callback_data=CB_TOGGLE+"stake_mode"), ]) # Parlay Segurito ps = profile.get("parlay_segurito", {}) kb.append([ InlineKeyboardButton(text=f"Segurito Legs: {ps.get('max_legs',3)}", callback_data=CB_INPUT+"ps_legs"), InlineKeyboardButton(text=f"Min cuota: {ps.get('min_odds',1.8)}", callback_data=CB_INPUT+"ps_odds"), ]) kb.append([ InlineKeyboardButton(text=f"Reutilizar: {_fmt_bool(ps.get('reuse_picks',False))}", callback_data=CB_TOGGLE+"ps_reuse") ]) # Parlay Soñador pd = profile.get("parlay_sonadora", {}) kb.append([ InlineKeyboardButton(text=f"Soñador Legs: {pd.get('max_legs',8)}", callback_data=CB_INPUT+"pd_legs"), InlineKeyboardButton(text=f"Min cuota: {pd.get('min_odds',10.0)}", callback_data=CB_INPUT+"pd_odds"), ]) kb.append([ InlineKeyboardButton(text=f"Reutilizar: {_fmt_bool(pd.get('reuse_picks',True))}", callback_data=CB_TOGGLE+"pd_reuse") ]) # Notificaciones notif = profile.get("notifications", {}) kb.append([ InlineKeyboardButton(text=f"Global {_fmt_bool(notif.get('enabled',True))}", callback_data=CB_TOGGLE+"n_enabled"), InlineKeyboardButton(text=f"Inicio {_fmt_bool(notif.get('start',True))}", callback_data=CB_TOGGLE+"n_start"), InlineKeyboardButton(text=f"Fin {_fmt_bool(notif.get('end',True))}", callback_data=CB_TOGGLE+"n_end"), ]) kb.append([ InlineKeyboardButton(text=f"50% {_fmt_bool(notif.get('progress_50',False))}", callback_data=CB_TOGGLE+"n_50"), InlineKeyboardButton(text=f"75% {_fmt_bool(notif.get('progress_75',True))}", callback_data=CB_TOGGLE+"n_75"), InlineKeyboardButton(text=f"Resultado {_fmt_bool(notif.get('result',True))}", callback_data=CB_TOGGLE+"n_result"), ]) # Próximas Fechas kb.append([ InlineKeyboardButton(text=f"Próximos: {profile.get('next_events_hours',48)}h", callback_data=CB_INPUT+"next_hours") ]) # Deportes (una línea por 3-4 deportes) row: list[InlineKeyboardButton] = [] for i, dep in enumerate(DEPORTES, 1): on = profile["sports_enabled"].get(dep, False) row.append(InlineKeyboardButton(text=f"{('✅' if on else '❌')} {dep}", callback_data=CB_TOGGLE+f"sport::{dep}")) if i % 3 == 0: kb.append(row); row = [] if row: kb.append(row) # Históricos hist = profile.get("historical_load", {}) kb.append([ InlineKeyboardButton(text=f"Periodo: {hist.get('periods',3)}", callback_data=CB_INPUT+"hist_periods") ]) for dep in list(hist.get("progress", {}).keys())[:6]: kb.append([InlineKeyboardButton(text=f"🔄 Actualizar {dep}", callback_data=CB_ACTION+f"hist_load::{dep}")]) # Sheets kb.append([ InlineKeyboardButton(text=f"Google Sheets {_fmt_bool(profile.get('google_sheets_sync', True))}", callback_data=CB_TOGGLE+"sheets") ]) # Volver kb.append([InlineKeyboardButton(text="« Volver", callback_data=CB_ACTION+"back")]) return InlineKeyboardMarkup(inline_keyboard=kb)

=============================

Comandos estilo BotFather

=============================

BOT_COMMANDS = [ BotCommand(command="start", description="Abrir menú principal"), BotCommand(command="help", description="Ayuda y comandos"), BotCommand(command="menu", description="Reenviar menú principal"), BotCommand(command="deportes", description="Abrir submenú de deportes"), BotCommand(command="config", description="Abrir configuración"), ]

=============================

Modo online (aiogram) — Handlers y arranque

=============================

if not OFFLINE_MODE: bot = Bot(TOKEN or "", default=DefaultBotProperties(parse_mode=ParseMode.HTML)) dp = Dispatcher()

def _uid(message: Message) -> int:
    return message.from_user.id if message.from_user else 0

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

" "Selecciona una opción del menú.

" "Consejo: escribe <code>/help</code> para ver los comandos." ), reply_markup=MENUS.principal, )

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

@dp.message(F.text == BT