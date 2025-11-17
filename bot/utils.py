from bot.database import get_player_stats
from bot.rooms import save_rooms
import logging
import asyncio
import random
import os
import json
import time
import psutil
import aiosqlite
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeAllPrivateChats, FSInputFile, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, PreCheckoutQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import uuid
import aiohttp
import tenacity
from collections import deque
from bot.database import db_init, get_player_stats, DB_PATH
from bot.rooms import load_rooms, cleanup_rooms, save_rooms, rooms, user_message_times
from bot.game import matchmaking_processor

# Решта коду utils.py без змін
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(admin_id.strip()) for admin_id in os.getenv('ADMIN_ID').split(',')]
USE_POLLING = os.getenv('USE_POLLING', 'false').lower() == 'true'
RENDER_EXTERNAL_HOSTNAME = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
maintenance_mode = False
active_users = set()
rooms = {}
user_message_times = {}
matchmaking_queue = []
maintenance_timer_task = None
DB_PATH = os.getenv('RENDER_DISK_PATH', '') + '/players.db' if os.getenv('RENDER_DISK_PATH') else 'players.db'
BASE_LOCATIONS = [
    "Аеропорт", "Банк", "Пляж", "Казино", "Цирк", "Школа", "Лікарня",
    "Готель", "Музей", "Ресторан", "Театр", "Парк", "Космічна станція",
    "Підвал", "Океан", "Острів", "Кафе", "Аквапарк", "Магазин", "Аптека",
    "Зоопарк", "Місяць", "Річка", "Озеро", "Море", "Ліс", "Храм",
    "Поле", "Село", "Місто", "Ракета", "Атомна станція", "Ферма",
    "Водопад", "Спа салон", "Квартира", "Метро", "Каналізація", "Порт"
]
PACKS = {
    "fantasy": ["Замок", "Ліс ельфів", "Печера дракона", "Магічна академія"],
    "sci_fi": ["Космічний корабель", "Планета інопланетян", "Лабораторія майбутнього", "Роботичний завод"]
}
CALLSIGNS = [
    "Бобр Курва", "Кличко", "Фенікс", "Шашлик", "Мамкін хакер", "Сігма", "Деві Джонс", "Курт Кобейн",
    "Шрек", "Тигр", "Тарас", "Він Дізель", "Дикий борщ", "Раян Гослінг", "Том Круз", "Лео Ді Капрізник",
    "Місцевий свата", "Банан4ік", "Мегагей", "Туалетний Філософ", "Свій Шпигун", "Не Шпигун", "Санечка",
    "Скала", "Захар Кокос", "Козак", "Чорний", "Аня 15см", "Анімешнік", "Джамал", "Ловець Натуралів",
    "Натурал", "Санс", "Гетеросексуал", "Рікрол", "Сапорт", "Туалетний Монстр", "456", "Скажений Пельмень"
]
last_save_time = 0
SAVE_INTERVAL = 5
ROOM_EXPIRY = 3600
XP_CIVILIAN_WIN = 10
XP_SPY_WIN = 20
MESSAGE_MAX_LENGTH = 120
SHOP_ITEMS = {  # Дублюю тут, якщо потрібно, але в payments.py
    "VIP_1D": {"title": "VIP на 1 день", "description": "Отримати преміум статус на 1 день.", "price": 1, "payload": "vip_1d", "duration": 86400},
    "PACK_FANTASY": {"title": "Набір Fantasy", "description": "Додатковий набір локацій: Fantasy.", "price": 1, "payload": "pack_fantasy"},
    "BOOST_SPY": {"title": "Буст Шпигуна", "description": "Збільшити шанси стати шпигуном в наступній грі.", "price": 1, "payload": "boost_spy"}
}
kb_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Знайти Гру")],
        [KeyboardButton(text="🚪 Створити Кімнату"), KeyboardButton(text="🤝 Приєднатися")],
        [KeyboardButton(text="📊 Моя Статистика"), KeyboardButton(text="❓ Допомога")],
        [KeyboardButton(text="🛍️ Магазин")]
    ],
    resize_keyboard=True
)
kb_in_queue = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Скасувати Пошук")]],
    resize_keyboard=True
)
kb_in_lobby = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🚪 Покинути Лобі")]],
    resize_keyboard=True
)
kb_in_game = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="❓ Моя роль"), KeyboardButton(text="🗳️ Достр. Голосування")],
        [KeyboardButton(text="🚪 Покинути Гру")]
    ],
    resize_keyboard=True
)
cmds_default = [
    BotCommand(command="start", description="Головне меню"),
    BotCommand(command="find_match", description="Швидкий пошук гри"),
    BotCommand(command="create", description="Створити приватну кімнату"),
    BotCommand(command="join", description="Приєднатися до кімнати"),
    BotCommand(command="stats", description="Моя статистика"),
    BotCommand(command="leave", description="Покинути кімнату/гру"),
    BotCommand(command="my_info", description="Моя роль"),
    BotCommand(command="early_vote", description="Дострокове голосування"),
    BotCommand(command="set_pack", description="Вибрати набір локацій (для власників кімнат)"),
    BotCommand(command="shop", description="Адмін: Переглянути магазин (тест)"),
    BotCommand(command="purchases", description="Адмін: Переглянути покупки"),
    BotCommand(command="refund", description="Адмін: Refund покупки")
]
process = psutil.Process()
logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
async def health_check(request):
    logger.info(f"Health check: {request.method} {request.path}")
    try:
        info = await bot.get_webhook_info()
        memory_usage = process.memory_info().rss / 1024 / 1024
        logger.info(f"Webhook status: {info}, Memory usage: {memory_usage:.2f} MB")
        return web.Response(text=f"OK\nWebhook: {info}", status=200)
    except Exception as e:
        logger.error(f"Health check error: {e}", exc_info=True)
        return web.Response(text=f"ERROR: {e}", status=500)

async def check_webhook_periodically():
    await asyncio.sleep(20)
    while True:
        try:
            webhook_host = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
            webhook_url = f"https://{webhook_host}/webhook"
            info = await bot.get_webhook_info()
            logger.info(f"Periodic webhook check: {info.url}")
            if not info.url or info.url != webhook_url:
                logger.warning(f"Webhook is NOT SET or incorrect. Re-setting! Current: {info.url}, Expected: {webhook_url}")
                await set_webhook_with_retry(webhook_url)
            await asyncio.sleep(120)
        except Exception as e:
            logger.error(f"Periodic webhook check failed: {e}", exc_info=True)
            await asyncio.sleep(120)

@tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_exponential(multiplier=2, min=5, max=60),
    retry=tenacity.retry_if_exception_type(aiohttp.ClientError),
    before_sleep=lambda retry_state: logger.info(f"Retrying webhook setup, attempt {retry_state.attempt_number}")
)
async def set_webhook_with_retry(webhook_url):
    logger.info(f"Attempting to set webhook: {webhook_url}")
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(webhook_url, drop_pending_updates=True, max_connections=100, request_timeout=30)
    webhook_info = await bot.get_webhook_info()
    logger.info(f"Webhook set, current info: {webhook_info}")
    if not webhook_info.url:
        logger.error("Webhook URL is empty after setting!")
        raise aiohttp.ClientError("Webhook URL is still empty after setting")
    logger.info(f"Webhook successfully set to {webhook_url}")

async def get_user_from_event(event):
    if isinstance(event, types.Message):
        user = event.from_user
    elif isinstance(event, types.CallbackQuery):
        user = event.from_user
    else:
        return None, None
    username = f"@{user.username}" if user.username else user.first_name
    return user.id, username

async def check_ban_and_reply(event):
    user_id, username = await get_user_from_event(event)
    if not user_id:
        return False
    if user_id in ADMIN_IDS:
        return False
    try:
        stats = await get_player_stats(user_id, username)
        banned_until = stats[6]
        if banned_until == -1:
            reply_text = "Ви заблоковані назавжди."
        elif banned_until > time.time():
            remaining = timedelta(seconds=int(banned_until - time.time()))
            reply_text = f"Ви заблоковані. Залишилось: {remaining}"
        else:
            return False
        if isinstance(event, types.Message):
            await event.reply(reply_text)
        elif isinstance(event, types.CallbackQuery):
            await event.answer(reply_text, show_alert=True)
        return True
    except Exception as e:
        logger.error(f"Failed to check ban status for {user_id}: {e}")
        return False

def parse_ban_time(time_str: str) -> int:
    current_time = int(time.time())
    if time_str == 'perm':
        return -1
    duration_seconds = 0
    try:
        if time_str.endswith('m'):
            duration_seconds = int(time_str[:-1]) * 60
        elif time_str.endswith('h'):
            duration_seconds = int(time_str[:-1]) * 3600
        elif time_str.endswith('d'):
            duration_seconds = int(time_str[:-1]) * 86400
        else:
            return 0
    except ValueError:
        return 0
    return current_time + duration_seconds

async def on_startup(_):
    logger.info("Starting bot initialization")
    await db_init()
    load_rooms()
    logger.info("Bot initialization completed")
    webhook_info = await bot.get_webhook_info()
    logger.info(f"Webhook status after startup: {webhook_info}")
    await bot.set_my_commands(cmds_default, scope=BotCommandScopeAllPrivateChats())
    logger.info("Default slash commands set for all users.")

async def on_shutdown(_):
    logger.info("Shutting down server...")
    save_rooms()
    for token, room in list(rooms.items()):
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
    await bot.session.close()
    logger.info("Bot session closed. Shutdown successful.")