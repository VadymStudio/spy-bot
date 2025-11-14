import logging
import asyncio
import random
import os
import json
import time
import psutil
import aiosqlite  # Потрібно для бази даних
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F  # --- НОВЕ: F для фільтрів тексту ---
# --- ФІКС: Додаємо StorageKey ---
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
# --- НОВЕ: ReplyKeyboardMarkup ---
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeChat,
    FSInputFile, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web, ClientSession
import uuid
import aiohttp
import tenacity
from collections import deque

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Завантажуємо змінні з .env
load_dotenv()
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")

# --- ЗМІНЕНО: Завантажуємо список адмінів ---
ADMIN_IDS_STR = os.getenv('ADMIN_ID')
if not ADMIN_IDS_STR:
    raise ValueError("ADMIN_ID is not set in environment variables. Please set it (comma-separated if multiple).")

ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',')]
logger.info(f"Loaded Admin IDs: {ADMIN_IDS}")
# --- Кінець зміни ---

USE_POLLING = os.getenv('USE_POLLING', 'false').lower() == 'true'
RENDER_EXTERNAL_HOSTNAME = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)

# --- Глобальні змінні ---
maintenance_mode = False
active_users = set()
rooms = {}
user_message_times = {}
matchmaking_queue = []  # --- ЗМІНЕНО: Тепер зберігає (user_id, username, timestamp) ---
maintenance_timer_task = None
DB_PATH = os.getenv('RENDER_DISK_PATH', '') + '/players.db' if os.getenv('RENDER_DISK_PATH') else 'players.db'  # Використовуємо persistent disk на Render

class PlayerState(StatesGroup):
    in_queue = State()
    waiting_for_token = State()

class AdminState(StatesGroup):
    waiting_for_db_file = State()

# --- Константи ---
LOCATIONS = [
    "Аеропорт", "Банк", "Пляж", "Казино", "Цирк", "Школа", "Лікарня",
    "Готель", "Музей", "Ресторан", "Театр", "Парк", "Космічна станція",
    "Підвал", "Океан", "Острів", "Кафе", "Аквапарк", "Магазин", "Аптека",
    "Зоопарк", "Місяць", "Річка", "Озеро", "Море", "Ліс", "Храм",
    "Поле", "Село", "Місто", "Ракета", "Атомна станція", "Ферма",
    "Водопад", "Спа салон", "Квартира", "Метро", "Каналізація", "Порт"
]
CALLSIGNS = [
    "Бобр Курва", "Кличко", "Фенікс", "Шашлик", "Мамкін хакер", "Сігма", "Деві Джонс", "Курт Кобейн",
    "Шрек", "Тигр", "Тарас", "Він Дізель", "Дикий борщ", "Раян Гослінг", "Том Круз", "Лео Ді Капрізник",
    "Місцевий свата", "Банан4ік", "Мегагей", "Туалетний Філософ", "Свій Шпигун", "Не Шпигун", "Санечка",
    "Скала", "Захар Кокос", "Козак", "Чорний", "Аня 15см", "Анімешнік", "Джамал", "Ловець Натуралів",
    "Натурал", "Санс", "Гетеросексуал", "Рікрол", "Сапорт", "Туалетний Монстр", "456", "Скажений Пельмень"
]
last_save_time = 0
SAVE_INTERVAL = 5
ROOM_EXPIRY = 3600  # 1 година
XP_CIVILIAN_WIN = 10
XP_SPY_WIN = 20
MESSAGE_MAX_LENGTH = 120  # НОВЕ: Обмеження на довжину повідомлення

# --- НОВЕ: Визначаємо наші постійні клавіатури ---
kb_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Знайти Гру")],
        [KeyboardButton(text="🚪 Створити Кімнату"), KeyboardButton(text="🤝 Приєднатися")],
        [KeyboardButton(text="📊 Моя Статистика"), KeyboardButton(text="❓ Допомога")]
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

# --- НОВЕ: Визначаємо списки слеш-команд ---
cmds_default = [
    BotCommand(command="start", description="Головне меню"),
    BotCommand(command="find_match", description="Швидкий пошук гри"),
    BotCommand(command="create", description="Створити приватну кімнату"),
    BotCommand(command="join", description="Приєднатися до кімнати"),
    BotCommand(command="stats", description="Моя статистика"),
]
cmds_in_lobby = [
    BotCommand(command="leave", description="Покинути лобі")
]
cmds_in_game = [
    BotCommand(command="my_info", description="Нагадати мою роль/локацію"),
    BotCommand(command="early_vote", description="Дострокове голосування"),
    BotCommand(command="leave", description="Покинути гру")
]

# --- НОВЕ: Хелпер-функції для встановлення команд ---
async def set_default_commands_for_user(bot: Bot, user_id: int):
    """Встановлює дефолтний набір команд для юзера (головне меню)."""
    try:
        await bot.set_my_commands(cmds_default, scope=BotCommandScopeChat(chat_id=user_id))
    except Exception as e:
        logger.warning(f"Failed to set default commands for {user_id}: {e}")

async def set_lobby_commands_for_user(bot: Bot, user_id: int):
    """Встановлює команди для лобі."""
    try:
        await bot.set_my_commands(cmds_in_lobby, scope=BotCommandScopeChat(chat_id=user_id))
    except Exception as e:
        logger.warning(f"Failed to set lobby commands for {user_id}: {e}")

async def set_game_commands_for_user(bot: Bot, user_id: int):
    """Встановлює команди для гри."""
    try:
        await bot.set_my_commands(cmds_in_game, scope=BotCommandScopeChat(chat_id=user_id))
    except Exception as e:
        logger.warning(f"Failed to set game commands for {user_id}: {e}")

# Логування
logger.info(f"Using aiohttp version: {aiohttp.__version__}")
process = psutil.Process()
logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# --- Функції Бази Даних (SQLite) ---
async def db_init():
    """Ініціалізує базу даних та додає колонку `banned_until`, якщо її немає."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0,
                banned_until INTEGER DEFAULT 0
            )
        ''')
       
        try:
            await db.execute("ALTER TABLE players ADD COLUMN banned_until INTEGER DEFAULT 0")
            logger.info("Added 'banned_until' column to players table.")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e):
                pass
            else:
                raise e
        await db.commit()
    logger.info("Database initialized successfully.")

async def get_player_stats(user_id, username):
    """Отримує статистику гравця (включаючи бан). Створює або оновлює, якщо не існує."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''INSERT INTO players (user_id, username) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
            ''', (user_id, username)
        )
        await db.commit()
       
        async with db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)) as cursor:
            player = await cursor.fetchone()
           
        if player is None:
            logger.error(f"Failed to create or find player {user_id}")
            return (user_id, username, 0, 0, 0, 0, 0)
           
        return player

async def update_player_stats(user_id, is_spy, is_winner):
    """Оновлює статистику гравця після гри."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT total_xp, games_played, spy_wins, civilian_wins FROM players WHERE user_id = ?", (user_id,)) as cursor:
                stats = await cursor.fetchone()
                if not stats:
                    logger.warning(f"Could not update stats: Player {user_id} not found.")
                    return
           
            total_xp, games_played, spy_wins, civilian_wins = stats
           
            games_played += 1
           
            if is_winner:
                if is_spy:
                    spy_wins += 1
                    total_xp += XP_SPY_WIN
                else:
                    civilian_wins += 1
                    total_xp += XP_CIVILIAN_WIN
           
            await db.execute(
                "UPDATE players SET total_xp = ?, games_played = ?, spy_wins = ?, civilian_wins = ? WHERE user_id = ?",
                (total_xp, games_played, spy_wins, civilian_wins, user_id)
            )
            await db.commit()
            logger.info(f"Stats updated for {user_id}. XP: {total_xp}, Games: {games_played}")
           
    except Exception as e:
        logger.error(f"Failed to update stats for {user_id}: {e}", exc_info=True)

# --- Функції Рівнів та XP ---
xp_level_cache = {}

def get_level_from_xp(total_xp):
    if total_xp < 20:
        return 1, 20, total_xp, 0  # (Рівень, XP до наступного, XP в поточному, XP для старту рівня)
    if total_xp in xp_level_cache:
        return xp_level_cache[total_xp]
    level = 1
    xp_needed_for_next = 20
    current_total_xp_needed = 0
   
    multiplier = 1.50
    while True:
        current_total_xp_needed += xp_needed_for_next
        level += 1
       
        if total_xp < current_total_xp_needed:
            level -= 1
            xp_at_level_start = current_total_xp_needed - xp_needed_for_next
            xp_in_level = total_xp - xp_at_level_start
            xp_level_cache[total_xp] = (level, xp_needed_for_next, xp_in_level, xp_at_level_start)
            return level, xp_needed_for_next, xp_in_level, xp_at_level_start
           
        xp_needed_for_next = int(xp_needed_for_next * multiplier)
       
        if multiplier > 1.20:
            multiplier = max(1.20, multiplier - 0.02)

# --- Функції збереження кімнат та очистки ---
def save_rooms():
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time < SAVE_INTERVAL:
        return
    try:
        room_copy = {}
        for token, room in rooms.items():
            # Робимо копію і конвертуємо set в list для JSON
            room_copy[token] = room.copy()
            room_copy[token]['banned_from_voting'] = list(room['banned_from_voting'])
            room_copy[token]['voters'] = list(room['voters'])
            room_copy[token]['messages'] = room_copy[token]['messages'][-100:]  # Зберігаємо тільки 100 ост.
           
            # Видаляємо об'єкти, які не серіалізуються
            room_copy[token].pop('timer_task', None)
            room_copy[token].pop('spy_guess_timer_task', None)  # Видаляємо новий таймер
        with open('rooms.json', 'w') as f:
            json.dump(room_copy, f, indent=4)
        last_save_time = current_time
        logger.info("Rooms saved to rooms.json")
    except Exception as e:
        logger.error(f"Failed to save rooms: {e}", exc_info=True)

def load_rooms():
    global rooms
    try:
        if os.path.exists('rooms.json'):
            with open('rooms.json', 'r') as f:
                loaded_rooms = json.load(f)
                rooms = {k: v for k, v in loaded_rooms.items()}
                for room in rooms.values():
                    # Відновлюємо set з list
                    room['banned_from_voting'] = set(room['banned_from_voting'])
                    room['voters'] = set(room['voters'])
                    # Перетворюємо ключі 'votes' назад в int
                    room['votes'] = {int(k): int(v) for k, v in room['votes'].items()}
                    # Скидаємо таймери
                    room['timer_task'] = None
                    room['spy_guess_timer_task'] = None
                    # Встановлюємо last_activity, щоб кімнати не видалились одразу
                    room['last_activity'] = time.time()
                    room['created_at'] = room.get('created_at', time.time())
                logger.info("Rooms loaded from rooms.json")
    except Exception as e:
        logger.error(f"Failed to load rooms: {e}", exc_info=True)

async def cleanup_rooms():
    while True:
        try:
            current_time = time.time()
            expired = []
            for token, room in list(rooms.items()):
                if room.get('game_started'):
                    continue
               
                if current_time - room.get('last_activity', current_time) > ROOM_EXPIRY:
                    expired.append(token)
                   
            for token in expired:
                room = rooms.get(token)
                if room:
                    if room.get('timer_task') and not room['timer_task'].done():
                        room['timer_task'].cancel()
                    if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
                        room['spy_guess_timer_task'].cancel()
                if token in rooms:
                    del rooms[token]
                    logger.info(f"Removed expired/finished room: {token}")
           
            expired_users = [uid for uid, data in user_message_times.items() if current_time - data.get('last_seen', 0) > 3600]
            for uid in expired_users:
                del user_message_times[uid]
           
            save_rooms()
            memory_usage = process.memory_info().rss / 1024 / 1024
            logger.info(f"Cleanup complete. Memory usage: {memory_usage:.2f} MB, Active rooms: {len(rooms)}")
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Cleanup rooms error: {e}", exc_info=True)
            await asyncio.sleep(300)

# --- Функції для Render (Keep-alive, Webhook) ---
async def keep_alive():
    async with ClientSession() as session:
        while True:
            try:
                webhook_host = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
                logger.info(f"Sending keep-alive ping to https://{webhook_host}/health")
                async with session.get(f"https://{webhook_host}/health") as resp:
                    logger.info(f"Keep-alive ping response: {resp.status}")
            except Exception as e:
                logger.error(f"Keep-alive error: {e}", exc_info=True)
            await asyncio.sleep(300)

async def health_check(request):
    logger.info(f"Health check received: {request.method} {request.path}")
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

# --- Функції Бану ---
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

# --- Команди Адміністратора ---
async def check_maintenance(message: types.Message):
    if maintenance_mode and message.from_user.id not in ADMIN_IDS:
        await message.reply("Бот на технічному обслуговуванні. Зачекайте, будь ласка.")
        return True
    return False

async def start_maintenance_now():
    global maintenance_mode, rooms
    maintenance_mode = True
   
    all_user_ids = set()
    for token, room in list(rooms.items()):
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
           
        logger.info(f"Cancelled timers for room {token} during maintenance")
        for pid, _, _ in room['participants']:
            if pid > 0:
                all_user_ids.add(pid)
               
    rooms.clear()
    save_rooms()
   
    logger.info("Maintenance mode ON. All rooms cleared.")
   
    for user_id in all_user_ids:
        try:
            await bot.send_message(user_id, "Увага! Бот переходить на технічне обслуговування. Усі ігри завершено.")
        except Exception as e:
            logger.error(f"Failed to send maintenance_on message to {user_id}: {e}")

@dp.message(Command("maintenance_on"))
async def maintenance_on(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Ви не адміністратор!")
        return
   
    global maintenance_timer_task
    if maintenance_timer_task and not maintenance_timer_task.done():
        maintenance_timer_task.cancel()
        maintenance_timer_task = None
       
    await start_maintenance_now()
    await message.reply("Технічне обслуговування увімкнено. Всі кімнати очищено.")

@dp.message(Command("maintenance_off"))
async def maintenance_off(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Ви не адміністратор!")
        return
    global maintenance_mode
    maintenance_mode = False
    active_users.add(message.from_user.id)
    await message.reply("Технічне обслуговування вимкнено.")

async def send_maint_warning(text: str):
    all_user_ids = set()
    for room in rooms.values():
        for pid, _, _ in room['participants']:
            if pid > 0:
                all_user_ids.add(pid)
   
    logger.info(f"Sending maintenance warning to {len(all_user_ids)} users: {text}")
    for uid in all_user_ids:
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass

async def run_maintenance_timer():
    global maintenance_timer_task
    try:
        await send_maint_warning("Увага! Заплановані технічні роботи.\nВсі ігри будуть зупинені через **10 хвилин**.")
        await asyncio.sleep(300)  # 5 хв
       
        await send_maint_warning("Повторне попередження: Технічні роботи почнуться через **5 хвилин**.")
        await asyncio.sleep(240)  # 4 хв
       
        await send_maint_warning("Останнє попередження! Технічні роботи почнуться через **1 хвилину**.")
        await asyncio.sleep(60)  # 1 хв
       
        await send_maint_warning("Починаємо технічні роботи...")
        await start_maintenance_now()
       
    except asyncio.CancelledError:
        logger.info("Maintenance timer was cancelled.")
        await send_maint_warning("Таймер технічних робіт скасовано.")
    except Exception as e:
        logger.error(f"Maintenance timer failed: {e}", exc_info=True)
    finally:
        maintenance_timer_task = None

@dp.message(Command("maint_timer"))
async def start_maint_timer(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
   
    global maintenance_timer_task
    if maintenance_timer_task and not maintenance_timer_task.done():
        await message.reply("Таймер вже запущено.")
        return
       
    maintenance_timer_task = asyncio.create_task(run_maintenance_timer())
    await message.reply("Запущено 10-хвилинний таймер до технічних робіт.\nЩоб скасувати: /cancel_maint")

@dp.message(Command("cancel_maint"))
async def cancel_maint_timer(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
       
    global maintenance_timer_task
    if not maintenance_timer_task or maintenance_timer_task.done():
        await message.reply("Таймер не запущено.")
        return
       
    maintenance_timer_task.cancel()
    maintenance_timer_task = None
    await message.reply("Таймер технічних робіт скасовано.")

@dp.message(Command("check_webhook"))
async def check_webhook(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
    try:
        info = await bot.get_webhook_info()
        await message.reply(f"Webhook info: {info}")
    except Exception as e:
        await message.reply(f"Error checking webhook: {e}")

@dp.message(Command("reset_state"))
async def reset_state(message: types.Message, state: FSMContext):
    if message.from_user.id in ADMIN_IDS:
        try:
            await state.clear()
            await message.reply("Стан FSM скинуто.", reply_markup=kb_main_menu)
            await set_default_commands_for_user(bot, message.from_user.id)
        except Exception as e:
            await message.reply(f"Помилка при скиданні стану: {e}")
    else:
        if await check_ban_and_reply(message): return
       
        for room in rooms.values():
            if message.from_user.id in [p[0] for p in room['participants']]:
                await message.reply("Ви не можете скинути стан, перебуваючи в кімнаті. Напишіть /leave.")
                return
        try:
            await state.clear()
            await message.reply("Ваш стан скинуто. Ви можете приєднатися до гри.", reply_markup=kb_main_menu)
            await set_default_commands_for_user(bot, message.from_user.id)
        except Exception as e:
            await message.reply(f"Помилка при скиданні стану: {e}")

@dp.message(Command("testgame"))
async def test_game(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    if await check_maintenance(message):
        return
   
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошук: /cancel_match")
        return
       
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"Admin {user_id} starting test game (BOT IS SPY)")
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    room_token = f"test_{uuid.uuid4().hex[:4]}"
    participants = [ (user_id, username, None), (-1, "Бот Василь", None), (-2, "Бот Степан", None), (-3, "Бот Галина", None) ]
   
    rooms[room_token] = {
        'owner': user_id, 'participants': participants, 'game_started': False, 'is_test_game': True,
        'spy': None, 'location': None, 'messages': [], 'votes': {}, 'banned_from_voting': set(),
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'spy_guess_timer_task': None,
        'last_activity': time.time(), 'last_minute_chat': False, 'waiting_for_spy_guess': False,
        'spy_guess': None, 'votes_for': 0, 'votes_against': 0, 'created_at': time.time(),
        'results_processed': False
    }
   
    room = rooms[room_token]
    await start_game_logic(room, room_token, admin_is_spy=False)
    await message.reply(f"Тестову кімнату створено: {room_token}\nШпигун: {room['spy']} (Бот)\nЛокація: {room['location']}", reply_markup=kb_in_game)

@dp.message(Command("testgamespy"))
async def test_game_as_spy(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    if await check_maintenance(message):
        return
       
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошук: /cancel_match")
        return
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"Admin {user_id} starting test game (ADMIN IS SPY)")
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    room_token = f"test_spy_{uuid.uuid4().hex[:4]}"
    participants = [ (user_id, username, None), (-1, "Бот Василь", None), (-2, "Бот Степан", None), (-3, "Бот Галина", None) ]
   
    rooms[room_token] = {
        'owner': user_id, 'participants': participants, 'game_started': False, 'is_test_game': True,
        'spy': None, 'location': None, 'messages': [], 'votes': {}, 'banned_from_voting': set(),
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'spy_guess_timer_task': None,
        'last_activity': time.time(), 'last_minute_chat': False, 'waiting_for_spy_guess': False,
        'spy_guess': None, 'votes_for': 0, 'votes_against': 0, 'created_at': time.time(),
        'results_processed': False
    }
   
    room = rooms[room_token]
    await start_game_logic(room, room_token, admin_is_spy=True)
    await message.reply(f"Тестову кімнату створено: {room_token}\nШпигун: {room['spy']} (ВИ)\nЛокація: {room['location']}", reply_markup=kb_in_game)

@dp.message(Command("whois"))
async def whois_spy(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        logger.warning(f"Non-admin user {message.from_user.id} tried to use /whois")
        return
   
    user_id = message.from_user.id
    user_room = None
   
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            user_room = room
            break
           
    if not user_room or not user_room['game_started']:
        await message.reply("[DEBUG] Ви не в активній грі.")
        return
    try:
        if user_id == user_room['spy']:
            await message.reply(f"[DEBUG] Локація: {user_room['location']}")
        else:
            spy_id = user_room['spy']
            spy_info = next((p for p in user_room['participants'] if p[0] == spy_id), None)
            if spy_info:
                await message.reply(f"[DEBUG] Шпигун: {spy_info[1]} ({spy_info[2]})")
            else:
                await message.reply(f"[DEBUG] Не можу знайти шпигуна (ID: {spy_id}).")
    except Exception as e:
        logger.error(f"Failed to send /whois info to admin: {e}")
        await message.reply(f"[DEBUG] Помилка: {e}")

@dp.message(Command("getdb"))
async def get_database_file(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        logger.warning(f"Non-admin user {message.from_user.id} tried to use /getdb")
        return
    try:
        if not os.path.exists(DB_PATH):
            await message.reply("Файл бази даних `players.db` ще не створено. Зіграйте хоча б одну гру.")
            return
           
        db_file = FSInputFile(DB_PATH)
        await message.reply_document(db_file, caption="Ось твоя база даних `players.db`.")
        logger.info(f"Admin {message.from_user.id} successfully requested DB file.")
       
    except Exception as e:
        logger.error(f"Failed to send DB file to admin: {e}", exc_info=True)
        await message.reply(f"Не вдалося надіслати файл: {e}")

# --- ДОДАЙ ЦІ ДВІ ФУНКЦІЇ ---

@dp.message(Command("updatedb"))
async def request_db_update(message: types.Message, state: FSMContext):
    """
    (ТІЛЬКИ АДМІН) Активує режим очікування файлу .db
    """
    if message.from_user.id not in ADMIN_IDS:
        return # Ігноруємо не-адмінів

    await message.reply("Переводжу в режим оновлення бази. Будь ласка, надішліть файл `players.db`.\n"
                        "УВАГА: Поточна база на сервері буде **ПОВНІСТЮ ЗАМІНЕНА**.\n"
                        "Для скасування просто нічого не надсилайте або напишіть /cancel.")
    await state.set_state(AdminState.waiting_for_db_file)

@dp.message(F.document, StateFilter(AdminState.waiting_for_db_file))
async def process_db_upload(message: types.Message, state: FSMContext):
    """
    (ТІЛЬКИ АДМІН) Ловить файл у стані waiting_for_db_file
    """
    if message.from_user.id not in ADMIN_IDS:
        await state.clear()
        return # Подвійна перевірка

    if message.document.file_name != 'players.db':
        await message.reply(f"❌ Помилка. Очікувався файл `players.db`, але отримано `{message.document.file_name}`.\nОновлення скасовано.")
        await state.clear()
        return

    try:
        await message.reply(f"✅ Отримав `{message.document.file_name}`. Починаю завантаження на сервер...")
        
        # Завантажуємо файл з серверів Telegram
        # file_info = await bot.get_file(message.document.file_id)
        
        # Зберігаємо файл поверх старого DB_PATH (players.db)
        await bot.download(message.document, DB_PATH) 
        
        await message.reply("🚀 Успіх! Базу даних на сервері оновлено. "
                            "Зміни вступлять в силу для нових ігор та гравців. "
                            "Для 100% ефекту краще перезапустити бота (/maint_timer).")
        logger.info(f"Admin {message.from_user.id} successfully updated players.db")

    except Exception as e:
        logger.error(f"Failed to update DB: {e}", exc_info=True)
        await message.reply(f"Помилка під час збереження файлу: {e}")
    finally:
        await state.clear()

@dp.message(Command("getlog"))
async def get_game_log(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        logger.warning(f"Non-admin user {message.from_user.id} tried to use /getlog")
        return
       
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Потрібно вказати токен кімнати: /getlog <token>")
        return
       
    token = args[1].lower().strip()
   
    room = rooms.get(token)
    if not room:
        await message.reply(f"Кімнату з токеном `{token}` не знайдено.\nМожливо, гра ще йде, або пройшло більше 1 години і логи очищено.")
        return
       
    if room.get('game_started', False):
        await message.reply("Не можна отримати лог, поки гра ще триває.")
        return
    try:
        log_content = f"--- ЛОГ КІМНАТИ: {token} ---\n"
        log_content += f"Локація: {room.get('location', 'Невідомо')}\n"
        spy_id = room.get('spy')
       
        log_content += "\n--- УЧАСНИКИ ---\n"
        participants = room.get('participants', [])
        for pid, username, callsign in participants:
            is_spy_str = " (ШПИГУН)" if pid == spy_id else ""
            log_content += f"• {username} ({callsign}){is_spy_str} [ID: {pid}]\n"
           
        log_content += "\n--- ІСТОРІЯ ЧАТУ ---\n"
        messages = room.get('messages', [])
        if messages:
            for msg in messages:
                log_content += f"{msg}\n"
        else:
            log_content += "[Повідомлень не знайдено]\n"
       
        log_content += "\n--- КІНЕЦЬ ЛОГУ ---"
       
        log_filename = f"log_{token}.txt"
        with open(log_filename, 'w', encoding='utf-8') as f:
            f.write(log_content)
           
        log_file = FSInputFile(log_filename)
        await message.reply_document(log_file, caption=f"Лог-файл для кімнати {token}")
       
        os.remove(log_filename)
       
    except Exception as e:
        logger.error(f"Failed to generate or send log for token {token}: {e}", exc_info=True)
        await message.reply(f"Помилка при створенні логу: {e}")

@dp.message(Command("recentgames"))
async def get_recent_games(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        logger.warning(f"Non-admin user {message.from_user.id} tried to use /recentgames")
        return
       
    try:
        current_time = time.time()
        one_hour_ago = current_time - 3600
       
        recent_rooms = []
        for token, room in rooms.items():
            if room.get('created_at', 0) >= one_hour_ago:
                recent_rooms.append((token, room))
       
        if not recent_rooms:
            await message.reply("За останню годину не було створено жодної кімнати.")
            return
           
        reply_text = f"**Активні/нещодавні кімнати (за 1 год):**\n\n"
       
        for token, room in sorted(recent_rooms, key=lambda item: item[1].get('created_at', 0), reverse=True):
            status = "В грі" if room.get('game_started') else "В лобі"
            players = len(room.get('participants', []))
            time_ago = timedelta(seconds=int(current_time - room.get('created_at', 0)))
           
            reply_text += f"🔑 **{token}**\n"
            reply_text += f" • Статус: {status}\n"
            reply_text += f" • Гравців: {players}\n"
            reply_text += f" • Створено: {time_ago} тому\n\n"
           
        await message.reply(reply_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to get recent games: {e}", exc_info=True)
        await message.reply(f"Помилка: {e}")

@dp.message(Command("ban"))
async def ban_user(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Неправильне використання.\nНапишіть /ban <час> (відповіддю на повідомлення)\nАБО\n/ban <@username> <час>")
        return
    target_id = None
    target_username = None
    time_str = ""
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        target_id = target_user.id
        target_username = f"@{target_user.username}" if target_user.username else target_user.first_name
        time_str = args[1].lower()
   
    elif len(args) == 3:
        username_arg = args[1]
        time_str = args[2].lower()
       
        if username_arg.startswith('@'):
            target_username = username_arg
        else:
            target_username = f"@{username_arg}"
           
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM players WHERE username = ?", (target_username,)) as cursor:
                result = await cursor.fetchone()
                if result:
                    target_id = result[0]
                else:
                    await message.reply(f"Не можу знайти гравця {target_username} в базі. Він має хоча б раз запустити бота. Спробуйте забанити через 'Reply'.")
                    return
    else:
        await message.reply("Неправильне використання.\nНапишіть /ban <час> (відповіддю на повідомлення)\nАБО\n/ban <@username> <час>")
        return
    try:
        banned_until_timestamp = parse_ban_time(time_str)
       
        if banned_until_timestamp == 0:
            await message.reply("Неправильний формат часу. Використовуйте 'm', 'h', 'd' або 'perm'.")
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await get_player_stats(target_id, target_username)
            await db.execute("UPDATE players SET banned_until = ? WHERE user_id = ?", (banned_until_timestamp, target_id))
            await db.commit()
       
        ban_message = f"Гравець {target_username} (ID: {target_id}) отримав бан."
        if banned_until_timestamp == -1:
            ban_message_user = "Ви отримали бан від адміністратора **назавжди**."
            ban_message += " Бан назавжди."
        else:
            remaining = timedelta(seconds=int(banned_until_timestamp - time.time()))
            ban_message += f" Час: {remaining}."
            ban_message_user = f"Ви отримали бан від адміністратора.\nЗалишилось: **{remaining}**."
        await message.reply(ban_message)
       
        try:
            await bot.send_message(target_id, ban_message_user, parse_mode="Markdown")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to ban user: {e}", exc_info=True)
        await message.reply(f"Помилка при бані: {e}")

@dp.message(Command("unban"))
async def unban_user(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return
    target_id = None
    target_username = None
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        target_id = target_user.id
        target_username = f"@{target_user.username}" if target_user.username else target_user.first_name
   
    else:
        args = message.text.split()
        if len(args) == 2:
            username_arg = args[1]
            if username_arg.startswith('@'):
                target_username = username_arg
            else:
                target_username = f"@{username_arg}"
               
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT user_id FROM players WHERE username = ?", (target_username,)) as cursor:
                    result = await cursor.fetchone()
                    if result:
                        target_id = result[0]
                    else:
                        await message.reply(f"Не можу знайти гравця {target_username} в базі.")
                        return
        else:
            await message.reply("Неправильне використання.\nНапишіть /unban (відповіддю на повідомлення)\nАБО\n/unban <@username>")
            return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await get_player_stats(target_id, target_username)
            await db.execute("UPDATE players SET banned_until = 0 WHERE user_id = ?", (target_id,))
            await db.commit()
        await message.reply(f"Гравець {target_username} (ID: {target_id}) розбанений.")
       
        try:
            await bot.send_message(target_id, "Вас було розбанено адміністратором.")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to unban user: {e}", exc_info=True)
        await message.reply(f"Помилка при розбані: {e}")

# --- ФУНКЦІЇ МАТЧМЕЙКІНГУ ---
async def notify_queue_updates():
    queue_size = len(matchmaking_queue)
    if queue_size == 0:
        return
       
    logger.info(f"Notifying {queue_size} players in queue.")
    for pid, _, _ in matchmaking_queue:
        try:
            await bot.send_message(pid, f"Пошук... з вами в черзі: {queue_size} гравців.")
        except Exception:
            pass

async def create_game_from_queue(players: list):
    if not players:
        return
       
    logger.info(f"Creating game from queue for {len(players)} players.")
   
    room_token = f"auto_{uuid.uuid4().hex[:4]}"
    owner_id = random.choice([p[0] for p in players])
    participants_list = [(pid, uname, None) for pid, uname, _ in players]
   
    rooms[room_token] = {
        'owner': owner_id, 'participants': participants_list, 'game_started': False, 'is_test_game': False,
        'spy': None, 'location': None, 'messages': [], 'votes': {}, 'banned_from_voting': set(),
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'spy_guess_timer_task': None,
        'last_activity': time.time(), 'last_minute_chat': False, 'waiting_for_spy_guess': False,
        'spy_guess': None, 'votes_for': 0, 'votes_against': 0, 'created_at': time.time(),
        'results_processed': False
    }
   
    room = rooms[room_token]
   
    for pid, _, _ in players:
        try:
            key = StorageKey(bot_id=bot.id, chat_id=pid, user_id=pid)
            await dp.storage.set_state(key=key, state=None)
            await bot.send_message(pid, f"Гру знайдено! Підключаю до кімнати {room_token}...", reply_markup=kb_in_game)
        except Exception as e:
            logger.error(f"Failed to notify player {pid} about matched game: {e}")
           
    await start_game_logic(room, room_token)

async def matchmaking_processor():
    global matchmaking_queue
    while True:
        await asyncio.sleep(10)
       
        try:
            if maintenance_mode:
                continue
               
            current_time = time.time()
           
            # --- ФІКС 2: Таймаут матчмейкінгу (2 хвилини) ---
            timed_out_users = [p for p in matchmaking_queue if current_time - p[2] > 120] # p[2] це timestamp
            matchmaking_queue = [p for p in matchmaking_queue if current_time - p[2] <= 120]
           
            if timed_out_users:
                logger.info(f"Timing out {len(timed_out_users)} users from queue.")
                for pid, username, _ in timed_out_users:
                    try:
                        await bot.send_message(pid, "Час пошуку вичерпано. Спробуйте ще раз пізніше.", reply_markup=kb_main_menu)
                        await set_default_commands_for_user(bot, pid)
                        key = StorageKey(bot_id=bot.id, chat_id=pid, user_id=pid)
                        await dp.storage.set_state(key=key, state=None)
                    except Exception as e:
                        logger.warning(f"Failed to notify user {pid} about timeout: {e}")
            # --- Кінець таймауту ---
            queue_size = len(matchmaking_queue)
            if queue_size < 3:
                continue
               
            logger.info(f"Matchmaking processor running with {queue_size} players.")
           
            players_to_process = matchmaking_queue.copy()
            matchmaking_queue.clear()
            random.shuffle(players_to_process)
           
            while len(players_to_process) >= 3:
                total = len(players_to_process)
               
                if 6 <= total <= 16:
                    room_size = total // 2
                elif total > 16:
                    room_size = 8
                else:
                    room_size = total
               
                room_players = players_to_process[:room_size]
                players_to_process = players_to_process[room_size:]
               
                await create_game_from_queue(room_players)
           
            if players_to_process:
                logger.info(f"Putting {len(players_to_process)} players back in queue.")
                matchmaking_queue.extend(players_to_process)
                await notify_queue_updates()
               
        except Exception as e:
            logger.error(f"Matchmaking processor error: {e}", exc_info=True)

# --- Команда /stats (ПЕРЕНЕСЕНО ВГОРУ) ---
@dp.message(Command("stats"))
@dp.message(F.text == "📊 Моя Статистика")
async def show_stats(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
       
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    try:
        await get_player_stats(user_id, username)
        stats = await get_player_stats(user_id, username)
        _, _, total_xp, games_played, spy_wins, civilian_wins, _ = stats
       
        # --- ФІКС: Повідомлення для новачків ---
   
        level, xp_needed_for_level, xp_in_current_level, _ = get_level_from_xp(total_xp)
       
        total_wins = spy_wins + civilian_wins
        winrate = (total_wins / games_played * 100) if games_played > 0 else 0
       
        stats_text = (
            f"📊 **Ваша статистика** 📊\n\n"
            f"👤 **Нік:** {username}\n"
            f"🎖 **Рівень:** {level}\n"
            f"✨ **Досвід (XP):** {xp_in_current_level} / {xp_needed_for_level}\n"
            f"*(Всього: {total_xp} XP)*\n"
            f"🏆 **Вінрейт:** {winrate:.1f}% (всього перемог: {total_wins})\n"
            f"🕹 **Всього ігор:** {games_played}\n\n"
            f"🕵️ **Перемог за Шпигуна:** {spy_wins}\n"
            f"👨‍🌾 **Перемог за Мирного:** {civilian_wins}"
        )
       
        await message.reply(stats_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to get stats for {user_id}: {e}", exc_info=True)
        await message.reply("Не вдалося завантажити вашу статистику. Спробуйте пізніше.")

# --- Основні Ігрові Команди (Розділені на Текст та Слеш) ---
@dp.message(Command("start"))
@dp.message(F.text == "❓ Допомога")
async def send_welcome(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
   
    active_users.add(message.from_user.id)
    if await check_maintenance(message):
        return
   
    await state.clear()
       
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Обери дію на клавіатурі внизу:"
    )
    await message.reply(menu_text, reply_markup=kb_main_menu)
    await set_default_commands_for_user(bot, message.from_user.id)
   
    if message.from_user.id in ADMIN_IDS:
        await message.answer(
            "Вітаю, Адмін. Тобі доступні спец. команди (тільки через слеш-меню):\n"
            "/maintenance_on, /maintenance_off, /maint_timer, /cancel_maint, "
            "/check_webhook, /testgame, /testgamespy, /whois, /getdb, /updatedb, /getlog, /recentgames, /ban, /unban"
        )

@dp.message(Command("find_match"))
@dp.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
       
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
   
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
           
    if any(user_id == p[0] for p in matchmaking_queue):
        await message.reply("Ви вже у пошуку! Щоб скасувати: /cancel_match", reply_markup=kb_in_queue)
        return
       
    matchmaking_queue.append((user_id, username, time.time()))
    await state.set_state(PlayerState.in_queue)
    await message.reply("Пошук почався, заждіть... (макс. 2 хв)\nЩоб скасувати: /cancel_match", reply_markup=kb_in_queue)
   
    await notify_queue_updates()
   

@dp.message(Command("cancel_match"), StateFilter(PlayerState.in_queue))
@dp.message(F.text == "❌ Скасувати Пошук", StateFilter(PlayerState.in_queue))
async def cancel_match(message: types.Message, state: FSMContext):
    global matchmaking_queue
    user_id = message.from_user.id
   
    matchmaking_queue = [p for p in matchmaking_queue if p[0] != user_id]
    await state.clear()
    await message.reply("Пошук скасовано.", reply_markup=kb_main_menu)
    await set_default_commands_for_user(bot, user_id)
   
    await notify_queue_updates()

@dp.message(Command("create"))
@dp.message(F.text == "🚪 Створити Кімнату")
async def create_room(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
       
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошук: /cancel_match")
        return
       
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            if room['game_started']:
                await message.reply("Ви в активній грі! Спочатку покиньте її (/leave).")
                return
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.")
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, f"Гравець {username} покинув кімнату {token}.")
                    except Exception: pass
            if not room['participants']:
                if token in rooms: del rooms[token]
            elif room['owner'] == user_id:
                if token in rooms: del rooms[token]
                for pid, _, _ in room['participants']:
                    if pid > 0:
                        try:
                            await bot.send_message(pid, f"Кімната {token} закрита, бо власник покинув її.")
                        except Exception: pass
            save_rooms()
           
    room_token = str(uuid.uuid4())[:8].lower()
    rooms[room_token] = {
        'owner': user_id, 'participants': [(user_id, username, None)], 'game_started': False,
        'is_test_game': False, 'spy': None, 'location': None, 'messages': [], 'votes': {},
        'banned_from_voting': set(), 'vote_in_progress': False, 'voters': set(), 'timer_task': None,
        'spy_guess_timer_task': None, 'last_activity': time.time(), 'last_minute_chat': False, 'waiting_for_spy_guess': False,
        'spy_guess': None, 'votes_for': 0, 'votes_against': 0, 'created_at': time.time(),
        'results_processed': False
    }
    save_rooms()
    logger.info(f"Room created: {room_token}")
    await message.reply(
        f"Кімнату створено! Токен: `{room_token}`\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame.",
        parse_mode="Markdown", reply_markup=kb_in_lobby
    )
    await set_lobby_commands_for_user(bot, user_id)

@dp.message(Command("join"))
@dp.message(F.text == "🤝 Приєднатися")
async def join_room(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
       
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошук: /cancel_match")
        return
       
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
   
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
           
    await message.answer("Введіть токен кімнати:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(PlayerState.waiting_for_token)
    logger.info(f"User {user_id} prompted for room token")

@dp.message(StateFilter(PlayerState.waiting_for_token))
async def process_token(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        await state.clear()
        return
   
    active_users.add(message.from_user.id)
    token = message.text.strip().lower()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
   
    if token in rooms:
        if rooms[token].get('is_test_game', False):
             await message.reply("Це тестова кімната, до неї не можна приєднатися.", reply_markup=kb_main_menu)
        elif rooms[token]['game_started']:
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.", reply_markup=kb_main_menu)
        elif user_id not in [p[0] for p in rooms[token]['participants']]:
            rooms[token]['participants'].append((user_id, username, None))
            rooms[token]['last_activity'] = time.time()
            save_rooms()
            logger.info(f"User {user_id} ({username}) joined room {token}")
            for pid, _, _ in rooms[token]['participants']:
                if pid != user_id and pid > 0:
                    try:
                        await bot.send_message(pid, f"Гравець {username} приєднався до кімнати {token}!")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about join: {e}")
            await message.reply(f"Ви приєдналися до кімнати {token}!\nЧекайте, поки власник запустить гру (/startgame).", reply_markup=kb_in_lobby)
            await set_lobby_commands_for_user(bot, user_id)
        else:
            await message.reply("Ви вже в цій кімнаті!", reply_markup=kb_in_lobby)
    else:
        await message.reply(f"Кімнати з токеном {token} не існує. Спробуйте ще раз.", reply_markup=kb_main_menu)
       
    await state.clear()

@dp.message(Command("leave"))
@dp.message(F.text.startswith("🚪 Покинути"))  # Ловить "Лобі" і "Гру"
async def leave_room(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
       
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
   
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        return await cancel_match(message, state)
       
    active_users.add(message.from_user.id)
    logger.info(f"User {user_id} sent /leave")
   
    room_found = False
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            room_found = True
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            room['last_activity'] = time.time()
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.", reply_markup=kb_main_menu)
            await set_default_commands_for_user(bot, user_id)
           
            # --- ФІКС: Перевірка на кінець гри, якщо хтось лівнув ---
            if room.get('game_started'):
                # Якщо лівнув шпигун
                if user_id == room.get('spy'):
                    logger.info(f"Spy left room {token}. Ending game.")
                    await end_game(token, "Шпигун втік! Гра завершена.")
                    return
               
                # Якщо залишилось < 2 реальних гравців
                real_players_left = sum(1 for p in room['participants'] if p[0] > 0)
                if real_players_left < 2:
                    logger.info(f"Only {real_players_left} players left in {token}. Ending game.")
                    await end_game(token, "Залишилось занадто мало гравців. Гра завершена.")
                    return
            # --- Кінець фіксу ---
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, f"Гравець {username} покинув кімнату {token}.")
                    except Exception: pass
           
            if not room['participants'] or all(p[0] < 0 for p in room['participants']):
                if room.get('timer_task'): room['timer_task'].cancel()
                if room.get('spy_guess_timer_task'): room['spy_guess_timer_task'].cancel()
                if token in rooms: del rooms[token]
                logger.info(f"Room {token} deleted (empty or only bots left)")
            elif room['owner'] == user_id:
                if room.get('timer_task'): room['timer_task'].cancel()
                if room.get('spy_guess_timer_task'): room['spy_guess_timer_task'].cancel()
                if token in rooms: del rooms[token]
                logger.info(f"Room {token} deleted (owner left)")
                for pid, _, _ in room['participants']:
                    if pid > 0:
                        try:
                            await bot.send_message(pid, f"Кімната {token} закрита, бо власник покинув її.")
                        except Exception: pass
            save_rooms()
            return
           
    if not room_found:
        logger.info(f"User {user_id} not in any room or queue")
        await message.reply("Ви не перебуваєте в жодній кімнаті або черзі.", reply_markup=kb_main_menu)

@dp.message(Command("startgame"))
async def start_game(message: types.Message):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    logger.info(f"User {user_id} sent /startgame")
   
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if room.get('is_test_game', False):
                await message.reply("Тестова гра вже запущена!")
                return
            if room['owner'] != user_id:
                await message.reply("Тільки власник може запустити гру!")
                return
            if room['game_started']:
                await message.reply("Гра вже почалася!")
                return
            if len(room['participants']) < 3:
                await message.reply("Потрібно щонайменше 3 гравці, щоб почати гру.")
                return
               
            await start_game_logic(room, token)
            return
           
    logger.info(f"User {user_id} not in any room for /startgame")
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

async def start_game_logic(room, token, admin_is_spy: bool = False):
   
    logger.info(f"Starting game logic for room {token}...")
   
    if room.get('timer_task') and not room['timer_task'].done():
        room['timer_task'].cancel()
    if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
        room['spy_guess_timer_task'].cancel()
       
    available_callsigns = CALLSIGNS.copy()
    random.shuffle(available_callsigns)
    participant_list = [(pid, username, None) for pid, username, _ in room['participants']]
    room['participants'] = [(pid, username, available_callsigns[i]) for i, (pid, username, _) in enumerate(participant_list)]
   
    room['game_started'] = True
    room['location'] = random.choice(LOCATIONS)
    room['messages'] = []
   
    if room.get('is_test_game'):
        participant_ids = [p[0] for p in room['participants']]
        if admin_is_spy:
            room['spy'] = room['owner']
        else:
            bot_ids = [pid for pid in participant_ids if pid < 0]
            room['spy'] = random.choice(bot_ids) if bot_ids else room['owner']  # Якщо ботів нема, адмін - шпигун
    else:
        room['spy'] = random.choice([p[0] for p in room['participants']])
       
    room['banned_from_voting'] = set()
    room['votes'] = {}
    room['vote_in_progress'] = False
    room['voters'] = set()
    room['waiting_for_spy_guess'] = False
    room['spy_guess'] = None
    room['votes_for'] = 0
    room['votes_against'] = 0
    room['last_activity'] = time.time()
    room['results_processed'] = False  # Скидаємо прапорець
    save_rooms()
   
    logger.info(f"Game started in room {token}, spy: {room['spy']}, location: {room['location']}")
   
    player_count = len(room['participants'])
    all_callsigns = [c for _, _, c in room['participants']]
    random.shuffle(all_callsigns)
    info_block = (
        f"Всього гравців: {player_count}\n"
        f"Позивні в грі: {', '.join(all_callsigns)}"
    )
    for pid, username, callsign in room['participants']:
        if pid > 0:
            try:
                await set_game_commands_for_user(bot, pid)
                await bot.send_message(pid, "Гра почалась!", reply_markup=kb_in_game)
               
                user_nickname = f"@{username}" if username.startswith('@') else username
               
                if pid == room['spy']:
                    message_text = f"Ваш нік: {user_nickname}\n\nВи ШПИГУН ({callsign})! Спробуйте не видати себе."
                else:
                    message_text = f"Ваш нік: {user_nickname}\n\nЛокація: {room['location']}\nВи {callsign}. Один із гравців — шпигун!"
                   
                if room.get('is_test_game') and pid == room['owner'] and pid == room['spy']:
                     message_text += f"\n(DEBUG: Локація {room['location']})"
                await bot.send_message(pid, f"{message_text}\n\n{info_block}")
               
                if room.get('is_test_game'):
                    await bot.send_message(pid, "ТЕСТОВА ГРА: Боти проголосують за 1 хвилину.")
                else:
                    await bot.send_message(pid, "Спілкуйтеся вільно. Час гри: 20 хвилин.")
            except Exception as e:
                logger.error(f"Failed to send start message to user {pid}: {e}")
               
    room['timer_task'] = asyncio.create_task(run_timer(token))

@dp.message(Command("my_info"))
@dp.message(F.text == "❓ Моя роль")
async def my_info(message: types.Message):
    if await check_ban_and_reply(message): return
   
    user_id = message.from_user.id
   
    user_room = None
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            user_room = room
            break
           
    if not user_room or not user_room.get('game_started'):
        await message.reply("Ця команда працює тільки під час активної гри.")
        return
       
    try:
        if user_id == user_room['spy']:
            await bot.send_message(user_id, "Нагадуємо: Ви - ШПИГУН. 🤫")
        else:
            await bot.send_message(user_id, f"Нагадуємо: Ви - Мирний. 😇\nЛокація: {user_room['location']}")
           
        if message.text.startswith("/"):
             await message.answer("Нагадування надіслано в особисті повідомлення.", reply_markup=kb_in_game)
            
    except Exception as e:
        logger.error(f"Failed to send /my_info to {user_id}: {e}")
        try:
            await message.reply("Не вдалося надіслати нагадування. Можливо, ви не почали чат з ботом? Напишіть йому в ПП.")
        except Exception: pass

@dp.message(Command("early_vote"))
@dp.message(F.text == "🗳️ Достр. Голосування")
async def early_vote(message: types.Message):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
   
    # --- ФІКС 1: Перевіряємо стан FSM ---
    current_state = await dp.storage.get_state(StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id))
    if current_state == PlayerState.in_queue.state:
        await message.reply("Ви у черзі! Спочатку скасуйте пошук: /cancel_match")
        return
    # --- Кінець фіксу ---
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
           
            # --- ФІКС 2: Дозволяємо дострокове голосування в тестових іграх ---
            # (Старий код закоментовано)
            # if room.get('is_test_game', False):
            # await message.reply("Ця функція вимкнена у тестових іграх.")
            # return
           
            if not room['game_started']:
                await message.reply("Гра не активна!")
                return
            if user_id in room['banned_from_voting']:
                await message.reply("Ви вже використали дострокове голосування в цій партії!")
                return
            if room['vote_in_progress']:
                await message.reply("Голосування вже триває!")
                return
           
            room['vote_in_progress'] = True
            room['votes_for'] = 0
            room['votes_against'] = 0
            room['voters'] = set()
            room['banned_from_voting'].add(user_id)
            room['last_activity'] = time.time()
            try:
                await bot.send_message(user_id, "Ви ініціювали дострокове голосування. Ви не зможете зробити це знову в цій грі.")
            except Exception as e:
                logger.error(f"Failed to send early vote notice to user {user_id}: {e}")
            save_rooms()
           
            # --- ВИПРАВЛЕНО: Клавіатура для За/Проти, а не для гравців ---
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ За дострокове завершення", callback_data=f"early_vote_for:{token}")],
                [InlineKeyboardButton(text="❌ Продовжити гру", callback_data=f"early_vote_against:{token}")]
            ])
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, "Голосування за дострокове завершення гри! Час: 15 секунд.", reply_markup=keyboard)
                    except Exception: pass
           
            # --- ФІКС: Таймер голосування (з урахуванням ботів) ---
            asyncio.create_task(early_vote_timer(token))
            return
           
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

async def early_vote_timer(token):
    """Фоновий таймер для дострокового голосування."""
    await asyncio.sleep(15)
   
    room = rooms.get(token)
    if not room or not room.get('vote_in_progress'):
        return # Голосування вже завершено або скасовано
    # Час вийшов, підраховуємо
    await finalize_early_vote(token)

async def finalize_early_vote(token):
    """Підбиває підсумки дострокового голосування."""
    room = rooms.get(token)
    if not room: return
   
    room['vote_in_progress'] = False
    votes_for = room['votes_for']
    votes_against = room['votes_against']
    room['last_activity'] = time.time()
   
    # --- ФІКС 2 (testgame): Боти голосують ЗА ---
    if room.get('is_test_game'):
        bot_count = sum(1 for p in room['participants'] if p[0] < 0)
        votes_for += bot_count
    if votes_for > votes_against:
        room['game_started'] = False
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        for pid, _, _ in room['participants']:
            if pid > 0:
                try:
                    await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                    await set_game_commands_for_user(bot, pid)
                except Exception: pass
        await show_voting_buttons(token)
    else:
        for pid, _, _ in room['participants']:
            if pid > 0:
                try:
                    await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                except Exception: pass
    save_rooms()

@dp.callback_query(lambda c: c.data.startswith("early_vote_"))
async def early_vote_callback(callback: types.CallbackQuery):
    if await check_ban_and_reply(callback): return
   
    user_id = callback.from_user.id
    # --- ФІКС: Парсинг з ':' ---
    data_parts = callback.data.split(':')
    if len(data_parts) < 2:
        await callback.answer("Помилка даних!")
        return
    token = data_parts[-1]  # Останній елемент - токен
    room = rooms.get(token)
    if not room or user_id not in [p[0] for p in room['participants']]:
        await callback.answer("Ви не в цій грі!")
        return
    if not room['vote_in_progress']:
        await callback.answer("Голосування закінчено!")
        return
    if user_id in room['voters']:
        await callback.answer("Ви вже проголосували!")
        return
       
    room['voters'].add(user_id)
    if data_parts[0] == "early_vote_for":
        room['votes_for'] += 1
        await callback.answer("Ви проголосували 'За'!")
    else:
        room['votes_against'] += 1
        await callback.answer("Ви проголосували 'Проти'!")
    room['last_activity'] = time.time()
    save_rooms()
   
    # Перевіряємо, чи всі *реальні* гравці проголосували
    real_players_count = sum(1 for p in room['participants'] if p[0] > 0)
    if len(room['voters']) == real_players_count:
        await finalize_early_vote(token)

async def run_timer(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"Run timer: Room {token} not found")
            return
       
        # Визначаємо час очікування
        wait_time = 60 if room.get('is_test_game') else 1140 # 1 хв для тесту, 19 хв для гри
       
        await asyncio.sleep(wait_time)
        if token not in rooms or not rooms[token]['game_started']:
            return
           
        room = rooms.get(token)
        if not room: return
        room['last_minute_chat'] = True
       
        # Не надсилаємо "1 хвилина" у тестовій грі
        if not room.get('is_test_game'):
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, "Залишилась 1 хвилина до кінця гри! Спілкуйтеся вільно.")
                    except Exception as e:
                        logger.error(f"Failed to send 1-minute warning to user {pid}: {e}")
            await asyncio.sleep(50) # --- ФІКС: Чекаємо 50 сек, а не 60 ---
       
        if token not in rooms or not rooms[token]['game_started']:
            return
           
        # --- ФІКС 4: Повертаємо 10-секундний відлік ---
        if not room.get('is_test_game'):
            for i in range(10, 0, -1):
                if token not in rooms or not rooms[token]['game_started']:
                    return
                for pid, _, _ in room['participants']:
                    if pid > 0:
                        try:
                            await bot.send_message(pid, f"До кінця гри: {i} секунд")
                        except Exception: pass
                await asyncio.sleep(1)
       
        if token not in rooms: return # Перевірка ще раз
           
        room = rooms.get(token)
        if not room: return
        room['game_started'] = False
        room['last_minute_chat'] = False
        room['last_activity'] = time.time()
        room['results_processed'] = False # Скидаємо прапорець перед голосуванням
        save_rooms()
        for pid, _, _ in room['participants']:
            if pid > 0:
                try:
                    await bot.send_message(pid, "Час вийшов! Голосуйте, хто шпигун.", reply_markup=kb_in_game)
                except Exception as e:
                    logger.error(f"Failed to send game end message to user {pid}: {e}")
        await show_voting_buttons(token)
    except asyncio.CancelledError:
        logger.info(f"Run timer: Timer for room {token} was cancelled")
    except Exception as e:
        logger.error(f"Run timer error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token, "Помилка таймера. Гру завершено.")

async def show_voting_buttons(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"show_voting_buttons: Room {token} not found")
            return
        room['last_activity'] = time.time()
       
        all_callsigns = [c for _, _, c in room['participants']]
        random.shuffle(all_callsigns)
        callsigns_list_str = f"Позивні в грі: {', '.join(all_callsigns)}"
        # --- ФІКС: Змінено роздільник на ':' ---
        # --- ВИПРАВЛЕНО: Тільки позивні на кнопках ---
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{callsign}", callback_data=f"vote:{token}:{pid}")]
            for pid, username, callsign in room['participants']
        ])
       
        if room.get('is_test_game'):
            admin_id = room['owner']
            spy_id = room['spy']
           
            # Боти миттєво голосують
            for pid, _, _ in room['participants']:
                if pid < 0: # Це бот
                    room['votes'][pid] = spy_id
            save_rooms()
            logger.info(f"Test game {token}: Bots have voted for spy {spy_id}.")
            try:
                await bot.send_message(admin_id, f"Тестова гра: Боти проголосували.\nОберіть, хто шпигун (30 секунд):\n\n{callsigns_list_str}", reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to send test voting keyboard to admin {admin_id}: {e}")
        else:
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, f"Оберіть, хто шпигун (30 секунд):\n\n{callsigns_list_str}", reply_markup=keyboard)
                    except Exception as e:
                        logger.error(f"Failed to send voting keyboard to user {pid}: {e}")
        # Запускаємо таймер голосування
        asyncio.create_task(voting_timer_task(token))
    except Exception as e:
        logger.error(f"Show voting buttons error in room {token}: {e}", exc_info=True)
        await end_game(token, "Помилка при запуску голосування.")

async def voting_timer_task(token):
    """Фоновий таймер на 30 сек для голосування."""
   
    # --- ФІКС 4: Повертаємо 10-секундний відлік ---
    await asyncio.sleep(20) # Чекаємо 20 сек
   
    room = rooms.get(token)
    # Якщо гра закінчилась або результати вже підраховані - виходимо
    if not room or room.get('results_processed'):
        return
       
    # Посилаємо 10-секундний відлік
    for i in range(10, 0, -1):
        if not room or room.get('results_processed'):
            return
        for pid, _, _ in room['participants']:
            if pid > 0:
                try: await bot.send_message(pid, f"Час для голосування: {i} секунд")
                except Exception: pass
        await asyncio.sleep(1)
    # --- Кінець відліку ---
   
    room = rooms.get(token)
    # Якщо гра ще існує, АЛЕ результати ще не були підраховані
    if room and room.get('results_processed') == False:
        logger.info(f"Voting timer expired for room {token}. Processing results...")
        await process_voting_results(token)

@dp.callback_query(lambda c: c.data.startswith('vote:'))
async def process_vote(callback_query: types.CallbackQuery):
    if await check_ban_and_reply(callback_query): return
   
    logger.info(f"Vote callback received: {callback_query.data}")  # ДЕБАГ
   
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data.split(':')
        if len(data) != 3:
            await callback_query.answer("Помилка даних!")
            return
           
        token, voted_pid = data[1], int(data[2])
        room = rooms.get(token)
        if not room or user_id not in [p[0] for p in room['participants']]:
            await callback_query.answer("Ви не в цій грі!")
            return
       
        # Перевіряємо, чи голосування ще активне
        # (гра не завершена І ми не в режимі вгадування шпигуна)
        if room.get('game_started') == False and room.get('waiting_for_spy_guess') == False:
             await callback_query.answer("Голосування завершено!")
             return
        # --- ФІКС: Дозволяємо змінювати голос ---
        room['votes'][user_id] = voted_pid
        room['last_activity'] = time.time()
        save_rooms()
       
        await callback_query.answer("Ваш голос враховано!")
       
        # --- ФІКС: Миттєва перевірка завершення ---
        voted_count = len(room['votes'])
        total_players = len(room['participants'])
       
        is_finished = False
        if room.get('is_test_game', False):
            # У тестовій грі чекаємо тільки голос адміна (і голоси ботів вже мають бути)
            real_voters = {k:v for k,v in room['votes'].items() if k > 0}
            if room['owner'] in real_voters:
                is_finished = True
        else:
            # У звичайній грі чекаємо поки проголосують ВСІ
            if voted_count == total_players:
                is_finished = True
           
        if is_finished:
            logger.info(f"Voting finished in room {token}. Processing results...")
            await process_voting_results(token)
           
    except Exception as e:
        logger.error(f"Process vote error: {e}", exc_info=True)
        await callback_query.answer("Помилка!")

# --- ФІКС 3: Нова функція таймера ---
async def spy_guess_timer_task(token):
    """Фонова функція, яка чекає 30 секунд і перевіряє шпигуна."""
   
    # --- ФІКС 4: Повертаємо 10-секундний відлік ---
    await asyncio.sleep(20) # Чекаємо 20 сек
    room = rooms.get(token)
    if not room or not room.get('waiting_for_spy_guess'):
        return # Шпигун вже вгадав
       
    # Посилаємо 10-секундний відлік
    for i in range(10, 0, -1):
        if not room or not room.get('waiting_for_spy_guess'):
            return
        for pid, _, _ in room['participants']:
            if pid > 0:
                try: await bot.send_message(pid, f"Час для вгадування локації: {i} секунд")
                except Exception: pass
        await asyncio.sleep(1)
    # --- Кінець відліку ---
   
    room = rooms.get(token)
    # Якщо кімната ще існує і шпигун досі думає ('waiting_for_spy_guess' == True)
    if room and room.get('waiting_for_spy_guess'):
        room['waiting_for_spy_guess'] = False
        save_rooms()
       
        logger.info(f"Spy timeout in room {token}")
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
       
        result = (
            f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {room['location']}\n"
            f"⏳ Час вийшов! Шпигун не вгадав локацію. Гравці перемогли!"
        )
        await end_game(token, result_message=result)

def build_locations_keyboard(token: str, locations: list, columns: int = 3) -> InlineKeyboardMarkup:
    """Створює InlineKeyboard з кнопками локацій для шпигуна.
    
    - locations: список локацій (random.shuffle вже зроблено зовні).
    - columns: кількість кнопок в рядку (за замовчуванням 3).
    - callback_data: 'spy_guess:{token}:{safe_loc}' з ':' роздільником (БЕЗПЕЧНО!).
    """
    inline_keyboard = []
    row = []
    for loc in locations:
        safe_loc = loc.replace(' ', '---')  # Заміна пробілів для callback_data
        button = InlineKeyboardButton(text=loc, callback_data=f"spy_guess:{token}:{safe_loc}")
        row.append(button)
        if len(row) == columns:
            inline_keyboard.append(row)
            row = []
    if row:  # Додаємо останній рядок, якщо залишились кнопки
        inline_keyboard.append(row)
    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

# --- ФІКС 3: Виправлена функція ---
async def process_voting_results(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"process_voting_results: Room {token} not found")
            return
           
        # Захист від повторного виклику (якщо таймер і голосування спрацювали одночасно)
        if room.get('results_processed'):
            logger.warning(f"Results already processed for room {token}, skipping.")
            return
        room['results_processed'] = True # Ставимо прапорець
           
        room['last_activity'] = time.time()
        save_rooms()
       
        if not room['votes']:
            logger.info(f"No votes in room {token}")
            await end_game(token, "Ніхто не проголосував. Шпигун переміг!")
            return
           
        vote_counts = {}
        for voted_id in room['votes'].values():
            vote_counts[voted_id] = vote_counts.get(voted_id, 0) + 1
       
        if not vote_counts:
             await end_game(token, "Ніхто не проголосував. Шпигун переміг!")
             return
           
        max_votes = max(vote_counts.values())
        suspected = [pid for pid, count in vote_counts.items() if count == max_votes]
        logger.info(f"process_voting_results: Suspected players: {suspected}, Spy: {room['spy']}")
       
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
       
       
        if len(suspected) == 1 and suspected[0] == room['spy']:
           
            room['waiting_for_spy_guess'] = True
            room['spy_guess'] = None
            room['last_activity'] = time.time()
           
            locations_for_spy = LOCATIONS.copy()
            random.shuffle(locations_for_spy)
            reply_markup = build_locations_keyboard(token, locations_for_spy, columns=3)
           
            save_rooms()
            logger.info(f"Spy {room['spy']} detected in room {token}, sending ALL guess options")
           
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        if pid == room['spy']:
                            await bot.send_message(pid, "Гравці проголосували за вас! Вгадайте локацію (30 секунд):", reply_markup=reply_markup)
                        else:
                            await bot.send_message(pid, f"Гравці вважають, що шпигун — {spy_username} ({spy_callsign}). Чекаємо, чи вгадає він локацію (30 секунд).")
                    except Exception as e:
                        logger.error(f"Failed to send spy guess prompt to user {pid}: {e}")
           
            # --- ФІКС: Запускаємо таймер як окрему задачу ---
            room['spy_guess_timer_task'] = asyncio.create_task(spy_guess_timer_task(token))
        else:
            result = (
                f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}\n"
                f"Шпигуна не знайшли. Шпигун переміг!"
            )
            await end_game(token, result_message=result)
           
    except Exception as e:
        logger.error(f"Process voting results error in room {token}: {e}", exc_info=True)
        await end_game(token, "Помилка при підрахунку голосів.")

# --- ФІКС 3: Виправлена функція ---
@dp.callback_query(lambda c: c.data.startswith('spy_guess:'))
async def process_spy_guess_callback(callback_query: types.CallbackQuery):
    if await check_ban_and_reply(callback_query): return
   
    try:
        user_id = callback_query.from_user.id
       
        # --- ФІКС: Правильне розбиття callback_data ---
        # Формат: spy_guess:{TOKEN}:{Location---Name}
        data_parts = callback_query.data.split(':')
        if len(data_parts) != 3 or data_parts[0] != 'spy_guess':
            logger.error(f"CRITICAL: Invalid spy_guess format: {callback_query.data}")
            await callback_query.answer("Помилка! Неправильний формат кнопки.")
            return
        
        token = data_parts[1]
        guessed_location_safe = data_parts[2]
        guessed_location = guessed_location_safe.replace('---', ' ')
        
        logger.info(f"Parsed: token={token}, guessed={guessed_location}")  # ДЕБАГ
        
        room = rooms.get(token)
       
        if not room:
            await callback_query.answer("Помилка! Гру не знайдено. Можливо, час вийшов.")
            return
       
        if user_id != room.get('spy'):
            await callback_query.answer("Це не ваша гра або ви не шпигун!")
            return
       
        if not room.get('waiting_for_spy_guess'):
            await callback_query.answer("Час на вгадування вийшов!")
            return
       
        # --- МИТТЄВИЙ РЕЗУЛЬТАТ ---
        room['waiting_for_spy_guess'] = False
        room['spy_guess'] = guessed_location.strip()
        room['last_activity'] = time.time()
       
        # Зупиняємо таймер, якщо він ще є
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
           
        save_rooms()
       
        await callback_query.answer(f"Ваш вибір: {guessed_location}")
        try:
            await callback_query.message.edit_text(f"Шпигун зробив свій вибір: {guessed_location}")
        except Exception as e:
            logger.info(f"Couldn't edit spy guess message: {e}")
        # Визначаємо результат
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
       
        if guessed_location.lower() == room['location'].lower():
            result = (
                f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}\n"
                f"🎯 Шпигун вгадав локацію! Шпигун переміг!"
            )
        else:
            result = (
                f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}\n"
                f"❌ Шпигун не вгадав локацію ({guessed_location}). Гравці перемогли!"
            )
       
        await end_game(token, result_message=result)
    except Exception as e:
        logger.error(f"Process spy guess callback error: {e}", exc_info=True)
        await callback_query.answer("Критична помилка під час вибору!")

@dp.message()
async def handle_room_message(message: types.Message, state: FSMContext):
    if await check_ban_and_reply(message): return
    try:
        if await check_maintenance(message):
            return
           
        user_id = message.from_user.id
        current_time = time.time()
        if user_id not in ADMIN_IDS:
            if user_id not in user_message_times:
                user_message_times[user_id] = {'timestamps': deque(), 'muted_until': 0, 'warned_spam': False, 'warned_unmuted': False}
           
            user_data = user_message_times[user_id]
            user_data['last_seen'] = current_time
            if user_data['muted_until'] > current_time:
                if not user_data['warned_spam']:
                    try:
                        await message.reply("ваш спам ніхто не бачить)")
                        user_data['warned_spam'] = True
                    except Exception: pass
                return
           
            if user_data['muted_until'] > 0 and current_time > user_data['muted_until']:
                user_data['muted_until'] = 0
                user_data['warned_spam'] = False
                user_data['warned_unmuted'] = True
           
            user_data['timestamps'].append(current_time)
           
            while user_data['timestamps'] and current_time - user_data['timestamps'][0] > 1:
                user_data['timestamps'].popleft()
           
            if len(user_data['timestamps']) > 4:
                user_data['muted_until'] = current_time + 5
                user_data['warned_spam'] = True
                user_data['timestamps'].clear()
                try:
                    await message.reply("ваш спам ніхто не бачить)")
                except Exception: pass
                return
        active_users.add(message.from_user.id)
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        username_clean = username.lstrip('@')
       
        for token, room in rooms.items():
            if user_id in [p[0] for p in room['participants']]:
               
                if not message.text:
                    try:
                        await message.reply("Ніхто це не побачив( \n(Підтримуються тільки текстові повідомлення)")
                    except Exception: pass
                    return
               
                # НОВЕ: Обмеження на довжину повідомлення
                if len(message.text) > MESSAGE_MAX_LENGTH:
                    await bot.send_message(user_id, f"Обмеження на повідомлення: {MESSAGE_MAX_LENGTH} символів. Ваше повідомлення не відправлено.")
                    return
               
                if user_id not in ADMIN_IDS:
                    user_data = user_message_times[user_id]
                    if user_data.get('warned_unmuted', False):
                        user_data['warned_unmuted'] = False
                        try:
                            await message.reply("інші вже знову бачать що ви пишете.")
                        except Exception: pass
                callsign = next((c for p, u, c in room['participants'] if p == user_id), None)
                if (room['game_started'] or room['last_minute_chat']) and callsign:
                    msg = f"{callsign}: {message.text}"
                else:
                    msg = f"@{username_clean}: {message.text}"
                   
                room['messages'].append(msg)
                room['messages'] = room['messages'][-100:]
                for pid, _, _ in room['participants']:
                    if pid != user_id and pid > 0:
                        try:
                            await bot.send_message(pid, msg)
                        except Exception as e:
                            logger.error(f"Failed to send chat message to user {pid}: {e}")
                room['last_activity'] = time.time()
                save_rooms()
                return
               
        logger.info(f"User {user_id} not in any room for room message handler")
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create), приєднайтесь (/join) або шукайте гру (/find_match).", reply_markup=kb_main_menu)
    except Exception as e:
        logger.error(f"Handle room message error: {e}", exc_info=True)
        await message.reply("Виникла помилка при обробці повідомлення.")

async def end_game(token, result_message: str = None):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"end_game: Room {token} not found")
            return
       
        # Зупиняємо всі таймери кімнати
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
        # Нараховуємо XP (тільки якщо це не тестова гра)
        if not room.get('is_test_game'):
            spy_id = room.get('spy')
           
            # Визначаємо переможця
            spy_won = False
            if result_message:
                if "Шпигун переміг" in result_message or "Шпигун вгадав" in result_message:
                    spy_won = True
           
            all_participants = room.get('participants', [])
            for pid, username, _ in all_participants:
                if pid <= 0: continue
               
                is_player_spy = (pid == spy_id)
                is_player_winner = (is_player_spy == spy_won)
               
                await update_player_stats(pid, is_player_spy, is_player_winner)
        # Формуємо фінальне повідомлення
        if result_message:
            final_message = result_message
        else:
            spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
            spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
            final_message = (
                f"Гру завершено достроково!\n"
                f"Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}"
            )
           
        reveal_message = "\n\nРозподіл позивних:\n"
        all_participants = room.get('participants', [])
        for pid, username, callsign in all_participants:
            if pid > 0:
                reveal_message += f"• {username} був '{callsign}'\n"
        final_message += reveal_message
        final_message += f"\nКод кімнати: `{token}`\nОпції:\n/leave - Покинути кімнату\n"
        owner_id = room['owner']
        for pid, _, _ in all_participants:
            if pid > 0:
                try:
                    reply_markup = kb_main_menu
                    extra_options = "\n/stats - Моя статистика"
                   
                    if not room.get('is_test_game'):
                        if str(token).startswith("auto_"):
                            extra_options += "\n/find_match - Шукати нову гру"
                        elif pid == owner_id:
                            extra_options += "\n/startgame - Почати нову гру"
                    await bot.send_message(pid, final_message + extra_options, reply_markup=reply_markup, parse_mode="Markdown")
                    await set_default_commands_for_user(bot, pid)
                except Exception as e:
                    logger.error(f"Failed to send end game message to user {pid}: {e}")
       
        # --- ОНОВЛЕННЯ СТАНУ КІМНАТИ ---
        room['game_started'] = False
        room['spy'] = None
        # 'location' та 'participants' залишаємо для логів
        room['votes'] = {}
        # 'messages' залишаємо для логів
        room['vote_in_progress'] = False
        room['banned_from_voting'] = set()
        room['timer_task'] = None
        room['spy_guess_timer_task'] = None
        room['last_activity'] = time.time()
        room['last_minute_chat'] = False
        room['waiting_for_spy_guess'] = False
        room['spy_guess'] = None
        room['votes_for'] = 0
        room['votes_against'] = 0
        room['results_processed'] = False # Скидаємо прапорець
       
        # Якщо це тестова гра або матчмейкінг, видаляємо одразу
        if room.get('is_test_game') or str(token).startswith("auto_"):
            await asyncio.sleep(120)
            if token in rooms:
                del rooms[token]
                logger.info(f"Auto/Test room {token} deleted after game end.")
                save_rooms()
        else:
            # Приватна кімната живе 1 годину з логами
            save_rooms()
            logger.info(f"Private game ended in room {token}. Room reset, logs preserved for 1 hour.")
    except Exception as e:
        logger.error(f"End game error in room {token}: {e}", exc_info=True)
        # Надійний вивід: Спробуємо надіслати результат, навіть якщо помилка
        spy_username = "Невідомо"
        spy_callsign = "Невідомо"
        location = "Невідомо"
        if room:
            spy_username = next((username for pid, username, _ in room['participants'] if pid == room.get('spy')), "Невідомо")
            spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
            location = room.get('location', "Невідомо")
        fallback_message = (
            f"Гра завершена з помилкою!\n"
            f"Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {location}"
        )
        for pid, _, _ in room.get('participants', []):
            if pid > 0:
                try:
                    await bot.send_message(pid, fallback_message)
                except Exception:
                    pass

# --- Функції запуску та Webhook ---
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

async def set_default_commands(bot_instance: Bot):
    """Встановлює дефолтні слеш-команди для всіх юзерів."""
    try:
        await bot_instance.set_my_commands(cmds_default, scope=types.BotCommandScopeAllPrivateChats())
        logger.info("Default slash commands set for all users.")
    except Exception as e:
        logger.error(f"Failed to set default commands: {e}")

async def on_startup(_):
    try:
        logger.info("Starting bot initialization")
       
        await db_init()
       
        load_rooms()
       
        await set_default_commands(bot)
       
        asyncio.create_task(matchmaking_processor())
       
        if USE_POLLING:
            logger.info("Starting bot in polling mode")
            await bot.delete_webhook(drop_pending_updates=True)
            asyncio.create_task(cleanup_rooms())
        else:
            webhook_url = f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"
            logger.info(f"Setting up webhook: {webhook_url}")
            await set_webhook_with_retry(webhook_url)
            asyncio.create_task(cleanup_rooms())
            asyncio.create_task(keep_alive())
            asyncio.create_task(check_webhook_periodically())
        logger.info("Bot initialization completed")
        webhook_info = await bot.get_webhook_info()
        logger.info(f"Webhook status after startup: {webhook_info}")
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        raise

async def on_shutdown(_):
    try:
        logger.info("Shutting down server...")
        save_rooms()
        for token, room in list(rooms.items()):
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
                room['spy_guess_timer_task'].cancel()
       
        await bot.session.close()
        logger.info("Bot session closed. Shutdown successful.")
    except Exception as e:
        logger.error(f"Shutdown failed: {e}", exc_info=True)

app = web.Application()
webhook_path = "/webhook"

class CustomRequestHandler(SimpleRequestHandler):
    async def post(self, request):
        logger.debug(f"Received webhook request: {request.method} {request.path}")
        try:
            data = await request.json()
            update = types.Update(**data)
            await dp.feed_update(bot, update)
            logger.debug("Update successfully processed")
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Webhook processing error: {e}", exc_info=True)
            return web.Response(status=500)

if not USE_POLLING:
    CustomRequestHandler(dispatcher=dp, bot=bot).register(app, path=webhook_path)
    app.router.add_route('GET', '/health', health_check)
    app.router.add_route('HEAD', '/health', health_check)
    setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    try:
        port = int(os.getenv("PORT", 443))
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)
        if USE_POLLING:
            logger.info("Starting bot in polling mode")
            asyncio.run(dp.start_polling(bot))
        else:
            logger.info(f"Starting server on port {port}")
            web.run_app(app, host="0.0.0.0", port=port)
    except Exception as e:
        logger.error(f"Server failed to start: {e}", exc_info=True)
        raise