import logging
import asyncio
import random
import os
import json
import time
import psutil
import aiosqlite # --- НОВЕ: для бази даних ---
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter 
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeChat
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
ADMIN_ID = int(os.getenv('ADMIN_ID', '5280737551'))
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
matchmaking_queue = [] 
maintenance_timer_task = None 
DB_PATH = 'players.db' # --- НОВЕ: Шлях до нашої бази даних ---

class PlayerState(StatesGroup):
    in_queue = State() 
    waiting_for_token = State() 

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
ROOM_EXPIRY = 3600 

# --- НОВЕ: Базові налаштування XP (ЗМІНЕНО) ---
XP_CIVILIAN_WIN = 10
XP_SPY_WIN = 20

# Логування
logger.info(f"Using aiohttp version: {aiohttp.__version__}")
process = psutil.Process()
logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# --- НОВЕ: Всі функції для роботи з Базою Даних (SQLite) ---

async def db_init():
    """Ініціалізує базу даних та створює таблицю, якщо її немає."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0
            )
        ''')
        await db.commit()
    logger.info("Database initialized successfully.")

async def get_player_stats(user_id, username):
    """Отримує статистику гравця. Створює, якщо не існує."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)) as cursor:
            player = await cursor.fetchone()
            
        if player is None:
            # Створюємо нового гравця
            await db.execute(
                "INSERT INTO players (user_id, username, total_xp, games_played, spy_wins, civilian_wins) VALUES (?, ?, 0, 0, 0, 0)",
                (user_id, username)
            )
            await db.commit()
            logger.info(f"New player created: {user_id} ({username})")
            return (user_id, username, 0, 0, 0, 0)
        
        # Оновлюємо юзернейм, якщо він змінився
        if player[1] != username:
            await db.execute("UPDATE players SET username = ? WHERE user_id = ?", (username, user_id))
            await db.commit()
            logger.info(f"Player username updated: {user_id} ({username})")
            player = (user_id, username, player[2], player[3], player[4], player[5])
            
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
            
            # 1. Додаємо гру
            games_played += 1
            
            # 2. Оновлюємо перемоги та XP
            if is_winner:
                if is_spy:
                    spy_wins += 1
                    total_xp += XP_SPY_WIN
                else:
                    civilian_wins += 1
                    total_xp += XP_CIVILIAN_WIN
            
            # 3. Зберігаємо в БД
            await db.execute(
                "UPDATE players SET total_xp = ?, games_played = ?, spy_wins = ?, civilian_wins = ? WHERE user_id = ?",
                (total_xp, games_played, spy_wins, civilian_wins, user_id)
            )
            await db.commit()
            logger.info(f"Stats updated for {user_id}. XP: {total_xp}, Games: {games_played}")
            
    except Exception as e:
        logger.error(f"Failed to update stats for {user_id}: {e}", exc_info=True)

# --- НОВЕ: Функції розрахунку рівня та XP (ЗМІНЕНО) ---

# Кеш для XP (щоб не рахувати 100 разів)
xp_level_cache = {}

def get_level_from_xp(total_xp):
    """Розраховує рівень гравця на основі загального XP."""
    if total_xp < 20: # Базовий рівень (ЗМІНЕНО)
        return 1, 20 # (Рівень 1, XP до наступного) (ЗМІНЕНО)

    if total_xp in xp_level_cache:
        return xp_level_cache[total_xp]

    level = 1
    xp_needed_for_next = 20 # (ЗМІНЕНО)
    current_total_xp_needed = 0 # Загальна сума XP для досягнення рівня
    
    multiplier = 1.50 # Початковий множник

    while True:
        current_total_xp_needed += xp_needed_for_next
        level += 1
        
        if total_xp < current_total_xp_needed:
            # Гравець знаходиться на цьому рівні
            level -= 1
            xp_to_next = current_total_xp_needed - (current_total_xp_needed - xp_needed_for_next)
            xp_level_cache[total_xp] = (level, xp_to_next)
            return level, xp_to_next # (Поточний рівень, XP для наступного)
            
        # Розрахунок XP для *наступного* рівня
        xp_needed_for_next = int(xp_needed_for_next * multiplier)
        
        # Зменшуємо множник, але не нижче 1.2
        if multiplier > 1.20:
            multiplier = max(1.20, multiplier - 0.02)

# --- Функції збереження та очистки ---

def save_rooms():
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time < SAVE_INTERVAL:
        return
    try:
        room_copy = {}
        for token, room in rooms.items():
            room_copy[token] = room.copy()
            room_copy[token]['banned_from_voting'] = list(room['banned_from_voting'])
            room_copy[token]['voters'] = list(room['voters'])
            room_copy[token]['messages'] = room_copy[token]['messages'][-100:]
        with open('rooms.json', 'w') as f:
            json.dump(room_copy, f)
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
                    room['owner'] = int(room['owner'])
                    room['participants'] = [(int(p[0]), p[1], p[2]) for p in room['participants']]
                    room['banned_from_voting'] = set(room['banned_from_voting'])
                    room['voters'] = set(room['voters'])
                    room['votes'] = {int(k): int(v) for k, v in room['votes'].items()}
                    room['timer_task'] = None
                    room['last_activity'] = time.time()
                    room['last_minute_chat'] = room.get('last_minute_chat', False)
                    room['waiting_for_spy_guess'] = room.get('waiting_for_spy_guess', False)
                    room['is_test_game'] = room.get('is_test_game', False) 
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
                if room and room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                if token in rooms:
                    del rooms[token]
                    logger.info(f"Removed expired room: {token}")
            
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
            await asyncio.sleep(120)  # Перевіряємо кожні 2 хвилини
        except Exception as e:
            logger.error(f"Periodic webhook check failed: {e}", exc_info=True)
            await asyncio.sleep(120) 

async def check_maintenance(message: types.Message):
    if maintenance_mode and message.from_user.id != ADMIN_ID:
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
            logger.info(f"Cancelled timer for room {token} during maintenance")
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
    if message.from_user.id != ADMIN_ID:
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
    if message.from_user.id != ADMIN_ID:
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
        await asyncio.sleep(300) # 5 хв
        
        await send_maint_warning("Повторне попередження: Технічні роботи почнуться через **5 хвилин**.")
        await asyncio.sleep(240) # 4 хв
        
        await send_maint_warning("Останнє попередження! Технічні роботи почнуться через **1 хвилину**.")
        await asyncio.sleep(60) # 1 хв
        
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
    if message.from_user.id != ADMIN_ID:
        return
    
    global maintenance_timer_task
    if maintenance_timer_task and not maintenance_timer_task.done():
        await message.reply("Таймер вже запущено.")
        return
        
    maintenance_timer_task = asyncio.create_task(run_maintenance_timer())
    await message.reply("Запущено 10-хвилинний таймер до технічних робіт.\nЩоб скасувати: /cancel_maint")

@dp.message(Command("cancel_maint"))
async def cancel_maint_timer(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
        
    global maintenance_timer_task
    if not maintenance_timer_task or maintenance_timer_task.done():
        await message.reply("Таймер не запущено.")
        return
        
    maintenance_timer_task.cancel()
    maintenance_timer_task = None
    await message.reply("Таймер технічних робіт скасовано.")

@dp.message(Command("check_webhook"))
async def check_webhook(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        info = await bot.get_webhook_info()
        await message.reply(f"Webhook info: {info}")
    except Exception as e:
        await message.reply(f"Error checking webhook: {e}")

@dp.message(Command("reset_state"))
async def reset_state(message: types.Message, state: FSMContext):
    try:
        await state.clear()
        await message.reply("Стан FSM скинуто.")
    except Exception as e:
        await message.reply("Помилка при скиданні стану.")

def build_locations_keyboard(token: str, locations: list, columns: int = 3) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for location in locations:
        location_safe = location.replace(' ', '---')
        callback_data = f"spy_guess_{token}_{location_safe}"
        
        if len(callback_data.encode('utf-8')) > 64:
            logger.warning(f"Callback data too long, skipping location: {location} ({callback_data})")
            continue
            
        row.append(InlineKeyboardButton(text=location, callback_data=callback_data))
        if len(row) == columns:
            buttons.append(row)
            row = []
    if row: 
        buttons.append(row)
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.message(Command("testgame"))
async def test_game(message: types.Message, state: FSMContext): 
    if message.from_user.id != ADMIN_ID:
        return
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
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'last_activity': time.time(),
        'last_minute_chat': False, 'waiting_for_spy_guess': False, 'spy_guess': None, 'votes_for': 0, 'votes_against': 0
    }
    
    room = rooms[room_token]
    await start_game_logic(room, room_token, admin_is_spy=False) 
    await message.reply(f"Тестову кімнату створено: {room_token}\nШпигун: {room['spy']} (Бот)\nЛокація: {room['location']}")

@dp.message(Command("testgamespy"))
async def test_game_as_spy(message: types.Message, state: FSMContext): 
    if message.from_user.id != ADMIN_ID:
        return
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
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'last_activity': time.time(),
        'last_minute_chat': False, 'waiting_for_spy_guess': False, 'spy_guess': None, 'votes_for': 0, 'votes_against': 0
    }
    
    room = rooms[room_token]
    await start_game_logic(room, room_token, admin_is_spy=True) 
    await message.reply(f"Тестову кімнату створено: {room_token}\nШпигун: {room['spy']} (ВИ)\nЛокація: {room['location']}")


@dp.message(Command("whois"))
async def whois_spy(message: types.Message):
    if message.from_user.id != ADMIN_ID:
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

@dp.message(Command("stats"))
async def show_stats(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        return
        
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    try:
        # Отримуємо дані з БД
        stats = await get_player_stats(user_id, username)
        _, _, total_xp, games_played, spy_wins, civilian_wins = stats
        
        # Розраховуємо рівень
        level, xp_for_next = get_level_from_xp(total_xp)
        
        # Рахуємо вінрейт
        total_wins = spy_wins + civilian_wins
        winrate = (total_wins / games_played * 100) if games_played > 0 else 0
        
        # Розраховуємо XP для прогрес-бару
        xp_needed_for_level_start = get_level_from_xp(total_xp)[1] if level == 1 else get_level_from_xp(total_xp-xp_for_next)[1]
        xp_in_current_level = total_xp - (get_level_from_xp(total_xp)[1] - xp_for_next) # Це складно, але це вірно
        
        stats_text = (
            f"📊 **Ваша статистика** 📊\n\n"
            f"👤 **Нік:** {username}\n"
            f"🎖 **Рівень:** {level}\n"
            f"✨ **Досвід (XP):** {total_xp} / {xp_for_next} (до наступного рівня)\n"
            f"🏆 **Вінрейт:** {winrate:.1f}% (всього перемог: {total_wins})\n"
            f"🕹 **Всього ігор:** {games_played}\n\n"
            f"🕵️ **Перемог за Шпигуна:** {spy_wins}\n"
            f"👨‍🌾 **Перемог за Мирного:** {civilian_wins}"
        )
        
        # Parse mode MarkdownV2 вимагає екранування символів, тому використовуємо Markdown
        await message.reply(stats_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Failed to get stats for {user_id}: {e}", exc_info=True)
        await message.reply("Не вдалося завантажити вашу статистику. Спробуйте пізніше.")


# --- ФУНКЦІЇ МАТЧМЕЙКІНГУ (ПЕРЕНЕСЕНО ВГОРУ ДЛЯ ВИПРАВЛЕННЯ Pylance) ---
async def notify_queue_updates():
    """Повідомляє всіх гравців у черзі про поточний розмір черги."""
    queue_size = len(matchmaking_queue)
    if queue_size == 0:
        return
        
    logger.info(f"Notifying {queue_size} players in queue.")
    for pid, _ in matchmaking_queue:
        try:
            await bot.send_message(pid, f"Пошук... з вами в черзі: {queue_size} гравців.")
        except Exception:
            pass # Юзер міг заблокувати бота

async def create_game_from_queue(players: list):
    """Створює кімнату та запускає гру для списку гравців з черги."""
    if not players:
        return
        
    logger.info(f"Creating game from queue for {len(players)} players.")
    
    room_token = f"auto_{uuid.uuid4().hex[:4]}"
    owner_id = random.choice([p[0] for p in players]) # Випадковий власник
    participants_list = [(pid, uname, None) for pid, uname in players]
    
    rooms[room_token] = {
        'owner': owner_id, 'participants': participants_list, 'game_started': False, 'is_test_game': False, 
        'spy': None, 'location': None, 'messages': [], 'votes': {}, 'banned_from_voting': set(),
        'vote_in_progress': False, 'voters': set(), 'timer_task': None, 'last_activity': time.time(),
        'last_minute_chat': False, 'waiting_for_spy_guess': False, 'spy_guess': None, 'votes_for': 0, 'votes_against': 0
    }
    
    room = rooms[room_token]
    
    for pid, _ in players:
        try:
            # Скидаємо стан FSM
            await dp.storage.set_state(bot=bot, chat_id=pid, user_id=pid, state=None)
            await bot.send_message(pid, f"Гру знайдено! Підключаю до кімнати {room_token}...")
        except Exception as e:
            logger.error(f"Failed to notify player {pid} about matched game: {e}")
            
    await start_game_logic(room, room_token) # Запускаємо гру

async def matchmaking_processor():
    """Фоновий процес, який кожні 10 сек аналізує чергу та створює ігри."""
    global matchmaking_queue
    while True:
        await asyncio.sleep(10) # Чекаємо 10 секунд
        
        try:
            if maintenance_mode or not matchmaking_queue:
                continue
                
            queue_size = len(matchmaking_queue)
            if queue_size < 3:
                continue # Недостатньо гравців
                
            logger.info(f"Matchmaking processor running with {queue_size} players.")
            
            # Робимо копію і очищуємо оригінал
            players_to_process = matchmaking_queue.copy()
            matchmaking_queue.clear()
            random.shuffle(players_to_process)
            
            # Логіка "Кращого рішення"
            while len(players_to_process) >= 3:
                total = len(players_to_process)
                
                # 1. Випадки, коли ділимо навпіл (6-16 гравців)
                if 6 <= total <= 16:
                    room_size = total // 2
                # 2. Випадок, коли гравців забагато (>16)
                elif total > 16:
                    room_size = 8
                # 3. Випадки 3, 4, 5 - забираємо всіх
                else: 
                    room_size = total # (3, 4, or 5)
                
                # Забираємо гравців з черги
                room_players = players_to_process[:room_size]
                players_to_process = players_to_process[room_size:]
                
                # Створюємо гру
                await create_game_from_queue(room_players)
            
            # Повертаємо "залишок" (0, 1 або 2 гравці) назад у чергу
            if players_to_process:
                logger.info(f"Putting {len(players_to_process)} players back in queue.")
                matchmaking_queue.extend(players_to_process)
                # Повідомляємо тих, хто залишився
                await notify_queue_updates()
                
        except Exception as e:
            logger.error(f"Matchmaking processor error: {e}", exc_info=True)


# --- Основні Ігрові Команди ---

@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    active_users.add(message.from_user.id)
    if await check_maintenance(message):
        return
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Команди:\n"
        "/find_match - Швидкий пошук гри\n"
        "/create - Створити приватну кімнату\n"
        "/join - Приєднатися до кімнати за токеном\n\n"
        "Інші команди:\n"
        "/stats - Моя статистика та рівень\n"
        "/leave - Покинути кімнату/чергу\n"
        "/reset_state - Скинути стан бота (якщо щось зламалось)\n"
    )
    # --- ВИПРАВЛЕНО: прибрано parse_mode ---
    await message.reply(menu_text) 
    
    if message.from_user.id == ADMIN_ID:
        await message.reply(
            "Команди адміністратора:\n"
            "/maintenance_on - Увімкнути тех. роботи (миттєво)\n"
            "/maintenance_off - Вимкнути тех. роботи\n"
            "/maint_timer - Запустити 10-хв таймер до тех. робіт\n"
            "/cancel_maint - Скасувати таймер\n"
            "/check_webhook - Перевірити стан webhook\n"
            "/testgame - Запустити тестову гру (бот - шпигун)\n"
            "/testgamespy - Запустити тестову гру (ви - шпигун)\n"
            "/whois - (В приватні повідомлення) Показати шпигуна/локацію"
        )

@dp.message(Command("find_match"))
async def find_match(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        return
        
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    
    # Перевірка, чи юзер вже в кімнаті
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
            
    # Перевірка, чи юзер вже в черзі
    if any(user_id == p[0] for p in matchmaking_queue):
        await message.reply("Ви вже у пошуку! Щоб скасувати: /cancel_match")
        return
        
    matchmaking_queue.append((user_id, username))
    await state.set_state(PlayerState.in_queue)
    await message.reply("Пошук почався, заждіть... Щоб скасувати: /cancel_match")
    
    await notify_queue_updates() # Повідомляємо всіх у черзі про зміну
    
@dp.message(Command("cancel_match"), StateFilter(PlayerState.in_queue))
async def cancel_match(message: types.Message, state: FSMContext):
    global matchmaking_queue
    user_id = message.from_user.id
    
    matchmaking_queue = [p for p in matchmaking_queue if p[0] != user_id]
    await state.clear()
    await message.reply("Пошук скасовано.")
    
    await notify_queue_updates() # Повідомляємо інших

@dp.message(Command("create"))
async def create_room(message: types.Message, state: FSMContext):
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
        'last_activity': time.time(), 'last_minute_chat': False, 'waiting_for_spy_guess': False,
        'spy_guess': None, 'votes_for': 0, 'votes_against': 0
    }
    save_rooms()
    logger.info(f"Room created: {room_token}")
    await message.reply(
        f"Кімнату створено! Токен: {room_token}\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame."
    )

@dp.message(Command("join"))
async def join_room(message: types.Message, state: FSMContext):
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
            
    await message.answer("Введіть токен кімнати:")
    await state.set_state(PlayerState.waiting_for_token) 
    logger.info(f"User {user_id} prompted for room token")

@dp.message(StateFilter(PlayerState.waiting_for_token)) 
async def process_token(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        await state.clear()
        return
    
    active_users.add(message.from_user.id)
    token = message.text.strip().lower()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    
    if token in rooms:
        if rooms[token].get('is_test_game', False):
             await message.reply("Це тестова кімната, до неї не можна приєднатися.")
        elif rooms[token]['game_started']:
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.")
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
            await message.reply(f"Ви приєдналися до кімнати {token}!\nЧекайте, поки власник запустить гру (/startgame).")
        else:
            await message.reply("Ви вже в цій кімнаті!")
    else:
        await message.reply(f"Кімнати з токеном {token} не існує. Спробуйте ще раз.")
        
    await state.clear() 

@dp.message(Command("leave"))
async def leave_room(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        return
        
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        global matchmaking_queue
        matchmaking_queue = [p for p in matchmaking_queue if p[0] != user_id]
        await state.clear()
        await message.reply("Ви покинули чергу пошуку.")
        await notify_queue_updates()
        return
        
    active_users.add(message.from_user.id)
    logger.info(f"User {user_id} sent /leave")
    
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            room['last_activity'] = time.time()
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.")
            
            for pid, _, _ in room['participants']:
                if pid > 0: 
                    try:
                        await bot.send_message(pid, f"Гравець {username} покинув кімнату {token}.")
                    except Exception: pass
            
            if not room['participants'] or all(p[0] < 0 for p in room['participants']): 
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                if token in rooms: del rooms[token]
                logger.info(f"Room {token} deleted (empty or only bots left)")
            elif room['owner'] == user_id:
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                if token in rooms: del rooms[token]
                logger.info(f"Room {token} deleted (owner left)")
                for pid, _, _ in room['participants']:
                    if pid > 0: 
                        try:
                            await bot.send_message(pid, f"Кімната {token} закрита, бо власник покинув її.")
                        except Exception: pass
            save_rooms()
            return
            
    logger.info(f"User {user_id} not in any room or queue")
    await message.reply("Ви не перебуваєте в жодній кімнаті або черзі.")

@dp.message(Command("startgame"))
async def start_game(message: types.Message):
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
        
    available_callsigns = CALLSIGNS.copy()
    random.shuffle(available_callsigns)
    participant_list = [(pid, username, None) for pid, username, _ in room['participants']]
    room['participants'] = [(pid, username, available_callsigns[i]) for i, (pid, username, _) in enumerate(participant_list)]
    
    room['game_started'] = True
    room['location'] = random.choice(LOCATIONS)
    
    if room.get('is_test_game'):
        participant_ids = [p[0] for p in room['participants']]
        if admin_is_spy:
            room['spy'] = room['owner'] 
        else:
            bot_ids = [pid for pid in participant_ids if pid < 0]
            room['spy'] = random.choice(bot_ids) 
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
    save_rooms()
    
    logger.info(f"Game started in room {token}, spy: {room['spy']}, location: {room['location']}")
    
    commands = [BotCommand(command="early_vote", description="Дострокове завершення гри")]
    
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
                if not room.get('is_test_game'): 
                    await bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=pid))
                
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
                    await bot.send_message(pid, "Гра почалася! Спілкуйтеся вільно. Час гри: 20 хвилин.")

            except Exception as e:
                logger.error(f"Failed to send start message to user {pid}: {e}")
                
    room['timer_task'] = asyncio.create_task(run_timer(token))


@dp.message(Command("early_vote"))
async def early_vote(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if room.get('is_test_game', False):
                await message.reply("Ця функція вимкнена у тестових іграх.")
                return
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
                await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=user_id))
            except Exception as e:
                logger.error(f"Failed to delete commands for user {user_id}: {e}")
            save_rooms()
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="За", callback_data=f"early_vote_for_{token}"),
                    InlineKeyboardButton(text="Проти", callback_data=f"early_vote_against_{token}"),
                ]
            ])
            for pid, _, _ in room['participants']:
                if pid > 0: 
                    try:
                        await bot.send_message(pid, "Голосування за дострокове завершення гри! Час: 15 секунд.", reply_markup=keyboard)
                    except Exception: pass
            
            for i in range(15, 0, -1):
                if token not in rooms or not rooms[token]['vote_in_progress']:
                    return
                if i == 5:
                    for pid, _, _ in room['participants']:
                        if pid > 0: 
                            try:
                                await bot.send_message(pid, "5 секунд до кінця голосування!")
                            except Exception: pass
                await asyncio.sleep(1)
            
            if token not in rooms: return
            room['vote_in_progress'] = False
            votes_for = room['votes_for']
            votes_against = room['votes_against']
            room['last_activity'] = time.time()
            save_rooms()
            
            if votes_for > votes_against:
                room['game_started'] = False
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                for pid, _, _ in room['participants']:
                    if pid > 0: 
                        try:
                            await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                            await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                        except Exception: pass
                await show_voting_buttons(token)
            else:
                for pid, _, _ in room['participants']:
                    if pid > 0: 
                        try:
                            await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                        except Exception: pass
            return
            
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

@dp.callback_query(lambda c: c.data.startswith("early_vote_"))
async def early_vote_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    token = callback.data.split('_')[-1]
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
    if callback.data.startswith("early_vote_for"):
        room['votes_for'] += 1
        await callback.answer("Ви проголосували 'За'!")
    else:
        room['votes_against'] += 1
        await callback.answer("Ви проголосували 'Проти'!")
    room['last_activity'] = time.time()
    save_rooms()
    
    if len(room['voters']) == len(room['participants']):
        room['vote_in_progress'] = False
        votes_for = room['votes_for']
        votes_against = room['votes_against']
        room['last_activity'] = time.time()
        save_rooms()
        
        if votes_for > votes_against:
            room['game_started'] = False
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            for pid, _, _ in room['participants']:
                if pid > 0: 
                    try:
                        await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                        await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                    except Exception: pass
            await show_voting_buttons(token)
        else:
            for pid, _, _ in room['participants']:
                if pid > 0: 
                    try:
                        await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                    except Exception: pass

async def run_timer(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"Run timer: Room {token} not found")
            return
        
        if room.get('is_test_game'):
            await asyncio.sleep(60) # Чекаємо 1 хвилину
            if token not in rooms or not rooms[token]['game_started']:
                return
            
            logger.info(f"Test game {token} timer expired. Starting vote.")
            admin_id = room['owner']
            if admin_id > 0:
                try:
                    await bot.send_message(admin_id, "Тестова гра: 1 хвилина вийшла! Починаємо голосування.")
                except Exception as e:
                    logger.error(f"Failed to send test timer warning to admin {admin_id}: {e}")
            
            room['game_started'] = False
            room['last_activity'] = time.time()
            save_rooms()
            await show_voting_buttons(token) 
            return

        # Звичайна логіка таймера
        await asyncio.sleep(1140)
        if token not in rooms or not rooms[token]['game_started']:
            return
        room = rooms.get(token)
        if not room: return
        room['last_minute_chat'] = True
        for pid, _, _ in room['participants']:
            if pid > 0: 
                try:
                    await bot.send_message(pid, "Залишилась 1 хвилина до кінця гри! Спілкуйтеся вільно.")
                except Exception as e:
                    logger.error(f"Failed to send 1-minute warning to user {pid}: {e}")
        await asyncio.sleep(50)
        if token not in rooms or not rooms[token]['game_started']:
            return
        for i in range(10, -1, -1):
            if token not in rooms or not rooms[token]['game_started']:
                return
            room = rooms.get(token)
            if not room: return
            for pid, _, _ in room['participants']:
                if pid > 0: 
                    try:
                        await bot.send_message(pid, f"До кінця гри: {i} секунд")
                    except Exception as e:
                        logger.error(f"Failed to send timer update to user {pid}: {e}")
            await asyncio.sleep(1)
        room = rooms.get(token)
        if not room: return
        room['game_started'] = False
        room['last_minute_chat'] = False
        room['last_activity'] = time.time()
        save_rooms()
        for pid, _, _ in room['participants']:
            if pid > 0: 
                try:
                    await bot.send_message(pid, "Час вийшов! Голосуйте, хто шпигун.")
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
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
            await end_game(token)

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

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{username} ({callsign})", callback_data=f"vote_{token}_{pid}")]
            for pid, username, callsign in room['participants']
        ])
        
        if room.get('is_test_game'):
            admin_id = room['owner']
            spy_id = room['spy']
            
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

        # Таймер голосування
        for i in range(30, 0, -1):
            if token not in rooms: return
            room = rooms.get(token)
            if not room: return
            if i <= 10:
                for pid, _, _ in room['participants']:
                    if pid > 0: 
                        try:
                            await bot.send_message(pid, f"Час для голосування: {i} секунд")
                        except Exception as e:
                            logger.error(f"Failed to send voting timer to user {pid}: {e}")
            await asyncio.sleep(1)
            
            all_voted = len(room['votes']) == len(room['participants'])
            if all_voted:
                break
                
        room['last_activity'] = time.time()
        await process_voting_results(token)
    except Exception as e:
        logger.error(f"Show voting buttons error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

@dp.callback_query(lambda c: c.data.startswith('vote_'))
async def process_vote(callback_query: types.CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data.split('_')
        if len(data) != 3:
            logger.info(f"Invalid vote callback data: {callback_query.data}")
            await callback_query.answer("Помилка в голосуванні!")
            return
        token, voted_pid = data[1], data[2]
        voted_pid = int(voted_pid)
        room = rooms.get(token)
        if not room or user_id not in [p[0] for p in room['participants']]:
            logger.info(f"User {user_id} not in room {token} for voting")
            await callback_query.answer("Ви не в цій грі!")
            return
        if user_id in room['votes']:
            logger.info(f"User {user_id} already voted in room {token}")
            await callback_query.answer("Ви вже проголосували!")
            return
        room['votes'][user_id] = voted_pid
        room['last_activity'] = time.time()
        save_rooms()
        logger.info(f"User {user_id} voted for {voted_pid} in room {token}")
        await callback_query.answer("Ваш голос враховано!")
        
        all_voted = len(room['votes']) == len(room['participants'])
        if all_voted:
            await process_voting_results(token)
            
    except Exception as e:
        logger.error(f"Process vote error: {e}", exc_info=True)
        await callback_query.answer("Помилка при голосуванні!")

async def process_voting_results(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"process_voting_results: Room {token} not found")
            return
        room['last_activity'] = time.time()
        save_rooms()
        if not room['votes']:
            logger.info(f"No votes in room {token}")
            result = "Ніхто не проголосував. Шпигун переміг!"
            await end_game(token, result_message=result)
            return
            
        vote_counts = {}
        for voted_id in room['votes'].values():
            vote_counts[voted_id] = vote_counts.get(voted_id, 0) + 1
        
        if not vote_counts:
             await end_game(token)
             return
            
        max_votes = max(vote_counts.values())
        suspected = [pid for pid, count in vote_counts.items() if count == max_votes]
        logger.info(f"process_voting_results: Suspected players: {suspected}, Spy: {room['spy']}")
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
        
        
        if len(suspected) == 1 and suspected[0] == room['spy']:
            
            if not room.get('waiting_for_spy_guess') and room.get('spy_guess') is None:
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
                
                for i in range(30, 0, -1):
                    if token not in rooms: return 
                    room = rooms.get(token)
                    if not room: return
                    if not room['waiting_for_spy_guess']: 
                        return 

                    if i <= 10:
                        for pid, _, _ in room['participants']:
                            if pid > 0: 
                                try:
                                    await bot.send_message(pid, f"Час для вгадування локації: {i} секунд")
                                except Exception as e:
                                    logger.error(f"Failed to send spy guess timer to user {pid}: {e}")
                    await asyncio.sleep(1)
                
                room = rooms.get(token)
                if not room: return
                
                if room['waiting_for_spy_guess']: 
                    room['waiting_for_spy_guess'] = False
                    room['last_activity'] = time.time()
                    save_rooms()
                    logger.info(f"Spy {room['spy']} timed out in room {token}")
                    result = (
                        f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                        f"Локація: {room['location']}\n"
                        f"Час на вгадування вийшов! Шпигун не вгадав локацію. Гравці перемогли!"
                    )
                    await end_game(token, result_message=result) 
            
            elif not room.get('waiting_for_spy_guess') and room.get('spy_guess') is not None:
                spy_guess = room.get('spy_guess', '').lower().strip()
                logger.info(f"Spy guess in room {token}: {spy_guess}, Actual location: {room['location']}")
                
                if spy_guess == room['location'].lower():
                    result = (
                        f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                        f"Локація: {room['location']}\n"
                        f"Шпигун вгадав локацію! Шпигун переміг!"
                    )
                else:
                    result = (
                        f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                        f"Локація: {room['location']}\n"
                        f"Шпигун не вгадав локацію ({spy_guess}). Гравці перемогли!"
                    )
                
                await end_game(token, result_message=result) 

        else:
            result = (
                f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}\n"
                f"Шпигуна не знайшли. Шпигун переміг!"
            )
            await end_game(token, result_message=result)
            
    except Exception as e:
        logger.error(f"Process voting results error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

@dp.callback_query(lambda c: c.data.startswith('spy_guess_'))
async def process_spy_guess_callback(callback_query: types.CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        
        data_parts = callback_query.data.split('_', 2) 
        if len(data_parts) != 3:
            logger.warning(f"Invalid spy guess callback data (len): {callback_query.data}")
            await callback_query.answer("Помилка даних (1).")
            return

        token = data_parts[1]
        
        guessed_location_safe = data_parts[2]
        guessed_location = guessed_location_safe.replace('---', ' ')
        
        room = rooms.get(token)
        
        if not room:
            logger.warning(f"Spy guess: Room {token} not found for user {user_id}")
            await callback_query.answer("Помилка! Гру не знайдено. Можливо, час вийшов.")
            return
        
        if user_id != room.get('spy'):
            logger.warning(f"Spy guess: User {user_id} is not spy in room {token}")
            await callback_query.answer("Це не ваша гра або ви не шпигун!")
            return
        
        if not room.get('waiting_for_spy_guess'):
            logger.warning(f"Spy guess: Guessing time is over for room {token}")
            await callback_query.answer("Час на вгадування вийшов!")
            return
        
        room['spy_guess'] = guessed_location.strip()
        room['waiting_for_spy_guess'] = False 
        room['last_activity'] = time.time()
        save_rooms()
        
        await callback_query.answer(f"Ваш вибір: {guessed_location}")
        try:
            await callback_query.message.edit_text(f"Шпигун зробив свій вибір: {guessed_location}")
        except Exception as e:
            logger.info(f"Couldn't edit spy guess message: {e}")

        await process_voting_results(token)

    except Exception as e:
        logger.error(f"Process spy guess callback error: {e}", exc_info=True)
        await callback_query.answer("Критична помилка під час вибору!")


@dp.message()
async def handle_room_message(message: types.Message, state: FSMContext):
    try:
        if await check_maintenance(message):
            return
            
        user_id = message.from_user.id
        current_time = time.time()

        # --- ПОКРАЩЕНИЙ АНТИ-СПАМ: Працює завжди, 4 повідомлення/сек ---
        if user_id != ADMIN_ID: # Адмін має імунітет
            if user_id not in user_message_times:
                user_message_times[user_id] = {'timestamps': deque(), 'muted_until': 0, 'warned_spam': False, 'warned_unmuted': False}
            
            user_data = user_message_times[user_id]
            user_data['last_seen'] = current_time

            # 1. Перевірка, чи юзер в муті
            if user_data['muted_until'] > current_time:
                if not user_data['warned_spam']:
                    try:
                        await message.reply("ваш спам ніхто не бачить)")
                        user_data['warned_spam'] = True
                    except Exception: pass
                return 
            
            # 2. Перевірка, чи мут щойно закінчився
            if user_data['muted_until'] > 0 and current_time > user_data['muted_until']:
                user_data['muted_until'] = 0
                user_data['warned_spam'] = False
                user_data['warned_unmuted'] = True 
            
            # 3. Додаємо час повідомлення і перевіряємо на спам
            user_data['timestamps'].append(current_time)
            
            # 4. Видаляємо старі часові мітки (старіші за 1 секунду)
            while user_data['timestamps'] and current_time - user_data['timestamps'][0] > 1:
                user_data['timestamps'].popleft()
            
            # 5. Перевірка на спам (більше 4 повідомлень за 1 секунду)
            if len(user_data['timestamps']) > 4:
                user_data['muted_until'] = current_time + 5 
                user_data['warned_spam'] = True
                user_data['timestamps'].clear() 
                try:
                    await message.reply("ваш спам ніхто не бачить)")
                except Exception: pass
                return 

        # --- Кінець логіки Анти-спаму ---

        active_users.add(message.from_user.id)
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        username_clean = username.lstrip('@')
        
        for token, room in rooms.items():
            if user_id in [p[0] for p in room['participants']]:
                
                # --- НОВЕ: Блокування стікерів, GIF, тощо. ---
                if not message.text:
                    try:
                        await message.reply("Ніхто це не побачив( \n(Підтримуються тільки текстові повідомлення)")
                    except Exception: pass
                    return # Не відправляти
                
                # --- НОВЕ: Надсилаємо повідомлення про зняття муту ---
                if user_id != ADMIN_ID:
                    user_data = user_message_times[user_id]
                    if user_data.get('warned_unmuted', False):
                        user_data['warned_unmuted'] = False
                        try:
                            await message.reply("інші вже знову бачать що ви пишете.")
                        except Exception: pass
                # --- Кінець ---

                callsign = next((c for p, u, c in room['participants'] if p == user_id), None)
                if (room['game_started'] or room['last_minute_chat']) and callsign:
                    msg = f"{callsign}: {message.text}"
                else:
                    msg = f"@{username_clean}: {message.text}"
                    
                room['messages'].append(msg)
                room['messages'] = room['messages'][-100:]
                for pid, _, _ in room['participants']:
                    if pid != user_id and pid > 0: # не надсилати ботам
                        try:
                            await bot.send_message(pid, msg)
                        except Exception as e:
                            logger.error(f"Failed to send chat message to user {pid}: {e}")
                room['last_activity'] = time.time()
                save_rooms()
                return
                
        logger.info(f"User {user_id} not in any room for room message handler")
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create), приєднайтесь (/join) або шукайте гру (/find_match).")
    except Exception as e:
        logger.error(f"Handle room message error: {e}", exc_info=True)
        await message.reply("Виникла помилка при обробці повідомлення.")

async def end_game(token, result_message: str = None):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"end_game: Room {token} not found")
            return
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()

        # --- НОВЕ: Нагороджуємо XP та оновлюємо статистику ---
        if not room.get('is_test_game'): # Не нараховуємо XP за ігри з ботами
            spy_id = room.get('spy')
            spy_guess = room.get('spy_guess', '').lower().strip()
            correct_location = room.get('location', 'ERROR').lower()
            
            # Визначаємо, хто переміг
            spy_won = False
            if result_message: # Якщо є повідомлення про результат
                if "Шпигун переміг" in result_message:
                    spy_won = True
                elif "Шпигун вгадав локацію" in result_message:
                    spy_won = True
            
            # Оновлюємо стату для кожного гравця
            for pid, username, _ in room.get('participants', []):
                if pid <= 0: continue # Ігноруємо ботів
                
                is_player_spy = (pid == spy_id)
                is_player_winner = (is_player_spy == spy_won) # (Шпигун і шпигун виграв) АБО (Не шпигун і шпигун програв)
                
                await update_player_stats(pid, is_player_spy, is_player_winner)

        # --- Кінець логіки XP ---

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
        final_message += f"\nКод кімнати: {token}\nОпції:\n/leave - Покинути кімнату\n"

        owner_id = room['owner']
        for pid, _, _ in all_participants:
            if pid > 0: 
                try:
                    if not room.get('is_test_game') and str(token).startswith("auto_"):
                         await bot.send_message(pid, final_message + "/find_match - Шукати нову гру\n/stats - Моя статистика")
                    elif pid == owner_id and not room.get('is_test_game'): 
                        await bot.send_message(pid, final_message + "/startgame - Почати нову гру\n/stats - Моя статистика")
                    else:
                        await bot.send_message(pid, final_message + "\n/stats - Моя статистика")
                        
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                except Exception as e:
                    logger.error(f"Failed to send end game message to user {pid}: {e}")
        
        if room.get('is_test_game') or str(token).startswith("auto_"):
            await asyncio.sleep(5) 
            if token in rooms:
                del rooms[token]
                logger.info(f"Auto/Test room {token} deleted after game end.")
                save_rooms()
        else:
            room['game_started'] = False
            room['spy'] = None
            room['location'] = None
            room['votes'] = {}
            room['messages'] = []
            room['vote_in_progress'] = False
            room['banned_from_voting'] = set()
            room['timer_task'] = None
            room['last_activity'] = time.time()
            room['last_minute_chat'] = False
            room['waiting_for_spy_guess'] = False
            room['spy_guess'] = None
            room['votes_for'] = 0
            room['votes_against'] = 0
            room['participants'] = [(pid, username, None) for pid, username, _ in all_participants]
            save_rooms()
            logger.info(f"Private game ended in room {token}. Room reset.")

    except Exception as e:
        logger.error(f"End game error in room {token}: {e}", exc_info=True)

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

async def on_startup(_):
    try:
        logger.info("Starting bot initialization")
        
        # --- НОВЕ: Ініціалізуємо БД ---
        await db_init() 
        
        load_rooms()
        
        # --- ВИПРАВЛЕНО: Запускаємо matchmaking_processor ---
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