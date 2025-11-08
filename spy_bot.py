import logging
import asyncio
import random
import os
import json
import time
import psutil
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeChat
from aiogram.fsm.context import FSMContext
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web, ClientSession
import uuid
import aiohttp
import tenacity

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

# Глобальні змінні
maintenance_mode = False
active_users = set()
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
rooms = {}
last_save_time = 0
SAVE_INTERVAL = 5
ROOM_EXPIRY = 3600

# Логування версії та пам’яті
logger.info(f"Using aiohttp version: {aiohttp.__version__}")
process = psutil.Process()
logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# Функції для роботи з rooms
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
                    room['is_test_game'] = room.get('is_test_game', False) # Додаємо прапорець
                logger.info("Rooms loaded from rooms.json")
    except Exception as e:
        logger.error(f"Failed to load rooms: {e}", exc_info=True)

async def cleanup_rooms():
    while True:
        try:
            logger.info("Starting cleanup_rooms iteration")
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
                    del rooms[token]
                    logger.info(f"Removed expired room: {token}")
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
    await asyncio.sleep(20) # Дамо боту запуститися перед першою перевіркою
    while True:
        try:
            webhook_host = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
            webhook_url = f"https://{webhook_host}/webhook"
            info = await bot.get_webhook_info()
            logger.info(f"Periodic webhook check: {info.url}") # Скорочений лог
            if not info.url or info.url != webhook_url:
                logger.warning(f"Webhook is NOT SET or incorrect. Re-setting! Current: {info.url}, Expected: {webhook_url}")
                await set_webhook_with_retry(webhook_url)
            await asyncio.sleep(120)  # Перевіряємо кожні 2 хвилини
        except Exception as e:
            logger.error(f"Periodic webhook check failed: {e}", exc_info=True)
            await asyncio.sleep(120) # Повторити спробу через 2 хвилини

# Перевірка техобслуговування
async def check_maintenance(message: types.Message):
    if maintenance_mode and message.from_user.id != ADMIN_ID:
        await message.reply("Бот на технічному обслуговуванні. Зачекайте, будь ласка.")
        return True
    return False

# Команди адміністратора
@dp.message(Command("maintenance_on"))
async def maintenance_on(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ви не адміністратор!")
        return
    global maintenance_mode, rooms
    maintenance_mode = True
    active_users.add(message.from_user.id)
    for token, room in list(rooms.items()):
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
            logger.info(f"Cancelled timer for room {token} during maintenance")
    rooms.clear()
    save_rooms()
    for user_id in active_users:
        if user_id > 0: # Не надсилаємо ботам
            try:
                await bot.send_message(user_id, "Увага! Бот переходить на технічне обслуговування. Усі ігри завершено.")
            except Exception as e:
                logger.error(f"Failed to send maintenance_on message to {user_id}: {e}")
    await message.reply("Технічне обслуговування увімкнено.")

@dp.message(Command("maintenance_off"))
async def maintenance_off(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ви не адміністратор!")
        return
    global maintenance_mode
    maintenance_mode = False
    active_users.add(message.from_user.id)
    for user_id in active_users:
        if user_id > 0: # Не надсилаємо ботам
            try:
                await bot.send_message(user_id, "Технічне обслуговування завершено! Бот знову доступний для гри.")
            except Exception as e:
                logger.error(f"Failed to send maintenance_off message to {user_id}: {e}")
    await message.reply("Технічне обслуговування вимкнено.")

@dp.message(Command("check_webhook"))
async def check_webhook(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ця команда доступна лише адміністратору!")
        return
    try:
        info = await bot.get_webhook_info()
        logger.info(f"Webhook check requested by admin: {info}")
        await message.reply(f"Webhook info: {info}")
    except Exception as e:
        logger.error(f"Failed to check webhook: {e}", exc_info=True)
        await message.reply(f"Error checking webhook: {e}")

@dp.message(Command("reset_state"))
async def reset_state(message: types.Message, state: FSMContext):
    try:
        await state.clear()
        logger.info(f"FSM state reset for user {message.from_user.id}")
        await message.reply("Стан FSM скинуто.")
    except Exception as e:
        logger.error(f"Failed to reset FSM state: {e}", exc_info=True)
        await message.reply("Помилка при скиданні стану.")

# --- НОВА АДМІН-КОМАНДА ДЛЯ ТЕСТУВАННЯ ---
@dp.message(Command("testgame"))
async def test_game(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ви не адміністратор!")
        return
    if await check_maintenance(message):
        return
    
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"Admin {user_id} starting test game")

    # Перевіряємо, чи адмін вже в кімнаті
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return

    room_token = f"test_{uuid.uuid4().hex[:4]}" # Створюємо унікальний тестовий токен
    
    # Створюємо учасників: адмін + 3 боти (з негативними ID)
    participants = [
        (user_id, username, None),
        (-1, "Бот Василь", None),
        (-2, "Бот Степан", None),
        (-3, "Бот Галина", None)
    ]
    
    rooms[room_token] = {
        'owner': user_id,
        'participants': participants,
        'game_started': False,
        'is_test_game': True, # Головний прапорець
        'spy': None,
        'location': None,
        'messages': [],
        'votes': {},
        'banned_from_voting': set(),
        'vote_in_progress': False,
        'voters': set(),
        'timer_task': None,
        'last_activity': time.time(),
        'last_minute_chat': False,
        'waiting_for_spy_guess': False,
        'spy_guess': None,
        'votes_for': 0,
        'votes_against': 0
    }
    
    room = rooms[room_token]
    
    # --- Логіка, скопійована з /startgame ---
    available_callsigns = CALLSIGNS.copy()
    random.shuffle(available_callsigns)
    room['participants'] = [(pid, uname, available_callsigns[i]) for i, (pid, uname, _) in enumerate(room['participants'])]
    
    room['game_started'] = True
    room['location'] = random.choice(LOCATIONS)
    
    # Адмін може обрати себе шпигуном: /testgame spy
    make_admin_spy = 'spy' in message.text.lower()
    participant_ids = [p[0] for p in room['participants']]
    
    if make_admin_spy:
        room['spy'] = user_id
        logger.info(f"Test game {room_token}: Admin is spy.")
    else:
        bot_ids = [pid for pid in participant_ids if pid < 0]
        room['spy'] = random.choice(bot_ids)
        logger.info(f"Test game {room_token}: Bot {room['spy']} is spy.")

    room['last_activity'] = time.time()
    save_rooms()
    
    logger.info(f"Test game started in room {room_token}, spy: {room['spy']}, location: {room['location']}")

    # Надсилаємо роль тільки адміну
    admin_callsign = next(c for p, u, c in room['participants'] if p == user_id)
    if user_id == room['spy']:
        await bot.send_message(user_id, f"ТЕСТОВА ГРА.\nВи ШПИГУН ({admin_callsign})!\nЛокація: {room['location']} (ви її бачите для тесту).\nБоти проголосують за 1 хв.")
    else:
        await bot.send_message(user_id, f"ТЕСТОВА ГРА.\nЛокація: {room['location']}\nВи {admin_callsign}. Боти проголосують за 1 хв.")

    await message.reply(f"Тестову кімнату створено: {room_token}\nШпигун: {room['spy']}\nЛокація: {room['location']}")
    
    # Запускаємо таймер
    room['timer_task'] = asyncio.create_task(run_timer(room_token))

# --- Кінець нової команди ---

# Команди гри
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    active_users.add(message.from_user.id)
    logger.info(f"Processing /start command from user {message.from_user.id}")
    if await check_maintenance(message):
        return
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Команди:\n"
        "/create - Створити нову кімнату\n"
        "/join - Приєднатися до кімнати за токеном\n"
        "/startgame - Запустити гру (тільки власник)\n"
        "/leave - Покинути кімнату\n"
        "/early_vote - Дострокове завершення гри (під час гри)\n"
        "/reset_state - Скинути стан бота\n\n"
        "Гравці спілкуються вільно, гра триває 20 хвилин."
    )
    await message.reply(menu_text)
    if message.from_user.id == ADMIN_ID:
        await message.reply(
            "Команди адміністратора:\n"
            "/maintenance_on - Увімкнути технічне обслуговування\n"
            "/maintenance_off - Вимкнути технічне обслуговування\n"
            "/check_webhook - Перевірити стан webhook\n"
            "/testgame - Запустити тестову гру з ботами\n"
            "/testgame spy - Запустити тестову гру, де ви шпигун"
        )

@dp.message(Command("create"))
async def create_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"User {user_id} ({username}) attempting to create room")
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            if room['game_started']:
                logger.info(f"User {user_id} is in active game in room {token}")
                await message.reply("Ви в активній грі! Спочатку покиньте її (/leave).")
                return
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.")
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"Гравець {username} покинув кімнату {token}.")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about leave: {e}")
            if not room['participants']:
                del rooms[token]
                logger.info(f"Room {token} deleted (empty)")
            elif room['owner'] == user_id:
                del rooms[token]
                logger.info(f"Room {token} deleted (owner left)")
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, f"Кімната {token} закрита, бо власник покинув її.")
                        except Exception as e:
                            logger.error(f"Failed to notify user {pid} about room closure: {e}")
            save_rooms()
    room_token = str(uuid.uuid4())[:8].lower()
    rooms[room_token] = {
        'owner': user_id,
        'participants': [(user_id, username, None)],
        'game_started': False,
        'is_test_game': False, # Звичайна гра
        'spy': None,
        'location': None,
        'messages': [],
        'votes': {},
        'banned_from_voting': set(),
        'vote_in_progress': False,
        'voters': set(),
        'timer_task': None,
        'last_activity': time.time(),
        'last_minute_chat': False,
        'waiting_for_spy_guess': False,
        'spy_guess': None,
        'votes_for': 0,
        'votes_against': 0
    }
    save_rooms()
    logger.info(f"Room created: {room_token}, rooms: {list(rooms.keys())}")
    await message.reply(
        f"Кімнату створено! Токен: {room_token}\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame."
    )

@dp.message(Command("join"))
async def join_room(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    logger.info(f"User {user_id} sent /join")
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            logger.info(f"User {user_id} already in a room")
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    await message.answer("Введіть токен кімнати:")
    await state.set_state("waiting_for_token")
    logger.info(f"User {user_id} prompted for room token")

@dp.message(StateFilter("waiting_for_token"))
async def process_token(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        await state.clear()
        return
    active_users.add(message.from_user.id)
    token = message.text.strip().lower()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"User {user_id} attempting to join room with token: {token}, available rooms: {list(rooms.keys())}")
    if token in rooms:
        if rooms[token].get('is_test_game', False):
             await message.reply("Це тестова кімната, до неї не можна приєднатися.")
        elif rooms[token]['game_started']:
            logger.info(f"User {user_id} tried to join active game in room {token}")
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.")
        elif user_id not in [p[0] for p in rooms[token]['participants']]:
            rooms[token]['participants'].append((user_id, username, None))
            rooms[token]['last_activity'] = time.time()
            save_rooms()
            logger.info(f"User {user_id} ({username}) joined room {token}")
            for pid, _, _ in rooms[token]['participants']:
                if pid != user_id and pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"Гравець {username} приєднався до кімнати {token}!")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about join: {e}")
            await message.reply(f"Ви приєдналися до кімнати {token}!\nЧекайте, поки власник запустить гру (/startgame).")
        else:
            logger.info(f"User {user_id} already in room {token}")
            await message.reply("Ви вже в цій кімнаті!")
    else:
        logger.info(f"Room {token} not found for user {user_id}")
        await message.reply(f"Кімнати з токеном {token} не існує. Спробуйте ще раз.")
    await state.clear()

@dp.message(Command("leave"))
async def leave_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"User {user_id} sent /leave, checking rooms: {list(rooms.keys())}")
    for token, room in list(rooms.items()):
        logger.info(f"Room {token} participants: {[p[0] for p in room['participants']]}")
        if user_id in [p[0] for p in room['participants']]:
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            room['last_activity'] = time.time()
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.")
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"Гравець {username} покинув кімнату {token}.")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about leave: {e}")
            if not room['participants'] or all(p[0] < 0 for p in room['participants']): # Якщо кімната пуста або лишились тільки боти
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                del rooms[token]
                logger.info(f"Room {token} deleted (empty or only bots left)")
            elif room['owner'] == user_id:
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                del rooms[token]
                logger.info(f"Room {token} deleted (owner left)")
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, f"Кімната {token} закрита, бо власник покинув її.")
                        except Exception as e:
                            logger.error(f"Failed to notify user {pid} about room closure: {e}")
            save_rooms()
            return
    logger.info(f"User {user_id} not in any room")
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

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
                logger.info(f"User {user_id} is not owner of room {token}")
                await message.reply("Тільки власник може запустити гру!")
                return
            if room['game_started']:
                logger.info(f"Game already started in room {token}")
                await message.reply("Гра вже почалася!")
                return
            if len(room['participants']) < 3:
                logger.info(f"Not enough players in room {token}: {len(room['participants'])}")
                await message.reply("Потрібно щонайменше 3 гравці, щоб почати гру.")
                return
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            available_callsigns = CALLSIGNS.copy()
            random.shuffle(available_callsigns)
            participant_list = [(pid, username, None) for pid, username, _ in room['participants']]
            room['participants'] = [(pid, username, available_callsigns[i]) for i, (pid, username, _) in enumerate(participant_list)]
            room['game_started'] = True
            room['location'] = random.choice(LOCATIONS)
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
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=pid))
                    except Exception as e:
                        logger.error(f"Failed to set commands for user {pid}: {e}")
            for pid, username, callsign in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        if pid == room['spy']:
                            await bot.send_message(pid, f"Ви ШПИГУН ({callsign})! Спробуйте не видати себе.")
                        else:
                            await bot.send_message(pid, f"Локація: {room['location']}\nВи {callsign}. Один із гравців — шпигун!")
                    except Exception as e:
                        logger.error(f"Failed to send start message to user {pid}: {e}")
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, "Гра почалася! Спілкуйтеся вільно. Час гри: 20 хвилин.")
                    except Exception as e:
                        logger.error(f"Failed to send game start message to user {pid}: {e}")
            room['timer_task'] = asyncio.create_task(run_timer(token))
            return
    logger.info(f"User {user_id} not in any room for /startgame")
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

@dp.message(Command("early_vote"))
async def early_vote(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    logger.info(f"User {user_id} sent /early_vote")
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if room.get('is_test_game', False):
                await message.reply("Ця функція вимкнена у тестових іграх.")
                return
            if not room['game_started']:
                logger.info(f"Game not active in room {token}")
                await message.reply("Гра не активна!")
                return
            if user_id in room['banned_from_voting']:
                logger.info(f"User {user_id} banned from voting in room {token}")
                await message.reply("Ви вже використали дострокове голосування в цій партії!")
                return
            if room['vote_in_progress']:
                logger.info(f"Vote already in progress in room {token}")
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
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, "Голосування за дострокове завершення гри! Час: 15 секунд.", reply_markup=keyboard)
                    except Exception as e:
                        logger.error(f"Failed to send early vote keyboard to user {pid}: {e}")
            for i in range(15, 0, -1):
                if token not in rooms or not rooms[token]['vote_in_progress']:
                    return
                if i == 5:
                    for pid, _, _ in room['participants']:
                        if pid > 0: # Додано перевірку
                            try:
                                await bot.send_message(pid, "5 секунд до кінця голосування!")
                            except Exception as e:
                                logger.error(f"Failed to send 5s warning to user {pid}: {e}")
                await asyncio.sleep(1)
            if token not in rooms:
                return
            room['vote_in_progress'] = False
            votes_for = room['votes_for']
            votes_against = room['votes_against']
            room['last_activity'] = time.time()
            save_rooms()
            logger.info(f"Early vote result in room {token}: For={votes_for}, Against={votes_against}")
            if votes_for > votes_against:
                room['game_started'] = False
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                            await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                        except Exception as e:
                            logger.error(f"Failed to send early vote result to user {pid}: {e}")
                await show_voting_buttons(token)
            else:
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                        except Exception as e:
                            logger.error(f"Failed to send early vote failure to user {pid}: {e}")
            return
    logger.info(f"User {user_id} not in any room for /early_vote")
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

@dp.callback_query(lambda c: c.data.startswith("early_vote_"))
async def early_vote_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    token = callback.data.split('_')[-1]
    room = rooms.get(token)
    if not room or user_id not in [p[0] for p in room['participants']]:
        logger.info(f"User {user_id} not in room {token} for early vote")
        await callback.answer("Ви не в цій грі!")
        return
    if not room['vote_in_progress']:
        logger.info(f"Vote not in progress in room {token}")
        await callback.answer("Голосування закінчено!")
        return
    if user_id in room['voters']:
        logger.info(f"User {user_id} already voted in room {token}")
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
    logger.info(f"User {user_id} voted in room {token}, voters: {len(room['voters'])}, participants: {len(room['participants'])}")
    if len(room['voters']) == len(room['participants']):
        room['vote_in_progress'] = False
        votes_for = room['votes_for']
        votes_against = room['votes_against']
        room['last_activity'] = time.time()
        save_rooms()
        logger.info(f"Early vote completed in room {token}: For={votes_for}, Against={votes_against}")
        if votes_for > votes_against:
            room['game_started'] = False
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                        await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                    except Exception as e:
                        logger.error(f"Failed to send early vote result to user {pid}: {e}")
            await show_voting_buttons(token)
        else:
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                    except Exception as e:
                        logger.error(f"Failed to send early vote failure to user {pid}: {e}")

async def run_timer(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"Run timer: Room {token} not found")
            return
        
        # --- МОДИФІКОВАНИЙ БЛОК ДЛЯ ТЕСТ-ГРИ ---
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
            await show_voting_buttons(token) # Переходимо до голосування
            return
        # --- КІНЕЦЬ МОДИФІКОВАНОГО БЛОКУ ---

        # Звичайна логіка таймера
        await asyncio.sleep(1140)
        if token not in rooms or not rooms[token]['game_started']:
            return
        room = rooms.get(token)
        if not room:
            return
        room['last_minute_chat'] = True
        for pid, _, _ in room['participants']:
            if pid > 0: # Додано перевірку
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
            if not room:
                return
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, f"До кінця гри: {i} секунд")
                    except Exception as e:
                        logger.error(f"Failed to send timer update to user {pid}: {e}")
            await asyncio.sleep(1)
        room = rooms.get(token)
        if not room:
            return
        room['game_started'] = False
        room['last_minute_chat'] = False
        room['last_activity'] = time.time()
        save_rooms()
        for pid, _, _ in room['participants']:
            if pid > 0: # Додано перевірку
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
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{username} ({callsign})", callback_data=f"vote_{token}_{pid}")]
            for pid, username, callsign in room['participants']
        ])
        
        # --- МОДИФІКОВАНИЙ БЛОК ДЛЯ ТЕСТ-ГРИ ---
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
                await bot.send_message(admin_id, "Тестова гра: Боти проголосували.\nОберіть, хто шпигун (30 секунд):", reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Failed to send test voting keyboard to admin {admin_id}: {e}")
        # --- КІНЕЦЬ МОДИФІКОВАНОГО БЛОКУ ---
        else:
            # Звичайна логіка
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, "Оберіть, хто шпигун (30 секунд):", reply_markup=keyboard)
                    except Exception as e:
                        logger.error(f"Failed to send voting keyboard to user {pid}: {e}")

        # Таймер голосування
        for i in range(30, 0, -1):
            if token not in rooms:
                logger.info(f"show_voting_buttons: Room {token} deleted during voting")
                return
            room = rooms.get(token)
            if not room:
                logger.info(f"show_voting_buttons: Room {token} not found during voting")
                return
            if i <= 10:
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, f"Час для голосування: {i} секунд")
                        except Exception as e:
                            logger.error(f"Failed to send voting timer to user {pid}: {e}")
            await asyncio.sleep(1)
            
            # Перевірка завершення голосування
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
        
        # Перевірка, чи всі проголосували
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
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, "Ніхто не проголосував. Шпигун переміг!")
                    except Exception as e:
                        logger.error(f"Failed to send no-votes result to user {pid}: {e}")
            room['last_activity'] = time.time()
            await end_game(token)
            return
        vote_counts = {}
        for voted_id in room['votes'].values():
            vote_counts[voted_id] = vote_counts.get(voted_id, 0) + 1
        
        # Може бути 0 голосів, якщо в кімнаті тільки боти (адмін лівнув)
        if not vote_counts:
             await end_game(token)
             return
            
        max_votes = max(vote_counts.values())
        suspected = [pid for pid, count in vote_counts.items() if count == max_votes]
        logger.info(f"process_voting_results: Suspected players: {suspected}, Spy: {room['spy']}")
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
        
        # --- ПОЧАТОК ЗМІНЕНОГО БЛОКУ ---
        
        if len(suspected) == 1 and suspected[0] == room['spy']:
            
            # Якщо ми ще не відправили кнопки (перший захід)
            if not room.get('waiting_for_spy_guess') and room.get('spy_guess') is None:
                room['waiting_for_spy_guess'] = True
                room['spy_guess'] = None # Скидаємо минулу спробу, якщо є
                room['last_activity'] = time.time()
                
                # --- Генеруємо кнопки ---
                correct_location = room['location']
                # Беремо 5 випадкових неправильних локацій
                wrong_locations = [loc for loc in LOCATIONS if loc != correct_location]
                random.shuffle(wrong_locations)
                options = wrong_locations[:5] 
                options.append(correct_location)
                # Перемішуємо правильну відповідь з неправильними
                random.shuffle(options)

                keyboard_buttons = []
                for location in options:
                    # Увага: переконуємось, що дані не довші за 64 байти
                    # Формат: spy_guess_TOKEN_ЛОКАЦІЯ
                    callback_data = f"spy_guess_{token}_{location}"
                    if len(callback_data.encode('utf-8')) > 64:
                        logger.warning(f"Callback data too long, skipping: {callback_data}")
                        continue # Пропускаємо занадто довгі назви (якщо є)
                    keyboard_buttons.append([InlineKeyboardButton(text=location, callback_data=callback_data)])
                
                reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
                # -------------------------

                save_rooms()
                logger.info(f"Spy {room['spy']} detected in room {token}, sending guess options")
                
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            if pid == room['spy']:
                                await bot.send_message(pid, "Гравці проголосували за вас! Вгадайте локацію (30 секунд):", reply_markup=reply_markup)
                            else:
                                await bot.send_message(pid, f"Гравці вважають, що шпигун — {spy_username} ({spy_callsign}). Чекаємо, чи вгадає він локацію (30 секунд).")
                        except Exception as e:
                            logger.error(f"Failed to send spy guess prompt to user {pid}: {e}")
                
                # Запускаємо 30-секундний таймер на вгадування
                for i in range(30, 0, -1):
                    if token not in rooms: return # Кімнату видалили
                    room = rooms.get(token)
                    if not room: return
                    # Якщо шпигун вгадав (змінив 'waiting_for_spy_guess' на False), виходимо з таймера
                    if not room['waiting_for_spy_guess']: 
                        return # Результат вже обробляється в process_spy_guess_callback

                    if i <= 10:
                        for pid, _, _ in room['participants']:
                            if pid > 0: # Додано перевірку
                                try:
                                    await bot.send_message(pid, f"Час для вгадування локації: {i} секунд")
                                except Exception as e:
                                    logger.error(f"Failed to send spy guess timer to user {pid}: {e}")
                    await asyncio.sleep(1)
                
                # Якщо ми дійшли сюди, 30 секунд вийшло, а шпигун не відповів
                room = rooms.get(token)
                if not room: return
                
                if room['waiting_for_spy_guess']: # Перевіряємо, чи він ще не відповів
                    room['waiting_for_spy_guess'] = False
                    room['last_activity'] = time.time()
                    save_rooms()
                    logger.info(f"Spy {room['spy']} timed out in room {token}")
                    # Шпигун не вгадав (час вийшов)
                    result = (
                        f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                        f"Локація: {room['location']}\n"
                        f"Час на вгадування вийшов! Шпигун не вгадав локацію. Гравці перемогли!"
                    )
                    for pid, _, _ in room['participants']:
                        if pid > 0: # Додано перевірку
                            try:
                                await bot.send_message(pid, result)
                            except Exception as e:
                                logger.error(f"Failed to send game result to user {pid}: {e}")
                    await end_game(token) # Завершуємо гру
            
            # Сюди ми потрапляємо, коли шпигун вже натиснув кнопку
            # і 'process_spy_guess_callback' викликав нас знову
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
                
                for pid, _, _ in room['participants']:
                    if pid > 0: # Додано перевірку
                        try:
                            await bot.send_message(pid, result)
                        except Exception as e:
                            logger.error(f"Failed to send game result to user {pid}: {e}")
                
                await end_game(token) # Завершуємо гру

        # Це стара логіка, якщо шпигуна НЕ вгадали
        else:
            result = (
                f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
                f"Локація: {room['location']}\n"
                f"Шпигуна не знайшли. Шпигун переміг!"
            )
            for pid, _, _ in room['participants']:
                if pid > 0: # Додано перевірку
                    try:
                        await bot.send_message(pid, result)
                    except Exception as e:
                        logger.error(f"Failed to send game result to user {pid}: {e}")
            await end_game(token)
            
        # --- КІНЕЦЬ ЗМІНЕНОГО БЛОКУ ---
            
    except Exception as e:
        logger.error(f"Process voting results error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

# --- ПОЧАТОК НОВОЇ ФУНКЦІЇ ---

@dp.callback_query(lambda c: c.data.startswith('spy_guess_'))
async def process_spy_guess_callback(callback_query: types.CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        # Розбиваємо дані: spy_guess_TOKEN_ЛОКАЦІЯ
        data_parts = callback_query.data.split('_', 2) 
        if len(data_parts) != 3:
            logger.warning(f"Invalid spy guess callback data: {callback_query.data}")
            await callback_query.answer("Помилка даних.")
            return

        token = data_parts[1]
        guessed_location = data_parts[2]
        
        room = rooms.get(token)
        
        # Перевірки
        if not room or user_id != room.get('spy'):
            await callback_query.answer("Це не ваша гра або ви не шпигун!")
            return
        
        if not room.get('waiting_for_spy_guess'):
            await callback_query.answer("Час на вгадування вийшов!")
            return
        
        # Все добре, ми отримали відповідь
        room['spy_guess'] = guessed_location.strip()
        room['waiting_for_spy_guess'] = False # Зупиняємо очікування
        room['last_activity'] = time.time()
        save_rooms()
        
        await callback_query.answer(f"Ваш вибір: {guessed_location}")
        # Прибираємо кнопки
        try:
            await callback_query.message.edit_text(f"Шпигун зробив свій вибір: {guessed_location}")
        except Exception as e:
            logger.info(f"Couldn't edit spy guess message: {e}")

        # Тепер запускаємо фінальну обробку результатів
        await process_voting_results(token)

    except Exception as e:
        logger.error(f"Process spy guess callback error: {e}", exc_info=True)
        await callback_query.answer("Помилка під час вибору!")

# --- КІНЕЦЬ НОВОЇ ФУНКЦІЇ ---


@dp.message()
async def handle_room_message(message: types.Message, state: FSMContext):
    try:
        if await check_maintenance(message):
            return
        active_users.add(message.from_user.id)
        user_id = message.from_user.id
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        username_clean = username.lstrip('@')
        logger.info(f"User {user_id} sent message in room handler: {message.text}, available rooms: {list(rooms.keys())}")
        for token, room in rooms.items():
            logger.info(f"Checking room {token} for user {user_id}, participants: {[p[0] for p in room['participants']]}")
            if user_id in [p[0] for p in room['participants']]:
                callsign = next((c for p, u, c in room['participants'] if p == user_id), None)
                if room['game_started'] or room['last_minute_chat']:
                    msg = f"{callsign}: {message.text}"
                else:
                    msg = f"@{username_clean}: {message.text}"
                room['messages'].append(msg)
                room['messages'] = room['messages'][-100:]
                for pid, _, _ in room['participants']:
                    if pid != user_id and pid > 0: # Додано перевірку (не надсилати ботам)
                        try:
                            await bot.send_message(pid, msg)
                        except Exception as e:
                            logger.error(f"Failed to send chat message to user {pid}: {e}")
                room['last_activity'] = time.time()
                save_rooms()
                logger.info(f"Chat message in room {token}: {msg}")
                return
        logger.info(f"User {user_id} not in any room for room message handler")
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create) або приєднайтесь (/join).")
    except Exception as e:
        logger.error(f"Handle room message error: {e}", exc_info=True)
        await message.reply("Виникла помилка при обробці повідомлення.")

async def end_game(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"end_game: Room {token} not found")
            return
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
        result = (
            f"Гра завершена! Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {room['location']}\n"
            f"Код кімнати: {token}\n"
            "Опції:\n"
            "/leave - Покинути кімнату\n"
        )
        owner_id = room['owner']
        for pid, _, _ in room['participants']:
            if pid > 0: # Додано перевірку
                try:
                    if pid == owner_id:
                        await bot.send_message(pid, result + "/startgame - Почати нову гру")
                    else:
                        await bot.send_message(pid, result)
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                except Exception as e:
                    logger.error(f"Failed to send end game message to user {pid}: {e}")
        
        # Скидаємо кімнату (але не видаляємо, якщо це тест)
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
        room['participants'] = [(pid, username, None) for pid, username, _ in room['participants']]
        
        # Якщо це не тестова гра, скидаємо позивні
        if not room.get('is_test_game'):
             room['participants'] = [(pid, username, None) for pid, username, _ in room['participants']]
        
        save_rooms()
        logger.info(f"Game ended in room {token}")
        
        # Якщо це тестова гра, видаляємо кімнату
        if room.get('is_test_game'):
            await asyncio.sleep(5) # Даємо час прочитати результат
            if token in rooms:
                del rooms[token]
                logger.info(f"Test room {token} deleted after game end.")
                save_rooms()

    except Exception as e:
        logger.error(f"End game error in room {token}: {e}", exc_info=True)

@tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_exponential(multiplier=2, min=5, max=60),
    retry=tenacity.retry_if_exception_type(aiohttp.ClientError),
    before_sleep=lambda retry_state: logger.info(f"Retrying webhook setup, attempt {retry_state.attempt_number}")
)
async def set_webhook_with_retry(webhook_url):
    logger.info(f"Attempting to set webhook: {webhook_url}")
    await bot.delete_webhook(drop_pending_updates=True)
    # --- ВИПРАВЛЕНО: timeout -> request_timeout ---
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
        load_rooms()
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
        
        # Ми більше не видаляємо webhook тут, щоб уникнути "гонки станів"
        
        await bot.session.close()
        logger.info("Bot session closed. Shutdown successful.")
    except Exception as e:
        logger.error(f"Shutdown failed: {e}", exc_info=True)

app = web.Application()
webhook_path = "/webhook"
class CustomRequestHandler(SimpleRequestHandler):
    async def post(self, request):
        logger.info(f"Received webhook request: {request.method} {request.path}")
        try:
            data = await request.json()
            logger.info(f"Webhook data received: {data}")
            update = types.Update(**data)
            logger.info(f"Processed update: {update}")
            await dp.feed_update(bot, update)
            logger.info("Update successfully processed")
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Webhook processing error: {e}", exc_info=True)
            return web.Response(status=500)

if not USE_POLLING:
    CustomRequestHandler(dispatcher=dp, bot=bot).register(app, path=webhook_path)
    app.router.add_route('GET', '/health', health_check)
    app.router.add_route('HEAD', '/health', health_check) # Виправлено синтаксичну помилку
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
        raise # Виправлено синтаксичну помилку