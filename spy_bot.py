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
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.fsm.context import FSMContext
from aiohttp import web, ClientSession
import uuid
import aiohttp
import tenacity

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Завантажуємо змінні з .env
load_dotenv()

# Ініціалізація бота
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")
ADMIN_ID = int(os.getenv('ADMIN_ID', '5280737551'))
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
    "Водопад", "Спа салон", "Квартира", "Метро", "Кабінет 3011", "Каналізація",
    "Порт"
]
ANSWER_TIMEOUT_COMMENTS = [
    "{username} рішив поспати",
    "{username}! В тебе клавіатура заїла?",
    "{username}! Мовчиш? Мовчання - знак згоди!",
    "{username} відмовився відповідати",
    "{username} пішов чай заварити?"
]
rooms = {}
last_save_time = 0
SAVE_INTERVAL = 5
ROOM_EXPIRY = 3600  # 1 година

# Логування версії aiohttp і пам’яті
logger.info(f"Using aiohttp version: {aiohttp.__version__}")
process = psutil.Process()
logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

# Функція для збереження rooms
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

# Функція для завантаження rooms
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
                    room['question_timer_task'] = None
                    room['answer_timer_task'] = None
                    room['last_activity'] = time.time()
                    room['first_round_completed'] = room.get('first_round_completed', False)
                    room['last_minute_chat'] = room.get('last_minute_chat', False)
                    logger.info(f"Loaded room with participants: {room['participants']}")
            logger.info("Rooms loaded from rooms.json")
    except Exception as e:
        logger.error(f"Failed to load rooms: {e}", exc_info=True)

# Функція для очищення старих кімнат
async def cleanup_rooms():
    while True:
        try:
            logger.info("Starting cleanup_rooms iteration")
            current_time = time.time()
            expired = []
            for token, room in list(rooms.items()):
                if room.get('waiting_for_spy_guess'):
                    continue  # Не видаляємо кімнату, якщо шпигун вгадує
                if current_time - room.get('last_activity', current_time) > ROOM_EXPIRY:
                    expired.append(token)
            for token in expired:
                room = rooms.get(token)
                if room:
                    if room.get('timer_task') and not room['timer_task'].done():
                        room['timer_task'].cancel()
                    if room.get('question_timer_task') and not room['question_timer_task'].done():
                        room['question_timer_task'].cancel()
                    if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                        room['answer_timer_task'].cancel()
                    del rooms[token]
                    logger.info(f"Removed expired room: {token}")
            save_rooms()
            memory_usage = process.memory_info().rss / 1024 / 1024
            logger.info(f"Cleanup complete. Memory usage: {memory_usage:.2f} MB, Active rooms: {len(rooms)}")
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Cleanup rooms error: {e}", exc_info=True)
            await asyncio.sleep(300)

# Keep-alive пінг
async def keep_alive():
    async with ClientSession() as session:
        while True:
            try:
                logger.info("Sending keep-alive ping")
                async with session.get(f"https://spy-game-bot.onrender.com/health") as resp:
                    logger.info(f"Keep-alive ping response: {resp.status}")
            except Exception as e:
                logger.error(f"Keep-alive error: {e}", exc_info=True)
            await asyncio.sleep(300)

# Обробник пінгу
async def health_check(request):
    logger.info(f"Health check received: {request.method} {request.path}")
    try:
        info = await bot.get_webhook_info()
        memory_usage = process.memory_info().rss / 1024 / 1024
        logger.info(f"Webhook status: {info}, Memory usage: {memory_usage:.2f} MB")
        if not info.url:
            webhook_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}/webhook"
            logger.warning(f"Webhook URL is empty, resetting to {webhook_url}")
            await bot.set_webhook(webhook_url, drop_pending_updates=True, max_connections=100)
            logger.info(f"Webhook reset to {webhook_url}")
        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error(f"Health check error: {e}", exc_info=True)
        return web.Response(text="ERROR", status=500)

# Стани для FSM
class RoomStates:
    waiting_for_token = "waiting_for_token"
    waiting_for_target = "waiting_for_target"

# Перевірка техобслуговування
async def check_maintenance(message: types.Message):
    if maintenance_mode and message.from_user.id != ADMIN_ID:
        await message.reply("Бот на технічному обслуговуванні. Зачекайте, будь ласка.")
        return True
    return False

# Команди техобслуговування
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
        if room.get('question_timer_task') and not room['question_timer_task'].done():
            room['question_timer_task'].cancel()
            logger.info(f"Cancelled question timer for room {token} during maintenance")
        if room.get('answer_timer_task') and not room['answer_timer_task'].done():
            room['answer_timer_task'].cancel()
            logger.info(f"Cancelled answer timer for room {token} during maintenance")
    rooms.clear()
    save_rooms()
    for user_id in active_users:
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
        try:
            await bot.send_message(user_id, "Технічне обслуговування завершено! Бот знову доступний для гри.")
        except Exception as e:
            logger.error(f"Failed to send maintenance_off message to {user_id}: {e}")
    await message.reply("Технічне обслуговування вимкнено.")

# Команда /check_webhook
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

# Команда /start
@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    active_users.add(message.from_user.id)
    if await check_maintenance(message):
        return
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Команди:\n"
        "/create - Створити нову кімнату\n"
        "/join - Приєднатися до кімнати за токеном\n"
        "/startgame - Запустити гру (тільки власник)\n"
        "/leave - Покинути кімнату\n"
        "/early_vote - Дострокове завершення гри (під час гри)\n\n"
        "Гравці задають питання по черзі (1 хвилина на питання, 30 секунд на відповідь)."
    )
    await message.reply(menu_text)
    if message.from_user.id == ADMIN_ID:
        await message.reply(
            "Команди адміністратора:\n"
            "/maintenance_on - Увімкнути технічне обслуговування\n"
            "/maintenance_off - Вимкнути технічне обслуговування\n"
            "/check_webhook - Перевірити стан webhook"
        )

# Команда /create
@dp.message(Command("create"))
async def create_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    # Видаляємо гравця з попередньої кімнати, якщо гра не почалася
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            if room['game_started']:
                await message.reply("Ви в активній грі! Спочатку покиньте її (/leave).")
                return
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            await message.reply(f"Ви покинули кімнату `{token}`.")
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(
                        pid,
                        f"Гравець {username} покинув кімнату `{token}`."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {pid} about leave: {e}")
            if not room['participants']:
                del rooms[token]
            elif room['owner'] == user_id:
                del rooms[token]
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(pid, f"Кімната `{token}` закрита, бо власник покинув її.")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about room closure: {e}")
            save_rooms()

    # Створюємо нову кімнату
    room_token = str(uuid.uuid4())[:8].lower()
    rooms[room_token] = {
        'owner': user_id,
        'participants': [(user_id, username, 0)],
        'game_started': False,
        'spy': None,
        'location': None,
        'messages': [],
        'votes': {},
        'waiting_for_spy_guess': False,
        'banned_from_voting': set(),
        'vote_in_progress': False,
        'votes_for': 0,
        'votes_against': 0,
        'voters': set(),
        'timer_task': None,
        'current_questioner': None,
        'current_target': None,
        'question_timer_task': None,
        'answer_timer_task': None,
        'waiting_for_answer': False,
        'question_order': 0,
        'first_round_completed': False,
        'last_minute_chat': False,
        'last_activity': time.time()
    }
    save_rooms()
    logger.info(f"Room created: {room_token}, rooms: {list(rooms.keys())}")

    await message.reply(
        f"Кімнату створено! Токен: `{room_token}`\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame."
    )

# Команда /join
@dp.message(Command("join"))
async def join_room(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    await message.answer("Введіть токен кімнати:")
    await state.set_state(RoomStates.waiting_for_token)
    logger.info(f"User {user_id} prompted for room token")

# Обробка токена
@dp.message(StateFilter(RoomStates.waiting_for_token))
async def process_token(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        await state.clear()
        return
    active_users.add(message.from_user.id)
    token = message.text.strip().lower()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    logger.info(f"Attempting to join room with token: {token}, available rooms: {list(rooms.keys())}")
    if token in rooms:
        if rooms[token]['game_started']:
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.")
        elif user_id not in [p[0] for p in rooms[token]['participants']]:
            rooms[token]['participants'].append((user_id, username, 0))
            rooms[token]['last_activity'] = time.time()
            save_rooms()
            for pid, _, _ in rooms[token]['participants']:
                if pid != user_id:
                    try:
                        await bot.send_message(
                            pid,
                            f"Гравець {username} приєднався до кімнати `{token}`!"
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about join: {e}")
            await message.reply(
                f"Ви приєдналися до кімнати `{token}`!\n"
                "Чекайте, поки власник запустить гру (/startgame)."
            )
        else:
            await message.reply("Ви вже в цій кімнаті!")
    else:
        await message.reply(f"Кімнати з токеном `{token}` не існує. Спробуйте ще раз.")
    await state.clear()

# Команда /leave
@dp.message(Command("leave"))
async def leave_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            room['last_activity'] = time.time()
            await message.reply(f"Ви покинули кімнату `{token}`.")
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(
                        pid,
                        f"Гравець {message.from_user.first_name} покинув кімнату `{token}`."
                    )
                except Exception as e:
                    logger.error(f"Failed to notify user {pid} about leave: {e}")
            if not room['participants']:
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                    logger.info(f"Cancelled timer for room {token} due to empty room")
                if room.get('question_timer_task') and not room['question_timer_task'].done():
                    room['question_timer_task'].cancel()
                    logger.info(f"Cancelled question timer for room {token} due to empty room")
                if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                    room['answer_timer_task'].cancel()
                    logger.info(f"Cancelled answer timer for room {token} due to empty room")
                del rooms[token]
            elif room['owner'] == user_id:
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                    logger.info(f"Cancelled timer for room {token} due to owner leaving")
                if room.get('question_timer_task') and not room['question_timer_task'].done():
                    room['question_timer_task'].cancel()
                    logger.info(f"Cancelled question timer for room {token} due to owner leaving")
                if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                    room['answer_timer_task'].cancel()
                    logger.info(f"Cancelled answer timer for room {token} due to owner leaving")
                del rooms[token]
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(pid, f"Кімната `{token}` закрита, бо власник покинув її.")
                    except Exception as e:
                        logger.error(f"Failed to notify user {pid} about room closure: {e}")
            save_rooms()
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Команда /startgame
@dp.message(Command("startgame"))
async def start_game(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if room['owner'] != user_id:
                await message.reply("Тільки власник може запустити гру!")
                return
            if room['game_started']:
                await message.reply("Гра вже почалася!")
                return
            if len(room['participants']) < 3:
                await message.reply("Потрібно щонайменше 3 гравці, щоб почати гру.")
                return
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            if room.get('question_timer_task') and not room['question_timer_task'].done():
                room['question_timer_task'].cancel()
            if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                room['answer_timer_task'].cancel()
            participant_list = [(pid, username, 0) for pid, username, _ in room['participants']]
            random.shuffle(participant_list)
            room['participants'] = [(pid, username, i + 1) for i, (pid, username, _) in enumerate(participant_list)]
            room['game_started'] = True
            room['location'] = random.choice(LOCATIONS)
            room['spy'] = random.choice([p[0] for p in room['participants']])
            room['banned_from_voting'] = set()
            room['votes'] = {}
            room['vote_in_progress'] = False
            room['votes_for'] = 0
            room['votes_against'] = 0
            room['voters'] = set()
            room['current_questioner'] = None
            room['current_target'] = None
            room['question_order'] = 0
            room['waiting_for_answer'] = False
            room['first_round_completed'] = False
            room['last_minute_chat'] = False
            room['last_activity'] = time.time()
            save_rooms()

            commands = [BotCommand(command="early_vote", description="Дострокове завершення гри")]
            for pid, _, _ in room['participants']:
                try:
                    await bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=pid))
                except Exception as e:
                    logger.error(f"Failed to set commands for user {pid}: {e}")
            for pid, username, order in room['participants']:
                try:
                    if pid == room['spy']:
                        await bot.send_message(pid, f"Ви ШПИГУН (Гравець {order})! Спробуйте вгадати локацію, не видавши себе.")
                    else:
                        await bot.send_message(pid, f"Локація: {room['location']}\nВи Гравець {order}. Один із гравців — шпигун!")
                except Exception as e:
                    logger.error(f"Failed to send start message to user {pid}: {e}")
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(pid, "Гра почалася! Задавайте питання по черзі (1 хвилина на питання, 30 секунд на відповідь). Час гри: 20 хвилин.")
                except Exception as e:
                    logger.error(f"Failed to send game start message to user {pid}: {e}")
            room['timer_task'] = asyncio.create_task(run_timer(token))
            room['question_timer_task'] = asyncio.create_task(manage_question_queue(token))
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Команда /early_vote
@dp.message(Command("early_vote"))
async def early_vote(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
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
                try:
                    await bot.send_message(
                        pid,
                        "Голосування за дострокове завершення гри! Час: 15 секунд.",
                        reply_markup=keyboard
                    )
                except Exception as e:
                    logger.error(f"Failed to send early vote keyboard to user {pid}: {e}")

            for i in range(15, 0, -1):
                if token not in rooms or not rooms[token]['vote_in_progress']:
                    return
                if i == 5:
                    for pid, _, _ in room['participants']:
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
            if votes_for > votes_against:
                room['game_started'] = False
                if room.get('timer_task') and not room['timer_task'].done():
                    room['timer_task'].cancel()
                if room.get('question_timer_task') and not room['question_timer_task'].done():
                    room['question_timer_task'].cancel()
                if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                    room['answer_timer_task'].cancel()
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                        await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                    except Exception as e:
                        logger.error(f"Failed to send early vote result to user {pid}: {e}")
                await show_voting_buttons(token)
            else:
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                    except Exception as e:
                        logger.error(f"Failed to send early vote failure to user {pid}: {e}")
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Обробник дострокового голосування
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
            if room.get('question_timer_task') and not room['question_timer_task'].done():
                room['question_timer_task'].cancel()
            if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                room['answer_timer_task'].cancel()
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                except Exception as e:
                    logger.error(f"Failed to send early vote result to user {pid}: {e}")
            await show_voting_buttons(token)
        else:
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                except Exception as e:
                    logger.error(f"Failed to send early vote failure to user {pid}: {e}")

# Таймер гри
async def run_timer(token):
    try:
        room = rooms.get(token)
        if not room:
            return
        await asyncio.sleep(1140)  # 20 хвилин - 60 секунд
        if token not in rooms or not rooms[token]['game_started']:
            return
        room = rooms.get(token)
        if not room:
            return
        room['last_minute_chat'] = True
        for pid, _, _ in room['participants']:
            try:
                await bot.send_message(pid, "Залишилась 1 хвилина до кінця гри! Ви можете спілкуватися вільно.")
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
        if room.get('question_timer_task') and not room['question_timer_task'].done():
            room['question_timer_task'].cancel()
        if room.get('answer_timer_task') and not room['answer_timer_task'].done():
            room['answer_timer_task'].cancel()
        room['last_activity'] = time.time()
        save_rooms()
        for pid, _, _ in room['participants']:
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

# Вибір цілі для питання
async def select_target(token, questioner_pid, questioner_username, questioner_order):
    room = rooms.get(token)
    if not room:
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{username} (Гравець {order})", callback_data=f"select_target_{token}_{pid}")]
        for pid, username, order in room['participants'] if pid != questioner_pid
    ])
    try:
        await bot.send_message(
            questioner_pid,
            f"Оберіть, кому задати питання (30 секунд):",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Failed to send target selection keyboard to user {questioner_pid}: {e}")
    questioner_username = questioner_username.lstrip('@')
    for pid, _, _ in room['participants']:
        if pid != questioner_pid:
            try:
                await bot.send_message(
                    pid,
                    f"@{questioner_username} обирає, кого запитати."
                )
            except Exception as e:
                logger.error(f"Failed to send target selection notification to user {pid}: {e}")
    await asyncio.sleep(30)
    room = rooms.get(token)
    if not room or not room['game_started']:
        return
    if room['current_questioner'] == questioner_pid and not room['current_target']:
        questioner_username = questioner_username.lstrip('@')
        for pid, _, _ in room['participants']:
            try:
                await bot.send_message(
                    pid,
                    f"Гравець {questioner_order} (@{questioner_username}) не обрав ціль для питання."
                )
            except Exception as e:
                logger.error(f"Failed to send target timeout message to user {pid}: {e}")
        room['current_questioner'] = None
        room['question_order'] += 1
        room['last_activity'] = time.time()
        save_rooms()

# Обробник вибору цілі
@dp.callback_query(lambda c: c.data.startswith('select_target_'))
async def process_target_selection(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        data = callback.data.split('_')
        if len(data) != 3:
            await callback.answer("Помилка вибору!")
            return
        token, target_pid = data[1], int(data[2])
        room = rooms.get(token)
        if not room or user_id not in [p[0] for p in room['participants']]:
            await callback.answer("Ви не в цій грі!")
            return
        if user_id != room['current_questioner']:
            await callback.answer("Зараз не ваша черга обирати!")
            return
        if not room['game_started'] or room['current_target']:
            await callback.answer("Вибір уже зроблено або гра завершена!")
            return
        target = next((p for p in room['participants'] if p[0] == target_pid), None)
        if not target:
            await callback.answer("Гравець не знайдений!")
            return
        room['current_target'] = target_pid
        room['waiting_for_answer'] = False
        room['last_activity'] = time.time()
        save_rooms()
        questioner = next(p for p in room['participants'] if p[0] == user_id)
        questioner_username = questioner[1].lstrip('@')
        target_username = target[1].lstrip('@')
        for pid, _, _ in room['participants']:
            try:
                await bot.send_message(
                    pid,
                    f"@{questioner_username} задає питання @{target_username}. Час: 60 секунд."
                )
            except Exception as e:
                logger.error(f"Failed to send question prompt to user {pid}: {e}")
        await callback.answer("Ціль обрано!")
    except Exception as e:
        logger.error(f"Process target selection error: {e}", exc_info=True)
        await callback.answer("Помилка при виборі!")

# Керування чергою питань
async def manage_question_queue(token):
    try:
        room = rooms.get(token)
        if not room:
            return
        while token in rooms and rooms[token]['game_started'] and not rooms[token].get('vote_in_progress'):
            room = rooms.get(token)
            if not room:
                return
            room['question_order'] = room['question_order'] % len(room['participants'])
            questioner_idx = room['question_order']
            questioner = room['participants'][questioner_idx]
            room['current_questioner'] = questioner[0]
            room['current_target'] = None
            room['waiting_for_answer'] = False
            room['last_activity'] = time.time()
            save_rooms()

            if room['first_round_completed']:
                await select_target(token, questioner[0], questioner[1], questioner[2])
            else:
                possible_targets = [p for p in room['participants'] if p[0] != questioner[0]]
                if not possible_targets:
                    break
                target = random.choice(possible_targets)
                room['current_target'] = target[0]
                questioner_username = questioner[1].lstrip('@')
                target_username = target[1].lstrip('@')
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(
                            pid,
                            f"@{questioner_username} задає питання @{target_username}. Час: 60 секунд."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send question prompt to user {pid}: {e}")

            # Таймер питання з попередженням за 10 секунд
            for i in range(60, 0, -1):
                if token not in rooms or not rooms[token]['game_started'] or room['waiting_for_answer']:
                    return
                if i == 10:
                    try:
                        await bot.send_message(
                            questioner[0],
                            "10 секунд до кінця часу на питання!"
                        )
                    except Exception as e:
                        logger.error(f"Failed to send 10s warning to questioner {questioner[0]}: {e}")
                await asyncio.sleep(1)

            room = rooms.get(token)
            if not room:
                return
            if room['current_questioner'] == questioner[0] and not room['waiting_for_answer']:
                questioner_username = questioner[1].lstrip('@')
                for pid, _, _ in room['participants']:
                    try:
                        await bot.send_message(
                            pid,
                            f"Гравець {questioner[2]} (@{questioner_username}) не встиг задати питання."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send timeout message to user {pid}: {e}")
                room['current_questioner'] = None
                room['current_target'] = None
                room['question_order'] += 1
                if room['question_order'] == 0:
                    room['first_round_completed'] = True
                room['last_activity'] = time.time()
                save_rooms()
            else:
                room['question_order'] += 1
                if room['question_order'] == 0:
                    room['first_round_completed'] = True
                save_rooms()
    except asyncio.CancelledError:
        logger.info(f"Question queue: Question timer for room {token} was cancelled")
    except Exception as e:
        logger.error(f"Question queue error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

# Таймер відповіді
async def answer_timer(token, target_pid, target_username, target_order):
    try:
        room = rooms.get(token)
        if not room:
            return
        # Таймер відповіді з попередженням за 10 секунд
        for i in range(30, 0, -1):
            if token not in rooms or not rooms[token]['game_started'] or not room['waiting_for_answer']:
                return
            if i == 10:
                try:
                    await bot.send_message(
                        target_pid,
                        "10 секунд до кінця часу на відповідь!"
                    )
                except Exception as e:
                    logger.error(f"Failed to send 10s warning to target {target_pid}: {e}")
            await asyncio.sleep(1)

        room = rooms.get(token)
        if not room:
            return
        if room['waiting_for_answer'] and room['current_target'] == target_pid:
            target_username = target_username.lstrip('@')
            comment = random.choice(ANSWER_TIMEOUT_COMMENTS).format(username=f"@{target_username}")
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(
                        pid,
                        f"Гравець {target_order} (@{target_username}) не встиг відповісти: {comment}"
                    )
                except Exception as e:
                    logger.error(f"Failed to send answer timeout to user {pid}: {e}")
            room['waiting_for_answer'] = False
            room['current_questioner'] = None
            room['current_target'] = None
            room['question_order'] += 1
            if room['question_order'] == 0:
                room['first_round_completed'] = True
            room['last_activity'] = time.time()
            save_rooms()
    except asyncio.CancelledError:
        logger.info(f"Answer timer: Timer for room {token} was cancelled")
    except Exception as e:
        logger.error(f"Answer timer error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

# Голосування
async def show_voting_buttons(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"show_voting_buttons: Room {token} not found")
            return
        room['last_activity'] = time.time()  # Оновлюємо активність
        save_rooms()
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{username} (Гравець {order})", callback_data=f"vote_{token}_{pid}")]
            for pid, username, order in room['participants']
        ])
        for pid, _, _ in room['participants']:
            try:
                await bot.send_message(
                    pid,
                    "Оберіть, хто шпигун (30 секунд):",
                    reply_markup=keyboard
                )
            except Exception as e:
                logger.error(f"Failed to send voting keyboard to user {pid}: {e}")
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
                    try:
                        await bot.send_message(pid, f"Час для голосування: {i} секунд")
                    except Exception as e:
                        logger.error(f"Failed to send voting timer to user {pid}: {e}")
            await asyncio.sleep(1)
            if len(room['votes']) == len(room['participants']):
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

# Обробка голосів
@dp.callback_query(lambda c: c.data.startswith('vote_'))
async def process_vote(callback_query: types.CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data.split('_')
        if len(data) != 3:
            await callback_query.answer("Помилка в голосуванні!")
            return
        token, voted_pid = data[1], data[2]
        voted_pid = int(voted_pid)
        room = rooms.get(token)
        if not room or user_id not in [p[0] for p in room['participants']]:
            await callback_query.answer("Ви не в цій грі!")
            return
        if user_id in room['votes']:
            await callback_query.answer("Ви вже проголосували!")
            return
        room['votes'][user_id] = voted_pid
        room['last_activity'] = time.time()
        save_rooms()
        await callback_query.answer("Ваш голос враховано!")
    except Exception as e:
        logger.error(f"Process vote error: {e}", exc_info=True)
        await callback_query.answer("Помилка при голосуванні!")

# Підрахунок голосів
async def process_voting_results(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"process_voting_results: Room {token} not found")
            return
        room['last_activity'] = time.time()  # Оновлюємо активність
        save_rooms()
        if not room['votes']:
            for pid, _, _ in room['participants']:
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
        max_votes = max(vote_counts.values())
        suspected = [pid for pid, count in vote_counts.items() if count == max_votes]
        logger.info(f"process_voting_results: Suspected players: {suspected}, Spy: {room['spy']}")
        if len(suspected) == 1 and suspected[0] == room['spy']:
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(pid, f"Ви знайшли шпигуна, але він ще може вгадати локацію!")
                except Exception as e:
                    logger.error(f"Failed to send spy found message to user {pid}: {e}")
            if room['spy'] in [p[0] for p in room['participants']]:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[])
                row = []
                for loc in LOCATIONS:
                    row.append(InlineKeyboardButton(text=loc, callback_data=f"spy_guess_{token}_{loc}"))
                    if len(row) == 3:
                        keyboard.inline_keyboard.append(row)
                        row = []
                if row:
                    keyboard.inline_keyboard.append(row)
                try:
                    await bot.send_message(room['spy'], "Вгадайте локацію (30 секунд):", reply_markup=keyboard)
                    logger.info(f"Sent location guess keyboard to spy {room['spy']}")
                except Exception as e:
                    logger.error(f"Failed to send location keyboard to spy {room['spy']}: {e}")
                room['waiting_for_spy_guess'] = True
                for i in range(30, 0, -1):
                    if token not in rooms:
                        logger.info(f"process_voting_results: Room {token} deleted during spy guess")
                        return
                    room = rooms.get(token)
                    if not room:
                        logger.info(f"process_voting_results: Room {token} not found during spy guess")
                        return
                    if i <= 10:
                        try:
                            await bot.send_message(room['spy'], f"Час для вгадування: {i} секунд")
                        except Exception as e:
                            logger.error(f"Failed to send guess timer to spy {room['spy']}: {e}")
                    await asyncio.sleep(1)
                room = rooms.get(token)
                if not room:
                    return
                if room.get('waiting_for_spy_guess'):
                    room['waiting_for_spy_guess'] = False
                    spy_username = next(username for pid, username, _ in room['participants'] if pid == room['spy'])
                    result = (
                        f"Гра завершена! Шпигун: {spy_username}\n"
                        f"Локація: {room['location']}\n"
                        f"Шпигун не вгадав локацію. Гравці перемогли!"
                    )
                    for pid, _, _ in room['participants']:
                        try:
                            await bot.send_message(pid, result)
                            await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                        except Exception as e:
                            logger.error(f"Failed to send timeout guess result to user {pid}: {e}")
                    room['last_activity'] = time.time()
                    await end_game(token)
        else:
            spy_username = next(username for pid, username, _ in room['participants'] if pid == room['spy'])
            result = (
                f"Гра завершена! Шпигун: {spy_username}\n"
                f"Локація: {room['location']}\n"
                f"Шпигуна не знайшли. Шпигун переміг!"
            )
            for pid, _, _ in room['participants']:
                try:
                    await bot.send_message(pid, result)
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
                except Exception as e:
                    logger.error(f"Failed to send game result to user {pid}: {e}")
            room['last_activity'] = time.time()
            await end_game(token)
    except Exception as e:
        logger.error(f"Process voting results error in room {token}: {e}", exc_info=True)
        room = rooms.get(token)
        if room:
            room['game_started'] = False
            room['last_activity'] = time.time()
            await end_game(token)

# Обробка вгадування шпигуна
@dp.callback_query(lambda c: c.data.startswith('spy_guess_'))
async def handle_spy_guess_callback(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        data = callback.data.split('_', 2)
        if len(data) != 3:
            await callback.answer("Помилка вгадування!")
            return
        token, guess = data[1], data[2]
        room = rooms.get(token)
        if not room:
            await callback.answer("Гра не існує!")
            return
        if user_id != room['spy']:
            await callback.answer("Ви не шпигун!")
            return
        if not room.get('waiting_for_spy_guess'):
            await callback.answer("Час для вгадування минув!")
            return
        room['waiting_for_spy_guess'] = False
        room['last_activity'] = time.time()  # Оновлюємо активність
        save_rooms()
        actual_location = room['location'].lower()
        spy_username = next(username for pid, username, _ in room['participants'] if pid == room['spy'])
        if guess.lower() == actual_location:
            result = (
                f"Гра завершена! Шпигун: {spy_username}\n"
                f"Локація: {room['location']}\n"
                f"Шпигун вгадав локацію! Шпигун переміг!"
            )
        else:
            result = (
                f"Гра завершена! Шпигун: {spy_username}\n"
                f"Локація: {room['location']}\n"
                f"Шпигун не вгадав локацію. Гравці перемогли!"
            )
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('question_timer_task') and not room['question_timer_task'].done():
            room['question_timer_task'].cancel()
        if room.get('answer_timer_task') and not room['answer_timer_task'].done():
            room['answer_timer_task'].cancel()
        for pid, _, _ in room['participants']:
            try:
                await bot.send_message(pid, result)
                await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
            except Exception as e:
                logger.error(f"Failed to send guess result to user {pid}: {e}")
        room['last_activity'] = time.time()
        await end_game(token)
        await callback.answer("Ваш вибір враховано!")
    except Exception as e:
        logger.error(f"Handle spy guess error: {e}", exc_info=True)
        await callback.answer("Помилка при вгадуванні!")

# Чат у кімнаті
@dp.message()
async def handle_room_message(message: types.Message):
    try:
        if await check_maintenance(message):
            return
        active_users.add(message.from_user.id)
        user_id = message.from_user.id
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        username_clean = username.lstrip('@')
        for token, room in rooms.items():
            if user_id in [p[0] for p in room['participants']]:
                if not room['game_started'] or room['last_minute_chat']:
                    msg = f"@{username_clean}: {message.text}"
                    room['messages'].append(msg)
                    room['messages'] = room['messages'][-100:]
                    for pid, _, _ in room['participants']:
                        if pid != user_id:
                            try:
                                await bot.send_message(pid, msg)
                            except Exception as e:
                                logger.error(f"Failed to send free chat message to user {pid}: {e}")
                    room['last_activity'] = time.time()
                    save_rooms()
                    return

                target_pid = room['current_target']
                target = next((p for p in room['participants'] if p[0] == target_pid), None) if target_pid else None
                questioner = next((p for p in room['participants'] if p[0] == room['current_questioner']), None) if room['current_questioner'] else None
                if user_id == room['current_questioner'] and not room['waiting_for_answer']:
                    if not target:
                        await message.reply("Помилка: ціль не знайдена.")
                        return
                    questioner_order = next(p[2] for p in room['participants'] if p[0] == user_id)
                    target_username = target[1].lstrip('@')
                    msg = f"@{username_clean} задає питання @{target_username}: {message.text}"
                    room['messages'].append(msg)
                    room['messages'] = room['messages'][-100:]
                    for pid, _, _ in room['participants']:
                        try:
                            await bot.send_message(pid, msg)
                        except Exception as e:
                            logger.error(f"Failed to send question message to user {pid}: {e}")
                    room['waiting_for_answer'] = True
                    if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                        room['answer_timer_task'].cancel()
                    room['answer_timer_task'] = asyncio.create_task(
                        answer_timer(token, target[0], target[1], target[2])
                    )
                    room['last_activity'] = time.time()
                    save_rooms()
                    return
                if user_id == room['current_target'] and room['waiting_for_answer']:
                    if not questioner:
                        await message.reply("Помилка: автор питання не знайдений.")
                        return
                    target_order = next(p[2] for p in room['participants'] if p[0] == user_id)
                    questioner_username = questioner[1].lstrip('@')
                    msg = f"@{username_clean} відповідає @{questioner_username}: {message.text}"
                    room['messages'].append(msg)
                    room['messages'] = room['messages'][-100:]
                    for pid, _, _ in room['participants']:
                        try:
                            await bot.send_message(pid, msg)
                        except Exception as e:
                            logger.error(f"Failed to send answer message to user {pid}: {e}")
                    if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                        room['answer_timer_task'].cancel()
                        room['answer_timer_task'] = None
                    room['waiting_for_answer'] = False
                    room['current_questioner'] = None
                    room['current_target'] = None
                    room['question_order'] += 1
                    if room['question_order'] == 0:
                        room['first_round_completed'] = True
                    room['last_activity'] = time.time()
                    save_rooms()
                    if room['game_started'] and not room.get('vote_in_progress'):
                        room['question_timer_task'] = asyncio.create_task(manage_question_queue(token))
                    return
                await message.reply("Зараз не ваша черга! Очікуйте питання або відповіді.")
                return
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create) або приєднайтесь (/join).")
    except Exception as e:
        logger.error(f"Handle room message error: {e}", exc_info=True)
        await message.reply("Виникла помилка при обробці повідомлення.")

# Завершення гри
async def end_game(token):
    try:
        room = rooms.get(token)
        if not room:
            logger.info(f"end_game: Room {token} not found")
            return
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('question_timer_task') and not room['question_timer_task'].done():
            room['question_timer_task'].cancel()
        if room.get('answer_timer_task') and not room['answer_timer_task'].done():
            room['answer_timer_task'].cancel()
        spy_username = next(username for pid, username, _ in room['participants'] if pid == room['spy']) if room['spy'] else "Невідомо"
        result = (
            f"Гра завершена! Шпигун: {spy_username}\n"
            f"Локація: {room['location']}\n"
            f"Код кімнати: {token}\n"
            "Опції:\n"
            "/leave - Покинути кімнату\n"
        )
        owner_id = room['owner']
        for pid, _, _ in room['participants']:
            try:
                if pid == owner_id:
                    await bot.send_message(pid, result + "/startgame - Почати нову гру")
                else:
                    await bot.send_message(pid, result)
                await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=pid))
            except Exception as e:
                logger.error(f"Failed to send end game message to user {pid}: {e}")
        room['game_started'] = False
        room['spy'] = None
        room['location'] = None
        room['votes'] = {}
        room['messages'] = []
        room['waiting_for_spy_guess'] = False
        room['vote_in_progress'] = False
        room['banned_from_voting'] = set()
        room['timer_task'] = None
        room['current_questioner'] = None
        room['current_target'] = None
        room['question_timer_task'] = None
        room['answer_timer_task'] = None
        room['waiting_for_answer'] = False
        room['question_order'] = 0
        room['first_round_completed'] = False
        room['last_minute_chat'] = False
        room['last_activity'] = time.time()
        save_rooms()
    except Exception as e:
        logger.error(f"End game error in room {token}: {e}", exc_info=True)

# Налаштування webhook з retry
@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(aiohttp.ClientError),
    before_sleep=lambda retry_state: logger.info(f"Retrying webhook setup, attempt {retry_state.attempt_number}")
)
async def set_webhook_with_retry(webhook_url):
    logger.info(f"Attempting to set webhook: {webhook_url}")
    await bot.set_webhook(webhook_url, drop_pending_updates=True, max_connections=100)
    webhook_info = await bot.get_webhook_info()
    logger.info(f"Webhook set, current info: {webhook_info}")
    if not webhook_info.url:
        raise aiohttp.ClientError("Webhook URL is still empty after setting")
    logger.info(f"Webhook successfully set to {webhook_url}")

# Резервний polling
async def start_polling():
    try:
        logger.info("Starting polling as fallback")
        await dp.start_polling(bot, handle_signals=False)
    except Exception as e:
        logger.error(f"Polling failed: {e}", exc_info=True)
        await asyncio.sleep(10)
        await start_polling()

# Налаштування webhook
async def on_startup(_):
    try:
        logger.info("Starting bot initialization")
        load_rooms()
        webhook_host = os.getenv('RENDER_EXTERNAL_HOSTNAME')
        logger.info(f"RENDER_EXTERNAL_HOSTNAME: {webhook_host}")
        if not webhook_host:
            raise ValueError("RENDER_EXTERNAL_HOSTNAME is not set")
        webhook_url = f"https://{webhook_host}/webhook"
        logger.info(f"Setting webhook: {webhook_url}")
        await set_webhook_with_retry(webhook_url)
        webhook_info = await bot.get_webhook_info()
        logger.info(f"Webhook info after setup: {webhook_info}")
        if not webhook_info.url:
            logger.warning("Webhook not set, falling back to polling")
            asyncio.create_task(start_polling())
        asyncio.create_task(cleanup_rooms())
        asyncio.create_task(keep_alive())
        logger.info("Cleanup rooms and keep-alive tasks started")
        logger.info("Bot initialization completed")
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        asyncio.create_task(start_polling())

async def on_shutdown(_):
    try:
        logger.info("Shutting down server...")
        save_rooms()
        for token, room in list(rooms.items()):
            if room.get('timer_task') and not room['timer_task'].done():
                room['timer_task'].cancel()
            if room.get('question_timer_task') and not room['question_timer_task'].done():
                room['question_timer_task'].cancel()
            if room.get('answer_timer_task') and not room['answer_timer_task'].done():
                room['answer_timer_task'].cancel()
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.session.close()
        logger.info("Bot shutdown successfully")
    except Exception as e:
        logger.error(f"Shutdown failed: {e}", exc_info=True)

# Налаштування сервера
app = web.Application()
webhook_path = "/webhook"
class CustomRequestHandler(SimpleRequestHandler):
    async def post(self, request):
        logger.info(f"Received webhook request: {request.method} {request.path}")
        try:
            response = await super().post(request)
            logger.info(f"Webhook response: {response.status}")
            return response
        except Exception as e:
            logger.error(f"Webhook processing error: {e}", exc_info=True)
            return web.Response(status=500)

CustomRequestHandler(dispatcher=dp, bot=bot).register(app, path=webhook_path)
app.router.add_route('GET', '/health', health_check)
app.router.add_route('HEAD', '/health', health_check)
setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    try:
        port = int(os.getenv("PORT", 8443))
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)
        logger.info(f"Starting server on port {port}")
        web.run_app(app, host="0.0.0.0", port=port)
    except Exception as e:
        logger.error(f"Server failed to start: {e}", exc_info=True)
        raise