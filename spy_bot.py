import logging
import asyncio
import random
import os
import json
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeChat
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import uuid

# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Завантажуємо змінні з .env
load_dotenv()

# Ініціалізація бота
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")
ADMIN_ID = int(os.getenv('ADMIN_ID', '5280737551'))  # Заміни на свій Telegram ID
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)

# Глобальні змінні
maintenance_mode = False
active_users = set()
LOCATIONS = [
    "Аеропорт", "Банк", "Пляж", "Казино", "Цирк", "Школа", "Лікарня",
    "Готель", "Музей", "Ресторан", "Театр", "Парк", "Космічна станція"
]
rooms = {}

# Функція для збереження rooms у файл
def save_rooms():
    try:
        with open('rooms.json', 'w') as f:
            json.dump(rooms, f, default=str)
        logger.info("Rooms saved to rooms.json")
    except Exception as e:
        logger.error(f"Failed to save rooms: {e}")

# Функція для завантаження rooms із файлу
def load_rooms():
    global rooms
    try:
        if os.path.exists('rooms.json'):
            with open('rooms.json', 'r') as f:
                loaded_rooms = json.load(f)
                rooms = {k: v for k, v in loaded_rooms.items()}
                # Конвертуємо owner і participants назад у int
                for room in rooms.values():
                    room['owner'] = int(room['owner'])
                    room['participants'] = [(int(pid), username) for pid, username in room['participants']]
                    room['banned_from_voting'] = set(room['banned_from_voting'])
                    room['voters'] = set(room['voters'])
                    room['votes'] = {int(k): int(v) for k, v in room['votes'].items()}
            logger.info("Rooms loaded from rooms.json")
    except Exception as e:
        logger.error(f"Failed to load rooms: {e}")

# Стани для FSM
class RoomStates:
    waiting_for_token = "waiting_for_token"

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
    rooms.clear()
    save_rooms()  # Зберігаємо порожній rooms
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
    info = await bot.get_webhook_info()
    await message.reply(f"Webhook info: {info}")

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
        "/early_vote - Дострокове завершення гри (під час гри)"
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
    room_token = str(uuid.uuid4())[:8].lower()  # Токен у нижньому регістрі
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    rooms[room_token] = {
        'owner': user_id,
        'participants': [(user_id, username)],
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
        'voters': set()
    }
    save_rooms()  # Зберігаємо rooms після створення
    logger.info(f"Room created: {room_token}, rooms: {list(rooms.keys())}")

    await message.reply(
        f"Кімнату створено! Токен: `{room_token}`\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame."
    )

# Команда /join
@dp.message(Command("join"))
async def join_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    await message.answer("Введіть токен кімнати:")
    await dp.storage.set_state(user=user_id, state=RoomStates.waiting_for_token)

# Обробка токена
@dp.message(StateFilter(RoomStates.waiting_for_token))
async def process_token(message: types.Message):
    if await check_maintenance(message):
        await dp.storage.set_state(user=message.from_user.id, state=None)
        return
    active_users.add(message.from_user.id)
    token = message.text.strip().lower()  # Ігноруємо регістр
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    logger.info(f"Attempting to join room with token: {token}, available rooms: {list(rooms.keys())}")
    if token in rooms:
        if rooms[token]['game_started']:
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.")
        elif user_id not in [p[0] for p in rooms[token]['participants']]:
            rooms[token]['participants'].append((user_id, username))
            save_rooms()  # Зберігаємо rooms після приєднання
            for pid, _ in rooms[token]['participants']:
                if pid != user_id:
                    await bot.send_message(
                        pid,
                        f"Гравець {username} приєднався до кімнати `{token}`!"
                    )
            await message.reply(
                f"Ви приєдналися до кімнати `{token}`!\n"
                "Чекайте, поки власник запустить гру (/startgame)."
            )
        else:
            await message.reply("Ви вже в цій кімнаті!")
    else:
        await message.reply(f"Кімнати з токеном `{token}` не існує. Спробуйте ще раз.")
    await dp.storage.set_state(user=user_id, state=None)

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
            await message.reply(f"Ви покинули кімнату `{token}`.")
            for pid, _ in room['participants']:
                await bot.send_message(
                    pid,
                    f"Гравець {message.from_user.first_name} покинув кімнату `{token}`."
                )
            if not room['participants']:
                del rooms[token]
            elif room['owner'] == user_id:
                del rooms[token]
                for pid, _ in room['participants']:
                    await bot.send_message(pid, f"Кімната `{token}` закрита, бо власник покинув її.")
            save_rooms()  # Зберігаємо rooms після виходу
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
            room['game_started'] = True
            room['location'] = random.choice(LOCATIONS)
            room['spy'] = random.choice([p[0] for p in room['participants']])
            room['banned_from_voting'] = set()
            save_rooms()  # Зберігаємо rooms після старту гри

            # Встановлюємо меню з /early_vote
            commands = [BotCommand(command="early_vote", description="Дострокове завершення гри")]
            await bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=message.chat.id))

            for pid, username in room['participants']:
                if pid == room['spy']:
                    await bot.send_message(pid, "Ви ШПИГУН! Спробуйте вгадати локацію, не видавши себе.")
                else:
                    await bot.send_message(pid, f"Локація: {room['location']}\nОдин із гравців — шпигун!")
            for pid, _ in room['participants']:
                await bot.send_message(pid, "Гра почалася! Обговорюйте і шукайте шпигуна. Час: 10 хвилин.")
            await run_timer(token)
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Команда /early_vote
@dp.message(Command("early_vote"))
async def early_vote(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    chat_id = message.chat.id
    user_id = message.from_user.id
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if not room['game_started']:
                await message.reply("Гра не активна!")
                return
            if user_id in room['banned_from_voting']:
                await message.reply("Ви вже ініціювали голосування в цій партії!")
                return
            if room['vote_in_progress']:
                await message.reply("Голосування вже триває!")
                return

            room['vote_in_progress'] = True
            room['votes_for'] = 0
            room['votes_against'] = 0
            room['voters'] = set()
            save_rooms()  # Зберігаємо rooms перед голосуванням

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton("За", callback_data=f"early_vote_for_{token}"),
                    InlineKeyboardButton("Проти", callback_data=f"early_vote_against_{token}"),
                ]
            ])

            await message.reply("Голосування за дострокове завершення гри! Час: 15 секунд.", reply_markup=keyboard)

            for i in range(15, 0, -1):
                if token not in rooms or not room['vote_in_progress']:
                    return
                if i == 5:
                    for pid, _ in room['participants']:
                        await bot.send_message(pid, "5 секунд до кінця голосування!")
                await asyncio.sleep(1)

            room['vote_in_progress'] = False
            votes_for = room['votes_for']
            votes_against = room['votes_against']
            save_rooms()  # Зберігаємо rooms після голосування

            if votes_for > votes_against:
                room['game_started'] = False
                await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=chat_id))
                for pid, _ in room['participants']:
                    await bot.send_message(pid, f"Голосування успішне! Гра завершена. За: {votes_for}, Проти: {votes_against}")
                await end_game(token)
            else:
                room['banned_from_voting'].add(user_id)
                for pid, _ in room['participants']:
                    await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
            save_rooms()  # Зберігаємо rooms після результату
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Обробник дострокового голосування
@dp.callback_query(lambda c: c.data.startswith("early_vote_"))
async def early_vote_callback(callback: types.CallbackQuery):
    if maintenance_mode and callback.from_user.id != ADMIN_ID:
        await callback.answer("Бот на технічному обслуговуванні!")
        return
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
    save_rooms()  # Зберігаємо rooms після голосування

# Таймер гри
async def run_timer(token):
    room = rooms.get(token)
    if not room:
        return
    await asyncio.sleep(590)  # 10 хвилин мінус 10 секунд для відліку
    for i in range(10, -1, -1):
        if token not in rooms or not room['game_started']:
            return
        for pid, _ in room['participants']:
            await bot.send_message(pid, f"До кінця гри: {i} секунд")
        await asyncio.sleep(1)
    room['game_started'] = False
    save_rooms()  # Зберігаємо rooms після таймера
    for pid, _ in room['participants']:
        await bot.send_message(pid, "Час вийшов! Голосуйте, хто шпигун.")
    await show_voting_buttons(token)

# Голосування
async def show_voting_buttons(token):
    room = rooms.get(token)
    if not room:
        return
    keyboard = InlineKeyboardMarkup()
    for pid, username in room['participants']:
        keyboard.add(InlineKeyboardButton(username, callback_data=f"vote_{token}_{pid}"))
    for pid, _ in room['participants']:
        await bot.send_message(pid, "Оберіть, хто шпигун (30 секунд):", reply_markup=keyboard)
    await asyncio.sleep(20)
    for i in range(10, -1, -1):
        if token not in rooms:
            return
        for pid, _ in room['participants']:
            await bot.send_message(pid, f"Час для голосування: {i} секунд")
        await asyncio.sleep(1)
    await process_voting_results(token)

# Обробка голосів
@dp.callback_query(lambda c: c.data.startswith('vote_'))
async def process_vote(callback_query: types.CallbackQuery):
    if maintenance_mode and callback_query.from_user.id != ADMIN_ID:
        await callback_query.answer("Бот на технічному обслуговуванні!")
        return
    user_id = callback_query.from_user.id
    token, voted_pid = callback_query.data.split('_')[1], callback_query.data.split('_')[2]
    voted_pid = int(voted_pid)
    room = rooms.get(token)
    if not room or user_id not in [p[0] for p in room['participants']]:
        await callback_query.answer("Ви не в цій грі!")
        return
    if user_id in room['votes']:
        await callback_query.answer("Ви вже проголосували!")
        return
    room['votes'][user_id] = voted_pid
    save_rooms()  # Зберігаємо rooms після голосування
    await callback_query.answer("Ваш голос враховано!")

# Підрахунок голосів
async def process_voting_results(token):
    room = rooms.get(token)
    if not room:
        return
    if not room['votes']:
        for pid, _ in room['participants']:
            await bot.send_message(pid, "Ніхто не проголосував. Шпигун переміг!")
        await end_game(token)
        return
    vote_counts = {}
    for voted_id in room['votes'].values():
        vote_counts[voted_id] = vote_counts.get(voted_id, 0) + 1
    max_votes = max(vote_counts.values())
    suspected = [pid for pid, count in vote_counts.items() if count == max_votes]
    if len(suspected) == 1 and suspected[0] == room['spy']:
        for pid, _ in room['participants']:
            await bot.send_message(pid, f"Ви знайшли шпигуна, але він ще може вгадати локацію!")
        if room['spy'] in [p[0] for p in room['participants']]:
            await bot.send_message(room['spy'], "Вгадайте локацію (30 секунд):")
            room['waiting_for_spy_guess'] = True
            await asyncio.sleep(20)
            for i in range(10, -1, -1):
                if token not in rooms:
                    return
                await bot.send_message(room['spy'], f"Час для вгадування: {i} секунд")
                await asyncio.sleep(1)
            if room.get('waiting_for_spy_guess'):
                await end_game(token)
    else:
        await end_game(token)

# Завершення гри
async def end_game(token):
    room = rooms.get(token)
    if not room:
        return
    spy_username = next(username for pid, username in room['participants'] if pid == room['spy'])
    result = (
        f"Гра завершена! Шпигун: {spy_username}\n"
        f"Локація: {room['location']}\n"
        f"Код кімнати: {token}\n"
        "Опції:\n"
        "/leave - Покинути кімнату\n"
    )
    owner_id = room['owner']
    for pid, _ in room['participants']:
        if pid == owner_id:
            await bot.send_message(pid, result + "/startgame - Почати нову гру")
        else:
            await bot.send_message(pid, result)
    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=room['participants'][0][0]))
    room['game_started'] = False
    room['spy'] = None
    room['location'] = None
    room['votes'] = {}
    room['messages'] = []
    room['waiting_for_spy_guess'] = False
    room['vote_in_progress'] = False
    room['banned_from_voting'] = set()
    save_rooms()  # Зберігаємо rooms після завершення гри

# Вгадування шпигуна
@dp.message(lambda message: any(room.get('waiting_for_spy_guess') and message.from_user.id == room['spy'] for room in rooms.values()))
async def handle_spy_guess(message: types.Message):
    if await check_maintenance(message):
        return
    user_id = message.from_user.id
    for token, room in list(rooms.items()):
        if room.get('waiting_for_spy_guess') and user_id == room['spy']:
            guess = message.text.strip().lower()
            actual_location = room['location'].lower()
            spy_username = next(username for pid, username in room['participants'] if pid == room['spy'])
            room['waiting_for_spy_guess'] = False
            if guess == actual_location:
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
            for pid, _ in room['participants']:
                await bot.send_message(pid, result)
            await end_game(token)
            break

# Чат у кімнаті
@dp.message()
async def handle_room_message(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    for token, room in rooms.items():
        if user_id in [p[0] for p in room['participants']]:
            if not room['game_started']:
                await message.reply("Гра ще не почалася. Чекайте на /startgame від власника.")
                return
            msg = f"{username}: {message.text}"
            room['messages'].append(msg)
            for pid, _ in room['participants']:
                if pid != user_id:
                    await bot.send_message(pid, msg)
            save_rooms()  # Зберігаємо rooms після повідомлення
            break
    else:
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create) або приєднайтесь (/join).")

# Налаштування webhook
async def on_startup(_):
    load_rooms()  # Завантажуємо rooms при старті
    webhook_host = os.getenv('RENDER_EXTERNAL_HOSTNAME')
    if not webhook_host:
        raise ValueError("RENDER_EXTERNAL_HOSTNAME is not set in environment variables")
    webhook_url = f"https://{webhook_host}/webhook"
    await bot.set_webhook(webhook_url, drop_pending_updates=True)
    logger.info(f"Webhook set to {webhook_url}")

async def on_shutdown(_):
    save_rooms()  # Зберігаємо rooms при зупинці
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.session.close()
    logger.info("Bot shutdown successfully")

# Налаштування сервера
app = web.Application()
webhook_path = "/webhook"
SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=webhook_path)
setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8443))
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=port)