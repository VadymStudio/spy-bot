import logging
import asyncio
import random
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import uuid

# Завантажуємо змінні з .env
load_dotenv()

# Налаштування логування для дебагу
logging.basicConfig(level=logging.INFO)

# Ініціалізація бота
API_TOKEN = os.getenv('API_TOKEN', 'ВАШ_ТОКЕН_ВІД_BOTFATHER')
ADMIN_ID = int(os.getenv('ADMIN_ID', '123456789'))
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Глобальний стан техобслуговування та активні користувачі
maintenance_mode = False
active_users = set()

# Масив локацій для гри
LOCATIONS = [
    "Аеропорт", "Банк", "Пляж", "Казино", "Цирк", "Школа", "Лікарня",
    "Готель", "Музей", "Ресторан", "Театр", "Парк", "Космічна станція"
]

# Словник для зберігання кімнат
rooms = {}

# Стани для FSM
class RoomStates(StatesGroup):
    waiting_for_token = State()
    waiting_for_spy_guess = State()

# Перевірка, чи бот на техобслуговуванні
async def check_maintenance(message: types.Message):
    if maintenance_mode and message.from_user.id != ADMIN_ID:
        await message.reply("Бот на технічному обслуговуванні. Зачекайте, будь ласка.")
        return True
    return False

# Функція завершення гри
async def end_game(token, message):
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
        f"{ '/startgame - Почати нову гру' if message.from_user.id == room['owner'] else '' }"
    )
    for pid, _ in room['participants']:
        await bot.send_message(pid, result)
    # Очищення стану гри
    room['game_started'] = False
    room['spy'] = None
    room['location'] = None
    room['votes'] = {}
    room['messages'] = []
    room['waiting_for_spy_guess'] = False

# Команда /start
@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Команди:\n"
        "/create - Створити нову кімнату\n"
        "/join - Приєднатися до кімнати за токеном\n"
        "/startgame - Запустити гру (тільки власник)\n"
        "/leave - Покинути кімнату"
    )
    await message.reply(menu_text)
    # Додаткове повідомлення для адміна
    if message.from_user.id == ADMIN_ID:
        await message.reply(
            "Команди адміністратора:\n"
            "/maintenance_on - Увімкнути технічне обслуговування\n"
            "/maintenance_off - Вимкнути технічне обслуговування"
        )

# Команда /create
@dp.message_handler(commands=['create'])
async def create_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    room_token = str(uuid.uuid4())[:8]
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
        'waiting_for_spy_guess': False
    }

    await message.reply(
        f"Кімнату створено! Токен: `{room_token}`\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame."
    )

# Команда /join
@dp.message_handler(commands=['join'])
async def join_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    await RoomStates.waiting_for_token.set()
    await message.reply("Введіть токен кімнати:")

# Обробка введеного токена
@dp.message_handler(state=RoomStates.waiting_for_token)
async def process_token(message: types.Message, state: FSMContext):
    if await check_maintenance(message):
        await state.finish()
        return
    active_users.add(message.from_user.id)
    token = message.text.strip()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    if token in rooms:
        if rooms[token]['game_started']:
            await message.reply("Гра в цій кімнаті вже почалася, ви не можете приєднатися.")
        elif user_id not in [p[0] for p in rooms[token]['participants']]:
            rooms[token]['participants'].append((user_id, username))
            # Сповіщення про приєднання
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
        await message.reply("Кімнати з таким токеном не існує. Спробуйте ще раз.")
    await state.finish()

# Команда /leave
@dp.message_handler(commands=['leave'])
async def leave_room(message: types.Message):
    if await check_maintenance(message):
        return
    active_users.add(message.from_user.id)
    user_id = message.from_user.id
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            await message.reply(f"Ви покинули кімнату `{token}`.")
            # Сповіщення про вихід
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
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Команда /startgame
@dp.message_handler(commands=['startgame'])
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
            for pid, username in room['participants']:
                if pid == room['spy']:
                    await bot.send_message(pid, "Ви ШПИГУН! Спробуйте вгадати локацію, не видавши себе.")
                else:
                    await bot.send_message(pid, f"Локація: {room['location']}\nОдин із гравців — шпигун!")
            for pid, _ in room['participants']:
                await bot.send_message(pid, "Гра почалася! Обговорюйте і шукайте шпигуна. Час: 5 хвилин.")
            await run_timer(token)
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

# Таймер гри (5 хвилин)
async def run_timer(token):
    room = rooms.get(token)
    if not room:
        return
    await asyncio.sleep(290)  # Чекаємо 4:50
    for i in range(10, -1, -1):
        if token not in rooms:
            return
        for pid, _ in room['participants']:
            await bot.send_message(pid, f"До кінця гри: {i} секунд")
        await asyncio.sleep(1)
    room['game_started'] = False
    for pid, _ in room['participants']:
        await bot.send_message(pid, "Час вийшов! Голосуйте, хто шпигун.")
    await show_voting_buttons(token)

# Голосування (30 секунд)
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
@dp.callback_query_handler(lambda c: c.data.startswith('vote_'))
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
    await callback_query.answer("Ваш голос враховано!")

# Підрахунок результатів голосування
async def process_voting_results(token):
    room = rooms.get(token)
    if not room:
        return
    if not room['votes']:
        for pid, _ in room['participants']:
            await bot.send_message(pid, "Ніхто не проголосував. Шпигун переміг!")
        await end_game(token, message=None)  # Викликаємо end_game без message
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
                await end_game(token, message=None)
    else:
        await end_game(token, message=None)

# Обробка вгадування локації шпигуном
@dp.message_handler(lambda message: any(room.get('waiting_for_spy_guess') and message.from_user.id == room['spy'] for room in rooms.values()))
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
            await end_game(token, message)
            break

# Чат у кімнаті
@dp.message_handler(content_types=['text'])
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
            break
    else:
        await message.reply("Ви не перебуваєте в жодній кімнаті. Створіть (/create) або приєднайтесь (/join).")

# Команда для увімкнення техобслуговування
@dp.message_handler(commands=['maintenance_on'])
async def maintenance_on(message: types.Message):
    global maintenance_mode, rooms
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ви не адміністратор!")
        return
    maintenance_mode = True
    for user_id in active_users:
        await bot.send_message(user_id, "Увага! Бот переходить на технічне обслуговування. Усі ігри завершено.")
    rooms.clear()
    active_users.clear()
    await message.reply("Технічне обслуговування увімкнено.")

# Команда для вимкнення техобслуговування
@dp.message_handler(commands=['maintenance_off'])
async def maintenance_off(message: types.Message):
    global maintenance_mode
    if message.from_user.id != ADMIN_ID:
        await message.reply("Ви не адміністратор!")
        return
    maintenance_mode = False
    for user_id in active_users:
        await bot.send_message(user_id, "Технічне обслуговування завершено! Бот знову доступний для гри.")
    await message.reply("Технічне обслуговування вимкнено.")

# Запуск бота
import asyncio

async def on_startup(_):
    # Очищення попередніх оновлень
    await bot.delete_webhook()
    await asyncio.sleep(2)  # Затримка для завершення попередніх сесій
    logging.info("Starting polling...")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)