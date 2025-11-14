# bot.py
import asyncio
import logging
import os
import random
import string
import time
from collections import defaultdict
from typing import List, Tuple

import aiofiles
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

# -------------------- CONFIG --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # 0 = без адміна

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set in .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
logger = logging.getLogger("UkraineSpy")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# -------------------- DATA --------------------
rooms: dict[str, dict] = {}
banned_users: set[int] = set()

# список локацій (можна розширити)
LOCATIONS = [
    "Казино", "Школа", "Море", "Пляж", "Місяць", "Ліс", "Музей", "Озеро", "Магазин",
    "Атомна станція", "Водолад", "Храм", "Аквапарк", "Космічна станція", "Аптека",
    "Місто", "Готель", "Каналізація", "Цирк", "Парк", "Порт", "Острів", "Ракета",
    "Спа салон", "Лікарня", "Село", "Квартира",
]

# -------------------- HELPERS --------------------
def generate_token(length: int = 8) -> str:
    """Генерує випадковий токен (hex)."""
    return "".join(random.choices(string.hexdigits.lower(), k=length))

def save_rooms():
    """Асинхронно зберігає rooms у файл."""
    asyncio.create_task(_save_rooms_async())

async def _save_rooms_async():
    try:
        async with aiofiles.open("rooms.json", "w", encoding="utf-8") as f:
            await f.write(__import__("json").dumps(rooms, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.error(f"save_rooms error: {e}")

async def load_rooms():
    global rooms
    try:
        async with aiofiles.open("rooms.json", "r", encoding="utf-8") as f:
            data = await f.read()
            rooms = __import__("json").loads(data)
        logger.info("rooms.json loaded")
    except FileNotFoundError:
        rooms = {}
    except Exception as e:
        logger.error(f"load_rooms error: {e}")
        rooms = {}

# -------------------- KEYBOARDS --------------------
def build_locations_keyboard(token: str, locations: List[str], columns: int = 3) -> InlineKeyboardMarkup:
    """Клавіатура для шпигуна – безпечний callback_data."""
    random.shuffle(locations)
    kb = InlineKeyboardBuilder()
    for loc in locations:
        safe = loc.replace(" ", "---")
        kb.button(text=loc, callback_data=f"spy_guess:{token}:{safe}")
    kb.adjust(columns)
    return kb.as_markup()

def build_vote_keyboard(token: str, participants: List[Tuple[int, str, str]]) -> InlineKeyboardMarkup:
    """Клавіатура голосування за підозрюваного."""
    kb = InlineKeyboardBuilder()
    for pid, username, callsign in participants:
        text = f"{username} ({callsign})"
        kb.button(text=text, callback_data=f"vote:{token}:{pid}")
    kb.adjust(1)
    return kb.as_markup()

def early_vote_kb(token: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="За", callback_data=f"early_vote_for:{token}")
    kb.button(text="Проти", callback_data=f"early_vote_against:{token}")
    kb.adjust(2)
    return kb.as_markup()

# -------------------- GAME LOGIC --------------------
async def start_game(token: str):
    room = rooms[token]
    if len(room["participants"]) < 4:
        await bot.send_message(room["owner"], "Недостатньо гравців (потрібно ≥4).")
        return

    # вибір локації та шпигуна
    location = random.choice(LOCATIONS)
    spy_idx = random.randint(0, len(room["participants"]) - 1)
    spy_pid = room["participants"][spy_idx][0]

    room.update(
        {
            "location": location,
            "spy": spy_pid,
            "game_started": True,
            "vote_in_progress": False,
            "votes": {},
            "voters": set(),
            "votes_for": 0,
            "votes_against": 0,
            "last_activity": time.time(),
            "waiting_for_spy_guess": False,
            "spy_guess": None,
        }
    )
    save_rooms()

    # розсилка ролей
    for pid, username, callsign in room["participants"]:
        try:
            if pid == spy_pid:
                await bot.send_message(
                    pid,
                    f"<b>Ви — ШПИГУН!</b>\n"
                    f"Ваша мета — не видати себе.\n"
                    f"Локація <u>невідома</u> вам.",
                )
            else:
                await bot.send_message(
                    pid,
                    f"<b>Ви — цивільний.</b>\n"
                    f"Локація: <code>{location}</code>\n"
                    f"Шпигуна треба знайти!",
                )
        except Exception as e:
            logger.error(f"role msg to {pid}: {e}")

    # старт таймера (тест – 1 хв, звичайна – 7 хв)
    timer_seconds = 60 if room.get("is_test_game") else 420
    asyncio.create_task(game_timer(token, timer_seconds))

async def game_timer(token: str, seconds: int):
    await asyncio.sleep(seconds)
    room = rooms.get(token)
    if not room or not room.get("game_started"):
        return
    await start_voting(token)

async def start_voting(token: str):
    room = rooms.get(token)
    if not room or room.get("vote_in_progress"):
        return

    room.update(
        {
            "vote_in_progress": True,
            "votes": {},
            "last_activity": time.time(),
        }
    )
    save_rooms()

    callsigns = [c for _, _, c in room["participants"]]
    random.shuffle(callsigns)
    callsigns_str = f"Позивні в грі: {', '.join(callsigns)}"

    keyboard = build_vote_keyboard(token, room["participants"])

    if room.get("is_test_game"):
        admin_id = room["owner"]
        spy_id = room["spy"]
        # боти автоматично голосують за шпигуна
        for pid, _, _t in room["participants"]:
            if pid < 0:  # бот
                room["votes"][pid] = spy_id
        save_rooms()
        await bot.send_message(
            admin_id,
            f"Тестова гра: Боти проголосували.\n"
            f"Оберіть, хто шпигун (30 сек):\n\n{callsigns_str}",
            reply_markup=keyboard,
        )
    else:
        for pid, _, _ in room["participants"]:
            if pid > 0:
                try:
                    await bot.send_message(
                        pid,
                        f"Оберіть, хто шпигун (30 сек):\n\n{callsigns_str}",
                        reply_markup=keyboard,
                    )
                except Exception as e:
                    logger.error(f"vote kb to {pid}: {e}")

    asyncio.create_task(voting_timer_task(token))

async def voting_timer_task(token: str):
    await asyncio.sleep(30)
    await process_voting_results(token)

async def process_voting_results(token: str):
    room = rooms.get(token)
    if not room or not room.get("vote_in_progress"):
        return

    # підрахунок
    votes = room["votes"]
    if not votes:
        result = "Ніхто не проголосував → цивільні перемогли!"
        await end_game(token, result)
        return

    # хто отримав найбільше голосів
    tally = defaultdict(int)
    for voted_pid in votes.values():
        tally[voted_pid] += 1

    max_votes = max(tally.values())
    candidates = [pid for pid, cnt in tally.items() if cnt == max_votes]

    if len(candidates) != 1:
        result = "Нічия у голосуванні → цивільні перемогли!"
        await end_game(token, result)
        return

    accused_pid = candidates[0]
    spy_pid = room["spy"]
    spy_username = next((u for p, u, _ in room["participants"] if p == spy_pid), "Невідомо")
    spy_callsign = next((c for p, _, c in room["participants"] if p == spy_pid), "Невідомо")

    if accused_pid == spy_pid:
        # шпигуна знайшли → даємо шанс вгадати локацію
        room["waiting_for_spy_guess"] = True
        room["last_activity"] = time.time()
        save_rooms()

        kb = build_locations_keyboard(token, LOCATIONS.copy())
        try:
            await bot.send_message(
                spy_pid,
                f"<b>Вас викрили!</b>\n"
                f"У вас 30 секунд, щоб вгадати локацію:",
                reply_markup=kb,
            )
        except Exception as e:
            logger.error(f"spy guess kb to {spy_pid}: {e}")

        # таймер на вгадування
        task = asyncio.create_task(spy_guess_timer(token))
        room["spy_guess_timer_task"] = task
    else:
        # шпигуна НЕ знайшли
        result = (
            f"Гравці помилились! Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {room['location']}\n"
            f"Шпигун переміг!"
        )
        await end_game(token, result)

async def spy_guess_timer(token: str):
    await asyncio.sleep(30)
    room = rooms.get(token)
    if not room or not room.get("waiting_for_spy_guess"):
        return
    # час вийшов → цивільні перемогли
    spy_pid = room["spy"]
    spy_username = next((u for p, u, _ in room["participants"] if p == spy_pid), "Невідомо")
    spy_callsign = next((c for p, _, c in room["participants"] if p == spy_pid), "Невідомо")
    result = (
        f"Шпигун не встиг вгадати локацію.\n"
        f"Шпигун: {spy_username} ({spy_callsign})\n"
        f"Локація: {room['location']}\n"
        f"Гравці перемогли!"
    )
    await end_game(token, result)

# -------------------- END GAME --------------------
async def end_game(token: str, result_message: str = None):
    room = rooms.get(token)
    if not room:
        return

    if result_message is None:
        result_message = "Гра завершена без результату."

    # розсилка результату
    for pid, _, _ in room["participants"]:
        if pid > 0:
            try:
                await bot.send_message(pid, result_message)
            except Exception as e:
                logger.error(f"end msg to {pid}: {e}")

    # очищення
    delay = 120 if room.get("is_test_game") or token.startswith("auto_") else 5
    await asyncio.sleep(delay)

    if token in rooms:
        del rooms[token]
        save_rooms()
        logger.info(f"Room {token} deleted after game end.")

# -------------------- COMMANDS --------------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "<b>Ласкаво просимо до Ukraine Spy!</b>\n"
        "• /create – створити кімнату\n"
        "• /join <токен> – приєднатись\n"
        "• /testgame – тест з ботами (шпигун – бот)\n"
        "• /testgamespy – тест з ботами (ви – шпигун)\n"
        "• /leave – покинути кімнату\n"
        "• /ban <reply> – (адмін) забанити"
    )

@dp.message(Command("create"))
async def cmd_create(message: types.Message):
    user_id = message.from_user.id
    if any(r["owner"] == user_id for r in rooms.values()):
        await message.answer("Ви вже маєте кімнату.")
        return

    token = generate_token()
    rooms[token] = {
        "owner": user_id,
        "participants": [(user_id, message.from_user.full_name, random_callsign())],
        "game_started": False,
        "is_test_game": False,
        "last_activity": time.time(),
    }
    save_rooms()
    await message.answer(f"Кімната створена! Токен: <code>{token}</code>\n"
                         "Поділіться ним, щоб інші приєднались.")

@dp.message(Command("join"))
async def cmd_join(message: types.Message):
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Використання: /join <токен>")
        return
    token = parts[1].lower()
    room = rooms.get(token)
    if not room:
        await message.answer("Кімнату не знайдено.")
        return
    user_id = message.from_user.id
    if user_id in [p[0] for p in room["participants"]:
        await message.answer("Ви вже в кімнаті.")
        return
    if room.get("game_started"):
        await message.answer("Гра вже почалась, приєднатись неможливо.")
        return

    room["participants"].append((user_id, message.from_user.full_name, random_callsign()))
    room["last_activity"] = time.time()
    save_rooms()
    await message.answer(f"Ви приєднались до кімнати <code>{token}</code>.")
    await bot.send_message(room["owner"], f"{message.from_user.full_name} приєднався.")

@dp.message(Command("leave"))
async def cmd_leave(message: types.Message):
    user_id = message.from_user.id
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room["participants"]]:
            room["participants"] = [p for p in room["participants"] if p[0] != user_id]
            room["last_activity"] = time.time()
            save_rooms()
            await message.answer("Ви покинули кімнату.")
            if not room["participants"]:
                del rooms[token]
                save_rooms()
            return
    await message.answer("Ви не в кімнаті.")

def random_callsign() -> str:
    adjectives = ["Натурал", "Чорний", "Дикий", "Анна 15см", "Василь", "Степан", "Галина"]
    nouns = ["Борщ", "Борщ", "Борщ", "Борщ", "Борщ"]
    return f"{random.choice(adjectives)} {random.choice(nouns)}"

# -------------------- TEST COMMANDS --------------------
@dp.message(Command("testgame"))
async def cmd_testgame(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Тільки адмін.")
        return
    await _start_test(message, spy_is_bot=True)

@dp.message(Command("testgamespy"))
async def cmd_testgamespy(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Тільки адмін.")
        return
    await _start_test(message, spy_is_bot=False)

async def _start_test(message: types.Message, spy_is_bot: bool):
    token = f"test_{generate_token(6)}"
    admin_id = message.from_user.id
    admin_name = message.from_user.full_name

    participants = [(admin_id, admin_name, random_callsign())]
    bot_names = ["Василь", "Степан", "Галина"]
    for name in bot_names:
        participants.append((-abs(hash(name)), f"Бот {name}", random_callsign()))

    if not spy_is_bot:
        # адмін – шпигун
        spy_pid = admin_id
    else:
        # випадковий бот – шпигун
        spy_pid = random.choice([p[0] for p in participants if p[0] < 0])

    rooms[token] = {
        "owner": admin_id,
        "participants": participants,
        "location": random.choice(LOCATIONS),
        "spy": spy_pid,
        "game_started": True,
        "vote_in_progress": False,
        "votes": {},
        "voters": set(),
        "votes_for": 0,
        "votes_against": 0,
        "last_activity": time.time(),
        "is_test_game": True,
        "waiting_for_spy_guess": False,
        "spy_guess": None,
    }
    save_rooms()

    await message.answer(f"Тестова гра створена! Токен: <code>{token}</code>\n"
                         "Через 60 сек почнеться голосування.")
    asyncio.create_task(game_timer(token, 60))

# -------------------- EARLY VOTE --------------------
@dp.message(Command("early_vote"), F.text == "Dostrokove Golosuvannya")
async def cmd_early_vote(message: types.Message):
    user_id = message.from_user.id
    token = _find_user_room(user_id)
    if not token:
        await message.answer("Ви не в грі.")
        return
    room = rooms[token]
    if not room.get("game_started") or room.get("vote_in_progress"):
        await message.answer("Голосування вже йде або гра не почалась.")
        return

    room.update(
        {
            "vote_in_progress": True,
            "voters": {user_id},
            "votes_for": 1,
            "votes_against": 0,
            "last_activity": time.time(),
        }
    )
    save_rooms()

    await bot.send_message(
        room["owner"],
        "Запущено дострокове голосування. Голосувати за чи проти?",
        reply_markup=early_vote_kb(token),
    )
    # повідомляємо інших реальних гравців
    for pid, _, _ in room["participants"]:
        if pid > 0 and pid != user_id:
            try:
                await bot.send_message(
                    pid,
                    "Триває дострокове голосування. За чи проти?",
                    reply_markup=early_vote_kb(token),
                )
            except Exception as e:
                logger.error(f"early kb to {pid}: {e}")

    asyncio.create_task(early_vote_timer(token))

async def early_vote_timer(token: str):
    await asyncio.sleep(15)
    await finalize_early_vote(token)

async def finalize_early_vote(token: str):
    room = rooms.get(token)
    if not room or not room.get("vote_in_progress"):
        return
    total_real = sum(1 for p in room["participants"] if p[0] > 0)
    if room["votes_for"] > room["votes_against"] and room["votes_for"] * 2 > total_real:
        await start_voting(token)
    else:
        await bot.send_message(
            room["owner"],
            "Дострокове голосування не пройшло. Гра продовжується.",
        )
        room["vote_in_progress"] = False
        save_rooms()

# -------------------- CALLBACKS --------------------
@dp.callback_query(lambda c: c.data.startswith("early_vote_"))
async def cb_early_vote(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    if len(parts) != 3 or parts[0] not in ("early_vote_for", "early_vote_against"):
        await callback.answer("Помилка.")
        return
    token = parts[2]

    room = rooms.get(token)
    if not room or user_id not in [p[0] for p in room["participants"]]:
        await callback.answer("Ви не в грі.")
        return
    if user_id in room.get("voters", set()):
        await callback.answer("Ви вже проголосували.")
        return

    room["voters"].add(user_id)
    if parts[0] == "early_vote_for":
        room["votes_for"] += 1
        await callback.answer("За")
    else:
        room["votes_against"] += 1
        await callback.answer("Проти")
    save_rooms()

    # якщо всі реальні проголосували – завершуємо
    real = sum(1 for p in room["participants"] if p[0] > 0)
    if len(room["voters"]) == real:
        await finalize_early_vote(token)

@dp.callback_query(lambda c: c.data.startswith("vote:"))
async def cb_vote(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Помилка даних.")
        return
    token, voted_pid_str = parts[1], parts[2]
    try:
        voted_pid = int(voted_pid_str)
    except ValueError:
        await callback.answer("Помилка.")
        return

    user_id = callback.from_user.id
    room = rooms.get(token)
    if not room or user_id not in [p[0] for p in room["participants"]]:
        await callback.answer("Ви не в грі.")
        return
    if not room.get("vote_in_progress"):
        await callback.answer("Голосування завершено.")
        return

    room["votes"][user_id] = voted_pid
    room["last_activity"] = time.time()
    save_rooms()
    await callback.answer("Голос враховано!")

    # перевірка завершення
    total = len(room["participants"])
    if len(room["votes"]) == total or (room.get("is_test_game") and user_id == room["owner"]):
        await process_voting_results(token)

@dp.callback_query(lambda c: c.data.startswith("spy_guess:"))
async def cb_spy_guess(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Помилка.")
        return
    token, safe_loc = parts[1], parts[2]
    guessed = safe_loc.replace("---", " ")

    user_id = callback.from_user.id
    room = rooms.get(token)
    if not room or user_id != room.get("spy"):
        await callback.answer("Це не ваша гра.")
        return
    if not room.get("waiting_for_spy_guess"):
        await callback.answer("Час вийшов.")
        return

    room["waiting_for_spy_guess"] = False
    room["spy_guess"] = guessed
    room["last_activity"] = time.time()
    if room.get("spy_guess_timer_task"):
        room["spy_guess_timer_task"].cancel()
    save_rooms()

    await callback.answer(f"Ви вибрали: {guessed}")
    try:
        await callback.message.edit_text(f"Шпигун обрав: {guessed}")
    except Exception:
        pass

    spy_username = next((u for p, u, _ in room["participants"] if p == room["spy"]), "Невідомо")
    spy_callsign = next((c for p, _, c in room["participants"] if p == room["spy"]), "Невідомо")
    if guessed.lower() == room["location"].lower():
        result = (
            f"Шпигун вгадав локацію!\n"
            f"Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {room['location']}\n"
            f"Шпигун переміг!"
        )
    else:
        result = (
            f"Шпигун не вгадав.\n"
            f"Шпигун: {spy_username} ({spy_callsign})\n"
            f"Локація: {room['location']}\n"
            f"Гравці перемогли!"
        )
    await end_game(token, result)

# -------------------- ADMIN --------------------
@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    if not message.reply_to_message:
        await message.answer("Reply на користувача.")
        return
    target = message.reply_to_message.from_user
    banned_users.add(target.id)
    await message.answer(f"{target.full_name} забанений.")
    # виганяємо з усіх кімнат
    for token, room in list(rooms.items()):
        room["participants"] = [p for p in room["participants"] if p[0] != target.id]
        if not room["participants"]:
            del rooms[token]
    save_rooms()

async def check_ban_and_reply(obj):
    if obj.from_user.id in banned_users:
        await obj.answer("Ви забанені.")
        return True
    return False

# -------------------- UTILS --------------------
def _find_user_room(user_id: int) -> str | None:
    for token, room in rooms.items():
        if user_id in [p[0] for p in room["participants"]]:
            return token
    return None

# -------------------- MAIN --------------------
async def main():
    await load_rooms()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())