from aiogram.filters import Command, F
from bot.utils import ADMIN_IDS
import os
import uuid
import asyncio
import time
import aiosqlite
from aiogram import types
from bot.utils import kb_in_game, DB_PATH  # Якщо DB_PATH в utils, або в database
from bot.game import start_game_logic, end_game  # Якщо end_game використовується
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
from bot.utils import bot, logger, ADMIN_IDS, maintenance_mode, rooms, kb_main_menu, time  # Якщо time вже імпортовано
from bot.database import get_purchases, refund_purchase, get_player_stats, DB_PATH
from bot.rooms import save_rooms
from bot.handlers import dp, check_maintenance, check_ban_and_reply
from bot.utils import parse_ban_time, get_user_from_event

# Решта коду admin.py без змін
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
        await asyncio.sleep(300)
        await send_maint_warning("Повторне попередження: Технічні роботи почнуться через **5 хвилин**.")
        await asyncio.sleep(240)
        await send_maint_warning("Останнє попередження! Технічні роботи почнуться через **1 хвилину**.")
        await asyncio.sleep(60)
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
        except Exception as e:
            await message.reply(f"Помилка при скиданні стану: {e}")

@dp.message(Command("testgame"))
async def test_game(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    if await check_maintenance(message):
        return
    current_state = await state.get_state()
    if current_state == "PlayerState:in_queue":
        await message.reply("Ви у черзі! Спочатку скасуйте пошуку: /cancel_match")
        return
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"Admin {user_id} starting test game (BOT IS SPY)")
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    room_token = f"test_{uuid.uuid4().hex[:4]}"
    participants = [(user_id, username, None), (-1, "Бот Василь", None), (-2, "Бот Степан", None), (-3, "Бот Галина", None)]
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
    if current_state == "PlayerState:in_queue":
        await message.reply("Ви у черзі! Спочатку скасуйте пошуку: /cancel_match")
        return
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    logger.info(f"Admin {user_id} starting test game (ADMIN IS SPY)")
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    room_token = f"test_spy_{uuid.uuid4().hex[:4]}"
    participants = [(user_id, username, None), (-1, "Бот Василь", None), (-2, "Бот Степан", None), (-3, "Бот Галина", None)]
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
    if not user_room or not user_room.get('game_started'):
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

@dp.message(Command("updatedb"))
async def request_db_update(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.reply("Переводжу в режим оновлення бази. Будь ласка, надішліть файл `players.db`.\n"
                        "УВАГА: Поточна база на сервері буде **ПОВНІСТЮ ЗАМІНЕНА**.\n"
                        "Для скасування просто нічого не надсилайте або напишіть /cancel.")
    await state.set_state("AdminState:waiting_for_db_file")

@dp.message(F.document, StateFilter("AdminState:waiting_for_db_file"))
async def process_db_upload(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    if message.document.file_name != 'players.db':
        await message.reply(f"❌ Помилка. Очікувався файл `players.db`, але отримано `{message.document.file_name}`.\nОновлення скасовано.")
        await state.clear()
        return
    try:
        await message.reply(f"✅ Отримав `{message.document.file_name}`. Починаю завантаження на сервер...")
        file_info = await bot.get_file(message.document.file_id)
        await bot.download_file(file_info.file_path, DB_PATH)
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

@dp.message(Command("purchases"))
async def show_purchases(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    purchases = await get_purchases()
    if not purchases:
        await message.reply("Немає покупок.")
        return
    text = "Покупки:\n"
    for p in purchases:
        text += f"ID: {p[0]}, User: {p[1]}, Item: {p[2]}, Stars: {p[3]}, Time: {datetime.fromtimestamp(p[4])}\n"
    await message.reply(text)

@dp.message(Command("refund"))
async def refund_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("/refund <ID>")
        return
    try:
        purchase_id = int(args[1])
    except ValueError:
        await message.reply("ID має бути числом.")
        return
    result = await refund_purchase(purchase_id)
    if not result:
        await message.reply("Покупку не знайдено.")
        return
    user_id, item_code = result
    await message.reply(f"Refund для ID {purchase_id} ({item_code}). Ефект скасовано. Зірки 'повернено' внутрішньо.")
    try:
        await bot.send_message(user_id, f"Ваша покупка {item_code} refunded (компенсація в магазині).")
    except:
        pass