import time
import uuid
import asyncio
from collections import deque
from aiogram import types
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from bot.utils import user_message_times
from bot.game import end_game, show_voting_buttons, process_voting_results
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
from bot.utils import dp, kb_main_menu, kb_in_queue, kb_in_lobby, kb_in_game, bot, rooms, matchmaking_queue, ADMIN_IDS, maintenance_mode, logger
from bot.game import start_game_logic, notify_queue_updates, early_vote_timer, finalize_early_vote, process_vote, process_spy_guess_callback
from bot.stats import show_stats
from bot.admin import check_maintenance, check_ban_and_reply, start_maint_timer  # Якщо є інші, додай
from bot.rooms import save_rooms

# Решта коду handlers.py без змін (встав свій оригінал після імпортів)
class PlayerState(StatesGroup):
    in_queue = State()
    waiting_for_token = State()
    set_pack = State()
class AdminState(StatesGroup):
    waiting_for_db_file = State()

@dp.message(Command("start"))
@dp.message(F.text == "❓ Допомога")
async def send_welcome(message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    await state.clear()
    menu_text = (
        "Привіт! Це бот для гри 'Шпигун'.\n\n"
        "Обери дію на клавіатурі внизу:"
    )
    await message.reply(menu_text, reply_markup=kb_main_menu)
    if message.from_user.id in ADMIN_IDS:
        await message.answer(
            "Вітаю, Адмін. Тобі доступні спец. команди (тільки через слеш-меню):\n"
            "/maintenance_on, /maintenance_off, /maint_timer, /cancel_maint, "
            "/check_webhook, /testgame, /testgamespy, /whois, /getdb, /updatedb, /getlog, "
            "/recentgames, /ban, /unban, /shop, /purchases, /refund"
        )

@dp.message(Command("find_match"))
@dp.message(F.text == "🎮 Знайти Гру")
async def find_match(message, state: FSMContext):
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
async def cancel_match(message, state: FSMContext):
    global matchmaking_queue
    user_id = message.from_user.id
    matchmaking_queue = [p for p in matchmaking_queue if p[0] != user_id]
    await state.clear()
    await message.reply("Пошук скасовано.", reply_markup=kb_main_menu)
    await notify_queue_updates()

@dp.message(Command("create"))
@dp.message(F.text == "🚪 Створити Кімнату")
async def create_room(message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошуку: /cancel_match")
        return
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
        'results_processed': False,
        'pack': None
    }
    save_rooms()
    logger.info(f"Room created: {room_token}")
    await message.reply(
        f"Кімнату створено! Токен: `{room_token}`\n"
        "Поділіться токеном з іншими. Ви власник, запустіть гру командою /startgame.",
        parse_mode="Markdown", reply_markup=kb_in_lobby
    )

@dp.message(Command("join"))
@dp.message(F.text == "🤝 Приєднатися")
async def join_room(message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошуку: /cancel_match")
        return
    user_id = message.from_user.id
    for room in rooms.values():
        if user_id in [p[0] for p in room['participants']]:
            await message.reply("Ви вже в кімнаті! Спочатку покиньте її (/leave).")
            return
    await message.answer("Введіть токен кімнати:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(PlayerState.waiting_for_token)
    logger.info(f"User {user_id} prompted for room token")

@dp.message(StateFilter(PlayerState.waiting_for_token))
async def process_token(message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        await state.clear()
        return
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
        else:
            await message.reply("Ви вже в цій кімнаті!", reply_markup=kb_in_lobby)
    else:
        await message.reply(f"Кімнати з токеном {token} не існує. Спробуйте ще раз.", reply_markup=kb_main_menu)
    await state.clear()

@dp.message(Command("leave"))
@dp.message(F.text.startswith("🚪 Покинути"))
async def leave_room(message, state: FSMContext):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    current_state = await state.get_state()
    if current_state == PlayerState.in_queue:
        return await cancel_match(message, state)
    logger.info(f"User {user_id} sent /leave")
    room_found = False
    for token, room in list(rooms.items()):
        if user_id in [p[0] for p in room['participants']]:
            room_found = True
            room['participants'] = [p for p in room['participants'] if p[0] != user_id]
            room['last_activity'] = time.time()
            logger.info(f"User {user_id} left room {token}")
            await message.reply(f"Ви покинули кімнату {token}.", reply_markup=kb_main_menu)
            if room.get('game_started'):
                if user_id == room.get('spy'):
                    logger.info(f"Spy left room {token}. Ending game.")
                    await end_game(token, "Шпигун втік! Гра завершена.")
                    return
                real_players_left = sum(1 for p in room['participants'] if p[0] > 0)
                if real_players_left < 2:
                    logger.info(f"Only {real_players_left} players left in {token}. Ending game.")
                    await end_game(token, "Залишилось занадто мало гравців. Гра завершена.")
                    return
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
        except: pass

@dp.message(Command("early_vote"))
@dp.message(F.text == "🗳️ Достр. Голосування")
async def early_vote(message: types.Message):
    if await check_ban_and_reply(message): return
    if await check_maintenance(message):
        return
    user_id = message.from_user.id
    current_state = await dp.storage.get_state(StorageKey(bot_id=bot.id, chat_id=user_id, user_id=user_id))
    if current_state == PlayerState.in_queue:
        await message.reply("Ви у черзі! Спочатку скасуйте пошуку: /cancel_match")
        return
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
                await bot.send_message(user_id, "Ви ініціювали дострокове голосування. Ви не зможете зробити це знову в цій грі.")
            except Exception as e:
                logger.error(f"Failed to send early vote notice to user {user_id}: {e}")
            save_rooms()
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ За дострокове завершення", callback_data=f"early_vote_for:{token}")],
                [InlineKeyboardButton(text="❌ Продовжити гру", callback_data=f"early_vote_against:{token}")]
            ])
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, "Голосування за дострокове завершення гри! Час: 15 секунд.", reply_markup=keyboard)
                    except Exception: pass
            asyncio.create_task(early_vote_timer(token))
            return
    await message.reply("Ви не перебуваєте в жодній кімнаті.")

async def early_vote_timer(token):
    await asyncio.sleep(15)
    room = rooms.get(token)
    if not room or not room.get('vote_in_progress'):
        return
    await finalize_early_vote(token)

async def finalize_early_vote(token):
    room = rooms.get(token)
    if not room: return
    room['vote_in_progress'] = False
    votes_for = room['votes_for']
    votes_against = room['votes_against']
    room['last_activity'] = time.time()
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
                except Exception: pass
        await show_voting_buttons(token)
    else:
        for pid, _, _ in room['participants']:
            if pid > 0:
                try:
                    await bot.send_message(pid, f"Голосування провалено. За: {votes_for}, Проти: {votes_against}")
                except Exception: pass
    save_rooms()

@dp.callback_query(F.data.startswith("early_vote_"))
async def early_vote_callback(callback: CallbackQuery):
    if await check_ban_and_reply(callback): return
    user_id = callback.from_user.id
    data_parts = callback.data.split(':')
    if len(data_parts) < 2:
        await callback.answer("Помилка даних!")
        return
    token = data_parts[-1]
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
    real_players_count = sum(1 for p in room['participants'] if p[0] > 0)
    if len(room['voters']) == real_players_count:
        await finalize_early_vote(token)

@dp.callback_query(F.data.startswith('vote:'))
async def process_vote(callback_query: CallbackQuery):
    if await check_ban_and_reply(callback_query): return
    logger.info(f"Vote callback received: {callback_query.data}")
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
        if room.get('game_started') == False and room.get('waiting_for_spy_guess') == False:
            await callback_query.answer("Голосування завершено!")
            return
        room['votes'][user_id] = voted_pid
        room['last_activity'] = time.time()
        save_rooms()
        await callback_query.answer("Ваш голос враховано!")
        voted_count = len(room['votes'])
        total_players = len(room['participants'])
        is_finished = False
        if room.get('is_test_game', False):
            real_voters = {k:v for k,v in room['votes'].items() if k > 0}
            if room['owner'] in real_voters:
                is_finished = True
        else:
            if voted_count == total_players:
                is_finished = True
        if is_finished:
            logger.info(f"Voting finished in room {token}. Processing results...")
            await process_voting_results(token)
    except Exception as e:
        logger.error(f"Process vote error: {e}", exc_info=True)
        await callback_query.answer("Помилка!")

@dp.callback_query(F.data.startswith('spy_guess:'))
async def process_spy_guess_callback(callback_query: CallbackQuery):
    if await check_ban_and_reply(callback_query): return
    try:
        user_id = callback_query.from_user.id
        data_parts = callback_query.data.split(':')
        if len(data_parts) != 3 or data_parts[0] != 'spy_guess':
            await callback_query.answer("Помилка! Неправильний формат кнопки.")
            return
        token = data_parts[1]
        guessed_location_safe = data_parts[2]
        guessed_location = guessed_location_safe.replace('---', ' ')
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
        room['waiting_for_spy_guess'] = False
        room['spy_guess'] = guessed_location.strip()
        room['last_activity'] = time.time()
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
        save_rooms()
        await callback_query.answer(f"Ваш вибір: {guessed_location}")
        try:
            await callback_query.message.edit_text(f"Шпигун зробив свій вибір: {guessed_location}")
        except Exception as e:
            logger.info(f"Couldn't edit spy guess message: {e}")
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
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        username_clean = username.lstrip('@')
        for token, room in rooms.items():
            if user_id in [p[0] for p in room['participants']]:
                if not message.text:
                    try:
                        await message.reply("Ніхто це не побачив( \n(Підтримуються тільки текстові повідомлення)")
                    except Exception: pass
                    return
                if len(message.text) > 120:
                    await bot.send_message(user_id, f"Обмеження на повідомлення: 120 символів. Ваше повідомлення не відправлено.")
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