from aiogram.fsm.storage.base import StorageKey
from bot.utils import kb_main_menu, dp, maintenance_mode
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
from bot.utils import BASE_LOCATIONS, PACKS, CALLSIGNS, rooms, bot, logger, kb_in_game, kb_main_menu, dp, maintenance_mode, StorageKey
from bot.database import update_player_stats
from bot.rooms import save_rooms

# Решта коду game.py без змін
async def start_game_logic(room, token, admin_is_spy=False):
    if room.get('timer_task') and not room['timer_task'].done():
        try:
            room['timer_task'].cancel()
            await room['timer_task']
        except:
            pass
    if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
        try:
            room['spy_guess_timer_task'].cancel()
            await room['spy_guess_timer_task']
        except:
            pass
    available_callsigns = CALLSIGNS.copy()
    random.shuffle(available_callsigns)
    participant_list = [(pid, username, None) for pid, username, _ in room['participants']]
    room['participants'] = [(pid, username, available_callsigns[i]) for i, (pid, username, _) in enumerate(participant_list)]
    room['game_started'] = True
    locations = BASE_LOCATIONS.copy()
    if room.get('pack'):
        locations = PACKS[room['pack']]
    room['location'] = random.choice(locations)
    room['messages'] = []
    if room.get('is_test_game'):
        participant_ids = [p[0] for p in room['participants']]
        if admin_is_spy:
            room['spy'] = room['owner']
        else:
            bot_ids = [pid for pid in participant_ids if pid < 0]
            room['spy'] = random.choice(bot_ids) if bot_ids else room['owner']
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
    room['results_processed'] = False
    save_rooms()
    logger.info(f"Game started in room {token}, spy: {room['spy']}, location: {room['location']}")
    player_count = len(room['participants'])
    all_callsigns = [c for _, _, c in room['participants']]
    random.shuffle(all_callsigns)
    info_block = (
        f"Всього гравців: {player_count}\n"
        f"Позивні в грі: {', '.join(all_callsigns)}"
    )
    pack_info = f"\nВикористовується набір: {room['pack']}" if room.get('pack') else ""
    for pid, username, callsign in room['participants']:
        if pid > 0:
            try:
                await bot.send_message(pid, "Гра почалась!" + pack_info, reply_markup=kb_in_game)
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

async def run_timer(token):
    try:
        room = rooms.get(token)
        if not room:
            return
        wait_time = 60 if room.get('is_test_game') else 1140
        await asyncio.sleep(wait_time)
        if token not in rooms or not rooms[token]['game_started']:
            return
        room = rooms.get(token)
        if not room: return
        room['last_minute_chat'] = True
        if not room.get('is_test_game'):
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, "Залишилась 1 хвилина до кінця гри! Спілкуйтеся вільно.")
                    except Exception as e:
                        logger.error(f"Failed to send 1-minute warning to user {pid}: {e}")
            await asyncio.sleep(50)
        if token not in rooms or not rooms[token]['game_started']:
            return
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
        room = rooms.get(token)
        if not room: return
        room['game_started'] = False
        room['last_minute_chat'] = False
        room['last_activity'] = time.time()
        room['results_processed'] = False
        save_rooms()
        for pid, _, _ in room['participants']:
            if pid > 0:
                try:
                    await bot.send_message(pid, "Час вийшов! Голосуйте, хто шпигун.", reply_markup=kb_in_game)
                except Exception as e:
                    logger.error(f"Failed to send game end message to user {pid}: {e}")
        await show_voting_buttons(token)
    except asyncio.CancelledError:
        logger.info(f"Timer for room {token} was cancelled")
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
            return
        room['last_activity'] = time.time()
        all_callsigns = [c for _, _, c in room['participants']]
        random.shuffle(all_callsigns)
        callsigns_list_str = f"Позивні в грі: {', '.join(all_callsigns)}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{username} ({callsign})", callback_data=f"vote:{token}:{pid}")]
            for pid, username, callsign in room['participants']
        ])
        if room.get('is_test_game'):
            admin_id = room['owner']
            spy_id = room['spy']
            for pid, _, _ in room['participants']:
                if pid < 0:
                    room['votes'][pid] = spy_id
            save_rooms()
            logger.info(f"Test game {token}: Bots have voted for spy {spy_id}.")
            await bot.send_message(admin_id, f"Тестова гра: Боти проголосували.\nОберіть, хто шпигун (30 секунд):\n\n{callsigns_list_str}", reply_markup=keyboard)
        else:
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        await bot.send_message(pid, f"Оберіть, хто шпигун (30 секунд):\n\n{callsigns_list_str}", reply_markup=keyboard)
                    except Exception as e:
                        logger.error(f"Failed to send voting keyboard to user {pid}: {e}")
        asyncio.create_task(voting_timer_task(token))
    except Exception as e:
        logger.error(f"Show voting buttons error in room {token}: {e}", exc_info=True)
        await end_game(token, "Помилка при запуску голосування.")

async def voting_timer_task(token):
    await asyncio.sleep(20)
    room = rooms.get(token)
    if not room or room.get('results_processed'):
        return
    for i in range(10, 0, -1):
        if not room or room.get('results_processed'):
            return
        for pid, _, _ in room['participants']:
            if pid > 0:
                try: await bot.send_message(pid, f"Час для голосування: {i} секунд")
                except Exception: pass
        await asyncio.sleep(1)
    room = rooms.get(token)
    if room and not room.get('results_processed'):
        logger.info(f"Voting timer expired for room {token}. Processing results...")
        await process_voting_results(token)

async def process_voting_results(token):
    try:
        room = rooms.get(token)
        if not room:
            return
        if room.get('results_processed'):
            return
        room['results_processed'] = True
        room['last_activity'] = time.time()
        save_rooms()
        if not room['votes']:
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
        spy_username = next((username for pid, username, _ in room['participants'] if pid == room['spy']), "Невідомо")
        spy_callsign = next((callsign for pid, _, callsign in room['participants'] if pid == room['spy']), "Невідомо")
        if len(suspected) == 1 and suspected[0] == room['spy']:
            room['waiting_for_spy_guess'] = True
            room['spy_guess'] = None
            room['last_activity'] = time.time()
            locations_for_spy = BASE_LOCATIONS.copy()
            random.shuffle(locations_for_spy)
            reply_markup = build_locations_keyboard(token, locations_for_spy, columns=3)
            save_rooms()
            for pid, _, _ in room['participants']:
                if pid > 0:
                    try:
                        if pid == room['spy']:
                            await bot.send_message(pid, "Гравці проголосували за вас! Вгадайте локацію (30 секунд):", reply_markup=reply_markup)
                        else:
                            await bot.send_message(pid, f"Гравці вважають, що шпигун — {spy_username} ({spy_callsign}). Чекаємо, чи вгадає він локацію (30 секунд).")
                    except Exception as e:
                        logger.error(f"Failed to send spy guess prompt to user {pid}: {e}")
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

def build_locations_keyboard(token: str, locations: list, columns: int = 3) -> InlineKeyboardMarkup:
    inline_keyboard = []
    row = []
    for loc in locations:
        safe_loc = loc.replace(' ', '---')
        button = InlineKeyboardButton(text=loc, callback_data=f"spy_guess:{token}:{safe_loc}")
        row.append(button)
        if len(row) == columns:
            inline_keyboard.append(row)
            row = []
    if row:
        inline_keyboard.append(row)
    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

async def spy_guess_timer_task(token):
    await asyncio.sleep(20)
    room = rooms.get(token)
    if not room or not room.get('waiting_for_spy_guess'):
        return
    for i in range(10, 0, -1):
        if not room or not room.get('waiting_for_spy_guess'):
            return
        for pid, _, _ in room['participants']:
            if pid > 0:
                try: await bot.send_message(pid, f"Час для вгадування локації: {i} секунд")
                except Exception: pass
        await asyncio.sleep(1)
    room = rooms.get(token)
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

async def end_game(token, result_message: str = None):
    try:
        room = rooms.get(token)
        if not room:
            return
        if room.get('timer_task') and not room['timer_task'].done():
            room['timer_task'].cancel()
        if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
            room['spy_guess_timer_task'].cancel()
        if not room.get('is_test_game'):
            spy_id = room.get('spy')
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
                except Exception as e:
                    logger.error(f"Failed to send end game message to {pid}: {e}")
        room['game_started'] = False
        room['spy'] = None
        room['votes'] = {}
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
        room['results_processed'] = False
        if room.get('is_test_game') or str(token).startswith("auto_"):
            await asyncio.sleep(120)
            if token in rooms: del rooms[token]
            logger.info(f"Auto/Test room {token} deleted after game end.")
            save_rooms()
        else:
            save_rooms()
            logger.info(f"Private game ended in room {token}. Room reset, logs preserved for 1 hour.")
    except Exception as e:
        logger.error(f"End game error in {token}: {e}", exc_info=True)
        fallback_message = (
            f"Гра завершена з помилкою!\n"
            f"Шпигун: Невідомо\n"
            f"Локація: Невідомо"
        )
        for pid, _, _ in room.get('participants', []):
            if pid > 0:
                try:
                    await bot.send_message(pid, fallback_message)
                except:
                    pass

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
        'results_processed': False,
        'pack': None
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
            timed_out_users = [p for p in matchmaking_queue if current_time - p[2] > 120]
            matchmaking_queue = [p for p in matchmaking_queue if current_time - p[2] <= 120]
            if timed_out_users:
                logger.info(f"Timing out {len(timed_out_users)} users from queue.")
                for pid, username, _ in timed_out_users:
                    try:
                        await bot.send_message(pid, "Час пошуку вичерпано. Спробуйте ще раз пізніше.", reply_markup=kb_main_menu)
                        key = StorageKey(bot_id=bot.id, chat_id=pid, user_id=pid)
                        await dp.storage.set_state(key=key, state=None)
                    except Exception as e:
                        logger.warning(f"Failed to notify user {pid} about timeout: {e}")
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