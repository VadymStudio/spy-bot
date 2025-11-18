import logging
import asyncio
import random
import time
from datetime import datetime

from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from bot import bot
from config import (
    matchmaking_queue, 
    add_active_user, 
    rooms, 
    LOCATIONS, 
    GAME_DURATION_SECONDS, 
    BOT_IDS, 
    BOT_NAMES, 
    BOT_AVATARS
)
# Додано імпорт in_game_menu
from keyboards.keyboards import (
    in_queue_menu,
    in_lobby_menu,
    main_menu,
    in_game_menu, 
    get_early_vote_keyboard,
    get_voting_keyboard,
    get_locations_keyboard,
    get_in_lobby_keyboard
)
from utils.helpers import maintenance_blocked, generate_room_token
from utils.matchmaking import enqueue_user, dequeue_user
from utils.states import PlayerState
from database.crud import update_player_stats, get_or_create_player
from database.models import Room, UserState

router = Router()
logger = logging.getLogger(__name__)

# Глобальний словник для станів користувачів
user_states = {}

@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    user_id = message.from_user.id
    add_active_user(user_id)
    enqueue_user(user_id)
    await message.answer("🔍 Шукаємо гру...", reply_markup=in_queue_menu)


@router.message(F.text == "❌ Скасувати Пошук")
async def cancel_search(message: types.Message):
    user_id = message.from_user.id
    if user_id in matchmaking_queue:
        dequeue_user(user_id)
        await message.answer("❌ Пошук скасовано.", reply_markup=main_menu)
    else:
        await message.answer("ℹ️ Ви не в черзі.", reply_markup=main_menu)


# ------------------- Ручні кімнати -------------------

@router.message(F.text == "🚪 Створити Кімнату")
async def create_room_cmd(message: types.Message):
    logger.debug("Create room clicked by %s", message.from_user.id)
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return

    # Перевіряємо, чи гравець вже в іншій кімнаті
    for room in rooms.values():
        if message.from_user.id in room.players:
            await message.answer("❌ Ви вже знаходитесь в іншій кімнаті")
            return

    # Створюємо нову кімнату
    token = generate_room_token()
    room = Room(
        token=token,
        admin_id=message.from_user.id,
        players={message.from_user.id: message.from_user.full_name},
        player_roles={},
        player_votes={},
        early_votes=set(),
        game_started=False
    )
    rooms[token] = room
    
    # Оновлюємо стан користувача
    if message.from_user.id not in user_states:
        user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    # 1. Спочатку даємо меню для виходу (Reply Keyboard)
    await message.answer("✅ Лобі створено. Чекаємо гравців...", reply_markup=in_lobby_menu)

    # 2. Потім панель управління (Inline Keyboard)
    await message.answer(
        f"Кімната: <code>{token}</code>\n\n"
        "Запрошіть друзів або додайте ботів командою /add_bot",
        parse_mode="HTML",
        # Важливо: передаємо token, щоб кнопка спрацювала
        reply_markup=get_in_lobby_keyboard(is_admin=True, room_token=token)
    )

@router.message(Command("add_bot"))
async def cmd_add_bot(message: types.Message):
    """Додає бота до поточної кімнати (тільки для адміна)"""
    token, room = _find_user_room(message.from_user.id)
    if not room:
        await message.answer("❌ Ви не знаходитесь у кімнаті")
        return
    
    if message.from_user.id != room.admin_id:
        await message.answer("❌ Тільки адміністратор кімнати може додавати ботів")
        return
    
    if room.game_started:
        await message.answer("❌ Не можна додавати ботів після початку гри")
        return
    
    bot_id = None
    for bid in BOT_IDS:
        if bid not in room.players:
            bot_id = bid
            break
    
    if bot_id is None:
        await message.answer("❌ Досягнуто максимальну кількість ботів")
        return
    
    bot_num = abs(bot_id)
    bot_name = f"{BOT_AVATARS[bot_num % len(BOT_AVATARS)]} Бот-{bot_num}"
    room.players[bot_id] = bot_name
    
    for player_id in room.players:
        try:
            if player_id > 0:
                await bot.send_message(player_id, f"🤖 {bot_name} приєднався до кімнати")
        except:
            pass
    
    await message.answer(f"✅ {bot_name} додано до кімнати")


@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask_token(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    await message.answer("🔢 Введіть код кімнати:")
    await state.set_state(PlayerState.waiting_for_token)


@router.message(PlayerState.waiting_for_token)
async def join_room_process_token(message: types.Message, state: FSMContext):
    user = message.from_user
    token = (message.text or "").strip().upper()
    if token not in rooms:
        await message.answer("❌ Кімнату не знайдено. Повертаємось до меню.", reply_markup=main_menu)
        await state.clear()
        return
    room = rooms[token]
    if len(room.players) >= 6:
        await message.answer("❌ Кімната вже заповнена (6/6).")
        await state.clear()
        return
    
    if user.id in room.players:
        await message.answer("ℹ️ Ви вже в цій кімнаті.")
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        room.last_activity = int(datetime.now().timestamp())
        
        if user.id not in user_states:
            user_states[user.id] = UserState()
        user_states[user.id].current_room = token

        for pid in room.players:
            if pid == user.id:
                continue
            try:
                await bot.send_message(pid, f"👤 {user.full_name} приєднався до кімнати. 👥 {len(room.players)}/6")
            except Exception:
                pass
        
        await message.answer(
            f"✅ Ви приєднались до кімнати {token}. 👥 {len(room.players)}/6",
            reply_markup=in_lobby_menu,
        )
    await state.clear()


@router.message(F.text == "🚪 Покинути Лобі")
async def leave_lobby(message: types.Message, state: FSMContext):
    user = message.from_user
    target_token = None
    for t, r in rooms.items():
        if user.id in r.players and not r.game_started:
            target_token = t
            break
    if not target_token:
        await message.answer("ℹ️ Ви не в лобі жодної кімнати.", reply_markup=main_menu)
        try:
            await state.clear()
        except Exception:
            pass
        return
    room = rooms[target_token]
    username = room.players.get(user.id, "Гравець")
    
    if user.id in room.players:
        del room.players[user.id]
    if user.id in user_states:
        del user_states[user.id]

    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли. Кімнату закрито (порожня).", reply_markup=main_menu)
        return

    if user.id == room.admin_id:
        # Передаємо права першому живому гравцю, якщо є
        human_players = [p for p in room.players if p > 0]
        if human_players:
            room.admin_id = human_players[0]
            try:
                await bot.send_message(room.admin_id, "👑 Ви тепер адміністратор кімнати.")
                # Можна надіслати нову клавіатуру старту новому адміну
                await bot.send_message(
                    room.admin_id, 
                    "Панель управління:", 
                    reply_markup=get_in_lobby_keyboard(is_admin=True, room_token=target_token)
                )
            except Exception:
                pass
        else:
            del rooms[target_token] # Якщо лишились тільки боти - видаляємо
            return

    for pid in list(room.players.keys()):
        try:
            await bot.send_message(pid, f"🚪 {username} покинув лобі. 👥 {len(room.players)}/6")
        except Exception:
            pass
    await message.answer("✅ Ви покинули лобі.", reply_markup=main_menu)
    try:
        await state.clear()
    except Exception:
        pass


# ------------------- Старт гри -------------------

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players:
            return t, r
    return None, None

async def _game_timer(token: str):
    """Таймер гри: якщо час вийшов, перемагає шпигун."""
    try:
        await asyncio.sleep(GAME_DURATION_SECONDS)
        room = rooms.get(token)
        if room and room.game_started:
            await end_game(token, spy_won=True, reason="⏰ Час вичерпано! Шпигун переміг.")
    except asyncio.CancelledError:
        pass

@router.callback_query(F.data.startswith("start_game:"))
async def on_start_game_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    
    if not room:
        await callback.answer("Кімната не знайдена або гра вже закінчилася.", show_alert=True)
        return

    if callback.from_user.id != room.admin_id:
        await callback.answer("Тільки адміністратор кімнати може почати гру!", show_alert=True)
        return

    if len(room.players) < 3:
        await callback.answer("Потрібно мінімум 3 гравці!", show_alert=True)
        return

    # Початок гри
    await start_game(room)
    try:
        await callback.message.edit_text(f"🎮 Гра почалася! Гравців: {len(room.players)}")
    except Exception:
        pass

async def start_game(room: Room):
    """Починає гру в кімнаті"""
    players = list(room.players.keys())
    
    human_players = [p for p in players if p > 0]
    if not human_players: 
        human_players = players
    spy_id = random.choice(human_players)
    
    room.spy_id = spy_id
    room.location = random.choice(LOCATIONS)
    room.game_started = True
    
    for player_id in players:
        try:
            if player_id == spy_id:
                room.player_roles[player_id] = "spy"
                if player_id > 0:
                    await bot.send_message(
                        player_id,
                        "🕵️ *Ви ШПИГУН!* Вам потрібно з'ясувати локацію, не видаючи себе.",
                        parse_mode="Markdown",
                        reply_markup=in_game_menu
                    )
            else:
                room.player_roles[player_id] = "civilian"
                if player_id > 0:
                    await bot.send_message(
                        player_id,
                        f"👥 Ви ЦИВІЛЬНИЙ. Локація: *{room.location}*",
                        parse_mode="Markdown",
                        reply_markup=in_game_menu
                    )
                else:
                    room.player_roles[player_id] = "civilian"
        except Exception as e:
            logger.error(f"Помилка при надсиланні ролі гравцю {player_id}: {e}")
    
    room.end_time = int(time.time()) + GAME_DURATION_SECONDS
    room._timer_task = asyncio.create_task(_game_timer(room.token))
    
    for bot_id in BOT_IDS:
        if bot_id in room.players:
            asyncio.create_task(_bot_behavior(bot_id, room))

@router.message(F.text == "❓ Моя роль")
async def my_role(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    role = room.player_roles.get(message.from_user.id)
    if role == "spy":
        await message.answer("🕵️ Ти ШПИГУН.")
    else:
        await message.answer(f"👥 Ти ЦИВІЛЬНИЙ. Локація: <b>{room.location}</b>", parse_mode="HTML")


async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room or not room.game_started:
        return
    room.game_started = False
    task = getattr(room, "_timer_task", None)
    if task and not task.done():
        task.cancel()
    players = list(room.players.keys())
    for uid in players:
        try:
            await bot.send_message(uid, f"🏁 Гру завершено. {reason}", reply_markup=main_menu)
        except Exception:
            pass
    if grant_xp:
        for uid in players:
            if uid < 0: continue
            is_spy = (uid == room.spy_id)
            winner = (spy_won and is_spy) or ((not spy_won) and (not is_spy))
            try:
                level_before, current_xp, xp_needed = await update_player_stats(uid, is_spy=is_spy, is_winner=winner)
                player = await get_or_create_player(uid, "")
                level_after, _, _ = player.level_info
                if level_after > level_before:
                    await bot.send_message(uid, f"🎉 Вітаємо! Ви отримали {level_after} рівень!")
            except Exception as e:
                logger.error(f"Помилка XP {uid}: {e}")


# ------------------- Дострокове завершення -------------------

@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_request(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    room.early_votes = set()
    voters = list(room.players.keys())
    for uid in voters:
        try:
            await bot.send_message(uid, "🗳️ Достроково завершити гру?", reply_markup=get_early_vote_keyboard(token))
        except Exception:
            pass

    async def _finalize():
        await asyncio.sleep(30)
        if token not in rooms or not rooms[token].game_started:
            return
        votes_yes = len(room.early_votes)
        total = len(room.players)
        if votes_yes > total / 2:
            await end_game(token, spy_won=False, reason="🗳️ Гру завершено достроково більшістю.", grant_xp=False)
        else:
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, f"ℹ️ Дострокове завершення не прийнято ({votes_yes}/{total}).")
                except Exception:
                    pass

    room._early_vote_task = asyncio.create_task(_finalize())


@router.callback_query(F.data.startswith("early_vote_") )
async def early_vote_callback(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
        action = parts[0]
        token = parts[1]
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id not in room.players:
        await callback.answer()
        return
    if action == "early_vote_yes":
        room.early_votes.add(callback.from_user.id)
        await callback.answer("Ви проголосували: Так")
    else:
        await callback.answer("Ви проголосували: Ні")


# ------------------- Голосування проти гравця -------------------

@router.message(Command("vote"))
async def start_vote(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    room.player_votes = {}
    players = dict(room.players)
    for voter_id in players.keys():
        if voter_id < 0: continue 
        try:
            kb = get_voting_keyboard(token, players, voter_id)
            await bot.send_message(voter_id, "🗳️ Кого ви підозрюєте?", reply_markup=kb)
        except Exception:
            pass

    async def _finalize_vote():
        await asyncio.sleep(45)
        if token not in rooms or not rooms[token].game_started:
            return
        tally = {}
        for v in room.player_votes.values():
            tally[v] = tally.get(v, 0) + 1
        if not tally:
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, "ℹ️ Голосування не відбулось.")
                except Exception:
                    pass
            return
        max_votes = max(tally.values())
        top = [pid for pid, cnt in tally.items() if cnt == max_votes]
        if len(top) != 1:
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, "ℹ️ Нічия. Нікого не вигнали.")
                except Exception:
                    pass
            return
        target = top[0]
        for uid in list(room.players.keys()):
            try:
                await bot.send_message(uid, f"🚷 Вигнано гравця: {room.players.get(target, str(target))}")
            except Exception:
                pass
        if target == room.spy_id:
            await end_game(token, spy_won=False, reason="✅ Шпигуна викрито! Перемога цивільних.")
            return
        room.players.pop(target, None)
        room.player_roles.pop(target, None)
        room.player_votes = {}
        if len(room.players) < 3:
            await end_game(token, spy_won=True, reason="👥 Занадто мало гравців для продовження. Перемога шпигуна.")

    room._voting_task = asyncio.create_task(_finalize_vote())


@router.callback_query(F.data.startswith("vote:"))
async def vote_callback(callback: types.CallbackQuery):
    try:
        _, token, target_str = callback.data.split(":", 2)
        target_id = int(target_str)
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id not in room.players:
        await callback.answer()
        return
    room.player_votes[callback.from_user.id] = target_id
    await callback.answer("Ваш голос враховано")


# ------------------- Вгадування локації шпигуном -------------------

@router.message(Command("spy_guess"))
async def spy_guess(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    if message.from_user.id != room.spy_id:
        await message.answer("❌ Лише шпигун може робити здогад локації.")
        return
    kb = get_locations_keyboard(token, LOCATIONS, columns=3)
    await message.answer("📍 Оберіть локацію, яку ви вважаєте правильною:", reply_markup=kb)


@router.callback_query(F.data.startswith("guess:"))
async def spy_guess_callback(callback: types.CallbackQuery):
    try:
        _, token, location = callback.data.split(":", 2)
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id != room.spy_id:
        await callback.answer("Це діє лише для шпигуна")
        return
    if location == room.location:
        await callback.answer("✅ Вірно!")
        await end_game(token, spy_won=True, reason=f"🕵️ Шпигун вгадав локацію: {location}")
    else:
        await callback.answer("❌ Невірно")


async def _bot_behavior(bot_id: int, room: Room):
    """Поведінка бота під час гри"""
    if bot_id not in room.players or not room.game_started:
        return
    
    is_spy = (bot_id == room.spy_id)
    bot_name = room.players[bot_id]
    
    await asyncio.sleep(random.uniform(1, 3))
    
    if not is_spy and room.voting_started and not room.voting_ended:
        players = [p for p in room.players.keys() 
                  if p != bot_id and room.player_roles.get(p) != "civilian"]
        if players:
            target = random.choice(players)
            room.player_votes[bot_id] = target
    
    if is_spy and room.spy_guessing and not room.spy_guessed:
        await asyncio.sleep(random.uniform(2, 5))
        if random.random() < 0.3:
            room.spy_guess = random.choice(LOCATIONS)
            room.spy_guessed = True
            
            if room.spy_guess.lower() == room.location.lower():
                await end_game(room.token, spy_won=True, reason="✅ Шпигун вгадав локацію!")
            else:
                await end_game(room.token, spy_won=False, 
                             reason=f"❌ Шпигун не вгадав. Це було: {room.location}")