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

user_states = {}

# --- ДОПОМІЖНА ФУНКЦІЯ ВХОДУ ---
async def _process_join_room(message: types.Message, token: str, state: FSMContext):
    user = message.from_user
    token = token.upper().strip()
    
    if token not in rooms:
        if len(token) in [4, 5] and token.isalnum():
            await message.answer(f"❌ Кімнату <code>{token}</code> не знайдено.", parse_mode="HTML")
        else:
            await message.answer("❌ Невірний код або кімнати не існує.", reply_markup=main_menu)
        return

    room = rooms[token]
    
    if len(room.players) >= 6:
        await message.answer("❌ Кімната заповнена.")
        return
    
    if room.game_started:
        await message.answer("❌ Гра вже йде.")
        return

    if user.id in room.players:
        await message.answer("ℹ️ Ви вже тут.", reply_markup=in_lobby_menu)
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        room.last_activity = int(datetime.now().timestamp())
        
        if user.id not in user_states: user_states[user.id] = UserState()
        user_states[user.id].current_room = token

        for pid in room.players:
            if pid == user.id: continue
            try: await bot.send_message(pid, f"👤 {user.full_name} зайшов! ({len(room.players)}/6)")
            except: pass
        
        await message.answer(f"✅ Ви в кімнаті <code>{token}</code>", parse_mode="HTML", reply_markup=in_lobby_menu)
        await message.answer("Меню:", reply_markup=get_in_lobby_keyboard(False, token))

    await state.clear()

# --- BASIC HANDLERS ---

@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    add_active_user(message.from_user.id)
    enqueue_user(message.from_user.id)
    await message.answer("🔍 Шукаємо...", reply_markup=in_queue_menu)

@router.message(F.text == "❌ Скасувати Пошук")
async def cancel_search(message: types.Message):
    if message.from_user.id in matchmaking_queue:
        dequeue_user(message.from_user.id)
        await message.answer("❌ Скасовано.", reply_markup=main_menu)
    else:
        await message.answer("ℹ️ Ви не в черзі.", reply_markup=main_menu)

@router.message(F.text == "🚪 Створити Кімнату")
async def create_room_cmd(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    for r in rooms.values():
        if message.from_user.id in r.players:
            await message.answer("❌ Вийдіть з поточної кімнати.")
            return

    token = generate_room_token()
    room = Room(token=token, admin_id=message.from_user.id, players={message.from_user.id: message.from_user.full_name}, player_roles={}, player_votes={}, early_votes=set())
    rooms[token] = room
    
    if message.from_user.id not in user_states: user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    await message.answer("✅ Лобі створено.", reply_markup=in_lobby_menu)
    await message.answer(f"Код: <code>{token}</code>", parse_mode="HTML", reply_markup=get_in_lobby_keyboard(True, token))

@router.callback_query(F.data.startswith("add_bot_btn:"))
async def on_add_bot_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room or callback.from_user.id != room.admin_id or room.game_started: return
    
    bot_id = None
    for bid in BOT_IDS:
        if bid not in room.players:
            bot_id = bid
            break
    if not bot_id:
        await callback.answer("Максимум ботів!", show_alert=True)
        return
        
    bot_name = f"{BOT_AVATARS[abs(bot_id) % len(BOT_AVATARS)]} Бот-{abs(bot_id)}"
    room.players[bot_id] = bot_name
    await callback.answer(f"✅ {bot_name} додано!")
    try: await callback.message.edit_text(f"Кімната: <code>{token}</code>\n👥 {len(room.players)}/6\n{', '.join(room.players.values())}", parse_mode="HTML", reply_markup=get_in_lobby_keyboard(True, token))
    except: pass

@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id): return
    await message.answer("🔢 Введіть код:")
    await state.set_state(PlayerState.waiting_for_token)

@router.message(PlayerState.waiting_for_token)
async def join_room_process(message: types.Message, state: FSMContext):
    await _process_join_room(message, message.text, state)

@router.message(F.text.regexp(r'^[A-Za-z0-9]{4,5}$'))
async def quick_join(message: types.Message, state: FSMContext):
    if await state.get_state() in [PlayerState.in_game, PlayerState.in_lobby]: return
    token = message.text.upper().strip()
    if token in rooms: await _process_join_room(message, token, state)
    else: await message.answer(f"❌ Кімнату <code>{token}</code> не знайдено.", parse_mode="HTML")

@router.message(F.text == "🚪 Покинути Лобі")
@router.message(F.text == "🚪 Покинути Гру")
async def leave_lobby(message: types.Message, state: FSMContext):
    user = message.from_user
    target_token = None
    for t, r in rooms.items():
        if user.id in r.players:
            target_token = t
            break
    if not target_token:
        await message.answer("ℹ️ Ви не в кімнаті.", reply_markup=main_menu)
        await state.clear()
        return

    room = rooms[target_token]
    if user.id in room.players: del room.players[user.id]
    if user.id in user_states: del user_states[user.id]

    # Якщо вийшли під час гри
    if room.game_started:
         # Якщо гравців стало < 3, гра ламається
         if len(room.players) < 3:
             await end_game(target_token, True, "👥 Недостатньо гравців. Шпигун переміг (технічна перемога).")
             return

    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли.", reply_markup=main_menu)
        return

    if user.id == room.admin_id:
        humans = [p for p in room.players if p > 0]
        if humans:
            room.admin_id = humans[0]
            try: await bot.send_message(room.admin_id, "👑 Ви адмін.", reply_markup=get_in_lobby_keyboard(True, target_token))
            except: pass
        else:
            del rooms[target_token]
            return

    for pid in room.players:
        try: await bot.send_message(pid, f"🚪 {user.full_name} вийшов.")
        except: pass
    
    await message.answer("✅ Ви вийшли.", reply_markup=main_menu)
    await state.clear()


# ------------------- GAME LOGIC -------------------

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players: return t, r
    return None, None

async def _game_timer(token: str):
    try:
        # Чекаємо час гри
        await asyncio.sleep(GAME_DURATION_SECONDS)
        room = rooms.get(token)
        if room and room.game_started:
            # Час вийшов! Примусове голосування.
            room.voting_started = True # Це прапор примусового голосування (is_forced)
            
            for uid in room.players:
                try: await bot.send_message(uid, "⏰ ЧАС ВИЙШОВ! Негайно голосуйте!", reply_markup=types.ReplyKeyboardRemove())
                except: pass
            
            # Запускаємо голосування з прапором forced=True
            await start_vote_procedure(token, forced=True)
            
    except asyncio.CancelledError: pass

@router.callback_query(F.data.startswith("start_game:"))
async def on_start_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room or callback.from_user.id != room.admin_id: return
    if len(room.players) < 3:
        await callback.answer("Треба мін. 3 гравці!", show_alert=True)
        return
    await start_game(room)
    try: await callback.message.edit_text(f"🎮 Гра почалася!")
    except: pass

async def start_game(room: Room):
    players = list(room.players.keys())
    humans = [p for p in players if p > 0] or players
    spy_id = random.choice(humans)
    
    room.spy_id = spy_id
    room.location = random.choice(LOCATIONS)
    room.game_started = True
    room.voting_started = False
    room.spy_guessed = False
    
    for pid in players:
        role = "spy" if pid == spy_id else "civilian"
        room.player_roles[pid] = role
        txt = "🕵️ ТИ — ШПИГУН! Вгадай локацію." if role == "spy" else f"👥 МИРНИЙ. Локація: {room.location}"
        try: 
            if pid > 0: await bot.send_message(pid, txt, reply_markup=in_game_menu)
        except: pass
    
    room.end_time = int(time.time()) + GAME_DURATION_SECONDS
    room._timer_task = asyncio.create_task(_game_timer(room.token))
    
    for bid in BOT_IDS:
        if bid in room.players: asyncio.create_task(_bot_behavior(bid, room))

async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room: return
    
    # Зупиняємо таймери
    if hasattr(room, "_timer_task"): room._timer_task.cancel()
    if hasattr(room, "_voting_task"): room._voting_task.cancel()
    if hasattr(room, "_early_vote_task"): room._early_vote_task.cancel()

    room.game_started = False
    
    players = list(room.players.keys())
    res_text = f"🏁 {reason}\n\n🕵️ Шпигуном був: {room.players.get(room.spy_id, 'Unknown')}\n📍 Локація: {room.location}"
    
    for uid in players:
        try: await bot.send_message(uid, res_text, reply_markup=main_menu)
        except: pass
        
    if grant_xp:
        for uid in players:
            if uid < 0: continue
            is_spy = (uid == room.spy_id)
            is_winner = (spy_won and is_spy) or (not spy_won and not is_spy)
            try:
                lvl_old, _, _ = await update_player_stats(uid, is_spy, is_winner)
                p = await get_or_create_player(uid, "")
                if p.level_info[0] > lvl_old:
                    await bot.send_message(uid, f"🎉 Новий рівень: {p.level_info[0]}!")
            except: pass

# --- ГОЛОСУВАННЯ (EARLY VOTE -> MAIN VOTE) ---

@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_req(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started: return
    
    # Запускаємо голосування ТАК/НІ
    room.early_votes = set()
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "🗳️ Завершити гру і голосувати?", reply_markup=get_early_vote_keyboard(token))
    
    # Таймер на прийняття рішення
    room._early_vote_task = asyncio.create_task(_finalize_early_vote(token))

async def _finalize_early_vote(token: str):
    await asyncio.sleep(30)
    room = rooms.get(token)
    if not room or not room.game_started: return
    
    votes = len(room.early_votes)
    if votes > len(room.players) / 2:
        for uid in room.players:
            try: await bot.send_message(uid, "✅ Більшість 'ЗА'. Починаємо пошук шпигуна!")
            except: pass
        await start_vote_procedure(token, forced=False)
    else:
        for uid in room.players:
            try: await bot.send_message(uid, "❌ Голосування відхилено. Граємо далі.")
            except: pass

@router.callback_query(F.data.startswith("early_vote_"))
async def early_vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    room = rooms.get(token)
    if not room: return
    if "yes" in cb.data: room.early_votes.add(cb.from_user.id)
    await cb.answer("Прийнято")

# --- ПРОЦЕДУРА ГОЛОСУВАННЯ ЗА ШПИГУНА ---

@router.message(Command("vote"))
async def manual_vote(message: types.Message):
    # Дозволяємо ручне голосування, якщо ще не йде
    token, room = _find_user_room(message.from_user.id)
    if room and room.game_started:
        await start_vote_procedure(token, forced=False)

async def start_vote_procedure(token: str, forced: bool = False):
    room = rooms.get(token)
    if not room: return
    
    room.player_votes = {}
    # forced = True означає, що це голосування через таймер (якщо нічия - шпигун виграє)
    # forced = False означає, що це дострокове (якщо нічия - граємо далі)
    room.voting_started = forced # Використаємо цей прапор або додамо новий атрибут, хай поки буде передаватись у finalize
    
    for uid in room.players:
        if uid > 0:
            await bot.send_message(uid, "🗳️ ХТО ШПИГУН? Оберіть гравця:", reply_markup=get_voting_keyboard(token, room.players, uid))
    
    # 45 секунд на вибір
    room._voting_task = asyncio.create_task(_finalize_suspect_vote(token, forced))

@router.callback_query(F.data.startswith("vote:"))
async def vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    target = int(cb.data.split(":")[2])
    room = rooms.get(token)
    if room:
        room.player_votes[cb.from_user.id] = target
        await cb.answer("Голос прийнято")

async def _finalize_suspect_vote(token: str, forced: bool):
    await asyncio.sleep(45)
    room = rooms.get(token)
    if not room or not room.game_started: return
    
    tally = {}
    for v in room.player_votes.values():
        tally[v] = tally.get(v, 0) + 1
    
    if not tally:
        # Ніхто не проголосував
        if forced: await end_game(token, True, "⏰ Час вийшов, ніхто не голосував. Перемога Шпигуна!")
        else: 
            for uid in room.players:
                try: await bot.send_message(uid, "ℹ️ Ніхто не проголосував. Граємо далі.")
                except: pass
        return

    max_votes = max(tally.values())
    top = [pid for pid, cnt in tally.items() if cnt == max_votes]
    
    if len(top) != 1:
        # НІЧИЯ
        if forced:
            await end_game(token, True, "⚖️ Нічия у фінальному голосуванні. Перемога Шпигуна!")
        else:
            for uid in room.players:
                try: await bot.send_message(uid, "⚖️ Нічия. Нікого не вигнали. Граємо далі.")
                except: pass
        return
    
    target = top[0]
    target_name = room.players.get(target, "Unknown")
    
    for uid in room.players:
        try: await bot.send_message(uid, f"👉 Більшістю голосів обрано: {target_name}")
        except: pass
        
    # Перевіряємо роль
    if target == room.spy_id:
        # ШПИГУН СПІЙМАНИЙ -> ОСТАННІЙ ШАНС
        room.spy_guessed = True # Блокуємо звичайні дії
        spy_id = room.spy_id
        
        for uid in room.players:
             try: await bot.send_message(uid, f"😲 {target_name} — це ШПИГУН! Але у нього є шанс вгадати локацію...")
             except: pass
        
        # Меню для шпигуна
        if spy_id > 0:
            await bot.send_message(spy_id, "📍 ТЕБЕ ВИКРИЛИ! Вгадай локацію, щоб виграти!", reply_markup=get_locations_keyboard(token, LOCATIONS))
            
            # Авто-програш, якщо шпигун тупить 30 сек
            async def _spy_last_chance_timer():
                await asyncio.sleep(30)
                if rooms.get(token) and rooms[token].game_started:
                    await end_game(token, False, "⏳ Шпигун не встиг обрати локацію. Перемога мирних!")
            asyncio.create_task(_spy_last_chance_timer())
            
    else:
        # Вигнали мирного
        room.players.pop(target, None) # Видаляємо гравця
        room.player_roles.pop(target, None)
        room.player_votes = {}
        
        for uid in room.players:
             try: await bot.send_message(uid, f"❌ {target_name} був МИРНИМ! Гра триває.")
             except: pass
             
        if len(room.players) < 3:
            await end_game(token, True, "👥 Занадто мало гравців. Перемога Шпигуна.")

# --- ВГАДУВАННЯ ЛОКАЦІЇ (ШПИГУН) ---

@router.message(Command("spy_guess"))
async def spy_guess_cmd(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and message.from_user.id == room.spy_id:
        await message.answer("Обери локацію:", reply_markup=get_locations_keyboard(token, LOCATIONS))

@router.callback_query(F.data.startswith("guess:"))
async def on_location_guess(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    loc = cb.data.split(":")[2]
    room = rooms.get(token)
    if not room: return
    
    if cb.from_user.id != room.spy_id:
        await cb.answer("Ти не шпигун!")
        return
        
    if loc.lower() == room.location.lower():
        await end_game(token, True, f"🗺️ Шпигун ВГАДАВ локацію ({loc})! Перемога Шпигуна!")
    else:
        await end_game(token, False, f"❌ Шпигун помилився ({loc}). Правильна локація: {room.location}. Перемога Мирних!")

# --- ЧАТ МІЖ ГРАВЦЯМИ (РОЗМІСТИТИ В КІНЦІ!) ---

@router.message(F.text & ~F.text.startswith("/"))
async def room_chat(message: types.Message):
    """Пересилає повідомлення іншим гравцям у кімнаті"""
    token, room = _find_user_room(message.from_user.id)
    if not room: return # Не в кімнаті - ігноруємо
    
    sender_name = room.players.get(message.from_user.id, "Unknown")
    formatted_text = f"<b>{sender_name}:</b> {message.text}"
    
    for uid in room.players:
        if uid == message.from_user.id: continue # Собі не шлемо
        if uid < 0: continue # Ботам не шлемо
        try:
            await bot.send_message(uid, formatted_text, parse_mode="HTML")
        except: pass

# --- ПОВЕДІНКА БОТІВ ---
async def _bot_behavior(bot_id, room):
    while room.game_started:
        await asyncio.sleep(random.uniform(10, 30))
        # Тут можна додати випадкові фрази в чат від ботів
        # Але поки залишимо тільки логіку голосування, якщо вона викликана