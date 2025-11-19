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
from database.crud import update_player_stats, get_or_create_player, get_player_stats
from database.models import Room, UserState

router = Router()
logger = logging.getLogger(__name__)

user_states = {}
GAME_CALLSIGNS = [
    "Альфа", "Браво", "Чарлі", "Дельта", "Ехо", "Фокстрот", 
    "Гольф", "Хантер", "Індіго", "Джульєтта", "Кіло", "Ліма", 
    "Майк", "Нова", "Оскар", "Папа", "Ромео", "Сьєрра", 
    "Танго", "Віктор", "Віскі", "Рентген", "Янкі", "Зулу"
]

# --- 0. СТАТИСТИКА (ПЕРЕНОСИМО СЮДИ, ЩОБ ПРАЦЮВАЛА) ---
@router.message(F.text == "📊 Моя Статистика")
@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    
    # Перевіряємо, чи гравець не в грі (щоб не спамив статистикою під час матчу)
    # Хоча, якщо хочеш щоб працювало завжди - прибери цей if
    
    user = message.from_user
    stats = await get_player_stats(user.id)
    if not stats:
        await get_or_create_player(user.id, user.username)
        stats = {'games_played': 0, 'spy_wins': 0, 'civilian_wins': 0, 'total_xp': 0}
    
    games = stats.get('games_played', 0)
    wins = stats.get('spy_wins', 0) + stats.get('civilian_wins', 0)
    win_rate = (wins / games * 100) if games > 0 else 0
    level, current_xp, xp_for_next = stats.get('level_info', (1, 0, 20))
    
    text = (
        f"📊 <b>СТАТИСТИКА:</b> {user.full_name}\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"⭐ Рівень: <b>{level}</b> ({current_xp}/{xp_for_next} XP)\n"
        f"🎮 Ігор: <b>{games}</b>\n"
        f"🏆 Перемог: <b>{wins}</b> ({win_rate:.1f}%)\n"
        f"🕵️ Як шпигун: {stats.get('spy_wins', 0)}\n"
        f"👥 Як мирний: {stats.get('civilian_wins', 0)}"
    )
    await message.answer(text, parse_mode="HTML")


# --- 1. МЕНЮ ---

@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    add_active_user(message.from_user.id)
    enqueue_user(message.from_user.id)
    await message.answer("🔍 Шукаємо гру (макс. 2 хв)...", reply_markup=in_queue_menu)

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
            await message.answer("❌ Ви вже в кімнаті.", reply_markup=in_lobby_menu)
            return

    token = generate_room_token()
    room = Room(
        token=token, 
        admin_id=message.from_user.id, 
        players={message.from_user.id: message.from_user.full_name}, 
        player_roles={}, 
        player_votes={}, 
        early_votes=set()
    )
    room.player_callsigns = {}
    rooms[token] = room
    
    if message.from_user.id not in user_states: user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    await message.answer("✅ Лобі створено.", reply_markup=in_lobby_menu)
    await message.answer(
        f"Кімната: <code>{token}</code>\n\nТільки адмін може додавати ботів.", 
        parse_mode="HTML", 
        reply_markup=get_in_lobby_keyboard(True, token)
    )

@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id): return
    await message.answer("🔢 Введіть код кімнати:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(PlayerState.waiting_for_token)

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
    if hasattr(room, 'player_callsigns') and user.id in room.player_callsigns:
        del room.player_callsigns[user.id]

    if room.game_started:
         if len(room.players) < 3:
             await end_game(target_token, True, "👥 Недостатньо гравців. Технічна перемога.")
             return

    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли.", reply_markup=main_menu)
        return

    if user.id == room.admin_id:
        humans = [p for p in room.players if p > 0]
        if humans:
            room.admin_id = humans[0]
            try: await bot.send_message(room.admin_id, "👑 Ви новий адмін.", reply_markup=get_in_lobby_keyboard(True, target_token))
            except: pass
        else:
            del rooms[target_token]
            return

    for pid in room.players:
        try: await bot.send_message(pid, f"🚪 {user.full_name} вийшов.")
        except: pass
    
    await message.answer("✅ Ви вийшли.", reply_markup=main_menu)
    await state.clear()

# --- 2. ВХІД ---

async def _process_join_room(message: types.Message, token: str, state: FSMContext):
    user = message.from_user
    token = token.upper().strip()
    
    if token not in rooms:
        if len(token) in [4, 5] and token.isalnum():
            await message.answer(f"❌ Кімнату <code>{token}</code> не знайдено.", parse_mode="HTML")
        else:
            await message.answer("❌ Невірний код.", reply_markup=main_menu)
        return

    room = rooms[token]
    if len(room.players) >= 6:
        await message.answer("❌ Кімната заповнена.", reply_markup=main_menu)
        return
    if room.game_started:
        await message.answer("❌ Гра вже йде.", reply_markup=main_menu)
        return

    if user.id in room.players:
        await message.answer("ℹ️ Ви вже тут.", reply_markup=in_lobby_menu)
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        if user.id not in user_states: user_states[user.id] = UserState()
        user_states[user.id].current_room = token

        for pid in room.players:
            if pid == user.id: continue
            try: await bot.send_message(pid, f"👤 {user.full_name} зайшов! ({len(room.players)}/6)")
            except: pass
        
        await message.answer(f"✅ Ви в кімнаті <code>{token}</code>", parse_mode="HTML", reply_markup=in_lobby_menu)
        # Перевірка, чи користувач адмін (на випадок перезаходу)
        is_admin = (user.id == room.admin_id)
        await message.answer("Меню лобі:", reply_markup=get_in_lobby_keyboard(is_admin, token))

    await state.clear()

@router.message(PlayerState.waiting_for_token)
async def join_room_process(message: types.Message, state: FSMContext):
    await _process_join_room(message, message.text, state)

@router.message(F.text.regexp(r'^[A-Za-z0-9]{4,5}$'))
async def quick_join(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state in [PlayerState.in_game, PlayerState.in_lobby]: return
    token = message.text.upper().strip()
    if token in rooms: await _process_join_room(message, token, state)

# --- 3. УПРАВЛІННЯ (БОТИ) ---

@router.callback_query(F.data.startswith("add_bot_btn:"))
async def on_add_bot_click(callback: types.CallbackQuery):
    try:
        token = callback.data.split(":")[1]
        room = rooms.get(token)
        if not room: 
            await callback.answer("Кімнати не існує", show_alert=True)
            return
        
        # СУВОРА ПЕРЕВІРКА АДМІНА
        if callback.from_user.id != room.admin_id:
            await callback.answer("❌ Тільки адмін кімнати може додавати ботів!", show_alert=True)
            return
        
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
        
        text = f"Кімната: <code>{token}</code>\n👥 {len(room.players)}/6\n\n" + "\n".join([f"- {name}" for name in room.players.values()])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_in_lobby_keyboard(True, token))
    except Exception as e:
        logger.error(f"Bot add error: {e}")
        await callback.answer("Помилка", show_alert=True)

@router.callback_query(F.data.startswith("start_game:"))
async def on_start_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room or callback.from_user.id != room.admin_id: return
    if len(room.players) < 3:
        await callback.answer("Треба мін. 3 гравці!", show_alert=True)
        return
    
    await start_game(room)
    try: await callback.message.delete() 
    except: pass
    await callback.message.answer("🎮 Гра почалася! Перевірте свої ролі.")

# --- 4. ГРА ---

async def start_game(room: Room):
    players = list(room.players.keys())
    
    available_callsigns = GAME_CALLSIGNS.copy()
    random.shuffle(available_callsigns)
    
    room.player_callsigns = {}
    for pid in players:
        callsign = available_callsigns.pop() if available_callsigns else f"Agent-{abs(pid)}"
        room.player_callsigns[pid] = callsign
        
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
        my_callsign = room.player_callsigns[pid]
        
        if role == "spy":
            txt = f"🕵️ ТИ — ШПИГУН!\nТвій позивний: <b>{my_callsign}</b>\nТвоя мета: вгадати локацію."
        else:
            txt = f"👥 ТИ — МИРНИЙ.\nТвій позивний: <b>{my_callsign}</b>\n📍 Локація: <b>{room.location}</b>"
            
        try: 
            if pid > 0: await bot.send_message(pid, txt, parse_mode="HTML", reply_markup=in_game_menu)
        except: pass
    
    room.end_time = int(time.time()) + GAME_DURATION_SECONDS
    room._timer_task = asyncio.create_task(_game_timer(room.token))
    
    for bid in BOT_IDS:
        if bid in room.players: asyncio.create_task(_bot_behavior(bid, room))

async def _game_timer(token: str):
    try:
        await asyncio.sleep(GAME_DURATION_SECONDS)
        room = rooms.get(token)
        if room and room.game_started:
            for uid in room.players:
                if uid > 0: await bot.send_message(uid, "⏰ ЧАС ВИЙШОВ! Голосування!", reply_markup=types.ReplyKeyboardRemove())
            await start_vote_procedure(token, forced=True)
    except asyncio.CancelledError: pass

async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room: return
    
    if hasattr(room, "_timer_task"): room._timer_task.cancel()
    if hasattr(room, "_voting_task"): room._voting_task.cancel()
    if hasattr(room, "_early_vote_task"): room._early_vote_task.cancel()

    room.game_started = False
    
    players = list(room.players.keys())
    spy_real = room.players.get(room.spy_id, "Bot")
    spy_call = room.player_callsigns.get(room.spy_id, "???")
    
    res_text = (
        f"🏁 <b>ГРУ ЗАВЕРШЕНО!</b>\n\n"
        f"{reason}\n\n"
        f"🕵️ Шпигун: <b>{spy_call}</b> ({spy_real})\n"
        f"📍 Локація: <b>{room.location}</b>"
    )
    
    for uid in players:
        try: await bot.send_message(uid, res_text, parse_mode="HTML", reply_markup=main_menu)
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

# --- 5. ГОЛОСУВАННЯ ---

@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_req(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started: return
    
    room.early_votes = set()
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "🗳️ Голосуємо за завершення?", reply_markup=get_early_vote_keyboard(token))
    
    asyncio.create_task(_finalize_early_vote(token))

async def _finalize_early_vote(token: str):
    await asyncio.sleep(30)
    room = rooms.get(token)
    if not room or not room.game_started: return
    
    if len(room.early_votes) > len(room.players) / 2:
        for uid in room.players: 
            if uid > 0: await bot.send_message(uid, "✅ Більшість ЗА.")
        await start_vote_procedure(token, forced=False)
    else:
        for uid in room.players:
            if uid > 0: await bot.send_message(uid, "❌ Відхилено. Граємо далі.")

@router.callback_query(F.data.startswith("early_vote_"))
async def early_vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    room = rooms.get(token)
    if not room: return
    if "yes" in cb.data: room.early_votes.add(cb.from_user.id)
    await cb.answer("Прийнято")
    try: await cb.message.delete()
    except: pass

@router.message(Command("vote"))
async def manual_vote(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and room.game_started:
        await start_vote_procedure(token, forced=False)

async def start_vote_procedure(token: str, forced: bool = False):
    room = rooms.get(token)
    if not room: return
    
    room.player_votes = {}
    # ВИКОРИСТОВУЄМО ПОЗИВНІ
    for uid in room.players:
        if uid > 0:
            await bot.send_message(
                uid, 
                "☠️ ХТО ШПИГУН?", 
                reply_markup=get_voting_keyboard(token, room.player_callsigns, uid)
            )
    
    room._voting_task = asyncio.create_task(_finalize_suspect_vote(token, forced))

@router.callback_query(F.data.startswith("vote:"))
async def vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    target = int(cb.data.split(":")[2])
    room = rooms.get(token)
    if room:
        room.player_votes[cb.from_user.id] = target
        voted_name = room.player_callsigns.get(target, "Unknown")
        await cb.answer(f"Ви обрали: {voted_name}")
        await cb.message.edit_text(f"Голос проти: <b>{voted_name}</b>", parse_mode="HTML")

async def _finalize_suspect_vote(token: str, forced: bool):
    await asyncio.sleep(45)
    room = rooms.get(token)
    if not room or not room.game_started: return
    
    tally = {}
    for v in room.player_votes.values():
        tally[v] = tally.get(v, 0) + 1
    
    if not tally:
        if forced: await end_game(token, True, "⏰ Час вийшов. Шпигун переміг!")
        else: 
             for uid in room.players:
                 if uid > 0: await bot.send_message(uid, "ℹ️ Ніхто не проголосував.")
        return

    max_votes = max(tally.values())
    top = [pid for pid, cnt in tally.items() if cnt == max_votes]
    
    if len(top) != 1:
        if forced:
            await end_game(token, True, "⚖️ Нічия. Шпигун переміг!")
        else:
            for uid in room.players:
                if uid > 0: await bot.send_message(uid, "⚖️ Нічия. Граємо далі.")
        return
    
    target = top[0]
    target_callsign = room.player_callsigns.get(target, "Unknown")
    
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, f"👉 Вигнано: <b>{target_callsign}</b>", parse_mode="HTML")
        
    if target == room.spy_id:
        room.spy_guessed = True 
        spy_id = room.spy_id
        if spy_id > 0:
            await bot.send_message(spy_id, "😱 ТЕБЕ ВИКРИЛИ! 30с на вгадування!", reply_markup=get_locations_keyboard(token, LOCATIONS))
        
        await asyncio.sleep(30)
        if rooms.get(token) and rooms[token].game_started:
             await end_game(token, False, "⏳ Шпигун не встиг. Мирні виграли!")
            
    else:
        room.players.pop(target, None)
        room.player_callsigns.pop(target, None)
        if len(room.players) < 3:
            await end_game(token, True, "👥 Мало гравців. Шпигун переміг!")

# --- 6. ФІНАЛ (ЧАТ І ІНШЕ) ---

@router.message(F.text == "❓ Моя роль")
async def my_role(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and room.game_started:
        role = room.player_roles.get(message.from_user.id)
        callsign = room.player_callsigns.get(message.from_user.id)
        msg = f"🕵️ ШПИГУН ({callsign})" if role == "spy" else f"👥 МИРНИЙ ({callsign}). {room.location}"
        await message.answer(msg)

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
    if cb.from_user.id != room.spy_id: return
        
    if loc.lower() == room.location.lower():
        await end_game(token, True, f"🗺️ Шпигун вгадав ({loc})! Перемога Шпигуна!")
    else:
        await end_game(token, False, f"❌ Помилка ({loc}). Перемога Мирних!")

@router.message(F.text & ~F.text.startswith("/"))
async def room_chat(message: types.Message):
    """Розумний чат: в лобі імена, в грі позивні"""
    token, room = _find_user_room(message.from_user.id)
    if not room: return 
    
    user_id = message.from_user.id
    
    if room.game_started:
        # В ГРІ: ПОЗИВНІ
        sender_name = room.player_callsigns.get(user_id, "Unknown")
        formatted_text = f"📻 <b>{sender_name}:</b> {message.text}"
    else:
        # В ЛОБІ: СПРАВЖНІ ІМЕНА
        sender_name = room.players.get(user_id, message.from_user.first_name)
        formatted_text = f"👤 <b>{sender_name}:</b> {message.text}"
    
    for uid in room.players:
        if uid == user_id: continue
        if uid < 0: continue
        try: await bot.send_message(uid, formatted_text, parse_mode="HTML")
        except: pass

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players: return t, r
    return None, None

async def _bot_behavior(bot_id, room):
    while room.game_started:
        await asyncio.sleep(random.uniform(30, 90))
        # Боти просто мовчать або можна додати логіку