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
    add_active_user, 
    rooms, 
    LOCATIONS, 
    GAME_DURATION_SECONDS, 
    BOT_IDS, 
    BOT_AVATARS
)
from utils.helpers import maintenance_blocked, generate_room_token, is_admin
# Імпортуємо нові функції з matchmaking
from utils.matchmaking import enqueue_user, dequeue_user, is_in_queue
from utils.states import PlayerState
from database.crud import update_player_stats, get_or_create_player, get_player_stats
from database.models import Room, UserState
from keyboards.keyboards import (
    in_queue_menu, in_lobby_menu, main_menu, in_game_menu, 
    get_early_vote_keyboard, get_voting_keyboard, get_locations_keyboard, get_in_lobby_keyboard
)

router = Router()
logger = logging.getLogger(__name__)

user_states = {}
GAME_CALLSIGNS = ["Альфа", "Браво", "Чарлі", "Дельта", "Ехо", "Фокстрот", "Гольф", "Хантер", "Індіго", "Джульєтта", "Кіло", "Ліма", "Майк", "Нова", "Оскар", "Папа", "Ромео", "Сьєрра", "Танго", "Віктор", "Віскі", "Рентген", "Янкі", "Зулу", "Прайм", "Тінь", "Привид"]

# --- 1. СТАТИСТИКА ---
@router.message(F.text == "📊 Моя Статистика")
@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    user = message.from_user
    stats = await get_player_stats(user.id)
    if not stats:
        await get_or_create_player(user.id, user.username)
        stats = await get_player_stats(user.id)
    
    games = stats['games_played']
    wins = stats['spy_wins'] + stats['civilian_wins']
    win_rate = (wins / games * 100) if games > 0 else 0
    level, cur_xp, need_xp = stats['level_info']
    
    text = (
        f"📊 <b>СТАТИСТИКА:</b> {user.full_name}\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"⭐ Рівень: <b>{level}</b> ({cur_xp}/{need_xp} XP)\n"
        f"🎮 Ігор: <b>{games}</b>\n"
        f"🏆 Перемог: <b>{wins}</b> ({win_rate:.1f}%)\n"
        f"🕵️ Шпигун: {stats['spy_wins']}\n"
        f"👥 Мирний: {stats['civilian_wins']}"
    )
    await message.answer(text, parse_mode="HTML")

# --- 2. МЕНЮ І ПОШУК (ОНОВЛЕНО) ---
@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
    
    user_id = message.from_user.id
    add_active_user(user_id)
    
    # Якщо вже в черзі
    if is_in_queue(user_id):
        await message.answer("Ви вже в черзі.", reply_markup=in_queue_menu)
        return

    # Надсилаємо повідомлення, яке будемо редагувати
    status_msg = await message.answer(
        "🔍 <b>Шукаємо гру...</b>\n⏳ У черзі: <b>1/6</b> гравців", 
        parse_mode="HTML", 
        reply_markup=in_queue_menu
    )
    
    # Передаємо ID повідомлення в чергу
    enqueue_user(user_id, status_msg.message_id)

@router.message(F.text == "❌ Скасувати Пошук")
async def cancel_search(message: types.Message):
    if is_in_queue(message.from_user.id):
        dequeue_user(message.from_user.id)
        await message.answer("❌ Пошук скасовано.", reply_markup=main_menu)
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
        token=token, admin_id=message.from_user.id, players={message.from_user.id: message.from_user.full_name},
        player_roles={}, player_votes={}, early_votes=set()
    )
    room.player_callsigns = {}
    room.votes_yes = set()
    room.votes_no = set()
    rooms[token] = room
    
    if message.from_user.id not in user_states: user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    await message.answer("✅ Лобі створено.", reply_markup=in_lobby_menu)
    
    show_bot = is_admin(message.from_user.id)
    await message.answer(
        f"Кімната: <code>{token}</code>", 
        parse_mode="HTML", 
        reply_markup=get_in_lobby_keyboard(True, token, show_add_bot=show_bot)
    )

@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id): return
    await message.answer("🔢 Код:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(PlayerState.waiting_for_token)

async def _process_join_room(message: types.Message, token: str, state: FSMContext):
    user = message.from_user
    token = token.upper().strip()
    if token not in rooms:
        if len(token) in [4,5] and token.isalnum(): await message.answer("❌ Не знайдено.", reply_markup=main_menu)
        else: await message.answer("❌ Невірний код.", reply_markup=main_menu)
        return
    room = rooms[token]
    if len(room.players) >= 6:
        await message.answer("❌ Повна.", reply_markup=main_menu)
        return
    if room.game_started:
        await message.answer("❌ Гра йде.", reply_markup=main_menu)
        return
    if user.id in room.players:
        await message.answer("ℹ️ Вже тут.", reply_markup=in_lobby_menu)
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        if user.id not in user_states: user_states[user.id] = UserState()
        user_states[user.id].current_room = token
        
        for pid in room.players:
            if pid == user.id: continue
            try: await bot.send_message(pid, f"➕ {user.full_name} зайшов! ({len(room.players)}/6)")
            except: pass
            
        await message.answer(f"✅ Ви в кімнаті <code>{token}</code>", parse_mode="HTML", reply_markup=in_lobby_menu)
        
        is_room_admin = (user.id == room.admin_id)
        show_bot = is_admin(user.id) and is_room_admin
        await message.answer("Меню:", reply_markup=get_in_lobby_keyboard(is_room_admin, token, show_bot))
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
    if hasattr(room, 'player_callsigns') and user.id in room.player_callsigns: del room.player_callsigns[user.id]
    
    if room.game_started:
         if len(room.players) < 3:
             await end_game(target_token, True, "👥 Недостатньо гравців.")
             return
    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли.", reply_markup=main_menu)
        return
    if user.id == room.admin_id:
        humans = [p for p in room.players if p > 0]
        if humans:
            room.admin_id = humans[0]
            new_adm_show_bot = is_admin(humans[0])
            try: await bot.send_message(room.admin_id, "👑 Ви адмін.", reply_markup=get_in_lobby_keyboard(True, target_token, new_adm_show_bot))
            except: pass
        else:
            del rooms[target_token]
            return
    for pid in room.players:
        try: await bot.send_message(pid, f"🚪 {user.full_name} вийшов.")
        except: pass
    await message.answer("✅ Ви вийшли.", reply_markup=main_menu)
    await state.clear()

# --- 3. БОТИ І СТАРТ ---
@router.callback_query(F.data.startswith("add_bot_btn:"))
async def on_add_bot_click(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
         await callback.answer("Доступ заборонено", show_alert=True)
         return
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room or callback.from_user.id != room.admin_id: return
    
    bot_id = None
    for bid in BOT_IDS:
        if bid not in room.players:
            bot_id = bid
            break
    if not bot_id:
        await callback.answer("Максимум.", show_alert=True)
        return
    
    bot_name = f"{BOT_AVATARS[abs(bot_id) % len(BOT_AVATARS)]} Бот-{abs(bot_id)}"
    room.players[bot_id] = bot_name
    await callback.answer(f"✅ {bot_name} додано!")
    
    for pid in room.players:
        try: await bot.send_message(pid, f"🤖 Додано бота: {bot_name} ({len(room.players)}/6)")
        except: pass

@router.callback_query(F.data.startswith("start_game:"))
async def on_start_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room or callback.from_user.id != room.admin_id: return
    if len(room.players) < 3:
        await callback.answer("Мін 3 гравці.", show_alert=True)
        return
    await start_game(room)
    try: await callback.message.delete() 
    except: pass
    await callback.message.answer("🎮 Почали!")

# --- 4. ГРА ---
async def start_game(room: Room):
    players = list(room.players.keys())
    av_calls = GAME_CALLSIGNS.copy()
    random.shuffle(av_calls)
    room.player_callsigns = {}
    for pid in players:
        room.player_callsigns[pid] = av_calls.pop() if av_calls else f"A-{abs(pid)}"
        
    humans = [p for p in players if p > 0] or players
    spy_id = random.choice(humans)
    room.spy_id = spy_id
    room.location = random.choice(LOCATIONS)
    room.game_started = True
    room.voting_started = False
    room.spy_guessed = False
    room.votes_yes = set()
    room.votes_no = set()
    
    for pid in players:
        role = "spy" if pid == spy_id else "civilian"
        room.player_roles[pid] = role
        callsign = room.player_callsigns[pid]
        txt = f"🕵️ ТИ — ШПИГУН!\nПозивний: <b>{callsign}</b>\nВгадай локацію." if role == "spy" else f"👥 МИРНИЙ.\nПозивний: <b>{callsign}</b>\n📍 Локація: <b>{room.location}</b>"
        try: 
            if pid > 0: await bot.send_message(pid, txt, parse_mode="HTML", reply_markup=in_game_menu)
        except: pass
    
    room.end_time = int(time.time()) + GAME_DURATION_SECONDS
    room._timer_task = asyncio.create_task(_game_timer(room.token))
    for bid in BOT_IDS:
        if bid in room.players: asyncio.create_task(_bot_behavior(bid, room))

async def _game_timer(token: str):
    try:
        room = rooms.get(token)
        if not room: return
        while True:
            now = int(time.time())
            rem = room.end_time - now
            if rem <= 0: break
            if rem <= 5 and room.game_started and not room.voting_started:
                 for uid in room.players:
                     if uid > 0:
                         try: await bot.send_message(uid, f"⏰ {rem}...")
                         except: pass
            await asyncio.sleep(1)
            if token not in rooms or not rooms[token].game_started: return
        if room and room.game_started:
            for uid in room.players:
                if uid > 0: await bot.send_message(uid, "⏰ ЧАС! Голосуємо!")
            await start_vote_procedure(token, forced=True)
    except asyncio.CancelledError: pass

async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room: return
    for t in ["_timer_task", "_voting_task", "_early_vote_task"]:
        tk = getattr(room, t, None)
        if tk: tk.cancel()
    room.game_started = False
    
    players = list(room.players.keys())
    spy_real = room.players.get(room.spy_id, "Bot")
    spy_call = room.player_callsigns.get(room.spy_id, "???")
    res_text = f"🏁 <b>ГРУ ЗАВЕРШЕНО!</b>\n{reason}\n\n🕵️ Шпигун: <b>{spy_call}</b> ({spy_real})\n📍 Локація: <b>{room.location}</b>"
    
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
            except: pass
            
    if room.admin_id > 0 and room.admin_id in room.players:
        show_bot = is_admin(room.admin_id)
        try: await bot.send_message(room.admin_id, "⚙️ Меню:", reply_markup=get_in_lobby_keyboard(True, token, show_bot))
        except: pass

# --- ГОЛОСУВАННЯ ---
@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_req(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started: return
    room.votes_yes = set()
    room.votes_no = set()
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "🗳️ Завершити гру?", reply_markup=get_early_vote_keyboard(token))
    
    room._early_vote_task = asyncio.create_task(_finalize_early_vote(token))

async def _finalize_early_vote(token: str):
    await asyncio.sleep(30)
    room = rooms.get(token)
    if not room or not room.game_started: return
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "⏰ Час вийшов. Граємо далі.")

@router.callback_query(F.data.startswith("early_vote_"))
async def early_vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    room = rooms.get(token)
    if not room or not room.game_started: return
    uid = cb.from_user.id
    choice = "yes" if "yes" in cb.data else "no"
    if choice == "yes": room.votes_yes.add(uid)
    else: room.votes_no.add(uid)
    await cb.answer("OK")
    try: await cb.message.delete()
    except: pass
    
    total = len(room.players)
    if len(room.votes_yes) > total / 2:
        if hasattr(room, "_early_vote_task"): room._early_vote_task.cancel()
        for u in room.players: 
            if u > 0: await bot.send_message(u, "✅ Більшість ЗА.")
        await start_vote_procedure(token, forced=False)
    elif len(room.votes_no) >= total / 2:
        if hasattr(room, "_early_vote_task"): room._early_vote_task.cancel()
        for u in room.players:
            if u > 0: await bot.send_message(u, "❌ Відхилено.")

async def start_vote_procedure(token: str, forced: bool = False):
    room = rooms.get(token)
    if not room: return
    room.player_votes = {}
    room.voting_started = True
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "☠️ ХТО ШПИГУН?", reply_markup=get_voting_keyboard(token, room.player_callsigns, uid))
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
    for i in range(45, 0, -1):
        if i <= 5 and rooms.get(token):
             for uid in rooms[token].players:
                 if uid > 0: 
                     try: await bot.send_message(uid, f"⏳ {i}...")
                     except: pass
        await asyncio.sleep(1)
    room = rooms.get(token)
    if not room or not room.game_started: return
    room.voting_started = False
    tally = {}
    for v in room.player_votes.values(): tally[v] = tally.get(v, 0) + 1
    if not tally:
        if forced: await end_game(token, True, "⏰ Ніхто не голосував. Шпигун переміг!")
        else: 
             for uid in room.players:
                 if uid > 0: await bot.send_message(uid, "ℹ️ Пропуск.")
        return
    max_v = max(tally.values())
    top = [p for p, c in tally.items() if c == max_v]
    if len(top) != 1:
        if forced: await end_game(token, True, "⚖️ Нічия. Шпигун переміг!")
        else:
             for uid in room.players:
                 if uid > 0: await bot.send_message(uid, "⚖️ Нічия. Граємо далі.")
        return
    target = top[0]
    t_call = room.player_callsigns.get(target, "Unknown")
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, f"👉 Вигнано: <b>{t_call}</b>", parse_mode="HTML")
    
    if target == room.spy_id:
        room.spy_guessed = True
        spy_id = room.spy_id
        if spy_id > 0: await bot.send_message(spy_id, "😱 ТЕБЕ ВИКРИЛИ! 30с на вгадування!", reply_markup=get_locations_keyboard(token, LOCATIONS))
        
        # Чекаємо 30 сек
        for i in range(30, 0, -1):
             if rooms.get(token) and not rooms[token].game_started: return # Шпигун вже вгадав
             if i <= 5:
                 try: await bot.send_message(spy_id, f"⏳ {i}...")
                 except: pass
             await asyncio.sleep(1)

        if rooms.get(token) and rooms[token].game_started: await end_game(token, False, "⏳ Шпигун не встиг.")
    else:
        room.players.pop(target, None)
        if len(room.players) < 3: await end_game(token, True, "👥 Мало гравців.")

@router.message(Command("spy_guess"))
async def spy_guess_cmd(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and message.from_user.id == room.spy_id:
        await message.answer("Локація:", reply_markup=get_locations_keyboard(token, LOCATIONS))

@router.callback_query(F.data.startswith("guess:"))
async def on_location_guess(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    loc = cb.data.split(":")[2]
    room = rooms.get(token)
    if not room or not room.game_started: return
    if cb.from_user.id != room.spy_id: return
    if loc.lower() == room.location.lower(): await end_game(token, True, f"🗺️ Шпигун вгадав ({loc})!")
    else: await end_game(token, False, f"❌ Помилка ({loc}).")

@router.message(F.text == "❓ Моя роль")
async def my_role(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and room.game_started:
        role = room.player_roles.get(message.from_user.id)
        callsign = room.player_callsigns.get(message.from_user.id)
        msg = f"🕵️ ШПИГУН ({callsign})" if role == "spy" else f"👥 МИРНИЙ ({callsign}). {room.location}"
        await message.answer(msg)

@router.message(F.text & ~F.text.startswith("/"))
async def room_chat(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room: return 
    uid = message.from_user.id
    if room.game_started:
        name = room.player_callsigns.get(uid, "Unknown")
        txt = f"📻 <b>{name}:</b> {message.text}"
    else:
        name = room.players.get(uid, message.from_user.first_name)
        txt = f"👤 <b>{name}:</b> {message.text}"
    for pid in room.players:
        if pid != uid and pid > 0:
            try: await bot.send_message(pid, txt, parse_mode="HTML")
            except: pass

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players: return t, r
    return None, None

async def _bot_behavior(bot_id, room):
    while room.game_started:
        await asyncio.sleep(random.uniform(5, 15))
        
        if room.voting_started and bot_id not in room.player_votes:
             cands = [u for u in room.players if u != bot_id]
             if cands: room.player_votes[bot_id] = random.choice(cands)

        if room.early_votes:
            if bot_id not in room.votes_yes and bot_id not in room.votes_no:
                if random.random() < 0.3: room.votes_yes.add(bot_id)
                else: room.votes_no.add(bot_id)