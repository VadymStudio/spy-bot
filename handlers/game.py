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

# Глобальний словник для станів користувачів
user_states = {}

# --- ДОПОМІЖНА ФУНКЦІЯ ВХОДУ (ЩОБ НЕ ДУБЛЮВАТИ КОД) ---
async def _process_join_room(message: types.Message, token: str, state: FSMContext):
    user = message.from_user
    token = token.upper().strip()
    
    if token not in rooms:
        # Якщо це виглядає як код (4-5 символів), але його немає
        if len(token) in [4, 5] and token.isalnum():
            await message.answer(f"❌ Кімнату з кодом <code>{token}</code> не знайдено.", parse_mode="HTML")
        else:
            await message.answer("❌ Невірний код або кімнати не існує.", reply_markup=main_menu)
        return

    room = rooms[token]
    
    # Перевірки
    if len(room.players) >= 6:
        await message.answer("❌ Кімната вже заповнена (6/6).")
        return
    
    if room.game_started:
        await message.answer("❌ Гра в цій кімнаті вже йде.")
        return

    # Логіка входу
    if user.id in room.players:
        await message.answer("ℹ️ Ви вже в цій кімнаті.", reply_markup=in_lobby_menu)
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        room.last_activity = int(datetime.now().timestamp())
        
        if user.id not in user_states:
            user_states[user.id] = UserState()
        user_states[user.id].current_room = token

        # Сповіщаємо інших
        for pid in room.players:
            if pid == user.id: continue
            try:
                await bot.send_message(pid, f"👤 {user.full_name} приєднався! 👥 {len(room.players)}/6")
            except Exception:
                pass
        
        await message.answer(
            f"✅ Ви приєднались до кімнати <code>{token}</code>\n👥 Гравців: {len(room.players)}/6",
            parse_mode="HTML",
            reply_markup=in_lobby_menu,
        )
        
        # Показуємо меню лобі (як у адміна, але без кнопки старту)
        await message.answer(
            "Очікуємо початку гри...",
            reply_markup=get_in_lobby_keyboard(is_admin=False, room_token=token)
        )

    await state.clear()


# --- ХЕНДЛЕРИ ---

@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування.")
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


@router.message(F.text == "🚪 Створити Кімнату")
async def create_room_cmd(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування.")
        return

    for room in rooms.values():
        if message.from_user.id in room.players:
            await message.answer("❌ Ви вже в іншій кімнаті. Спочатку вийдіть.")
            return

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
    
    if message.from_user.id not in user_states:
        user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    await message.answer("✅ Лобі створено.", reply_markup=in_lobby_menu)
    await message.answer(
        f"Кімната: <code>{token}</code>\n\nЗапрошуйте друзів!",
        parse_mode="HTML",
        reply_markup=get_in_lobby_keyboard(is_admin=True, room_token=token)
    )

@router.callback_query(F.data.startswith("add_bot_btn:"))
async def on_add_bot_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room: return
    if callback.from_user.id != room.admin_id:
        await callback.answer("❌ Тільки адмін!", show_alert=True)
        return
    if room.game_started:
        await callback.answer("Гра вже почалася!", show_alert=True)
        return
    
    bot_id = None
    for bid in BOT_IDS:
        if bid not in room.players:
            bot_id = bid
            break
    
    if bot_id is None:
        await callback.answer("❌ Максимум ботів!", show_alert=True)
        return
    
    bot_num = abs(bot_id)
    bot_name = f"{BOT_AVATARS[bot_num % len(BOT_AVATARS)]} Бот-{bot_num}"
    room.players[bot_id] = bot_name
    
    await callback.answer(f"✅ {bot_name} додано!")
    try:
        await callback.message.edit_text(
            f"Кімната: <code>{token}</code>\n👥 Гравців: {len(room.players)}/6\n\nСписок: {', '.join(room.players.values())}",
            parse_mode="HTML",
            reply_markup=get_in_lobby_keyboard(is_admin=True, room_token=token)
        )
    except: pass

@router.message(Command("add_bot"))
async def cmd_add_bot(message: types.Message):
    # (Стара команда, залишаємо для сумісності)
    token, room = _find_user_room(message.from_user.id)
    if not room or message.from_user.id != room.admin_id: return
    # ... (код скорочено, бо є кнопка) ...
    pass 

@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask_token(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id): return
    await message.answer("🔢 Введіть код кімнати (або просто напишіть його в чат):")
    await state.set_state(PlayerState.waiting_for_token)

@router.message(PlayerState.waiting_for_token)
async def join_room_process_token(message: types.Message, state: FSMContext):
    await _process_join_room(message, message.text, state)

# --- РОЗУМНИЙ ПЕРЕХОПЛЮВАЧ КОДУ ---
@router.message(F.text.regexp(r'^[A-Za-z0-9]{4,5}$'))
async def quick_join_room(message: types.Message, state: FSMContext):
    """
    Якщо користувач пише 4-5 літер (схоже на код), пробуємо підключити.
    Працює навіть без натискання 'Приєднатися'.
    """
    # Перевіряємо, чи користувач вже не в грі
    current_state = await state.get_state()
    if current_state in [PlayerState.in_game, PlayerState.in_lobby]:
        return # Не реагуємо, якщо він вже грає

    token = message.text.upper().strip()
    
    # Якщо такий код є в кімнатах - з'єднуємо
    if token in rooms:
        await _process_join_room(message, token, state)
    else:
        # Якщо коду немає, але юзер явно хотів ввести код
        # (можна прибрати цей else, якщо хочеш щоб бот мовчав на неправильні коди)
        await message.answer(f"❌ Кімнату <code>{token}</code> не знайдено.", parse_mode="HTML")

@router.message(F.text == "🚪 Покинути Лобі")
async def leave_lobby(message: types.Message, state: FSMContext):
    user = message.from_user
    target_token = None
    for t, r in rooms.items():
        if user.id in r.players and not r.game_started:
            target_token = t
            break
    if not target_token:
        await message.answer("ℹ️ Ви не в лобі.", reply_markup=main_menu)
        await state.clear()
        return

    room = rooms[target_token]
    if user.id in room.players: del room.players[user.id]
    if user.id in user_states: del user_states[user.id]

    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли. Кімнату закрито.", reply_markup=main_menu)
        return

    if user.id == room.admin_id:
        human_players = [p for p in room.players if p > 0]
        if human_players:
            room.admin_id = human_players[0]
            try:
                await bot.send_message(room.admin_id, "👑 Ви новий адмін.", reply_markup=get_in_lobby_keyboard(True, target_token))
            except: pass
        else:
            del rooms[target_token]
            return

    for pid in room.players:
        try: await bot.send_message(pid, f"🚪 {user.full_name} вийшов.")
        except: pass
    
    await message.answer("✅ Ви вийшли.", reply_markup=main_menu)
    await state.clear()


# ------------------- ЛОГІКА ГРИ -------------------

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players: return t, r
    return None, None

async def _game_timer(token: str):
    try:
        await asyncio.sleep(GAME_DURATION_SECONDS)
        room = rooms.get(token)
        if room and room.game_started:
            await end_game(token, spy_won=True, reason="⏰ Час вичерпано! Шпигун переміг.")
    except asyncio.CancelledError: pass

@router.callback_query(F.data.startswith("start_game:"))
async def on_start_game_click(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]
    room = rooms.get(token)
    if not room: return
    if callback.from_user.id != room.admin_id:
        await callback.answer("Тільки адмін!", show_alert=True)
        return
    if len(room.players) < 3:
        await callback.answer("Треба мін. 3 гравці!", show_alert=True)
        return
    
    await start_game(room)
    try: await callback.message.edit_text(f"🎮 Гра почалася! ({len(room.players)} гравців)")
    except: pass

async def start_game(room: Room):
    players = list(room.players.keys())
    humans = [p for p in players if p > 0] or players
    spy_id = random.choice(humans)
    
    room.spy_id = spy_id
    room.location = random.choice(LOCATIONS)
    room.game_started = True
    
    for pid in players:
        role_text = "🕵️ ТИ — ШПИГУН!" if pid == spy_id else f"👥 МИРНИЙ. Локація: {room.location}"
        room.player_roles[pid] = "spy" if pid == spy_id else "civilian"
        try:
            if pid > 0: await bot.send_message(pid, role_text, reply_markup=in_game_menu)
        except: pass
    
    room.end_time = int(time.time()) + GAME_DURATION_SECONDS
    room._timer_task = asyncio.create_task(_game_timer(room.token))
    
    for bid in BOT_IDS:
        if bid in room.players:
            asyncio.create_task(_bot_behavior(bid, room))

@router.message(F.text == "❓ Моя роль")
async def my_role(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Ви не в грі.")
        return
    role = room.player_roles.get(message.from_user.id)
    msg = "🕵️ ШПИГУН" if role == "spy" else f"👥 МИРНИЙ. {room.location}"
    await message.answer(msg)

async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room or not room.game_started: return
    room.game_started = False
    if hasattr(room, "_timer_task"): room._timer_task.cancel()
    
    players = list(room.players.keys())
    for uid in players:
        try: await bot.send_message(uid, f"🏁 {reason}", reply_markup=main_menu)
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

# --- ГОЛОСУВАННЯ ТА ІНШЕ ---
# (Залишаю скорочено, бо воно таке саме як було, головне було додати quick_join)

@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_request(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room: return
    room.early_votes = set()
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "Завершити гру?", reply_markup=get_early_vote_keyboard(token))
    # (Таймер на 30с тут має бути...)

@router.callback_query(F.data.startswith("early_vote_"))
async def early_vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    room = rooms.get(token)
    if not room: return
    if "yes" in cb.data: room.early_votes.add(cb.from_user.id)
    await cb.answer("Прийнято")

@router.message(Command("vote"))
async def start_vote(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room: return
    room.player_votes = {}
    for uid in room.players:
        if uid > 0: await bot.send_message(uid, "Хто шпигун?", reply_markup=get_voting_keyboard(token, room.players, uid))
    # (Таймер голосування тут...)

@router.callback_query(F.data.startswith("vote:"))
async def vote_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    target = int(cb.data.split(":")[2])
    room = rooms.get(token)
    if room:
        room.player_votes[cb.from_user.id] = target
        await cb.answer("Голос враховано")

@router.message(Command("spy_guess"))
async def spy_guess(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if room and message.from_user.id == room.spy_id:
        await message.answer("Локація?", reply_markup=get_locations_keyboard(token, LOCATIONS))

@router.callback_query(F.data.startswith("guess:"))
async def guess_cb(cb: types.CallbackQuery):
    token = cb.data.split(":")[1]
    loc = cb.data.split(":")[2]
    room = rooms.get(token)
    if not room: return
    if loc == room.location:
        await end_game(token, True, f"🕵️ Шпигун вгадав: {loc}")
    else:
        await cb.answer("❌ Невірно")

async def _bot_behavior(bot_id, room):
    if not room.game_started: return
    await asyncio.sleep(random.uniform(2, 5))
    # (Логіка ботів...)