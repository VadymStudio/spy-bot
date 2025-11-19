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
    "Танго", "Віктор", "Віскі", "Рентген", "Янкі", "Зулу", "Прайм"
]

@router.message(F.text == "📊 Моя Статистика")
@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if maintenance_blocked(message.from_user.id): return
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

# --- МЕНЮ І ПОШУК ---

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
    room.votes_yes = set()
    room.votes_no = set()
    
    rooms[token] = room
    
    if message.from_user.id not in user_states: user_states[message.from_user.id] = UserState()
    user_states[message.from_user.id].current_room = token
    
    await message.answer("✅ Лобі створено.", reply_markup=in_lobby_menu)
    await message.answer(
        f"Кімната: <code>{token}</code>\n\nАдмін може додати ботів:", 
        parse_mode="HTML", 
        reply_markup=get_in_lobby_keyboard(True, token)
    )

@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id): return
    await message.answer("🔢 Введіть код кімнати:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(PlayerState.waiting_for_token)

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

@router.callback_query(F.data.startswith("add_bot_btn:"))
async def on_add_bot_click(callback: types.CallbackQuery):
    try:
        token = callback.data.split(":")[1]
        room = rooms.get(token)
        if not room: 
            await callback.answer("Кімнати не існує", show_alert=True)
            return
        
        if callback.from_user.id != room.admin_id:
            await callback.answer("❌ Тільки адмін!", show_alert=True)
            return
        
        bot_id = None
        for bid in BOT_IDS:
            if bid not in room.players:
                bot_id = bid
                break
        
        if not bot_id: