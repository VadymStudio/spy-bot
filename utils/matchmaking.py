import asyncio
import time
import logging
from typing import List, Dict, Optional
from contextlib import suppress

from aiogram.exceptions import TelegramBadRequest

from bot import bot
from config import matchmaking_queue, rooms
from keyboards.keyboards import in_lobby_menu, main_menu, get_in_lobby_keyboard, in_queue_menu
from utils.helpers import generate_room_token
from database.models import Room

logger = logging.getLogger(__name__)

# --- ВНУТРІШНІЙ СТАН ---
_enqueued_at: Dict[int, float] = {}
_queue_messages: Dict[int, int] = {}
_processor_task: Optional[asyncio.Task] = None

# НАЛАШТУВАННЯ
MM_MIN = 3
MM_MAX = 6
MM_TIMEOUT = 120 
MM_WAIT_IF_NOT_FULL = 15

async def enqueue_user(user_id: int, message_id: int) -> None:
    """Додає гравця в чергу і МИТТЄВО оновлює всім лічильник."""
    if user_id not in matchmaking_queue:
        matchmaking_queue.append(user_id)
        _enqueued_at[user_id] = time.time()
        _queue_messages[user_id] = message_id
        
        # Миттєво оновлюємо всім статус, щоб не чекати циклу
        await _update_queue_status()

def dequeue_user(user_id: int) -> None:
    """Прибирає гравця і оновлює лічильник іншим."""
    if user_id in matchmaking_queue:
        matchmaking_queue.remove(user_id)
    _enqueued_at.pop(user_id, None)
    _queue_messages.pop(user_id, None)
    
    # Запускаємо оновлення для тих, хто залишився (у фоні)
    asyncio.create_task(_update_queue_status())

def is_in_queue(user_id: int) -> bool:
    return user_id in matchmaking_queue

async def _update_queue_status():
    """Оновлює повідомлення ВСІМ гравцям у черзі."""
    count = len(matchmaking_queue)
    if count == 0: return

    # Різний текст для атмосфери
    if count == 1:
        status = "⏳ Чекаємо інших гравців..."
    elif count < MM_MIN:
        status = "🔎 Гравці підключаються..."
    else:
        status = "🚀 Скоро старт! Формуємо гру..."

    text = (
        f"🔍 <b>Пошук гри...</b>\n"
        f"👥 У черзі: <b>{count}/{MM_MAX}</b>\n"
        f"<i>{status}</i>"
    )
    
    for uid in list(matchmaking_queue):
        msg_id = _queue_messages.get(uid)
        if msg_id:
            # Використовуємо suppress, щоб ігнорувати помилки "message not modified"
            with suppress(TelegramBadRequest, Exception):
                await bot.edit_message_text(
                    text=text,
                    chat_id=uid,
                    message_id=msg_id,
                    parse_mode="HTML",
                    reply_markup=in_queue_menu # Важливо: лишаємо кнопку скасування
                )

async def _create_room_for_users(players: List[int]):
    token = generate_room_token()
    
    room = Room(
        token=token,
        admin_id=players[0],
        players={uid: f"Гравець-{uid}" for uid in players},
        player_roles={}, player_votes={}, early_votes=set()
    )
    room.player_callsigns = {}
    room.votes_yes = set()
    room.votes_no = set()
    rooms[token] = room
    
    for uid in players:
        # Важливо: видаляємо з черги БЕЗ виклику update_status (бо вони вже в грі)
        if uid in matchmaking_queue: matchmaking_queue.remove(uid)
        _enqueued_at.pop(uid, None)
        _queue_messages.pop(uid, None)
        
        try:
            is_adm = (uid == players[0])
            await bot.send_message(
                uid,
                f"✅ <b>Гру знайдено!</b>\n🔑 Кімната: <code>{token}</code>\n👥 Гравців: {len(players)}",
                parse_mode="HTML",
                reply_markup=in_lobby_menu
            )
            await bot.send_message(uid, "Меню:", reply_markup=get_in_lobby_keyboard(is_adm, token))
        except Exception as e:
            logger.error(f"Notify error {uid}: {e}")

async def _processor_loop() -> None:
    while True:
        try:
            await asyncio.sleep(2)
            now = time.time()
            
            # 1. Timeout check
            for uid in list(matchmaking_queue):
                start_time = _enqueued_at.get(uid, 0)
                if now - start_time > MM_TIMEOUT:
                    dequeue_user(uid)
                    with suppress(Exception):
                        await bot.send_message(uid, "⏰ Час вийшов. Людей замало.", reply_markup=main_menu)

            # 2. Match logic
            q_len = len(matchmaking_queue)
            if q_len == 0: continue
                
            if q_len >= MM_MAX:
                await _create_room_for_users(matchmaking_queue[:MM_MAX])
                continue
            
            if q_len >= MM_MIN:
                first_user = matchmaking_queue[0]
                # Якщо чекаємо вже довго - запускаємо тих хто є
                if now - _enqueued_at.get(first_user, now) > MM_WAIT_IF_NOT_FULL:
                    await _create_room_for_users(matchmaking_queue[:])
                    continue

        except Exception as e:
            logger.error(f"MM Loop: {e}")
            await asyncio.sleep(5)

def start_matchmaking_processor() -> None:
    global _processor_task
    if _processor_task and not _processor_task.done(): return
    _processor_task = asyncio.create_task(_processor_loop())