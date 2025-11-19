import asyncio
import time
import logging
from typing import List, Dict, Optional

from bot import bot
from config import matchmaking_queue, rooms
from keyboards.keyboards import in_lobby_menu, main_menu, get_in_lobby_keyboard
from utils.helpers import generate_room_token
from database.models import Room

logger = logging.getLogger(__name__)

# --- ВНУТРІШНІЙ СТАН ЧЕРГИ ---
# Зберігаємо час входу: {user_id: timestamp}
_enqueued_at: Dict[int, float] = {}
# Зберігаємо ID повідомлення, щоб оновлювати цифри: {user_id: message_id}
_queue_messages: Dict[int, int] = {}

_processor_task: Optional[asyncio.Task] = None

# НАЛАШТУВАННЯ
MM_MIN = 3           # Мін. гравців для старту
MM_MAX = 6           # Макс. гравців
MM_TIMEOUT = 120     # Час очікування (2 хв)
MM_WAIT_IF_NOT_FULL = 15 # Скільки чекати, якщо набралось мінімум (3-5 людей), але не повна

def enqueue_user(user_id: int, message_id: int) -> None:
    """Додає гравця в чергу."""
    if user_id not in matchmaking_queue:
        matchmaking_queue.append(user_id)
        _enqueued_at[user_id] = time.time()
        _queue_messages[user_id] = message_id

def dequeue_user(user_id: int) -> None:
    """Видаляє гравця з черги."""
    if user_id in matchmaking_queue:
        matchmaking_queue.remove(user_id)
    _enqueued_at.pop(user_id, None)
    _queue_messages.pop(user_id, None)

def is_in_queue(user_id: int) -> bool:
    return user_id in matchmaking_queue

async def _update_queue_status():
    """Оновлює 'живий' лічильник гравців у повідомленні."""
    count = len(matchmaking_queue)
    text = f"🔍 <b>Шукаємо гру...</b>\n⏳ У черзі: <b>{count}/{MM_MAX}</b> гравців"
    
    # Проходимо по всіх, хто чекає, і змінюємо їм текст
    for uid in list(matchmaking_queue):
        msg_id = _queue_messages.get(uid)
        if msg_id:
            try:
                await bot.edit_message_text(
                    text=text,
                    chat_id=uid,
                    message_id=msg_id,
                    parse_mode="HTML",
                    reply_markup=None # Можна лишити кнопку скасування, якщо вона була інлайн, але тут вона Reply
                )
            except Exception:
                pass # Ігноруємо помилки (наприклад, якщо текст не змінився)

async def _create_room_for_users(players: List[int]):
    """Створює кімнату для знайденої групи."""
    token = generate_room_token()
    
    # Створення об'єкта кімнати
    room = Room(
        token=token,
        admin_id=players[0], # Перший гравець стає адміном
        players={uid: f"Гравець-{uid}" for uid in players}, 
        player_roles={},
        player_votes={},
        early_votes=set()
    )
    # Важливо: ініціалізація пустих полів
    room.player_callsigns = {}
    room.votes_yes = set()
    room.votes_no = set()
    
    rooms[token] = room
    
    # Розсилка запрошень
    for uid in players:
        dequeue_user(uid) # Видаляємо з черги
        
        try:
            # Чи є цей гравець адміном кімнати?
            is_adm = (uid == players[0])
            
            # 1. Повідомлення про успіх
            await bot.send_message(
                uid,
                f"✅ <b>Гру знайдено!</b>\n🔑 Кімната: <code>{token}</code>\n👥 Гравців: {len(players)}",
                parse_mode="HTML",
                reply_markup=in_lobby_menu # Кнопка "Покинути Лобі"
            )
            
            # 2. Панель керування (кнопка Старт)
            await bot.send_message(
                uid,
                "Очікуйте початку гри...",
                reply_markup=get_in_lobby_keyboard(is_adm, token)
            )
        except Exception as e:
            logger.error(f"Error notifying user {uid}: {e}")

async def _processor_loop() -> None:
    """Головний цикл: перевіряє чергу кожні 2 секунди."""
    while True:
        try:
            await asyncio.sleep(2)
            now = time.time()
            
            # 1. ЧИСТКА (Тайм-аут 2 хв)
            for uid in list(matchmaking_queue):
                start_time = _enqueued_at.get(uid, 0)
                if now - start_time > MM_TIMEOUT:
                    dequeue_user(uid)
                    try:
                        await bot.send_message(
                            uid,
                            "⏰ <b>Час пошуку вийшов (2 хв).</b>\nГравців не знайдено. Спробуйте пізніше.",
                            parse_mode="HTML",
                            reply_markup=main_menu
                        )
                    except: pass
            
            # 2. ОНОВЛЕННЯ ЛІЧИЛЬНИКА
            if matchmaking_queue:
                await _update_queue_status()

            # 3. ПІДБІР (Логіка старту)
            q_len = len(matchmaking_queue)
            
            if q_len == 0:
                continue
                
            # А) ПОВНА КІМНАТА (6 гравців) -> Старт миттєво
            if q_len >= MM_MAX:
                chunk = matchmaking_queue[:MM_MAX]
                await _create_room_for_users(chunk)
                continue
            
            # Б) НЕПОВНА КІМНАТА (3-5 гравців) -> Старт через 15 сек очікування
            if q_len >= MM_MIN:
                first_user = matchmaking_queue[0]
                wait_time = now - _enqueued_at.get(first_user, now)
                
                if wait_time > MM_WAIT_IF_NOT_FULL:
                    chunk = matchmaking_queue[:] # Беремо всіх, хто є
                    await _create_room_for_users(chunk)
                    continue
                    
        except Exception as e:
            logger.error(f"Matchmaking Loop Error: {e}")
            await asyncio.sleep(5)

def start_matchmaking_processor() -> None:
    global _processor_task
    if _processor_task and not _processor_task.done():
        return
    _processor_task = asyncio.create_task(_processor_loop())