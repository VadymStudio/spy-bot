import asyncio
import time
import logging
from typing import List, Dict, Optional

from bot import bot
from config import rooms
from keyboards.keyboards import in_lobby_menu, main_menu, get_in_lobby_keyboard
from utils.helpers import generate_room_token
from database.models import Room, UserState

logger = logging.getLogger(__name__)

# --- ВНУТРІШНІЙ СТАН ЧЕРГИ ---
# user_id -> час входу
_enqueued_at: Dict[int, float] = {}
# user_id -> message_id (щоб редагувати повідомлення)
_queue_messages: Dict[int, int] = {}
# Список черги (зберігає порядок)
_matchmaking_queue: List[int] = []

_processor_task: Optional[asyncio.Task] = None

# Налаштування
MM_MIN = 3
MM_MAX = 6
MM_TIMEOUT = 120  # 2 хвилини
MM_WAIT_IF_NOT_FULL = 15 # Якщо є 3 людини, чекаємо 15 сек і запускаємо

def enqueue_user(user_id: int, message_id: int) -> None:
    """Додає гравця в чергу і запам'ятовує ID повідомлення для оновлення."""
    if user_id not in _matchmaking_queue:
        _matchmaking_queue.append(user_id)
        _enqueued_at[user_id] = time.time()
        _queue_messages[user_id] = message_id

def dequeue_user(user_id: int) -> None:
    """Прибирає гравця з черги."""
    if user_id in _matchmaking_queue:
        _matchmaking_queue.remove(user_id)
    _enqueued_at.pop(user_id, None)
    _queue_messages.pop(user_id, None)

def is_in_queue(user_id: int) -> bool:
    return user_id in _matchmaking_queue

async def _update_queue_messages():
    """Оновлює текст повідомлення для всіх, хто в черзі."""
    count = len(_matchmaking_queue)
    text = f"🔍 Шукаємо гру...\n👥 У черзі: <b>{count}</b> гравців"
    
    # Щоб не спамити API, робимо це обережно
    for user_id in list(_matchmaking_queue): # Копія списку
        msg_id = _queue_messages.get(user_id)
        if msg_id:
            try:
                # Aiogram не оновить, якщо текст не змінився (це добре)
                await bot.edit_message_text(
                    text=text,
                    chat_id=user_id,
                    message_id=msg_id,
                    parse_mode="HTML"
                )
            except Exception:
                pass

async def _create_room_for_users(players: List[int]):
    """Створює кімнату для списку гравців."""
    token = generate_room_token()
    
    # Ініціалізація кімнати
    room = Room(
        token=token,
        admin_id=players[0], # Перший стає адміном
        players={uid: f"Гравець-{uid}" for uid in players}, # Тимчасові імена, потім оновляться в game.py
        player_roles={},
        player_votes={},
        early_votes=set()
    )
    room.player_callsigns = {}
    room.votes_yes = set()
    room.votes_no = set()
    
    rooms[token] = room
    
    # Сповіщаємо гравців
    for uid in players:
        # Видаляємо з черги
        dequeue_user(uid)
        
        # Встановлюємо UserState (в game.py це оновиться детальніше, але тут база)
        # (Тут ми не маємо доступу до user_states змінної з game.py, 
        #  але це не критично, бо game.py опрацьовує події)
        
        try:
            is_admin = (uid == players[0])
            await bot.send_message(
                uid,
                f"✅ <b>Гру знайдено!</b>\nКімната: <code>{token}</code>\nГравців: {len(players)}",
                parse_mode="HTML",
                reply_markup=in_lobby_menu
            )
            # Окремо кидаємо меню
            await bot.send_message(
                uid,
                "Меню лобі:",
                reply_markup=get_in_lobby_keyboard(is_admin, token)
            )
        except Exception as e:
            logger.error(f"Fail notify {uid}: {e}")

async def _processor_loop() -> None:
    """Головний цикл матчмейкінгу."""
    while True:
        try:
            await asyncio.sleep(2) # Перевірка кожні 2 секунди
            now = time.time()
            
            # 1. ВИДАЛЕННЯ "ПРОСТРОЧЕНИХ" (TIMEOUT)
            for uid in list(_matchmaking_queue):
                start_time = _enqueued_at.get(uid, 0)
                if now - start_time > MM_TIMEOUT:
                    dequeue_user(uid)
                    try:
                        await bot.send_message(
                            uid,
                            "⏰ <b>Час пошуку вийшов (2 хв).</b>\nНа жаль, групу не знайдено. Спробуйте пізніше.",
                            parse_mode="HTML",
                            reply_markup=main_menu
                        )
                    except: pass
            
            # 2. ОНОВЛЕННЯ ЛІЧИЛЬНИКА (Для тих, хто лишився)
            await _update_queue_messages()

            # 3. АЛГОРИТМ ПІДБОРУ
            q_len = len(_matchmaking_queue)
            
            if q_len == 0:
                continue
                
            # ВАРІАНТ А: Повна кімната (6+)
            if q_len >= MM_MAX:
                # Беремо перших 6
                chunk = _matchmaking_queue[:MM_MAX]
                await _create_room_for_users(chunk)
                continue
            
            # ВАРІАНТ Б: Неповна кімната (3-5), але чекають довго
            if q_len >= MM_MIN:
                # Перевіряємо, скільки часу чекає найперший гравець
                first_user = _matchmaking_queue[0]
                wait_time = now - _enqueued_at.get(first_user, now)
                
                if wait_time > MM_WAIT_IF_NOT_FULL:
                    # Створюємо кімнату для всіх, хто є (3, 4 або 5)
                    chunk = _matchmaking_queue[:] 
                    await _create_room_for_users(chunk)
                    continue
                    
        except Exception as e:
            logger.error(f"MM Error: {e}")
            await asyncio.sleep(5)

def start_matchmaking_processor() -> None:
    global _processor_task
    if _processor_task and not _processor_task.done():
        return
    _processor_task = asyncio.create_task(_processor_loop())