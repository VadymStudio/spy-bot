import asyncio
import time
import logging
from typing import List, Dict, Optional

from bot import bot
from config import matchmaking_queue, rooms, add_active_user
from keyboards.keyboards import in_lobby_menu, main_menu
from utils.helpers import generate_room_token
from database.models import Room, UserState

logger = logging.getLogger(__name__)

# Час, коли гравець став у чергу {user_id: timestamp}
_enqueued_at: Dict[int, float] = {}
_queue_last_change: float = time.time()
_processor_task: Optional[asyncio.Task] = None
_last_notify_ts: float = 0.0

MM_MIN = 3
MM_MAX = 6
MM_TIMEOUT = 120  # 2 хвилини

def enqueue_user(user_id: int) -> None:
    """Додає гравця в чергу."""
    if user_id not in matchmaking_queue:
        matchmaking_queue.append(user_id)
        _enqueued_at[user_id] = time.time()
        _mark_change()

def dequeue_user(user_id: int) -> None:
    """Видаляє гравця з черги."""
    if user_id in matchmaking_queue:
        matchmaking_queue.remove(user_id)
    if user_id in _enqueued_at:
        del _enqueued_at[user_id]
    _mark_change()

def _mark_change() -> None:
    global _queue_last_change
    _queue_last_change = time.time()

async def _create_rooms_from_queue(sizes: List[int]) -> None:
    """Створює кімнати для заданих розмірів груп."""
    from handlers.game import user_states  # Імпорт тут, щоб уникнути циклічності, якщо треба
    
    for size in sizes:
        if len(matchmaking_queue) < size:
            break
            
        # Беремо гравців
        players = []
        for _ in range(size):
            if matchmaking_queue:
                uid = matchmaking_queue.pop(0)
                _enqueued_at.pop(uid, None)
                players.append(uid)
        
        if not players:
            continue

        token = generate_room_token()
        
        # Отримуємо імена (треба робити запит до API або кешу, тут спрощено)
        # В реальності краще брати з БД або кешу user.py, але поки беремо ID
        players_dict = {}
        for uid in players:
            players_dict[uid] = f"Гравець-{uid}" 

        # Створюємо кімнату
        room = Room(
            token=token,
            admin_id=players[0], # Перший стає адміном
            players=players_dict,
            player_roles={},
            player_votes={},
            early_votes=set()
        )
        # Важливо: ініціалізуємо позивні
        room.player_callsigns = {}
        
        rooms[token] = room
        
        # Сповіщаємо
        for uid in players:
            # Встановлюємо стан (це милиця, бо user_states в game.py, але працюватиме)
            # Найкраще перенести user_states в окремий файл states_storage.py, але поки так:
            try:
                await bot.send_message(
                    uid,
                    (
                        "🎮 <b>Кімнату знайдено!</b>\n"
                        f"🔑 Код: <code>{token}</code>\n"
                        f"👥 Гравців: {len(players)}/{MM_MAX}\n\n"
                        "Чекаємо поки адмін запустить гру..."
                    ),
                    parse_mode="HTML",
                    reply_markup=in_lobby_menu,
                )
                # Якщо це адмін, даємо йому кнопку старту
                if uid == players[0]:
                    from keyboards.keyboards import get_in_lobby_keyboard
                    await bot.send_message(
                        uid, 
                        "Ви адміністратор кімнати!", 
                        reply_markup=get_in_lobby_keyboard(True, token)
                    )
            except Exception as e:
                logger.error(f"Failed to notify {uid}: {e}")

async def _processor_loop() -> None:
    """Фоновий процес, який формує пари і перевіряє тайм-аути."""
    try:
        while True:
            await asyncio.sleep(1)
            now = time.time()
            
            # 1. ПЕРЕВІРКА ТАЙМ-АУТІВ
            # Копіюємо ключі, бо змінюємо словник під час ітерації
            for uid, enqueued_time in list(_enqueued_at.items()):
                if now - enqueued_time > MM_TIMEOUT:
                    dequeue_user(uid)
                    try:
                        await bot.send_message(
                            uid, 
                            "⏰ <b>Час пошуку вийшов (2 хв).</b>\nСпробуйте пізніше або створіть власну кімнату.", 
                            parse_mode="HTML",
                            reply_markup=main_menu
                        )
                    except Exception:
                        pass

            # 2. ФОРМУВАННЯ КІМНАТ
            qlen = len(matchmaking_queue)
            if qlen < MM_MIN:
                continue
            
            # Якщо назбиралось 6 людей - одразу старт
            if qlen >= MM_MAX:
                await _create_rooms_from_queue([MM_MAX])
                continue
            
            # Якщо люди чекають більше 10 сек і їх достатньо (3+) - запускаємо
            # (Можна налаштувати логіку "хвиль", але поки проста)
            oldest_wait = now - min(_enqueued_at.values()) if _enqueued_at else 0
            if oldest_wait > 15 and qlen >= MM_MIN:
                await _create_rooms_from_queue([qlen])

    except asyncio.CancelledError:
        return

def start_matchmaking_processor() -> None:
    global _processor_task
    if _processor_task and not _processor_task.done():
        return
    _processor_task = asyncio.create_task(_processor_loop())