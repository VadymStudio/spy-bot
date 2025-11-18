import asyncio
import time
from typing import List, Dict, Optional

from bot import bot
from config import matchmaking_queue, rooms
from config import add_active_user  # optional use if needed
from database.models import Room
from keyboards.keyboards import in_lobby_menu
from utils.helpers import generate_room_token

# Internal state for matchmaking timing
_enqueued_at: Dict[int, float] = {}
_queue_last_change: float = time.time()
_processor_task: Optional[asyncio.Task] = None

MM_MIN = 3
MM_MAX = 6


def _mark_change() -> None:
    global _queue_last_change
    _queue_last_change = time.time()


def enqueue_user(user_id: int) -> None:
    if user_id not in matchmaking_queue:
        matchmaking_queue.append(user_id)
        _enqueued_at[user_id] = time.time()
        _mark_change()


def dequeue_user(user_id: int) -> None:
    if user_id in matchmaking_queue:
        matchmaking_queue.remove(user_id)
        _enqueued_at.pop(user_id, None)
        _mark_change()


def _optimal_partition(n: int) -> List[int]:
    """Return a list of room sizes (3..6) that sum to n or as many as possible without leaving 1-2 leftover.
    Strategy: maximize 6's, then handle remainder by lookup.
    """
    sizes: List[int] = []
    while n >= 12:
        sizes.append(6)
        n -= 6
    # Handle remainder 0..11
    mapping = {
        0: [],
        3: [3],
        4: [4],
        5: [5],
        6: [6],
        7: [4, 3],
        8: [4, 4],
        9: [6, 3],
        10: [6, 4],  # or [5,5]
        11: [6, 5],
    }
    if n in mapping:
        sizes.extend(mapping[n])
        return sizes
    # For n in {1,2}, we cannot form a room; ignore for now
    return sizes


def _force_full_room_ready() -> bool:
    """Return True if we should force-start a full room of 6 based on 10s wait for the earliest in that block."""
    if len(matchmaking_queue) < MM_MAX:
        return False
    block = matchmaking_queue[:MM_MAX]
    first = block[0]
    t0 = _enqueued_at.get(first, time.time())
    return (time.time() - t0) >= 10


async def _create_rooms_from_queue(group_sizes: List[int]) -> None:
    idx = 0
    for size in group_sizes:
        if len(matchmaking_queue) < size:
            break
        players = matchmaking_queue[:size]
        del matchmaking_queue[:size]
        for uid in players:
            _enqueued_at.pop(uid, None)
        token = generate_room_token()
        while token in rooms:
            token = generate_room_token()
        admin_id = players[0]
        room = Room(token=token, admin_id=admin_id, last_activity=int(time.time()))
        for uid in players:
            room.players[uid] = f"Гравець {len(room.players)+1}"
        rooms[token] = room
        # notify
        for uid in players:
            try:
                await bot.send_message(
                    uid,
                    (
                        "🎮 Знайдено кімнату!\n"
                        f"🔑 Код: {token}\n"
                        f"👥 Гравців: {len(players)}/{MM_MAX}\n\n"
                        "Очікування старту гри..."
                    ),
                    reply_markup=in_lobby_menu,
                )
            except Exception:
                pass
        idx += size


async def _processor_loop() -> None:
    try:
        while True:
            await asyncio.sleep(1)
            qlen = len(matchmaking_queue)
            if qlen < MM_MIN:
                continue
            # Force-start for full block of 6 if waited 10s
            if _force_full_room_ready():
                await _create_rooms_from_queue([MM_MAX])
                _mark_change()
                continue
            # If queue stable for >=5s, do optimal partition
            if (time.time() - _queue_last_change) >= 5:
                sizes = _optimal_partition(qlen)
                if sizes:
                    await _create_rooms_from_queue(sizes)
                    _mark_change()
    except asyncio.CancelledError:
        return


def start_matchmaking_processor() -> None:
    global _processor_task
    if _processor_task and not _processor_task.done():
        return
    _processor_task = asyncio.create_task(_processor_loop())
