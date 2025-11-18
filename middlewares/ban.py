import time
from aiogram import BaseMiddleware
from aiogram.types import Message
from database.crud import get_player

class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if not isinstance(event, Message):
            return await handler(event, data)
        if not event.from_user:
            return await handler(event, data)
        user_id = event.from_user.id
        player = await get_player(user_id)
        if player and getattr(player, 'banned_until', 0) and int(player.banned_until) > int(time.time()):
            remaining = int(player.banned_until) - int(time.time())
            try:
                await event.answer(f"🚫 Ви заблоковані. Залишилось: ~{remaining} сек.")
            except Exception:
                pass
            return  # drop event
        return await handler(event, data)
