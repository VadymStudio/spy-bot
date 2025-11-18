import asyncio
import time
from typing import Dict, List

from aiogram import BaseMiddleware
from aiogram.types import Message

from bot import bot
from config import (
    MAX_MSG_PER_SEC,
    SPAM_COOLDOWN_SECONDS,
    MAX_TEXT_LENGTH,
    BLOCK_MEDIA,
)


class AntiSpamMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        super().__init__()
        self._recent: Dict[int, List[float]] = {}
        self._cooldown_until: Dict[int, float] = {}
        self._cooldown_task: Dict[int, asyncio.Task] = {}

    def _in_cooldown(self, user_id: int) -> bool:
        now = time.time()
        until = self._cooldown_until.get(user_id, 0)
        return now < until

    async def _schedule_cooldown_end_notice(self, user_id: int) -> None:
        # cancel existing task if any
        task = self._cooldown_task.get(user_id)
        if task and not task.done():
            task.cancel()
        async def _runner():
            try:
                while True:
                    now = time.time()
                    until = self._cooldown_until.get(user_id, 0)
                    delta = until - now
                    if delta <= 0:
                        break
                    await asyncio.sleep(min(1.0, delta))
                # cooldown finished
                self._cooldown_until.pop(user_id, None)
                self._recent.pop(user_id, None)
                try:
                    await bot.send_message(user_id, "✅ Обмеження знято. Ви можете знову писати.")
                except Exception:
                    pass
            except asyncio.CancelledError:
                return
        self._cooldown_task[user_id] = asyncio.create_task(_runner())

    async def __call__(self, handler, event: Message, data):
        # Only Messages
        if not isinstance(event, Message):
            return await handler(event, data)

        user_id = event.from_user.id if event.from_user else None
        if not user_id:
            return await handler(event, data)

        # Media blocking (gif/photo/sticker)
        if BLOCK_MEDIA and (event.animation or event.photo or event.sticker):
            try:
                await event.answer("🛑 Медіа (гіфки/фото/стікери) заборонені в цій грі.")
            except Exception:
                pass
            return  # drop

        # Text length limit
        if event.text and len(event.text) > MAX_TEXT_LENGTH:
            try:
                await event.answer(f"🛑 Повідомлення надто довге (> {MAX_TEXT_LENGTH} символів). Скоротіть, будь ласка.")
            except Exception:
                pass
            return  # drop

        now = time.time()

        # Rate limiting
        if self._in_cooldown(user_id):
            # prolong cooldown while spamming
            self._cooldown_until[user_id] = now + SPAM_COOLDOWN_SECONDS
            await self._schedule_cooldown_end_notice(user_id)
            # only warn once at start of cooldown (task sends end-notice later)
            return  # drop during cooldown

        # Update sliding window (1 second)
        buf = self._recent.setdefault(user_id, [])
        # remove entries older than 1s
        buf = [t for t in buf if now - t <= 1.0]
        buf.append(now)
        self._recent[user_id] = buf

        if len(buf) > MAX_MSG_PER_SEC:
            # enter cooldown
            self._cooldown_until[user_id] = now + SPAM_COOLDOWN_SECONDS
            try:
                await event.answer(
                    f"⛔ Спам виявлено: не більше {MAX_MSG_PER_SEC} повідомлень/сек. "
                    f"Зачекайте {SPAM_COOLDOWN_SECONDS} секунд."
                )
            except Exception:
                pass
            await self._schedule_cooldown_end_notice(user_id)
            return  # drop this message

        # Allowed -> continue
        return await handler(event, data)
