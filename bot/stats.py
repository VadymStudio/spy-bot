from bot.admin import check_ban_and_reply
import logging
import asyncio
import random
import os
import json
import time
import psutil
import aiosqlite
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeAllPrivateChats, FSInputFile, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, PreCheckoutQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import uuid
import aiohttp
import tenacity
from collections import deque
from bot.database import get_player_stats
from bot.utils import logger
from bot.admin import check_ban_and_reply  # Якщо потрібно

# Решта коду stats.py без змін
xp_level_cache = {}

def get_level_from_xp(total_xp):
    if total_xp < 20:
        return 1, 20, total_xp, 0
    if total_xp in xp_level_cache:
        return xp_level_cache[total_xp]
    level = 1
    xp_needed_for_next = 20
    current_total_xp_needed = 0
    multiplier = 1.50
    while True:
        current_total_xp_needed += xp_needed_for_next
        level += 1
        if total_xp < current_total_xp_needed:
            level -= 1
            xp_at_level_start = current_total_xp_needed - xp_needed_for_next
            xp_in_level = total_xp - xp_at_level_start
            xp_level_cache[total_xp] = (level, xp_needed_for_next, xp_in_level, xp_at_level_start)
            return level, xp_needed_for_next, xp_in_level, xp_at_level_start
        xp_needed_for_next = int(xp_needed_for_next * multiplier)
        if multiplier > 1.20:
            multiplier = max(1.20, multiplier - 0.02)

async def show_stats(message, state):
    if await check_ban_and_reply(message): return
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    try:
        stats = await get_player_stats(user_id, username)
        user_id, username, total_xp, games_played, spy_wins, civilian_wins, banned_until, premium_until, owned_packs = stats
        level, xp_needed_for_level, xp_in_current_level, _ = get_level_from_xp(total_xp)
        total_wins = spy_wins + civilian_wins
        winrate = (total_wins / games_played * 100) if games_played > 0 else 0
        stats_text = (
            f"📊 <b>Ваша статистика</b> 📊\n\n"
            f"👤 <b>Нік:</b> {username}\n"
            f"🎖 <b>Рівень:</b> {level}\n"
            f"✨ <b>Досвід (XP):</b> {xp_in_current_level} / {xp_needed_for_level}\n"
            f"🏆 <b>Вінрейт:</b> {winrate:.1f}% (всього перемог: {total_wins})\n"
            f"🕹 <b>Всього ігор:</b> {games_played}\n\n"
            f"🕵️ <b>Перемог за Шпигуна:</b> {spy_wins}\n"
            f"👨‍🌾 <b>Перемог за Мирного:</b> {civilian_wins}"
        )
        await message.reply(stats_text)
    except Exception as e:
        logger.error(f"Failed to get stats for {user_id}: {e}", exc_info=True)
        await message.reply("Не вдалося завантажити вашу статистику. Спробуйте пізніше.")