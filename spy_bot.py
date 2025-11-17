from bot.utils import on_startup
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
from dotenv import load_dotenv
from bot.database import db_init
from bot.rooms import load_rooms, cleanup_rooms, save_rooms
from bot.utils import health_check, set_webhook_with_retry, check_webhook_periodically
from bot.handlers import dp
from bot.game import matchmaking_processor

# Решта коду spy_bot.py без змін
# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Завантажуємо змінні з .env
load_dotenv()
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")
ADMIN_IDS_STR = os.getenv('ADMIN_ID')
if not ADMIN_IDS_STR:
    raise ValueError("ADMIN_ID is not set in environment variables. Please set it (comma-separated if multiple).")
ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',')]
logger.info(f"Loaded Admin IDs: {ADMIN_IDS}")
USE_POLLING = os.getenv('USE_POLLING', 'false').lower() == 'true'
RENDER_EXTERNAL_HOSTNAME = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')

bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

async def main():
    await db_init()
    load_rooms()
    asyncio.create_task(matchmaking_processor())
    asyncio.create_task(cleanup_rooms())
    await on_startup(dp)
    if USE_POLLING:
        await dp.start_polling(bot)
    else:
        app = web.Application()
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
        app.router.add_route('GET', '/health', health_check)
        app.router.add_route('HEAD', '/health', health_check)
        setup_application(app, dp, bot=bot)
        webhook_url = f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"
        await set_webhook_with_retry(webhook_url)
        asyncio.create_task(check_webhook_periodically())
        port = int(os.getenv("PORT", 443))
        web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    asyncio.run(main())