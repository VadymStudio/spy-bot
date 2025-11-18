import asyncio
import logging
import os
import sys

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage

# Імпорти з твоїх модулів
# Важливо дотримуватися структури папок
try:
    from config import (
        API_TOKEN, 
        USE_POLLING, 
        RENDER_EXTERNAL_HOSTNAME, #пж работай
        WEBHOOK_PATH, 
        DB_PATH
    )
    from database.crud import init_db
    from utils.matchmaking import start_matchmaking_processor
    from middlewares.antispam import AntiSpamMiddleware
    from middlewares.ban import BanMiddleware
    from handlers import setup_handlers
    from bot import bot, dp
except ImportError as e:
    print(f"CRITICAL IMPORT ERROR: {e}")
    print("Make sure your folder structure matches the imports (handlers/, database/, utils/, middlewares/)")
    sys.exit(1)

# Налаштування логування
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def on_startup(app):
    """Дії при старті веб-додатку"""
    logger.info("Starting up...")
    
    # 1. Ініціалізація БД
    await init_db()
    logger.info("Database initialized.")

    # 2. Налаштування хендлерів
    setup_handlers(dp)
    
    # 3. Мідлварі
    dp.message.middleware(AntiSpamMiddleware())
    dp.message.middleware(BanMiddleware())
    
    # 4. Запуск матмейкінгу
    start_matchmaking_processor()
    
    # 5. Налаштування Telegram
    if USE_POLLING:
        logger.info("Mode: POLLING")
        await bot.delete_webhook(drop_pending_updates=True)
        # Запускаємо polling у фоновому завданні, щоб не блокувати web server
        asyncio.create_task(dp.start_polling(bot))
    else:
        logger.info("Mode: WEBHOOK")
        webhook_url = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"
        await bot.set_webhook(webhook_url)
        logger.info(f"Webhook set to: {webhook_url}")

async def handle_webhook(request):
    """Обробка вхідних вебхуків від Telegram"""
    try:
        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return web.Response(text="ok")
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(text="error", status=500)

async def health_check(request):
    """Проста перевірка, що сервер живий (для Render)"""
    return web.Response(text="I am alive!", status=200)

def main():
    # Створення веб-додатку aiohttp
    app = web.Application()
    
    # Додавання маршрутів
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    
    # Якщо режим вебхуку - додаємо POST endpoint
    if not USE_POLLING:
        app.router.add_post(WEBHOOK_PATH, handle_webhook)
    
    # Реєстрація startup події
    app.on_startup.append(on_startup)
    
    # Отримання порту від Render
    port = int(os.getenv("PORT", 10000))
    
    logger.info(f"Starting web server on port {port}")
    
    # Запуск веб-сервера
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()