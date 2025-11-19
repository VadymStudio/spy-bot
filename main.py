import asyncio
import logging
import os

from bot import bot, dp
from handlers import setup_handlers
from config import USE_POLLING, RENDER_EXTERNAL_HOSTNAME, WEBHOOK_PATH
from database.crud import init_db
from utils.matchmaking import start_matchmaking_processor
from middlewares.antispam import AntiSpamMiddleware
from middlewares.ban import BanMiddleware
from aiohttp import web
from aiogram.types import Update

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def on_startup(app):
    await init_db()
    setup_handlers(dp)
    dp.message.middleware(AntiSpamMiddleware())
    dp.message.middleware(BanMiddleware())
    start_matchmaking_processor()
    
    # ВИДАЛЯЄМО КНОПКУ МЕНЮ (ТРИ СМУЖКИ)
    await bot.delete_my_commands()

    if USE_POLLING:
        await bot.delete_webhook(drop_pending_updates=True)
        asyncio.create_task(dp.start_polling(bot))
    else:
        webhook_url = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"
        await bot.set_webhook(webhook_url)

async def handle_webhook(request):
    try:
        data = await request.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return web.Response(text="ok")
    except:
        return web.Response(text="error", status=400)

async def health_check(request):
    return web.Response(text="I am alive!")

def main():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    if not USE_POLLING:
        app.router.add_post(WEBHOOK_PATH, handle_webhook)
    
    app.on_startup.append(on_startup)
    
    port = int(os.getenv("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()