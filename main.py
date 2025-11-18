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

async def main() -> None:
    # Підключаємо всі обробники
    setup_handlers(dp)
    # Підключаємо middleware
    dp.message.middleware(AntiSpamMiddleware())
    dp.message.middleware(BanMiddleware())

    # Спільна ініціалізація
    await init_db()
    start_matchmaking_processor()

    if USE_POLLING:
        # Режим POLLING: видаляємо вебхук і запускаємо polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook deleted (drop_pending_updates=True)")
        except Exception as e:
            logging.warning(f"Failed to delete webhook: {e}")
        await dp.start_polling(bot)
        return

    # Режим WEBHOOK (Render Web Service)
    webhook_url = f"https://{RENDER_EXTERNAL_HOSTNAME}{WEBHOOK_PATH}"
    logging.info(f"Webhook: {webhook_url}")
    try:
        await bot.set_webhook(webhook_url)
        logging.info(f"Webhook set: {webhook_url}")
    except Exception as e:
        logging.error(f"Failed to set webhook: {e}")
        raise

    app = web.Application()

    async def handle_webhook(request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad request")
        try:
            update = Update.model_validate(data)
        except Exception:
            return web.Response(status=400, text="invalid update")
        try:
            await dp.feed_update(bot, update)
        except Exception:
            # Не падаємо на окремих апдейтах
            pass
        return web.Response(text="ok")

    async def health(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/health", health)

    async def on_shutdown(app: web.Application):
        try:
            await bot.session.close()
        except Exception:
            pass
    app.on_shutdown.append(on_shutdown)

    port = int(os.getenv("PORT", "10000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    logging.info(f"Init complete\n======== Running on http://0.0.0.0:{port} ========")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())

