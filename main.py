import asyncio
import logging

from bot import bot, dp
from handlers import setup_handlers
from config import USE_POLLING
from database.crud import init_db
from utils.matchmaking import start_matchmaking_processor
from middlewares.antispam import AntiSpamMiddleware
from middlewares.ban import BanMiddleware

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def main() -> None:
    # Підключаємо всі обробники
    setup_handlers(dp)
    # Підключаємо middleware
    dp.message.middleware(AntiSpamMiddleware())
    dp.message.middleware(BanMiddleware())

    # Запускаємо бота у режимі polling (поки без вебхука)
    await init_db()
    start_matchmaking_processor()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
