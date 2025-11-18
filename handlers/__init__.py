from aiogram import Dispatcher, Router
# Імпортуємо модулі з поточної папки (handlers)
from . import admin, game, user

def setup_handlers(dp: Dispatcher) -> None:
    """Налаштовує всі обробники подій"""
    main_router = Router()
    
    # Підключаємо роутери
    main_router.include_router(user.router)
    main_router.include_router(game.router)
    main_router.include_router(admin.router)
    
    # Додаємо в диспетчер
    dp.include_router(main_router)

__all__ = ['setup_handlers']