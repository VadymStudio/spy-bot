from aiogram import Dispatcher, Router
from . import admin, game, user

def setup_handlers(dp: Dispatcher) -> None:
    """Налаштовує всі обробники подій"""
    # Єдиний комбінований роутер
    main_router = Router()
    
    # Підключаємо роутери з модулів
    main_router.include_router(admin.router)
    main_router.include_router(user.router)
    main_router.include_router(game.router)
    
    # Підключаємо головний роутер до диспетчера
    dp.include_router(main_router)

__all__ = ['setup_handlers']