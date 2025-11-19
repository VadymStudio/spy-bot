from aiogram import Dispatcher, Router
from . import admin, game, user

def setup_handlers(dp: Dispatcher) -> None:
    """Налаштовує всі обробники подій"""
    main_router = Router()
    
    # ВАЖЛИВО: Порядок має значення!
    # 1. Спочатку Адмінка (щоб працювала завжди)
    main_router.include_router(admin.router)
    
    # 2. Потім Гра (щоб перехоплювати стани введення коду)
    main_router.include_router(game.router)
    
    # 3. В кінці - звичайні юзерські команди (щоб не перебивати гру)
    main_router.include_router(user.router)
    
    # Підключаємо головний роутер до диспетчера
    dp.include_router(main_router)

__all__ = ['setup_handlers']