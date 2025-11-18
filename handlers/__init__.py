from aiogram import Dispatcher, Router
from . import admin, game, user

def setup_handlers(dp: Dispatcher) -> None:
    """Налаштовує всі обробники подій"""
    # Єдиний комбінований роутер
    router = Router()
    router.include_router(user.router)
    router.include_router(game.router)
    router.include_router(admin.router)
    dp.include_router(router)

__all__ = ['setup_handlers']
