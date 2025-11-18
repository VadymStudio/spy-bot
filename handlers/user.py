import logging
from aiogram import Router, types
from aiogram.filters import Command

from keyboards.keyboards import main_menu
from database.crud import get_or_create_player, get_player_stats
from utils.helpers import maintenance_blocked
from config import add_active_user

router = Router()
logger = logging.getLogger(__name__)

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        return
    user = message.from_user
    # Переконуємось, що гравець існує в БД
    await get_or_create_player(user.id, user.username)
    await message.answer(
        "👋 Вітаю у грі 'Шпигун!'\n\n"
        "🎮 Грай з друзями або знаходь нових гравців.\n"
        "📌 Використовуй кнопки внизу для керування грою.",
        reply_markup=main_menu
    )
    add_active_user(user.id)

@router.message(Command("stats"))
@router.message(types.F.text == "📊 Моя Статистика")
async def cmd_stats(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    user = message.from_user
    stats = await get_player_stats(user.id)
    if not stats:
        # Створюємо запис і показуємо нульові значення
        await get_or_create_player(user.id, user.username)
        stats = {
            'games_played': 0,
            'spy_wins': 0,
            'civilian_wins': 0,
            'total_xp': 0
        }
    games = stats.get('games_played', 0)
    spy_w = stats.get('spy_wins', 0)
    civ_w = stats.get('civilian_wins', 0)
    total_xp = stats.get('total_xp', 0)
    wins = spy_w + civ_w
    win_rate = (wins / games * 100) if games > 0 else 0
    
    await message.answer(
        (
            "📊 <b>Ваша статистика</b>\n\n"
            f"🎮 Ігор: <b>{games}</b>\n"
            f"🏆 Перемог: <b>{wins}</b> (<i>{win_rate:.1f}%</i>)\n"
            f"🕵️ Шпигун перемоги: <b>{spy_w}</b>\n"
            f"👥 Цивільний перемоги: <b>{civ_w}</b>\n"
            f"⭐ Досвід: <b>{total_xp}</b> XP"
        ),
        parse_mode="HTML"
    )
    add_active_user(user.id)
