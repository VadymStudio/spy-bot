import logging
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from keyboards.keyboards import main_menu, get_admin_keyboard
from database.crud import get_or_create_player, get_player_stats
from utils.helpers import maintenance_blocked, is_admin
from config import add_active_user

router = Router()
logger = logging.getLogger(__name__)

@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id):
        return
    
    # Скидаємо будь-які стани при старті
    await state.clear()
    
    user = message.from_user
    await get_or_create_player(user.id, user.username)
    await message.answer(
        "👋 Вітаю у грі 'Шпигун!'\n\n"
        "🎮 Грай з друзями або знаходь нових гравців.\n"
        "📌 Використовуй кнопки внизу для керування грою.",
        reply_markup=main_menu
    )
    add_active_user(user.id)

@router.message(Command("admin"))
async def admin_menu(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🛠 Адмін-меню", reply_markup=get_admin_keyboard())

@router.message(Command("main_menu"))
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Головне меню", reply_markup=main_menu)

@router.message(F.text == "📊 Моя Статистика")
@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    user = message.from_user
    stats = await get_player_stats(user.id)
    if not stats:
        await get_or_create_player(user.id, user.username)
        stats = {'games_played': 0, 'spy_wins': 0, 'civilian_wins': 0, 'total_xp': 0}
    
    games = stats.get('games_played', 0)
    wins = stats.get('spy_wins', 0) + stats.get('civilian_wins', 0)
    total_xp = stats.get('total_xp', 0)
    
    # Розрахунок рівня
    level, current_xp, xp_for_next = stats.get('level_info', (1, 0, 20))
    
    await message.answer(
        (
            f"📊 <b>Ваша статистика:</b>\n\n"
            f"⭐ Рівень: <b>{level}</b>\n"
            f"📈 XP: {current_xp}/{xp_for_next}\n"
            f"🎮 Ігор зіграно: {games}\n"
            f"🏆 Перемог: {wins}\n"
            f"🕵️ За шпигуна: {stats.get('spy_wins', 0)}\n"
            f"👥 За мирного: {stats.get('civilian_wins', 0)}"
        ),
        parse_mode="HTML"
    )

@router.message(F.text == "❓ Допомога")
@router.message(Command("help"))
async def cmd_help(message: types.Message):
    text = (
        "<b>📖 Як грати в Шпигуна?</b>\n\n"
        "1. Гравці опиняються в одній локації (наприклад, Банк), але Шпигун не знає, де він.\n"
        "2. <b>Завдання мирних:</b> вичислити шпигуна, ставлячи питання один одному.\n"
        "3. <b>Завдання шпигуна:</b> зрозуміти, що це за локація, і не видати себе.\n\n"
        "Ви можете створити власну кімнату і запросити друзів за кодом, або знайти випадкову гру."
    )
    await message.answer(text, parse_mode="HTML")

# --- ЦЕ ВИПРАВЛЯЄ ПРОБЛЕМУ З ВВЕДЕННЯМ КОДУ ---
@router.message(F.text)
async def unknown_message(message: types.Message, state: FSMContext):
    """
    Цей хендлер ловить весь текст, який не підійшов під команди.
    АЛЕ він перевіряє, чи не знаходиться гравець у процесі введення чогось важливого.
    """
    current_state = await state.get_state()
    
    # Якщо у гравця є активний стан (наприклад, він вводить код кімнати),
    # то ми ігноруємо це повідомлення тут, щоб воно пішло в game.py
    if current_state is not None:
        return
        
    # Якщо станів немає, то це просто невідомий текст
    if maintenance_blocked(message.from_user.id):
        return

    await message.answer(
        "🤔 Я не розумію цього повідомлення.\nБудь ласка, користуйтеся кнопками меню.",
        reply_markup=main_menu
    )