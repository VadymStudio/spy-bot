from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Dict, Optional, Union, Tuple

# Головне меню
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Знайти Гру")],
        [KeyboardButton(text="🚪 Створити Кімнату"), KeyboardButton(text="🤝 Приєднатися")],
        [KeyboardButton(text="📊 Моя Статистика"), KeyboardButton(text="❓ Допомога")]
    ],
    resize_keyboard=True
)

# Меню в черзі
in_queue_menu = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Скасувати Пошук")]],
    resize_keyboard=True
)

# Меню в лобі
in_lobby_menu = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🚪 Покинути Лобі")]],
    resize_keyboard=True
)

# Меню в грі
in_game_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="❓ Моя роль"), KeyboardButton(text="🗳️ Достр. Голосування")],
        [KeyboardButton(text="🚪 Покинути Гру")]
    ],
    resize_keyboard=True
)

def get_voting_keyboard(room_token: str, players: Dict[int, str], voter_id: int) -> InlineKeyboardMarkup:
    """Повертає клавіатуру для голосування за вигнання гравців."""
    builder = InlineKeyboardBuilder()
    
    for player_id, username in players.items():
        if player_id != voter_id:  # Не дозволяємо голосувати за себе
            builder.button(
                text=f"👤 {username}",
                callback_data=f"vote:{room_token}:{player_id}"
            )
    
    builder.button(
        text="❌ Скасувати",
        callback_data=f"vote_cancel:{room_token}"
    )
    
    builder.adjust(2)  # 2 кнопки в рядок
    return builder.as_markup()

def get_locations_keyboard(room_token: str, locations: List[str], columns: int = 3) -> InlineKeyboardMarkup:
    """Повертає клавіатуру з варіантами локацій для шпигуна."""
    builder = InlineKeyboardBuilder()
    
    for location in locations:
        builder.button(
            text=location,
            callback_data=f"guess:{room_token}:{location}"
        )
    
    builder.adjust(columns)  # Вказуємо кількість кнопок у рядку
    return builder.as_markup()

def get_confirm_keyboard(room_token: str) -> InlineKeyboardMarkup:
    """Повертає клавіатуру з підтвердженням для початку гри."""
    builder = InlineKeyboardBuilder()
    
    builder.button(
        text="✅ Так, почати гру",
        callback_data=f"start_game:{room_token}"
    )
    
    builder.button(
        text="❌ Ні, скасувати",
        callback_data=f"cancel_start:{room_token}"
    )
    
    builder.adjust(1)  # По одній кнопці в рядок
    return builder.as_markup()

def get_early_vote_keyboard(room_token: str) -> InlineKeyboardMarkup:
    """Повертає клавіатуру для дострокового завершення гри."""
    builder = InlineKeyboardBuilder()
    
    builder.button(
        text="✅ Так, завершити гру",
        callback_data=f"early_vote_yes:{room_token}"
    )
    
    builder.button(
        text="❌ Ні, продовжити гру",
        callback_data=f"early_vote_no:{room_token}"
    )
    
    builder.adjust(1)  # По одній кнопці в рядок
    return builder.as_markup()

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Повертає клавіатуру адміністратора."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/maintenance_on"), KeyboardButton(text="/maintenance_off")],
            [KeyboardButton(text="/ban"), KeyboardButton(text="/unban")],
            [KeyboardButton(text="/stats"), KeyboardButton(text="/whois")],
            [KeyboardButton(text="/get_db"), KeyboardButton(text="/get_logs")],
            [KeyboardButton(text="/main_menu")]
        ],
        resize_keyboard=True
    )
