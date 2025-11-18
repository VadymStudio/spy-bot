from typing import Dict, List
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- Reply Keyboards (Меню) ---

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

# --- Inline Keyboards (Кнопки дій) ---

def get_in_lobby_keyboard(is_admin: bool = False, room_token: str = "") -> InlineKeyboardMarkup:
    """
    Клавіатура лобі.
    Для адміна кімнати додає кнопку старту.
    """
    builder = InlineKeyboardBuilder()
    
    if is_admin and room_token:
        builder.button(text="🚀 Почати Гру", callback_data=f"start_game:{room_token}")
    
    # Можна додати інші кнопки, наприклад "Поділитися кодом"
    return builder.as_markup()

def get_voting_keyboard(room_token: str, players: Dict[int, str], voter_id: int) -> InlineKeyboardMarkup:
    """Клавіатура для голосування за вигнання."""
    builder = InlineKeyboardBuilder()
    
    for player_id, username in players.items():
        if player_id != voter_id:  # Не можна голосувати проти себе
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
    """Клавіатура локацій для шпигуна."""
    builder = InlineKeyboardBuilder()
    
    for location in locations:
        builder.button(
            text=location,
            callback_data=f"guess:{room_token}:{location}"
        )
    
    builder.adjust(columns)
    return builder.as_markup()

def get_confirm_keyboard(room_token: str) -> InlineKeyboardMarkup:
    """Підтвердження старту."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Так, почати гру", callback_data=f"start_game:{room_token}")
    builder.button(text="❌ Ні, скасувати", callback_data=f"cancel_start:{room_token}")
    builder.adjust(1)
    return builder.as_markup()

def get_early_vote_keyboard(room_token: str) -> InlineKeyboardMarkup:
    """Дострокове завершення."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Так, завершити гру", callback_data=f"early_vote_yes:{room_token}")
    builder.button(text="❌ Ні, продовжити гру", callback_data=f"early_vote_no:{room_token}")
    builder.adjust(1)
    return builder.as_markup()

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавіатура адміна бота."""
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