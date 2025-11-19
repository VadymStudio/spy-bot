from typing import Dict, List
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- Reply Keyboards (Меню) ---

main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Знайти Гру")],
        [KeyboardButton(text="🚪 Створити Кімнату"), KeyboardButton(text="🤝 Приєднатися")],
        [KeyboardButton(text="📊 Моя Статистика"), KeyboardButton(text="❓ Допомога")]
    ],
    resize_keyboard=True
)

in_queue_menu = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Скасувати Пошук")]],
    resize_keyboard=True
)

in_lobby_menu = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="🚪 Покинути Лобі")]],
    resize_keyboard=True
)

in_game_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="❓ Моя роль"), KeyboardButton(text="🗳️ Достр. Голосування")],
        [KeyboardButton(text="🚪 Покинути Гру")]
    ],
    resize_keyboard=True
)

# --- Inline Keyboards (Кнопки дій) ---

def get_in_lobby_keyboard(is_admin: bool = False, room_token: str = "") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if is_admin and room_token:
        builder.button(text="🚀 Почати Гру", callback_data=f"start_game:{room_token}")
        builder.button(text="🤖 Додати Бота", callback_data=f"add_bot_btn:{room_token}")
    builder.adjust(1)
    return builder.as_markup()

def get_voting_keyboard(room_token: str, names_dict: Dict[int, str], voter_id: int) -> InlineKeyboardMarkup:
    """
    names_dict: словник {id: "Позивний"} (якщо гра) або {id: "Ім'я"}
    """
    builder = InlineKeyboardBuilder()
    
    for player_id, name in names_dict.items():
        # Не показуємо кнопку проти себе
        if player_id != voter_id:
            builder.button(
                text=f"👉 {name}",
                callback_data=f"vote:{room_token}:{player_id}"
            )
    
    builder.adjust(2)
    return builder.as_markup()

def get_locations_keyboard(room_token: str, locations: List[str], columns: int = 3) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for location in locations:
        builder.button(text=location, callback_data=f"guess:{room_token}:{location}")
    builder.adjust(columns)
    return builder.as_markup()

def get_early_vote_keyboard(room_token: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Так", callback_data=f"early_vote_yes:{room_token}")
    builder.button(text="❌ Ні", callback_data=f"early_vote_no:{room_token}")
    builder.adjust(2)
    return builder.as_markup()

def get_admin_keyboard() -> ReplyKeyboardMarkup:
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