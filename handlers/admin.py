import logging
import asyncio
import os
from aiogram import Router, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile

from config import (
    set_maintenance_mode,
    is_maintenance_mode,
    DB_PATH,
    rooms,
    ADMIN_IDS
)
from utils.helpers import is_admin, parse_ban_time, compute_ban_until
from database.crud import update_player, get_player, get_recent_games, get_player_stats, reset_player_stats, get_all_users
from bot import bot

router = Router()
logger = logging.getLogger(__name__)

# --- СТАНИ ДЛЯ АДМІНКИ ---
class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    waiting_for_whois_id = State()

def _admin_only(message: types.Message) -> bool:
    return is_admin(message.from_user.id)

# --- ГОЛОВНЕ МЕНЮ АДМІНА ---
@router.message(Command("admin"))
async def admin_panel(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear() # Скидаємо всі зависання
    from keyboards.keyboards import get_admin_keyboard
    await message.answer("👮‍♂️ Адмін-панель активована.", reply_markup=get_admin_keyboard())

@router.message(Command("main_menu"))
async def back_to_main(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    from keyboards.keyboards import main_menu
    await message.answer("🏠 Головне меню", reply_markup=main_menu)

# --- 1. СКИНУТИ МОЮ СТАТИСТИКУ ---
@router.message(Command("reset_me"))
async def reset_me_cmd(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    await reset_player_stats(message.from_user.id)
    await message.answer("✅ Вашу статистику повністю скинуто.")

# --- 2. ПІДГЛЯНУТИ (PEEK) ---
@router.message(Command("peek"))
async def peek_cmd(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    
    # Знаходимо кімнату адміна
    found_room = None
    for room in rooms.values():
        if message.from_user.id in room.players:
            found_room = room
            break
    
    if not found_room or not found_room.game_started:
        await message.answer("❌ Ви не в активній грі.")
        return

    info = []
    info.append(f"📍 Локація: <b>{found_room.location}</b>")
    
    spy_id = found_room.spy_id
    spy_name = found_room.players.get(spy_id, "Unknown")
    spy_call = found_room.player_callsigns.get(spy_id, "???")
    
    info.append(f"🕵️ Шпигун: <b>{spy_call}</b> ({spy_name})")
    
    await message.answer("\n".join(info), parse_mode="HTML")

# --- 3. РОЗСИЛКА (BROADCAST) ---
@router.message(Command("broadcast"))
async def broadcast_start(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    await message.answer("✍️ Напишіть текст повідомлення для розсилки (або /cancel):")
    await state.set_state(AdminStates.waiting_for_broadcast)

@router.message(AdminStates.waiting_for_broadcast)
async def broadcast_process(message: types.Message, state: FSMContext):
    if message.text.startswith("/"): 
        await state.clear()
        await message.answer("❌ Скасовано (введено команду).")
        return

    text = message.text
    users = await get_all_users()
    count = 0
    
    status_msg = await message.answer(f"🚀 Розсилка на {len(users)} користувачів...")
    
    for user_id in users:
        try:
            await bot.send_message(user_id, f"📢 <b>ОГОЛОШЕННЯ:</b>\n\n{text}", parse_mode="HTML")
            count += 1
            await asyncio.sleep(0.05) 
        except Exception:
            pass
            
    await status_msg.edit_text(f"✅ Розсилку завершено. Отримали: {count}")
    await state.clear()

# --- 4. БАН (BAN) ---
@router.message(Command("ban"))
async def ban_start(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    
    # Якщо це реплаєм
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        await update_player(target_id, banned_until=-1) # Перманент
        await message.answer(f"🚫 Користувача {target_id} забанено.")
        return

    await message.answer("✍️ Введіть ID користувача для бану:")
    await state.set_state(AdminStates.waiting_for_ban_id)

@router.message(AdminStates.waiting_for_ban_id)
async def ban_process(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Це не ID. Скасовано.")
        await state.clear()
        return
        
    target_id = int(message.text)
    await update_player(target_id, banned_until=-1) # -1 = назавжди
    await message.answer(f"🚫 Користувача {target_id} забанено назавжди.")
    await state.clear()

# --- 5. РОЗБАН (UNBAN) ---
@router.message(Command("unban"))
async def unban_start(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    await message.answer("✍️ Введіть ID користувача для розбану:")
    await state.set_state(AdminStates.waiting_for_unban_id)

@router.message(AdminStates.waiting_for_unban_id)
async def unban_process(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Це не ID.")
        await state.clear()
        return
        
    target_id = int(message.text)
    await update_player(target_id, banned_until=0)
    await message.answer(f"✅ Користувача {target_id} розбанено.")
    await state.clear()

# --- 6. WHOIS (ІНФО ПРО ЮЗЕРА) ---
@router.message(Command("whois"))
async def whois_start(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    await message.answer("✍️ Введіть ID користувача:")
    await state.set_state(AdminStates.waiting_for_whois_id)

@router.message(AdminStates.waiting_for_whois_id)
async def whois_process(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("❌ Це не ID.")
        await state.clear()
        return
        
    target_id = int(message.text)
    stats = await get_player_stats(target_id)
    
    if not stats:
        await message.answer("❌ Користувача не знайдено в базі.")
    else:
        await message.answer(
            f"👤 <b>ID:</b> {target_id}\n"
            f"🏷 <b>User:</b> {stats['username']}\n"
            f"🎮 <b>Ігор:</b> {stats['games_played']}\n"
            f"🏆 <b>Spy/Civ:</b> {stats['spy_wins']} / {stats['civilian_wins']}\n"
            f"⭐ <b>XP/Level:</b> {stats['total_xp']} (Lvl {stats['level_info'][0]})",
            parse_mode="HTML"
        )
    await state.clear()

# --- 7. ТЕХ. РОБОТИ ---
@router.message(Command("maintenance_on"))
async def maintenance_on_cmd(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    set_maintenance_mode(True)
    await message.answer("🟠 Тех. роботи УВІМКНЕНО. Гравці не можуть грати.")

@router.message(Command("maintenance_off"))
async def maintenance_off_cmd(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    set_maintenance_mode(False)
    await message.answer("🟢 Тех. роботи ВИМКНЕНО. Гра доступна.")

# --- 8. ФАЙЛИ БД І ЛОГІВ ---
@router.message(Command("get_db"))
async def get_db_file(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    if os.path.exists(DB_PATH):
        await message.answer_document(FSInputFile(DB_PATH))
    else:
        await message.answer("❌ Файл БД не знайдено.")

@router.message(Command("get_logs"))
async def get_logs_file(message: types.Message, state: FSMContext):
    if not _admin_only(message): return
    await state.clear()
    
    # Оскільки Render пише логи в консоль, ми створимо текстовий файл зі звітом
    log_content = "Logs are stored in Render Dashboard (Events/Logs tab).\nCurrently active rooms: " + str(len(rooms))
    
    with open("bot_status.txt", "w") as f:
        f.write(log_content)
        
    await message.answer_document(FSInputFile("bot_status.txt"))