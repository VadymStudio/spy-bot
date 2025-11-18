import logging
import asyncio
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import FSInputFile

from config import (
    set_maintenance_mode,
    is_maintenance_mode,
    set_maintenance_task,
    get_maintenance_task,
    get_active_users,
    DB_PATH,
    rooms,
)
from utils.helpers import is_admin, parse_ban_time, compute_ban_until
from database.crud import update_player, get_player, get_recent_games, get_player_stats
from bot import bot

router = Router()
logger = logging.getLogger(__name__)


def _admin_only(message: types.Message) -> bool:
    return is_admin(message.from_user.id)


@router.message(Command("ping"))
async def admin_ping(message: types.Message):
    if not _admin_only(message):
        return
    await message.answer("🏓 pong (admin)")


@router.message(Command("maintenance_on"))
async def maintenance_on_cmd(message: types.Message):
    if not _admin_only(message):
        return
    if is_maintenance_mode():
        await message.answer("⚠️ Режим обслуговування вже увімкнено.")
        return
    set_maintenance_mode(True)
    await message.answer("🟠 Увімкнено режим обслуговування. Нові команди для звичайних користувачів тимчасово обмежені.")


@router.message(Command("maintenance_off"))
async def maintenance_off_cmd(message: types.Message):
    if not _admin_only(message):
        return
    if not is_maintenance_mode():
        await message.answer("ℹ️ Режим обслуговування вже вимкнено.")
        return
    set_maintenance_mode(False)
    await message.answer("🟢 Режим обслуговування вимкнено.")


@router.message(Command("ban"))
async def ban_user_cmd(message: types.Message):
    """/ban <user_id> <duration> [reason] або через reply з <duration> [reason].
    duration: 10m, 2h, 3d, perm
    """
    if not _admin_only(message):
        return

    args = message.text.split(maxsplit=3)
    target_id = None
    duration_str = None
    reason = None

    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        if len(args) >= 2:
            duration_str = args[1]
        if len(args) >= 3:
            reason = args[2]
    else:
        if len(args) < 3:
            await message.answer("❗ Використання: /ban <user_id> <duration> [reason] або reply на повідомлення користувача з /ban <duration> [reason]")
            return
        try:
            target_id = int(args[1])
        except ValueError:
            await message.answer("❗ Невірний user_id")
            return
        duration_str = args[2]
        if len(args) >= 4:
            reason = args[3]

    duration = parse_ban_time(duration_str or "")
    if duration is None:
        await message.answer("❗ Невірний формат часу. Використовуйте: 10m, 2h, 3d або perm")
        return
    banned_until = compute_ban_until(duration)

    # Перевіримо, що користувач існує (або створимо запис мінімально?)
    player = await get_player(target_id)
    if not player:
        # Якщо юзера ще не було в БД, створювати запис для бана не обов'язково, але зробимо оновлення умовно
        await update_player(target_id, banned_until=banned_until)
    else:
        await update_player(target_id, banned_until=banned_until)

    reason_text = f" Причина: {reason}" if reason else ""
    await message.answer(f"🔒 Користувача <code>{target_id}</code> заблоковано до <code>{banned_until}</code>.{reason_text}", parse_mode="HTML")


@router.message(Command("unban"))
async def unban_user_cmd(message: types.Message):
    if not _admin_only(message):
        return
    args = message.text.split(maxsplit=1)
    target_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
    else:
        if len(args) < 2:
            await message.answer("❗ Використання: /unban <user_id> або reply на користувача з /unban")
            return
        try:
            target_id = int(args[1])
        except ValueError:
            await message.answer("❗ Невірний user_id")
            return

    await update_player(target_id, banned_until=0)
    await message.answer(f"🔓 Користувача <code>{target_id}</code> розблоковано.", parse_mode="HTML")


async def _broadcast(text: str) -> None:
    """Шле повідомлення всім активним користувачам."""
    users = list(get_active_users())
    for uid in users:
        try:
            await bot.send_message(uid, text)
        except Exception:
            # Ігноруємо помилки доставки окремим користувачам
            pass


@router.message(Command("maintenance_timer"))
async def maintenance_timer_cmd(message: types.Message):
    """/maintenance_timer <minutes> [message]

    Стартує відлік з розсилкою попереджень кожну хвилину. В кінці вмикає maintenance.
    """
    if not _admin_only(message):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.answer("❗ Використання: /maintenance_timer <minutes> [message]")
        return
    try:
        minutes = int(args[1])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Невірне значення хвилин")
        return
    extra = args[2] if len(args) >= 3 else "Технічні роботи"

    # Якщо вже є таймер — скасовуємо
    current = get_maintenance_task()
    if current and not current.done():
        current.cancel()

    async def _run_timer(total_minutes: int, note: str):
        try:
            await _broadcast(f"🟠 Заплановані технічні роботи через {total_minutes} хв. {note}")
            remaining = total_minutes
            while remaining > 0:
                await asyncio.sleep(60)
                remaining -= 1
                if remaining in (10, 5, 3, 2, 1):
                    await _broadcast(f"🟠 Технічні роботи через {remaining} хв.")
            # Вмикаємо maintenance
            set_maintenance_mode(True)
            await _broadcast("🔧 Технічні роботи розпочато. Деякі функції недоступні.")
        except asyncio.CancelledError:
            await _broadcast("ℹ️ Таймер технічних робіт скасовано.")
            raise

    task = asyncio.create_task(_run_timer(minutes, extra))
    set_maintenance_task(task)
    await message.answer(f"⏱️ Таймер технічних робіт запущено на {minutes} хв.")


@router.message(Command("maintenance_cancel"))
async def maintenance_cancel_cmd(message: types.Message):
    if not _admin_only(message):
        return
    current = get_maintenance_task()
    if not current or current.done():
        await message.answer("ℹ️ Активного таймера немає.")
        return
    current.cancel()
    await message.answer("🛑 Таймер технічних робіт скасовано.")


# --- Інші адмін-команди ---

@router.message(Command("maintenance_status"))
async def maintenance_status_cmd(message: types.Message):
    if not _admin_only(message):
        return
    status = "ON" if is_maintenance_mode() else "OFF"
    running = get_maintenance_task()
    timer = "⏱️ активний" if running and not running.done() else "без таймера"
    await message.answer(f"🔧 Maintenance: <b>{status}</b> ({timer})", parse_mode="HTML")


@router.message(Command("broadcast"))
async def broadcast_cmd(message: types.Message):
    if not _admin_only(message):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("❗ Використання: /broadcast <text>")
        return
    text = args[1].strip()
    users = list(get_active_users())
    ok = 0
    for uid in users:
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            pass
    await message.answer(f"📣 Розсилка надіслана {ok}/{len(users)} користувачам.")


@router.message(Command("get_db"))
async def get_db_cmd(message: types.Message):
    if not _admin_only(message):
        return
    try:
        doc = FSInputFile(DB_PATH)
        await message.answer_document(doc, caption=f"DB file: {DB_PATH}")
    except Exception as e:
        await message.answer(f"❌ Не вдалося надіслати БД: {e}")


@router.message(Command("recent_games"))
async def recent_games_cmd(message: types.Message):
    if not _admin_only(message):
        return
    rows = await get_recent_games(limit=10)
    if not rows:
        await message.answer("Поки немає записів ігор.")
        return
    lines = [
        f"#{r['id']}: token={r['room_token']}, loc={r['location']}, spy={r['spy_id']}, winner={r['winner']}, ts={r['timestamp']}"
        for r in rows
    ]
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await message.answer(f"<code>{text}</code>", parse_mode="HTML")


@router.message(Command("whois"))
async def whois_cmd(message: types.Message):
    if not _admin_only(message):
        return
    args = message.text.split(maxsplit=1)
    target_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
    elif len(args) == 2:
        try:
            target_id = int(args[1])
        except ValueError:
            await message.answer("❗ Невірний user_id")
            return
    else:
        await message.answer("❗ Використання: /whois <user_id> або reply на користувача")
        return
    stats = await get_player_stats(target_id)
    if not stats:
        await message.answer("Немає записів про цього користувача.")
        return
    await message.answer(
        (
            f"👤 <b>{target_id}</b>\n"
            f"🎮 Ігор: <b>{stats['games_played']}</b>\n"
            f"🕵️ Spy W: <b>{stats['spy_wins']}</b>\n"
            f"👥 Civ W: <b>{stats['civilian_wins']}</b>\n"
            f"⭐ XP: <b>{stats['total_xp']}</b>\n"
            f"🚫 banned_until: <code>{stats['banned_until']}</code>"
        ),
        parse_mode="HTML"
    )


@router.message(Command("reset_state"))
async def reset_state_cmd(message: types.Message):
    if not _admin_only(message):
        return
    try:
        rooms.clear()
        await message.answer("♻️ Скинуто стан кімнат (in-memory).")
    except Exception as e:
        await message.answer(f"❌ Помилка скидання стану: {e}")
