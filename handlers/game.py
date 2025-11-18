import logging
import asyncio
import random
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from datetime import datetime

from bot import bot
from config import matchmaking_queue, add_active_user, rooms, LOCATIONS, GAME_DURATION_SECONDS, XP_CIVILIAN_WIN, XP_SPY_WIN
from keyboards.keyboards import (
    in_queue_menu,
    in_lobby_menu,
    main_menu,
    get_early_vote_keyboard,
    get_voting_keyboard,
    get_locations_keyboard,
)
from utils.helpers import maintenance_blocked, generate_room_token
from utils.matchmaking import enqueue_user, dequeue_user
from utils.states import PlayerState
from database.crud import update_player_stats

router = Router()
logger = logging.getLogger(__name__)


@router.message(F.text == "🎮 Знайти Гру")
async def find_match(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    user_id = message.from_user.id
    add_active_user(user_id)
    enqueue_user(user_id)
    await message.answer("🔍 Шукаємо гру...", reply_markup=in_queue_menu)


@router.message(F.text == "❌ Скасувати Пошук")
async def cancel_search(message: types.Message):
    user_id = message.from_user.id
    if user_id in matchmaking_queue:
        dequeue_user(user_id)
        await message.answer("❌ Пошук скасовано.")
    else:
        await message.answer("ℹ️ Ви не в черзі.")


# ------------------- Ручні кімнати -------------------

@router.message(F.text == "🚪 Створити Кімнату")
async def create_room_cmd(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    user = message.from_user
    # Генеруємо унікальний токен
    token = generate_room_token()
    while token in rooms:
        token = generate_room_token()
    # Створюємо кімнату та додаємо творця
    from database.models import Room
    room = Room(token=token, admin_id=user.id, last_activity=int(datetime.now().timestamp()))
    room.players[user.id] = user.full_name or (user.username or str(user.id))
    rooms[token] = room
    await message.answer(
        (
            "🚪 Створено кімнату!\n"
            f"🔑 Код кімнати: <code>{token}</code>\n"
            f"👥 Гравців: 1/6\n\n"
            "Поділись кодом, щоб інші могли приєднатися."
        ),
        parse_mode="HTML",
        reply_markup=in_lobby_menu,
    )


@router.message(F.text == "🤝 Приєднатися")
async def join_room_ask_token(message: types.Message, state: FSMContext):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    await message.answer("🔢 Введіть код кімнати:")
    await state.set_state(PlayerState.waiting_for_token)


@router.message(PlayerState.waiting_for_token)
async def join_room_process_token(message: types.Message, state: FSMContext):
    user = message.from_user
    token = (message.text or "").strip().upper()
    if token not in rooms:
        await message.answer("❌ Кімнату не знайдено. Перевірте код.")
        return
    room = rooms[token]
    # Перевіряємо розмір кімнати
    if len(room.players) >= 6:
        await message.answer("❌ Кімната вже заповнена (6/6).")
        await state.clear()
        return
    # Додаємо гравця
    if user.id in room.players:
        await message.answer("ℹ️ Ви вже в цій кімнаті.")
    else:
        room.players[user.id] = user.full_name or (user.username or str(user.id))
        room.last_activity = int(datetime.now().timestamp())
        # Сповістити інших
        for pid in room.players:
            if pid == user.id:
                continue
            try:
                await bot.send_message(pid, f"👤 {user.full_name} приєднався до кімнати. 👥 {len(room.players)}/6")
            except Exception:
                pass
        await message.answer(
            f"✅ Ви приєднались до кімнати {token}. 👥 {len(room.players)}/6",
            reply_markup=in_lobby_menu,
        )
    await state.clear()


@router.message(F.text == "🚪 Покинути Лобі")
async def leave_lobby(message: types.Message):
    user = message.from_user
    # Знайти кімнату, де є користувач
    target_token = None
    for t, r in rooms.items():
        if user.id in r.players and not r.game_started:
            target_token = t
            break
    if not target_token:
        await message.answer("ℹ️ Ви не в лобі жодної кімнати.", reply_markup=main_menu)
        return
    room = rooms[target_token]
    username = room.players.get(user.id, "Гравець")
    # Видалити користувача з кімнати
    if user.id in room.players:
        del room.players[user.id]
    # Якщо кімната спорожніла — прибрати її
    if not room.players:
        del rooms[target_token]
        await message.answer("🚪 Ви вийшли. Кімнату закрито (порожня).", reply_markup=main_menu)
        return
    # Якщо вийшов адмін — призначити нового (першого ж гравця)
    if user.id == room.admin_id:
        room.admin_id = next(iter(room.players))
        try:
            await bot.send_message(room.admin_id, "👑 Ви тепер адміністратор кімнати.")
        except Exception:
            pass
    # Сповістити інших
    for pid in list(room.players.keys()):
        try:
            await bot.send_message(pid, f"🚪 {username} покинув лобі. 👥 {len(room.players)}/6")
        except Exception:
            pass
    await message.answer("✅ Ви покинули лобі.", reply_markup=main_menu)


# ------------------- Старт гри -------------------

def _find_user_room(user_id: int):
    for t, r in rooms.items():
        if user_id in r.players:
            return t, r
    return None, None


@router.message(Command("start_game"))
async def start_game_cmd(message: types.Message):
    if maintenance_blocked(message.from_user.id):
        await message.answer("🟠 Режим обслуговування. Спробуйте пізніше.")
        return
    token, room = _find_user_room(message.from_user.id)
    if not room:
        await message.answer("ℹ️ Ви не у кімнаті.")
        return
    if room.game_started:
        await message.answer("ℹ️ Гра вже розпочата в цій кімнаті.")
        return
    if message.from_user.id != room.admin_id:
        await message.answer("❌ Лише адміністратор кімнати може почати гру.")
        return
    if len(room.players) < 3:
        await message.answer("❌ Потрібно мінімум 3 гравці для старту гри.")
        return

    # Призначаємо ролі
    players = list(room.players.keys())
    spy_id = random.choice(players)
    location = random.choice(LOCATIONS)
    room.spy_id = spy_id
    room.location = location
    room.player_roles = {uid: ("spy" if uid == spy_id else "civilian") for uid in players}
    room.game_started = True
    room.end_time = int(datetime.now().timestamp()) + GAME_DURATION_SECONDS

    # Розсилаємо приватно ролі
    for uid in players:
        try:
            if uid == spy_id:
                await bot.send_message(uid, "🕵️ Ти ШПИГУН! Вигадай локацію, не виказавши себе.")
            else:
                await bot.send_message(uid, f"👥 Ти ЦИВІЛЬНИЙ. Локація: <b>{location}</b>", parse_mode="HTML")
        except Exception:
            pass

    # Повідомлення в лобі
    for uid in players:
        try:
            await bot.send_message(uid, "▶️ Гру розпочато! Таймер: 20 хв. Використовуйте '❓ Моя роль' у разі потреби.")
        except Exception:
            pass

    # Запускаємо таймер раунду
    async def _round_timer(tok: str, sec: int):
        try:
            await asyncio.sleep(sec)
            # Якщо гра ще активна — завершуємо на користь шпигуна (за замовчуванням)
            await end_game(tok, spy_won=True, reason="⏱️ Час вийшов. Перемога шпигуна.")
        except asyncio.CancelledError:
            return

    room._timer_task = asyncio.create_task(_round_timer(token, GAME_DURATION_SECONDS))


@router.message(F.text == "❓ Моя роль")
async def my_role(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    role = room.player_roles.get(message.from_user.id)
    if role == "spy":
        await message.answer("🕵️ Ти ШПИГУН.")
    else:
        await message.answer(f"👥 Ти ЦИВІЛЬНИЙ. Локація: <b>{room.location}</b>", parse_mode="HTML")


async def end_game(token: str, spy_won: bool, reason: str, grant_xp: bool = True):
    room = rooms.get(token)
    if not room or not room.game_started:
        return
    room.game_started = False
    # Скасовуємо таймер, якщо є
    task = getattr(room, "_timer_task", None)
    if task and not task.done():
        task.cancel()
    # Повідомити всіх
    players = list(room.players.keys())
    for uid in players:
        try:
            await bot.send_message(uid, f"🏁 Гру завершено. {reason}")
        except Exception:
            pass
    # Нарахувати XP (опційно)
    if grant_xp:
        for uid in players:
            is_spy = (uid == room.spy_id)
            winner = (spy_won and is_spy) or ((not spy_won) and (not is_spy))
            try:
                await update_player_stats(uid, is_spy=is_spy, is_winner=winner)
            except Exception:
                pass


# ------------------- Дострокове завершення (голосування) -------------------

@router.message(F.text == "🗳️ Достр. Голосування")
async def early_vote_request(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    # Скидаємо попередні голоси та запускаємо вікно голосування
    room.early_votes = set()
    voters = list(room.players.keys())
    for uid in voters:
        try:
            await bot.send_message(uid, "🗳️ Достроково завершити гру?", reply_markup=get_early_vote_keyboard(token))
        except Exception:
            pass

    async def _finalize():
        await asyncio.sleep(30)  # 30с на голосування
        # Якщо гри вже немає — вихід
        if token not in rooms or not rooms[token].game_started:
            return
        votes_yes = len(room.early_votes)
        total = len(room.players)
        if votes_yes > total / 2:
            await end_game(token, spy_won=False, reason="🗳️ Гру завершено достроково більшістю.", grant_xp=False)
        else:
            # Повідомити результат
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, f"ℹ️ Дострокове завершення не прийнято ({votes_yes}/{total}).")
                except Exception:
                    pass

    room._early_vote_task = asyncio.create_task(_finalize())


@router.callback_query(F.data.startswith("early_vote_") )
async def early_vote_callback(callback: types.CallbackQuery):
    try:
        parts = callback.data.split(":")
        action = parts[0]  # early_vote_yes / early_vote_no
        token = parts[1]
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id not in room.players:
        await callback.answer()
        return
    if action == "early_vote_yes":
        room.early_votes.add(callback.from_user.id)
        await callback.answer("Ви проголосували: Так")
    else:
        # Явне 'ні' не рахуємо, просто підтверджуємо
        await callback.answer("Ви проголосували: Ні")


# ------------------- Голосування проти гравця -------------------

@router.message(Command("vote"))
async def start_vote(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    # Скидаємо голоси
    room.player_votes = {}
    players = dict(room.players)
    # Надсилаємо кожному його клавіатуру без себе
    for voter_id in players.keys():
        try:
            kb = get_voting_keyboard(token, players, voter_id)
            await bot.send_message(voter_id, "🗳️ Кого ви підозрюєте?", reply_markup=kb)
        except Exception:
            pass

    # Запускаємо таймер підсумку
    async def _finalize_vote():
        await asyncio.sleep(45)
        if token not in rooms or not rooms[token].game_started:
            return
        # Підрахунок
        tally = {}
        for v in room.player_votes.values():
            tally[v] = tally.get(v, 0) + 1
        if not tally:
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, "ℹ️ Голосування не відбулось.")
                except Exception:
                    pass
            return
        # Знайти максимум
        max_votes = max(tally.values())
        top = [pid for pid, cnt in tally.items() if cnt == max_votes]
        if len(top) != 1:
            # Нічия
            for uid in list(room.players.keys()):
                try:
                    await bot.send_message(uid, "ℹ️ Нічия. Нікого не вигнали.")
                except Exception:
                    pass
            return
        target = top[0]
        # Оголосити вигнання
        for uid in list(room.players.keys()):
            try:
                await bot.send_message(uid, f"🚷 Вигнано гравця: {room.players.get(target, str(target))}")
            except Exception:
                pass
        # Перевірка на шпигуна
        if target == room.spy_id:
            await end_game(token, spy_won=False, reason="✅ Шпигуна викрито! Перемога цивільних.")
            return
        # Інакше прибираємо гравця і гра триває
        room.players.pop(target, None)
        room.player_roles.pop(target, None)
        room.player_votes = {}
        # Якщо залишилось <3 — завершити на користь шпигуна (неможливо продовжувати)
        if len(room.players) < 3:
            await end_game(token, spy_won=True, reason="👥 Занадто мало гравців для продовження. Перемога шпигуна.")

    room._voting_task = asyncio.create_task(_finalize_vote())


@router.callback_query(F.data.startswith("vote:"))
async def vote_callback(callback: types.CallbackQuery):
    try:
        _, token, target_str = callback.data.split(":", 2)
        target_id = int(target_str)
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id not in room.players:
        await callback.answer()
        return
    # Зберігаємо голос
    room.player_votes[callback.from_user.id] = target_id
    await callback.answer("Ваш голос враховано")


@router.callback_query(F.data.startswith("vote_cancel:"))
async def vote_cancel_callback(callback: types.CallbackQuery):
    await callback.answer("Скасовано")


# ------------------- Вгадування локації шпигуном -------------------

@router.message(Command("spy_guess"))
async def spy_guess(message: types.Message):
    token, room = _find_user_room(message.from_user.id)
    if not room or not room.game_started:
        await message.answer("ℹ️ Зараз ви не у грі.")
        return
    if message.from_user.id != room.spy_id:
        await message.answer("❌ Лише шпигун може робити здогад локації.")
        return
    kb = get_locations_keyboard(token, LOCATIONS, columns=3)
    await message.answer("📍 Оберіть локацію, яку ви вважаєте правильною:", reply_markup=kb)


@router.callback_query(F.data.startswith("guess:"))
async def spy_guess_callback(callback: types.CallbackQuery):
    try:
        _, token, location = callback.data.split(":", 2)
    except Exception:
        await callback.answer()
        return
    room = rooms.get(token)
    if not room or not room.game_started:
        await callback.answer()
        return
    if callback.from_user.id != room.spy_id:
        await callback.answer("Це діє лише для шпигуна")
        return
    if location == room.location:
        await callback.answer("✅ Вірно!")
        await end_game(token, spy_won=True, reason=f"🕵️ Шпигун вгадав локацію: {location}")
    else:
        await callback.answer("❌ Невірно")
