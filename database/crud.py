import aiosqlite
import logging
import os
from typing import Optional, Dict, Any, List
from .models import Player
from config import DB_PATH

logger = logging.getLogger(__name__)

def _ensure_db_dir(path: str) -> None:
    """Створює директорію для БД, якщо її ще не існує."""
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

# Ініціалізація бази даних
async def init_db():
    """Ініціалізує базу даних та створює необхідні таблиці, якщо вони не існують."""
    _ensure_db_dir(DB_PATH)
    async with aiosqlite.connect(DB_PATH) as db:
        # Налаштування SQLite для кращої стабільності на Render
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0,
                banned_until INTEGER DEFAULT 0
            )
            '''
        )

        # Додаткові таблиці (логи ігор)
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS game_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_token TEXT,
                location TEXT,
                spy_id INTEGER,
                players TEXT,
                winner TEXT,
                timestamp INTEGER
            )
            '''
        )

        await db.commit()
    logger.info(f"Database initialized at {DB_PATH}")

# Операції з гравцями
async def get_player(user_id: int) -> Optional[Player]:
    """Отримує гравця за ID. Якщо не існує - повертає None."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return Player(*row)
            return None

async def get_or_create_player(user_id: int, username: str = None) -> Player:
    """Отримує гравця за ID або створює нового, якщо не існує."""
    player = await get_player(user_id)
    if player is None:
        # Створюємо нового гравця
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO players (user_id, username) 
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
                """,
                (user_id, username)
            )
            await db.commit()
        player = await get_player(user_id)
    elif username and player.username != username:
        # Оновлюємо ім'я користувача, якщо воно змінилося
        await update_player(user_id, username=username)
        player.username = username
    return player

async def update_player(
    user_id: int,
    username: str = None,
    total_xp: int = None,
    games_played: int = None,
    spy_wins: int = None,
    civilian_wins: int = None,
    banned_until: int = None
) -> bool:
    """Оновлює дані гравця. Повертає True, якщо оновлення пройшло успішно."""
    updates = []
    params = []
    
    if username is not None:
        updates.append("username = ?")
        params.append(username)
    if total_xp is not None:
        updates.append("total_xp = ?")
        params.append(total_xp)
    if games_played is not None:
        updates.append("games_played = ?")
        params.append(games_played)
    if spy_wins is not None:
        updates.append("spy_wins = ?")
        params.append(spy_wins)
    if civilian_wins is not None:
        updates.append("civilian_wins = ?")
        params.append(civilian_wins)
    if banned_until is not None:
        updates.append("banned_until = ?")
        params.append(banned_until)
    
    if not updates:
        return False
    
    params.append(user_id)  # Додаємо user_id в кінець для WHERE умови
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE players SET {', '.join(updates)} WHERE user_id = ?",
            params
        )
        await db.commit()
    return True

async def update_player_stats(
    user_id: int,
    is_spy: bool,
    is_winner: bool
) -> bool:
    """Оновлює статистику гравця після гри."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Отримуємо поточну статистику
        async with db.execute(
            "SELECT total_xp, games_played, spy_wins, civilian_wins FROM players WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            stats = await cursor.fetchone()
            
            if not stats:
                logger.warning(f"Player {user_id} not found when updating stats")
                return False
                
            total_xp, games_played, spy_wins, civilian_wins = stats
            
            # Оновлюємо статистику
            games_played += 1
            
            if is_winner:
                if is_spy:
                    spy_wins += 1
                    total_xp += 20  # XP за перемогу як шпигун
                else:
                    civilian_wins += 1
                    total_xp += 10  # XP за перемогу як цивільний
            
            # Зберігаємо оновлену статистику
            await db.execute(
                """
                UPDATE players 
                SET total_xp = ?, games_played = ?, spy_wins = ?, civilian_wins = ?
                WHERE user_id = ?
                """,
                (total_xp, games_played, spy_wins, civilian_wins, user_id)
            )
            await db.commit()
            return True

async def log_game(
    room_token: str,
    location: str,
    spy_id: int,
    players: List[Dict[str, Any]],
    winner: str
) -> int:
    """Логує гру в базу даних. Повертає ID створеного запису."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO game_logs (room_token, location, spy_id, players, winner, timestamp)
            VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'))
            """,
            (room_token, location, spy_id, str(players), winner)
        )
        await db.commit()
        return cursor.lastrowid

async def get_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    """Отримує останні ігри з логів."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT * FROM game_logs 
            ORDER BY timestamp DESC 
            LIMIT ?
            """,
            (limit,)
        ) as cursor:
            return [dict(row) async for row in cursor]

async def get_player_stats(user_id: int) -> Optional[Dict[str, Any]]:
    """Отримує статистику гравця."""
    player = await get_player(user_id)
    if not player:
        return None
        
    return {
        'user_id': player.user_id,
        'username': player.username,
        'total_xp': player.total_xp,
        'games_played': player.games_played,
        'spy_wins': player.spy_wins,
        'civilian_wins': player.civilian_wins,
        'banned_until': player.banned_until
    }
