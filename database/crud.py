import aiosqlite
import logging
import os
from typing import Optional, Dict, Any, List
from .models import Player, get_level_from_xp
from config import DB_PATH

logger = logging.getLogger(__name__)

def _ensure_db_dir(path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

async def init_db():
    _ensure_db_dir(DB_PATH)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0,
                banned_until INTEGER DEFAULT 0
            )
            '''
        )
        # Міграція для старих баз
        try:
            await db.execute("ALTER TABLE players ADD COLUMN level INTEGER DEFAULT 1")
        except Exception:
            pass
            
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

async def get_player(user_id: int) -> Optional[Player]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                lvl = row['level'] if 'level' in row.keys() and row['level'] else 1
                return Player(
                    user_id=row['user_id'],
                    username=row['username'],
                    total_xp=row['total_xp'],
                    level=lvl,
                    games_played=row['games_played'],
                    spy_wins=row['spy_wins'],
                    civilian_wins=row['civilian_wins'],
                    banned_until=row['banned_until']
                )
            return None

async def get_or_create_player(user_id: int, username: str = "") -> Player:
    player = await get_player(user_id)
    if player:
        return player
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO players (user_id, username, level) VALUES (?, ?, 1)",
            (user_id, username)
        )
        await db.commit()
    return Player(user_id, username)

async def update_player(user_id: int, **kwargs) -> None:
    if not kwargs: return
    set_parts = []
    values = []
    for key, value in kwargs.items():
        set_parts.append(f"{key} = ?")
        values.append(value)
    values.append(user_id)
    sql = f"UPDATE players SET {', '.join(set_parts)} WHERE user_id = ?"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, tuple(values))
        await db.commit()

# --- НОВІ ФУНКЦІЇ ---

async def reset_player_stats(user_id: int) -> None:
    """Скидає статистику конкретного гравця в 0."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE players 
            SET total_xp = 0, level = 1, games_played = 0, spy_wins = 0, civilian_wins = 0 
            WHERE user_id = ?
            """, 
            (user_id,)
        )
        await db.commit()

async def get_all_users() -> List[int]:
    """Повертає список ID всіх гравців (для розсилки)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM players") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

async def get_player_stats(user_id: int) -> Optional[Dict[str, Any]]:
    player = await get_player(user_id)
    if not player: return None
    
    # --- АВТО-ФІКС РІВНЯ ---
    # Якщо в базі рівень 1, а XP вистачає на 5, ми це виправимо прямо зараз
    calc_level, cur_xp, need = get_level_from_xp(player.total_xp)
    
    if calc_level != player.level:
        await update_player(user_id, level=calc_level)
        player.level = calc_level
        # Оновлюємо значення для return
        lvl, cur, need = calc_level, cur_xp, need
    else:
        lvl, cur, need = player.level_info

    return {
        'user_id': player.user_id,
        'username': player.username,
        'games_played': player.games_played,
        'spy_wins': player.spy_wins,
        'civilian_wins': player.civilian_wins,
        'total_xp': player.total_xp,
        'level_info': (lvl, cur, need),
        'banned_until': player.banned_until
    }

async def update_player_stats(user_id: int, is_spy: bool, is_winner: bool) -> tuple[int, int, int]:
    player = await get_or_create_player(user_id)
    old_level = player.level
    
    xp_gain = 0
    if is_winner:
        xp_gain = 20 if is_spy else 10
        
    new_total_xp = player.total_xp + xp_gain
    new_level, current_progress_xp, xp_needed = get_level_from_xp(new_total_xp)
    
    async with aiosqlite.connect(DB_PATH) as db:
        sql = "UPDATE players SET total_xp = ?, level = ?, games_played = games_played + 1"
        if is_winner:
            if is_spy: sql += ", spy_wins = spy_wins + 1"
            else: sql += ", civilian_wins = civilian_wins + 1"
        sql += " WHERE user_id = ?"
        await db.execute(sql, (new_total_xp, new_level, user_id))
        await db.commit()
        
    return old_level, current_progress_xp, xp_needed

async def get_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM game_logs ORDER BY timestamp DESC LIMIT ?", (limit,)) as cursor:
            return [dict(row) async for row in cursor]