import asyncpg
import logging
import os
import asyncio
from typing import Optional, Dict, Any, List
from .models import Player, get_level_from_xp

logger = logging.getLogger(__name__)

# Глобальний пул з'єднань
pool: Optional[asyncpg.Pool] = None

async def init_db():
    """Ініціалізація підключення до PostgreSQL та створення таблиць."""
    global pool
    
    # Отримуємо налаштування з змінних оточення (які ми прописали в docker-compose)
    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASSWORD", "supersecretpassword")
    db_name = os.getenv("DB_NAME", "spygame")

    logger.info(f"Connecting to Postgres at {db_host}...")

    # Чекаємо поки БД прокинеться (важливо для Docker)
    for i in range(10):
        try:
            pool = await asyncpg.create_pool(
                user=db_user,
                password=db_pass,
                database=db_name,
                host=db_host
            )
            break
        except Exception as e:
            logger.warning(f"DB not ready yet, retrying... ({e})")
            await asyncio.sleep(2)
    
    if not pool:
        raise Exception("Could not connect to Database")

    async with pool.acquire() as conn:
        # Створення таблиць
        # BIGINT - важливо для Telegram ID
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0,
                banned_until BIGINT DEFAULT 0
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS game_logs (
                id SERIAL PRIMARY KEY,
                room_token TEXT,
                location TEXT,
                spy_id BIGINT,
                players TEXT,
                winner TEXT,
                timestamp BIGINT
            )
        ''')
    
    logger.info("✅ Database initialized (PostgreSQL).")

async def get_player(user_id: int) -> Optional[Player]:
    if not pool: await init_db()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM players WHERE user_id = $1", user_id)
        
        if row:
            return Player(
                user_id=row['user_id'],
                username=row['username'],
                total_xp=row['total_xp'],
                level=row['level'],
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
    
    async with pool.acquire() as conn:
        # ON CONFLICT DO NOTHING - захист від дублікатів
        await conn.execute(
            """
            INSERT INTO players (user_id, username, level) 
            VALUES ($1, $2, 1)
            ON CONFLICT (user_id) DO NOTHING
            """,
            user_id, username
        )
    return await get_player(user_id)

async def update_player(user_id: int, **kwargs) -> None:
    if not kwargs: return
    
    set_parts = []
    values = []
    for i, (key, value) in enumerate(kwargs.items(), start=1):
        set_parts.append(f"{key} = ${i}")
        values.append(value)
    
    # user_id буде останнім параметром
    values.append(user_id)
    
    sql = f"UPDATE players SET {', '.join(set_parts)} WHERE user_id = ${len(values)}"
    
    async with pool.acquire() as conn:
        await conn.execute(sql, *values)

async def reset_player_stats(user_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE players 
            SET total_xp = 0, level = 1, games_played = 0, spy_wins = 0, civilian_wins = 0 
            WHERE user_id = $1
            """, 
            user_id
        )

async def get_all_users() -> List[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM players")
        return [row['user_id'] for row in rows]

async def get_recent_games(limit: int = 10) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM game_logs ORDER BY timestamp DESC LIMIT $1", limit)
        return [dict(row) for row in rows]

async def update_player_stats(user_id: int, is_spy: bool, is_winner: bool) -> tuple[int, int, int]:
    player = await get_or_create_player(user_id)
    old_level = player.level
    
    xp_gain = 0
    if is_winner:
        xp_gain = 20 if is_spy else 10
        
    new_total_xp = player.total_xp + xp_gain
    new_level, current_progress_xp, xp_needed = get_level_from_xp(new_total_xp)
    
    async with pool.acquire() as conn:
        sql = """
            UPDATE players 
            SET total_xp = $1, 
                level = $2, 
                games_played = games_played + 1,
                spy_wins = spy_wins + CASE WHEN $3 THEN 1 ELSE 0 END,
                civilian_wins = civilian_wins + CASE WHEN $4 THEN 1 ELSE 0 END
            WHERE user_id = $5
        """
        # Параметри: new_xp, new_lvl, is_spy_win, is_civ_win, uid
        is_spy_win = (is_winner and is_spy)
        is_civ_win = (is_winner and not is_spy)
        
        await conn.execute(sql, new_total_xp, new_level, is_spy_win, is_civ_win, user_id)
        
    return old_level, current_progress_xp, xp_needed

async def get_player_stats(user_id: int) -> Optional[Dict[str, Any]]:
    player = await get_player(user_id)
    if not player: return None
    
    # Авто-фікс рівня
    calc_level, cur_xp, need = get_level_from_xp(player.total_xp)
    if calc_level != player.level:
        await update_player(user_id, level=calc_level)
        player.level = calc_level
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