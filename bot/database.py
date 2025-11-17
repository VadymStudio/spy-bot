import logging
import aiosqlite
import time
from bot.utils import DB_PATH, logger

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                total_xp INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                spy_wins INTEGER DEFAULT 0,
                civilian_wins INTEGER DEFAULT 0,
                banned_until INTEGER DEFAULT 0,
                premium_until INTEGER DEFAULT 0,
                owned_packs TEXT DEFAULT ''
            )
        ''')
        try:
            await db.execute("ALTER TABLE players ADD COLUMN premium_until INTEGER DEFAULT 0")
            await db.execute("ALTER TABLE players ADD COLUMN owned_packs TEXT DEFAULT ''")
            logger.info("Added 'premium_until' and 'owned_packs' columns to players table.")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" in str(e):
                pass
            else:
                raise e
        await db.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_code TEXT,
                stars INTEGER,
                created_at INTEGER
            )
        ''')
        await db.commit()
    logger.info("Database initialized successfully.")

async def get_player_stats(user_id, username):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''INSERT INTO players (user_id, username) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
            ''', (user_id, username)
        )
        await db.commit()
        async with db.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)) as cursor:
            player = await cursor.fetchone()
        if player is None:
            logger.error(f"Failed to create or find player {user_id}")
            return (user_id, username, 0, 0, 0, 0, 0, 0, '')
        return player

async def update_player_stats(user_id, is_spy, is_winner):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT total_xp, games_played, spy_wins, civilian_wins FROM players WHERE user_id = ?", (user_id,)) as cursor:
                stats = await cursor.fetchone()
                if not stats:
                    logger.warning(f"Could not update stats: Player {user_id} not found.")
                    return
            total_xp, games_played, spy_wins, civilian_wins = stats
            games_played += 1
            if is_winner:
                if is_spy:
                    spy_wins += 1
                    total_xp += 20  # XP_SPY_WIN
                else:
                    civilian_wins += 1
                    total_xp += 10  # XP_CIVILIAN_WIN
            await db.execute(
                "UPDATE players SET total_xp = ?, games_played = ?, spy_wins = ?, civilian_wins = ? WHERE user_id = ?",
                (total_xp, games_played, spy_wins, civilian_wins, user_id)
            )
            await db.commit()
            logger.info(f"Stats updated for {user_id}. XP: {total_xp}, Games: {games_played}")
    except Exception as e:
        logger.error(f"Failed to update stats for {user_id}: {e}", exc_info=True)

async def update_premium(user_id, duration_seconds):
    current_time = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE players SET premium_until = max(premium_until, ?) WHERE user_id = ?",
            (current_time + duration_seconds, user_id)
        )
        await db.commit()

async def add_pack(user_id, pack_name):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT owned_packs FROM players WHERE user_id = ?", (user_id,)) as cursor:
            result = await cursor.fetchone()
            owned = result[0].split(',') if result[0] else []
        owned = list(set(owned + [pack_name]))
        await db.execute(
            "UPDATE players SET owned_packs = ? WHERE user_id = ?",
            (','.join(owned), user_id)
        )
        await db.commit()

async def add_purchase(user_id, item_code, stars):
    current_time = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO purchases (user_id, item_code, stars, created_at) VALUES (?, ?, ?, ?)",
            (user_id, item_code, stars, current_time)
        )
        await db.commit()

async def get_purchases():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM purchases ORDER BY created_at DESC") as cursor:
            return await cursor.fetchall()

async def refund_purchase(purchase_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, item_code FROM purchases WHERE id = ?", (purchase_id,)) as cursor:
            purchase = await cursor.fetchone()
            if not purchase:
                return None
        await db.execute("DELETE FROM purchases WHERE id = ?", (purchase_id,))
        await db.commit()
        user_id, item_code = purchase
        if item_code.startswith("VIP_"):
            await db.execute("UPDATE players SET premium_until = 0 WHERE user_id = ?", (user_id,))
            await db.commit()
        elif item_code.startswith("PACK_"):
            pack_name = item_code.lower().replace("pack_", "")
            async with db.execute("SELECT owned_packs FROM players WHERE user_id = ?", (user_id,)) as cursor:
                result = await cursor.fetchone()
                owned = result[0].split(',') if result[0] else []
            if pack_name in owned:
                owned.remove(pack_name)
                await db.execute("UPDATE players SET owned_packs = ? WHERE user_id = ?", (','.join(owned), user_id))
                await db.commit()
        return user_id, item_code