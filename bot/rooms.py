from bot.constants import logger, DB_PATH
import json
import time
import asyncio
import os
import psutil
from collections import deque
from bot.state import (
    rooms, user_message_times, last_save_time, SAVE_INTERVAL, ROOM_EXPIRY, 
    logger, DB_PATH, LOCATIONS, process
)


def save_rooms():
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time < SAVE_INTERVAL:
        return
    try:
        room_copy = {}
        for token, room in rooms.items():
            room_copy[token] = room.copy()
            room_copy[token]['banned_from_voting'] = list(room['banned_from_voting'])
            room_copy[token]['voters'] = list(room['voters'])
            room_copy[token]['messages'] = room_copy[token]['messages'][-100:]
            room_copy[token].pop('timer_task', None)
            room_copy[token].pop('spy_guess_timer_task', None)
        with open('rooms.json', 'w') as f:
            json.dump(room_copy, f, indent=4)
        last_save_time = current_time
        logger.info("Rooms saved to rooms.json")
    except Exception as e:
        logger.error(f"Failed to save rooms: {e}", exc_info=True)

def load_rooms():
    global rooms
    try:
        if os.path.exists('rooms.json'):
            with open('rooms.json', 'r') as f:
                loaded_rooms = json.load(f)
                rooms = {k: v for k, v in loaded_rooms.items()}
                for room in rooms.values():
                    room['banned_from_voting'] = set(room['banned_from_voting'])
                    room['voters'] = set(room['voters'])
                    room['votes'] = {int(k): int(v) for k, v in room['votes'].items()}
                    room['timer_task'] = None
                    room['spy_guess_timer_task'] = None
                    room['last_activity'] = time.time()
                    room['created_at'] = room.get('created_at', time.time())
                logger.info("Rooms loaded from rooms.json")
    except Exception as e:
        logger.error(f"Failed to load rooms: {e}", exc_info=True)

async def cleanup_rooms():
    while True:
        try:
            current_time = time.time()
            expired = []
            for token, room in list(rooms.items()):
                if room.get('game_started'):
                    continue
                if current_time - room.get('last_activity', current_time) > ROOM_EXPIRY:
                    expired.append(token)
            for token in expired:
                room = rooms.get(token)
                if room:
                    if room.get('timer_task') and not room['timer_task'].done():
                        room['timer_task'].cancel()
                    if room.get('spy_guess_timer_task') and not room['spy_guess_timer_task'].done():
                        room['spy_guess_timer_task'].cancel()
                if token in rooms:
                    del rooms[token]
                    logger.info(f"Removed expired/finished room: {token}")
            expired_users = [uid for uid, data in user_message_times.items() if current_time - data.get('last_seen', 0) > 3600]
            for uid in expired_users:
                del user_message_times[uid]
            save_rooms()
            memory_usage = process.memory_info().rss / 1024 / 1024
            logger.info(f"Cleanup complete. Memory usage: {memory_usage:.2f} MB, Active rooms: {len(rooms)}")
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Cleanup rooms error: {e}", exc_info=True)
            await asyncio.sleep(300)