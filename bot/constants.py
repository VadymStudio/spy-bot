# bot/constants.py
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.getenv('RENDER_DISK_PATH', '') + '/players.db' if os.getenv('RENDER_DISK_PATH') else 'players.db'
