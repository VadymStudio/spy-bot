import re
import time
import random
import string
from typing import Optional

from config import ADMIN_IDS, is_maintenance_mode


def is_admin(user_id: int) -> bool:
    """Перевіряє, чи є користувач адміністратором."""
    try:
        return int(user_id) in ADMIN_IDS
    except Exception:
        return False


def maintenance_blocked(user_id: int) -> bool:
    """Чи заблокований користувач режимом обслуговування (адміни мають доступ)."""
    return is_maintenance_mode() and not is_admin(user_id)


def parse_ban_time(time_str: str) -> Optional[int]:
    """Парсить строку часу у секунди. Підтримує: Xm, Xh, Xd, 'perm'.

    Повертає кількість секунд або -1 для перманентного бана.
    Якщо формат невірний — повертає None.
    """
    if not time_str:
        return None

    s = time_str.strip().lower()
    if s in ("perm", "permanent", "forever"):
        return -1

    m = re.fullmatch(r"(\d+)([smhd])", s)
    if not m:
        return None

    value = int(m.group(1))
    unit = m.group(2)

    multipliers = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400,
    }
    return value * multipliers[unit]


def compute_ban_until(duration_sec: int) -> int:
    """Обчислює timestamp до якого діє бан. -1 означає перманент."""
    if duration_sec == -1:
        return 2**31 - 1  # великий timestamp (~2038), достатньо як 'перманентний'
    return int(time.time()) + max(0, duration_sec)


def generate_room_token(length: int = 6) -> str:
    """Генерує унікальний токен кімнати у форматі A-Z0-9."""
    alphabet = string.ascii_uppercase + string.digits
    return ''.join(random.choices(alphabet, k=length))
