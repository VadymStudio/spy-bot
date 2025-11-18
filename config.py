import os
from dotenv import load_dotenv

# Завантажуємо змінні з .env
load_dotenv()

# Налаштування бота
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")

ADMIN_IDS_STR = os.getenv('ADMIN_ID')
if not ADMIN_IDS_STR:
    raise ValueError("ADMIN_ID is not set in environment variables. Please set it (comma-separated if multiple).")

ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',')]

# Налаштування вебхуків
USE_POLLING = os.getenv('USE_POLLING', 'false').lower() == 'true'
RENDER_EXTERNAL_HOSTNAME = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'spy-game-bot.onrender.com')
WEBHOOK_PATH = "/webhook"

# Налаштування бази даних
DB_PATH = os.getenv('RENDER_DISK_PATH', '') + '/players.db' if os.getenv('RENDER_DISK_PATH') else 'players.db'

# Ігрові константи
LOCATIONS = [
    "Аеропорт", "Банк", "Пляж", "Казино", "Цирк", "Школа", "Лікарня",
    "Готель", "Музей", "Ресторан", "Театр", "Парк", "Космічна станція",
    "Підвал", "Океан", "Острів", "Кафе", "Аквапарк", "Магазин", "Аптека",
    "Зоопарк", "Місяць", "Річка", "Озеро", "Море", "Ліс", "Храм",
    "Поле", "Село", "Місто", "Ракета", "Атомна станція", "Ферма",
    "Водопад", "Спа салон", "Квартира", "Метро", "Каналізація", "Порт"
]

CALLSIGNS = [
    "Бобр Курва", "Кличко", "Фенікс", "Шашлик", "Мамкін хакер", "Сігма", "Деві Джонс", "Курт Кобейн",
    "Шрек", "Тигр", "Тарас", "Він Дізель", "Дикий борщ", "Раян Гослінг", "Том Круз", "Лео Ді Капрізник",
    "Місцевий свата", "Банан4ік", "Мегагей", "Туалетний Філософ", "Свій Шпигун", "Не Шпигун", "Санечка",
    "Скала", "Захар Кокос", "Козак", "Чорний", "Аня 15см", "Анімешнік", "Джамал", "Ловець Натуралів",
    "Натурал", "Санс", "Гетеросексуал", "Рікрол", "Сапорт", "Туалетний Монстр", "456", "Скажений Пельмень"
]

# Налаштування гри
XP_CIVILIAN_WIN = 10
XP_SPY_WIN = 20
MESSAGE_MAX_LENGTH = 120
GAME_DURATION_SECONDS = 20 * 60  # 20 хвилин за замовчуванням
ROOM_EXPIRY = 3600  # 1 година
SAVE_INTERVAL = 10  # секунд

# Антиспам та обмеження повідомлень
MAX_MSG_PER_SEC = 3
SPAM_COOLDOWN_SECONDS = 5
MAX_TEXT_LENGTH = 150
BLOCK_MEDIA = True  # блокуємо фото/гіф/стікери за замовчуванням

# Глобальні змінні
maintenance_mode = False
active_users = set()
rooms = {}
user_message_times = {}
matchmaking_queue = []
maintenance_timer_task = None
last_save_time = 0

# Геттери/сеттери для режиму обслуговування
def set_maintenance_mode(value: bool) -> None:
    global maintenance_mode
    maintenance_mode = bool(value)

def is_maintenance_mode() -> bool:
    return bool(maintenance_mode)

# Доступ до таймера мейнтенансу
def set_maintenance_task(task) -> None:
    global maintenance_timer_task
    maintenance_timer_task = task

def get_maintenance_task():
    return maintenance_timer_task

# Управління активними користувачами для розсилок
def add_active_user(user_id: int) -> None:
    active_users.add(int(user_id))

def remove_active_user(user_id: int) -> None:
    active_users.discard(int(user_id))

def get_active_users():
    return set(active_users)
