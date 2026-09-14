import asyncio
import html
from html.parser import HTMLParser
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)
import json
import logging
import math
import os
from pathlib import Path
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.error import (
    BadRequest,
    InvalidToken,
    NetworkError,
    RetryAfter,
    TimedOut,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import daily_analytics
import mapa_client
import ua_siren


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BASE_DIR = Path(
    os.getenv("BOT_DATA_DIR")
    or Path(__file__).resolve().parent
)
MEDIA_DIR = BASE_DIR / "media"
ADMIN_IDS = frozenset(
    int(part)
    for part in os.getenv("BOT_ADMIN_IDS", "").replace(",", " ").split()
    if part.lstrip("-").isdigit()
)
ANALYTICS_CHAT_ID = os.getenv("BOT_ANALYTICS_CHAT_ID", "")

THREATS_URL = "https://neptun.in.ua/api/v1/threats"
ALERTS_URL = "https://neptun.in.ua/api/v1/alerts"
SOURCE_URL = "https://neptun.in.ua/"
DNIPRO_ALERT_CHANNEL = "dnipro_alerts"
DNIPRO_ALERT_CHANNEL_URL = (
    "https://t.me/s/dnipro_alerts"
)
FAST_DNIPRO_ALERT_CHANNEL = "timofii_kucher"
FAST_DNIPRO_ALERT_CHANNEL_URL = (
    "https://t.me/s/timofii_kucher"
)
DNIPRO_ALERT_CHECK_INTERVAL_SECONDS = 20
LOCAL_CHANNEL_POLL_INTERVAL_SECONDS = 30

CHECK_INTERVAL_SECONDS = 5
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BASE_DELAY_SECONDS = 1
HTTP_RETRY_MAX_DELAY_SECONDS = 8
TELEGRAM_SEND_RETRY_ATTEMPTS = 3
TELEGRAM_SEND_RETRY_BASE_DELAY_SECONDS = 2
TELEGRAM_SEND_RETRY_MAX_DELAY_SECONDS = 30
MONITOR_RESTART_DELAY_SECONDS = 5
CHAT_CONCURRENCY_LIMIT = 10
UPDATE_INTERVAL_SECONDS = 120
ALERT_HISTORY_LIMIT = 200
SETTINGS_FILE = str(BASE_DIR / "user_settings.json")
RUNTIME_STATE_FILE = str(BASE_DIR / "runtime_state.json")
RUNTIME_STATE_VERSION = 2
TARGET_NUMBER_LIMIT = 5000
RUNTIME_STATE_SAVE_INTERVAL_SECONDS = 30
AUTO_DELETE_SECONDS = 600
AUTO_DELETE_RETRY_SECONDS = 60
DAILY_SILENCE_RETRY_WINDOW_MINUTES = 60
RED_DISTANCE_KM = 50
# Орієнтовна зона міста для повідомлення про вхід цілі.
# Це не точна адміністративна межа.
CITY_ENTRY_RADIUS_KM = 15
NIGHT_THREAT_RADIUS_KM = 30
DISTANCE_CHANGE_KM = 5
COURSE_TOLERANCE_DEGREES = 50
WAR_START_DATE = date(2022, 2, 24)

CITIES = {
    "Dnipro": (
        "Дніпро",
        48.4647,
        35.0462,
        "Дніпропетров",
    ),
    "Tsarychanka": (
        "Царичанка",
        48.937,
        34.478,
        "Дніпропетров",
    ),
    "Kyiv": (
        "Київ",
        50.4501,
        30.5234,
        "Київ",
    ),
    "Kharkiv": (
        "Харків",
        49.9935,
        36.2304,
        "Харків",
    ),
    "Zaporizhzhia": (
        "Запоріжжя",
        47.8388,
        35.1396,
        "Запорізь",
    ),
    "Odesa": (
        "Одеса",
        46.4825,
        30.7233,
        "Одесь",
    ),
    "Lviv": (
        "Львів",
        49.8397,
        24.0297,
        "Львів",
    ),
    "Vinnytsia": (
        "Вінниця",
        49.2331,
        28.4682,
        "Вінниць",
    ),
}

RADIUS_OPTIONS = {
    "10": 10,
    "50": 50,
    "100": 100,
    "200": 200,
    "500": 500,
    "ukraine": None,
}

user_settings = {}
known_threats = {}
last_update_time = {}
last_alert_states = {}
last_dnipro_city_alert_states = {}
alert_start_times = {}
city_threats_inside = {}
alert_history = {}
quiet_mode = {}
daily_silence_delivery_dates = {}
target_numbers = {}
next_target_number = 1
pending_message_deletions = {}
message_deletion_tasks = {}
dnipro_city_alert_cache = {
    "checked_at": 0.0,
    "state": None,
    "tsar_state": None,
    "post_id": None,
    "channel": FAST_DNIPRO_ALERT_CHANNEL,
}
runtime_state_dirty = False
last_runtime_state_save = 0.0
last_silence_date = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class HealthRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/healthz"):
            self.send_error(404)
            return

        payload = json.dumps(
            {
                "status": "ok",
                "service": "dnipro-air-alert-bot",
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )
        self.send_header(
            "Content-Length",
            str(len(payload)),
        )
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        logger.debug(
            "Health server: " + format,
            *args,
        )


def start_health_server():
    raw_port = os.getenv("PORT")

    if not raw_port:
        logger.info(
            "PORT не задано; health endpoint вимкнений"
        )
        return None

    try:
        port = int(raw_port)
    except ValueError as error:
        raise RuntimeError(
            "PORT повинен бути цілим числом."
        ) from error

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        HealthRequestHandler,
    )
    thread = threading.Thread(
        target=server.serve_forever,
        name="health-server",
        daemon=True,
    )
    thread.start()
    logger.info(
        "Health endpoint запущено на порту %s",
        port,
    )
    return server


def load_settings():
    global user_settings

    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
            user_settings = data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        user_settings = {}


def save_settings():
    temporary_file = f"{SETTINGS_FILE}.tmp"

    try:
        with open(temporary_file, "w", encoding="utf-8") as file:
            json.dump(
                user_settings,
                file,
                ensure_ascii=False,
                indent=2,
            )
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_file, SETTINGS_FILE)
    except OSError:
        logger.exception("Не вдалося зберегти налаштування")


def load_runtime_state():
    global known_threats
    global last_alert_states
    global last_dnipro_city_alert_states
    global city_threats_inside
    global daily_silence_delivery_dates
    global target_numbers
    global next_target_number
    global last_silence_date
    global pending_message_deletions
    global runtime_state_dirty
    global alert_history
    global quiet_mode

    try:
        with open(
            RUNTIME_STATE_FILE,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
    except FileNotFoundError:
        return
    except (OSError, json.JSONDecodeError):
        logger.exception(
            "Не вдалося завантажити службовий стан"
        )
        return

    if not isinstance(data, dict):
        logger.warning(
            "Службовий стан має неправильний формат"
        )
        return

    state_version = data.get("version")
    restore_alert_states = (
        state_version == RUNTIME_STATE_VERSION
    )

    if not restore_alert_states:
        logger.info(
            "Стан тривог скинуто після оновлення "
            "логіки визначення зон"
        )

    for key, target_name in (
        ("known_threats", "known_threats"),
        ("last_alert_states", "last_alert_states"),
        (
            "last_dnipro_city_alert_states",
            "last_dnipro_city_alert_states",
        ),
        (
            "daily_silence_delivery_dates",
            "daily_silence_delivery_dates",
        ),
        ("target_numbers", "target_numbers"),
        ("alert_history", "alert_history"),
        ("quiet_mode", "quiet_mode"),
    ):
        value = data.get(key)

        if not isinstance(value, dict):
            continue

        if (
            target_name
            in (
                "last_alert_states",
                "last_dnipro_city_alert_states",
            )
            and not restore_alert_states
        ):
            continue

        if target_name == "known_threats":
            known_threats = value
        elif target_name == "last_alert_states":
            last_alert_states = value
        elif (
            target_name
            == "last_dnipro_city_alert_states"
        ):
            last_dnipro_city_alert_states = value
        elif (
            target_name
            == "daily_silence_delivery_dates"
        ):
            daily_silence_delivery_dates = value
        elif target_name == "target_numbers":
            target_numbers = value
        elif target_name == "alert_history":
            alert_history = {
                str(chat_key): entries[-ALERT_HISTORY_LIMIT:]
                for chat_key, entries in value.items()
                if isinstance(entries, list)
            }
        elif target_name == "quiet_mode":
            quiet_mode = {
                str(chat_key): bool(enabled)
                for chat_key, enabled in value.items()
            }

    stored_deletions = data.get(
        "pending_message_deletions"
    )
    if isinstance(stored_deletions, dict):
        pending_message_deletions = {
            str(key): value
            for key, value in stored_deletions.items()
            if (
                isinstance(value, dict)
                and isinstance(value.get("chat_id"), int)
                and isinstance(value.get("message_id"), int)
                and isinstance(value.get("delete_at"), (int, float))
            )
        }

    stored_inside = data.get("city_threats_inside")

    if isinstance(stored_inside, dict):
        city_threats_inside = {
            str(chat_key): set(threat_ids)
            for chat_key, threat_ids in stored_inside.items()
            if isinstance(threat_ids, list)
        }

    valid_numbers = [
        value
        for value in target_numbers.values()
        if isinstance(value, int)
    ]
    next_target_number = (
        max(valid_numbers, default=0) + 1
    )

    silence_date = data.get("last_silence_date")

    if isinstance(silence_date, str):
        try:
            last_silence_date = date.fromisoformat(
                silence_date
            )
        except ValueError:
            logger.warning(
                "Некоректна дата у службовому стані"
            )

    runtime_state_dirty = False
    logger.info("Службовий стан відновлено")


def mark_runtime_state_dirty():
    global runtime_state_dirty
    runtime_state_dirty = True


def save_runtime_state(force=False):
    global runtime_state_dirty
    global last_runtime_state_save

    now = time.monotonic()

    if not runtime_state_dirty:
        return

    if (
        not force
        and now - last_runtime_state_save
        < RUNTIME_STATE_SAVE_INTERVAL_SECONDS
    ):
        return

    payload = {
        "version": RUNTIME_STATE_VERSION,
        "known_threats": known_threats,
        "last_alert_states": last_alert_states,
        "last_dnipro_city_alert_states": (
            last_dnipro_city_alert_states
        ),
        "city_threats_inside": {
            chat_key: sorted(threat_ids)
            for chat_key, threat_ids
            in city_threats_inside.items()
        },
        "daily_silence_delivery_dates": (
            daily_silence_delivery_dates
        ),
        "target_numbers": target_numbers,
        "alert_history": alert_history,
        "quiet_mode": quiet_mode,
        "pending_message_deletions": (
            pending_message_deletions
        ),
        "last_silence_date": (
            last_silence_date.isoformat()
            if last_silence_date is not None
            else None
        ),
    }
    temporary_file = f"{RUNTIME_STATE_FILE}.tmp"

    try:
        with open(
            temporary_file,
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                payload,
                file,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            file.flush()
            os.fsync(file.fileno())

        os.replace(
            temporary_file,
            RUNTIME_STATE_FILE,
        )
        runtime_state_dirty = False
        last_runtime_state_save = now
    except OSError:
        logger.exception(
            "Не вдалося зберегти службовий стан"
        )


def default_settings():
    return {
        "city": "Dnipro",
        "radius": "100",
        "use_custom_location": False,
        "custom_lat": None,
        "custom_lon": None,
    }


def get_settings(chat_id):
    key = str(chat_id)

    if key not in user_settings or not isinstance(user_settings[key], dict):
        user_settings[key] = default_settings()
        save_settings()

    settings = user_settings[key]

    if settings.get("city") not in CITIES:
        settings["city"] = "Dnipro"

    if settings.get("radius") not in RADIUS_OPTIONS:
        settings["radius"] = "100"

    return settings


def monitoring_enabled(chat_id):
    settings = get_settings(chat_id)

    if "monitoring_enabled" not in settings:
        settings["monitoring_enabled"] = True
        save_settings()

    return settings["monitoring_enabled"]


async def start_monitoring_button(
    update,
    chat_id,
    reply_markup=None,
):
    settings = get_settings(chat_id)
    settings["monitoring_enabled"] = True
    save_settings()

    await update.message.reply_text(
        "▶️ <b>МОНІТОРИНГ УВІМКНЕНО</b>\n\n"
        "🟢 Ви знову отримуватимете автоматичні "
        "повідомлення про загрози та повітряні тривоги.",
        parse_mode="HTML",
        reply_markup=(
            reply_markup
            if reply_markup is not None
            else main_keyboard()
        ),
    )


async def stop_monitoring_button(
    update,
    chat_id,
    reply_markup=None,
):
    settings = get_settings(chat_id)
    settings["monitoring_enabled"] = False
    save_settings()

    await update.message.reply_text(
        "⏹ <b>МОНІТОРИНГ ВИМКНЕНО</b>\n\n"
        "🔕 Автоматичні повідомлення для цього чату "
        "тимчасово вимкнені.\n\n"
        "▶️ Натисніть «Старт», щоб знову їх увімкнути.",
        parse_mode="HTML",
        reply_markup=(
            reply_markup
            if reply_markup is not None
            else main_keyboard()
        ),
    )


def nearest_city_key(latitude, longitude):
    closest_key = "Dnipro"
    closest_distance = None
    for city_key, (_, city_lat, city_lon, _) in CITIES.items():
        current = distance_km(latitude, longitude, city_lat, city_lon)
        if closest_distance is None or current < closest_distance:
            closest_distance = current
            closest_key = city_key
    return closest_key

def city_info(chat_id):
    settings = get_settings(chat_id)
    city_key = settings["city"]
    city_name, latitude, longitude, oblast = CITIES[city_key]
    if settings.get("use_custom_location") and settings.get("custom_lat") is not None and settings.get("custom_lon") is not None:
        return ("Моя локація", float(settings["custom_lat"]), float(settings["custom_lon"]), oblast)
    return city_name, latitude, longitude, oblast


def radius_value(chat_id):
    radius_key = get_settings(chat_id)["radius"]
    return RADIUS_OPTIONS[radius_key]


def radius_label(chat_id):
    radius = radius_value(chat_id)
    return "Вся Україна" if radius is None else f"{radius} км"


def format_kyiv_time(iso_time):
    if not iso_time:
        return "—"

    try:
        dt = datetime.fromisoformat(
            iso_time.replace("Z", "+00:00")
        )
        dt = dt.astimezone(ZoneInfo("Europe/Kyiv"))
        return dt.strftime("%H:%M")
    except Exception:
        return "—"


def danger_level(alert):
    if alert:
        return (
            "🔴 <b>ВИСОКИЙ</b>",
            "Оголошено повітряну тривогу.",
        )

    return (
        "🟢 <b>НИЗЬКИЙ</b>",
        "Повітряна тривога не оголошена.",
    )


def is_night_hours():
    hour=datetime.now(ZoneInfo("Europe/Kyiv")).hour
    return hour < 6

def is_quiet_mode(chat_id):
    return quiet_mode.get(
        str(chat_id),
        False,
    )


def set_quiet_mode(chat_id, enabled):
    quiet_mode[str(chat_id)] = enabled
    mark_runtime_state_dirty()


def get_alert_history(chat_id):
    key = str(chat_id)

    if key not in alert_history:
        alert_history[key] = []

    return alert_history[key]


def add_alert_event(chat_id, event_type):
    history = get_alert_history(chat_id)

    history.append(
        {
            "type": event_type,
            "time": datetime.now(
                ZoneInfo("Europe/Kyiv")
            ).isoformat(),
        }
    )

    if len(history) > ALERT_HISTORY_LIMIT:
        del history[:-ALERT_HISTORY_LIMIT]

    mark_runtime_state_dirty()


def format_duration(seconds):
    seconds = max(0, int(seconds))

    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    if hours:
        return f"{hours} год {minutes} хв"

    return f"{minutes} хв"


def get_alert_statistics(chat_id, days=1):
    history = get_alert_history(chat_id)

    now = datetime.now(
        ZoneInfo("Europe/Kyiv")
    )
    start = now - timedelta(days=days)

    events = []

    for item in history:
        try:
            event_time = datetime.fromisoformat(
                item["time"]
            )

            if event_time.tzinfo is None:
                event_time = event_time.replace(
                    tzinfo=ZoneInfo("Europe/Kyiv")
                )

            if event_time >= start:
                events.append(
                    (
                        item["type"],
                        event_time,
                    )
                )
        except Exception:
            continue

    events.sort(key=lambda x: x[1])

    count = 0
    total_seconds = 0
    alert_started = None

    for event_type, event_time in events:
        if event_type == "start":
            count += 1

            if alert_started is None:
                alert_started = event_time

        elif (
            event_type == "end"
            and alert_started is not None
        ):
            total_seconds += (
                event_time - alert_started
            ).total_seconds()

            alert_started = None

    # Якщо тривога ще триває.
    if alert_started is not None:
        total_seconds += (
            now - alert_started
        ).total_seconds()

    return count, total_seconds


def main_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["▶️ Старт", "⏹ Стоп"],
            ["🚨 Стан", "🛸 Загрози"],
            ["📌 Моя локація", "🔕 Тихий режим"],
            ["⚙️ Налаштування", "ℹ️ Допомога"],
        ],
        resize_keyboard=True,
        is_persistent=True,
        selective=True,
    )


def settings_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["📍 Місто", "📏 Радіус"],
            ["📍 Точки", "📊 Обстановка"],
            ["📈 Історія", "📊 Статистика"],
            ["📋 Журнал", "🚨 Небезпека"],
            ["🔄 Оновити", "❌ Вийти"],
            ["⬅️ Назад"],
        ],
        resize_keyboard=True,
        is_persistent=True,
        selective=True,
    )



# Кнопки, що змінюють налаштування чату або стан моніторингу.
RESTRICTED_BUTTONS = frozenset(
    {
        "▶️ Старт",
        "⏹ Стоп",
        "🔕 Тихий режим",
        "📍 Місто",
        "📌 Моя локація",
        "📏 Радіус",
        "❌ Вийти",
    }
    | {city_name for city_name, _, _, _ in CITIES.values()}
    | {
        "🔴 10 км",
        "🔴 50 км",
        "🟠 100 км",
        "🟡 200 км",
        "🟢 500 км",
        "🇺🇦 Вся Україна",
    }
)


async def can_use_keyboard(update, context):
    chat = update.effective_chat

    if chat is None:
        return False

    if chat.type == "private":
        return True

    user = update.effective_user
    bot = getattr(context, "bot", None)

    if user is None or bot is None:
        return False

    try:
        member = await bot.get_chat_member(
            chat.id,
            user.id,
        )
    except Exception:
        logger.exception(
            "Не вдалося перевірити права користувача "
            f"{user.id} у чаті {chat.id}"
        )
        return False

    return member.status in {
        "creator",
        "administrator",
    }


async def keyboard_for_user(update, context):
    if await can_use_keyboard(update, context):
        return main_keyboard()

    return ReplyKeyboardRemove()


def city_keyboard():
    rows = [[city_name] for city_name, _, _, _ in CITIES.values()]
    rows.insert(0, [KeyboardButton("📌 Надіслати геолокацію", request_location=True)])
    rows.append(["⬅️ Назад"])
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def radius_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔴 10 км",
                    callback_data="radius:10",
                ),
                InlineKeyboardButton(
                    "🔴 50 км",
                    callback_data="radius:50",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🟠 100 км",
                    callback_data="radius:100",
                ),
                InlineKeyboardButton(
                    "🟡 200 км",
                    callback_data="radius:200",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🟢 500 км",
                    callback_data="radius:500",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🇺🇦 Вся Україна",
                    callback_data="radius:ukraine",
                ),
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data="back",
                ),
            ],
        ]
    )


def distance_km(lat1, lon1, lat2, lon2):
    earth_radius_km = 6371
    point1_lat = math.radians(lat1)
    point2_lat = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(point1_lat)
        * math.cos(point2_lat)
        * math.sin(delta_lon / 2) ** 2
    )

    return earth_radius_km * 2 * math.atan2(
        math.sqrt(haversine),
        math.sqrt(1 - haversine),
    )


def heading_toward_point(
    heading,
    a,
    b,
    c,
    d,
    tolerance=COURSE_TOLERANCE_DEGREES,
):
    if heading is None: return False
    try: h=float(heading)
    except (TypeError, ValueError): return False
    sl,so,el,eo=map(math.radians,[a,b,c,d])
    x=math.sin(eo-so)*math.cos(el)
    y=math.cos(sl)*math.sin(el)-math.sin(sl)*math.cos(el)*math.cos(eo-so)
    target=(math.degrees(math.atan2(x,y))+360)%360
    return abs((h-target+180)%360-180)<=tolerance

def direction(degrees):
    if degrees is None:
        return "—"

    directions = [
        "північ",
        "північний схід",
        "схід",
        "південний схід",
        "південь",
        "південний захід",
        "захід",
        "північний захід",
    ]
    try:
        degrees = float(degrees)
    except (TypeError, ValueError):
        return "—"

    return directions[int(math.floor((degrees % 360) / 45 + 0.5)) % 8]


def dnipro_bank_label(lat, lon):
    if lat is None or lon is None: return None
    try: lat=float(lat); lon=float(lon)
    except (TypeError, ValueError): return None
    if distance_km(lat, lon, 48.4647, 35.0462) > 22: return None
    return "лівий берег" if lon >= 35.055 else "правий берег"

def threat_name(threat):
    if threat.get("title"):
        return str(threat["title"])

    names = {
        "uav": "БпЛА",
        "drone": "БпЛА",
        "recon": "Розвідувальний БпЛА",
        "missile": "Ракета",
        "ballistic": "Балістична ракета",
        "kab": "КАБ",
        "mig31k": "МіГ-31К",
    }
    threat_type = str(threat.get("type", "")).lower()
    return names.get(threat_type, "Невідома загроза")


def target_number(threat):
    global next_target_number

    threat_id = str(threat.get("id"))

    if threat_id not in target_numbers:
        target_numbers[threat_id] = next_target_number
        next_target_number += 1
        mark_runtime_state_dirty()

    return target_numbers[threat_id]


def prune_target_numbers():
    """Обмежує розмір кешу номерів цілей."""
    if len(target_numbers) <= TARGET_NUMBER_LIMIT:
        return

    active_ids = {
        threat_id
        for threats in known_threats.values()
        for threat_id in threats
    }
    stale_ids = [
        threat_id
        for threat_id in target_numbers
        if threat_id not in active_ids
    ]
    stale_ids.sort(key=lambda key: target_numbers[key])

    for threat_id in stale_ids[
        : len(target_numbers) - TARGET_NUMBER_LIMIT
    ]:
        del target_numbers[threat_id]

    mark_runtime_state_dirty()


def retry_delay(attempt, base, maximum):
    return min(
        base * (2**attempt),
        maximum,
    )


def fetch_response(url, headers=None):
    for attempt in range(HTTP_RETRY_ATTEMPTS):
        try:
            response = requests.get(
                url,
                timeout=10,
                headers=headers,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            response = getattr(error, "response", None)
            status_code = getattr(
                response,
                "status_code",
                None,
            )
            retryable = (
                status_code is None
                or status_code == 429
                or status_code >= 500
            )

            if (
                not retryable
                or attempt == HTTP_RETRY_ATTEMPTS - 1
            ):
                raise

            delay = retry_delay(
                attempt,
                HTTP_RETRY_BASE_DELAY_SECONDS,
                HTTP_RETRY_MAX_DELAY_SECONDS,
            )
            logger.warning(
                "Зовнішній запит не вдався "
                "(спроба %s/%s), повтор через %ss: %s",
                attempt + 1,
                HTTP_RETRY_ATTEMPTS,
                delay,
                type(error).__name__,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Не вдалося виконати запит до {url}"
    )


def fetch_json(url):
    response = fetch_response(url)

    try:
        return response.json()
    except ValueError:
        logger.error(
            "Зовнішній сервіс повернув некоректний JSON: %s",
            url,
        )
        raise


class TelegramChannelPostParser(HTMLParser):
    def __init__(self, channel):
        super().__init__(convert_charrefs=True)
        self.channel = channel
        self.posts = []
        self.current_post = None
        self.text_depth = 0
        self.text_parts = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)

        if tag == "div":
            data_post = attributes.get(
                "data-post",
                "",
            )
            prefix = f"{self.channel}/"

            if data_post.startswith(prefix):
                post_id = data_post.removeprefix(
                    prefix
                )
                self.current_post = {
                    "id": int(post_id),
                }
                self.text_depth = 0
                self.text_parts = []

            if self.current_post is not None:
                classes = attributes.get(
                    "class",
                    "",
                ).split()

                if self.text_depth > 0:
                    self.text_depth += 1
                elif "tgme_widget_message_text" in classes:
                    self.text_depth = 1

        elif (
            tag == "br"
            and self.text_depth > 0
        ):
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        if tag != "div" or self.text_depth <= 0:
            return

        self.text_depth -= 1

        if self.text_depth == 0:
            text = "".join(self.text_parts).strip()

            if text and self.current_post is not None:
                self.posts.append(
                    {
                        **self.current_post,
                        "text": text,
                    }
                )

            self.current_post = None
            self.text_parts = []

    def handle_data(self, data):
        if self.text_depth > 0:
            self.text_parts.append(data)


def classify_dnipro_city_alert(text):
    content = text.split(
        "💙 Дніпро Alerts",
        1,
    )[0]
    normalized = re.sub(
        r"\s+",
        " ",
        content.lower().replace("’", "'"),
    ).strip()

    if "відбій" in normalized:
        city_end_markers = (
            "дніпро",
            "дніпровськ",
            "локальн",
            "правобереж",
            "по області",
        )

        if any(
            marker in normalized
            for marker in city_end_markers
        ):
            return False

    city_start_patterns = (
        (
            r"\bдніпро\b.{0,100}"
            r"(?:червон\w*|жовтогаряч\w*|"
            r"тривог\w*|тривож\w*)"
        ),
        (
            r"\bдніпровськ\w*\s+район\w*"
            r".{0,100}(?:тривог\w*|тривож\w*)"
        ),
        (
            r"\b(?:тривог\w*|тривож\w*).{0,100}"
            r"дніпровськ\w*\s+район"
        ),
    )

    if any(
        re.search(pattern, normalized)
        for pattern in city_start_patterns
    ):
        return True

    return None


pending_kucher_locals=[]
last_local_channel_poll=0.0

DNIPROAL_HINTS=("бпла","шахед","ракет","баліст","курс","пуск","дніпр","нагорк","ігрень","чечел","шосе","лівий","правий","реактив","зник","укритт")
NYI_HINTS=("бпла","шахед","ракет","баліст","курс","пуск","дніпр","запорізьк","полтав","мілер","ппу","4.5.0","реактив","тривог")
KUCHER_HINTS=("бпла","шахед","ракет","баліст","курс","нагорк","ігрень","чечел","шосе","укритт","зник","низько","висота","реактив","парк","лівий","правий","новік","придні","дослідн")


def read_last_post_id(path):
    if not path.exists():
        return 0

    try:
        return int(path.read_text().strip() or 0)
    except (OSError, ValueError):
        return 0


def write_last_post_id(path, post_id):
    try:
        path.write_text(str(post_id))
    except OSError:
        logger.exception(
            "Не вдалося зберегти стан каналу %s",
            path.name,
        )


def fetch_channel_posts(channel, channel_url):
    response = fetch_response(
        channel_url,
        headers={
            "User-Agent": "Mozilla/5.0 DniproAirAlertBot/1.0",
        },
    )
    parser = TelegramChannelPostParser(channel)
    parser.feed(response.text)
    return sorted(parser.posts, key=lambda item: item["id"])


def collect_local_channel(
    channel,
    channel_url,
    state_file,
    hints,
    clean,
    line_limit,
):
    """Збирає нові локальні повідомлення каналу."""
    path = BASE_DIR / state_file
    last_id = read_last_post_id(path)
    posts = fetch_channel_posts(channel, channel_url)

    if not posts:
        return

    if last_id == 0:
        write_last_post_id(path, posts[-1]["id"])
        return

    for post in posts:
        if post["id"] <= last_id:
            continue

        last_id = post["id"]
        raw = clean(post["text"])
        normalized = raw.lower()

        if "повітряної тривоги" in normalized:
            continue

        if len(raw) > 5000:
            continue

        if not any(hint in normalized for hint in hints):
            continue

        line = raw.splitlines()[0][:line_limit]

        if line:
            pending_kucher_locals.append(line)

    write_last_post_id(path, last_id)


def collect_dniproal_local():
    collect_local_channel(
        DNIPRO_ALERT_CHANNEL,
        DNIPRO_ALERT_CHANNEL_URL,
        "last_dniproal_local.txt",
        DNIPROAL_HINTS,
        lambda text: text.split("t.me/")[0].strip(),
        160,
    )


def collect_nyi_local():
    collect_local_channel(
        "ny_i_dnipro",
        "https://t.me/s/ny_i_dnipro",
        "last_nyi_local.txt",
        NYI_HINTS,
        lambda text: (
            text.replace("t.me/ny_i_dnipro", "")
            .replace("НУ І ДНІПРО", "")
            .strip()
        ),
        160,
    )


def collect_kucher_local():
    collect_local_channel(
        FAST_DNIPRO_ALERT_CHANNEL,
        FAST_DNIPRO_ALERT_CHANNEL_URL,
        "last_kucher_local.txt",
        KUCHER_HINTS,
        lambda text: text.split("t.me/")[0].strip(),
        140,
    )


def fetch_channel_alert_state(channel, channel_url):
    posts = fetch_channel_posts(channel, channel_url)

    try:
        city_on, tsar_on = ua_siren.flags()
    except (requests.RequestException, RuntimeError, ValueError):
        logger.warning(
            "ukrainealarm недоступний; використовую лише "
            "дані Telegram-каналів"
        )
        city_on = None
        tsar_on = None

    latest_event = {
        "state": city_on,
        "tsar_state": tsar_on,
        "post_id": None,
        "channel": "ukrainealarm",
    }

    for post in posts:
        state = classify_dnipro_city_alert(
            post["text"]
        )

        if state is not None:
            latest_event = {
                "state": state,
                "tsar_state": tsar_on,
                "post_id": post["id"],
                "channel": channel,
            }

    return latest_event


def get_dnipro_city_alert_state():
    now = time.monotonic()
    checked_at = dnipro_city_alert_cache[
        "checked_at"
    ]

    if (
        checked_at
        and now - checked_at
        < DNIPRO_ALERT_CHECK_INTERVAL_SECONDS
    ):
        return dict(dnipro_city_alert_cache)

    source_errors = []
    latest_event = None

    for channel, channel_url in (
        (
            FAST_DNIPRO_ALERT_CHANNEL,
            FAST_DNIPRO_ALERT_CHANNEL_URL,
        ),
        (
            DNIPRO_ALERT_CHANNEL,
            DNIPRO_ALERT_CHANNEL_URL,
        ),
    ):
        try:
            latest_event = fetch_channel_alert_state(
                channel,
                channel_url,
            )
            break
        except Exception as error:
            source_errors.append(
                f"{channel}: {type(error).__name__}"
            )

    if latest_event is None:
        raise RuntimeError(
            "Не вдалося отримати стан тривоги "
            "з жодного Telegram-каналу: "
            + ", ".join(source_errors)
        )

    dnipro_city_alert_cache["checked_at"] = now
    dnipro_city_alert_cache.update(latest_event)

    return dict(dnipro_city_alert_cache)


def get_alert_state_from_data(chat_id, data):
    oblast_alerts = data.get("oblasts", [])
    raion_alerts = data.get("raions", [])
    alerts = oblast_alerts + raion_alerts
    settings = get_settings(chat_id)

    if settings["radius"] == "ukraine":
        if not alerts:
            return False, None

        since_values = [
            alert.get("since")
            for alert in alerts
            if alert.get("since")
        ]

        if since_values:
            return True, min(since_values)

        return True, None

    city_name, _, _, oblast = city_info(chat_id)
    matching_alerts = []

    for alert in oblast_alerts:
        alert_name = str(alert.get("name", ""))
        alert_oblast = str(alert.get("oblast", ""))
        if (
            oblast.lower() in alert_name.lower()
            or oblast.lower() in alert_oblast.lower()
        ):
            matching_alerts.append(alert)

    local_area_marker = (
        "Дніпровськ"
        if settings["city"] == "Dnipro"
        else city_name
    )

    for alert in raion_alerts:
        alert_name = str(alert.get("name", ""))

        if local_area_marker.lower() in alert_name.lower():
            matching_alerts.append(alert)

    if not matching_alerts:
        return False, None

    since_values = [
        alert.get("since")
        for alert in matching_alerts
        if alert.get("since")
    ]

    if since_values:
        return True, min(since_values)

    return True, None


def get_alert_state(chat_id):
    return get_alert_state_from_data(
        chat_id,
        fetch_json(ALERTS_URL),
    )


def get_all_active_threats():
    data = fetch_json(THREATS_URL)
    result = []

    for threat in data.get("threats", []):
        if threat.get("status") != "active" or not threat.get("id"):
            continue

        lat = threat.get("lat")
        lon = threat.get("lon")
        if (
            lat is None
            or lon is None
            or threat.get("areaOnly", False)
        ):
            continue

        try:
            result.append(
                (
                    float(lat),
                    float(lon),
                    threat,
                )
            )
        except (TypeError, ValueError):
            continue

    return result


def get_nearby_threats_for_chat(chat_id):
    _, target_lat, target_lon, _ = city_info(chat_id)
    radius = radius_value(chat_id)
    nearby = []

    for lat, lon, threat in get_all_active_threats():
        distance = distance_km(
            target_lat,
            target_lon,
            lat,
            lon,
        )
        if radius is None or distance <= radius:
            nearby.append((distance, threat))

    nearby.sort(key=lambda item: item[0])
    return nearby


def message_deletion_key(chat_id, message_id):
    return f"{chat_id}:{message_id}"


async def delete_message_later(
    bot,
    chat_id,
    message_id,
    delete_at,
):
    key = message_deletion_key(chat_id, message_id)
    completed = False

    try:
        while True:
            delay = max(0, delete_at - time.time())
            if delay:
                await asyncio.sleep(delay)

            try:
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=message_id,
                )
            except RetryAfter as error:
                retry_after = error.retry_after
                if hasattr(retry_after, "total_seconds"):
                    retry_after = retry_after.total_seconds()

                delay = max(
                    float(retry_after),
                    AUTO_DELETE_RETRY_SECONDS,
                )
                logger.warning(
                    "Telegram відклав авто-видалення "
                    "повідомлення %s у чаті %s; повтор через %.1fs",
                    message_id,
                    chat_id,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            except (TimedOut, NetworkError) as error:
                logger.warning(
                    "Мережева помилка авто-видалення "
                    "повідомлення %s у чаті %s: %s; "
                    "повтор через %ss",
                    message_id,
                    chat_id,
                    type(error).__name__,
                    AUTO_DELETE_RETRY_SECONDS,
                )
                await asyncio.sleep(
                    AUTO_DELETE_RETRY_SECONDS
                )
                continue
            except BadRequest as error:
                logger.warning(
                    "Telegram не видалив повідомлення %s "
                    "у чаті %s: %s",
                    message_id,
                    chat_id,
                    error,
                )
                completed = True
                break
            except Exception:
                logger.exception(
                    "Непередбачена помилка авто-видалення "
                    "повідомлення %s у чаті %s",
                    message_id,
                    chat_id,
                )
                completed = True
                break

            logger.info(
                "Повідомлення %s у чаті %s авто-видалено",
                message_id,
                chat_id,
            )
            completed = True
            break
    finally:
        if completed:
            pending_message_deletions.pop(key, None)
        message_deletion_tasks.pop(key, None)
        if completed:
            mark_runtime_state_dirty()
            await asyncio.to_thread(
                save_runtime_state,
                True,
            )


def schedule_message_deletion(
    bot,
    chat_id,
    message_id,
    delete_at=None,
):
    key = message_deletion_key(chat_id, message_id)
    if delete_at is None:
        delete_at = time.time() + AUTO_DELETE_SECONDS

    pending_message_deletions[key] = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "delete_at": float(delete_at),
    }
    mark_runtime_state_dirty()

    task = message_deletion_tasks.get(key)
    if task is None or task.done():
        message_deletion_tasks[key] = asyncio.create_task(
            delete_message_later(
                bot,
                int(chat_id),
                int(message_id),
                float(delete_at),
            ),
            name=f"delete-message-{chat_id}-{message_id}",
        )


def restore_message_deletion_tasks(bot):
    for deletion in tuple(
        pending_message_deletions.values()
    ):
        schedule_message_deletion(
            bot,
            deletion["chat_id"],
            deletion["message_id"],
            deletion["delete_at"],
        )


async def send_message(
    bot,
    chat_id,
    text,
    reply_markup=None,
    auto_delete=True,
):
    message = None

    for attempt in range(
        TELEGRAM_SEND_RETRY_ATTEMPTS
    ):
        try:
            message = await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
            break
        except RetryAfter as error:
            if (
                attempt
                == TELEGRAM_SEND_RETRY_ATTEMPTS - 1
            ):
                raise

            retry_after = error.retry_after

            if hasattr(retry_after, "total_seconds"):
                retry_after = retry_after.total_seconds()

            delay = min(
                max(
                    float(retry_after),
                    TELEGRAM_SEND_RETRY_BASE_DELAY_SECONDS,
                ),
                TELEGRAM_SEND_RETRY_MAX_DELAY_SECONDS,
            )
            logger.warning(
                "Telegram обмежив відправлення до чату %s "
                "(спроба %s/%s), повтор через %.1fs",
                chat_id,
                attempt + 1,
                TELEGRAM_SEND_RETRY_ATTEMPTS,
                delay,
            )
            await asyncio.sleep(delay)
        except (TimedOut, NetworkError):
            # Результат sendMessage може бути невідомим:
            # Telegram міг прийняти повідомлення до розриву
            # з'єднання. Сліпий повтор створює дублікати.
            raise

    if message is None:
        raise RuntimeError(
            "Telegram не повернув надіслане повідомлення"
        )

    if auto_delete:
        schedule_message_deletion(
            bot,
            chat_id,
            message.message_id,
        )

    return message


async def run_bounded(
    semaphore,
    async_function,
    *args,
):
    async with semaphore:
        return await async_function(*args)


async def send_daily_silence_to_chat(
    bot,
    chat_key,
    text,
    delivery_date,
):
    chat_id = int(chat_key)

    if not monitoring_enabled(chat_id):
        return

    if (
        daily_silence_delivery_dates.get(chat_key)
        == delivery_date
    ):
        return

    try:
        await send_message(
            bot,
            chat_id,
            text,
            auto_delete=False,
        )
    except TimedOut:
        logger.warning(
            "Результат хвилини мовчання для "
            "чату %s невідомий; повтор пропущено, "
            "щоб уникнути дублювання",
            chat_id,
        )

    daily_silence_delivery_dates[
        chat_key
    ] = delivery_date
    mark_runtime_state_dirty()


def get_war_day():
    today = datetime.now(
        ZoneInfo("Europe/Kyiv")
    ).date()

    return (
        today - WAR_START_DATE
    ).days + 1


last_morning_date=None
async def send_daily_morning(bot):
    global last_morning_date
    now=datetime.now(ZoneInfo("Europe/Kyiv"))
    today=now.date()
    path=BASE_DIR / "last_morning.txt"
    if last_morning_date is None and path.exists():
        try: last_morning_date=date.fromisoformat(path.read_text().strip())
        except (OSError, ValueError): last_morning_date=None
    if last_morning_date==today: return
    start=now.replace(hour=6, minute=0, second=0, microsecond=0)
    if not (start<=now<start+timedelta(minutes=60)): return
    text=("🌅 <b>НОВИЙ ДЕНЬ</b>\n\n"+"На один день ближче до смерті хуйла.")
    for chat_key in tuple(user_settings.keys()):
        try:
            chat_id=int(chat_key)
            if is_quiet_mode(chat_id): continue
            await send_message(bot, chat_id, text, auto_delete=False)
        except Exception:
            logger.exception("morning %s", chat_key)
    last_morning_date=today
    path.write_text(today.isoformat())

async def send_daily_silence(bot):
    global last_silence_date

    now = datetime.now(
        ZoneInfo("Europe/Kyiv")
    )

    today = now.date()

    # Не надсилати повторно цього дня.
    if last_silence_date == today:
        return

    scheduled_time = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0,
    )
    retry_window_end = (
        scheduled_time
        + timedelta(
            minutes=DAILY_SILENCE_RETRY_WINDOW_MINUTES
        )
    )

    # Початок о 09:00; невдалі доставки повторюються
    # лише в межах контрольованого вікна.
    if not (
        scheduled_time <= now < retry_window_end
    ):
        return

    day = get_war_day()

    text = (
        f"🇺🇦 <b>ДЕНЬ ВІЙНИ — {day}</b>\n\n"
        "🕯️ <b>09:00 — хвилина мовчання</b>\n"
        "Вшануймо пам'ять полеглих Героїв.\n\n"
        "🤍 Ще один день до Перемоги.\n"
        "<b>Пам'ятаємо. Тримаємося. "
        "Переможемо.</b>"
    )

    chat_keys = tuple(user_settings.keys())
    delivery_date = today.isoformat()
    semaphore = asyncio.Semaphore(
        CHAT_CONCURRENCY_LIMIT
    )
    results = await asyncio.gather(
        *(
            run_bounded(
                semaphore,
                send_daily_silence_to_chat,
                bot,
                chat_key,
                text,
                delivery_date,
            )
            for chat_key in chat_keys
        ),
        return_exceptions=True,
    )

    for chat_key, result in zip(chat_keys, results):
        if isinstance(result, BaseException):
            logger.error(
                "Не вдалося надіслати щоденне "
                "повідомлення до чату %s: %s",
                chat_key,
                type(result).__name__,
                exc_info=(
                    type(result),
                    result,
                    result.__traceback__,
                ),
            )

    if not any(
        isinstance(result, BaseException)
        for result in results
    ):
        last_silence_date = today
        mark_runtime_state_dirty()

    await asyncio.to_thread(
        save_runtime_state,
        True,
    )


def format_threat_message(chat_id, distance, threat, update=False):
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    if distance <= RED_DISTANCE_KM:
        icon = "🔴"
        level = (
            "ОНОВЛЕННЯ — ДО 50 КМ"
            if update
            else "НЕБЕЗПЕЧНА ВІДСТАНЬ"
        )
    else:
        icon = "🟠"
        level = "ОНОВЛЕННЯ" if update else "ЦІЛЬ НАБЛИЖАЄТЬСЯ"

    text = (
        f"{icon} <b>{level}</b>\n\n"
        f"🎯 <b>ЦІЛЬ №{target_number(threat)}</b>\n"
        f"🛸 <b>{html.escape(threat_name(threat))}</b>\n"
        f"📍 Моніторинг: <b>{html.escape(city_name)}</b>\n"
        f"📏 Відстань: "
        f"<b>~{round(distance)} км</b>\n"
    )

    heading = threat.get("heading")
    if heading is not None:
        if threat.get("lat") is not None and threat.get("lon") is not None and heading_toward_point(heading, float(threat.get("lat")), float(threat.get("lon")), target_lat, target_lon): text += f"🎯 <b>Курс: на {html.escape(city_name)}</b>\n"
        bank = dnipro_bank_label(threat.get("lat"), threat.get("lon"))
        if bank: text += f"🏙 Дніпро, <b>{bank}</b>\n"
        text += f"➡️ Напрямок руху: <b>{direction(heading)}</b>\n"

    velocity = threat.get("velocity") or {}
    speed = velocity.get("speedKmh")
    if speed:
        text += f"💨 Швидкість: <b>~{round(speed)} км/год</b>\n"

    uncertainty = threat.get("uncertaintyKm")
    if uncertainty and not update:
        text += f"🎯 Похибка: ±{round(uncertainty)} км\n"

    return text + (
        "\n⚠️ Інформація орієнтовна.\n"
        "Орієнтуйтеся на офіційні сигнали тривоги.\n\n"
        ""
    )


def format_threat_list(chat_id, nearby):
    city_name, target_lat, target_lon, _ = city_info(chat_id)
    radius_text = radius_label(chat_id)

    if not nearby:
        return (
            "🟢 <b>ЗАГРОЗ НЕ ВИЯВЛЕНО</b>\n\n"
            f"📍 Точка: <b>{html.escape(city_name)}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_text)}</b>\n\n"
            "Система продовжує моніторинг.\n\n"
            "⚠️ <i>Орієнтуйтеся на офіційні "
            "сигнали повітряної тривоги.</i>"
        )

    text = (
        "🛡️ <b>АКТИВНІ ЗАГРОЗИ</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"📍 Точка: <b>{html.escape(city_name)}</b>\n"
        f"📏 Радіус: <b>{html.escape(radius_text)}</b>\n\n"
    )

    for distance, threat in nearby:
        icon = "🔴" if distance <= RED_DISTANCE_KM else "🟠"
        text += (
            f"{icon} <b>ЦІЛЬ №{target_number(threat)}: "
            f"{html.escape(threat_name(threat))}</b>\n"
            f"📏 <b>~{round(distance)} км</b>\n"
        )

        heading = threat.get("heading")
        if heading is not None and threat.get("lat") is not None and threat.get("lon") is not None and heading_toward_point(heading, float(threat.get("lat")), float(threat.get("lon")), target_lat, target_lon):
            text += "🎯 Курс у вашу сторону\n"
        if heading is not None:
            text += f"➡️ {direction(heading)}\n"

        velocity = threat.get("velocity") or {}
        speed = velocity.get("speedKmh")
        if speed:
            text += f"💨 ~{round(speed)} км/год\n"

        text += "\n"

    return text + (
        "⚠️ <i>Інформація орієнтовна. "
        "Орієнтуйтеся на офіційні сигнали "
        "повітряної тривоги.</i>"
    )


async def is_bot_admin(update):
    """Дозволяє службові команди лише власникам бота."""
    user = update.effective_user

    if user is None or user.id not in ADMIN_IDS:
        if update.message is not None:
            await update.message.reply_text(
                "⛔️ Команда доступна лише адміністраторам бота."
            )

        return False

    return True


async def testcard_command(update, context):
    if not await is_bot_admin(update):
        return

    chat_id=update.effective_chat.id
    bot=context.bot
    pairs=((True,"ТЕСТ. Тривога. Листівка."),(False,"ТЕСТ. Відбій. Листівка."))
    for state, cap in pairs:
        st=MEDIA_DIR / ("tryvoga.webp" if state else "vidbiy.webp")
        with open(st, "rb") as sticker_file:
            await bot.send_sticker(chat_id=chat_id, sticker=sticker_file)
        await bot.send_message(chat_id=chat_id, text=cap)


async def send_daily_analytics(bot):
    da = daily_analytics
    now = datetime.now(ZoneInfo("Europe/Kyiv"))
    data = da.load()
    if data.get("sent_evening"):
        return
    if now.hour != 21:
        return
    text = da.build_text()
    data["sent_evening"] = True
    da.save(data)
    for key in list(user_settings.keys()):
        cid = int(key)
        if not monitoring_enabled(cid):
            continue
        if get_settings(cid).get("city") not in ("Dnipro", "Tsarychanka"):
            continue
        try:
            await send_message(bot, cid, text, auto_delete=False)
        except Exception:
            logger.exception("daily analytics")



async def chek_command(update, context):
    if not await is_bot_admin(update):
        return

    total = sum(1 for k in user_settings if str(k).lstrip("-").isdigit() and int(k)>0)
    on = 0
    for key in list(user_settings.keys()):
        try:
            if monitoring_enabled(int(key)):
                on += 1
        except (TypeError, ValueError):
            continue
    text = (
        f"Людей: {total}\n"
        f"Моніторинг увімкнено: {on}"
    )
    await update.message.reply_text(text)


async def testdaystats_command(update, context):
    if not await is_bot_admin(update):
        return

    await update.message.reply_text(
        daily_analytics.build_text(),
        parse_mode="HTML",
    )


async def testday_command(update, context):
    if not await is_bot_admin(update):
        return

    day=get_war_day()
    await update.message.reply_text("🌅 <b>НОВИЙ ДЕНЬ</b>\n\nНа один день ближче до смерті хуйла.", parse_mode="HTML")
    await update.message.reply_text("🇺🇦 <b>ДЕНЬ ВІЙНИ — "+str(day)+"</b>\n\n🕯️ <b>09:00 — хвилина мовчання</b>\nВшануймо полеглих Героїв.\n\n🤍 Ще один день до Перемоги.\n<b>Памятаємо. Тримаємося. Переможемо.</b>", parse_mode="HTML")

async def test_command(update, context):
    if not await is_bot_admin(update):
        return

    samples=[
    "🚨 <b>ТРИВОГА САМЕ У ДНІПРІ</b>\n\n📍 <b>Місто Дніпро</b>\n\n⚠️ Негайно пройдіть в укриття.",
    "🟠 <b>ЗАГРОЗА В РАДІУСІ</b>\n\n🎯 <b>ЦІЛЬ №3</b>\n🛸 <b>БпЛА</b>\n📏 Відстань: <b>~12 км</b>\n🏙 Дніпро, <b>лівий берег</b>\n🎯 <b>Курс у вашу сторону</b>",
    "🛸 <b>Локально</b>\nНагорка",
    "ℹ️ <b>Нічний режим</b>\nЗ 00:00 до 06:00 лише сирена в місті та ціль ближче 30 км."
    ]
    for s in samples:
        await update.message.reply_text(s, parse_mode="HTML")

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.effective_chat is None or update.message is None:
        return

    chat_id = update.effective_chat.id
    get_settings(chat_id)
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    await update.message.reply_text(
        (
            "🛡️ <b>ДНІПРО • AIR MONITOR</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"📍 Точка: <b>{html.escape(city_name)}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
            "🚨 Бот відстежує доступні дані "
            "про повітряні загрози.\n\n"
            "🔴 <b>0–50 км</b> — загроза поруч\n"
            "🟠 <b>понад 50 км</b> — загроза в межах радіуса\n\n"
            "⚙️ Місто та радіус можна змінити кнопками нижче.\n\n"
            "⚠️ <i>Інформація є додатковою. "
            "Завжди орієнтуйтеся на офіційні "
            "сигнали повітряної тривоги.</i>"
        ),
        parse_mode="HTML",
        reply_markup=await keyboard_for_user(
            update,
            context,
        ),
    )


async def status_text(chat_id):
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    try:
        alert, alert_since = await asyncio.to_thread(
            get_alert_state,
            chat_id,
        )
    except Exception:
        logger.exception("Не вдалося отримати стан тривоги")
        return (
            "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ СТАН</b>\n\n"
            "Спробуйте оновити пізніше."
        )

    if alert:
        return (
            "🔴 <b>ПОВІТРЯНА ТРИВОГА</b>\n\n"
            f"📍 Точка моніторингу: <b>{html.escape(city_name)}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
            "⚠️ Перебувайте в укритті."
        )

    return (
        "🟢 <b>ТРЕВОГИ НЕ ВИЯВЛЕНО</b>\n\n"
        f"📍 Точка моніторингу: <b>{html.escape(city_name)}</b>\n"
        f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
        "Система продовжує моніторинг."
    )


async def situation_text(chat_id):
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    try:
        alert, alert_since = await asyncio.to_thread(
            get_alert_state,
            chat_id,
        )
    except Exception:
        logger.exception(
            "Не вдалося отримати обстановку"
        )

        return (
            "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ "
            "ОБСТАНОВКУ</b>"
        )

    level, level_description = danger_level(
        alert
    )

    today_count, today_seconds = (
        get_alert_statistics(
            chat_id,
            days=1,
        )
    )

    week_count, week_seconds = (
        get_alert_statistics(
            chat_id,
            days=7,
        )
    )

    if alert_since:
        start_text = format_kyiv_time(
            alert_since
        )
    else:
        start_text = "—"

    return (
        "🧠 <b>ОБСТАНОВКА</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"📍 Точка: <b>{html.escape(city_name)}</b>\n"
        f"🚨 Стан: {level}\n"
        f"ℹ️ {level_description}\n\n"
        "📊 <b>СЬОГОДНІ</b>\n"
        f"🚨 Тривог: <b>{today_count}</b>\n"
        "⏱ Загальна тривалість: "
        f"<b>{format_duration(today_seconds)}</b>\n\n"
        "📅 <b>ОСТАННІ 7 ДНІВ</b>\n"
        f"🚨 Тривог: <b>{week_count}</b>\n"
        "⏱ Загальна тривалість: "
        f"<b>{format_duration(week_seconds)}</b>\n\n"
        "🕐 Початок поточної тривоги: "
        f"<b>{start_text}</b>\n\n"
        "⚠️ <i>Інформація додаткова. "
        "Орієнтуйтеся на офіційні сигнали.</i>"
    )


async def history_text(chat_id):
    history = get_alert_history(chat_id)

    if not history:
        return (
            "📋 <b>ІСТОРІЯ ПОРОЖНЯ</b>\n\n"
            "Подій ще не зафіксовано."
        )

    lines = [
        "📋 <b>ІСТОРІЯ ТРИВОГ</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for item in reversed(history[-30:]):
        try:
            dt = datetime.fromisoformat(
                item["time"]
            )

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=ZoneInfo("Europe/Kyiv")
                )

            dt = dt.astimezone(
                ZoneInfo("Europe/Kyiv")
            )
        except Exception:
            continue

        event_type = item.get("type")

        if event_type == "start":
            icon = "🔴"
            text = "Оголошено тривогу"
        elif event_type == "end":
            icon = "🟢"
            text = "Відбій тривоги"
        elif event_type == "dnipro_city_start":
            icon = "🚨"
            text = "Початок тривоги у Дніпрі"
        elif event_type == "dnipro_city_end":
            icon = "✅"
            text = "Відбій тривоги у Дніпрі"
        else:
            continue

        lines.append(
            f"{icon} {dt.strftime('%d.%m %H:%M')} — {text}"
        )

    return "\n".join(lines)


async def event_history_text(chat_id):
    history = get_alert_history(chat_id)

    if not history:
        return (
            "📋 <b>ЖУРНАЛ ПОРОЖНІЙ</b>\n\n"
            "Подій ще не зафіксовано."
        )

    lines = [
        "📋 <b>ЖУРНАЛ ПОДІЙ</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for item in reversed(history[-50:]):
        try:
            dt = datetime.fromisoformat(
                item["time"]
            )

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=ZoneInfo("Europe/Kyiv")
                )

            dt = dt.astimezone(
                ZoneInfo("Europe/Kyiv")
            )
        except Exception:
            continue

        event_type = item.get("type")

        if event_type == "start":
            icon = "🔴"
            text = "Початок повітряної тривоги"
        elif event_type == "end":
            icon = "🟢"
            text = "Відбій повітряної тривоги"
        elif event_type == "dnipro_city_start":
            icon = "🚨"
            text = "Початок тривоги саме у Дніпрі"
        elif event_type == "dnipro_city_end":
            icon = "✅"
            text = "Відбій тривоги саме у Дніпрі"
        else:
            icon = "ℹ️"
            text = str(event_type or "Невідома подія")

        lines.append(
            f"{icon} {dt.strftime('%d.%m %H:%M')} — {text}"
        )

    return "\n".join(lines)


async def statistics_text(chat_id):
    today_count, today_seconds = (
        get_alert_statistics(
            chat_id,
            days=1,
        )
    )

    week_count, week_seconds = (
        get_alert_statistics(
            chat_id,
            days=7,
        )
    )

    month_count, month_seconds = (
        get_alert_statistics(
            chat_id,
            days=30,
        )
    )

    return (
        "📊 <b>СТАТИСТИКА ТРИВОГ</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "📅 <b>СЬОГОДНІ</b>\n"
        f"🚨 Кількість: <b>{today_count}</b>\n"
        "⏱ Тривалість: "
        f"<b>{format_duration(today_seconds)}</b>\n\n"
        "📅 <b>ОСТАННІ 7 ДНІВ</b>\n"
        f"🚨 Кількість: <b>{week_count}</b>\n"
        "⏱ Тривалість: "
        f"<b>{format_duration(week_seconds)}</b>\n\n"
        "📅 <b>ОСТАННІ 30 ДНІВ</b>\n"
        f"🚨 Кількість: <b>{month_count}</b>\n"
        "⏱ Тривалість: "
        f"<b>{format_duration(month_seconds)}</b>"
    )


async def points_text(chat_id):
    city_name, latitude, longitude, _ = (
        city_info(chat_id)
    )

    return (
        "📍 <b>ТОЧКА МОНІТОРИНГУ</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"🏙 Місто: <b>{html.escape(city_name)}</b>\n"
        f"🌐 Координати: "
        f"<b>{latitude:.4f}, {longitude:.4f}</b>\n"
        f"📏 Радіус: "
        f"<b>{html.escape(radius_label(chat_id))}</b>\n\n"
        "Налаштування застосовуються для цього чату."
    )


async def danger_text(chat_id):
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    try:
        alert, alert_since = await asyncio.to_thread(
            get_alert_state,
            chat_id,
        )
    except Exception:
        logger.exception(
            "Не вдалося отримати рівень небезпеки"
        )

        return (
            "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ "
            "РІВЕНЬ НЕБЕЗПЕКИ</b>"
        )

    level, level_description = danger_level(
        alert
    )

    start_text = (
        format_kyiv_time(alert_since)
        if alert_since
        else "—"
    )

    return (
        "🚨 <b>РІВЕНЬ НЕБЕЗПЕКИ</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"📍 Точка: <b>{html.escape(city_name)}</b>\n"
        f"⚠️ Рівень: {level}\n"
        f"ℹ️ {level_description}\n\n"
        f"🕐 Початок поточної тривоги: "
        f"<b>{start_text}</b>\n\n"
        "Орієнтуйтеся на офіційні сигнали "
        "повітряної тривоги."
    )


async def toggle_quiet_mode(
    chat_id,
    message,
    reply_markup=None,
):
    enabled = not is_quiet_mode(chat_id)

    set_quiet_mode(
        chat_id,
        enabled,
    )

    if enabled:
        text = (
            "🔕 <b>ТИХИЙ РЕЖИМ УВІМКНЕНО</b>\n\n"
            "Бот продовжить моніторинг та "
            "збиратиме статистику.\n\n"
            "Сповіщення про зміну стану тривоги "
            "тимчасово не надсилатимуться."
        )
    else:
        text = (
            "🔔 <b>ТИХИЙ РЕЖИМ ВИМКНЕНО</b>\n\n"
            "Сповіщення знову активні."
        )

    await message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=(
            reply_markup
            if reply_markup is not None
            else main_keyboard()
        ),
    )


async def history_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if (
        update.effective_chat is None
        or update.message is None
    ):
        return

    chat_id = update.effective_chat.id
    history = get_alert_history(chat_id)

    if not history:
        await update.message.reply_text(
            "📋 <b>ІСТОРІЯ ПОРОЖНЯ</b>\n\n"
            "Подій ще не зафіксовано.",
            parse_mode="HTML",
            reply_markup=await keyboard_for_user(
                update,
                context,
            ),
        )
        return

    lines = [
        "📋 <b>ІСТОРІЯ ТРИВОГ</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for item in reversed(history[-30:]):
        try:
            dt = datetime.fromisoformat(
                item["time"]
            ).astimezone(
                ZoneInfo("Europe/Kyiv")
            )
        except Exception:
            continue

        if item["type"] == "start":
            icon = "🔴"
            text = "Оголошено тривогу"
        else:
            icon = "🟢"
            text = "Відбій тривоги"

        lines.append(
            f"{icon} {dt.strftime('%d.%m %H:%M')} — {text}"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=await keyboard_for_user(
            update,
            context,
        ),
    )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.effective_chat is None or update.message is None:
        return

    chat_id = update.effective_chat.id
    get_settings(chat_id)
    await update.message.reply_text(
        await status_text(chat_id),
        parse_mode="HTML",
        reply_markup=await keyboard_for_user(
            update,
            context,
        ),
    )


async def threats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.effective_chat is None or update.message is None:
        return

    chat_id = update.effective_chat.id
    get_settings(chat_id)

    try:
        nearby = await asyncio.to_thread(
            get_nearby_threats_for_chat,
            chat_id,
        )
        text = format_threat_list(chat_id, nearby)
    except Exception:
        logger.exception("Не вдалося отримати загрози")
        text = "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ ДАНІ ПРО ЗАГРОЗИ</b>"

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=await keyboard_for_user(
            update,
            context,
        ),
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.effective_chat is None or update.message is None:
        return

    chat_id = update.effective_chat.id
    city_name, target_lat, target_lon, _ = city_info(chat_id)

    await update.message.reply_text(
        (
            "ℹ️ <b>ЯК ПРАЦЮЄ БОТ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"📍 Місто: <b>{html.escape(city_name)}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
            "Кожен користувач може окремо вибрати "
            "місто та радіус.\n\n"
            "🔴 <b>0–50 км</b> — червоне позначення.\n"
            "🟠 <b>понад 50 км</b> — помаранчеве позначення.\n"
            "🇺🇦 <b>Вся Україна</b> — усі доступні загрози.\n\n"
            "⚠️ <b>УВАГА:</b> бот не замінює "
            "офіційні сигнали повітряної тривоги."
        ),
        parse_mode="HTML",
        reply_markup=await keyboard_for_user(
            update,
            context,
        ),
    )


async def location_handler(update, context):
    if update.message is None or update.message.location is None: return
    chat_id=update.effective_chat.id
    lat=float(update.message.location.latitude); lon=float(update.message.location.longitude)
    s=get_settings(chat_id)
    s["use_custom_location"]=True; s["custom_lat"]=lat; s["custom_lon"]=lon; s["city"]=nearest_city_key(lat,lon); save_settings()
    await update.message.reply_text("✅ Локацію збережено. Курс рахується від вас.", reply_markup=await keyboard_for_user(update, context))

async def text_button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if (
        update.effective_chat is None
        or update.message is None
        or not update.message.text
    ):
        return

    chat_id = update.effective_chat.id
    get_settings(chat_id)
    button = update.message.text

    if (
        button in RESTRICTED_BUTTONS
        and not await can_use_keyboard(update, context)
    ):
        await update.message.reply_text(
            "⛔️ Керування доступне лише власнику "
            "або адміністратору чату."
        )
        return

    keyboard = await keyboard_for_user(
        update,
        context,
    )

    if button == "❌ Вийти":
        await update.message.reply_text(
            "✅ <b>Клавіатуру приховано.</b>\n\n"
            "Щоб повернути меню, надішліть команду /start.",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove(),
        )
        return


    if button == "⚙️ Налаштування":
        await update.effective_message.reply_text(
            "Налаштування:",
            reply_markup=settings_keyboard(),
        )
        return

    if button == "⬅️ Назад":
        await update.effective_message.reply_text(
            "Головне меню",
            reply_markup=await keyboard_for_user(update, context),
        )
        return

    if button == "▶️ Старт":
        await start_monitoring_button(
            update,
            chat_id,
            keyboard,
        )
        return

    if button == "⏹ Стоп":
        await stop_monitoring_button(
            update,
            chat_id,
            keyboard,
        )
        return

    if button == "🚨 Стан":
        await update.message.reply_text(
            await status_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "📊 Обстановка":
        await update.message.reply_text(
            await situation_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "📈 Історія":
        await update.message.reply_text(
            await history_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "📋 Журнал":
        await update.message.reply_text(
            await event_history_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "📊 Статистика":
        await update.message.reply_text(
            await statistics_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "📍 Точки":
        await update.message.reply_text(
            await points_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "🔕 Тихий режим":
        await toggle_quiet_mode(
            chat_id,
            update.message,
            keyboard,
        )
        return

    if button == "🚨 Небезпека":
        await update.message.reply_text(
            await danger_text(chat_id),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button in ("🛸 Загрози", "🔄 Оновити"):
        try:
            nearby = await asyncio.to_thread(
                get_nearby_threats_for_chat,
                chat_id,
            )
            text = format_threat_list(chat_id, nearby)
        except Exception:
            logger.exception("Не вдалося оновити загрози")
            text = "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ ДАНІ ПРО ЗАГРОЗИ</b>"

        await update.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    if button == "ℹ️ Допомога":
        await help_command(update, context)
        return

    if button in ("📍 Місто", "📌 Моя локація"):
        await update.message.reply_text(
            "📍 <b>ОБЕРІТЬ МІСТО МОНІТОРИНГУ</b>",
            parse_mode="HTML",
            reply_markup=(
                city_keyboard()
                if isinstance(
                    keyboard,
                    ReplyKeyboardMarkup,
                )
                else ReplyKeyboardRemove()
            ),
        )
        return

    if button == "📏 Радіус":
        await update.message.reply_text(
            "📏 <b>ОБЕРІТЬ РАДІУС МОНІТОРИНГУ</b>",
            parse_mode="HTML",
            reply_markup=(
                radius_keyboard()
                if isinstance(
                    keyboard,
                    ReplyKeyboardMarkup,
                )
                else ReplyKeyboardRemove()
            ),
        )
        return

    city_key = next(
        (
            key
            for key, (city_name, _, _, _) in CITIES.items()
            if city_name == button
        ),
        None,
    )
    if city_key is not None:
        user_settings[str(chat_id)]["city"] = city_key
        user_settings[str(chat_id)]["use_custom_location"] = False
        save_settings()
        await update.message.reply_text(
            (
                "✅ <b>МІСТО ЗМІНЕНО</b>\n\n"
                f"📍 Точка моніторингу: "
                f"<b>{html.escape(city_info(chat_id)[0])}</b>\n"
                f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>"
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    radius_key = {
        "🔴 10 км": "10",
        "🔴 50 км": "50",
        "🟠 100 км": "100",
        "🟡 200 км": "200",
        "🟢 500 км": "500",
        "🇺🇦 Вся Україна": "ukraine",
    }.get(button)
    if radius_key is not None:
        user_settings[str(chat_id)]["radius"] = radius_key
        save_settings()
        await update.message.reply_text(
            (
                "✅ <b>РАДІУС ЗМІНЕНО</b>\n\n"
                f"📍 Точка: <b>{html.escape(city_info(chat_id)[0])}</b>\n"
                f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
                "Налаштування збережено для цього чату."
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
        )


async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if query is None or query.message is None:
        return

    if not await can_use_keyboard(update, context):
        await query.answer(
            "Кнопки доступні лише власнику "
            "або адміністратору чату.",
            show_alert=True,
        )
        return

    await query.answer()
    chat_id = query.message.chat_id
    get_settings(chat_id)
    menu_keyboard = await keyboard_for_user(
        update,
        context,
    )

    if query.data == "status":
        text = await status_text(chat_id)
        markup = menu_keyboard

    elif query.data in ("threats", "refresh"):
        try:
            nearby = await asyncio.to_thread(
                get_nearby_threats_for_chat,
                chat_id,
            )
            text = format_threat_list(chat_id, nearby)
        except Exception:
            logger.exception("Не вдалося оновити загрози")
            text = "⚠️ <b>НЕ ВДАЛОСЯ ОТРИМАТИ ДАНІ ПРО ЗАГРОЗИ</b>"
        markup = menu_keyboard

    elif query.data == "city":
        text = "📍 <b>ОБЕРІТЬ МІСТО МОНІТОРИНГУ</b>"
        markup = city_keyboard()

    elif query.data == "radius":
        text = "📏 <b>ОБЕРІТЬ РАДІУС МОНІТОРИНГУ</b>"
        markup = radius_keyboard()

    elif query.data.startswith("city:"):
        city_key = query.data.split(":", 1)[1]

        if city_key not in CITIES:
            return

        user_settings[str(chat_id)]["city"] = city_key
        save_settings()
        city_name, target_lat, target_lon, _ = city_info(chat_id)

        text = (
            "✅ <b>МІСТО ЗМІНЕНО</b>\n\n"
            f"📍 Точка моніторингу: <b>{html.escape(city_name)}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>"
        )
        markup = menu_keyboard

    elif query.data.startswith("radius:"):
        radius_key = query.data.split(":", 1)[1]

        if radius_key not in RADIUS_OPTIONS:
            return

        user_settings[str(chat_id)]["radius"] = radius_key
        save_settings()

        text = (
            "✅ <b>РАДІУС ЗМІНЕНО</b>\n\n"
            f"📍 Точка: <b>{html.escape(city_info(chat_id)[0])}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
            "Налаштування збережено для цього чату."
        )
        markup = menu_keyboard

    elif query.data == "help":
        text = (
            "ℹ️ <b>ЯК ПРАЦЮЄ БОТ</b>\n\n"
            f"📍 Місто: <b>{html.escape(city_info(chat_id)[0])}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>\n\n"
            "Місто та радіус можна змінити кнопками нижче.\n\n"
            "⚠️ Бот не замінює офіційні сигнали тривоги."
        )
        markup = menu_keyboard

    elif query.data == "back":
        text = (
            "🛡️ <b>ДНІПРО • AIR MONITOR</b>\n\n"
            f"📍 Точка: <b>{html.escape(city_info(chat_id)[0])}</b>\n"
            f"📏 Радіус: <b>{html.escape(radius_label(chat_id))}</b>"
        )
        markup = menu_keyboard

    else:
        return

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=(
            markup
            if isinstance(markup, InlineKeyboardMarkup)
            else None
        ),
    )

    if isinstance(markup, ReplyKeyboardMarkup):
        await send_message(
            context.bot,
            chat_id,
            "⬇️ <b>Оберіть наступну дію</b>",
            reply_markup=markup,
        )


async def check_city_entry(
    bot,
    chat_key,
    chat_id,
    city_name,
    target_lat,
    target_lon,
    all_threats,
):
    """
    Перевіряє, чи нова ціль увійшла
    в орієнтовну зону міста.
    """

    previous_inside = city_threats_inside.get(
        chat_key,
        set(),
    )

    current_inside = set()

    for lat, lon, threat in all_threats:
        threat_id = str(threat.get("id"))

        if not threat_id:
            continue

        distance = distance_km(
            target_lat,
            target_lon,
            lat,
            lon,
        )

        if distance <= CITY_ENTRY_RADIUS_KM:
            current_inside.add(threat_id)

    if previous_inside != current_inside:
        city_threats_inside[chat_key] = current_inside
        mark_runtime_state_dirty()


async def notify_dnipro_city_alert(
    bot,
    chat_key,
    chat_id,
    source_state,
):
    state = source_state.get("state")
    if get_settings(chat_id).get("city") == "Tsarychanka":
        state = source_state.get("tsar_state", state)

    if state is None:
        return

    previous_state = last_dnipro_city_alert_states.get(
        chat_key,
        False,
    )

    if state == previous_state:
        return

    post_id = source_state.get("post_id")

    channel = source_state.get(
        "channel",
        FAST_DNIPRO_ALERT_CHANNEL,
    )

    if post_id:
        source_url = (
            f"https://t.me/{channel}/"
            f"{post_id}"
        )
    else:
        source_url = (
            f"https://t.me/{channel}"
        )

    source_name = "@" + channel
    is_tsarychanka = (
        get_settings(chat_id).get("city") == "Tsarychanka"
    )
    place = (
        "Царичанка"
        if is_tsarychanka
        else "Місто Дніпро"
    )
    title_place = "У ЦАРИЧАНЦІ" if is_tsarychanka else "САМЕ У ДНІПРІ"

    if state:
        event_type = "dnipro_city_start"
        text = (
            f"🚨 <b>ТРИВОГА {title_place}</b>\n\n"
            f"📍 <b>{place}</b> — оголошено тривогу.\n"
            "⚠️ Негайно пройдіть в укриття.\n\n"
            f"🔗 Джерело: "
            f"<a href='{source_url}'>"
            f"{html.escape(source_name)}</a>\n"
        )
    else:
        event_type = "dnipro_city_end"
        text = (
            f"✅ <b>ВІДБІЙ ТРИВОГИ {title_place}</b>\n\n"
            f"📍 <b>{place}</b> — зафіксовано відбій.\n\n"
            f"🔗 Джерело: "
            f"<a href='{source_url}'>"
            f"{html.escape(source_name)}</a>\n"
        )

    if is_quiet_mode(chat_id):
        last_dnipro_city_alert_states[chat_key] = state
        add_alert_event(chat_id, event_type)
        mark_runtime_state_dirty()
        return

    try:
        st=MEDIA_DIR / ("tryvoga.webp" if state else "vidbiy.webp")
        if st.exists():
            with open(st, "rb") as sticker_file:
                await bot.send_sticker(
                    chat_id=chat_id,
                    sticker=sticker_file,
                )
        await send_message(bot, chat_id, text)
    except TimedOut:
        logger.warning(
            "Результат міського сповіщення для "
            "чату %s невідомий; повтор пропущено, "
            "щоб уникнути дублювання",
            chat_id,
        )

    last_dnipro_city_alert_states[chat_key] = state
    add_alert_event(chat_id, event_type)
    mark_runtime_state_dirty()


async def monitor_alerts_for_chat(
    bot,
    chat_key,
    chat_id,
    alert_data,
    dnipro_city_source_state,
):
    if alert_data is not None:
        try:
            city_name, target_lat, target_lon, _ = city_info(chat_id)
            alert, alert_since = (
                get_alert_state_from_data(
                    chat_id,
                    alert_data,
                )
            )
            previous_alert = last_alert_states.get(
                chat_key,
                False,
            )

            if alert and alert_since:
                alert_start_times[chat_key] = alert_since
            elif not alert:
                alert_start_times.pop(chat_key, None)

            transition_recorded = (
                alert == previous_alert
            )

            if alert != previous_alert:
                if alert:
                    event_type = "start"
                    text = (
                        "🚨 <b>ПОВІТРЯНА ТРИВОГА</b>\n\n"
                        f"📍 <b>{html.escape(city_name)}</b>\n\n"
                        "⚠️ Негайно пройдіть в укриття.\n"
                        "Не ігноруйте офіційний сигнал тривоги."
                    )
                else:
                    event_type = "end"
                    text = (
                        "🟢 <b>ВІДБІЙ ТРИВОГИ</b>\n\n"
                        f"📍 <b>{html.escape(city_name)}</b>\n\n"
                        "Бережіть себе."
                    )

                try:
                    if (
                        is_quiet_mode(chat_id)
                        or is_night_hours()
                    ):
                        transition_recorded = True
                    else:
                        st=MEDIA_DIR / ("tryvoga.webp" if alert else "vidbiy.webp")
                        if st.exists():
                            with open(st, "rb") as sticker_file:
                                await bot.send_sticker(
                                    chat_id=chat_id,
                                    sticker=sticker_file,
                                )
                        await send_message(
                            bot,
                            chat_id,
                            text,
                        )
                        transition_recorded = True
                except TimedOut:
                    transition_recorded = True
                    logger.warning(
                        "Результат сповіщення про тривогу "
                        "для чату %s невідомий; повтор "
                        "пропущено, щоб уникнути дублювання",
                        chat_id,
                    )
                except Exception:
                    logger.exception(
                        "Не вдалося надіслати "
                        "стан тривоги"
                    )

                if transition_recorded:
                    add_alert_event(
                        chat_id,
                        event_type,
                    )

            if transition_recorded:
                last_alert_states[chat_key] = alert

                if alert != previous_alert:
                    mark_runtime_state_dirty()
                if ANALYTICS_CHAT_ID and str(chat_id)==ANALYTICS_CHAT_ID:
                    try:
                        daily_analytics.note_alert(
                            bool(previous_alert),
                            bool(alert),
                        )
                    except OSError:
                        logger.exception("daily analytics")
        except Exception:
            logger.exception(
                "Не вдалося перевірити стан тривоги "
                f"для чату {chat_key}"
            )

    if get_settings(chat_id).get("city") in ("Dnipro", "Tsarychanka"):
        try:
            await notify_dnipro_city_alert(
                bot,
                chat_key,
                chat_id,
                dnipro_city_source_state,
            )
        except Exception:
            logger.exception(
                "Не вдалося надіслати окремий "
                "стан тривоги у Дніпрі"
            )


async def monitor_threats_for_chat(
    bot,
    chat_key,
    chat_id,
    all_threats,
    now,
):
    city_name, target_lat, target_lon, _ = city_info(
        chat_id
    )
    radius = radius_value(chat_id)
    visible = []

    await check_city_entry(
        bot,
        chat_key,
        chat_id,
        city_name,
        target_lat,
        target_lon,
        all_threats,
    )

    for lat, lon, threat in all_threats:
        distance = distance_km(
            target_lat,
            target_lon,
            lat,
            lon,
        )

        if radius is None or distance <= radius:
            visible.append((distance, threat))

    visible.sort(key=lambda item: item[0])
    previous_ids = known_threats.get(
        chat_key,
        {},
    )
    current_threats = {}

    for distance, threat in visible:
        if ANALYTICS_CHAT_ID and str(chat_id)==ANALYTICS_CHAT_ID:
            try:
                daily_analytics.note_threat(threat.get('id'), distance)
            except OSError:
                logger.exception("daily analytics")
        threat_id = str(threat.get("id") or "")

        if not threat_id:
            continue

        current_threats[threat_id] = distance
        previous_distance = previous_ids.get(
            threat_id
        )
        state_key = f"{chat_key}:{threat_id}"
        previous_time = last_update_time.setdefault(
            state_key,
            now,
        )

        try:
            if previous_distance is None:
                if (
                    is_night_hours()
                    and distance > NIGHT_THREAT_RADIUS_KM
                ):
                    last_update_time[state_key] = now
                    continue
                await send_message(
                    bot,
                    chat_id,
                    format_threat_message(
                        chat_id,
                        distance,
                        threat,
                    ),
                )
                last_update_time[state_key] = now
            elif abs(
                distance - previous_distance
            ) >= DISTANCE_CHANGE_KM:
                text = format_threat_message(
                    chat_id,
                    distance,
                    threat,
                    update=True,
                )

                text += (
                    "\n\n📊 <b>ЗМІНА ДИСТАНЦІЇ</b>\n"
                    "Було: "
                    f"<b>~{round(previous_distance)} км</b>\n"
                    "Стало: "
                    f"<b>~{round(distance)} км</b>\n"
                )

                if distance < previous_distance:
                    text += "🔻 Ціль наближається."
                else:
                    text += "🔺 Ціль віддаляється."

                if (
                    is_night_hours()
                    and distance > NIGHT_THREAT_RADIUS_KM
                ):
                    last_update_time[state_key] = now
                    continue
                await send_message(
                    bot,
                    chat_id,
                    text,
                )
                last_update_time[state_key] = now
            elif (
                now - previous_time
                >= UPDATE_INTERVAL_SECONDS
            ):
                await send_message(
                    bot,
                    chat_id,
                    format_threat_message(
                        chat_id,
                        distance,
                        threat,
                        update=True,
                    ),
                )
                last_update_time[state_key] = now
        except TimedOut:
            logger.warning(
                "Результат оновлення цілі %s для "
                "чату %s невідомий; повтор пропущено, "
                "щоб уникнути дублювання",
                threat_id,
                chat_id,
            )
        except Exception:
            if previous_distance is None:
                current_threats.pop(
                    threat_id,
                    None,
                )
            else:
                current_threats[
                    threat_id
                ] = previous_distance

            logger.exception(
                "Не вдалося надіслати "
                f"оновлення цілі {threat_id}"
            )

    for gone_id in list(previous_ids):
        if gone_id not in current_threats:
            last_update_time.pop(f"{chat_key}:{gone_id}", None)

            try:
                await send_message(bot, chat_id, "✅ Ціль №"+str(target_numbers.get(str(gone_id), gone_id))+" більше не в радіусі.")
            except Exception:
                logger.exception(
                    "Не вдалося повідомити про зникнення цілі"
                )
    if known_threats.get(chat_key) != current_threats:
        known_threats[chat_key] = current_threats
        mark_runtime_state_dirty()


async def monitor_once(bot):
    active_chats = []

    for chat_key in tuple(user_settings.keys()):
        chat_id = int(chat_key)

        if monitoring_enabled(chat_id):
            active_chats.append((chat_key, chat_id))

    if not active_chats:
        return

    global last_local_channel_poll
    now_monotonic = time.monotonic()

    if (
        now_monotonic - last_local_channel_poll
        >= LOCAL_CHANNEL_POLL_INTERVAL_SECONDS
    ):
        last_local_channel_poll = now_monotonic

        try:
            await asyncio.to_thread(collect_kucher_local)
            await asyncio.to_thread(collect_nyi_local)
            await asyncio.to_thread(collect_dniproal_local)
        except Exception:
            logger.exception("kucher local")

    if pending_kucher_locals:
        texts=ua_siren.pick_locals(list(pending_kucher_locals))
        pending_kucher_locals.clear()
        for chat_key, chat_id in active_chats:
            if is_quiet_mode(chat_id): continue
            st=get_settings(chat_id)
            city=st.get("city")
            if city not in ("Dnipro", "Tsarychanka") and not st.get("use_custom_location"): continue
            for raw in texts:
                try:
                    cut=raw.split("Дніпро Alerts")[0].split("НУ І ДНІПРО")[0].split("t.me/")[0].strip(" •\n")
                    if cut:
                        if city=="Tsarychanka":
                            low=cut.lower()
                            keys=("царич","магдал","китайгород","піщан","личков","багате")
                            if not any(k in low for k in keys): continue
                        await send_message(bot, chat_id, "🛸 <b>Локально</b>\n"+html.escape(cut))
                except Exception: logger.exception("kucher send")

    dnipro_city_source_state = {
        "state": None,
        "tsar_state": None,
        "post_id": None,
    }
    has_dnipro_chat = any(
        get_settings(chat_id).get("city")
        in ("Dnipro", "Tsarychanka")
        for _, chat_id in active_chats
    )
    source_tasks = [
        asyncio.to_thread(
            fetch_json,
            ALERTS_URL,
        ),
        asyncio.to_thread(
            get_all_active_threats,
        ),
    ]

    if has_dnipro_chat:
        source_tasks.append(
            asyncio.to_thread(
                get_dnipro_city_alert_state
            )
        )

    source_results = await asyncio.gather(
        *source_tasks,
        return_exceptions=True,
    )

    alert_data = None
    all_threats = None
    alert_result = source_results[0]
    threat_result = source_results[1]

    if isinstance(alert_result, BaseException):
        logger.error(
            "Не вдалося отримати стан тривог: %s",
            type(alert_result).__name__,
            exc_info=(
                type(alert_result),
                alert_result,
                alert_result.__traceback__,
            ),
        )
    else:
        alert_data = alert_result

    if isinstance(threat_result, BaseException):
        logger.error(
            "Не вдалося отримати загрози: %s",
            type(threat_result).__name__,
            exc_info=(
                type(threat_result),
                threat_result,
                threat_result.__traceback__,
            ),
        )
    else:
        all_threats = threat_result

    if has_dnipro_chat:
        city_result = source_results[2]

        if isinstance(city_result, BaseException):
            logger.error(
                "Не вдалося отримати окремий стан "
                "тривоги у Дніпрі з Telegram-каналу: %s",
                type(city_result).__name__,
                exc_info=(
                    type(city_result),
                    city_result,
                    city_result.__traceback__,
                ),
            )
        else:
            dnipro_city_source_state = (
                city_result
            )

    alert_semaphore = asyncio.Semaphore(
        CHAT_CONCURRENCY_LIMIT
    )
    alert_tasks = [
        run_bounded(
            alert_semaphore,
            monitor_alerts_for_chat,
            bot,
            chat_key,
            chat_id,
            alert_data,
            dnipro_city_source_state,
        )
        for chat_key, chat_id in active_chats
    ]
    alert_results = await asyncio.gather(
        *alert_tasks,
        return_exceptions=True,
    )

    for (chat_key, _), result in zip(
        active_chats,
        alert_results,
    ):
        if isinstance(result, BaseException):
            logger.error(
                "Обробка тривоги для чату %s "
                "аварійно завершилася: %s",
                chat_key,
                type(result).__name__,
                exc_info=(
                    type(result),
                    result,
                    result.__traceback__,
                ),
            )

    await asyncio.to_thread(save_runtime_state)

    if all_threats is None:
        return

    now = time.monotonic()
    threat_semaphore = asyncio.Semaphore(
        CHAT_CONCURRENCY_LIMIT
    )
    threat_tasks = [
        run_bounded(
            threat_semaphore,
            monitor_threats_for_chat,
            bot,
            chat_key,
            chat_id,
            all_threats,
            now,
        )
        for chat_key, chat_id in active_chats
    ]
    threat_results = await asyncio.gather(
        *threat_tasks,
        return_exceptions=True,
    )

    for (chat_key, _), result in zip(
        active_chats,
        threat_results,
    ):
        if isinstance(result, BaseException):
            logger.error(
                "Обробка загроз для чату %s "
                "аварійно завершилася: %s",
                chat_key,
                type(result).__name__,
                exc_info=(
                    type(result),
                    result,
                    result.__traceback__,
                ),
            )

    prune_target_numbers()
    await asyncio.to_thread(save_runtime_state)


async def monitor_loop(application):
    failed_cycles = 0

    while True:
        cycle_started = time.monotonic()

        try:
            await send_daily_morning(application.bot)
            await send_daily_silence(
                application.bot,
            )
            await send_daily_analytics(application.bot)

            await monitor_once(application.bot)
            if failed_cycles:
                logger.info(
                    "Цикл моніторингу відновився "
                    "після %s помилок",
                    failed_cycles,
                )
                failed_cycles = 0
        except asyncio.CancelledError:
            raise
        except Exception:
            failed_cycles += 1
            logger.exception("Помилка циклу моніторингу")

        await asyncio.to_thread(save_runtime_state)
        elapsed = time.monotonic() - cycle_started
        await asyncio.sleep(
            max(
                0.1,
                CHECK_INTERVAL_SECONDS - elapsed,
            )
        )


async def monitor_supervisor(application):
    restart_count = 0

    while True:
        try:
            logger.info("Фоновий моніторинг запущено")
            await monitor_loop(application)
            logger.error(
                "Фоновий моніторинг несподівано завершився"
            )
        except asyncio.CancelledError:
            logger.info("Фоновий моніторинг зупиняється")
            raise
        except Exception:
            logger.exception(
                "Фоновий моніторинг аварійно завершився"
            )

        restart_count += 1
        delay = retry_delay(
            restart_count - 1,
            MONITOR_RESTART_DELAY_SECONDS,
            TELEGRAM_SEND_RETRY_MAX_DELAY_SECONDS,
        )
        logger.warning(
            "Перезапуск фонового моніторингу "
            "через %ss",
            delay,
        )
        await asyncio.sleep(delay)


async def post_init(application):
    load_settings()
    load_runtime_state()
    restore_message_deletion_tasks(
        application.bot
    )
    existing_task = application.bot_data.get(
        "monitor_task"
    )

    if (
        existing_task is not None
        and not existing_task.done()
    ):
        logger.warning(
            "Фоновий моніторинг уже працює, "
            "повторний запуск пропущено"
        )
        return

    application.bot_data["monitor_task"] = asyncio.create_task(
        monitor_supervisor(application),
        name="dnipro-air-monitor-supervisor",
    )
    logger.info(
        "Налаштування завантажено, "
        "нагляд за моніторингом активний"
    )


async def post_shutdown(application):
    task = application.bot_data.get("monitor_task")

    if task is not None:
        task.cancel()
        await asyncio.gather(
            task,
            return_exceptions=True,
        )
        await asyncio.to_thread(
            save_runtime_state,
            True,
        )
        logger.info(
            "Фоновий моніторинг коректно зупинено"
        )


async def error_handler(update, context):
    error = context.error
    logger.error(
        "Помилка Telegram: %s",
        type(error).__name__,
        exc_info=(
            type(error),
            error,
            error.__traceback__,
        ),
    )


def build_application():
    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start_command)
    )
    application.add_handler(
        CommandHandler("testday", testday_command),
    )
    application.add_handler(
            CommandHandler("testdaystats", testdaystats_command),
        )
    application.add_handler(
            CommandHandler("chek", chek_command),
        )
    application.add_handler(
        CommandHandler("testcard", testcard_command)
    )
    application.add_handler(
        CommandHandler("test", test_command)
    )
    application.add_handler(
        CommandHandler("testmapa", testmapa_command)
    )
    application.add_handler(
        CommandHandler("status", status_command)
    )
    application.add_handler(
        CommandHandler(
            "history",
            history_command,
        )
    )
    application.add_handler(
        CommandHandler("threats", threats_command)
    )
    application.add_handler(
        CommandHandler("help", help_command)
    )
    application.add_handler(
        MessageHandler(filters.LOCATION, location_handler),
    )
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_button_handler,
        )
    )
    application.add_handler(
        CallbackQueryHandler(button_handler)
    )
    application.add_error_handler(error_handler)
    return application


def run_application_supervisor(application_factory):
    restart_count = 0

    while True:
        application = application_factory()

        try:
            application.run_polling(
                timeout=20,
                bootstrap_retries=10,
                close_loop=False,
            )
            return
        except InvalidToken:
            raise RuntimeError(
                "Telegram відхилив TELEGRAM_BOT_TOKEN. "
                "Перевірте токен."
            ) from None
        except Exception as error:
            restart_count += 1
            delay = retry_delay(
                restart_count - 1,
                MONITOR_RESTART_DELAY_SECONDS,
                TELEGRAM_SEND_RETRY_MAX_DELAY_SECONDS,
            )
            logger.error(
                "Життєвий цикл Telegram аварійно "
                "завершився (%s). Новий запуск через %ss",
                type(error).__name__,
                delay,
                exc_info=(
                    type(error),
                    error,
                    error.__traceback__,
                ),
            )
            time.sleep(delay)


async def testmapa_command(update, context):
    if update.effective_chat is None or update.message is None:
        return

    if not await is_bot_admin(update):
        return

    try:
        data = await asyncio.to_thread(mapa_client.fetch_current)
        chat_id = update.effective_chat.id
        _, lat, lon, _ = city_info(chat_id)
        radius = radius_value(chat_id) or 50
        near = mapa_client.nearby_threats(data, lat, lon, radius_km=radius)
        if not near:
            await update.message.reply_text("MAPA: no targets in " + str(int(radius)) + " km")
            return
        out = ["MAPA \u2022 " + str(len(near)) + " in " + str(int(radius)) + " km"]
        for o in near[:8]:
            title = o.get("title") or o.get("kind") or "?"
            dist = o.get("distance_km")
            heading = o.get("heading")
            part = "\u2022 " + str(title)
            if dist is not None:
                part += " | ~" + str(dist) + " km"
            if heading is not None:
                part += " | h " + str(heading)
            out.append(part)
        await update.message.reply_text("\n".join(out))
    except Exception as e:
        await update.message.reply_text("MAPA error: " + type(e).__name__ + ": " + str(e)[:200])


def main():
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не знайдено. "
            "Додайте його до Secrets."
        )

    print("================================")
    print("🛡️ DNIPRO AIR MONITOR")
    print("🟢 БОТ ЗАПУЩЕНИЙ")
    print("📍 Персональне місто")
    print("📏 50 / 100 / 200 / 500 км")
    print("🇺🇦 Вся Україна")
    print("================================")
    start_health_server()
    run_application_supervisor(build_application)


if __name__ == "__main__":
    main()
