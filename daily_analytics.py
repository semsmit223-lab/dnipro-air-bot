from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import json
import os

BASE_DIR = Path(
    os.getenv("BOT_DATA_DIR")
    or Path(__file__).resolve().parent
)
PATH = BASE_DIR / "daily_stats.json"
TZ = ZoneInfo("Europe/Kyiv")


def _today():
    return datetime.now(TZ).date().isoformat()


def _empty(day):
    return {
        "day": day,
        "sirens": 0,
        "alert_seconds": 0.0,
        "alert_open_ts": None,
        "threats": [],
        "min_km": None,
        "sent_evening": False,
    }


def load():
    day = _today()
    try:
        data = json.loads(PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        data = _empty(day)

    if not isinstance(data, dict):
        data = _empty(day)
    if data.get("day") != day:
        data = _empty(day)
    return data


def save(data):
    temporary_path = PATH.with_suffix(".tmp")

    try:
        temporary_path.write_text(
            json.dumps(data, ensure_ascii=False),
            encoding="utf-8",
        )
        os.replace(temporary_path, PATH)
    except OSError:
        pass


def note_alert(prev, cur):
    data = load()
    now = datetime.now(TZ).timestamp()
    if cur and not prev:
        data["sirens"] += 1
        if data.get("alert_open_ts") is None:
            data["alert_open_ts"] = now
    if prev and not cur and data.get("alert_open_ts"):
        data["alert_seconds"] += max(0, now - data["alert_open_ts"])
        data["alert_open_ts"] = None
    save(data)


def note_threat(tid, km):
    data = load()
    tid = str(tid)
    if tid not in data["threats"]:
        data["threats"].append(tid)
    try:
        km = float(km)
        if data.get("min_km") is None or km < data["min_km"]:
            data["min_km"] = km
    except (TypeError, ValueError):
        pass
    save(data)


def fmt_hm(sec):
    sec = int(max(0, sec))
    return f"{sec // 3600} год {(sec % 3600) // 60} хв"


def build_text():
    data = load()
    now = datetime.now(TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = (now - start).total_seconds()
    alert = data["alert_seconds"]
    if data.get("alert_open_ts"):
        alert += max(0, now.timestamp() - data["alert_open_ts"])
    alert = min(alert, elapsed)
    quiet = max(0, elapsed - alert)
    km = data.get("min_km")
    km_s = f"{km:.0f} км" if km is not None else "—"
    months = (
        "січня лютого березня квітня травня червня "
        "липня серпня вересня жовтня листопада грудня"
    ).split()
    title = f"{now.day} {months[now.month - 1]}"
    return (
        f"📊 <b>{title}</b>\n"
        f"Сирен: {data['sirens']}\n"
        f"У тривозі: {fmt_hm(alert)}\n"
        f"Без тривоги: {fmt_hm(quiet)}\n"
        f"Цілей біля Дніпра: {len(data['threats'])}\n"
        f"Найближча: {km_s}"
    )
