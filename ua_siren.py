"""Стан тривог за даними api.ukrainealarm.com."""

import os
import time
from pathlib import Path

import requests

BASE_DIR = Path(
    os.getenv("BOT_DATA_DIR")
    or Path(__file__).resolve().parent
)
ALERTS_URL = "https://api.ukrainealarm.com/api/v3/alerts"
CACHE_TTL_SECONDS = 20
REQUEST_TIMEOUT_SECONDS = 15

_cache = {"checked_at": 0.0, "blob": ""}


def _token():
    token = os.getenv("UKRAINEALARM_TOKEN")

    if token:
        return token.strip()

    try:
        raw = (BASE_DIR / ".env").read_text(encoding="utf-8")
    except OSError:
        return ""

    for line in raw.splitlines():
        if line.startswith("UKRAINEALARM_TOKEN="):
            return line.split("=", 1)[1].strip()

    return ""


def check():
    now = time.monotonic()

    if (
        _cache["checked_at"]
        and now - _cache["checked_at"] < CACHE_TTL_SECONDS
    ):
        return _cache["blob"]

    token = _token()

    if not token:
        raise RuntimeError("UKRAINEALARM_TOKEN не налаштовано")

    response = requests.get(
        ALERTS_URL,
        headers={"Authorization": token},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    items = response.json()
    blob = " ".join(
        str(item.get("regionName") or "").lower()
        for item in items
    )
    _cache["checked_at"] = now
    _cache["blob"] = blob
    return blob


def flags():
    """Повертає (тривога у Дніпрі, тривога у Царичанці)."""
    blob = check()
    city = any(
        marker in blob
        for marker in (
            "м. дніпро",
            "дніпровська територіальна",
            "дніпровська міська",
            "дніпровський район",
            "дніпропетровська область",
        )
    )
    tsar = city or any(
        marker in blob
        for marker in (
            "царичан",
            "смт царич",
            "селище царич",
            "царичанська",
        )
    )
    return city, tsar


def pick_locals(texts):
    """Прибирає дублікати локальних повідомлень."""
    kept = []

    for text in texts:
        text = (text or "").strip()

        if not text:
            continue

        normalized = "".join(
            char.lower() if char.isalnum() else " "
            for char in text
        )
        words = set(normalized.split())
        found = False

        for index, existing in enumerate(kept):
            normalized_existing = "".join(
                char.lower() if char.isalnum() else " "
                for char in existing
            )
            existing_words = set(normalized_existing.split())
            same = (
                normalized in normalized_existing
                or normalized_existing in normalized
            )

            if words and existing_words:
                overlap = len(words & existing_words) / min(
                    len(words),
                    len(existing_words),
                )
                same = same or overlap >= 0.6

            if same:
                if len(text) > len(existing):
                    kept[index] = text

                found = True
                break

        if not found:
            kept.append(text)

    return kept
