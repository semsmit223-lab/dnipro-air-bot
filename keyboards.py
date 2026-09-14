from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Старт"), KeyboardButton(text="Стоп")],
            [KeyboardButton(text="Стан"), KeyboardButton(text="Загрози")],
            [KeyboardButton(text="Моя локація"), KeyboardButton(text="Тихий режим")],
            [KeyboardButton(text="Налаштування"), KeyboardButton(text="Допомога")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Оберіть дію…",
    )


def settings_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Місто"), KeyboardButton(text="Радіус")],
            [KeyboardButton(text="Точки"), KeyboardButton(text="Обстановка")],
            [KeyboardButton(text="Історія"), KeyboardButton(text="Статистика")],
            [KeyboardButton(text="Журнал"), KeyboardButton(text="Небезпека")],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
    )
