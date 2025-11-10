# ui_utils.py — утилиты для пользовательского интерфейса
# Изменения: вынесены функции клавиатур и утилиты нормализации из main.py

import re
from telegram import ReplyKeyboardMarkup

from config import LABEL_ADD_ADMIN, LABEL_ADD_OPERATOR, LABEL_REMOVE_OPERATOR
from config import LABEL_LIST_OPERATORS, LABEL_SCAN, LABEL_CANCEL
from user_management import get_user_role


def main_menu_keyboard(role: str):
    if role == "admin":
        return ReplyKeyboardMarkup(
            [
                [LABEL_ADD_ADMIN, LABEL_ADD_OPERATOR, LABEL_REMOVE_OPERATOR],
                [LABEL_LIST_OPERATORS, LABEL_SCAN]
            ],
            resize_keyboard=True
        )
    elif role == "operator":
        return ReplyKeyboardMarkup([[LABEL_SCAN]], resize_keyboard=True)
    else:
        return ReplyKeyboardMarkup([["/start"]], resize_keyboard=True)


def cancel_keyboard():
    return ReplyKeyboardMarkup([[LABEL_CANCEL]], resize_keyboard=True)


def chats_keyboard(chats):
    buttons = [[chat] for chat in chats[:25]]
    buttons.append([LABEL_CANCEL])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


# -----------------------
# === Утилиты / нормализация ===
# -----------------------
def normalize(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    if t.startswith("/"):
        t = t[1:]
    parts = t.split()
    return ".join(parts).lower()


def is_same_label(text: str, label: str) -> bool:
    return normalize(text) == normalize(label)


async def send_main_menu(update, user_id: int):
    role = get_user_role(user_id)
    try:
        await update.message.reply_text("Возврат в главное меню:", reply_markup=main_menu_keyboard(role))
    except Exception:
        from logging_utils import log_error
        log_error("Не удалось отправить главное меню:\n" + str(locals()))
