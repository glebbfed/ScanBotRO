# command_handlers.py — обработчики команд
# Изменения: вынесены обработчики команд из handlers.py

import traceback

from telegram import Update
from telegram.ext import ContextTypes

from utils.config import WARNING_TEXT
from utils.user_management import get_user_role
from utils.ui_utils import main_menu_keyboard
from utils.logging_utils import log_wrong_access, log_error
from utils.ui_utils import send_main_menu


# -----------------------
# === Обработчики команд ===
# -----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    role = get_user_role(user_id)
    if not role:
        log_wrong_access(user_id, "Попытка использовать /start без доступа")
        return

    try:
        await update.message.reply_text(WARNING_TEXT)
    except Exception:
        log_error("Не удалось отправить предупреждение:\n" + traceback.format_exc())

    try:
        await update.message.reply_text("Добро пожаловать в главное меню:", reply_markup=main_menu_keyboard(role))
    except Exception:
        log_error("Не удалось отправить главное меню при старте:\n" + traceback.format_exc())


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    role = get_user_role(user_id)
    if role is None:
        log_wrong_access(user_id, "Попытка использовать /cancel без доступа")
        return

    from utils.config import pending_action
    if user_id in pending_action:
        client = pending_action[user_id].get("client")
        try:
            if client:
                from telethon import TelegramClient
                await client.disconnect()
        except Exception:
            log_error("Ошибка отключения клиента при отмене:\n" + traceback.format_exc())
        try:
            from utils.message_cleanup import purge_auth_messages_for_user
            await purge_auth_messages_for_user(user_id)
        except Exception:
            log_error("Не удалось очистить авторизационные сообщения при отмене:\n" + traceback.format_exc())
        pending_action.pop(user_id, None)
    await send_main_menu(update, user_id)
