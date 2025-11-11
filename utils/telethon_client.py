# telethon_client.py — функции для работы с Telethon
# Изменения: вынесены функции сессий и авторизации из main.py

import os
import re
import hashlib
import time
from typing import Tuple, Optional

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, RPCError
from telethon.tl.types import ChannelParticipantsAdmins

from utils.config import API_ID, API_HASH, SESSIONS_DIR
from utils.logging_utils import log_session, log_error
from utils.user_management import users_data
from utils.ui_utils import get_user_role, chats_keyboard
from utils.message_cleanup import purge_auth_messages_for_user


def normalize_phone(raw: str) -> str:
    if not raw:
        return raw
    raw = raw.strip()
    plus_prefixed = raw.startswith("+")
    digits = re.sub(r"\D", "", raw)
    if plus_prefixed:
        return "+" + digits
    if len(digits) == 11 and digits.startswith("8"):
        return "+7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 10:
        return "+7" + digits
    if digits > 0:
        return "+" + digits
    return raw


def _session_filename_for_phone(phone: str):
    h = hashlib.sha256(phone.encode()).hexdigest()
    session_name = f"session_{h}"
    # Telethon создает несколько файлов: session_name.session, session_name.session-journal и т.д.
    session_filepath = os.path.join(SESSIONS_DIR, session_name + ".session")
    return session_name, session_filepath


async def telethon_send_code(phone: str) -> Tuple[Optional[TelegramClient], Optional[str]]:
    if API_ID is None or not API_HASH:
        raise RuntimeError("API_ID/API_HASH не заданы в .env")

    session_name, session_filepath = _session_filename_for_phone(phone)
    if os.path.exists(session_filepath):
        return None, "exists"

    client = TelegramClient(os.path.join(SESSIONS_DIR, session_name), API_ID, API_HASH)
    try:
        await client.connect()
        # send_code_request может вызвать RPCError/FloodWait и т.д.
        await client.send_code_request(phone)
        log_session(f"Отправлен запрос кода для телефона {phone} (session={session_filepath})")
        return client, None
    except RPCError as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        log_error("telethon_send_code RPCError:\n" + str(e))
        return None, str(e)
    except Exception as e:
        log_error("telethon_send_code ошибка:\n" + str(e))
        try:
            await client.disconnect()
        except Exception:
            pass
        return None, "send_code_error"


async def list_user_chats_and_store(client: TelegramClient, update, user_id: int):
    try:
        dialogs = await client.get_dialogs(limit=200)
    except Exception as e:
        log_error("Ошибка в get_dialogs:\n" + str(e))
        try:
            await update.message.reply_text("Ошибка при получении чатов.", reply_markup=main_menu_keyboard(get_user_role(user_id)))
        except Exception:
            log_error("Не удалось сообщить пользователю об ошибке get_dialogs:\n" + str(e))
        return

    dialogs_filtered = []
    titles = []
    for d in dialogs:
        title = None
        if hasattr(d, "title") and d.title:
            title = d.title
        elif hasattr(d, "name") and d.name:
            title = d.name
        else:
            try:
                title = getattr(d.entity, "title", None) or getattr(d.entity, "name", None)
            except Exception:
                title = None

        is_channel = getattr(d, "is_channel", False)
        is_group = getattr(d, "is_group", False)
        if (is_group or is_channel) and title:
            dialogs_filtered.append({"title": title, "id": getattr(d, "id", None), "dialog": d})
            titles.append(title)

    if not titles:
        try:
            await update.message.reply_text("⚠️ Не найдено доступных чатов.", reply_markup=main_menu_keyboard(get_user_role(update.effective_user.id)))
        except Exception:
            log_error("Не удалось ответить 'нет чатов':\n" + str(e))
        try:
            await client.disconnect()
        except Exception:
            pass
        return

    from utils.config import pending_action
    existing = pending_action.get(update.effective_user.id, {})
    auth_msgs = existing.get("auth_messages", [])
    # ensure auth_messages present
    pending_action[update.effective_user.id] = {
        "action": "choose_chat",
        "phone": existing.get("phone"),
        "client": client,
        "dialogs": dialogs_filtered,
        "auth_messages": auth_msgs,
        "start_time": existing.get("start_time", time.time())
    }

    # попытка удалить авторизационные сообщения (базовая очистка)
    try:
        await purge_auth_messages_for_user(update.effective_user.id)
    except Exception:
        log_error("Не удалось очистить авторизационные сообщения после получения списка чатов:\n" + str(e))

    try:
        await update.message.reply_text("Выберите чат:", reply_markup=chats_keyboard(titles))
    except Exception:
        log_error("Не удалось отправить сообщение со списком чатов:\n" + str(e))
