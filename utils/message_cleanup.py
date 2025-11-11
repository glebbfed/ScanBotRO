# message_cleanup.py — функции очистки сообщений
# Изменения: вынесены функции удаления временных сообщений из main.py

import time

from utils.config import GLOBAL_APP
from utils.logging_utils import log_error


async def purge_auth_messages_for_user(user_id: int):
    global GLOBAL_APP
    if GLOBAL_APP is None:
        return
    from utils.config import pending_action
    pa = pending_action.get(user_id)
    if not pa:
        return
    msgs = pa.get("auth_messages", [])
    for m in msgs:
        chat_id = m.get("chat_id")
        message_id = m.get("message_id")
        try:
            await GLOBAL_APP.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            log_error(f"Не удалось удалить сообщение {message_id} в чате {chat_id} для пользователя {user_id}:\n" + str(locals()))
    pa["auth_messages"] = []


def record_auth_message(user_id: int, chat_id: int, message_id: int, from_bot: bool):
    from utils.config import pending_action
    pa = pending_action.get(user_id)
    entry = {"chat_id": chat_id, "message_id": message_id, "from_bot": bool(from_bot)}
    if pa is None:
        pending_action[user_id] = {"auth_messages": [entry], "start_time": time.time()}
    else:
        lst = pa.get("auth_messages", [])
        lst.append(entry)
        pa["auth_messages"] = lst
        if "start_time" not in pa:
            pa["start_time"] = time.time()
