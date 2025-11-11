# background_tasks.py — фоновые задачи
# Изменения: вынесены функции фоновой очистки из main.py

import os
import asyncio
import time

from utils.config import SESSIONS_DIR, SESSION_TTL_SECONDS
from utils.config import active_sessions, pending_action
from utils.logging_utils import log_session, log_error


# -----------------------
# === Фоновая очистка сессий и pending ===
# -----------------------
async def session_and_pending_cleaner():
    global active_sessions, pending_action
    while True:
        now = time.time()
        # Удаляем устаревшие сессии по времени создания
        to_remove_sessions = []
        for session_path, meta in list(active_sessions.items()):
            created = meta.get("created", 0)
            if now >= (created + SESSION_TTL_SECONDS):
                session_base = os.path.splitext(session_path)[0]
                try:
                    for fname in os.listdir(SESSIONS_DIR):
                        if fname.startswith(os.path.basename(session_base)):
                            fpath = os.path.join(SESSIONS_DIR, fname)
                            try:
                                os.remove(fpath)
                                log_session(f"Удален устаревший файл сессии: {fpath}")
                            except Exception:
                                log_error("Не удалось удалить файл сессии в очистке:\n" + str(locals()))
                except Exception:
                    log_error("Ошибка сканирования директории сессий в очистке:\n" + str(locals()))
                to_remove_sessions.append(session_path)
        for p in to_remove_sessions:
            active_sessions.pop(p, None)

        # Очищаем зависшие pending flows старше TTL
        stale_users = []
        for uid, pa in list(pending_action.items()):
            start = pa.get("start_time")
            if start and (now - start) > SESSION_TTL_SECONDS:
                client = pa.get("client")
                try:
                    if client:
                        await client.disconnect()
                except Exception:
                    log_error("Ошибка отключения клиента во время очистки pending:\n" + str(locals()))
                try:
                    from utils.message_cleanup import purge_auth_messages_for_user
                    await purge_auth_messages_for_user(uid)
                except Exception:
                    log_error("Не удалось очистить авторизационные сообщения во время очистки pending:\n" + str(locals()))
                stale_users.append(uid)
        for uid in stale_users:
            pending_action.pop(uid, None)

        await asyncio.sleep(30)
