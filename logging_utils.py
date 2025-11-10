# logging_utils.py — функции логирования
# Изменения: вынесены все функции логирования из main.py

import os
from datetime import datetime

# -----------------------
# === Логи и конфигурация ===
# -----------------------
LOG_DIR = "./users_data/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_SESSIONS = os.path.join(LOG_DIR, "logging.log")
LOG_ERRORS = os.path.join(LOG_DIR, "error.log")
LOG_WRONG_ACCESS = os.path.join(LOG_DIR, "wrong_access.log")


def now_iso_local():
    return datetime.now().astimezone().isoformat()


def log_session(msg: str):
    try:
        with open(LOG_SESSIONS, "a", encoding="utf-8") as f:
            f.write(f"{now_iso_local()} | {msg}\n")
    except Exception:
        pass


def log_error(exc_text: str):
    try:
        with open(LOG_ERRORS, "a", encoding="utf-8") as f:
            f.write(f"{now_iso_local()} | {exc_text}\n\n")
    except Exception:
        pass


def log_wrong_access(user_id: int, msg: str):
    try:
        with open(LOG_WRONG_ACCESS, "a", encoding="utf-8") as f:
            f.write(f"{now_iso_local()} | user_id={user_id} | {msg}\n")
    except Exception:
        pass
