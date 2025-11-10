# user_management.py — управление пользователями и шифрование
# Изменения: вынесены функции управления пользователями из main.py

import json
import os
import traceback
from cryptography.fernet import Fernet

from config import USERS_FILE, KEY_FILE
from logging_utils import log_session, log_error


def generate_key():
    # Создаем директорию, если она не существует
    os.makedirs(os.path.dirname(KEY_FILE), exist_ok=True)
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f:
        f.write(key)


def load_key():
    if not os.path.exists(KEY_FILE):
        generate_key()
    with open(KEY_FILE, "rb") as f:
        return f.read()


fernet = Fernet(load_key())


def load_users():
    if not os.path.exists(USERS_FILE):
        # Не создаем файл автоматически при запуске, если его нет
        log_session("users.enc не найден, ожидание создания add_admin.py")
        return {"admins": [], "operators": []}
    try:
        with open(USERS_FILE, "rb") as f:
            data = f.read()
            if not data:
                log_session("users.enc пуст -> создание структуры по умолчанию")
                return {"admins": [], "operators": []}
            try:
                decoded = fernet.decrypt(data).decode()
                users = json.loads(decoded)
                log_session(f"Загружен users.enc: администраторы={users.get('admins', [])}, операторы={users.get('operators', [])}")
                return users
            except Exception:
                # if decryption/parsing fails — log and fallback to safe empty lists
                log_error("Не удалось расшифровать/разобрать users.enc:\n" + traceback.format_exc())
                return {"admins": [], "operators": []}
    except Exception:
        log_error("Не удалось прочитать users.enc:\n" + traceback.format_exc())
        return {"admins": [], "operators": []}


def save_users(data):
    try:
        with open(USERS_FILE, "wb") as f:
            f.write(fernet.encrypt(json.dumps(data).encode()))
        log_session("Сохранен users.enc")
    except Exception:
        log_error("Не удалось сохранить users.enc:\n" + traceback.format_exc())


# Загружаем данные пользователей при инициализации
users_data = load_users()


def get_user_role(user_id: int):
    users = users_data
    if user_id in users.get("admins", []):
        return "admin"
    if user_id in users.get("operators", []):
        return "operator"
    return None
