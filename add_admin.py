# add_admin.py
import os
import sys
import json
import traceback
from cryptography.fernet import Fernet

# -----------------------
# === Генерация/загрузка ключа ===
# -----------------------
KEY_FILE = "./users_data/secret.key"

def generate_key():
    # Создаем директорию, если она не существует
    os.makedirs(os.path.dirname(KEY_FILE), exist_ok=True)
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f:
        f.write(key)
    print(f"{KEY_FILE} создан.")

def load_key():
    if not os.path.exists(KEY_FILE):
        generate_key()
    with open(KEY_FILE, "rb") as f:
        return f.read()

fernet = Fernet(load_key())

# -----------------------
# === Загрузка/сохранение пользователей ===
# -----------------------
USERS_FILE = "./users_data/users.enc"

def load_users():
    # Создаем директорию, если она не существует
    os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
    if not os.path.exists(USERS_FILE):
        save_users({"admins": [], "operators": []})
    try:
        with open(USERS_FILE, "rb") as f:
            data = f.read()
            if not data:
                return {"admins": [], "operators": []}
            return json.loads(fernet.decrypt(data).decode())
    except Exception:
        print("Ошибка при загрузке users.enc:\n", traceback.format_exc())
        return {"admins": [], "operators": []}

def save_users(data):
    try:
        with open(USERS_FILE, "wb") as f:
            f.write(fernet.encrypt(json.dumps(data).encode()))
    except Exception:
        print("Ошибка при сохранении users.enc:\n", traceback.format_exc())

# -----------------------
# === Добавление администратора ===
# -----------------------
def add_admin(tg_id):
    users = load_users()
    if tg_id in users.get("admins", []):
        print(f"Пользователь {tg_id} уже является администратором.")
        return
    users.setdefault("admins", []).append(tg_id)
    save_users(users)
    print(f"Пользователь {tg_id} добавлен в администраторы.")

# -----------------------
# === Основная функция ===
# -----------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python add_admin.py {Telegram_ID}")
        sys.exit(1)
    try:
        tg_id = int(sys.argv[1])
    except ValueError:
        print("Telegram_ID должен быть числом.")
        sys.exit(1)
    add_admin(tg_id)
