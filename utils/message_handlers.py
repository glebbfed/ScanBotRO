# message_handlers.py — обработчики сообщений
# Изменения: вынесены обработчики сообщений из handlers.py

import time
import traceback

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes

from utils.config import LABEL_ADD_ADMIN, LABEL_ADD_OPERATOR, LABEL_REMOVE_OPERATOR
from utils.config import LABEL_LIST_OPERATORS, LABEL_SCAN, LABEL_CANCEL
from utils.config import pending_action, SESSIONS_DIR, API_ID, API_HASH, SESSION_TTL_SECONDS
from utils.user_management import get_user_role, users_data, save_users
from utils.ui_utils import main_menu_keyboard, cancel_keyboard, chats_keyboard, normalize, send_main_menu
from utils.logging_utils import log_wrong_access, log_error, log_session
from utils.telethon_client import list_user_chats_and_store, telethon_send_code, normalize_phone, _session_filename_for_phone
from utils.export_utils import export_members_to_xlsx_and_send
from utils.message_cleanup import record_auth_message, purge_auth_messages_for_user
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from utils.config import active_sessions


# -----------------------
# === Обработчик текстовых сообщений ===
# -----------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw_text = (update.message.text or "")
    text = raw_text.strip()
    n = normalize(text)
    role = get_user_role(user_id)

    # Если пользователь без доступа — логируем и игнорируем (не отвечаем)
    if role is None:
        log_wrong_access(user_id, f"Неавторизованное сообщение: {text[:200]}")
        return

    # если пользователь в pending flow — записываем входящее сообщение (чтобы потом удалить)
    if user_id in pending_action:
        try:
            record_auth_message(user_id, update.effective_chat.id, update.message.message_id, from_bot=False)
        except Exception:
            log_error("Не удалось записать авторизационное сообщение (сообщение пользователя):\n" + traceback.format_exc())

    # Обработка Отмена
    if n == normalize(LABEL_CANCEL):
        if user_id in pending_action:
            client = pending_action[user_id].get("client")
            try:
                if client:
                    await client.disconnect()
            except Exception:
                log_error("Ошибка отключения клиента при отмене (handle_message):\n" + traceback.format_exc())
            try:
                await purge_auth_messages_for_user(user_id)
            except Exception:
                log_error("Не удалось очистить авторизационные сообщения при отмене (handle_message):\n" + traceback.format_exc())
            pending_action.pop(user_id, None)
        await send_main_menu(update, user_id)
        return

    # Если в pending flow — обрабатываем шаги
    if user_id in pending_action:
        action = pending_action[user_id]
        act = action.get("action")

        if act == "login":
            step = action.get("step")
            if step == "phone":
                phone_raw = text
                if not phone_raw or (not phone_raw[0].isdigit() and phone_raw[0] not in ["+", "8"]):
                    try:
                        msg = await update.message.reply_text("❌ Некорректный номер. Введите в формате +79161234567 или нажмите Отмена.", reply_markup=cancel_keyboard())
                        record_auth_message(user_id, msg.chat.id, msg.message_id, from_bot=True)
                    except Exception:
                        log_error("Не удалось отправить сообщение о недействительном номере:\n" + traceback.format_exc())
                    return
                phone_norm = normalize_phone(phone_raw)
                pa = pending_action.get(user_id, {})
                pa["phone"] = phone_norm
                pa["start_time"] = pa.get("start_time", time.time())
                # ensure auth_messages list exists
                if "auth_messages" not in pa:
                    pa["auth_messages"] = []
                pending_action[user_id] = pa

                session_name, session_path = _session_filename_for_phone(phone_norm)
                # если сессия уже есть — используем её
                if os.path.exists(session_path):
                    try:
                        client = TelegramClient(os.path.join(SESSIONS_DIR, session_name), API_ID, API_HASH)
                        await client.connect()
                        now = time.time()
                        active_sessions[session_path] = {"created": now, "expiry": now + SESSION_TTL_SECONDS, "owner": user_id}
                        log_session(f"Повторно использованная сессия активирована для {phone_norm} (path={session_path}) пользователем {user_id}")
                        await list_user_chats_and_store(client, update, user_id)
                    except Exception:
                        log_error("Не удалось использовать существующую сессию:\n" + traceback.format_exc())
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                    return

                # иначе отправляем код
                try:
                    msg = await update.message.reply_text("⏳ Отправляем код подтверждения...", reply_markup=cancel_keyboard())
                    record_auth_message(user_id, msg.chat.id, msg.message_id, from_bot=True)
                except Exception:
                    log_error("Не удалось отправить сообщение 'отправка кода':\n" + traceback.format_exc())
                client, err = await telethon_send_code(phone_norm)
                if client is None:
                    if err == "exists":
                        try:
                            await update.message.reply_text("⚠️ Сессия уже существует.", reply_markup=main_menu_keyboard(role))
                        except Exception:
                            log_error("Не удалось ответить 'сессия существует':\n" + traceback.format_exc())
                    else:
                        try:
                            await update.message.reply_text(f"❌ Ошибка при отправке кода: {err}", reply_markup=main_menu_keyboard(role))
                        except Exception:
                            log_error("Не удалось ответить 'ошибка отправки кода':\n" + traceback.format_exc())
                    pending_action.pop(user_id, None)
                    return
                try:
                    msg2 = await update.message.reply_text("📩 Код подтверждения отправлен. Введите его:", reply_markup=cancel_keyboard())
                    record_auth_message(user_id, msg2.chat.id, msg2.message_id, from_bot=True)
                except Exception:
                    log_error("Не удалось отправить запрос 'введите код':\n" + traceback.format_exc())
                pending_action[user_id] = {"action": "login", "step": "code", "client": client, "phone": phone_norm, "start_time": time.time(), "auth_messages": pending_action[user_id].get("auth_messages", [])}
                return

            if step == "code":
                code = text
                client = action.get("client")
                phone = action.get("phone")
                try:
                    await client.sign_in(phone, code)
                    session_name, session_path = _session_filename_for_phone(phone)
                    now = time.time()
                    active_sessions[session_path] = {"created": now, "expiry": now + SESSION_TTL_SECONDS, "owner": user_id}
                    log_session(f"Сессия создана для {phone} (path={session_path}) пользователем {user_id}")
                    await list_user_chats_and_store(client, update, user_id)
                    return
                except SessionPasswordNeededError:
                    pending_action[user_id] = {"action": "login", "step": "password", "client": client, "phone": phone, "start_time": action.get("start_time", time.time()), "auth_messages": action.get("auth_messages", [])}
                    try:
                        msg = await update.message.reply_text("🔒 Введите пароль 2FA:", reply_markup=cancel_keyboard())
                        record_auth_message(user_id, msg.chat.id, msg.message_id, from_bot=True)
                    except Exception:
                        log_error("Не удалось отправить запрос 2FA:\n" + traceback.format_exc())
                    return
                except Exception:
                    log_error("Ошибка в шаге кода sign_in:\n" + traceback.format_exc())
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    pending_action.pop(user_id, None)
                    try:
                        await update.message.reply_text(f"❌ Ошибка авторизации.", reply_markup=main_menu_keyboard(role))
                    except Exception:
                        log_error("Не удалось ответить сообщением об ошибке авторизации:\n" + traceback.format_exc())
                    return

            if step == "password":
                password = text
                client = action.get("client")
                phone = action.get("phone")
                try:
                    await client.sign_in(password=password)
                    session_name, session_path = _session_filename_for_phone(phone)
                    now = time.time()
                    active_sessions[session_path] = {"created": now, "expiry": now + SESSION_TTL_SECONDS, "owner": user_id}
                    log_session(f"Сессия создана (2FA) для {phone} (path={session_path}) пользователем {user_id}")
                    await list_user_chats_and_store(client, update, user_id)
                    return
                except Exception:
                    log_error("Ошибка в шаге пароля sign_in:\n" + traceback.format_exc())
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    pending_action.pop(user_id, None)
                    try:
                        await update.message.reply_text(f"❌ Ошибка 2FA.", reply_markup=main_menu_keyboard(role))
                    except Exception:
                        log_error("Не удалось ответить ошибкой 2FA:\n" + traceback.format_exc())
                    return

        if act == "add_admin":
            incoming = text.strip()
            if not incoming:
                await update.message.reply_text("❌ Неверный ID. Введите Telegram ID (число) или нажмите Отмена.", reply_markup=cancel_keyboard())
                return
            try:
                new_id = int(incoming)
            except Exception:
                await update.message.reply_text("❌ ID должен быть числом. Введите корректный Telegram ID.", reply_markup=cancel_keyboard())
                return
            users = users_data
            admins = users.get("admins", [])
            if new_id in admins:
                await update.message.reply_text("⚠️ Этот пользователь уже является администратором.", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            else:
                admins.append(new_id)
                users["admins"] = admins
                save_users(users)
                await update.message.reply_text(f"✅ Добавлен администратор: {new_id}", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            pending_action.pop(user_id, None)
            return

        if act == "add_operator":
            incoming = text.strip()
            if not incoming:
                await update.message.reply_text("❌ Неверный ID. Введите Telegram ID (число) или нажмите Отмена.", reply_markup=cancel_keyboard())
                return
            try:
                new_id = int(incoming)
            except Exception:
                await update.message.reply_text("❌ ID должен быть числом. Введите корректный Telegram ID.", reply_markup=cancel_keyboard())
                return
            users = users_data
            ops = users.get("operators", [])
            if new_id in ops:
                await update.message.reply_text("⚠️ Этот пользователь уже является оператором.", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            else:
                ops.append(new_id)
                users["operators"] = ops
                save_users(users)
                await update.message.reply_text(f"✅ Добавлен оператор: {new_id}", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            pending_action.pop(user_id, None)
            return

        if act == "remove_operator":
            incoming = text.strip()
            if not incoming:
                await update.message.reply_text("❌ Неверный ввод. Нажмите ID оператора или введите его числом, либо нажмите Отмена.", reply_markup=cancel_keyboard())
                return
            try:
                remove_id = int(incoming)
            except Exception:
                await update.message.reply_text("❌ ID должен быть числом. Выберите ID оператора из списка или введите его вручную.", reply_markup=cancel_keyboard())
                return
            users = users_data
            ops = users.get("operators", [])
            if remove_id not in ops:
                await update.message.reply_text("⚠️ Такой оператор не найден.", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            else:
                ops.remove(remove_id)
                users["operators"] = ops
                save_users(users)
                await update.message.reply_text(f"✅ Оператор {remove_id} удалён.", reply_markup=main_menu_keyboard(get_user_role(user_id)))
            pending_action.pop(user_id, None)
            return

        if act == "choose_chat":
            dialogs = action.get("dialogs", [])
            matched = None
            for d in dialogs:
                if d["title"] == text:
                    matched = d
                    break
            if not matched:
                try:
                    await update.message.reply_text("❌ Чат не найден. Нажмите Отмена и попробуйте снова.", reply_markup=chats_keyboard([d["title"] for d in dialogs]))
                except Exception:
                    log_error("Не удалось отправить сообщение 'чат не найден':\n" + traceback.format_exc())
                return
            dialog_obj = matched.get("dialog")
            client = action.get("client")
            try:
                await export_members_to_xlsx_and_send(client, dialog_obj, update.effective_chat.id, context.application)
            except Exception:
                log_error("Ошибка экспорта участников:\n" + traceback.format_exc())
                try:
                    await context.application.bot.send_message(chat_id=update.effective_chat.id, text="❌ Ошибка при экспорте участников.")
                except Exception:
                    log_error("Не удалось уведомить пользователя об ошибке экспорта:\n" + traceback.format_exc())
            try:
                await client.disconnect()
            except Exception:
                log_error("Не удалось отключить клиент после экспорта:\n" + traceback.format_exc())
            pending_action.pop(user_id, None)
            await send_main_menu(update, user_id)
            return

    # -----------------------
    # === Главное меню: обработка кнопок ===
    # -----------------------
    if normalize(text) == normalize(LABEL_SCAN):
        if role not in ["admin", "operator"]:
            log_wrong_access(user_id, f"Попытка использовать Scan без прав")
            await update.message.reply_text("🚫 Недостаточно прав.")
            return
        pending_action[user_id] = {"action": "login", "step": "phone", "start_time": time.time(), "auth_messages": []}
        try:
            await update.message.reply_text("📱 Введите номер телефона для входа:", reply_markup=cancel_keyboard())
        except Exception:
            log_error("Не удалось отправить запрос 'введите телефон':\n" + traceback.format_exc())
        return

    if normalize(text) == normalize(LABEL_ADD_ADMIN):
        if role != "admin":
            log_wrong_access(user_id, f"Попытка добавить администратора без прав")
            await update.message.reply_text("🚫 Только админ может это делать.")
            return
        pending_action[user_id] = {"action": "add_admin", "start_time": time.time(), "auth_messages": []}
        await update.message.reply_text("Введите Telegram ID нового администратора (число):", reply_markup=cancel_keyboard())
        return

    if normalize(text) == normalize(LABEL_ADD_OPERATOR):
        if role != "admin":
            log_wrong_access(user_id, f"Попытка добавить оператора без прав")
            await update.message.reply_text("🚫 Только админ может это делать.")
            return
        pending_action[user_id] = {"action": "add_operator", "start_time": time.time(), "auth_messages": []}
        await update.message.reply_text("Введите Telegram ID нового оператора (число):", reply_markup=cancel_keyboard())
        return

    if normalize(text) == normalize(LABEL_REMOVE_OPERATOR):
        if role != "admin":
            log_wrong_access(user_id, f"Попытка удалить оператора без прав")
            await update.message.reply_text("🚫 Только админ может это делать.")
            return
        ops = users_data.get("operators", [])
        if not ops:
            await update.message.reply_text("Операторов нет.", reply_markup=main_menu_keyboard(role))
            return
        buttons = [[str(x)] for x in ops]
        buttons.append([LABEL_CANCEL])
        pending_action[user_id] = {"action": "remove_operator", "start_time": time.time(), "auth_messages": []}
        await update.message.reply_text("Выберите оператора для удаления (нажмите ID) или введите ID вручную:", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True))
        return

    if normalize(text) == normalize(LABEL_LIST_OPERATORS):
        if role != "admin":
            log_wrong_access(user_id, f"Попытка получить список операторов без прав")
            await update.message.reply_text("🚫 Только админ может это делать.")
            return
        ops = users_data.get("operators", [])
        if not ops:
            await update.message.reply_text("Операторов нет.", reply_markup=main_menu_keyboard(role))
            return
        await update.message.reply_text("Список операторов:\n" + "\n".join(str(x) for x in ops), reply_markup=main_menu_keyboard(role))
        return
    try:
        await update.message.reply_text("Неизвестная команда. Используйте кнопки меню.")
    except Exception:
        log_error("Не удалось отправить сообщение 'неизвестная команда':\n" + traceback.format_exc())
