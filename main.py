# main.py — исправленная версия (рефакторинг)
# Тихое игнорирование неавторизованных, предупреждение на /start, комментарии на русском.
# Изменения: Рефакторинг на модульную структуру, основной код перенесен в отдельные файлы

import os
import time
import asyncio

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from utils.config import BOT_TOKEN, SESSIONS_DIR, GLOBAL_APP
from utils.command_handlers import start, cancel
from utils.message_handlers import handle_message
from utils.background_tasks import session_and_pending_cleaner
from utils.logging_utils import log_error


# -----------------------
# === Supervisor и запуск приложения ===
# -----------------------
def main():
    global GLOBAL_APP
    if not os.path.exists(SESSIONS_DIR):
        os.makedirs(SESSIONS_DIR)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    GLOBAL_APP = app

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def _start_background_tasks(application):
        try:
            asyncio.create_task(session_and_pending_cleaner())
        except Exception:
            log_error("Не удалось запустить фоновые задачи:\n" + str(locals()))

    app.post_init = _start_background_tasks

    # Цикл supervisor
    while True:
        try:
            print("Бот запускается. Если он упадет, supervisor перезапустит его через 5 сек.")
            app.run_polling()
        except Exception:
            log_error("App.run_polling завершилась аварийно:\n" + str(locals()))
            time.sleep(5)
            continue
        else:
            break


if __name__ == "__main__":
    main()
