@echo off
REM -------------------------------
REM Создание и активация виртуального окружения
REM -------------------------------

IF NOT EXIST ".venv" (
    echo Создаём виртуальное окружение...
    python -m venv .venv
) ELSE (
    echo Виртуальное окружение уже существует
)

REM Активация виртуального окружения (CMD)
call .venv\Scripts\activate.bat

REM -------------------------------
REM Обновление pip и setuptools
REM -------------------------------
python -m pip install --upgrade pip setuptools wheel

REM -------------------------------
REM Установка всех необходимых пакетов
REM -------------------------------
python -m pip install --upgrade ^
    cryptography ^
    python-dotenv ^
    telethon ^
    python-telegram-bot ^
    openpyxl ^
    requests

echo.
echo Все пакеты успешно установлены в виртуальное окружение .venv
pause
