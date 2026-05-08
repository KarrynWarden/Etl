"""Загрузка .env на старте.

Вызывается из любого модуля, которому нужны переменные окружения.
Идемпотентен — повторный вызов ничего не ломает.
"""
from pathlib import Path

from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)
