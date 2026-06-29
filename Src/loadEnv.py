"""Загрузка .env на старте — без внешних зависимостей.

Читает файл `.env` в корне проекта и кладёт значения в os.environ, НЕ затирая
уже заданные переменные окружения (системные/шелловые имеют приоритет). За счёт
собственного парсера не ломается на значениях со спецсимволами/пробелами, в
отличие от `set -a; . .env` в bash. Идемпотентен, python-dotenv не требуется.

Вызывается побочным эффектом при импорте (этот модуль импортируют fullPath и
Connect), поэтому .env подхватывается в любом процессе, который трогает БД или
пути — в т.ч. в подпроцессе задачи airflow.
"""
import os
from pathlib import Path

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def loadEnv(path=_ENV_PATH):
    """Загрузить .env в os.environ (не затирая уже заданные переменные)."""
    try:
        with open(path, encoding="utf-8") as fp:
            lines = fp.readlines()
    except FileNotFoundError:
        return
    except PermissionError:
        raise RuntimeError(
            f"Файл {path} недоступен на чтение текущему пользователю. "
            f"Дай права, например: chgrp etldev {path} && chmod 640 {path}"
        )
    for raw in lines:
        line = raw.strip().lstrip("﻿")  # снять BOM, пробелы, CR
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        # снять обрамляющие кавычки, если есть
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            os.environ.setdefault(key, val)


loadEnv()