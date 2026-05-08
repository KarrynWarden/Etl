"""Базовые пути проекта.

FULL_PATH — корень airflow, MODE — суффикс среды (Prod / Test / "" для dev).
В реальной среде эти значения переопределяются переменными окружения
(см. .env / .env.example).
"""
import os

from Src.loadEnv import _ENV_PATH  # noqa: F401  — побочный эффект: загрузить .env

FULL_PATH = os.environ.get("ETL_FULL_PATH", "/opt/airflow/airflow/")
MODE = os.environ.get("ETL_MODE", "")
