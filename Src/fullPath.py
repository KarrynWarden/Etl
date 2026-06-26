"""Базовые пути проекта.

FULL_PATH — корень airflow.
В реальной среде эти значения переопределяются переменными окружения
(см. .env / .env.example).
"""
import os

from Src.loadEnv import _ENV_PATH  # noqa: F401  — побочный эффект: загрузить .env

FULL_PATH = os.environ.get("ETL_FULL_PATH", "/opt/airflow/airflow/")
WEB_BASE_URL = "https://airflow.oms66.ru"
