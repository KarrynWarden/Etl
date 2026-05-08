"""Базовые пути проекта.

FULL_PATH — корень airflow, MODE — суффикс среды (Prod / Test / "" для dev).
В реальной среде эти значения переопределяются переменными окружения.
"""
import os

FULL_PATH = os.environ.get("ETL_FULL_PATH", "/opt/airflow/airflow/")
MODE = os.environ.get("ETL_MODE", "")
