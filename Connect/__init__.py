"""Подключения к БД.

Параметры подтягиваются из переменных окружения. Удобный способ
их задать — файл `.env` рядом с корнем проекта (см. `.env.example`).
Файл .env лежит в .gitignore и в репозиторий не попадает.

Перечень переменных:
    ETL_FULL_PATH, ETL_MODE
    ETL_ORACLE_HOST, ETL_ORACLE_PORT, ETL_ORACLE_SID,
    ETL_ORACLE_USER, ETL_ORACLE_PWD, ETL_ORACLE_CONFIG_DIR
    ETL_POST_HOST, ETL_POST_PORT, ETL_POST_DB,
    ETL_POST_USER, ETL_POST_PWD
"""
import os
from pathlib import Path

import psycopg2
import cx_Oracle
from dotenv import load_dotenv


# .env лежит в корне репозитория (на один уровень выше этого файла).
# Если файла нет — load_dotenv тихо ничего не сделает, и значения
# подтянутся из системных переменных окружения.
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


_oracleClientInitialized = False


def DbConnectOrcl():
    global _oracleClientInitialized
    if not _oracleClientInitialized:
        cx_Oracle.init_oracle_client(
            config_dir=os.environ.get("ETL_ORACLE_CONFIG_DIR", "/opt/oracle/config"),
        )
        _oracleClientInitialized = True
    dsn = cx_Oracle.makedsn(
        os.environ["ETL_ORACLE_HOST"],
        int(os.environ.get("ETL_ORACLE_PORT", 1521)),
        sid=os.environ["ETL_ORACLE_SID"],
    )
    return cx_Oracle.connect(
        user=os.environ["ETL_ORACLE_USER"],
        password=os.environ["ETL_ORACLE_PWD"],
        dsn=dsn,
        encoding="UTF-8",
    )


def DbConnectPost():
    return psycopg2.connect(
        host=os.environ["ETL_POST_HOST"],
        port=os.environ.get("ETL_POST_PORT", "5432"),
        database=os.environ["ETL_POST_DB"],
        user=os.environ["ETL_POST_USER"],
        password=os.environ["ETL_POST_PWD"],
    )
