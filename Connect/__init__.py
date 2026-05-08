"""Подключения к БД. Параметры подтягиваются из переменных окружения,
чтобы не хранить секреты в репозитории.

Перечень переменных:
    ETL_ORACLE_HOST, ETL_ORACLE_PORT, ETL_ORACLE_SID,
    ETL_ORACLE_USER, ETL_ORACLE_PWD, ETL_ORACLE_CONFIG_DIR
    ETL_POST_HOST, ETL_POST_PORT, ETL_POST_DB,
    ETL_POST_USER, ETL_POST_PWD
"""
import os

import psycopg2
import cx_Oracle


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
    con = cx_Oracle.connect(
        user=os.environ["ETL_ORACLE_USER"],
        password=os.environ["ETL_ORACLE_PWD"],
        dsn=dsn,
        encoding="UTF-8",
    )
    return con


def DbConnectPost():
    return psycopg2.connect(
        host=os.environ["ETL_POST_HOST"],
        port=os.environ.get("ETL_POST_PORT", "5432"),
        database=os.environ["ETL_POST_DB"],
        user=os.environ["ETL_POST_USER"],
        password=os.environ["ETL_POST_PWD"],
    )
