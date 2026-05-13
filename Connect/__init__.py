"""Подключения к БД.

Параметры временно захардкожены (на сервере нет интернета — нельзя
поставить python-dotenv). Поправь значения констант ниже под свою среду.
"""
import cx_Oracle
import psycopg2


# --- Oracle ---
ORACLE_HOST = "10.0.15.9"
ORACLE_PORT = 1521
ORACLE_SID = "ias"
ORACLE_USER = "имя_пользователя"
ORACLE_PWD = "пароль"
ORACLE_CONFIG_DIR = "/opt/oracle/config"

# --- PostgreSQL ---
POSTGRES_HOST = "10.0.15.35"
POSTGRES_PORT = "5432"
POSTGRES_DB = "ias5db"
POSTGRES_USER = "имя_пользователя"
POSTGRES_PWD = "пароль"


_oracleClientInitialized = False


def DbConnectOrcl():
    global _oracleClientInitialized
    if not _oracleClientInitialized:
        cx_Oracle.init_oracle_client(config_dir=ORACLE_CONFIG_DIR)
        _oracleClientInitialized = True
    dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
    return cx_Oracle.connect(
        user=ORACLE_USER,
        password=ORACLE_PWD,
        dsn=dsn,
        encoding="UTF-8",
    )


def DbConnectPost():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD,
    )
