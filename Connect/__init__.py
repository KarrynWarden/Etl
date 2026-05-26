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
#from dotenv import load_dotenv


# .env лежит в корне репозитория (на один уровень выше этого файла).
# Если файла нет — load_dotenv тихо ничего не сделает, и значения
# подтянутся из системных переменных окружения.
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
#load_dotenv(_ENV_PATH)


_oracleClientInitialized = False


def DbConnectOrcl():
    username = "логин"
    userpwd = "пароль"
    global _oracleClientInitialized
    if not _oracleClientInitialized:
        #cx_Oracle.init_oracle_client(
        #    config_dir=os.environ.get("ETL_ORACLE_CONFIG_DIR", "/opt/oracle/config"),
        #)
        cx_Oracle.init_oracle_client(config_dir="/opt/oracle/config")
        _oracleClientInitialized = True
    #dsn = cx_Oracle.makedsn(
    #    os.environ["ETL_ORACLE_HOST"],
    #    int(os.environ.get("ETL_ORACLE_PORT", 1521)),
    #    sid=os.environ["ETL_ORACLE_SID"],
    #)
    dsn = cx_Oracle.makedsn("айпи", 1521, sid="сид")
    #return cx_Oracle.connect(
    #    user=os.environ["ETL_ORACLE_USER"],
    #    password=os.environ["ETL_ORACLE_PWD"],
    #    dsn=dsn,
    #    encoding="UTF-8",
    #)
    return cx_Oracle.connect(user=username, password=userpwd, dsn=dsn, encoding="UTF-8")


def DbConnectPost():
    #return psycopg2.connect(
    #    host=os.environ["ETL_POST_HOST"],
    #    port=os.environ.get("ETL_POST_PORT", "5432"),
    #    database=os.environ["ETL_POST_DB"],
    #    user=os.environ["ETL_POST_USER"],
    #    password=os.environ["ETL_POST_PWD"],
    #)
    return psycopg2.connect(database="бд", host="айпи", user="юзер", password="пароль", port="5432")

def DbConnectA56Orcl(): #есть задачи, которые требуют подключения через другого пользователя
    username = "логин2"
    userpwd = "пароль2"
    global _oracleClientInitialized
    if not _oracleClientInitialized:
        cx_Oracle.init_oracle_client(config_dir="/opt/oracle/config")
        _oracleClientInitialized = True
    dsn = cx_Oracle.makedsn("айпи", 1521, sid="сид")
    connection = cx_Oracle.connect(user=username, password=userpwd,
                                   dsn=dsn,
                                   encoding="UTF-8")
    print("Соединение тестового оракла подключено")
    return connection

def DbConnectA56Post():
    try:
        con2 = psycopg2.connect(database="бд",
                                host="хост",
                                user="юзер",
                                password="пароль",
                                port="5432")
        print ("соединение тестового постгреса подключено")
        return con2
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
