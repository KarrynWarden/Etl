"""Подключения к БД.

Все реквизиты берутся из переменных окружения — в коде ни логинов, ни паролей,
ни IP. Удобный способ задать их — файл `.env` рядом с корнем проекта (см.
`.env.example`); он в `.gitignore` и в репозиторий не попадает. `.env`
подхватывается шеллом при запуске (см. local/airflow-local.sh), а если установлен
python-dotenv — ещё и автоматически этим модулем.

Именованные наборы реквизитов (по одному на пользователя/БД в рамках сегмента):
    Oracle:    ETL_ORACLE_<NAME>_HOST, _PORT, _SID, _USER, _PWD
    Postgres:  ETL_POST_<NAME>_HOST, _PORT, _DB, _USER, _PWD
где <NAME> — MAIN, A56 и т.д. Сегмент (local/test/prod) определяется тем, какой
.env загружен: одни и те же имена, разные значения.

Клиент Oracle:
    ETL_ORACLE_CONFIG_DIR  — каталог с tnsnames.ora/sqlnet.ora (по умолчанию
                             /opt/oracle/config — как на сервере);
    ETL_ORACLE_LIB_DIR     — каталог Instant Client (опционально; если не задан,
                             библиотеки ищутся через LD_LIBRARY_PATH).
"""
import os
from pathlib import Path

import psycopg2
import cx_Oracle

# Если установлен python-dotenv — подгрузим .env автоматически. Не обязателен:
# при запуске через local/airflow-local.sh .env уже экспортирован в окружение.
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass


_oracleClientInitialized = False


def _env(name):
    """Обязательная переменная окружения; понятная ошибка, если не задана."""
    try:
        return os.environ[name]
    except KeyError:
        raise RuntimeError(
            f"Не задана переменная окружения {name}. "
            f"Заполни .env (шаблон — .env.example); он подхватывается при запуске."
        )


def _initOracleClient():
    global _oracleClientInitialized
    if _oracleClientInitialized:
        return
    kwargs = {"config_dir": os.environ.get("ETL_ORACLE_CONFIG_DIR", "/opt/oracle/config")}
    libDir = os.environ.get("ETL_ORACLE_LIB_DIR")
    if libDir:
        kwargs["lib_dir"] = libDir
    cx_Oracle.init_oracle_client(**kwargs)
    _oracleClientInitialized = True


def connectOracle(name="MAIN"):
    """Подключение к Oracle по имени набора реквизитов (MAIN, A56, ...)."""
    _initOracleClient()
    p = name.upper()
    dsn = cx_Oracle.makedsn(
        _env(f"ETL_ORACLE_{p}_HOST"),
        int(os.environ.get(f"ETL_ORACLE_{p}_PORT", 1521)),
        sid=_env(f"ETL_ORACLE_{p}_SID"),
    )
    return cx_Oracle.connect(
        user=_env(f"ETL_ORACLE_{p}_USER"),
        password=_env(f"ETL_ORACLE_{p}_PWD"),
        dsn=dsn,
        encoding="UTF-8",
    )


def connectPostgres(name="MAIN"):
    """Подключение к PostgreSQL по имени набора реквизитов (MAIN, A56, ...)."""
    p = name.upper()
    return psycopg2.connect(
        host=_env(f"ETL_POST_{p}_HOST"),
        port=os.environ.get(f"ETL_POST_{p}_PORT", "5432"),
        database=_env(f"ETL_POST_{p}_DB"),
        user=_env(f"ETL_POST_{p}_USER"),
        password=_env(f"ETL_POST_{p}_PWD"),
    )


# ─── Обратная совместимость: прежние имена функций ───
# MAIN — основной пользователь сегмента, A56 — второй пользователь (другой логин).
def DbConnectOrcl():
    return connectOracle("MAIN")


def DbConnectPost():
    return connectPostgres("MAIN")


def DbConnectA56Orcl():  # задачи, которым нужен другой пользователь Oracle
    return connectOracle("A56")


def DbConnectA56Post():  # задачи, которым нужен другой пользователь Postgres
    return connectPostgres("A56")
