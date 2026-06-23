#!/usr/bin/env bash
#
# Локальный airflow для разработки на dev-PC (Фаза 1).
#
# Запускает `airflow standalone` из venv, перенесённого с сервера, а DAG-и и код
# берёт прямо из этого репозитория. Правишь файл в VS Code → airflow видит его
# сразу, без копирования. Останавливается по Ctrl-C.
#
# На Astra SE запись в /opt закрыта даже для root (мандатный контроль), поэтому
# runtime распаковывается в домашнюю папку и перенастраивается скриптом
# local/relocate-venv.sh. Подробности — в local/README.md.
#
set -euo pipefail

# ─── Настройки (правь при необходимости или задавай через переменные окружения) ───
RUNTIME="${ETL_LOCAL_RUNTIME:-$HOME/airflow-runtime}"   # куда распакован архив с сервера
VENV="${ETL_LOCAL_VENV:-$RUNTIME/opt/airflow/venv}"     # venv внутри него
PYBASE="${ETL_LOCAL_PYBASE:-$RUNTIME/opt/python3.10}"   # базовый python3.10
ORACLE_CLIENT="${ETL_ORACLE_LIB:-$RUNTIME/opt/oracle/instantclient_19_3}"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow-local}"     # рантайм airflow, ВНЕ репозитория
WEB_PORT="${ETL_LOCAL_PORT:-8080}"

# Корень репозитория = папка на уровень выше этого скрипта.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ─── Проверки ───
if [[ ! -e "$VENV/bin/airflow" ]]; then
    echo "ОШИБКА: не найден $VENV/bin/airflow" >&2
    echo "Сначала перенеси и перенастрой runtime — см. local/README.md (шаги 1–3)." >&2
    exit 1
fi

# Реквизиты БД из .env (он в .gitignore) — сначала, чтобы локальные структурные
# значения ниже (ETL_MODE/ETL_FULL_PATH) гарантированно их перекрыли.
if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a; . "$REPO_ROOT/.env"; set +a
fi

# ─── Окружение проекта (всегда побеждает значения из .env) ───
export ETL_FULL_PATH="$REPO_ROOT/"    # ВАЖНО: со слешем на конце
export ETL_MODE=""                     # "" = dev → используется etlFolder (не Prod)
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

# venv в PATH: `airflow standalone` запускает scheduler/webserver/triggerer
# дочерними процессами командой `airflow`, ища её в PATH (шебанги чинит relocate-venv.sh).
export PATH="$VENV/bin:$PATH"

# Библиотеки: базовый python (если собран с разделяемой libpython) и Oracle-клиент.
# Oracle нужен только при реальном запуске задач, не для парсинга DAG.
export LD_LIBRARY_PATH="$PYBASE/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if [[ -d "$ORACLE_CLIENT" ]]; then
    export LD_LIBRARY_PATH="$ORACLE_CLIENT:$LD_LIBRARY_PATH"
fi

# ─── Окружение airflow (по умолчанию лёгкий режим: SQLite + SequentialExecutor) ───
# Для параллелизма/пулов/замков (аудит) задай:
#   ETL_LOCAL_EXECUTOR=LocalExecutor
#   ETL_LOCAL_DB_CONN=postgresql+psycopg2://USER:PWD@HOST:5432/airflow_local
# SQLite параллелизм не поддерживает — только SequentialExecutor (одна задача за раз).
export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_ROOT/dags"
export AIRFLOW__CORE__EXECUTOR="${ETL_LOCAL_EXECUTOR:-SequentialExecutor}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${ETL_LOCAL_DB_CONN:-sqlite:///$AIRFLOW_HOME/airflow.db}"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$WEB_PORT"
# Скромный параллелизм для dev-PC (актуально при LocalExecutor).
export AIRFLOW__CORE__PARALLELISM="${ETL_LOCAL_PARALLELISM:-4}"

mkdir -p "$AIRFLOW_HOME"

echo "──────────────────────────────────────────────────────"
echo " Локальный airflow"
echo "   REPO_ROOT    : $REPO_ROOT"
echo "   AIRFLOW_HOME : $AIRFLOW_HOME"
echo "   DAGs         : $AIRFLOW__CORE__DAGS_FOLDER"
echo "   venv         : $VENV"
echo "   UI           : http://localhost:$WEB_PORT"
echo "   Логин/пароль : будут напечатаны ниже при первом запуске"
echo "   Стоп         : Ctrl-C"
echo "──────────────────────────────────────────────────────"

# Запускаем через интерпретатор venv напрямую — не зависим от шебангов.
exec "$VENV/bin/python" "$VENV/bin/airflow" standalone
