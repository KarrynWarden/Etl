#!/usr/bin/env bash
#
# Локальный airflow для разработки на dev-PC (Фаза 1).
#
# Запускает `airflow standalone` из venv, перенесённого с сервера, а DAG-и и код
# берёт прямо из этого репозитория. Правишь файл в VS Code → airflow видит его
# сразу, без копирования. Останавливается по Ctrl-C.
#
# Подробная инструкция по переносу runtime с сервера — в local/README.md.
#
set -euo pipefail

# ─── Настройки (правь при необходимости или задавай через переменные окружения) ───
VENV="${ETL_LOCAL_VENV:-/opt/airflow/venv}"            # venv, перенесённый с сервера
ORACLE_CLIENT="${ETL_ORACLE_LIB:-/opt/oracle/instantclient_19_3}"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow-local}"    # рантайм airflow, ВНЕ репозитория
WEB_PORT="${ETL_LOCAL_PORT:-8080}"

# Корень репозитория = папка на уровень выше этого скрипта.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ─── Проверки ───
if [[ ! -x "$VENV/bin/airflow" ]]; then
    echo "ОШИБКА: не найден $VENV/bin/airflow" >&2
    echo "Сначала перенеси runtime с сервера — см. local/README.md (шаги 1–2)." >&2
    exit 1
fi

# ─── Окружение проекта ───
export ETL_FULL_PATH="$REPO_ROOT/"    # ВАЖНО: со слешем на конце
export ETL_MODE=""                     # "" = dev → используется etlFolder (не Prod)
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

# Oracle Instant Client нужен только при реальном запуске задач, не для парсинга DAG.
if [[ -d "$ORACLE_CLIENT" ]]; then
    export LD_LIBRARY_PATH="$ORACLE_CLIENT${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# Реквизиты БД из .env (он в .gitignore) — если есть, экспортируем в окружение.
if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a; . "$REPO_ROOT/.env"; set +a
fi

# ─── Окружение airflow (лёгкий режим: SQLite + SequentialExecutor) ───
export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$REPO_ROOT/dags"
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$WEB_PORT"

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

exec "$VENV/bin/airflow" standalone
