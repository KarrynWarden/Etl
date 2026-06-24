#!/usr/bin/env bash
#
# Разовая команда airflow в том же окружении, что и airflow-local.sh (без standalone).
#
# Метадата-БД берётся из ETL_LOCAL_DB_CONN — задавай её так же, как при запуске,
# иначе команда уйдёт в SQLite, а не в твой Postgres.
#
# Примеры:
#   ETL_LOCAL_DB_CONN="postgresql+psycopg2://USER:PWD@10.0.15.33:5432/airflow_local" \
#     bash local/airflow-cli.sh users create --username admin --password admin \
#         --firstname a --lastname a --role Admin --email a@a.a
#   ETL_LOCAL_DB_CONN="postgresql+psycopg2://USER:PWD@10.0.15.33:5432/airflow_local" \
#     bash local/airflow-cli.sh users list
#   bash local/airflow-cli.sh config get-value database sql_alchemy_conn
#
set -euo pipefail

RUNTIME="${ETL_LOCAL_RUNTIME:-$HOME/airflow-runtime}"
VENV="${ETL_LOCAL_VENV:-$RUNTIME/opt/airflow/venv}"
PYBASE="${ETL_LOCAL_PYBASE:-$RUNTIME/opt/python3.10}"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow-local}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -e "$VENV/bin/airflow" ]]; then
    echo "ОШИБКА: не найден $VENV/bin/airflow (перенеси runtime — см. local/README.md)." >&2
    exit 1
fi

export PATH="$VENV/bin:$PATH"
export PGGSSENCMODE="${PGGSSENCMODE:-disable}"
export AIRFLOW_HOME
export ETL_FULL_PATH="$REPO_ROOT/"
export ETL_MODE=""
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"
export LD_LIBRARY_PATH="$PYBASE/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${ETL_LOCAL_DB_CONN:-sqlite:///$AIRFLOW_HOME/airflow.db}"

exec "$VENV/bin/python" "$VENV/bin/airflow" "$@"
