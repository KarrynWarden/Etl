#!/usr/bin/env bash
#
# Обёртка над tools/check_dags.py: подобрать интерпретатор, в котором есть
# airflow, и прогнать проверку дерева.
#
#   bash deploy/check-dags.sh                 # это дерево
#   bash deploy/check-dags.sh /tmp/candidate  # чужое дерево (pre-receive)
#   REQUIRE_AIRFLOW=1 bash deploy/check-dags.sh ...   # нет airflow -> ошибка
#
# Отдельно запускать не нужно: вызывается из local/dev-push.sh,
# deploy/deploy-prod.sh и серверного pre-receive. Ручной запуск — если хочется
# посмотреть глазами до пуша.
#
# Зачем подбор интерпретатора: ярусы «синтаксис» и «конфиг» работают на любом
# python3, а парсинг DAG'ов требует пакета airflow. На dev-PC он в перенесённом
# с сервера venv (см. local/README.md), на сервере — в /opt/airflow/venv.
# Сам airflow при этом НЕ запускается: DagBag — это разбор файлов, а не сервис.
#
# Коды возврата — как у tools/check_dags.py: 0 прошло, 1 ошибки,
# 2 прошло частично (нет airflow, даги не парсились).
#
set -uo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ROOT="$(cd "$ROOT" && pwd)"

# Перенесённый с сервера runtime: те же переменные, что у local/airflow-local.sh,
# чтобы не заводить второй набор настроек.
RUNTIME="${ETL_LOCAL_RUNTIME:-$HOME/airflow-runtime}"
PYBASE="${ETL_LOCAL_PYBASE:-$RUNTIME/opt/python3.10}"
# Перенесённый python слинкован с libpython из своего PYBASE — без этого он не
# стартует. Oracle-клиент для парсинга не нужен (нужен только на запуске задач).
[[ -d "$PYBASE/lib" ]] && export LD_LIBRARY_PATH="$PYBASE/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

pick_python() {
    local cands=() p
    [[ -n "${ETL_CHECK_PYTHON:-}" ]]  && cands+=("$ETL_CHECK_PYTHON")
    [[ -n "${ETL_LOCAL_VENV:-}" ]]    && cands+=("$ETL_LOCAL_VENV/bin/python")
    cands+=("$RUNTIME/opt/airflow/venv/bin/python" "/opt/airflow/venv/bin/python")
    for p in "${cands[@]}"; do
        if [[ -x "$p" ]] && "$p" -c 'import airflow' >/dev/null 2>&1; then
            printf '%s\n' "$p"; return 0
        fi
    done
    # airflow не нашёлся — ярусы 1-2 всё равно отработают, скрипт скажет об этом
    # сам (код 2), а вызывающий решит, ругаться или блокировать выкладку.
    command -v python3
}

# Гейт обязан падать ЗАКРЫТО: нет самого скрипта проверки — это отказ, а не
# «проверять нечем, поеха��и». Иначе дерево, в котором проверки нет (старая
# ветка, случайно удалённый файл), проезжало бы гейт как исправное.
if [[ ! -f "$ROOT/tools/check_dags.py" ]]; then
    echo "check-dags: в дереве $ROOT нет tools/check_dags.py — проверить нечем." >&2
    echo "            Выкладка остановлена намеренно." >&2
    exit 1
fi

PY="$(pick_python)"
[[ -n "$PY" ]] || { echo "check-dags: не найден ни один python3" >&2; exit 1; }

ARGS=("$ROOT/tools/check_dags.py" "$ROOT")
[[ "${REQUIRE_AIRFLOW:-}" == "1" ]] && ARGS+=(--require-airflow)

echo "== проверка перед выкладкой (интерпретатор: $PY) =="
exec "$PY" "${ARGS[@]}"
