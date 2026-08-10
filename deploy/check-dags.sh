#!/usr/bin/env bash
#
# Обёртка над tools/check_dags.py: подобрать интерпретатор и прогнать проверку
# дерева.
#
#   bash deploy/check-dags.sh                 # это дерево
#   bash deploy/check-dags.sh /tmp/candidate  # чужое дерево (pre-receive)
#   REQUIRE_AIRFLOW=1 bash deploy/check-dags.sh ...   # нет airflow -> ошибка
#   ETL_CHECK_PYTHON=/путь/к/python bash deploy/check-dags.sh ...  # взять этот
#
# Отдельно запускать не нужно: вызывается из local/dev-push.sh,
# deploy/deploy-prod.sh и серверных хуков. Ручной запуск — если хочется
# посмотреть глазами до пуша.
#
# ПОЧЕМУ ИНТЕРПРЕТАТОР ВЫБИРАЕТСЯ ТАК ПРИДИРЧИВО.
# Первая версия брала любой python3, если под venv не импортировался airflow. На
# сервере это дало ложный отказ: код использует match/case (Functions/
# updateLogSp.py), это Python 3.10+, а /usr/bin/python3 там старше — и валидный
# файл был объявлен «invalid syntax», из-за чего pre-receive отклонил хороший
# push, деплой не поехал, а на тесте продолжал крутиться старый код. Поэтому:
#   * интерпретатор старше 3.10 не берётся ВООБЩЕ — его вердикт по синтаксису
#     ничего не значит (правильнее честно сказать «проверить нечем»);
#   * если подходящий по версии есть, но без airflow — берём его и пропускаем
#     ярус парсинга дагов, а не скатываемся на чужой python;
#   * почему кандидат отвергнут, печатается. Молчаливая подмена интерпретатора
#     один раз уже стоила заблокированной выкладки.
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

# Гейт обязан падать ЗАКРЫТО: нет самого скрипта проверки — это отказ, а не
# «проверять нечем, поехали». Иначе дерево, в котором проверки нет (старая
# ветка, случайно удалённый файл), проезжало бы гейт как исправное.
if [[ ! -f "$ROOT/tools/check_dags.py" ]]; then
    echo "check-dags: в дереве $ROOT нет tools/check_dags.py — проверить нечем." >&2
    echo "            Выкладка остановлена намеренно." >&2
    exit 1
fi

CHOSEN=""       # python с airflow (все три яруса)
NOWINGS=""      # python нужной версии, но без airflow (ярусы 1-2)
NOTES=()

_probe() {
    local p=$1 ver err
    [[ -n "$p" ]] || return 1
    [[ -x "$p" ]] || { NOTES+=("$p — нет файла или не исполняется"); return 1; }
    if ! ver=$("$p" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>&1); then
        NOTES+=("$p — не запускается: $(tail -1 <<<"$ver")"); return 1
    fi
    if ! "$p" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
        NOTES+=("$p — python $ver, а коду нужен 3.10+ (match/case); его вердикт по синтаксису недостоверен")
        return 1
    fi
    if err=$("$p" -c 'import airflow' 2>&1); then
        CHOSEN="$p"; return 0
    fi
    NOTES+=("$p — python $ver, но airflow не импортируется: $(tail -1 <<<"$err")")
    [[ -z "$NOWINGS" ]] && NOWINGS="$p"
    return 1
}

for cand in "${ETL_CHECK_PYTHON:-}" \
            "${ETL_LOCAL_VENV:+$ETL_LOCAL_VENV/bin/python}" \
            "$RUNTIME/opt/airflow/venv/bin/python" \
            "/opt/airflow/venv/bin/python" \
            "$(command -v python3 || true)"; do
    _probe "$cand" && break
done

PY="${CHOSEN:-$NOWINGS}"
if [[ -z "$PY" ]]; then
    echo "check-dags: не нашёл интерпретатор, которым можно проверять." >&2
    printf '  · %s\n' "${NOTES[@]}" >&2
    echo "  Задай явно: ETL_CHECK_PYTHON=/путь/к/python3.10+" >&2
    exit 1
fi

echo "== проверка перед выкладкой (интерпретатор: $PY) =="
if [[ -z "$CHOSEN" ]]; then
    echo "   airflow в нём нет — ярус парсинга DAG'ов будет пропущен"
fi
# Отвергнутых кандидатов показываем всегда: именно молчание об этом однажды
# превратило «нет airflow» в ложную синтаксическую ошибку. Взятый в работу
# интерпретатор в список не попадает, даже если у него нет airflow.
for n in "${NOTES[@]:-}"; do
    [[ -n "$n" && "$n" != "$PY — "* ]] && echo "   пропущен $n"
done

ARGS=("$ROOT/tools/check_dags.py" "$ROOT")
[[ "${REQUIRE_AIRFLOW:-}" == "1" ]] && ARGS+=(--require-airflow)

exec "$PY" "${ARGS[@]}"
