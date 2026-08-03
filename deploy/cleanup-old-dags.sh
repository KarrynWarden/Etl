#!/usr/bin/env bash
#
# Убрать из metadata прода даги, которых нет в новом коде.
#
#   sudo bash cleanup-old-dags.sh              # только показать, ничего не менять
#   sudo bash cleanup-old-dags.sh --delete     # удалить, с подтверждением
#
# Зачем: при переключении папки дагов файлы старых дагов исчезают, но их записи
# остаются в metadata — в UI они висят рядом с рабочими и мешают. Airflow сам их
# только деактивирует, не удаляет.
#
# ВАЖНО: удаление дага стирает и всю его историю — запуски, статусы задач, XCom.
# Файлы логов на диске остаются. Отменить нельзя (кроме как из дампа базы),
# поэтому перед --delete имеет смысл снять дамп:
#   sudo -u postgres pg_dump -Fc airflow > /var/tmp/airflow-metadata-$(date +%F).dump
#
set -uo pipefail

ROOT=/opt/airflow-prod
VENV=/opt/airflow/venv
RUNAS=airflow
ENVFILE=$ROOT/airflow-prod.env
SRC=$ROOT/prod-src

MODE=${1:-show}
[[ $EUID -eq 0 ]] || { echo "Запускай от root (sudo -s)"; exit 1; }
[[ -f $ENVFILE ]] || { echo "Нет $ENVFILE — сначала setup-airflow-prod.sh"; exit 1; }
[[ -d $SRC/dags ]] || { echo "Нет $SRC/dags — сначала выкати код"; exit 1; }
cd /

airflowNew() { sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/airflow" "$@"; }
pythonNew()  { sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/python" "$@"; }

# Даги, которые ЕСТЬ в новом коде (разбор папки, как это делает планировщик).
inCode=$(airflowNew dags list -o json 2>/dev/null | python3 -c '
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for r in rows:
    print(r.get("dag_id", ""))
' | grep -v '^$' | sort)

if [[ -z $inCode ]]; then
    echo "Не смог получить список дагов нового кода — проверь: sudo $ROOT/bin/check_dags.sh"
    exit 1
fi

# Даги, которые ЕСТЬ в metadata (там же fileloc — по нему видно происхождение).
inDb=$(pythonNew - <<'PY'
from airflow.models import DagModel
from airflow.utils.session import create_session

with create_session() as session:
    for dag in session.query(DagModel).order_by(DagModel.dag_id):
        print(f"{dag.dag_id}\t{dag.fileloc or ''}\t{'active' if dag.is_active else 'inactive'}")
PY
)

if [[ -z $inDb ]]; then
    echo "Не смог прочитать даги из metadata — проверь окружение в $ENVFILE"
    exit 1
fi

extra=$(comm -13 <(printf '%s\n' "$inCode") <(printf '%s\n' "$inDb" | cut -f1 | sort))

echo "== Даги в новом коде: $(printf '%s\n' "$inCode" | wc -l) =="
echo "== Даги в metadata:   $(printf '%s\n' "$inDb" | wc -l) =="
echo
if [[ -z $extra ]]; then
    echo "Лишних записей нет — metadata совпадает с кодом."
    exit 0
fi

echo "== Лишние (есть в metadata, нет в коде): $(printf '%s\n' "$extra" | wc -l) =="
while read -r dagId; do
    [[ -z $dagId ]] && continue
    line=$(printf '%s\n' "$inDb" | awk -F'\t' -v d="$dagId" '$1 == d {print $2 "  [" $3 "]"}')
    printf '  %-45s %s\n' "$dagId" "$line"
done <<< "$extra"

echo
if [[ $MODE != "--delete" ]]; then
    echo "Это только просмотр. Чтобы удалить: sudo bash cleanup-old-dags.sh --delete"
    echo "Удаление стирает вместе с дагом всю его историю запусков — сними дамп:"
    echo "  sudo -u postgres pg_dump -Fc airflow > /var/tmp/airflow-metadata-\$(date +%F).dump"
    exit 0
fi

echo "Будет удалено записей: $(printf '%s\n' "$extra" | wc -l), ВМЕСТЕ С ИСТОРИЕЙ ЗАПУСКОВ."
read -r -p "Удаляем? (y/N) " answer
[[ $answer == "y" || $answer == "Y" ]] || { echo "Отменено."; exit 1; }

failed=0
skipped=0
while read -r dagId; do
    [[ -z $dagId ]] && continue
    if out=$(airflowNew dags delete -y "$dagId" 2>&1); then
        echo "  удалён: $dagId"
    elif grep -qiE 'not found|does not exist|DagNotFound' <<< "$out"; then
        # SubDAG'и (dag_id вида «родитель.потомок») удаляются вместе с
        # родительским дагом, поэтому своей очереди уже не застают.
        echo "  пропущен: $dagId — записи уже нет"
        skipped=$((skipped + 1))
    else
        echo "  !! не удалось удалить: $dagId"
        printf '%s\n' "$out" | sed 's/^/       /' | tail -3
        failed=$((failed + 1))
    fi
done <<< "$extra"

echo
[[ $skipped -gt 0 ]] && echo "Пропущено $skipped — их записи ушли вместе с родительскими дагами."
if [[ $failed -eq 0 ]]; then
    echo "Готово. В UI останутся только даги нового кода (обнови страницу)."
    echo "Проверить: sudo bash cleanup-old-dags.sh  (без ключа)"
else
    echo "Готово, но $failed записей не удалились — посмотри вручную: airflow dags delete <dag_id>"
fi
