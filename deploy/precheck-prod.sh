#!/usr/bin/env bash
#
# Предпроверка ПЕРЕД переключением прода. Ничего не переключает и не
# перезапускает: только читает и печатает отчёт.
#
#   ssh devel@airflow → sudo -s → bash precheck-prod.sh
#
# Проверяет:
#   1. что подготовка (setup-airflow-prod.sh) легла и рубильник ещё выключен;
#   2. что новый код разложен и .env с боевыми реквизитами на месте и читаем;
#   3. что новые даги парсятся с окружением прода (тот же гейт, что в хуке);
#   4. пул Etl нужного размера;
#   5. какие dag_id совпадут со старыми (унаследуют паузу и историю),
#      какие появятся впервые, какие старые исчезнут;
#   6. какие даги прода сейчас ВКЛЮЧЕНЫ — список сохраняется в файл, по нему
#      потом гасить их перед переключением и возвращать при откате;
#   7. готовность боевых БД (tools/preflight.py), если есть .env.
#
# Единственное, что скрипт пишет — файл со списком включённых дагов
# в $ROOT/state/. Прод, его airflow.cfg и боевые БД не трогаются.
#
set -uo pipefail          # без -e: отчёт должен доходить до конца

### ─────────── КОНФИГ (как в setup-airflow-prod.sh) ───────────
ROOT=/opt/airflow-prod
GROUP=etlprod
VENV=/opt/airflow/venv
RUNAS=airflow
PROD_HOME=${PROD_HOME:-}               # пусто = определить по работающей службе
UNITS=(airflow-scheduler airflow-webserver)
ETL_POOL_NAME=Etl
ETL_POOL_SLOTS=100
DROPIN_NAME=10-etl-prod.conf

SRC=$ROOT/prod-src
ENVFILE=$ROOT/airflow-prod.env
STATE=$ROOT/state
### ────────────────────────────────────────────────────────────

[[ $EUID -eq 0 ]] || { echo "Запускай от root (sudo -s)"; exit 1; }
cd /

problems=0
ok()   { echo "  OK   $*"; }
bad()  { echo "  НЕТ  $*"; problems=$((problems + 1)); }
warn() { echo "  !!   $*"; problems=$((problems + 1)); }

unitFull() { [[ $1 == *.* ]] && echo "$1" || echo "$1.service"; }
dropinPath() { echo "/etc/systemd/system/$(unitFull "$1").d/$DROPIN_NAME"; }

# Переменная окружения работающей службы — правда о том, как прод запущен.
runningEnv() {
    local pid; pid=$(systemctl show -p MainPID --value "$1" 2>/dev/null || true)
    [[ -n ${pid:-} && $pid -gt 0 && -r /proc/$pid/environ ]] || return 1
    tr '\0' '\n' < "/proc/$pid/environ" | sed -n "s/^$2=//p" | head -1
}

detectProdHome() {
    local u home envFile
    for u in "${UNITS[@]}"; do
        home=$(runningEnv "$u" AIRFLOW_HOME) && [[ -n $home ]] && { echo "$home"; return 0; }
    done
    for u in "${UNITS[@]}"; do
        home=$(systemctl show -p Environment --value "$u" 2>/dev/null \
               | tr ' ' '\n' | sed -n 's/^AIRFLOW_HOME=//p' | head -1)
        [[ -n $home ]] && { echo "$home"; return 0; }
        while read -r envFile; do
            [[ -f $envFile ]] || continue
            home=$(sed -n 's/^[[:space:]]*AIRFLOW_HOME=//p' "$envFile" | tail -1)
            [[ -n $home ]] && { echo "$home"; return 0; }
        done < <(systemctl cat "$u" 2>/dev/null | sed -n 's/^EnvironmentFile=-\?//p')
    done
    return 1
}

# airflow со СТАРЫМ окружением прода (то, что работает сейчас)
airflowOld() { sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" "$VENV/bin/airflow" "$@"; }
# airflow с НОВЫМ окружением (папка дагов = prod-src/dags)
airflowNew() { sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/airflow" "$@"; }

# dag_id из вывода `airflow dags list` (json надёжнее таблицы)
dagIds() {
    python3 -c '
import json, sys
try:
    rows = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for r in rows:
    print(r.get("dag_id", ""))
' 2>/dev/null | grep -v '^$' | sort
}

echo "=== 0. AIRFLOW_HOME прода ==="
if [[ -z $PROD_HOME ]]; then
    if PROD_HOME=$(detectProdHome); then
        ok "определён по работающей службе: $PROD_HOME"
    else
        bad "не смог определить AIRFLOW_HOME прода — передай явно: PROD_HOME=/путь bash precheck-prod.sh"
        PROD_HOME=/opt/airflow
        echo "       (дальше считаю $PROD_HOME — результаты по метаданным могут врать)"
    fi
else
    ok "задан вручную: $PROD_HOME"
fi
[[ -f $PROD_HOME/airflow.cfg ]] && ok "$PROD_HOME/airflow.cfg на месте" \
    || bad "нет $PROD_HOME/airflow.cfg — это точно AIRFLOW_HOME прода?"
if [[ -f $ENVFILE ]]; then
    envHome=$(sed -n 's/^AIRFLOW_HOME=//p' "$ENVFILE" | head -1)
    [[ $envHome == "$PROD_HOME" ]] && ok "в $ENVFILE тот же AIRFLOW_HOME" \
        || bad "в $ENVFILE стоит AIRFLOW_HOME=$envHome, а прод работает с $PROD_HOME — переключение увело бы его на чужой airflow.cfg. Почини: PROD_HOME=$PROD_HOME bash setup-airflow-prod.sh"
fi

echo
echo "=== 1. Подготовка и рубильник ==="
for d in "$ROOT" "$SRC" "$ROOT/etl.git" "$ROOT/bin"; do
    [[ -d $d ]] && ok "каталог $d" || bad "нет каталога $d"
done
[[ -f $ENVFILE ]] && ok "EnvironmentFile $ENVFILE" || bad "нет $ENVFILE"
[[ -x $ROOT/bin/check_dags.sh ]] && ok "$ROOT/bin/check_dags.sh" || bad "нет $ROOT/bin/check_dags.sh"
[[ -x $ROOT/etl.git/hooks/post-receive ]] && ok "post-receive hook" || bad "нет post-receive hook"

active=0
for u in "${UNITS[@]}"; do
    [[ -f $(dropinPath "$u") ]] && active=$((active + 1))
done
if [[ $active -eq 0 ]]; then
    ok "рубильник ВЫКЛЮЧЕН — прод работает на старом коде (так и должно быть до переключения)"
elif [[ $active -eq ${#UNITS[@]} ]]; then
    echo "  --   рубильник включён у всех юнитов"
else
    bad "drop-in стоит только у части юнитов ($active из ${#UNITS[@]}) — почини до переключения"
fi
# Файлы drop-in'а — это намерение; правда в том, с чем реально работают процессы.
for u in "${UNITS[@]}"; do
    got=$(runningEnv "$u" AIRFLOW__CORE__DAGS_FOLDER || true)
    if [[ $active -eq ${#UNITS[@]} ]]; then
        [[ $got == "$SRC/dags" ]] && ok "$u читает $got" \
            || bad "$u читает '${got:-<из airflow.cfg>}', а не $SRC/dags — переключение не подействовало"
    else
        echo "       $u сейчас читает: ${got:-<из airflow.cfg>}"
    fi
done
# Каталог без суффикса .service systemd не читает — ранняя версия скрипта
# создавала именно такой, и переключение молча не срабатывало.
for u in "${UNITS[@]}"; do
    [[ -e /etc/systemd/system/$u.d/$DROPIN_NAME ]] && \
        bad "лишний каталог /etc/systemd/system/$u.d — systemd его игнорирует, удали (нужен $u.service.d)"
done

echo
echo "=== 2. Код и реквизиты ==="
if [[ -d $SRC/dags ]]; then
    ok "код разложен: $(ls "$SRC/dags"/*.py 2>/dev/null | wc -l) файлов дагов в $SRC/dags"
    echo "       версия: $(git --git-dir="$ROOT/etl.git" log -1 --format='%h %ad %s' --date=short 2>/dev/null || echo '?')"
else
    bad "в $SRC нет кода — запушь ветку prod (deploy/deploy-prod.sh)"
fi

if [[ -f $SRC/.env ]]; then
    perm=$(stat -c '%U:%G %a' "$SRC/.env")
    if sudo -u "$RUNAS" test -r "$SRC/.env"; then
        ok "$SRC/.env есть и читается пользователем $RUNAS ($perm)"
    else
        bad "$SRC/.env есть, но $RUNAS его НЕ читает ($perm) — нужно: chgrp $GROUP + chmod 640"
    fi
    if grep -qE '^[A-Z_]+=\s*$' "$SRC/.env"; then
        warn "в $SRC/.env есть незаполненные ключи:"
        grep -nE '^[A-Z_]+=\s*$' "$SRC/.env" | sed 's/^/         /'
    fi
else
    bad "нет $SRC/.env — реквизиты боевых БД (шаблон deploy/prod.env.example)"
fi

echo
echo "=== 3. Парсинг дагов с окружением прода ==="
if [[ -f $ENVFILE && -d $SRC/dags ]]; then
    if PARSE=$("$ROOT/bin/check_dags.sh" 2>&1); then
        ok "все даги нового кода импортируются"
    else
        bad "даги не парсятся — переключение сейчас отбилось бы:"
        printf '%s\n' "$PARSE" | sed 's/^/         /'
    fi
else
    warn "пропущено (нет env-файла или кода)"
fi

echo
echo "=== 4. Пул $ETL_POOL_NAME ==="
if poolOut=$(airflowOld pools get "$ETL_POOL_NAME" 2>/dev/null); then
    slots=$(printf '%s\n' "$poolOut" | grep -oE '[0-9]+' | head -1)
    if [[ -n ${slots:-} && $slots -ge $ETL_POOL_SLOTS ]]; then
        ok "пул есть, слотов $slots (нужно >= $ETL_POOL_SLOTS)"
    else
        bad "пул есть, но слотов $slots < $ETL_POOL_SLOTS — ETL-замок аудита не наберёт пул и повиснет"
    fi
else
    bad "пула $ETL_POOL_NAME нет — заведи: airflow pools set $ETL_POOL_NAME $ETL_POOL_SLOTS 'ETL'"
fi

echo
echo "=== 5. Совпадение dag_id (старый прод vs новый код) ==="
oldIds=$(airflowOld dags list -o json 2>/dev/null | dagIds)
newIds=$([[ -d $SRC/dags ]] && airflowNew dags list -o json 2>/dev/null | dagIds)
if [[ -z $oldIds || -z $newIds ]]; then
    warn "не смог получить списки дагов (старых: $(wc -l <<<"$oldIds"), новых: $(wc -l <<<"$newIds"))"
else
    echo "  --- унаследуют состояние старых (пауза, история, расписание) ---"
    comm -12 <(printf '%s\n' "$oldIds") <(printf '%s\n' "$newIds") | sed 's/^/       /'
    echo "  --- появятся впервые (встанут на паузу) ---"
    comm -13 <(printf '%s\n' "$oldIds") <(printf '%s\n' "$newIds") | sed 's/^/       /'
    echo "  --- исчезнут из новой папки (останутся в истории как removed) ---"
    comm -23 <(printf '%s\n' "$oldIds") <(printf '%s\n' "$newIds") | sed 's/^/       /'
fi

echo
echo "=== 6. Что на проде сейчас включено ==="
mkdir -p "$STATE"
SNAP=$STATE/unpaused-$(date '+%Y%m%d-%H%M').txt
airflowOld dags list -o json 2>/dev/null | python3 -c '
import json, sys
rows = json.load(sys.stdin)
# Если в выводе вообще нет признака паузы — молча выдать «все включены» нельзя.
if not any("paused" in r for r in rows):
    sys.exit(3)
for r in rows:
    if str(r.get("paused", "")).lower() in ("false", "none", ""):
        print(r["dag_id"])
' > "$SNAP" 2>/dev/null
rc=$?
if [[ $rc -ne 0 ]]; then
    warn "не смог определить включённые даги — посмотри в UI"
    rm -f "$SNAP"
elif [[ -s $SNAP ]]; then
    ok "включённых дагов: $(wc -l < "$SNAP") — список сохранён в $SNAP"
    sed 's/^/       /' "$SNAP"
    echo "       (по нему гасить перед переключением и возвращать при откате)"
else
    ok "включённых дагов нет — гасить перед переключением нечего"
fi

echo
echo "=== 7. Готовность боевых БД ==="
if [[ -f $SRC/.env ]]; then
    echo "  запускаю tools/preflight.py (только SELECT'ы)..."
    sudo -u "$RUNAS" env PYTHONPATH="$SRC" ETL_FULL_PATH="$SRC/" \
        "$VENV/bin/python" "$SRC/tools/preflight.py" | sed 's/^/  /'
    rc=${PIPESTATUS[0]}
    [[ $rc -eq 0 ]] && ok "замечаний по БД нет" || problems=$((problems + 1))
else
    warn "пропущено — нет $SRC/.env"
fi

echo
echo "======================================================================"
if [[ $problems -eq 0 ]]; then
    echo "Всё готово к переключению: sudo bash setup-airflow-prod.sh --cutover"
else
    echo "Замечаний: $problems — разбери их до переключения (deploy/README-prod.md)."
fi
exit $((problems > 0))
