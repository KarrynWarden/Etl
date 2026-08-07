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

detectProdVar() {
    local name=$1 u val envFile
    for u in "${UNITS[@]}"; do
        val=$(runningEnv "$u" "$name") && [[ -n $val ]] && { echo "$val"; return 0; }
    done
    for u in "${UNITS[@]}"; do
        val=$(systemctl show -p Environment --value "$u" 2>/dev/null \
              | tr ' ' '\n' | sed -n "s/^$name=//p" | head -1)
        [[ -n $val ]] && { echo "$val"; return 0; }
        while read -r envFile; do
            [[ -f $envFile ]] || continue
            val=$(sed -n "s/^[[:space:]]*$name=//p" "$envFile" | tail -1)
            [[ -n $val ]] && { echo "$val"; return 0; }
        done < <(systemctl cat "$u" 2>/dev/null | sed -n 's/^EnvironmentFile=-\?//p')
    done
    return 1
}

detectProdHome() { detectProdVar AIRFLOW_HOME; }

# airflow со СТАРЫМ окружением прода (то, что работает сейчас).
# AIRFLOW_CONFIG обязателен, если он есть у прода: без него airflow не найдёт
# конфиг, молча создаст новый рядом и уйдёт в sqlite вместо боевой базы —
# и весь отчёт ниже будет про пустую чужую БД.
airflowOld() {
    sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" \
        ${PROD_CONFIG:+AIRFLOW_CONFIG="$PROD_CONFIG"} "$VENV/bin/airflow" "$@"
}
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
PROD_CONFIG=${PROD_CONFIG:-$(detectProdVar AIRFLOW_CONFIG || true)}
[[ -n $PROD_CONFIG ]] && ok "AIRFLOW_CONFIG прода: $PROD_CONFIG"
CFG=${PROD_CONFIG:-$PROD_HOME/airflow.cfg}
if [[ -f $CFG ]]; then
    ok "airflow.cfg прода: $CFG"
else
    bad "не нашёл airflow.cfg прода (пробовал $CFG) — прод запущен не так, как я думаю. Посмотри: systemctl cat ${UNITS[0]}"
fi
if [[ -f $ENVFILE ]]; then
    for var in AIRFLOW_HOME AIRFLOW_CONFIG; do
        envVal=$(sed -n "s/^$var=//p" "$ENVFILE" | head -1)
        runVal=$(detectProdVar "$var" || true)
        if [[ -z $envVal && -z $runVal ]]; then
            continue
        elif [[ $envVal == "$runVal" ]]; then
            ok "в $ENVFILE тот же $var"
        else
            bad "в $ENVFILE стоит $var=${envVal:-<пусто>}, а прод работает с ${runVal:-<не задан>} — переключение увело бы его на чужой конфиг. Пересобери: bash setup-airflow-prod.sh"
        fi
    done
fi

echo
echo "=== 1. Подготовка и рубильник ==="
for d in "$ROOT" "$SRC" "$ROOT/etl.git"; do
    [[ -d $d ]] && ok "каталог $d" || bad "нет каталога $d"
done
[[ -f $ENVFILE ]] && ok "EnvironmentFile $ENVFILE" || bad "нет $ENVFILE"
# Проверка дагов больше не отдельная копия на сервере, а скрипт из репозитория.
[[ -f $SRC/deploy/check-dags.sh ]] && ok "$SRC/deploy/check-dags.sh" \
    || bad "нет $SRC/deploy/check-dags.sh (код ещё не выложен?)"
[[ -x $ROOT/etl.git/hooks/pre-receive ]] && ok "pre-receive hook (гейт выкладки)" \
    || bad "нет pre-receive hook — push на прод НЕ проверяется"
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
# У scheduler'а environ читается пустым: airflow меняет заголовок процесса
# (setproctitle) и затирает область окружения. Для таких — судим по systemd.
envFileApplied() {
    systemctl show -p EnvironmentFiles --value "$1" 2>/dev/null | grep -qF "$ENVFILE" \
        || systemctl cat "$1" 2>/dev/null | grep -qF "EnvironmentFile=$ENVFILE"
}
for u in "${UNITS[@]}"; do
    got=$(runningEnv "$u" AIRFLOW__CORE__DAGS_FOLDER || true)
    if [[ $active -ne ${#UNITS[@]} ]]; then
        echo "       $u сейчас читает: ${got:-<из airflow.cfg>}"
    elif [[ $got == "$SRC/dags" ]]; then
        ok "$u читает $got"
    elif [[ -n $got ]]; then
        bad "$u читает '$got', а не $SRC/dags — переключение не подействовало"
    elif envFileApplied "$u"; then
        ok "$u: systemd применил $ENVFILE (environ не читается — setproctitle)"
    else
        bad "$u: systemd не применил $ENVFILE — переключение не подействовало"
    fi
done
# Что планировщик разбирает прямо сейчас — пути видны в именах его процессов.
parsing=$(pgrep -a -f DagFileProcessor 2>/dev/null | grep -oE '/[^ ]+\.py' | head -3 || true)
[[ -n $parsing ]] && echo "       разбирается сейчас: $(echo "$parsing" | tr '\n' ' ')"
# Каталог без суффикса .service systemd не читает — ранняя версия скрипта
# создавала именно такой, и переключение молча не срабатывало.
for u in "${UNITS[@]}"; do
    [[ -e /etc/systemd/system/$u.d/$DROPIN_NAME ]] && \
        bad "лишний каталог /etc/systemd/system/$u.d — systemd его игнорирует, удали (нужен $u.service.d)"
done

echo
echo "=== 1b. Процессы airflow: все ли под своими юнитами ==="
# Второй scheduler, запущенный мимо systemd, не перезапускается вместе с юнитом
# и продолжает работать со старым окружением: переключение выглядит успешным,
# а UI и планирование остаются прежними. Два планировщика на одной metadata —
# ещё и дублирующиеся запуски задач.
mapfile -t apids < <(pgrep -f 'bin/airflow (scheduler|webserver)$|bin/airflow (scheduler|webserver) ' 2>/dev/null || true)
if [[ ${#apids[@]} -eq 0 ]]; then
    warn "не нашёл процессов airflow scheduler/webserver"
else
    declare -A unitPids=()
    for p in "${apids[@]}"; do
        [[ -r /proc/$p/cgroup ]] || continue
        punit=$(grep -oE '[A-Za-z0-9_@.\-]+\.service' "/proc/$p/cgroup" 2>/dev/null | tail -1)
        cmd=$(tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | cut -c1-60)
        started=$(ps -o lstart= -p "$p" 2>/dev/null | sed 's/^ *//')
        if [[ -z $punit ]]; then
            bad "PID $p вне systemd: $cmd (запущен $started)"
        else
            known=0
            for u in "${UNITS[@]}"; do [[ $punit == "$(unitFull "$u")" ]] && known=1; done
            if [[ $known -eq 1 ]]; then
                unitPids[$punit]="${unitPids[$punit]:-} $p"
            elif [[ $punit == *airflow-test* ]]; then
                echo "       PID $p — тестовый airflow ($punit), это нормально"
            else
                bad "PID $p принадлежит чужому юниту $punit: $cmd"
            fi
        fi
    done
    for u in "${!unitPids[@]}"; do
        ok "$u:${unitPids[$u]}"
    done
fi

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
    if PARSE=$(REQUIRE_AIRFLOW=1 bash "$SRC/deploy/check-dags.sh" "$SRC" 2>&1); then
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
