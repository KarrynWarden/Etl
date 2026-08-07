#!/usr/bin/env bash
#
# Перевод ПРОДА на текущую версию кода с деплоем по git — тем же механизмом,
# что уже работает на тесте (bare-репо + post-receive), но для ветки `prod`.
#
#   ssh devel@airflow → sudo -s → bash setup-airflow-prod.sh [режим]
#
# Режимы:
#   (без аргументов)  подготовка: группа, каталоги, bare-репо, hook, env-файл,
#                     sudoers, пул Etl. Работающий прод при этом НЕ меняется —
#                     он продолжает крутиться на старом коде.
#   --cutover         переключение: drop-in к юнитам прода (AIRFLOW_HOME тот же,
#                     DAGS_FOLDER/PYTHONPATH — на новый код) + рестарт.
#   --rollback        откат: снять drop-in + рестарт. Прод возвращается на
#                     старую папку дагов ровно в том виде, в каком был.
#   --status          что сейчас включено.
#
# Что НЕ трогается ни в одном режиме: airflow.cfg прода, metadata-база `airflow`,
# порт вебсервера, старая папка дагов, содержимое боевых БД.
#
# Идемпотентно: повторный запуск не ломает уже созданное.
#
set -euo pipefail

### ─────────── КОНФИГ (сверить перед первым запуском) ───────────
ROOT=/opt/airflow-prod
GROUP=etlprod                          # отдельная от etldev: прод пушит не каждый
MEMBERS=(devel airflow)                # кто пушит в прод + сам airflow (читает код)
VENV=/opt/airflow/venv                 # тот же venv, что у теста
RUNAS=airflow                          # под кем крутится прод
DEPLOY_BRANCH=prod                     # какая ветка разворачивается в prod-src
# AIRFLOW_HOME прода определяется АВТОМАТИЧЕСКИ по работающей службе — задавать
# его руками нельзя: ошибка здесь увела бы прод на чужой airflow.cfg и чужую
# metadata-базу. Переопределить можно только явно: PROD_HOME=... bash ...
PROD_HOME=${PROD_HOME:-}
UNITS=(airflow-scheduler airflow-webserver)   # существующие юниты прода
ETL_POOL_NAME=Etl                      # пул, который ждёт код (_dagHelpers.ETL_POOL)
ETL_POOL_SLOTS=100                     # = ETL_POOL_SLOTS в Functions/_dagHelpers.py

BARE=$ROOT/etl.git
SRC=$ROOT/prod-src
ENVFILE=$ROOT/airflow-prod.env         # systemd EnvironmentFile (структурные переменные)
DROPIN_NAME=10-etl-prod.conf           # drop-in к юнитам прода (он же «рубильник»)
### ──────────────────────────────────────────────────────────────

MODE=${1:-prepare}
[[ $EUID -eq 0 ]] || { echo "Запускай от root (sudo -s)"; exit 1; }
cd /    # чтобы sudo -u postgres/airflow не ругался на чужой cwd

# Имя юнита с суффиксом: каталог drop-in'а ДОЛЖЕН называться <юнит>.service.d,
# иначе systemd его просто не читает, а переключение молча не срабатывает.
unitFull() { [[ $1 == *.* ]] && echo "$1" || echo "$1.service"; }
dropinDir() { echo "/etc/systemd/system/$(unitFull "$1").d"; }
dropinPath() { echo "$(dropinDir "$1")/$DROPIN_NAME"; }

cutoverActive() {
    for u in "${UNITS[@]}"; do
        [[ -f "$(dropinPath "$u")" ]] || return 1
    done
    return 0
}

# Переменная окружения работающей службы — единственный надёжный источник
# правды о том, как прод запущен на самом деле.
runningEnv() {
    local pid; pid=$(systemctl show -p MainPID --value "$1" 2>/dev/null || true)
    [[ -n ${pid:-} && $pid -gt 0 && -r /proc/$pid/environ ]] || return 1
    tr '\0' '\n' < "/proc/$pid/environ" | sed -n "s/^$2=//p" | head -1
}

# Любая переменная окружения прода: сначала у живого процесса, затем из
# определения юнита (Environment= и EnvironmentFile=).
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

# Где лежит airflow.cfg прода. AIRFLOW_CONFIG, если задан, важнее AIRFLOW_HOME:
# бывает, что дом указывает в одно место, а конфиг лежит в другом — тогда
# ориентироваться только на AIRFLOW_HOME нельзя.
prodCfgPath() {
    local cfg
    cfg=$(detectProdVar AIRFLOW_CONFIG) && [[ -n $cfg ]] && { echo "$cfg"; return 0; }
    [[ -n ${1:-} ]] && { echo "$1/airflow.cfg"; return 0; }
    return 1
}

showStatus() {
    echo "== Состояние =="
    if cutoverActive; then
        echo "  drop-in стоит у всех юнитов: ${UNITS[*]}"
    else
        echo "  drop-in НЕ установлен — прод на старом коде"
    fi
    # Факт важнее файлов: смотрим, что реально у работающих процессов.
    for u in "${UNITS[@]}"; do
        printf '  %-22s %-8s dags_folder=%s\n' "$u" \
            "$(systemctl is-active "$u" 2>/dev/null || true)" \
            "$(runningEnv "$u" AIRFLOW__CORE__DAGS_FOLDER || echo '<из airflow.cfg>')"
        printf '  %-22s %-8s AIRFLOW_HOME=%s\n' "" "" \
            "$(runningEnv "$u" AIRFLOW_HOME || echo '?')"
    done
    echo "  код       : $SRC$([[ -d $SRC/dags ]] && echo '' || echo '  (пусто — не было push)')"
    echo "  bare-репо : $BARE"
    echo "  env-файл  : $ENVFILE"
    if [[ -f $ENVFILE ]]; then
        echo "  --- $ENVFILE ---"
        sed 's/^/  /' "$ENVFILE"
    fi
}

# Применил ли systemd наш EnvironmentFile к юниту (по мнению самого systemd).
envFileApplied() {
    systemctl show -p EnvironmentFiles --value "$1" 2>/dev/null | grep -qF "$ENVFILE" \
        || systemctl cat "$1" 2>/dev/null | grep -qF "EnvironmentFile=$ENVFILE"
}

# После рестарта убедиться, что прод ДЕЙСТВИТЕЛЬНО читает новую папку дагов.
# Без этой проверки неверный путь drop-in'а выглядел бы как успешное
# переключение: файлы на месте, службы active, а окружение старое.
#
# Тонкость: у scheduler'а /proc/PID/environ читается пустым — airflow меняет
# заголовок процесса (setproctitle), затирая область окружения. Это не мешает
# работе, но судить по environ там нельзя, поэтому для таких процессов
# ориентируемся на то, что применил systemd.
verifyCutover() {
    local u got bad=0
    sleep 3
    for u in "${UNITS[@]}"; do
        got=$(runningEnv "$u" AIRFLOW__CORE__DAGS_FOLDER || true)
        if [[ $got == "$SRC/dags" ]]; then
            echo "  OK   $u читает $got"
        elif [[ -n $got ]]; then
            echo "  НЕТ  $u читает '$got', а не $SRC/dags"
            bad=1
        elif envFileApplied "$u"; then
            echo "  OK   $u: systemd применил $ENVFILE"
            echo "       (окружение процесса не читается — setproctitle, это нормально)"
        else
            echo "  НЕТ  $u: systemd не применил $ENVFILE"
            bad=1
        fi
    done
    # Заодно показать, что планировщик разбирает на самом деле: пути к файлам
    # видны прямо в именах его дочерних процессов.
    local parsing
    parsing=$(pgrep -a -f DagFileProcessor 2>/dev/null | grep -oE '/[^ ]+\.py' | head -3 || true)
    [[ -n $parsing ]] && echo "  сейчас разбираются: $(echo "$parsing" | tr '\n' ' ')"
    return $bad
}

# Выполнить airflow CLI прода с окружением нового кода.
airflowProd() {
    sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/airflow" "$@"
}

# Выполнить airflow CLI в СТАРОМ окружении прода (metadata прода, старые даги).
# AIRFLOW_CONFIG обязателен, если он есть у прода: без него airflow не найдёт
# конфиг, молча создаст новый рядом и уйдёт в sqlite вместо боевой базы.
airflowOld() {
    sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" \
        ${PROD_CONFIG:+AIRFLOW_CONFIG="$PROD_CONFIG"} "$VENV/bin/airflow" "$@"
}

case "$MODE" in
--status)
    showStatus
    exit 0
    ;;

--rollback)
    echo "== Откат: снимаю drop-in и возвращаю прод на старую папку дагов =="
    for u in "${UNITS[@]}"; do
        rm -f "$(dropinPath "$u")"
        rmdir "$(dropinDir "$u")" 2>/dev/null || true
        # каталог без суффикса .service мог остаться от ранней версии скрипта —
        # systemd его не читал, но пусть не мозолит глаза
        rm -f "/etc/systemd/system/$u.d/$DROPIN_NAME" 2>/dev/null || true
        rmdir "/etc/systemd/system/$u.d" 2>/dev/null || true
    done
    systemctl daemon-reload
    systemctl restart "${UNITS[@]}"
    echo "Готово. Прод снова читает даги из airflow.cfg (старая папка)."
    showStatus
    exit 0
    ;;

--cutover)
    [[ -d $SRC/dags ]] || { echo "В $SRC ещё нет кода — сначала запушь ветку $DEPLOY_BRANCH"; exit 1; }
    [[ -f $ENVFILE ]] || { echo "Нет $ENVFILE — сначала запусти скрипт без аргументов"; exit 1; }
    [[ -f $SRC/.env ]] || { echo "Нет $SRC/.env с реквизитами БД прода (шаблон deploy/prod.env.example)"; exit 1; }

    # Окружение в env-файле обязано совпадать с тем, как прод запущен сейчас:
    # иначе после рестарта он уедет на чужой airflow.cfg и чужую metadata-базу.
    echo "== Сверка окружения с работающим продом =="
    for var in AIRFLOW_HOME AIRFLOW_CONFIG; do
        envVal=$(sed -n "s/^$var=//p" "$ENVFILE" | head -1)
        runVal=$(detectProdVar "$var" || true)
        if [[ -z $runVal && -z $envVal ]]; then
            continue                                  # переменная не используется — и не надо
        elif [[ -z $runVal ]]; then
            echo "  !!   $var у работающего прода не найден, а в env-файле стоит '$envVal' — сверь руками:"
            echo "       systemctl cat ${UNITS[0]}"
        elif [[ $envVal != "$runVal" ]]; then
            echo "  НЕТ  в $ENVFILE стоит $var=${envVal:-<пусто>},"
            echo "       а прод работает с $var=$runVal."
            echo "       Переключение отменено: с чужим окружением прод прочитал бы"
            echo "       другой airflow.cfg и другую metadata-базу."
            echo "       Пересобери env-файл: bash setup-airflow-prod.sh"
            exit 1
        else
            echo "  OK   $var=$runVal"
        fi
    done

    # Конфиг, который прод читает сейчас, должен существовать — иначе после
    # рестарта airflow поднимется на пустой конфигурации по умолчанию.
    cfg=$(prodCfgPath "$(detectProdHome || true)" || true)
    if [[ -n $cfg && -f $cfg ]]; then
        echo "  OK   airflow.cfg: $cfg"
    else
        echo "  НЕТ  не нашёл airflow.cfg прода (пробовал '$cfg')."
        echo "       Переключение отменено — сначала разберись, откуда прод берёт конфиг:"
        echo "         systemctl cat ${UNITS[0]}"
        echo "         pid=\$(systemctl show -p MainPID --value ${UNITS[0]}); tr '\\0' '\\n' < /proc/\$pid/environ | grep -i airflow"
        exit 1
    fi

    echo "== Проверка, что новые даги парсятся с окружением прода =="
    REQUIRE_AIRFLOW=1 bash "$SRC/deploy/check-dags.sh" "$SRC" \
        || { echo "!! Даги не парсятся — переключение отменено."; exit 1; }

    echo "== drop-in к юнитам прода =="
    for u in "${UNITS[@]}"; do
        mkdir -p "$(dropinDir "$u")"
        cat > "$(dropinPath "$u")" <<DROPIN
# Переключение прода на код из $SRC. Ставится/снимается скриптом
# deploy/setup-airflow-prod.sh (--cutover / --rollback). Drop-in читается ПОСЛЕ
# основного юнита, поэтому его EnvironmentFile перекрывает Environment= юнита.
[Service]
EnvironmentFile=$ENVFILE
DROPIN
    done
    systemctl daemon-reload
    systemctl restart "${UNITS[@]}"

    echo "== Проверка, что прод реально переключился =="
    if ! verifyCutover; then
        echo
        echo "!! Переключение НЕ состоялось: службы перезапустились со старым"
        echo "!! окружением. Прод при этом не пострадал — он работает как работал."
        echo "!! Смотри, не перекрывает ли drop-in что-то в самих юнитах:"
        echo "!!   systemctl cat ${UNITS[0]}"
        showStatus
        exit 1
    fi

    echo
    echo "Переключено. Дальше — deploy/README-prod.md, раздел «После переключения»:"
    echo "  1) в UI все новые даги должны быть на паузе (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True);"
    echo "  2) снимать с паузы по одной линии и смотреть результат;"
    echo "  3) откат в любой момент: bash setup-airflow-prod.sh --rollback"
    showStatus
    exit 0
    ;;

prepare) ;;
*) echo "Неизвестный режим: $MODE (см. шапку скрипта)"; exit 1 ;;
esac

### ───────────────────────── режим prepare ─────────────────────────

echo "== 0. Проверки окружения =="
[[ -x $VENV/bin/airflow ]] || { echo "Нет $VENV/bin/airflow"; exit 1; }
for u in "${UNITS[@]}"; do
    systemctl cat "$u" >/dev/null 2>&1 || { echo "Нет юнита $u — проверь UNITS"; exit 1; }
done

if [[ -z $PROD_HOME ]]; then
    PROD_HOME=$(detectProdHome) || {
        echo "!! Не смог определить AIRFLOW_HOME прода по службам ${UNITS[*]}."
        echo "!! Посмотри сам и передай явно:"
        echo "     systemctl cat ${UNITS[0]}"
        echo "     pid=\$(systemctl show -p MainPID --value ${UNITS[0]}); tr '\\0' '\\n' < /proc/\$pid/environ | grep AIRFLOW_HOME"
        echo "     PROD_HOME=/путь bash setup-airflow-prod.sh"
        exit 1
    }
    echo "  AIRFLOW_HOME прода определён автоматически: $PROD_HOME"
else
    echo "  AIRFLOW_HOME задан вручную: $PROD_HOME"
fi
# Конфиг может лежать не в AIRFLOW_HOME: если у прода задан AIRFLOW_CONFIG,
# он главнее. Тогда эту переменную нужно пронести и в наш env-файл, иначе
# после рестарта airflow пойдёт искать конфиг в доме и не найдёт.
PROD_CONFIG=${PROD_CONFIG:-$(detectProdVar AIRFLOW_CONFIG || true)}
CFG=${PROD_CONFIG:-$PROD_HOME/airflow.cfg}
if [[ ! -f $CFG ]]; then
    echo "!! Не нашёл airflow.cfg прода (пробовал '$CFG')."
    echo "!! AIRFLOW_HOME=$PROD_HOME, AIRFLOW_CONFIG=${PROD_CONFIG:-<не задан>}."
    echo "!! Посмотри, как прод запущен на самом деле:"
    echo "     systemctl cat ${UNITS[0]}"
    echo "     pid=\$(systemctl show -p MainPID --value ${UNITS[0]}); tr '\\0' '\\n' < /proc/\$pid/environ | grep -i airflow"
    echo "!! и передай верные значения: PROD_HOME=/путь [PROD_CONFIG=/путь/airflow.cfg] bash setup-airflow-prod.sh"
    exit 1
fi
[[ -n $PROD_CONFIG ]] && echo "  AIRFLOW_CONFIG прода: $PROD_CONFIG"
echo "  ok: venv, $CFG, юниты ${UNITS[*]}"
echo "  metadata: $(awk -F'=' '/^[[:space:]]*sql_alchemy_conn/{sub(/^[^=]*=[[:space:]]*/,"");sub(/:[^:@]*@/,":***@");print;exit}' "$CFG")"

echo "== 1. Группа $GROUP и участники =="
groupadd -f "$GROUP"
for u in "${MEMBERS[@]}"; do
    if id "$u" &>/dev/null; then usermod -aG "$GROUP" "$u"; echo "  + $u"; else echo "  (нет пользователя $u, пропускаю)"; fi
done

echo "== 2. Каталоги и права =="
mkdir -p "$BARE" "$SRC" "$ROOT/bin"
# Пустой каталог плагинов. В старом AIRFLOW_HOME лежит копия кода времён MODE
# (plugins/Functions, plugins/Src) — новому коду плагины не нужны вообще, а эти
# модули ломаются об импорт и сыпят «Broken plugin». Старую папку не трогаем:
# при --rollback переменная исчезает и прод снова видит её как раньше.
mkdir -p "$ROOT/plugins"
chgrp -R "$GROUP" "$BARE" "$SRC"
chmod -R 2775 "$BARE" "$SRC"           # setgid: новые файлы наследуют группу

# DeleteDag ищет логи как <папка дагов>/../logs, то есть после переключения —
# в $SRC/logs. Связываем с настоящим каталогом логов прода: берём его из
# airflow.cfg (base_log_folder), а не из AIRFLOW_HOME — дом и конфиг могут
# лежать в разных местах. ('logs' в .gitignore, checkout симлинк не тронет.)
LOGDIR=$(awk -F'=' '/^[[:space:]]*base_log_folder/{sub(/^[^=]*=[[:space:]]*/,"");print;exit}' "$CFG")
[[ -z ${LOGDIR:-} ]] && LOGDIR=$(dirname "$CFG")/logs
if [[ -L $SRC/logs && $(readlink "$SRC/logs") != "$LOGDIR" ]]; then
    ln -sfn "$LOGDIR" "$SRC/logs"
    echo "  $SRC/logs перенацелен -> $LOGDIR"
elif [[ ! -e $SRC/logs ]]; then
    ln -s "$LOGDIR" "$SRC/logs"
    echo "  $SRC/logs -> $LOGDIR"
fi

echo "== 3. Проверка DAG'ов =="
# Своей копии проверки на сервере больше НЕТ — обе стороны зовут один скрипт из
# репозитория (deploy/check-dags.sh). Раньше здесь и в setup-airflow-test.sh
# генерились две отдельные обёртки; они разъехались, и 2026-08-07 тест сказал
# "OK" на коде, который прод не смог распарсить. Окружение скрипт строит себе
# сам (временный AIRFLOW_HOME, sqlite вместо metadata-базы), каталог дагов
# передаёт в DagBag явно и sudo ему не нужен.
rm -f "$ROOT/bin/check_dags.sh"
echo "  проверка живёт в репозитории: deploy/check-dags.sh"

echo "== 4. bare-репозиторий =="
[[ -e $BARE/HEAD ]] || git init --bare "$BARE" >/dev/null
git --git-dir="$BARE" config core.sharedRepository group
chgrp -R "$GROUP" "$BARE"; chmod -R 2775 "$BARE"

echo "== 4b. pre-receive hook (гейт ДО обновления ссылок) =="
# Единственный гейт, который реально защищает прод. post-receive проверяет код,
# который УЖЕ разложен в $SRC/dags — каталог, который scheduler перечитывает
# сам, без всякого рестарта; «сервисы не перезапущены, прод работает как
# работал» было неправдой. И его exit code git игнорирует, поэтому
# deploy-prod.sh печатал «Готово» даже на провалившейся проверке (так 2026-08-07
# и лёг прод). pre-receive проверяет временную распаковку присланного коммита:
# провал -> push отклонён, ссылки не двинулись, $SRC не тронут.
cat > "$BARE/hooks/pre-receive" <<HOOK
#!/bin/bash
umask 002
ZERO=0000000000000000000000000000000000000000
rc=0
while read oldrev newrev ref; do
    branch=\${ref#refs/heads/}
    [ "\$branch" = "$DEPLOY_BRANCH" ] || continue
    [ "\$newrev" = "\$ZERO" ] && continue          # удаление ветки — не проверяем
    TMP=\$(mktemp -d /tmp/etl-precheck-XXXXXX) || { echo "!! mktemp не отработал"; exit 1; }
    # Объекты присланного коммита ещё в карантине; git находит их по
    # GIT_OBJECT_DIRECTORY из окружения хука, так что распаковка работает
    # ДО обновления ссылок.
    git --git-dir=$BARE archive "\$newrev" | tar -x -C "\$TMP"
    if [ ! -f "\$TMP/deploy/check-dags.sh" ]; then
        # Старое дерево — почти наверняка ОТКАТ на прошлый тег, в котором
        # полной проверки ещё не было. Отклонять его нельзя: откат обязан
        # работать всегда, тем более в аварии. Но и пропускать вслепую не
        # будем — гоняем встроенный минимум, компиляцию python.
        echo "!! В дереве нет deploy/check-dags.sh (откат на старую версию?)."
        echo "   Полную проверку прогнать нечем — проверяю только синтаксис."
        DIRS=""
        for d in Functions Src Connect dags tools; do
            [ -d "\$TMP/\$d" ] && DIRS="\$DIRS \$TMP/\$d"
        done
        if [ -n "\$DIRS" ] && ! python3 -m compileall -q \$DIRS; then
            echo "!! Синтаксис не прошёл — push ОТКЛОНЁН."
            rm -rf "\$TMP"; rc=1; continue
        fi
        echo "   синтаксис чист — пропускаю."
        rm -rf "\$TMP"; continue
    fi
    echo "== проверка присланного кода (\$(git --git-dir=$BARE rev-parse --short "\$newrev")) =="
    if ! REQUIRE_AIRFLOW=1 bash "\$TMP/deploy/check-dags.sh" "\$TMP"; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "!! Push на ПРОД ОТКЛОНЁН: код не прошёл проверку.        !!"
        echo "!! На сервер ничего не легло, прод работает как работал. !!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        rc=1
    fi
    rm -rf "\$TMP"
done
exit \$rc
HOOK
chmod 2775 "$BARE/hooks/pre-receive"; chgrp "$GROUP" "$BARE/hooks/pre-receive"
echo "  ok"

echo "== 5. post-receive hook (деплой + рестарт) =="
cat > "$BARE/hooks/post-receive" <<HOOK
#!/bin/bash
set -e
umask 002

while read oldrev newrev ref; do
    branch=\${ref#refs/heads/}
    if [ "\$branch" != "$DEPLOY_BRANCH" ]; then
        echo "ветка \$branch получена (без деплоя)"
        continue
    fi

    git --git-dir=$BARE --work-tree=$SRC checkout -f "$DEPLOY_BRANCH"
    python3 "$SRC/tools/regen_config.py" config SpTableName SpOnce >/dev/null 2>&1 || true

    # Страховка, а не гейт: отказ делает pre-receive. Здесь проверяется уже
    # разложенное дерево — тем же скриптом из репозитория, без sudo.
    echo "== Проверка валидности DAG'ов =="
    if ! PARSE_LOG=\$(REQUIRE_AIRFLOW=1 bash "$SRC/deploy/check-dags.sh" "$SRC" 2>&1); then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "!! КРИТИЧЕСКАЯ ОШИБКА: новые DAG'и не парсятся!          !!"
        echo "\$PARSE_LOG"
        echo "!! Сервисы НЕ перезапущены — но код УЖЕ в $SRC/dags,  !!"
        echo "!! а его scheduler перечитывает сам. ОТКАТИТЬ НЕМЕДЛЕННО: !!"
        echo "!!   FROM=<прошлый тег prod-*> bash deploy/deploy-prod.sh !!"
        echo "!! Сюда доходят только те, кто обошёл pre-receive.       !!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        continue
    fi
    echo "  OK: все DAG'и распарсены."

    # Рестарт только если прод УЖЕ переключён на новый код. До переключения
    # (нет drop-in) код просто раскладывается — работающий прод не трогаем.
    if [ -f "$(dropinPath "${UNITS[0]}")" ]; then
        sudo systemctl restart ${UNITS[@]}
        echo "deploy: ветка $DEPLOY_BRANCH -> $SRC, прод перезапущен"
    else
        echo "deploy: ветка $DEPLOY_BRANCH -> $SRC (прод ещё на старом коде;"
        echo "        переключение: sudo bash setup-airflow-prod.sh --cutover)"
    fi
done
HOOK
chmod 2775 "$BARE/hooks/post-receive"; chgrp "$GROUP" "$BARE/hooks/post-receive"
echo "  ok"

echo "== 6. EnvironmentFile $ENVFILE =="
# Только структурные переменные. Строка подключения к metadata, порт вебсервера,
# executor и прочее остаются в airflow.cfg прода — их намеренно не дублируем.
cat > "$ENVFILE" <<ENV
AIRFLOW_HOME=$PROD_HOME
$([[ -n $PROD_CONFIG ]] && echo "AIRFLOW_CONFIG=$PROD_CONFIG")
PYTHONPATH=$SRC
ETL_FULL_PATH=$SRC/
ETL_MODE=
AIRFLOW__CORE__DAGS_FOLDER=$SRC/dags
# Новые даги появляются на ПАУЗЕ — после переключения линии включаются по одной.
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
# Примеры airflow в проде не нужны (в старом airflow.cfg они включены).
AIRFLOW__CORE__LOAD_EXAMPLES=False
# Плагины: пустой каталог. Код приезжает через PYTHONPATH, а в старом
# plugins/ лежит его копия времён MODE — она только сыплет «Broken plugin».
AIRFLOW__CORE__PLUGINS_FOLDER=$ROOT/plugins
ENV
chown "$RUNAS":"$GROUP" "$ENVFILE"; chmod 640 "$ENVFILE"
echo "  ok"

echo "== 7. sudoers: hook рестартит прод без пароля =="
# Строки под check_dags.sh больше нет: проверка сама строит себе окружение
# (временный AIRFLOW_HOME, sqlite вместо metadata-базы) и работает от того,
# кто пушит, — повышать права ей незачем.
SUDO=/etc/sudoers.d/airflow-prod
SCTL=$(command -v systemctl)
cat > "$SUDO.tmp" <<SUDOERS
%$GROUP ALL=(root) NOPASSWD: $SCTL restart ${UNITS[*]}
SUDOERS
if visudo -cf "$SUDO.tmp"; then
    mv "$SUDO.tmp" "$SUDO"; chmod 440 "$SUDO"; echo "  ok"
else
    echo "  !! sudoers невалиден, не ставлю"; rm -f "$SUDO.tmp"
fi

echo "== 8. git safe.directory =="
for d in "$BARE" "$SRC"; do
    git config --system --get-all safe.directory 2>/dev/null | grep -qxF "$d" \
        || git config --system --add safe.directory "$d"
done
echo "  ok"

echo "== 9. Пул $ETL_POOL_NAME ($ETL_POOL_SLOTS слотов) в metadata прода =="
# Код держит ETL-замок на весь пул (Functions/_dagHelpers.ETL_POOL_SLOTS).
# Если пула нет или он меньше — аудит-замок не наберёт слоты и повиснет.
if airflowOld pools get "$ETL_POOL_NAME" >/dev/null 2>&1; then
    echo "  пул уже есть — проверь размер (должно быть >= $ETL_POOL_SLOTS):"
    airflowOld pools get "$ETL_POOL_NAME" | sed 's/^/    /'
else
    airflowOld pools set "$ETL_POOL_NAME" "$ETL_POOL_SLOTS" "ETL: перенос данных" || \
        echo "  !! не смог создать пул — заведи руками в UI (Admin -> Pools)"
fi

echo
echo "Подготовка закончена. Работающий прод НЕ тронут."
echo
echo "Дальше (подробно — deploy/README-prod.md):"
echo "  1) реквизиты БОЕВЫХ БД:  cp deploy/prod.env.example $SRC/.env  (после первого push)"
echo "     затем: chgrp $GROUP $SRC/.env && chmod 640 $SRC/.env"
echo "  2) первый push кода с dev-PC:"
echo "       git remote add prod ssh://devel@airflow$BARE"
echo "       bash deploy/deploy-prod.sh"
echo "  3) готовность боевых БД:  PYTHONPATH=$SRC python3 $SRC/tools/preflight.py"
echo "  4) переключение:          sudo bash setup-airflow-prod.sh --cutover"
echo "  5) откат в любой момент:  sudo bash setup-airflow-prod.sh --rollback"
