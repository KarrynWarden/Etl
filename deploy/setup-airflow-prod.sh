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
PROD_HOME=/opt/airflow/airflow         # AIRFLOW_HOME прода (там же airflow.cfg)
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

dropinPath() { echo "/etc/systemd/system/$1.d/$DROPIN_NAME"; }

cutoverActive() {
    for u in "${UNITS[@]}"; do
        [[ -f "$(dropinPath "$u")" ]] || return 1
    done
    return 0
}

showStatus() {
    echo "== Состояние =="
    if cutoverActive; then
        echo "  ПЕРЕКЛЮЧЕНО на новый код: drop-in стоит у всех юнитов ${UNITS[*]}"
    else
        echo "  Прод на СТАРОМ коде (drop-in не установлен)"
    fi
    echo "  код       : $SRC$([[ -d $SRC/dags ]] && echo '' || echo '  (пусто — не было push)')"
    echo "  bare-репо : $BARE"
    echo "  env-файл  : $ENVFILE"
    for u in "${UNITS[@]}"; do
        printf '  %-22s %s\n' "$u" "$(systemctl is-active "$u" 2>/dev/null || true)"
    done
    if [[ -f $ENVFILE ]]; then
        echo "  --- $ENVFILE ---"
        sed 's/^/  /' "$ENVFILE"
    fi
}

# Выполнить airflow CLI прода с окружением нового кода.
airflowProd() {
    sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/airflow" "$@"
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

    echo "== Проверка, что новые даги парсятся с окружением прода =="
    "$ROOT/bin/check_dags.sh" || { echo "!! Даги не парсятся — переключение отменено."; exit 1; }

    echo "== drop-in к юнитам прода =="
    for u in "${UNITS[@]}"; do
        mkdir -p "/etc/systemd/system/$u.d"
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
[[ -f $PROD_HOME/airflow.cfg ]] || { echo "Нет $PROD_HOME/airflow.cfg — проверь PROD_HOME"; exit 1; }
for u in "${UNITS[@]}"; do
    systemctl cat "$u" >/dev/null 2>&1 || { echo "Нет юнита $u — проверь UNITS"; exit 1; }
done
echo "  ok: venv, $PROD_HOME/airflow.cfg, юниты ${UNITS[*]}"

echo "== 1. Группа $GROUP и участники =="
groupadd -f "$GROUP"
for u in "${MEMBERS[@]}"; do
    if id "$u" &>/dev/null; then usermod -aG "$GROUP" "$u"; echo "  + $u"; else echo "  (нет пользователя $u, пропускаю)"; fi
done

echo "== 2. Каталоги и права =="
mkdir -p "$BARE" "$SRC" "$ROOT/bin"
chgrp -R "$GROUP" "$BARE" "$SRC"
chmod -R 2775 "$BARE" "$SRC"           # setgid: новые файлы наследуют группу

# DeleteDag ищет логи как <папка дагов>/../logs. После переключения это
# $SRC/logs, а реальные логи airflow лежат в $PROD_HOME/logs — связываем.
# ('logs' в .gitignore, поэтому checkout симлинк не тронет.)
if [[ ! -e $SRC/logs ]]; then
    ln -s "$PROD_HOME/logs" "$SRC/logs"
    echo "  $SRC/logs -> $PROD_HOME/logs"
fi

echo "== 3. Wrapper для проверки DAG'ов =="
cat > "$ROOT/bin/check_dags.sh" <<WRAPPER
#!/bin/bash
# Проверка, что даги из нового кода парсятся с окружением прода.
# \`airflow dags list\` возвращает 0 даже при ошибках импорта, поэтому
# дополнительно смотрим list-import-errors: строка с *.py = сломанный даг.
# Запускается от root (см. sudoers), сам airflow — под $RUNAS, как в проде.
AIRFLOW="sudo -u $RUNAS env \$(grep -v '^#' $ENVFILE | xargs) $VENV/bin/airflow"
\$AIRFLOW dags list >/dev/null || exit 1
ERRORS=\$(\$AIRFLOW dags list-import-errors 2>&1 || true)
if printf '%s\n' "\$ERRORS" | grep -qE '\.py'; then
    echo "Ошибки импорта DAG'ов:"
    printf '%s\n' "\$ERRORS"
    exit 1
fi
exit 0
WRAPPER
chmod 750 "$ROOT/bin/check_dags.sh"
chown root:"$GROUP" "$ROOT/bin/check_dags.sh"
echo "  ok"

echo "== 4. bare-репозиторий =="
[[ -e $BARE/HEAD ]] || git init --bare "$BARE" >/dev/null
git --git-dir="$BARE" config core.sharedRepository group
chgrp -R "$GROUP" "$BARE"; chmod -R 2775 "$BARE"

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

    echo "== Проверка валидности DAG'ов =="
    if ! PARSE_LOG=\$(sudo $ROOT/bin/check_dags.sh 2>&1); then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "!! КРИТИЧЕСКАЯ ОШИБКА: новые DAG'и не парсятся!          !!"
        echo "\$PARSE_LOG"
        echo "!! Сервисы НЕ перезапущены — прод работает как работал.  !!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        continue
    fi
    echo "  OK: все DAG'и распарсены."

    # Рестарт только если прод УЖЕ переключён на новый код. До переключения
    # (нет drop-in) код просто раскладывается — работающий прод не трогаем.
    if [ -f "/etc/systemd/system/${UNITS[0]}.d/$DROPIN_NAME" ]; then
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
PYTHONPATH=$SRC
ETL_FULL_PATH=$SRC/
ETL_MODE=
AIRFLOW__CORE__DAGS_FOLDER=$SRC/dags
# Новые даги появляются на ПАУЗЕ — после переключения линии включаются по одной.
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ENV
chown "$RUNAS":"$GROUP" "$ENVFILE"; chmod 640 "$ENVFILE"
echo "  ok"

echo "== 7. sudoers: hook рестартит прод и проверяет даги без пароля =="
SUDO=/etc/sudoers.d/airflow-prod
SCTL=$(command -v systemctl)
cat > "$SUDO.tmp" <<SUDOERS
%$GROUP ALL=(root) NOPASSWD: $SCTL restart ${UNITS[*]}
%$GROUP ALL=(root) NOPASSWD: $ROOT/bin/check_dags.sh
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
if sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" "$VENV/bin/airflow" \
     pools get "$ETL_POOL_NAME" >/dev/null 2>&1; then
    echo "  пул уже есть — проверь размер (должно быть >= $ETL_POOL_SLOTS):"
    sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" "$VENV/bin/airflow" \
        pools get "$ETL_POOL_NAME" | sed 's/^/    /'
else
    sudo -u "$RUNAS" env AIRFLOW_HOME="$PROD_HOME" "$VENV/bin/airflow" \
        pools set "$ETL_POOL_NAME" "$ETL_POOL_SLOTS" "ETL: перенос данных" || \
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
