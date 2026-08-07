#!/usr/bin/env bash
#
# Поднимает ОТДЕЛЬНЫЙ тестовый airflow рядом с продом, с деплоем по git.
# Запускать НА СЕРВЕРЕ от root: ssh devel@airflow → sudo -s → bash setup-airflow-test.sh
#
# Прод НЕ трогается: создаются только новые сущности —
#   /opt/airflow-test/, база airflow_test, юниты airflow-test-scheduler/webserver.
#
# Идемпотентно: повторный запуск не ломает уже созданное.
#
set -euo pipefail

### ─────────── КОНФИГ (правь при необходимости) ───────────
ROOT=/opt/airflow-test
GROUP=etldev
MEMBERS=(devel jupyter airflow)        # кто может пушить/деплоить + сам airflow (читает код)
VENV=/opt/airflow/venv                 # общий с продом venv
RUNAS=airflow                          # под кем крутится тестовый airflow (как прод)
PORT=8082                              # UI теста (прод — 8080, 8081 занят JupyterLab)
DEPLOY_BRANCH=test                     # какая ветка разворачивается в test-src
PROD_CFG=/opt/airflow/airflow/airflow.cfg   # откуда взять реквизиты metadata-PG
TEST_DB=airflow_test                   # отдельная metadata-база в том же PG
TEST_DOMAIN=airflow-test.oms66.ru      # домен за Apache-прокси (для base_url)
JUPYTER_CLONE=/opt/jupyter/workdir/konkin/etl   # общий Jupyter-клон (авто-pull при деплое; если нет — пропускается)

BARE=$ROOT/etl.git
SRC=$ROOT/test-src
AHOME=$ROOT/home
ENVFILE=$ROOT/airflow-test.env         # systemd EnvironmentFile (структурные airflow-переменные)
### ────────────────────────────────────────────────────────

[[ $EUID -eq 0 ]] || { echo "Запускай от root (sudo -s)"; exit 1; }
[[ -x $VENV/bin/airflow ]] || { echo "Нет $VENV/bin/airflow"; exit 1; }
cd /    # чтобы sudo -u postgres не ругался на чужой cwd

echo "== 1. Группа $GROUP и участники =="
groupadd -f "$GROUP"
for u in "${MEMBERS[@]}"; do
    if id "$u" &>/dev/null; then usermod -aG "$GROUP" "$u"; echo "  + $u"; else echo "  (нет пользователя $u, пропускаю)"; fi
done

echo "== 2. Каталоги и права =="
mkdir -p "$BARE" "$SRC" "$AHOME/logs"
chgrp -R "$GROUP" "$BARE" "$SRC"
chmod -R 2775 "$BARE" "$SRC"           # setgid (2): новые файлы наследуют группу $GROUP
# logs/ РЕКУРСИЕЙ НЕ ТРОГАЕМ. AIRFLOW_HOME теста — он же каталог логов
# (base_log_folder по умолчанию = $AIRFLOW_HOME/logs), даги ходят раз в минуту,
# и там копятся сотни тысяч файлов. Прежний `chown -R "$AHOME"` на повторном
# запуске висел минутами, переназначая владельца тому, кто им и так владеет:
# логи пишет сам airflow от своего пользователя. Всё остальное в AIRFLOW_HOME —
# единицы файлов (airflow.cfg, webserver_config.py), их и обходим.
chown "$RUNAS":"$RUNAS" "$AHOME" "$AHOME/logs"
find "$AHOME" -mindepth 1 -maxdepth 1 ! -name logs \
     -exec chown -R "$RUNAS":"$RUNAS" {} +
echo "  ok (logs/ пропущены намеренно — рекурсия по ним занимает минуты)"

echo "== 2b. Проверка DAG'ов =="
# Своей копии проверки на сервере больше НЕТ. Раньше здесь генерился
# $ROOT/bin/check_dags.sh, и точно такой же — в setup-airflow-prod.sh; две копии
# успели разъехаться (разный способ подхватить окружение), из-за чего 2026-08-07
# тест сказал «OK» на коде, который прод не смог распарсить. Теперь обе стороны
# зовут ОДИН скрипт из репозитория — deploy/check-dags.sh, — и разойтись им негде.
# Окружение он строит себе сам (временный AIRFLOW_HOME, sqlite-заглушка вместо
# metadata-базы) и каталог дагов передаёт в DagBag явно.
rm -f "$ROOT/bin/check_dags.sh"
echo "  проверка живёт в репозитории: deploy/check-dags.sh"

echo "== 3. bare-репозиторий =="
if [[ ! -e "$BARE/HEAD" ]]; then
    git init --bare "$BARE" >/dev/null
fi
# Общий репо: в него пушат и devel (ssh с dev-PC), и jupyter (локально из клона),
# checkout делает hook. core.sharedRepository=group заставляет git создавать
# объекты/ссылки груп-записываемыми, иначе push второго пользователя падает с
# "unable to create temporary object directory".
git --git-dir="$BARE" config core.sharedRepository group
chgrp -R "$GROUP" "$BARE"; chmod -R 2775 "$BARE"

echo "== 3b. pre-receive hook (гейт ДО обновления ссылок) =="
# Единственный гейт, который реально защищает. post-receive проверяет код,
# который УЖЕ лежит в $SRC/dags — то есть в каталоге, который читает scheduler:
# отказ от рестарта ничего не спасает, даги всё равно перечитаются и покраснеют.
# И его exit code git игнорирует, поэтому dev-push.sh рапортовал «Готово» даже
# когда проверка падала. pre-receive работает по временной распаковке
# присланного коммита: провал -> push отклонён, ссылки не двинулись, рабочее
# дерево не тронуто, а ненулевой код виден клиенту.
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
    # Объекты присланного коммита лежат в карантине; git видит их по
    # GIT_OBJECT_DIRECTORY, который выставлен в окружении хука — поэтому
    # распаковка работает ДО того, как ссылки обновлены.
    git --git-dir=$BARE archive "\$newrev" | tar -x -C "\$TMP"
    # Полная проверка = ОБА файла. Если чего-то нет — это откат на старый тег
    # либо неполный перенос правок. Отклонять такое наглухо нельзя: откат обязан
    # работать всегда, тем более в аварии. Но и пропускать вслепую не будем —
    # гоняем встроенный минимум, компиляцию python.
    if [ ! -f "\$TMP/deploy/check-dags.sh" ] || [ ! -f "\$TMP/tools/check_dags.py" ]; then
        echo "!! Полной проверки в дереве нет:"
        [ -f "\$TMP/deploy/check-dags.sh" ] || echo "     нет deploy/check-dags.sh"
        [ -f "\$TMP/tools/check_dags.py" ] || echo "     нет tools/check_dags.py"
        echo "   Откат на старую версию — это нормально. Если нет, значит правки"
        echo "   перенесены не полностью: доложи недостающий файл и запушь снова."
        echo "   Сейчас проверяю только синтаксис."
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
        echo "!! Push ОТКЛОНЁН: код не прошёл проверку.                !!"
        echo "!! На сервер ничего не легло, тест работает как работал. !!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        rc=1
    fi
    rm -rf "\$TMP"
done
exit \$rc
HOOK
chmod 2775 "$BARE/hooks/pre-receive"; chgrp "$GROUP" "$BARE/hooks/pre-receive"
echo "  ok"

echo "== 4. post-receive hook (деплой + рестарт) =="
cat > "$BARE/hooks/post-receive" <<HOOK
#!/bin/bash
set -e
umask 002

while read oldrev newrev ref; do
    branch=\${ref#refs/heads/}
    if [ "\$branch" = "$DEPLOY_BRANCH" ]; then
        git --git-dir=$BARE --work-tree=$SRC checkout -f "$DEPLOY_BRANCH"
        python3 "$SRC/tools/regen_config.py" config SpTableName SpOnce >/dev/null 2>&1 || true

        # === ПРОВЕРКА ВАЛИДНОСТИ DAG'ОВ ===
        # Страховка, а не гейт: настоящий отказ делает pre-receive. Здесь
        # проверяется уже разложенное дерево — тем же скриптом из репозитория,
        # без sudo (окружение он строит себе сам, чужое ему не нужно).
        echo "== Проверка валидности DAG'ов =="
        if PARSE_LOG=\$(REQUIRE_AIRFLOW=1 bash "$SRC/deploy/check-dags.sh" "$SRC" 2>&1); then
            echo "  OK: Все DAG'и успешно распарсены."

            # Если всё хорошо, рестартуем сервисы и обновляем Jupyter
            sudo systemctl restart airflow-test-scheduler airflow-test-webserver

            if [ -d "$JUPYTER_CLONE/.git" ]; then
                ( unset GIT_DIR GIT_WORK_TREE
                # Ноутбук-лаунчер Jupyter автосохраняет (execution_count/выводы/виджеты) —
                # это шум, «пачкает» клон и блокирует ff-merge. skip-worktree = git
                # перестаёт следить за файлом (не проверяет и не перезаписывает).
                git -C "$JUPYTER_CLONE" update-index --skip-worktree tools/new_dag.ipynb 2>/dev/null || true
                git -C "$JUPYTER_CLONE" fetch -q origin "$DEPLOY_BRANCH" \
                && git -C "$JUPYTER_CLONE" merge -q --ff-only "origin/$DEPLOY_BRANCH" ) \
                && echo "Jupyter-клон обновлён до $DEPLOY_BRANCH" \
                || echo "Jupyter-клон не обновлён (несохранённые правки/дивергенция/права) — пропуск"
            else
                echo "Jupyter-клон: $JUPYTER_CLONE/.git не найден или недоступен под \$(id -un) — пропуск"
            fi
            echo "deploy: ветка $DEPLOY_BRANCH -> $SRC, airflow-test перезапущен"
        else
            # Если парсинг упал, выводим ошибку и НЕ рестартуем сервисы!
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            echo "!! КРИТИЧЕСКАЯ ОШИБКА: Новые DAG'и не парсятся!       !!"
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            echo "\$PARSE_LOG"
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            echo "!! Сервисы НЕ перезапущены — но код УЖЕ в $SRC/dags,   !!"
            echo "!! а это каталог, который scheduler перечитывает сам.    !!"
            echo "!! Чинить и пушить заново НЕМЕДЛЕННО, либо откатить:     !!"
            echo "!!   git --git-dir=$BARE --work-tree=$SRC checkout -f <прошлый commit> !!"
            echo "!! Сюда доходят только те, кто обошёл pre-receive.       !!"
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        fi
        # ==================================
    else
        echo "ветка \$branch получена (без деплоя)"
    fi
done
HOOK
chmod 2775 "$BARE/hooks/post-receive"; chgrp "$GROUP" "$BARE/hooks/post-receive"

echo "== 5. metadata-база $TEST_DB =="
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='$TEST_DB'" | grep -q 1; then
    sudo -u postgres psql -c "CREATE DATABASE $TEST_DB OWNER $RUNAS;"
    echo "  создана"
else
    echo "  уже существует"
fi

echo "== 5b. pg_hba: доступ $RUNAS -> $TEST_DB =="
HBA=$(sudo -u postgres psql -tAc "SHOW hba_file;" | tr -d '[:space:]')
if [[ -n "$HBA" && -f "$HBA" ]]; then
    if grep -qE "^[[:space:]]*host[[:space:]]+$TEST_DB[[:space:]]+$RUNAS[[:space:]]" "$HBA"; then
        echo "  запись уже есть"
    else
        # берём метод из существующей строки для базы airflow и дублируем на $TEST_DB
        base_line=$(grep -E "^[[:space:]]*host[[:space:]]+airflow[[:space:]]+$RUNAS[[:space:]]" "$HBA" | head -1 || true)
        if [[ -n "$base_line" ]]; then
            new_line=$(echo "$base_line" | sed -E "s/(^[[:space:]]*host[[:space:]]+)airflow([[:space:]]+)/\1$TEST_DB\2/")
        else
            new_line="host    $TEST_DB    $RUNAS    127.0.0.1/32    md5"
        fi
        cp -a "$HBA" "$HBA.bak.$$"
        printf '%s\n' "$new_line" >> "$HBA"
        sudo -u postgres psql -c "SELECT pg_reload_conf();" >/dev/null
        echo "  добавлено: $new_line  (бэкап: $HBA.bak.$$)"
    fi
else
    echo "  !! не нашёл pg_hba.conf — добавь правило для $TEST_DB вручную"
fi

# conn для metadata: берём прод-строку и меняем только имя БД на $TEST_DB
PROD_CONN=$(awk -F'=' '/^[[:space:]]*sql_alchemy_conn/{sub(/^[^=]*=[[:space:]]*/,"");print;exit}' "$PROD_CFG" 2>/dev/null || true)
if [[ -z "${PROD_CONN:-}" ]]; then
    echo "!! Не нашёл sql_alchemy_conn в $PROD_CFG."
    echo "!! Впиши строку подключения вручную в $ENVFILE (AIRFLOW__DATABASE__SQL_ALCHEMY_CONN) после запуска."
    TEST_CONN="postgresql+psycopg2://$RUNAS:ВПИШИ_ПАРОЛЬ@127.0.0.1:5432/$TEST_DB"
else
    TEST_CONN=$(echo "$PROD_CONN" | sed -E "s#/[A-Za-z0-9_]+([?]|$)#/$TEST_DB\1#")
fi

echo "== 6. EnvironmentFile $ENVFILE =="
cat > "$ENVFILE" <<ENV
AIRFLOW_HOME=$AHOME
PYTHONPATH=$SRC
ETL_FULL_PATH=$SRC/
ETL_MODE=
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=$SRC/dags
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$TEST_CONN
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT
AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
AIRFLOW__WEBSERVER__BASE_URL=https://$TEST_DOMAIN
AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
ENV
chown "$RUNAS":"$GROUP" "$ENVFILE"; chmod 640 "$ENVFILE"

echo "== 7. Миграция metadata $TEST_DB =="
sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$VENV/bin/airflow" db migrate

echo "== 8. systemd-юниты airflow-test-* =="
cat > /etc/systemd/system/airflow-test-scheduler.service <<UNIT
[Unit]
Description=Airflow TEST scheduler
After=network.target postgresql.service
[Service]
User=$RUNAS
Group=$GROUP
EnvironmentFile=$ENVFILE
ExecStart=$VENV/bin/airflow scheduler
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT
cat > /etc/systemd/system/airflow-test-webserver.service <<UNIT
[Unit]
Description=Airflow TEST webserver
After=network.target postgresql.service
[Service]
User=$RUNAS
Group=$GROUP
EnvironmentFile=$ENVFILE
ExecStart=$VENV/bin/airflow webserver
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
UNIT

echo "== 9. sudoers: hook может рестартить airflow-test без пароля =="
# Строки под check_dags.sh больше нет — см. пояснение в setup-airflow-prod.sh.
SUDO=/etc/sudoers.d/airflow-test
SCTL=$(command -v systemctl)

cat > "$SUDO.tmp" <<SUDOERS
%$GROUP ALL=(root) NOPASSWD: $SCTL restart airflow-test-scheduler airflow-test-webserver
SUDOERS

if visudo -cf "$SUDO.tmp"; then
    mv "$SUDO.tmp" "$SUDO"; chmod 440 "$SUDO"; echo "  ok"
else
    echo "  !! sudoers невалиден, не ставлю"; rm -f "$SUDO.tmp"
fi

echo "== 9b. git safe.directory (репо разных владельцев: hook/devel/root) =="
for d in "$BARE" "$SRC" "$JUPYTER_CLONE"; do
    git config --system --get-all safe.directory 2>/dev/null | grep -qxF "$d" \
        || git config --system --add safe.directory "$d"
done
echo "  ok"

echo "== 9c. права Jupyter-клона (общий доступ группе $GROUP) =="
# Клон правят и jupyter (редактор), и devel (force-sync из dev-push при сбросе
# несохранённых правок). Чтобы любой из группы мог удалять/заменять файлы коллег,
# каталоги должны быть груп-записываемыми с setgid (для unlink важна запись на
# КАТАЛОГ, а не на файл). Без этого reset --hard падает с «unable to unlink».
if [[ -d "$JUPYTER_CLONE/.git" ]]; then
    chgrp -R "$GROUP" "$JUPYTER_CLONE" 2>/dev/null || true
    find "$JUPYTER_CLONE" -type d -exec chmod 2775 {} + 2>/dev/null || true
    find "$JUPYTER_CLONE" -type f -exec chmod g+rw {} + 2>/dev/null || true
    echo "  ok"
else
    echo "  ($JUPYTER_CLONE не найден — пропуск)"
fi

echo "== 10. Запуск =="
systemctl daemon-reload
systemctl enable --now airflow-test-scheduler airflow-test-webserver

echo
echo "Готово. Прод не тронут."
echo "  bare-репо : $BARE"
echo "  код       : $SRC   (заполнится после первого push ветки '$DEPLOY_BRANCH')"
echo "  UI теста  : http://<IP-сервера>:$PORT"
echo
echo "Дальше — см. deploy/README.md (раздел «После установки»)."
