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
mkdir -p "$BARE" "$SRC" "$AHOME"
chgrp -R "$GROUP" "$BARE" "$SRC"
chmod -R 2775 "$BARE" "$SRC"           # setgid (2): новые файлы наследуют группу $GROUP
chown -R "$RUNAS":"$RUNAS" "$AHOME"

echo "== 2b. Wrapper для проверки DAG'ов =="
mkdir -p "$ROOT/bin"
cat > "$ROOT/bin/check_dags.sh" <<'WRAPPER'
#!/bin/bash
# Проверка валидности DAG'ов: поднимаем окружение и убеждаемся, что ни один DAG
# не падает при импорте.
# ВАЖНО: сам по себе `airflow dags list` возвращает 0 даже при ошибках импорта
# (он просто перечисляет то, что распарсилось), поэтому дополнительно проверяем
# `dags list-import-errors` — строки с путём *.py означают сломанный DAG.
set -a
source /opt/airflow-test/airflow-test.env
set +a
AIRFLOW=/opt/airflow/venv/bin/airflow
# 1) базовая проверка, что CLI и metadata поднимаются
"$AIRFLOW" dags list >/dev/null || exit 1
# 2) ошибки импорта DAG'ов -> падаем с ненулевым кодом
ERRORS=$("$AIRFLOW" dags list-import-errors 2>&1 || true)
if printf '%s\n' "$ERRORS" | grep -qE '\.py'; then
    echo "Ошибки импорта DAG'ов:"
    printf '%s\n' "$ERRORS"
    exit 1
fi
exit 0
WRAPPER
chmod 750 "$ROOT/bin/check_dags.sh"
chown root:"$GROUP" "$ROOT/bin/check_dags.sh"
echo "  ok"

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
        echo "== Проверка валидности DAG'ов =="
        if PARSE_LOG=\$(sudo -u "$RUNAS" "$ROOT/bin/check_dags.sh" 2>&1); then
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
            echo "!! Сервисы НЕ перезапущены, чтобы не ломать рабочую версию. !!"
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

echo "== 9. sudoers: hook может рестартить airflow-test и проверять даги без пароля =="
SUDO=/etc/sudoers.d/airflow-test
SCTL=$(command -v systemctl)

cat > "$SUDO.tmp" <<SUDOERS
%$GROUP ALL=(root) NOPASSWD: $SCTL restart airflow-test-scheduler airflow-test-webserver
%$GROUP ALL=($RUNAS) NOPASSWD: $ROOT/bin/check_dags.sh
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
