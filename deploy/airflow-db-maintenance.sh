#!/usr/bin/env bash
#
# Обслуживание metadata-БД airflow: удалить старые записи + VACUUM FULL
# (реальное освобождение места на диске) БЕЗ падений.
#
# Почему так: VACUUM FULL берёт ACCESS EXCLUSIVE lock и переписывает таблицу.
# Под ЖИВЫМ airflow это блокирует heartbeat (он пишет в те же task_instance/
# dag_run) -> «Heartbeat time limit exceeded» -> SIGKILL (твои падения 2-3 р/мес).
# Здесь airflow на время операции ОСТАНОВЛЕН, поэтому лок никому не мешает,
# место реально возвращается ОС, и падать нечему.
#
# Запускать от root по таймеру (airflow-db-maintenance.timer). Параметры — env:
#   SERVICES        systemd-юниты airflow для stop/start
#                   прод:  "airflow-scheduler airflow-webserver"
#                   тест:  "airflow-test-scheduler airflow-test-webserver"
#   AIRFLOW_BIN     путь к airflow (по умолчанию /opt/airflow/venv/bin/airflow)
#   RUNAS           пользователь airflow (по умолчанию airflow)
#   AIRFLOW_HOME    AIRFLOW_HOME прод-инстанса (по умолчанию /opt/airflow/airflow)
#   ENVFILE         для теста: /opt/airflow-test/airflow-test.env (вместо AIRFLOW_HOME)
#   PG_DB           имя metadata-БД (прод: airflow; тест: airflow_test)
#   RETENTION_DAYS  сколько хранить (по умолчанию 90)
#
set -uo pipefail

SERVICES="${SERVICES:-airflow-scheduler airflow-webserver}"
AIRFLOW_BIN="${AIRFLOW_BIN:-/opt/airflow/venv/bin/airflow}"
RUNAS="${RUNAS:-airflow}"
AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-/opt/airflow/airflow}"
ENVFILE="${ENVFILE:-}"
PG_DB="${PG_DB:-airflow}"
RETENTION_DAYS="${RETENTION_DAYS:-90}"

log(){ echo "[db-maint $(date '+%F %T')] $*"; }

# Что бы ни случилось дальше — airflow обязан подняться обратно.
restart_airflow(){ log "старт airflow: $SERVICES"; systemctl start $SERVICES || true; }
trap restart_airflow EXIT

log "стоп airflow: $SERVICES"
systemctl stop $SERVICES
sleep 5   # дать процессам закрыть соединения к БД

CUTOFF=$(date -u -d "$RETENTION_DAYS days ago" '+%Y-%m-%d %H:%M:%S+00:00')
log "airflow db clean: удаляю записи старше $CUTOFF"
if [[ -n "$ENVFILE" ]]; then
    sudo -u "$RUNAS" env $(grep -v '^#' "$ENVFILE" | xargs) "$AIRFLOW_BIN" \
        db clean --clean-before-timestamp "$CUTOFF" --skip-archive --yes \
        || log "db clean завершился с ошибкой — продолжаю к VACUUM"
else
    sudo -u "$RUNAS" env AIRFLOW_HOME="$AIRFLOW_HOME_DIR" "$AIRFLOW_BIN" \
        db clean --clean-before-timestamp "$CUTOFF" --skip-archive --yes \
        || log "db clean завершился с ошибкой — продолжаю к VACUUM"
fi

log "VACUUM (FULL, ANALYZE) базы $PG_DB (освобождение места)"
sudo -u postgres psql -d "$PG_DB" -c "VACUUM (FULL, ANALYZE);" \
    || log "VACUUM завершился с ошибкой"

log "обслуживание завершено (airflow поднимется в trap EXIT)"
