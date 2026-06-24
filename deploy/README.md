# Серверный тестовый airflow + деплой по git (Phase 3)

Отдельный тестовый airflow рядом с продом, код в него приезжает через git
(push в bare-репо → `post-receive` раскладывает ветку `test` и перезапускает сервис).
Прод (`/opt/airflow`, база `airflow`, юниты `airflow-scheduler/webserver`) не трогается.

```
        gitea (ETL.git, UI)            ← канон, ветки test/dev (prod позже)
            ▲   │
   dev-PC ──┘   │  push server test / bridge
            │   ▼
   СЕРВЕР: /opt/airflow-test/etl.git (bare) ──post-receive──► test-src ──► airflow-test (:8081)
                    ▲
         jupyter (коллеги): commit+push в этот же bare-репо
```

Раскладка на сервере:
| Путь | Что |
|---|---|
| `/opt/airflow-test/etl.git` | bare-репо (git-хаб), группа `etldev` |
| `/opt/airflow-test/test-src` | рабочая копия ветки `test` (код, который исполняет airflow) |
| `/opt/airflow-test/home` | `AIRFLOW_HOME` тестового (cfg/логи), владелец `airflow` |
| `/opt/airflow-test/airflow-test.env` | systemd EnvironmentFile (структурные переменные) |
| база `airflow_test` | metadata тестового в том же PG11 |

## Установка (на сервере, от root)

```bash
ssh devel@airflow
sudo -s
# перенеси сюда deploy/setup-airflow-test.sh и запусти:
bash setup-airflow-test.sh
```

Скрипт идемпотентный, прод не трогает. После него проверь:
```bash
systemctl status airflow-test-scheduler airflow-test-webserver --no-pager
cat /opt/airflow-test/airflow-test.env        # особенно AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
```
Если в conn-строке оказался `ВПИШИ_ПАРОЛЬ` — впиши пароль PG-пользователя `airflow`
(тот же, что у прода) и `systemctl restart airflow-test-*`.

## После установки

**1. Реквизиты тестовых БД** (читает код, не уходит в git):
```bash
cp deploy/test.env.example /opt/airflow-test/test-src/.env   # после первого push, см. ниже
# впиши значения ТЕСТОВЫХ Oracle/Postgres
chgrp etldev /opt/airflow-test/test-src/.env && chmod 640 /opt/airflow-test/test-src/.env
```

**2. Первый деплой кода — push ветки `test` с dev-PC** в серверный bare-репо:
```bash
cd ~/konkin/etl
git remote add server ssh://devel@airflow/opt/airflow-test/etl.git
git push server test
```
`post-receive` разложит код в `test-src` и перезапустит `airflow-test`.
(`.env` из шага 1 при checkout не затирается — он untracked.)

**3. Пользователь для UI теста** (своя metadata-база):
```bash
sudo -u airflow env $(grep -v '^#' /opt/airflow-test/airflow-test.env | xargs) \
  /opt/airflow/venv/bin/airflow users create --username admin --password admin \
  --firstname a --lastname a --role Admin --email a@a.a
```
UI: `http://<IP-сервера>:8081`.

## Повседневный поток (кнопки добавим следующим шагом)
- **Коллеги (Jupyter):** правят код в своём клоне → `deploy-test.sh` = commit + push в bare-репо → авто-деплой в тестовый airflow.
- **Ты (dev-PC):** `push-to-server.sh` = выкатить ветку `test` в тестовый airflow; `bridge.sh` = синхронизировать серверный bare-репо ↔ gitea.

## Прод (позже)
Тот же механизм для ветки `prod` и существующего `/opt/airflow/airflow` добавим
на финальном шаге, когда новая версия будет проверена.
