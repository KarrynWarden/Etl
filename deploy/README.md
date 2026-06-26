# Серверный тестовый airflow + деплой по git (Phase 3)

Отдельный тестовый airflow рядом с продом, код в него приезжает через git
(push в bare-репо → `post-receive` раскладывает ветку `test` и перезапускает сервис).
Прод (`/opt/airflow`, база `airflow`, юниты `airflow-scheduler/webserver`) не трогается.

```
        gitea (ETL.git, UI)            ← канон, ветки test/dev (prod позже)
            ▲   │
   dev-PC ──┘   │  push server test / bridge
            │   ▼
   СЕРВЕР: /opt/airflow-test/etl.git (bare) ──post-receive──► test-src ──► airflow-test (:8082)
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
UI: `http://<IP-сервера>:8082`.

## Повседневный поток (кнопки добавим следующим шагом)
- **Коллеги (Jupyter):** правят код в своём клоне → `deploy-test.sh` = commit + push в bare-репо → авто-деплой в тестовый airflow.
- **Ты (dev-PC):** `push-to-server.sh` = выкатить ветку `test` в тестовый airflow; `bridge.sh` = синхронизировать серверный bare-репо ↔ gitea.

## Доступ тестировщиков через Apache (как у прода)

Прод проксируется Apache2 на этом же сервере (`airflow.oms66.ru` → `localhost:8083`).
Для теста добавляем такой же vhost (`airflow-test.oms66.ru` → `localhost:8082`).

1. **vhost:** скопировать `deploy/airflow-test-apache.conf` →
   `/etc/apache2/sites-available/airflow-test.conf`, затем:
   ```bash
   a2ensite airflow-test
   apache2ctl configtest        # должно быть Syntax OK
   systemctl reload apache2     # reload, не restart — прод-сессии не рвутся
   ```
2. **airflow за прокси:** в `/opt/airflow-test/airflow-test.env` должны быть
   (скрипт их уже пишет):
   ```
   AIRFLOW__WEBSERVER__BASE_URL=https://airflow-test.oms66.ru
   AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
   ```
   после правки — `systemctl restart airflow-test-webserver`.
3. **DNS:** `airflow-test.oms66.ru` должен резолвиться (wildcard `*.oms66.ru` или
   отдельная запись — к администраторам DNS).
4. **Сертификат:** vhost переиспользует `oms66.crt`. Проверь, что он покрывает
   поддомен:
   ```bash
   openssl x509 -in /etc/ssl/certs/oms66.crt -noout -text | grep -A1 'Subject Alternative Name'
   ```
   Если в списке есть `*.oms66.ru` — ок; иначе нужен сертификат на новый поддомен.

После этого тестировщики заходят на `https://airflow-test.oms66.ru` — без ssh и туннелей.

## Авто-синхронизация Jupyter-клона

Общий Jupyter-workdir делаем клоном bare-репо, и `post-receive` после каждого
`git push server test` сам подтягивает в него последнюю версию (`git pull --ff-only`:
если кто-то редактирует с несохранёнными правками — не затрёт, просто пропустит).

Разовая настройка (на сервере, root; bare-репо уже должен иметь ветку `test`):
```bash
git clone /opt/airflow-test/etl.git /opt/jupyter/workdir/konkin/etl
git -C /opt/jupyter/workdir/konkin/etl checkout test
chown -R jupyter:etldev /opt/jupyter/workdir/konkin/etl
chmod -R g+rwX /opt/jupyter/workdir/konkin/etl
find /opt/jupyter/workdir/konkin/etl -type d -exec chmod g+s {} +   # новые файлы наследуют группу
```
Затем обнови hook (он уже умеет авто-pull): перенеси свежий `setup-airflow-test.sh`
на сервер и `bash setup-airflow-test.sh` (идемпотентно — перезапишет hook), либо
вручную допиши блок авто-pull в `/opt/airflow-test/etl.git/hooks/post-receive`.

Путь Jupyter-клона задаётся переменной `JUPYTER_CLONE` в `setup-airflow-test.sh`.
Если клона нет — hook просто пропускает этот шаг.

## Обслуживание metadata-БД

Чистка БД airflow остаётся airflow-дагами (`DbCleanup1/2`) — удобно смотреть логи и
результат в UI. Изредка падает на `VACUUM FULL` (эксклюзивный лок vs heartbeat),
но это приемлемо ради контроля из интерфейса.

## Прод (позже)
Тот же механизм для ветки `prod` и существующего `/opt/airflow/airflow` добавим
на финальном шаге, когда новая версия будет проверена.
