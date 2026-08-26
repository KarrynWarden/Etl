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

## Повседневный поток

Скрипты идемпотентны и при любой ошибке (конфликт, не та ветка, несохранённые
правки, битый конфиг) останавливаются с понятным сообщением, ничего не выкатывая
наполовину.

**Коллеги (на Jupyter)** — добавить новый даг или поправить существующий:
```bash
cd /opt/jupyter/workdir/konkin/etl
# ... правки дага/конфига/структур ...
bash deploy/deploy-test.sh "что сделал"
```
`deploy-test.sh` = commit → rebase на bare (чтобы не разойтись с чужими дагами) →
проверка сборки конфига → push в bare-репо → авто-деплой и рестарт airflow-test.
`origin` у клона уже указывает на серверный bare-репо.

**Ты (dev-PC):**
```bash
bash local/dev-pull.sh    # подтянуть изменения коллег с сервера (твоё — поверх, rebase)
bash local/dev-push.sh    # разослать ветку test в gitea (канон) и на сервер (деплой)
```
`dev-pull.sh` забирает коммиты коллег из remote `server` без затирания твоей работы;
`dev-push.sh` синхронизируется с сервером и пушит в `origin` (gitea) и `server`.
Переопределяемые переменные: `DEPLOY_BRANCH` (по умолч. `test`), `SERVER_REMOTE`
(`server`), `GITEA_REMOTE` (`origin`).

> **Важно про порядок.** Всегда заходи через эти скрипты: если запушить свои
> коммиты мимо `dev-pull.sh`, история разойдётся с сервером и push отлетит по
> non-fast-forward (не потеря данных, но придётся синхронизироваться вручную).

### Реквизиты БД на Jupyter (для будущего генератора структур)

Генератор дага (следующий шаг) снимает структуры таблиц прямо из БД через
`Connect/__init__.py`, который читает реквизиты из `.env` в корне клона
(`.env` в `.gitignore` — в репозиторий не уходит). Создай его **редактором**
(не командой с паролем — чтобы пароль не попал в историю shell):
```bash
cp .env.example .env        # затем открой .env в редакторе и впиши ТЕСТОВЫЕ реквизиты
chmod 600 .env
```
Проверка подключения **без вывода пароля** — см. «После установки» / команды,
которые даёт ассистент: тест печатает только `OK`/`FAIL`, реквизиты не показывает.

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
# общий репо: в .git пишут И jupyter (коммиты коллег), И devel (fetch в хуке),
# поэтому включаем групповой режим — git создаёт служебные файлы груп-записываемыми
git -C /opt/jupyter/workdir/konkin/etl config core.sharedRepository group
chown -R jupyter:etldev /opt/jupyter/workdir/konkin/etl
chmod -R g+rwX /opt/jupyter/workdir/konkin/etl
find /opt/jupyter/workdir/konkin/etl -type d -exec chmod g+s {} +   # новые файлы наследуют группу
```
Затем обнови hook (он уже умеет авто-pull): перенеси свежий `setup-airflow-test.sh`
на сервер и `bash setup-airflow-test.sh` (идемпотентно — перезапишет hook), либо
вручную допиши блок авто-pull в `/opt/airflow-test/etl.git/hooks/post-receive`.

Путь Jupyter-клона задаётся переменной `JUPYTER_CLONE` в `setup-airflow-test.sh`.
Если клона нет — hook просто пропускает этот шаг (теперь с явным сообщением в выводе push).

**Важно — права прохода до клона.** Hook выполняется под пользователем, под которым
приходит push (`devel`), а не под root. Чтобы он мог дойти до клона, у `devel` должен
быть бит прохода (`x`) на ВСЕХ родительских каталогах. Частая засада: `/opt/jupyter`
создан с правами `700` (`drwx------ jupyter jupyter`) — тогда `devel` не проходит внутрь,
`[ -d "$JUPYTER_CLONE/.git" ]` молча даёт false и авто-pull пропускается без обновления.
Проверка и починка (на сервере, root):
```bash
namei -l /opt/jupyter/workdir/konkin/etl/.git          # найди каталог без группового/world x
sudo -u devel test -d /opt/jupyter/workdir/konkin/etl/.git && echo OK || echo NOACCESS
# открыть только проход (x) для группы etldev, без чтения/записи:
chgrp etldev /opt/jupyter && chmod g+x /opt/jupyter     # подставь реальный «закрытый» каталог
```

## Перезапуск сервисов после push'а

Перезапуск бывает **двух видов**, и по выводу push'а их различить важно.

| в выводе хука | кто перезапускает | когда |
|---|---|---|
| `перезапущено: …` | сам хук через `sudo` | сразу, синхронно |
| `заявка на перезапуск подана …` | `etl-deploy-restart.service` | через несколько секунд, уже вне push'а |

Второй случай — это push **из конструктора**. Он идёт внутри systemd-сервиса
`etl-dagbuilder-api`, а у того `NoNewPrivileges=yes`. Флаг наследуют все
потомки (`git` → `hook` → `sudo`), setuid перестаёт действовать, и `sudo` не
может стать root в принципе:

```
sudo: эффективный uid не равен 0, возможно, /usr/bin/sudo находится
в файловой системе, смонтированной с битом «nosuid»
```

Строки в `sudoers` при этом на месте и бесполезны — дело не в правах.

Обходится без ослабления сервиса: хук трогает файл-заявку
`/opt/airflow-test/.restart-request`, за ней следит `etl-deploy-restart.path`,
перезапуск делает root вне процесса push'а. Ставится шагом 9d
`setup-airflow-test.sh`.

### Как убедиться, что перезапуск состоялся

```bash
bash deploy/check-restart.sh
```

Показывает время последнего коммита в разложенном дереве, время заявки, время
старта каждого сервиса и последние срабатывания path-юнита. **Время старта
сервиса должно быть позже времени коммита.** Раньше — сервис работает на старом
коде, и вывод push'а тут не указ: заявку он подаёт, а исполняется она уже без
него.

Для конструктора есть и встроенная проверка: он сам сравнивает дерево, с
которым стартовал, с текущим `HEAD`, и вешает в шапке предупреждение
«Сервис конструктора старше этой страницы».

### А можно просто выдать процессу права?

Можно — убрать `NoNewPrivileges=yes` из
`deploy/dagbuilder/etl-dagbuilder-api.service` и сделать `daemon-reload`. Но
**делать этого не стоит**, и не только из-за ослабления сервиса, который пишет
файлы в репозиторий, ходит в боевые БД и умеет `git push`.

Главное — синхронный `sudo systemctl restart etl-dagbuilder-api` из хука
означает, что конструктор убивает **сам себя** в момент, когда его дочерний
процесс ещё не отдал ответ на запрос. Человек увидит оборванный запрос вместо
«запушено». Путь через заявку решает и это: в `etl-deploy-restart.service`
стоит `ExecStartPre=/bin/sleep 3`, и ответ успевает уйти.

То есть заявка — не костыль вместо привилегий, а более правильный способ.
Привилегии нужны были бы только чтобы вернуть синхронность, а синхронность здесь
как раз вредна.

## Обслуживание metadata-БД

Чистка БД airflow остаётся airflow-дагами (`DbCleanup1/2`) — удобно смотреть логи и
результат в UI. Изредка падает на `VACUUM FULL` (эксклюзивный лок vs heartbeat),
но это приемлемо ради контроля из интерфейса.

## Прод
Тот же механизм для ветки `prod` и существующего `/opt/airflow/airflow` —
`deploy/setup-airflow-prod.sh` + `deploy/deploy-prod.sh`. Порядок работ,
подготовка боевых БД и откат — в **[README-prod.md](README-prod.md)**.

Коротко: прод остаётся в своём `AIRFLOW_HOME` и своей metadata-базе, новый код
раскладывается в `/opt/airflow-prod/prod-src`, а переключение на него —
systemd drop-in к существующим юнитам (`--cutover` / `--rollback`).
