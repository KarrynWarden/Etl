# Выкатка на ПРОД (Phase 4)

Перенос текущей версии процессов с `airflow-test` на боевой сервер: тот же код,
те же даги, но **боевые БД**. Прод сейчас работает на старом коде из
`/opt/airflow/airflow/dags` с конфигом, где пути прибиты к `etlFolderProd`.

Главная мысль: **код одинаковый для всех сегментов**, отличаются только два
файла — `.env` (реквизиты БД) и systemd-EnvironmentFile (пути/AIRFLOW_HOME).
Поэтому «выложить на прод» = разложить тот же git-код рядом со старым и
переключить на него рубильник, а не править код под прод.

```
        gitea (ETL.git)  ── ветка test ──► /opt/airflow-test/etl.git ──► airflow-test (:8082)
                         └─ ветка prod ──► /opt/airflow-prod/etl.git ──► airflow ПРОД (:8080)
                                                     │
                       конструктор на Jupyter пушит только в test — прод всегда
                       получает то, что уже отработало на тесте
```

| | Тест | Прод |
|---|---|---|
| bare-репо | `/opt/airflow-test/etl.git` | `/opt/airflow-prod/etl.git` |
| код | `/opt/airflow-test/test-src` | `/opt/airflow-prod/prod-src` |
| ветка | `test` | `prod` |
| AIRFLOW_HOME | `/opt/airflow-test/home` | `/opt/airflow/airflow` (как было) |
| metadata | база `airflow_test` | база `airflow` (как было) |
| юниты | `airflow-test-*` | `airflow-scheduler`, `airflow-webserver` (как были) |
| группа доступа | `etldev` (все) | `etlprod` (только devel + airflow) |
| реквизиты БД | `test-src/.env` | `prod-src/.env` |

Прод **не** переезжает в новый AIRFLOW_HOME и не меняет metadata-базу: вся
история запусков, пользователи UI, connections и порт остаются на месте.
Меняется ровно одно — откуда airflow берёт даги и python-код.

---

## Рубильник и откат

Переключение сделано systemd drop-in'ом к существующим юнитам прода:

```
/etc/systemd/system/airflow-scheduler.service.d/10-etl-prod.conf
/etc/systemd/system/airflow-webserver.service.d/10-etl-prod.conf
```

Внутри — одна строка `EnvironmentFile=/opt/airflow-prod/airflow-prod.env`,
которая переопределяет `AIRFLOW__CORE__DAGS_FOLDER` и `PYTHONPATH`. Drop-in
читается **после** основного юнита, поэтому перебивает его `Environment=`.

- **включить:** `sudo bash setup-airflow-prod.sh --cutover`
- **выключить (откат):** `sudo bash setup-airflow-prod.sh --rollback`
- **посмотреть состояние:** `sudo bash setup-airflow-prod.sh --status`

Откат — это удаление двух файлов и рестарт: прод возвращается на старую папку
дагов ровно в том виде, в каком был. Ни старый код, ни `airflow.cfg`, ни данные
при этом не меняются.

---

## Порядок работ

Самое долгое здесь — **не** airflow, а подготовка боевых БД (шаг 3): триггеры
на ведущих таблицах и первичный залив ведомых. Всё это делается **заранее**, на
работающем старом проде, и переключения не требует.

### Шаг 1. Подготовка сервера (прод не трогается)

```bash
ssh devel@airflow
sudo -s
# перенеси на сервер deploy/setup-airflow-prod.sh и сверь шапку скрипта
# (PROD_HOME, UNITS, VENV — они должны совпадать с реальным продом):
bash setup-airflow-prod.sh
```

Скрипт создаёт группу `etlprod`, каталоги, bare-репо с `post-receive`,
EnvironmentFile, sudoers, пул `Etl` (100 слотов — его ждёт код) и симлинк
`prod-src/logs -> /opt/airflow/airflow/logs`. Работающий прод при этом
продолжает крутиться на старом коде.

### Шаг 2. Первый push кода и реквизиты боевых БД

С dev-PC:
```bash
cd ~/konkin/etl
git remote add prod ssh://devel@airflow/opt/airflow-prod/etl.git
bash deploy/deploy-prod.sh          # прод <- origin/test, с подтверждением
```
`post-receive` разложит код в `prod-src`, проверит, что даги парсятся, и —
поскольку рубильник ещё не включён — **ничего не перезапустит**.

Затем `.env` с реквизитами боевых БД. **Лучший способ — создать его прямо на
сервере редактором**: пароль тогда нигде не путешествует и не оседает ни в
истории shell, ни во временных файлах.

```bash
sudo -s
cp /opt/airflow-prod/prod-src/deploy/prod.env.example /opt/airflow-prod/prod-src/.env
nano /opt/airflow-prod/prod-src/.env          # вписать значения
chown root:etlprod /opt/airflow-prod/prod-src/.env
chmod 640 /opt/airflow-prod/prod-src/.env     # читает группа: под ней ходит airflow
```

Где взять значения: они уже есть на этом же сервере — в старом прод-коде
(`/opt/airflow/airflow/`), либо в его `.env`, либо прямо в старом
`Connect/__init__.py`, где реквизиты были захардкожены:
```bash
grep -rniE 'host|user|password|passwd|pwd|dsn|makedsn' /opt/airflow/airflow/Connect/ | head
ls -la /opt/airflow/airflow/.env 2>/dev/null
```

Если удобнее заполнять на dev-PC — файл переносится по ssh и ставится на место
с нужными правами, а временная копия затирается:
```bash
scp prod.env devel@airflow:/home/devel/prod.env
ssh devel@airflow
sudo install -o root -g etlprod -m 640 /home/devel/prod.env /opt/airflow-prod/prod-src/.env
shred -u /home/devel/prod.env
```
> Название файла на dev-PC — **`prod.env`**, а не `.env.prod`: `.gitignore`
> ловит `.env` и `*.env`, но `.env.prod` не подходит ни под один шаблон и может
> уехать в репозиторий. Ещё надёжнее — держать его вообще вне клона.

Проверка (печатает только OK/FAIL, без паролей и IP):
```bash
sudo -u airflow env PYTHONPATH=/opt/airflow-prod/prod-src \
  /opt/airflow/venv/bin/python /opt/airflow-prod/prod-src/tools/check_db.py MAIN A56
```

`.env` читает только код процессов. В systemd-файле `airflow-prod.env` паролей
нет и быть не должно — там только пути.

### Шаг 3. Готовность боевых БД — основная работа

```bash
PYTHONPATH=/opt/airflow-prod/prod-src python3 /opt/airflow-prod/prod-src/tools/preflight.py
```

`tools/preflight.py` ничего не пишет — только читает и печатает по каждой линии:
структура ведущей и ведомой против json-эталона, есть ли включённый триггер на
ведущей, есть ли группы в `etl_jobs`, пуста ли ведомая. Плюс служебные таблицы
(`etl_log_iud_row`, `etl_jobs`, `etl_log`) на обеих сторонах. Ключ возврата 0 —
замечаний нет, 1 — есть, 2 — не достучался до БД.

Разбор замечаний:

| Что показал preflight | Что делать |
|---|---|
| нет служебной таблицы | `etlFolder/queries/oracleSetup/01_create_etl_log_iud_row.sql` (Oracle) / такая же таблица на Postgres |
| нет триггера на ведущей | `02_trigger_template.sql`, примеры — `03_example_triggers.sql` |
| структура ведомой не совпадает с json | привести таблицу к json-эталону (лишние колонки в БД допустимы, нехватка — нет). Иначе линия при первом же запуске встанет в FLK и заморозится |
| ведомая ПУСТА | первичный залив, см. ниже |
| `etl_jobs`: групп нет | нормально для новой линии — группы заведутся при первом переносе |

**Порядок первичного залива — важен.** Сначала триггер, потом снимок:

1. создать триггер на ведущей — с этого момента все изменения копятся в
   `etl_log_iud_row` и ничего не теряется;
2. залить снимок ведущей в ведомую (любым способом — `INSERT ... SELECT`,
   выгрузка/загрузка);
3. включить линию: ETL догонит изменения из журнала. Повтор уже перенесённой
   записи безопасен — запись идёт через UPSERT/MERGE.

Обратный порядок (сначала снимок, потом триггер) теряет всё, что изменилось
между снимком и созданием триггера — так делать нельзя.

Отдельно:

- **`Medree_prdisp`** — процесс из двух частей, ему нужны скрипты `04`–`06` в
  Oracle, первичное заполнение `05_medree_prdisp_initial_fill.sql` (~16 млн
  строк, один проход, часы), доводка ведомой
  `postgresSetup/01_medree_prdisp_slave.sql` и разовый bootstrap-блок из
  раздела 5 файла `06_*.sql`. Подробности — README, раздел «Medree_prdisp».
- **Справочники (`Sp*`)** первичного залива не требуют: их даги каждый раз
  очищают ведомую и заливают заново.
- **Линии в режиме `section` / `delete_insert`** перезаливают группу целиком,
  поэтому «догнать» их можно, проставив нужным периодам `etl_jobs.isokaudit = 4`.
- **Схема служебных таблиц на Oracle прибита в .sql** (`koknaev.etl_jobs`,
  `koknaev.etl_log_iud_row`, `koknaev.etl_log`). Если в боевой БД они лежат в
  другой схеме — это правка `etlFolder/queries/general/**`, а не конфига;
  сделай её на ветке `test`, проверь и только потом выкатывай.

### Шаг 4. Точка отката

```bash
# на сервере, под postgres — дамп metadata прода
sudo -u postgres pg_dump -Fc airflow > /var/tmp/airflow-metadata-$(date +%F).dump
# копия старой папки дагов (её же вернёт --rollback, копия — на всякий случай)
tar czf /var/tmp/prod-dags-$(date +%F).tgz -C /opt/airflow/airflow dags
```
Тег кода `prod-ГГГГММДД-ЧЧММ` ставит `deploy-prod.sh` сам.

Резервных копий **боевых данных** это не заменяет: первичные заливы и
`delete_insert` меняют содержимое ведомых таблиц. Убедись, что штатный бэкап БД
свежий, до шага 3.

### Шаг 5. Заморозка старого прода

Два airflow не должны одновременно возить одни и те же данные. Перед
переключением старые даги ставятся на паузу:

```bash
sudo -u airflow env AIRFLOW_HOME=/opt/airflow/airflow /opt/airflow/venv/bin/airflow dags list -o plain \
  | awk 'NR>1 {print $1}' \
  | xargs -r -n1 sudo -u airflow env AIRFLOW_HOME=/opt/airflow/airflow /opt/airflow/venv/bin/airflow dags pause
```
Дождись, пока текущие задачи доработают (в UI не должно остаться `running`).

### Шаг 5.5. Предпроверка (можно и нужно гонять заранее)

```bash
sudo bash precheck-prod.sh
```
Ничего не переключает и не перезапускает — только читает и печатает отчёт:
подготовка на месте, рубильник выключен, код разложен, `.env` читается
пользователем `airflow`, даги парсятся, пул `Etl` нужного размера, какие
`dag_id` совпадут со старыми, какие даги прода сейчас включены (список
сохраняется в `/opt/airflow-prod/state/` — по нему гасить перед переключением и
возвращать при откате), и в конце — `tools/preflight.py` по боевым БД.

Скрипт безопасно запускать в рабочее время и столько раз, сколько нужно:
пока не выполнен `--cutover`, работающий прод его не замечает.

### Шаг 6. Переключение

```bash
sudo bash setup-airflow-prod.sh --cutover
```
Скрипт сначала проверит, что даги парсятся с окружением прода, и только потом
поставит drop-in и перезапустит службы. Если парсинг падает — переключение
отменяется, прод остаётся на старом коде.

### Шаг 7. Приёмка — включать по одной

`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True` в EnvironmentFile: новые даги
появляются **на паузе**. Внимание: даги, чей `dag_id` совпадает со старым,
сохраняют прежнее состояние паузы — их проверь в UI отдельно (шаг 5 их уже
поставил на паузу).

Порядок включения:

1. `SpDagNew` / `SpOnce` — справочники: короткие, полностью перезаливают
   таблицу, хорошо показывают, что реквизиты и структуры в порядке;
2. одна небольшая линия `iud` — посмотреть `etl_log`, `etl_jobs`, счётчики;
3. остальные линии переноса;
4. `AuditAll` — он ходит по всем линиям и складывает вердикт в
   `etl_jobs.isokaudit` (`1` — совпало, `-4` — расхождения, `-2` — ошибка
   проверки). Первый прогон после залива — самая честная приёмка.

### Шаг 7.5. Убрать старые даги из metadata

После переключения файлы старых дагов исчезают, но их записи остаются в
metadata и висят в UI рядом с рабочими — airflow сам их только деактивирует.
Примеры (`example_dags`) приходят из `load_examples = True` в старом
`airflow.cfg`; их выключает `AIRFLOW__CORE__LOAD_EXAMPLES=False` в
EnvironmentFile (скрипт подготовки её ставит) — после рестарта они перестают
загружаться, но записи в базе тоже остаются.

```bash
sudo bash cleanup-old-dags.sh            # показать, что лишнее
sudo bash cleanup-old-dags.sh --delete   # удалить, с подтверждением
```

Скрипт сравнивает даги в metadata с тем, что реально есть в новом коде, и
показывает разницу с путями к файлам. Удаление стирает **вместе с дагом всю
его историю запусков** (статусы задач, XCom; файлы логов на диске остаются) и
не отменяется — перед `--delete` снимите дамп:
```bash
sudo -u postgres pg_dump -Fc airflow > /var/tmp/airflow-metadata-$(date +%F).dump
```

### Шаг 8. Дальше — повседневно

```bash
# разработка и проверка — как раньше, на тесте
bash deploy/deploy-test.sh "что сделал"     # коллеги на Jupyter
bash local/dev-push.sh                      # dev-PC -> gitea + тест

# выкатка проверенного на прод
bash deploy/deploy-prod.sh                  # прод <- origin/test, с подтверждением
```

`deploy-prod.sh` не даст разойтись историей с продом (требует fast-forward),
покажет список коммитов и файлов, проверит сборку конфига и поставит тег
`prod-ГГГГММДД-ЧЧММ`. После push'а `post-receive` сам проверит парсинг дагов и
перезапустит прод; при ошибке импорта — **не** перезапустит.

---

## Грабли, о которых стоит знать заранее

- **Совпадающие `dag_id`.** Если даг с таким именем уже был на проде, он
  наследует прежнее состояние (пауза, расписание, история). Новый код при этом
  видит старые записи — проверяй такие даги отдельно.
- **Пул `Etl`.** Код держит ETL-замок на 100 слотов пула `Etl`
  (`Functions/_dagHelpers.ETL_POOL_SLOTS`). Если пула нет или он меньше — замок
  никогда не наберёт слоты и аудит повиснет. Скрипт подготовки создаёт пул;
  если он уже был — сверь размер.
- **Старый прод-конфиг с абсолютными путями.** В старой версии пути к
  структурам прибиты к `etlFolderProd`. Новый код резолвит относительные пути
  от `ETL_FULL_PATH` — специально, чтобы один конфиг работал во всех сегментах.
  Абсолютные пути в конфиг не возвращаем.
- **Рестарт во время переноса.** Деплой перезапускает scheduler, задачи получают
  SIGTERM. Код это переживает: необработанные записи журнала остаются с
  `isetl = 0` и уедут следующим запуском (ничего не паркуется как ошибка). Но
  тяжёлые окна (ночные заливы, `Medree_prdisp`) деплоем лучше не задевать.
- **Старый `plugins/` в AIRFLOW_HOME.** В `/opt/airflow/airflow/plugins` лежит
  копия кода времён `MODE` (`Functions/`, `Src/`). Airflow грузит плагины из
  `$AIRFLOW_HOME/plugins`, и эти модули ломаются об импорт — в логах сыпется
  `Broken plugin: cannot import name 'MODE' from 'Src.fullPath'`. Новому коду
  плагины не нужны (всё приезжает через `PYTHONPATH`), поэтому EnvironmentFile
  переводит `AIRFLOW__CORE__PLUGINS_FOLDER` на пустой `/opt/airflow-prod/plugins`.
  Старую папку не трогаем — при `--rollback` переменная исчезает и прод снова
  видит её как раньше.
- **`.env` не в git.** При checkout он не затирается (untracked), но и не
  приезжает — на новом сервере его надо создать руками.
- **Прод пушит не каждый.** Bare-репо прода в группе `etlprod` (devel +
  airflow), Jupyter туда не пишет. Конструктор коллег продолжает работать с
  тестом.
