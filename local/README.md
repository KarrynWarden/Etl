# Локальный airflow на dev-PC (Фаза 1)

Лёгкий airflow прямо на рабочем ПК для экспериментов: правишь даги/код в VS Code —
airflow видит изменения сразу, без копирования. Запуск одной командой, остановка
по Ctrl-C, все файлы видны в обычном проводнике.

- **Без Docker.** Берём готовый venv с сервера → точный паритет с продом (airflow
  2.7.2, Python 3.10, cx_Oracle 8.3.0, psycopg2 2.9.9), и никакой возни с контейнерами.
- **Лёгкий режим:** SQLite + SequentialExecutor, без Postgres/Redis.
- **Изоляция:** на прод/тест-сервер это не влияет никак.

Предпосылки: dev-PC и сервер — Astra 1.7 x86-64 (совпадают), свободно ~2 ГБ на диске,
dev-PC достаёт боевые БД по сети.

> **Важно про Astra SE и /opt.** Запись в `/opt` закрыта даже для `root`
> (мандатный контроль). Поэтому runtime распаковываем **в домашнюю папку**
> (`~/airflow-runtime`), без `sudo`, и перенастраиваем venv на новое место.

---

## Шаг 1. Упаковать runtime на сервере

```bash
ssh devel@airflow
sudo -s
# скопируй на сервер local/pack-runtime.sh (или выполни команды из него):
bash pack-runtime.sh
```

Получится `~/airflow-runtime-ГГГГММДД.tar.gz` в домашней папке `devel`.

## Шаг 2. Перенести и распаковать на dev-PC (без sudo)

```bash
scp devel@airflow:/home/devel/airflow-runtime-*.tar.gz ~/
mkdir -p ~/airflow-runtime
tar xzf ~/airflow-runtime-*.tar.gz -C ~/airflow-runtime
ls -ld ~/airflow-runtime/opt/python3.10 ~/airflow-runtime/opt/airflow/venv ~/airflow-runtime/opt/oracle
```

Должны появиться все три папки внутри `~/airflow-runtime/opt/`.

## Шаг 3. Перенастроить venv на новое место

venv помнит старый путь `/opt/...` — перенацелим его:

```bash
bash local/relocate-venv.sh ~/airflow-runtime
```

Скрипт поправит симлинки, `pyvenv.cfg` и шебанги и в конце проверит:
```
Python 3.10.7
2.7.2
OK: venv работает из ...
```

## Шаг 4. Запустить локальный airflow

Из корня репозитория:
```bash
bash local/airflow-local.sh
```

При первом запуске airflow сам создаст БД и напечатает логин/пароль для UI.
Открой <http://localhost:8080>. Остановка — Ctrl-C.

Что делает скрипт:
- `AIRFLOW_HOME = ~/airflow-local` (рантайм airflow, **вне** репозитория);
- даги берутся из `dags/` этого репозитория, код — из `Functions/`, `Src/`, `Connect/`
  (через `PYTHONPATH`);
- `ETL_MODE=""`, `ETL_FULL_PATH=<корень репо>/` → конфиг читается из `etlFolder/`
  (dev-версия, не Prod);
- airflow вызывается через интерпретатор venv напрямую, без зависимости от шебангов.

Главное на этом шаге: **venv с сервера завёлся и DAG-и парсятся** — в UI не должно
быть красных ошибок импорта. Для парсинга и UI пароли к БД не нужны (подключения
происходят только при запуске задачи).

## Шаг 5 (опционально). Реальные запуски задач против БД

Чтобы запускать задачи (а не только смотреть, как даги парсятся), нужны реквизиты БД.
Скопируй шаблон и впиши реальные значения — файл `.env` в `.gitignore`:
```bash
cp .env.example .env
# отредактируй .env: ETL_ORACLE_*, ETL_POST_*
```

> Сейчас `Connect/__init__.py` берёт реквизиты из захардкоженных заглушек, а не из
> переменных окружения (ветки с `os.environ` закомментированы), и путь к Oracle-конфигу
> там прибит к `/opt/oracle/config` — которого на dev-PC нет (runtime в домашней папке).
> Чтобы `.env` и локальный Oracle заработали, эти места нужно перевести на переменные
> окружения — отдельный небольшой шаг, делаем после того, как airflow поднимется и
> даги станут парситься.

---

## Если что-то не так

- **`mkdir /opt/...: Отказано в доступе` даже под root** — это Astra SE, так и должно
  быть. Не распаковывай в `/`, используй `~/airflow-runtime` (Шаг 2).
- **`airflow version` падает после relocate** — пришли вывод; чаще всего не хватает
  `LD_LIBRARY_PATH` к `~/airflow-runtime/opt/python3.10/lib` (скрипт его выставляет сам).
- **`DPI-1047` / Oracle не находит клиент** — всплывает только при запуске задачи к
  Oracle, не при парсинге; разбираем на Шаге 5.
- **`port 8080 in use`** — `ETL_LOCAL_PORT=8081 bash local/airflow-local.sh`.
- **Мало памяти** — закрой лишнее; `airflow standalone` в лёгком режиме ест ~0.7–1 ГБ.
- **Распаковал не в `~/airflow-runtime`** — укажи свой путь:
  `ETL_LOCAL_RUNTIME=/твой/путь bash local/airflow-local.sh`.
