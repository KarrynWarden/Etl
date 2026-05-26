# ETL: универсальный синхронизатор Oracle ↔ PostgreSQL

Один универсальный процесс `Functions.do_etl.Do_etl` поверх стратегии
`etl_log_iud_row` для всех 4 направлений: Orcl→Post, Post→Orcl, Post→Post,
Orcl→Orcl.

## Структура

```
Functions/
    do_etl.py                  — ядро алгоритма
    updateLog.py               — запись в etl_log
    functionsFile/
        jsonLoad.py            — загрузка json-структур
        loadConfig.py          — config.json
        takeOneQuery.py        — чтение .sql
        structCheck.py         — сверка структуры
Connect/__init__.py            — подключения к БД (через env)
Src/
    fullPath.py                — пути и MODE
    generalQueries.py          — реестр SQL из .sql-файлов
etlFolder/
    config.json                — параметры по парам tableNameEtlJobs+dbMaster+dbSlave
    queries/
        general/               — структурные запросы
        general/log/           — добавление лога
        general/newEtl/        — стратегия etl_log_iud_row + section
        oracleSetup/           — DDL etl_log_iud_row + шаблон триггера
        customQueries/         — пользовательские SELECT-источники
    structures/                — json-описания таблиц
dags/                          — примеры DAG'ов под все 4 направления
```

## Режимы обработки

1. **`iud`** (по умолчанию) — точечные апдейты. Триггеры на ведущей пишут в
   `etl_log_iud_row` (`oper IN ('IU','D')`, `isetl = 0`). Процесс читает
   необработанные записи, точечно переносит в ведомую через UPSERT/MERGE и
   выставляет `isetl = 1`. Решает проблему отложенного коммита. Доп.
   запускает массовое обновление групп с `isokaudit = 4`.

2. **`section_compare`** — срезовый режим для mocheck и medree:
   - сравниваются массивы уникальных `(period, MAX(lastupdate))` на
     ведущей и ведомой;
   - дополнительно подтягиваются группы из `etl_log_iud_row` (с isetl=0)
     и группы с `isokaudit = 4` в `etl_jobs`;
   - для каждой группы — полное `DELETE WHERE period = X [AND filter]`
     и перезаливка из ведущей;
   - после успешной обработки помечаются `isetl = 1` те записи журнала,
     которые существовали в момент старта (новые остаются для
     следующего запуска — снимает гонки).

3. **`section`** — упрощённая срезовая логика: обрабатываются только
   группы с `isokaudit = 4`. Используется, когда нужен ручной триггер
   массового переобновления.

## Что учтено

| Особенность | Как поддержано |
|-------------|----------------|
| 4 направления | `dbMaster`/`dbSlave` ∈ {Post, Orcl} |
| Кастомное имя `createdate` | `periodColumn`, `slavePeriodColumn` |
| Составной PK | разделитель `'/'` в `etl_log_iud_row.id` (см. триггер) |
| Doctype-split (mocheck) | `filterClause` (фильтрует источник) + `filterClauseSlave` (фильтрует DELETE / SELECT по ведомой) + `conflictExtra=['doctype']` (для частичного индекса) |
| 9 логических групп mocheck из 6 oracle-таблиц | один общий `MOCHECK.sql` UNION ALL + 9 entries в `config.json` (по одной на doctype), DAG итерирует через все 9 |
| medree-стиль срезов (dcalc + lastupdate) | `mode: section_compare`, `periodColumn: dcalc` |
| Master = SQL-запрос | `selectSql` |
| Несколько направлений у одной таблицы | разные `tableNameEtlJobs` + по триггеру на каждое |
| Лишние колонки в БД | проверка `set.issubset` — ок |
| Длина / scale столбца | в проверке игнорируются |
| Подмножество ETL-полей | `etlFields` |
| Все SQL — в `.sql` | да, чтобы открывать в DBeaver |
| Аудит группы (`isokaudit=4`) | один и тот же путь обработки |

## Установка на Oracle

Запустить из `etlFolder/queries/oracleSetup/`:
1. `01_create_etl_log_iud_row.sql` — таблица + индексы.
2. На каждую ведущую таблицу — триггер из `02_trigger_template.sql`,
   подставив имена. Готовые примеры в `03_example_triggers.sql`.

## Переменные окружения

Хранятся в файле `.env` в корне проекта (он в `.gitignore`, в репозиторий
не уходит). Шаблон со всеми ключами и значениями-плейсхолдерами —
`.env.example`.

Подготовка: `cp .env.example .env` и подставить свои значения. На старте
любого модуля (`Connect/__init__.py`, `Src/fullPath.py`) автоматически
вызывается `python-dotenv`, который и подхватит `.env`. Если переменная
уже задана в системе (`export ...`), она имеет приоритет — `.env` не
перетирает то, что уже есть.

Список ключей — в `.env.example`:

```
ETL_FULL_PATH, ETL_MODE
ETL_ORACLE_HOST / PORT / SID / USER / PWD / CONFIG_DIR
ETL_POST_HOST / PORT / DB / USER / PWD
```
