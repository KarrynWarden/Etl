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

## Поведение

1. **Стратегия `iud`** (по умолчанию). Триггеры на ведущей пишут в
   `etl_log_iud_row` (`oper IN ('IU','D')`, `iseth = 0`). Процесс читает
   необработанные записи, точечно переносит в ведомую через UPSERT/MERGE и
   выставляет `iseth = 1`. Решает проблему отложенного коммита.
2. **Стратегия `section`** (медри-стиль). При `isokaudit = 4` в
   `etl_jobs` группа (createdate) полностью перезаливается: всё удаляется
   на ведомой и заливается заново из ведущей. Используется для
   агрегатных таблиц.
3. Группы с `isokaudit = 4` обрабатываются всегда — даже в режиме `iud`.

## Что учтено

| Особенность | Как поддержано |
|-------------|----------------|
| 4 направления | `dbMaster`/`dbSlave` ∈ {Post, Orcl} |
| Кастомное имя `createdate` | `periodColumn`, `slavePeriodColumn` |
| Составной PK | разделитель `'/'` в `etl_log_iud_row.id` (см. триггер) |
| Doctype-split (mocheck) | `filterClause` + `conflictExtra` в конфиге |
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

```
ETL_FULL_PATH=/opt/airflow/airflow/
ETL_MODE=Prod
ETL_ORACLE_HOST=10.0.15.9
ETL_ORACLE_PORT=1521
ETL_ORACLE_SID=ias
ETL_ORACLE_USER=...
ETL_ORACLE_PWD=...
ETL_POST_HOST=10.0.15.35
ETL_POST_PORT=5432
ETL_POST_DB=ias5db
ETL_POST_USER=...
ETL_POST_PWD=...
```
