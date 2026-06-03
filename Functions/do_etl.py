"""
Универсальный ETL-процесс синхронизации двух БД (Oracle / PostgreSQL).

Поддерживаются направления: Orcl->Post, Post->Orcl, Post->Post, Orcl->Orcl.

Стратегия по умолчанию — на основе etl_log_iud_row:
    - триггеры на ведущей таблице пишут в etl_log_iud_row id строки и тип
      операции (I/U/D) с isetl=0;
    - ETL читает все необработанные записи и переносит соответствующие
      строки в ведомую таблицу, в конце выставляя isetl=1;
    - решена проблема отложенного коммита, так как триггер ловит INSERT/UPDATE
      непосредственно перед коммитом, но фиксируется логом только при коммите.

Дополнительно поддерживается массовое обновление группы (isokaudit=4 в
etl_jobs) — при таком статусе вся группа (createdate) перезаливается целиком,
что нужно для "сечения"-стиля медри.

Особенности, которые учитывает алгоритм
---------------------------------------
1. Имена колонок ведущей и ведомой могут не совпадать — сопоставление
   производится по порядку колонок в json-структурах.
2. Имя поля группировки ('createdate') можно переопределить через
   periodColumn / slavePeriodColumn.
3. Составной первичный ключ — поддерживается перечислением всех колонок,
   помеченных как "Primary Key" в json-структуре.
4. Имя группы в etl_jobs (tableNameEtlJobs) может отличаться от имени
   ведущей таблицы (одна физическая таблица может участвовать в нескольких
   направлениях — например, reqprepmo одновременно в post->orcl и
   post->post; mocheck — несколько логических групп через doctype).
5. Дополнительный фильтр (filterClause + filterParams) — для doctype-split
   таблиц вроде mocheck.
6. Источником ведущей может быть как таблица, так и произвольный
   SQL-запрос (selectSql).
7. Список полей, участвующих в ETL, фильтруется через etlFields
   (если задан) — остальные колонки в БД остаются нетронутыми.
8. Длина / scale столбца не должны блокировать перенос — выводится только
   предупреждение.
"""

from __future__ import annotations

import datetime
import logging
from collections import defaultdict

import psycopg2
import cx_Oracle
from airflow.exceptions import (
    AirflowSkipException, AirflowException, AirflowFailException,
)

from Connect import DbConnectPost, DbConnectOrcl
from Functions.functionsFile.jsonLoad import JsonLoadPost, JsonLoadOrcl
from Functions.functionsFile.loadConfig import LoadConfig
from Functions.functionsFile.structCheck import (
    StructCheckDataBase,
    StructCheckOracleQuery,
    StructCheckPostgresQuery,
)
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Functions.updateLog import UpdateLog
from Src.generalQueries import (
    structureCheckOrclSql,
    structureCheckPostSql,
    structureEmptyQuerySql,
    # выбор групп / id
    dateSelectStatusOrclSql,
    dateSelectStatusPostSql,
    selectEtlIudOrclSql,
    selectEtlIudPostSql,
    selectDistinctOrclSql,
    selectDistinctPostSql,
    # запись в ведомую
    upsertOrclSql,
    upsertPostSql,
    deleteByIdOrclSql,
    deleteByIdPostSql,
    deletePeriodOrclSql,
    deletePeriodPostSql,
    insertOrclSql,
    insertPostSql,
    slavePeriodsByIdOrclSql,
    slavePeriodsByIdPostSql,
    # выборка из ведущей
    recordSelectByIdOrclSql,
    recordSelectByIdPostSql,
    recordSelectGroupOrclSql,
    recordSelectGroupPostSql,
    # сервисные запросы по etl_jobs / etl_log_iud_row
    etlIdUpdateOrclSql,
    etlIdUpdatePostSql,
    etlStatusChangeOrclSql,
    etlStatusChangePostSql,
    etlUpdateOrclSql,
    etlUpdatePostSql,
    etlErrorOrclSql,
    etlErrorPostSql,
    newDatesOrclSql,
    newDatesPostSql,
    # section_compare (mocheck / medree)
    dateSelectMasterPostSql,
    dateSelectMasterOrclSql,
    dateSelectSlavePostSql,
    dateSelectSlaveOrclSql,
    periodsFromIudPostSql,
    periodsFromIudOrclSql,
    markPeriodIudPostSql,
    markPeriodIudOrclSql,
    periodsIsokAudit4PostSql,
    periodsIsokAudit4OrclSql,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
#                       Классификация ошибок по типу
# ----------------------------------------------------------------------------

class RecordScopeError(AirflowException):
    """Часть записей не перенесена из-за ошибок per-record (плохие данные,
    констрейнт и т.п.). Эти записи припаркованы (isetl=-1), линия НЕ
    морозится — но текущий запуск красится 🟥 для видимости в общем списке.
    Подхватывается в runEtl: ставит XCom error_class='record', чтобы FSM
    ретраев и watcher не считали эту 🟥 за «сломанную линию».
    """
    pass


# DB-API 2.0 иерархия одинаково экспонируется psycopg2 и cx_Oracle, поэтому
# можно классифицировать ошибку независимо от драйвера.
_CONNECTION_EXCEPTIONS = (
    psycopg2.OperationalError, psycopg2.InterfaceError,
    cx_Oracle.OperationalError, cx_Oracle.InterfaceError,
)
_PROGRAMMING_EXCEPTIONS = (
    psycopg2.ProgrammingError,
    cx_Oracle.ProgrammingError,
)
_RECORD_EXCEPTIONS = (
    psycopg2.IntegrityError, psycopg2.DataError,
    cx_Oracle.IntegrityError, cx_Oracle.DataError,
)


def classifyError(err):
    """Классифицировать исключение → 'record' / 'retryable' / 'fatal'.

    record    — ошибка по конкретной записи (констрейнт, тип данных, баг
                кода): запись паркуем (isetl=-1), остальные продолжаем,
                запуск 🟥, линия НЕ морозится;
    retryable — соединение умерло (OperationalError/InterfaceError) —
                ретраи по backoff, потом заморозка;
    fatal     — битый SQL/структура (ProgrammingError, FLK) — сразу
                заморозка, ретраи бессмысленны.

    Неизвестный класс → record (см. обсуждение: системный код-баг
    засигналит чередой 🟥 и счётчиком провальных в общем списке,
    но не заморозит линию из-за одной странной записи).
    """
    # AirflowException обычно обёрнут вокруг настоящей причины через
    # `raise ... from err` — раскручиваем до корня.
    while isinstance(err, AirflowException) and err.__cause__ is not None:
        err = err.__cause__
    if isinstance(err, _CONNECTION_EXCEPTIONS):
        return "retryable"
    if isinstance(err, _PROGRAMMING_EXCEPTIONS):
        return "fatal"
    if isinstance(err, _RECORD_EXCEPTIONS):
        return "record"
    return None


# ----------------------------------------------------------------------------
#                              Вспомогательные утилиты
# ----------------------------------------------------------------------------

def _connect(dbType):
    return DbConnectPost() if dbType == "Post" else DbConnectOrcl()


def _loadStructure(path, dbType):
    return JsonLoadPost(path) if dbType == "Post" else JsonLoadOrcl(path)


def _isPost(dbType):
    return dbType == "Post"


def _pickSql(dbType, postSql, orclSql):
    return postSql if _isPost(dbType) else orclSql


def _bindName(dbType, name):
    """Имя плейсхолдера в зависимости от драйвера."""
    return f"%({name})s" if _isPost(dbType) else f":{name}"


def _filterEtlFields(jsonStruct, etlFields):
    """Оставить только те поля, которые участвуют в ETL.

    PK всегда сохраняется, даже если не указан явно — иначе нечем будет
    сопоставлять записи.
    """
    if not etlFields:
        return list(jsonStruct)
    allowed = {f.lower() for f in etlFields}
    return [f for f in jsonStruct
            if f[0].lower() in allowed or f[3] == "Primary Key"]


def _primaryKeys(jsonStruct):
    """Список колонок-PK в порядке, заданном json-структурой."""
    return [f[0] for f in jsonStruct if f[3] == "Primary Key"]


def _columnNames(jsonStruct):
    return [f[0] for f in jsonStruct]


def _executeQuery(cursor, sql, params=None):
    cursor.execute(sql, params or {})
    return cursor.fetchall()


def _normalizePeriod(value):
    """Привести значение периода к datetime.date.

    Oracle DATE → datetime.datetime (00:00:00), Postgres date → datetime.date.
    Без нормализации:
      - ключи set/dict разных типов не равны: date(2024,1,1) != datetime(2024,1,1);
      - sorted() падает с TypeError на смешанной коллекции.
    Приводим к date — это естественная гранулярность createdate, и оба
    драйвера принимают datetime.date в WHERE period = X.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    return value


def _bindList(values, prefix):
    """Сгенерировать (in_clause, params) для Oracle IN-выражения.

    cx_Oracle не умеет биндить Python-list в обычный SQL (ORA-01484
    "arrays can only be bound to PL/SQL statements"). Поэтому для Oracle
    приходится разворачивать список в конкретное число именованных
    плейсхолдеров: ":prefix0, :prefix1, ...".

    Возвращает строку для подстановки в `IN (...)` и словарь параметров.
    """
    placeholders = ", ".join(f":{prefix}{i}" for i in range(len(values)))
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return placeholders, params


def _appendFilter(sql, filterClause):
    """Добавить пользовательский WHERE-фильтр к подзапросу.

    Используется, когда одна физическая таблица служит источником сразу
    нескольких логических групп (mocheck doctype-split): selectSql у них
    общий, а отличается только дополнительный фильтр.
    """
    if not filterClause:
        return sql
    # Допускаем как полную форму "WHERE x = y", так и краткую "x = y".
    cleaned = filterClause.strip()
    if cleaned.lower().startswith("where"):
        cleaned = cleaned[5:].strip()
    return f"SELECT * FROM ({sql}) src WHERE {cleaned}"


# ----------------------------------------------------------------------------
#                       Построение SQL для ведомой таблицы
# ----------------------------------------------------------------------------

def _buildUpsertSql(dbSlave, tableNameSlave, structSlave,
                    conflictExtra, conflictWhere, filterClauseSlave, mode):
    """Сформировать SQL для записи строки в ведомую таблицу.

    Режим section_compare всегда сопровождается DELETE WHERE period = X
    [AND filter] перед заливкой группы — конфликта по уникальному индексу
    в нём в принципе быть не может, поэтому используется простой INSERT.
    Это также убирает требование «иметь уникальный индекс под conflictExtra
    на ведомой» — для mocheck с частичным индексом только под doctype=7
    это критично.

    В режиме iud точечно меняем строки по PK — без ON CONFLICT не обойтись.
    Если индекс на ведомой ЧАСТИЧНЫЙ (например,
        CREATE UNIQUE INDEX ... ON mocheck (doctype, idrw) WHERE doctype = 7
    ), нужно задать в конфиге conflictWhere = "doctype = 7" — это попадёт
    в ON CONFLICT (...) WHERE <conflictWhere>, чтобы PG нашёл тот самый
    частичный индекс.
    """
    columns = _columnNames(structSlave)
    pkCols = _primaryKeys(structSlave)

    if _isPost(dbSlave):
        columnsStr = ", ".join(columns)
        valuesStr = ", ".join(["%s"] * len(columns))
        if mode in ("section", "section_compare", "delete_insert"):
            # delete_insert тоже сначала удаляет (по idrw), поэтому конфликта
            # нет — простой INSERT без ON CONFLICT.
            return insertPostSql.format(
                tablename=tableNameSlave,
                columns_str=columnsStr,
                values_str=valuesStr,
            )
        primaryStr = ", ".join(pkCols + list(conflictExtra or ()))
        updateStr = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
        whereClause = f" WHERE {conflictWhere}" if conflictWhere else ""
        return upsertPostSql.format(
            tablename=tableNameSlave,
            columns_str=columnsStr,
            values_str=valuesStr,
            update_str=updateStr,
            primary_str=primaryStr,
            conflict_where=whereClause,
        )

    # Oracle: MERGE INTO ... USING dual — без изменений; в Oracle нет
    # «частичных» уникальных индексов и ON CONFLICT WHERE.
    insertFields, insertValues, updateParts = [], [], []
    pkCondParts = []
    for idx, field in enumerate(structSlave, start=1):
        name = field[0]
        placeholder = f":{idx}"
        insertFields.append(name)
        insertValues.append(placeholder)
        if name in pkCols:
            pkCondParts.append(f"{name} = {placeholder}")
        else:
            updateParts.append(f"{name} = {placeholder}")
    pkCondParts.extend(filterClauseSlave or [])
    return upsertOrclSql.format(
        tablename=tableNameSlave,
        primary_cond=" AND ".join(pkCondParts),
        update_str=", ".join(updateParts),
        insert_fields_str=", ".join(insertFields),
        insert_values_str=", ".join(insertValues),
    )




def _buildDeleteByIdSql(dbSlave, tableNameSlave, pkCols, filterClauseSlave):
    primaryCond = " AND ".join(
        f"{c} = " + _bindName(dbSlave, f"id{i}")
        for i, c in enumerate(pkCols)
    )
    if filterClauseSlave:
        primaryCond += " AND " + " AND ".join(filterClauseSlave)
    sqlTpl = _pickSql(dbSlave, deleteByIdPostSql, deleteByIdOrclSql)
    return sqlTpl.format(tablename=tableNameSlave, primary_cond=primaryCond)


def _buildSlavePeriodsByIdSql(dbSlave, tableNameSlave, pkColsSlave,
                              slavePeriodColumn, filterClauseSlave,
                              truncatePeriod=False):
    """SQL для DISTINCT периодов строк ведомой по логическому id (idrw).

    Нужен режиму delete_insert: перед удалением узнаём, какие группы
    etl_jobs затронуты на стороне ведомой (period в etl_log_iud_row для
    expmed декоративный и в логике не участвует).
    """
    if truncatePeriod:
        periodExpr = (f"DATE({slavePeriodColumn})" if _isPost(dbSlave)
                      else f"TRUNC({slavePeriodColumn})")
    else:
        periodExpr = slavePeriodColumn
    primaryCond = " AND ".join(
        f"{c} = " + _bindName(dbSlave, f"id{i}")
        for i, c in enumerate(pkColsSlave)
    )
    if filterClauseSlave:
        primaryCond += " AND " + " AND ".join(filterClauseSlave)
    sqlTpl = _pickSql(dbSlave, slavePeriodsByIdPostSql, slavePeriodsByIdOrclSql)
    return sqlTpl.format(period_expr=periodExpr, tablename=tableNameSlave,
                         primary_cond=primaryCond)


def _buildDeletePeriodSql(dbSlave, tableNameSlave, slavePeriodColumn,
                          filterClauseSlave, truncatePeriod=False):
    """SQL для удаления записей группы по периоду."""
    if truncatePeriod:
        if _isPost(dbSlave):
            periodExpr = f"DATE({slavePeriodColumn})"
        else:
            periodExpr = f"TRUNC({slavePeriodColumn})"
    else:
        periodExpr = slavePeriodColumn

    cond = f"{periodExpr} = " + _bindName(dbSlave, "createdate")
    if filterClauseSlave:
        cond += " AND " + " AND ".join(filterClauseSlave)
    sqlTpl = _pickSql(dbSlave, deletePeriodPostSql, deletePeriodOrclSql)
    return sqlTpl.format(tablename=tableNameSlave, period_cond=cond)


# ----------------------------------------------------------------------------
#                       Построение SQL для ведущей таблицы
# ----------------------------------------------------------------------------

def _buildFieldsStr(dbMaster, structMaster, periodColumn):
    """Собрать список полей для SELECT из ведущей.

    Для PostgreSQL применяется date_trunc('second', lastupdate) — иначе
    миллисекунды могут давать ложные различия в аудите.
    """
    parts = []
    for field in structMaster:
        name = field[0]
        if _isPost(dbMaster) and name == "lastupdate":
            parts.append(f"date_trunc('second', p.{name}) as {name}")
        else:
            parts.append(f"p.{name}")
    return ", ".join(parts)


def _buildRecordByIdSql(dbMaster, selectSql, structMaster, pkColsMaster,
                        periodColumn, filterClauseMaster, truncatePeriod=False):
    """SQL для выбора записи по PK (для индивидуальных обновлений)."""
    fieldsStr = _buildFieldsStr(dbMaster, structMaster, periodColumn)
    primaryCond = " AND ".join(
        f"p.{c} = " + _bindName(dbMaster, f"id{i}")
        for i, c in enumerate(pkColsMaster)
    )
    if filterClauseMaster:
        primaryCond += " AND " + " AND ".join(filterClauseMaster)
    sqlTpl = _pickSql(dbMaster, recordSelectByIdPostSql, recordSelectByIdOrclSql)
    return sqlTpl.format(
        fields_str=fieldsStr,
        select_sql=selectSql,
        primary_cond=primaryCond,
    )


def _buildRecordGroupSql(dbMaster, selectSql, structMaster, periodColumn,
                         filterClauseMaster, truncatePeriod=False):
    """Собрать SQL для выбора группы записей по периоду.

    truncatePeriod: если True, для Oracle применяется TRUNC(period),
                    для Postgres — DATE(period). Это нужно, когда в БД
                    колонка периода содержит время (например, dcalc в medree),
                    а в ETL процесс передается только дата.
    """
    fieldsStr = _buildFieldsStr(dbMaster, structMaster, periodColumn)

    # Формируем выражение для периода с учетом truncatePeriod
    if truncatePeriod:
        if _isPost(dbMaster):
            periodExpr = f"DATE(p.{periodColumn})"
        else:
            periodExpr = f"TRUNC(p.{periodColumn})"
    else:
        periodExpr = f"p.{periodColumn}"

    cond = f"{periodExpr} = " + _bindName(dbMaster, "createdate")
    if filterClauseMaster:
        cond += " AND " + " AND ".join(filterClauseMaster)
    sqlTpl = _pickSql(dbMaster, recordSelectGroupPostSql, recordSelectGroupOrclSql)
    return sqlTpl.format(
        fields_str=fieldsStr,
        select_sql=selectSql,
        period_col=periodColumn,
        period_cond=cond,
    )


# ----------------------------------------------------------------------------
#                              Группа ↔ периоды
# ----------------------------------------------------------------------------

def _registerNewPeriods(cursor, dbMaster, sourceFromClause, tableNameEtlJobs,
                        periodColumn):
    """Добавить в etl_jobs группы (createdate), которых ещё нет.

    sourceFromClause — то, что подставляется в FROM: имя таблицы либо
    обёрнутый '(...) ' для произвольного SQL.
    """
    sqlTpl = _pickSql(dbMaster, newDatesPostSql, newDatesOrclSql)
    print(sqlTpl.format(sourceFromClause, periodColumn), ' and ', tableNameEtlJobs, tableNameEtlJobs.lower())
    cursor.execute(
        sqlTpl.format(sourceFromClause, periodColumn),
        {
            "tablename": tableNameEtlJobs,
            #"tablenamelow": tableNameEtlJobs.lower(),
        },
    )


def _selectGroupsForMassUpdate(cursor, dbMaster, tableNameEtlJobs):
    """Группы, которые требуют полного обновления (isokaudit = 4)."""
    sqlTpl = _pickSql(dbMaster, periodsIsokAudit4PostSql, periodsIsokAudit4OrclSql)
    return _executeQuery(
        cursor,
        sqlTpl,
        {"tablename": tableNameEtlJobs},
    )


def _selectIudRecords(cursor, dbMaster, tableNameEtlJobs):
    sqlTpl = _pickSql(dbMaster, selectEtlIudPostSql, selectEtlIudOrclSql)
    #print('here is sqlTpl ', sqlTpl, tableNameEtlJobs)
    return _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs})


def _selectDistinctIds(cursor, dbMaster, idLogs):
    """По списку idrw из etl_log_iud_row — уникальные (id, period, oper).

    Postgres принимает массив через `= ANY(%(idlogs)s)` — psycopg2 это
    биндит как обычный list. Oracle с cx_Oracle такое не умеет
    (ORA-01484), поэтому для него разворачиваем IN-список в N именованных
    плейсхолдеров.
    """
    if not idLogs:
        return []
    if _isPost(dbMaster):
        return _executeQuery(cursor, selectDistinctPostSql,
                             {"idlogs": idLogs})
    inClause, params = _bindList(idLogs, "id")
    sql = selectDistinctOrclSql.format(in_clause=inClause)
    return _executeQuery(cursor, sql, params)


def _markEtlIud(cursor, dbMaster, idrws, isetl):
    """Пометить пачку idrw в etl_log_iud_row значением isetl (1 / -1).

    Та же история с массивом — Oracle разворачивает IN, Postgres получает
    list напрямую.
    """
    if not idrws:
        return
    if _isPost(dbMaster):
        cursor.execute(etlIdUpdatePostSql,
                       {"isetl": isetl, "idrws": idrws})
        return
    inClause, params = _bindList(idrws, "i")
    params["isetl"] = isetl
    cursor.execute(etlIdUpdateOrclSql.format(in_clause=inClause), params)


# ----------------------------------------------------------------------------
#                           Массовое обновление (isokaudit = 4)
# ----------------------------------------------------------------------------

def _processGroupUpdate(cfg, ctx, dateGroup):
    """Полная перезаливка группы (createdate) — режим isokaudit=4 / 'section'.

    Логика медри: удаляем все записи группы из ведомой и заливаем заново
    то, что отдаёт ведущий запрос.
    """
    createdate = dateGroup[0]
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    action = "ETL_ISOKAUDIT_4"

    logger.info("Обработка группы %s (массовое обновление)", createdate)

    conSlave = None
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()

        if not StructCheckDataBase(
            ctx["structSlave"], cursorSlave,
            _pickSql(dbSlave, structureCheckPostSql, structureCheckOrclSql),
            cfg["tableNameSlave"],
        ):
            logger.error("Структуры ведомой %s не совпадают", cfg["tableNameSlave"])
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "ведомых")
            raise AirflowFailException(
                f"FLK: структура ведомой {cfg['tableNameSlave']} не совпадает с json-эталоном"
            )

        # isetl: 0 -> 1 в etl_log_iud_row для уже зафиксированных записей
        # этой группы — они будут перезалиты вместе со всей группой.
        cursorMaster.execute(
            _pickSql(dbMaster, etlStatusChangePostSql, etlStatusChangeOrclSql),
            {"tablename": tableNameEtlJobs, "createdate": createdate},
        )

        # 1) удалить все записи группы из ведомой
        cursorSlave.execute(ctx["deletePeriodSql"], {"createdate": createdate})
        print('here is select ', ctx["recordGroupSql"], tableNameEtlJobs, createdate, **(cfg.get("filterParams") or {}))
        # 2) выбрать актуальные записи из ведущей и залить
        cursorMaster.execute(ctx["recordGroupSql"],
                             {"tablename": tableNameEtlJobs,
                              "createdate": createdate,
                              **(cfg.get("filterParams") or {})})
        records = cursorMaster.fetchall()
        logger.info("Найдено %d записей для группы %s", len(records), createdate)

        _bulkUpsert(cursorSlave, dbSlave, ctx["upsertSql"],
                    ctx["structSlave"], records)

        # 3) обновить etl_jobs (last_success_ts, isokaudit=0)
        cursorMaster.execute(
            _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
            {"LAST_SUCCESS_TS": ctx["currDt"],
             "TABLENAME": tableNameEtlJobs,
             "PERIOD": createdate},
        )

        conMaster.commit()
        conSlave.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, len(records), createdate)
        logger.info("Группа %s успешно обработана", createdate)
    except Exception as err:
        if conMaster:
            conMaster.rollback()
        if conSlave:
            conSlave.rollback()
        cursorMaster.execute(
            _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
            {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
             "PERIOD": createdate},
        )
        conMaster.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, 0, createdate)
        logger.error("Ошибка обработки группы %s: %s", createdate, err)
        raise
    finally:
        if conSlave:
            conSlave.close()


def _bulkUpsert(cursorSlave, dbSlave, upsertSql, structSlave, records):
    """Вставка/обновление пачки записей в ведомой."""
    if _isPost(dbSlave):
        if records:
            cursorSlave.executemany(upsertSql, records)
        return
    # Oracle: позиционные параметры :1 .. :N
    for record in records:
        params = {str(i + 1): value for i, value in enumerate(record)}
        cursorSlave.execute(upsertSql, params)


# ----------------------------------------------------------------------------
#                       Индивидуальные обновления (etl_log_iud_row)
# ----------------------------------------------------------------------------

def _processIndividualUpdates(cfg, ctx, distinctIds, iudRecords):
    """Точечная синхронизация по событиям из etl_log_iud_row.

    ИЗМЕНЕНИЕ: Master-запрос может вернуть НЕСКОЛЬКО записей для одного id.
    Все записи обрабатываются атомарно: если одна упала — роллбек всей пачки.
    """
    if not distinctIds:
        logger.info("Записи для индивидуального обновления отсутствуют")
        return

    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    action = "ETL_LOG_IUD_ROW"

    logger.info("Старт индивидуальных обновлений: %d записей", len(distinctIds))
    groupsData = _groupSummary(distinctIds)

    conSlave = None
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()

        if not StructCheckDataBase(
            ctx["structSlave"], cursorSlave,
            _pickSql(dbSlave, structureCheckPostSql, structureCheckOrclSql),
            cfg["tableNameSlave"],
        ):
            logger.error("Структуры ведомой %s не совпадают", cfg["tableNameSlave"])
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "ведомых")
            raise AirflowFailException(
                f"FLK: структура ведомой {cfg['tableNameSlave']} не совпадает с json-эталоном"
            )

        recordErrors = []  # [(recId, period, errStr), ...]

        for entry in distinctIds:
            recId, period, _timeoper, oper = entry[0], entry[1], entry[2], entry[3]

            try:
                pkParams = _splitPkValue(recId, len(ctx["pkColsMaster"]))

                if oper == "IU":
                    # ИЗМЕНЕНИЕ: fetchall() вместо fetchone()
                    cursorMaster.execute(ctx["recordByIdSql"],
                                         {**pkParams, **(cfg.get("filterParams") or {})})
                    rowsDb = cursorMaster.fetchall()

                    if not rowsDb:
                        logger.warning(
                            "Запись id=%s в ведущей не найдена — пропуск",
                            recId,
                        )
                    else:
                        logger.info(
                            "Запись id=%s: найдено %d строк для переноса",
                            recId, len(rowsDb),
                        )
                        # Переносим ВСЕ найденные записи
                        for rowDb in rowsDb:
                            if _isPost(dbSlave):
                                cursorSlave.execute(ctx["upsertSql"], rowDb)
                            else:
                                params = {str(i + 1): v for i, v in enumerate(rowDb)}
                                cursorSlave.execute(ctx["upsertSql"], params)

                else:  # 'D' — удаление
                    # Для DELETE удаляем ВСЕ записи с этим PK из slave
                    cursorSlave.execute(ctx["deleteByIdSql"], pkParams)
                    logger.info("Запись id=%s: удалена из ведомой", recId)

                # Коммитим только после успешной обработки ВСЕХ записей для этого id
                conSlave.commit()

                # Помечаем записи в etl_log_iud_row как обработанные
                idrws = [b for a, b in iudRecords if a == recId]
                _markEtlIud(cursorMaster, dbMaster, idrws, 1)
                conMaster.commit()

            except Exception as err:
                cls = classifyError(err) or "record"

                if conSlave:
                    conSlave.rollback()

                if cls != "record":
                    # connection / programming / fatal: прерываем цикл
                    logger.error(
                        "Ошибка по id=%s КЛАСС=%s — прерываю цикл: %s",
                        recId, cls, err,
                    )
                    raise

                # RECORD-ошибка: паркуем запись (isetl=-1)
                logger.error(
                    "RECORD-ошибка по id=%s, паркую isetl=-1: %s",
                    recId, err,
                )
                idrws = [b for a, b in iudRecords if a == recId]
                _markEtlIud(cursorMaster, dbMaster, idrws, -1)
                conMaster.commit()
                recordErrors.append((recId, period, str(err)))
                _markFailGroup(groupsData, period)

        # Перепроверить новые createdate
        _registerNewPeriods(cursorMaster, dbMaster, ctx["selectSql"],
                            tableNameEtlJobs, cfg["periodColumn"])

        for groupId, count, status in groupsData:
            if status == "ok":
                cursorMaster.execute(
                    _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
                    {"LAST_SUCCESS_TS": ctx["currDt"],
                     "TABLENAME": tableNameEtlJobs,
                     "PERIOD": groupId},
                )
                UpdateLog(tableNameEtlJobs, dbMaster, action,
                          cursorMaster, conMaster, count, groupId, 1)
            else:
                cursorMaster.execute(
                    _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
                    {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
                     "PERIOD": groupId},
                )
                UpdateLog(tableNameEtlJobs, dbMaster, action,
                          cursorMaster, conMaster, count, groupId)

        conMaster.commit()

        if recordErrors:
            sample = ", ".join(str(r[0]) for r in recordErrors[:5])
            more = f" (+{len(recordErrors) - 5} ещё)" if len(recordErrors) > 5 else ""
            raise RecordScopeError(
                f"{len(recordErrors)} записей не перенесено: {sample}{more}. "
                f"Записи припаркованы (isetl=-1); линия продолжает работать. "
                f"Повторить ручкой: UPDATE etl_log_iud_row SET isetl=0 "
                f"WHERE tablename='{tableNameEtlJobs}' AND isetl=-1;"
            )

    finally:
        if conSlave:
            conSlave.close()


def _processDeleteInsert(cfg, ctx, distinctIds, iudRecords):
    """Режим delete_insert: один логический id (idrw) = НЕСКОЛЬКО строк ведомой.

    Событийный, как iud (читает etl_log_iud_row по idrw), но на каждое
    событие idrw: удаляет ВСЕ строки этого idrw в ведомой и (для IU)
    заливает актуальные строки ведущей. Это убирает «осиротевшие» строки
    при смене doctype (старый doctype=2 idrw=123 остаётся, когда запись
    переехала в doctype=3) — чего upsert в режиме iud не ловит.

    period из etl_log_iud_row здесь ДЕКОРАТИВНЫЙ (для expmed это docexpdt,
    неверный для doctype=4) и в логике НЕ участвует. Реальные затронутые
    периоды берём из данных: для удаляемых строк — из ведомой (до удаления),
    для вставляемых — из ведущей.
    """
    if not distinctIds:
        logger.info("delete_insert: записей для обработки нет")
        return

    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    action = "ETL_DELETE_INSERT"
    # позиция колонки периода в строках ведущей (для periodColumn=createdate)
    periodIdx = _columnNames(ctx["structMaster"]).index(cfg["periodColumn"])

    logger.info("Старт delete_insert: %d событий", len(distinctIds))
    okPeriods, failPeriods = set(), set()
    periodCount = defaultdict(int)
    recordErrors = []  # [(recId, errStr), ...]

    conSlave = None
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()

        if not StructCheckDataBase(
            ctx["structSlave"], cursorSlave,
            _pickSql(dbSlave, structureCheckPostSql, structureCheckOrclSql),
            cfg["tableNameSlave"],
        ):
            logger.error("Структуры ведомой %s не совпадают", cfg["tableNameSlave"])
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "ведомых")
            raise AirflowFailException(
                f"FLK: структура ведомой {cfg['tableNameSlave']} не совпадает с json-эталоном"
            )

        for entry in distinctIds:
            recId, _logPeriod, _timeoper, oper = entry[0], entry[1], entry[2], entry[3]
            affected = set()
            try:
                pkParamsSlave = _splitPkValue(recId, len(ctx["pkColsSlave"]))

                # 1) реальные периоды удаляемых строк — из ведомой, ДО удаления
                cursorSlave.execute(ctx["slavePeriodsByIdSql"], pkParamsSlave)
                for row in cursorSlave.fetchall():
                    p = _normalizePeriod(row[0])
                    if p is not None:
                        affected.add(p)

                # 2) удалить ВСЕ строки этого idrw в ведомой
                cursorSlave.execute(ctx["deleteByIdSql"], pkParamsSlave)

                # 3) для IU — залить актуальные строки ведущей
                if oper == "IU":
                    pkParamsMaster = _splitPkValue(recId, len(ctx["pkColsMaster"]))
                    cursorMaster.execute(
                        ctx["recordByIdSql"],
                        {**pkParamsMaster, **(cfg.get("filterParams") or {})},
                    )
                    rowsDb = cursorMaster.fetchall()
                    for rowDb in rowsDb:
                        if _isPost(dbSlave):
                            cursorSlave.execute(ctx["upsertSql"], rowDb)
                        else:
                            params = {str(i + 1): v for i, v in enumerate(rowDb)}
                            cursorSlave.execute(ctx["upsertSql"], params)
                        # реальный период вставляемой строки — из ведущей
                        p = _normalizePeriod(rowDb[periodIdx])
                        if p is not None:
                            affected.add(p)
                            periodCount[p] += 1
                    logger.info("idrw=%s: -%s/+%d строк, периоды %s",
                                recId, "all", len(rowsDb),
                                sorted(x for x in affected if x is not None))
                else:  # 'D'
                    logger.info("idrw=%s: удалено из ведомой, периоды %s",
                                recId, sorted(x for x in affected if x is not None))

                conSlave.commit()

                idrws = [b for a, b in iudRecords if a == recId]
                _markEtlIud(cursorMaster, dbMaster, idrws, 1)
                conMaster.commit()

                okPeriods |= affected

            except Exception as err:
                cls = classifyError(err) or "record"
                if conSlave:
                    conSlave.rollback()
                if cls != "record":
                    logger.error("Ошибка idrw=%s КЛАСС=%s — прерываю цикл: %s",
                                 recId, cls, err)
                    raise
                logger.error("RECORD-ошибка idrw=%s, паркую isetl=-1: %s",
                             recId, err)
                idrws = [b for a, b in iudRecords if a == recId]
                _markEtlIud(cursorMaster, dbMaster, idrws, -1)
                conMaster.commit()
                recordErrors.append((recId, str(err)))
                # затронутые этой записью периоды — под подозрение (-1)
                failPeriods |= affected

        # зарегистрировать новые периоды (insert-side из ведущей) и обновить статус
        _registerNewPeriods(cursorMaster, dbMaster, ctx["selectSql"],
                            tableNameEtlJobs, cfg["periodColumn"])

        for period in okPeriods - failPeriods:
            cursorMaster.execute(
                _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
                {"LAST_SUCCESS_TS": ctx["currDt"],
                 "TABLENAME": tableNameEtlJobs,
                 "PERIOD": period},
            )
            UpdateLog(tableNameEtlJobs, dbMaster, action,
                      cursorMaster, conMaster, periodCount.get(period, 0),
                      period, 1)
        for period in failPeriods:
            cursorMaster.execute(
                _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
                {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
                 "PERIOD": period},
            )
            UpdateLog(tableNameEtlJobs, dbMaster, action,
                      cursorMaster, conMaster, 0, period)

        conMaster.commit()

        if recordErrors:
            sample = ", ".join(str(r[0]) for r in recordErrors[:5])
            more = f" (+{len(recordErrors) - 5} ещё)" if len(recordErrors) > 5 else ""
            raise RecordScopeError(
                f"delete_insert: {len(recordErrors)} idrw не перенесено: {sample}{more}. "
                f"Записи припаркованы (isetl=-1); линия продолжает работать. "
                f"Повторить ручкой: UPDATE etl_log_iud_row SET isetl=0 "
                f"WHERE tablename='{tableNameEtlJobs}' AND isetl=-1;"
            )

    finally:
        if conSlave:
            conSlave.close()


def _splitPkValue(value, pkCount):
    """id из etl_log_iud_row может быть составным — храним через '/'.

    В таблице id строки — текст; для составного PK склеиваем значения
    разделителем '/'. Здесь раскладываем обратно.
    """
    if pkCount == 1:
        return {"id0": value}
    parts = str(value).split("/")
    if len(parts) != pkCount:
        raise ValueError(
            f"Ожидалось {pkCount} компонент(ов) PK в '{value}', получили {len(parts)}"
        )
    return {f"id{i}": p for i, p in enumerate(parts)}


def _groupSummary(distinctIds):
    """[(period, count, 'ok')] — для последующего пересчёта статусов."""
    counts = {}
    for entry in distinctIds:
        period = entry[1]
        counts[period] = counts.get(period, 0) + 1
    return [[period, counts[period], "ok"] for period in sorted(counts.keys())]


def _markFailGroup(groupsData, period):
    for row in groupsData:
        if row[0] == period and row[2] == "ok":
            row[2] = "fail"
            break


# ----------------------------------------------------------------------------
#         Режим section_compare (mocheck / medree): сравнение master vs slave
# ----------------------------------------------------------------------------

def _selectMasterPeriods(cursor, dbMaster, selectSql, periodColumn):
    """Уникальные (createdate, max(lastupdate)) на стороне ведущей.

    periodColumn — имя колонки группировки в источнике (createdate / dcalc /
    reqdt и т.п.).
    """
    sqlTpl = _pickSql(dbMaster, dateSelectMasterPostSql, dateSelectMasterOrclSql)
    sql = sqlTpl.format(select_sql=selectSql, period_col=periodColumn)
    return _executeQuery(cursor, sql)


def _selectSlavePeriods(cursor, dbSlave, tableNameSlave,
                        slavePeriodColumn, filterClauseSlave):
    """Уникальные (createdate, max(lastupdate)) на стороне ведомой."""
    sqlTpl = _pickSql(dbSlave, dateSelectSlavePostSql, dateSelectSlaveOrclSql)
    filterSql = " AND ".join(filterClauseSlave or ["1=1"])
    sql = sqlTpl.format(
        tablename=tableNameSlave,
        period_col=slavePeriodColumn,
        filter=filterSql,
    )
    return _executeQuery(cursor, sql)


def _selectIudPeriods(cursor, dbMaster, tableNameEtlJobs):
    """Группы (createdate), для которых в etl_log_iud_row есть isetl=0."""
    sqlTpl = _pickSql(dbMaster, periodsFromIudPostSql, periodsFromIudOrclSql)
    rows = _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs})
    return [r[0] for r in rows]


def _maxIudIdrw(cursor, dbMaster, tableNameEtlJobs):
    """Граница idrw на момент старта — нужна, чтобы отметить как обработанные
    только те записи etl_log_iud_row, что были до начала переноса.
    Новые записи, появившиеся за время выполнения, останутся isetl=0
    и попадут в следующий запуск.
    """
    sql = (
        "SELECT COALESCE(MAX(idrw), 0) FROM koknaev.etl_log_iud_row "
        "WHERE tablename = "
        + _bindName(dbMaster, "tablename")
    )
    cursor.execute(sql, {"tablename": tableNameEtlJobs})
    return cursor.fetchone()[0]


def _diffPeriods(masterRows, slaveRows):
    """Группы, требующие обновления: либо нет на стороне ведомой, либо
    lastupdate на ведущей больше, либо отличаются.
    """
    #slaveByPeriod = {_normalizePeriod(row[0]): row[1] for row in slaveRows}
    #print('full slave', slaveByPeriod)
    def _bucket(rows):
        buckets = defaultdict(set)
        for period, lu in rows:
            buckets[_normalizePeriod(period)].add(lu)
        return buckets

    masterMap = _bucket(masterRows)
    slaveMap = _bucket(slaveRows)

    diff = []
    for period, masterSet in masterMap.items():
        if masterSet != slaveMap.get(period, set()):
            diff.append(period)
    #for period, masterUpd in masterRows:
    #    normPeriod = _normalizePeriod(period)
    #    slaveUpd = slaveByPeriod.get(normPeriod)
    #    print('DIFF ', normPeriod, ' slaveUpd ', slaveUpd, ' masterUpd ', masterUpd, ' slaveBy ', slaveByPeriod.get(normPeriod), ' append ? ', slaveUpd is None or masterUpd != slaveUpd)
    #    if slaveUpd is None or masterUpd != slaveUpd:
    #        diff.append(normPeriod)
    return diff


def _processSectionGroup(cfg, ctx, period, idrwBefore):
    """Полная перезаливка группы (createdate=period) с пометкой
    обработанных записей etl_log_iud_row.
    """
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    action = "ETL_SECTION_COMPARE"

    logger.info("section_compare: группа %s (%s)", period, tableNameEtlJobs)

    conSlave = None
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()

        # 1) удалить группу на стороне ведомой
        cursorSlave.execute(ctx["deletePeriodSql"], {"createdate": period})

        # 2) выбрать актуальные записи из ведущей и залить
        print('error 2 ', ctx["recordGroupSql"], tableNameEtlJobs, period, **(cfg.get("filterParams") or {}))
        cursorMaster.execute(
            ctx["recordGroupSql"],
            {"tablename": tableNameEtlJobs, "createdate": period,
             **(cfg.get("filterParams") or {})},
        )
        records = cursorMaster.fetchall()
        logger.info("Найдено %d записей для %s", len(records), period)
        _bulkUpsert(cursorSlave, dbSlave, ctx["upsertSql"],
                    ctx["structSlave"], records)

        # 3) пометить обработанными те записи журнала, что существовали
        # на момент старта (новые останутся для следующего запуска)
        markSql = _pickSql(dbMaster, markPeriodIudPostSql, markPeriodIudOrclSql)
        cursorMaster.execute(markSql, {
            "tablename": tableNameEtlJobs,
            "period": period,
            "idrwBefore": idrwBefore,
        })

        # 4) обновить etl_jobs.last_success_ts
        cursorMaster.execute(
            _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
            {"LAST_SUCCESS_TS": ctx["currDt"],
             "TABLENAME": tableNameEtlJobs,
             "PERIOD": period},
        )

        conMaster.commit()
        conSlave.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, len(records), period)
        logger.info("Группа %s обработана (section_compare)", period)
    except Exception as err:
        if conMaster:
            conMaster.rollback()
        if conSlave:
            conSlave.rollback()
        cursorMaster.execute(
            _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
            {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
             "PERIOD": period},
        )
        conMaster.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, 0, period)
        logger.error("Ошибка section_compare группы %s: %s", period, err)
        raise
    finally:
        if conSlave:
            conSlave.close()


def _runSectionCompare(cfg, ctx, selectSql):
    """Основной цикл section_compare: собрать группы из 3 источников
    (master vs slave diff, etl_log_iud_row, isokaudit=4) и обработать.
    """
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]

    idrwBefore = _maxIudIdrw(ctx["cursorMaster"], dbMaster, tableNameEtlJobs)

    # 1. master vs slave по (createdate, max(lastupdate))
    conSlave = _connect(dbSlave)
    try:
        cursorSlave = conSlave.cursor()
        slaveRows = _selectSlavePeriods(
            cursorSlave, dbSlave, cfg["tableNameSlave"],
            cfg["slavePeriodColumn"], cfg.get("filterClauseSlave"),
        )
    finally:
        conSlave.close()
    masterRows = _selectMasterPeriods(
        ctx["cursorMaster"], dbMaster, selectSql, cfg["periodColumn"],
    )

    needUpdate = set(_diffPeriods(masterRows, slaveRows))

    # 2. сигналы из etl_log_iud_row
    iudPeriods = _selectIudPeriods(ctx["cursorMaster"], dbMaster, tableNameEtlJobs)
    needUpdate.update(_normalizePeriod(p) for p in iudPeriods)

    # 3. явные группы с isokaudit=4 в etl_jobs
    sqlTpl = _pickSql(dbMaster, periodsIsokAudit4PostSql, periodsIsokAudit4OrclSql)
    explicit = _executeQuery(
        ctx["cursorMaster"], sqlTpl, {"tablename": tableNameEtlJobs},
    )
    needUpdate.update(_normalizePeriod(row[0]) for row in explicit)

    if not needUpdate:
        logger.info("section_compare %s: групп для обновления нет",
                    tableNameEtlJobs)
        raise AirflowSkipException

    logger.info("section_compare %s: %d групп для обновления: %s",
                tableNameEtlJobs, len(needUpdate), sorted(needUpdate))
    for period in sorted(needUpdate, key=lambda x: (x is None, x)):
        _processSectionGroup(cfg, ctx, period, idrwBefore)


# ----------------------------------------------------------------------------
#                                  Точка входа
# ----------------------------------------------------------------------------

def Do_etl(tableNameMaster, tableNameSlave=None, structureMaster=None,
           structureSlave=None, selectSql=None, dbMaster="Post",
           dbSlave="Post", tableNameEtlJobs=None, **overrides):
    """Универсальная точка входа.

    Поддерживает два варианта вызова:
      1. Со всеми параметрами явно (совместимо со старыми DAG'ами).
      2. С минимальным набором (tableNameMaster, dbMaster, dbSlave) — всё
         остальное берётся из config.json.

    Все необязательные опции (periodColumn, etlFields, filterClause,
    filterParams, conflictExtra, mode='iud'|'section') можно переопределить
    как именованные аргументы либо задать в config.json.
    """
    config = LoadConfig(_configKey(tableNameEtlJobs or tableNameMaster,
                                   dbMaster, dbSlave))
    config.setdefault("tableNameMaster", tableNameMaster)
    if tableNameSlave is not None:
        config["tableNameSlave"] = tableNameSlave
    if structureMaster is not None:
        config["structureMaster"] = structureMaster
    if structureSlave is not None:
        config["structureSlave"] = structureSlave
    if selectSql is not None:
        config["selectSql"] = selectSql
    config["dbMaster"] = dbMaster
    config["dbSlave"] = dbSlave
    config["tableNameEtlJobs"] = tableNameEtlJobs or config["tableNameMaster"] or tableNameMaster
    config.setdefault("periodColumn", "createdate")
    config.setdefault("slavePeriodColumn", config["periodColumn"])
    config.setdefault("mode", "iud")
    config.setdefault("etlFields", None)
    config.setdefault("filterClause", None)
    config.setdefault("filterClauseSlave", None)
    config.setdefault("filterParams", {})
    config.setdefault("conflictExtra", ())
    config.setdefault("conflictWhere", None)
    config.setdefault("truncatePeriod", False)
    config.update(overrides)
    print('caps name ', tableNameEtlJobs or config["tableNameMaster"] or tableNameMaster, tableNameEtlJobs,' or ', config["tableNameMaster"], ' or ', tableNameMaster)
    return _run(config)


def _configKey(tableName, dbMaster, dbSlave):
    return f"{tableName}{dbMaster}{dbSlave}"


def _run(cfg):
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameMaster = cfg["tableNameMaster"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]

    conMaster = _connect(dbMaster)
    cursorMaster = conMaster.cursor()
    try:
        # 0. Текущее серверное время для last_success_ts
        if _isPost(dbMaster):
            cursorMaster.execute("SELECT current_timestamp")
        else:
            cursorMaster.execute("SELECT CURRENT_DATE FROM dual")
        currDt = cursorMaster.fetchone()[0]
        logger.info("Текущее время БД: %s", currDt)

        # 1. Загрузка json-структур и приведение через etlFields
        structMasterFull = _loadStructure(cfg["structureMaster"], dbMaster)
        structSlaveFull = _loadStructure(cfg["structureSlave"], dbSlave)
        structMaster = _filterEtlFields(structMasterFull, cfg["etlFields"])
        structSlave = _filterEtlFields(structSlaveFull, cfg["etlFields"])
        if len(structMaster) != len(structSlave):
            logger.error(
                "Разные размеры json-структур %s и %s: %d vs %d",
                tableNameEtlJobs, cfg["tableNameSlave"],
                len(structMaster), len(structSlave),
            )
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "json")
            raise AirflowFailException(
                f"FLK: разные размеры json-структур ({len(structMaster)} vs "
                f"{len(structSlave)}) — линия заморожена, нужен человек"
            )

        # 2. Источник ведущей — таблица или sql
        if cfg.get("selectSql"):
            selectSql = TakeOneQuery(cfg["selectSql"])
        else:
            selectSql = structureEmptyQuerySql.format(tableNameMaster)
        # Дополнительный фильтр (например, doctype = 7) применяем к источнику.
        selectSql = _appendFilter(selectSql, cfg.get("filterClause"))

        # 3. Проверка структуры самого источника
        structCheck = (StructCheckPostgresQuery if _isPost(dbMaster)
                       else StructCheckOracleQuery)
        if not structCheck(structMaster, cursorMaster, selectSql):
            logger.error("Структура ведущего источника %s не совпадает",
                         tableNameEtlJobs)
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "ведущих")
            raise AirflowFailException(
                f"FLK: структура ведущего источника {tableNameEtlJobs} не совпадает"
            )

        # 4. Регистрация новых периодов
        _registerNewPeriods(cursorMaster, dbMaster, selectSql,
                            tableNameEtlJobs, cfg["periodColumn"])
        conMaster.commit()

        # 5. Подготовить контекст с готовыми SQL-шаблонами
        ctx = {
            "currDt": currDt,
            "conMaster": conMaster,
            "cursorMaster": cursorMaster,
            "selectSql": selectSql,
            "structMaster": structMaster,
            "structSlave": structSlave,
            "pkColsMaster": _primaryKeys(structMaster),
            "pkColsSlave": _primaryKeys(structSlave),
            "upsertSql": _buildUpsertSql(
                dbSlave, cfg["tableNameSlave"], structSlave,
                cfg.get("conflictExtra"),
                cfg.get("conflictWhere"),
                cfg.get("filterClauseSlave"),
                cfg.get("mode"),
            ),
            "deleteByIdSql": _buildDeleteByIdSql(
                dbSlave, cfg["tableNameSlave"],
                _primaryKeys(structSlave),
                cfg.get("filterClauseSlave"),
            ),
            "slavePeriodsByIdSql": _buildSlavePeriodsByIdSql(
                dbSlave, cfg["tableNameSlave"],
                _primaryKeys(structSlave),
                cfg["slavePeriodColumn"],
                cfg.get("filterClauseSlave"),
                cfg.get("truncatePeriod"),
            ),
            "deletePeriodSql": _buildDeletePeriodSql(
                dbSlave, cfg["tableNameSlave"],
                cfg["slavePeriodColumn"],
                cfg.get("filterClauseSlave"),
                cfg.get("truncatePeriod"),  # <-- ДОБАВЛЕНО
            ),
            "recordByIdSql": _buildRecordByIdSql(
                dbMaster, selectSql, structMaster,
                _primaryKeys(structMaster),
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
                cfg.get("truncatePeriod"),  # <-- ДОБАВЛЕНО
            ),
            "recordGroupSql": _buildRecordGroupSql(
                dbMaster, selectSql, structMaster,
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
                cfg.get("truncatePeriod"),  # <-- ДОБАВЛЕНО
            ),
        }

        # 6. Диспатч по режиму
        mode = cfg["mode"]
        if mode == "section_compare":
            # Универсальный режим срезов для mocheck / medree.
            _runSectionCompare(cfg, ctx, selectSql)
            return

        # 6a. Массовое обновление (isokaudit = 4) — обрабатывается всегда.
        groups = _selectGroupsForMassUpdate(
            cursorMaster, dbMaster, tableNameEtlJobs
        )
        if groups:
            logger.info("Группы для массового обновления: %s",
                        [g[0] for g in groups])
            for group in groups:
                _processGroupUpdate(cfg, ctx, group)
        else:
            logger.info("Групп с isokaudit=4 нет")

        if mode == "section":
            # Срезовый режим без сравнения с ведомой: только isokaudit=4.
            return

        # 7. Точечные обновления через etl_log_iud_row (mode='iud')
        iudRecords = _selectIudRecords(cursorMaster, dbMaster, tableNameEtlJobs)
        if not iudRecords:
            logger.info("В etl_log_iud_row для %s ничего нет", tableNameEtlJobs)
            if not groups:
                raise AirflowSkipException
            return
        distinctIds = _selectDistinctIds(
            cursorMaster, dbMaster, [r[1] for r in iudRecords],
        )
        if mode == "delete_insert":
            _processDeleteInsert(cfg, ctx, distinctIds, iudRecords)
        else:
            _processIndividualUpdates(cfg, ctx, distinctIds, iudRecords)
    except (AirflowSkipException, AirflowFailException, RecordScopeError):
        # пропускаем через — это специальные классы, runEtl их различает
        # (skip / fatal / record) и проставляет XCom error_class
        raise
    except Exception as err:
        # generic — runEtl классифицирует через classifyError(__cause__)
        raise AirflowException(f"Процесс остановлен, ошибка: {err}") from err
    finally:
        try:
            cursorMaster.close()
        finally:
            conMaster.close()
        logger.info("Соединение %s закрыто", dbMaster)
