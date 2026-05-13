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

from airflow.exceptions import AirflowSkipException, AirflowException

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
                    conflictExtra, filterClauseSlave):
    """Сформировать запрос upsert (insert .. on conflict / merge)."""
    columns = _columnNames(structSlave)
    pkCols = _primaryKeys(structSlave)

    if _isPost(dbSlave):
        primaryStr = ", ".join(pkCols + list(conflictExtra or ()))
        columnsStr = ", ".join(columns)
        valuesStr = ", ".join(["%s"] * len(columns))
        updateStr = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
        # Частичный уникальный индекс (например, mocheck.doctype = 7) ловится
        # как (pk, doctype) в on conflict.
        return upsertPostSql.format(
            tablename=tableNameSlave,
            columns_str=columnsStr,
            values_str=valuesStr,
            update_str=updateStr,
            primary_str=primaryStr,
        )

    # Oracle: MERGE INTO ... USING dual
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


def _buildDeletePeriodSql(dbSlave, tableNameSlave, slavePeriodColumn,
                          filterClauseSlave):
    cond = f"{slavePeriodColumn} = " + _bindName(dbSlave, "createdate")
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
                        periodColumn, filterClauseMaster):
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
                         filterClauseMaster):
    fieldsStr = _buildFieldsStr(dbMaster, structMaster, periodColumn)
    cond = f"p.{periodColumn} = " + _bindName(dbMaster, "createdate")
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
    cursor.execute(
        sqlTpl.format(sourceFromClause, periodColumn),
        {
            "tablename": tableNameEtlJobs,
            "tablenamelow": tableNameEtlJobs.lower(),
        },
    )


def _selectGroupsForMassUpdate(cursor, dbMaster, tableNameMaster,
                               tableNameEtlJobs, periodColumn):
    """Группы, которые требуют полного обновления (isokaudit = 4)."""
    sqlTpl = _pickSql(dbMaster, dateSelectStatusPostSql, dateSelectStatusOrclSql)
    return _executeQuery(
        cursor,
        sqlTpl.format(tableNameMaster, periodColumn),
        {"tablename": tableNameEtlJobs},
    )


def _selectIudRecords(cursor, dbMaster, tableNameEtlJobs):
    sqlTpl = _pickSql(dbMaster, selectEtlIudPostSql, selectEtlIudOrclSql)
    return _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs})


def _selectDistinctIds(cursor, dbMaster, idLogs):
    """По списку idrw из etl_log_iud_row — уникальные (id, period, oper)."""
    sqlTpl = _pickSql(dbMaster, selectDistinctPostSql, selectDistinctOrclSql)
    return _executeQuery(cursor, sqlTpl, {"idlogs": idLogs})


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
            return

        # isetl: 0 -> 1 в etl_log_iud_row для уже зафиксированных записей
        # этой группы — они будут перезалиты вместе со всей группой.
        cursorMaster.execute(
            _pickSql(dbMaster, etlStatusChangePostSql, etlStatusChangeOrclSql),
            {"tablename": tableNameEtlJobs, "createdate": createdate},
        )

        # 1) удалить все записи группы из ведомой
        cursorSlave.execute(ctx["deletePeriodSql"], {"createdate": createdate})

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

    distinctIds: [(id, period, timeoper, oper), ...] — уникальные строки
                  с последней актуальной операцией.
    iudRecords:  [(id, idrw), ...] — все исходные записи журнала.
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
            return

        for entry in distinctIds:
            recId, period, _timeoper, oper = entry[0], entry[1], entry[2], entry[3]
            try:
                # параметры для PK (одно или несколько полей, разделённых ',')
                pkParams = _splitPkValue(recId, len(ctx["pkColsMaster"]))

                if oper == "IU":
                    cursorMaster.execute(ctx["recordByIdSql"],
                                         {**pkParams,
                                          **(cfg.get("filterParams") or {})})
                    rowDb = cursorMaster.fetchone()
                    if rowDb is None:
                        logger.warning(
                            "Запись id=%s в ведущей не найдена — пропуск",
                            recId,
                        )
                    else:
                        if _isPost(dbSlave):
                            cursorSlave.execute(ctx["upsertSql"], rowDb)
                        else:
                            params = {str(i + 1): v for i, v in enumerate(rowDb)}
                            cursorSlave.execute(ctx["upsertSql"], params)
                else:  # 'D'
                    cursorSlave.execute(ctx["deleteByIdSql"], pkParams)

                conSlave.commit()
                # отметить пакет idrw как обработанные
                idrws = [b for a, b in iudRecords if a == recId]
                cursorMaster.execute(
                    _pickSql(dbMaster, etlIdUpdatePostSql, etlIdUpdateOrclSql),
                    {"isetl": 1, "idrws": idrws},
                )
                conMaster.commit()
            except Exception as err:
                logger.error("Ошибка по id=%s: %s", recId, err)
                if conSlave:
                    conSlave.rollback()
                idrws = [b for a, b in iudRecords if a == recId]
                cursorMaster.execute(
                    _pickSql(dbMaster, etlIdUpdatePostSql, etlIdUpdateOrclSql),
                    {"isetl": -1, "idrws": idrws},
                )
                conMaster.commit()
                _markFailGroup(groupsData, period)

        # перепроверить новые createdate, которые могли появиться, и
        # выставить статус группам без ошибок
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
        "SELECT COALESCE(MAX(idrw), 0) FROM etl_log_iud_row "
        "WHERE tablename = "
        + _bindName(dbMaster, "tablename")
    )
    cursor.execute(sql, {"tablename": tableNameEtlJobs})
    return cursor.fetchone()[0]


def _diffPeriods(masterRows, slaveRows):
    """Группы, требующие обновления: либо нет на стороне ведомой, либо
    lastupdate на ведущей больше, либо отличаются.
    """
    slaveByPeriod = {row[0]: row[1] for row in slaveRows}
    diff = []
    for period, masterUpd in masterRows:
        slaveUpd = slaveByPeriod.get(period)
        if slaveUpd is None or masterUpd != slaveUpd:
            diff.append(period)
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
    needUpdate.update(iudPeriods)

    # 3. явные группы с isokaudit=4 в etl_jobs
    sqlTpl = _pickSql(dbMaster, periodsIsokAudit4PostSql, periodsIsokAudit4OrclSql)
    explicit = _executeQuery(
        ctx["cursorMaster"], sqlTpl, {"tablename": tableNameEtlJobs},
    )
    needUpdate.update(row[0] for row in explicit)

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
    config["tableNameEtlJobs"] = tableNameEtlJobs or tableNameMaster
    config.setdefault("periodColumn", "createdate")
    config.setdefault("slavePeriodColumn", config["periodColumn"])
    config.setdefault("mode", "iud")
    config.setdefault("etlFields", None)
    config.setdefault("filterClause", None)
    config.setdefault("filterClauseSlave", None)
    config.setdefault("filterParams", {})
    config.setdefault("conflictExtra", ())
    config.update(overrides)

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
            return

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
            return

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
                cfg.get("filterClauseSlave"),
            ),
            "deleteByIdSql": _buildDeleteByIdSql(
                dbSlave, cfg["tableNameSlave"],
                _primaryKeys(structSlave),
                cfg.get("filterClauseSlave"),
            ),
            "deletePeriodSql": _buildDeletePeriodSql(
                dbSlave, cfg["tableNameSlave"],
                cfg["slavePeriodColumn"],
                cfg.get("filterClauseSlave"),
            ),
            "recordByIdSql": _buildRecordByIdSql(
                dbMaster, selectSql, structMaster,
                _primaryKeys(structMaster),
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
            ),
            "recordGroupSql": _buildRecordGroupSql(
                dbMaster, selectSql, structMaster,
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
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
            cursorMaster, dbMaster, tableNameMaster, tableNameEtlJobs,
            cfg["periodColumn"],
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
        _processIndividualUpdates(cfg, ctx, distinctIds, iudRecords)
    except AirflowSkipException:
        raise
    except Exception as err:
        raise AirflowException(f"Процесс остановлен, ошибка: {err}") from err
    finally:
        try:
            cursorMaster.close()
        finally:
            conMaster.close()
        logger.info("Соединение %s закрыто", dbMaster)
