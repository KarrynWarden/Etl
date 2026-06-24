"""
Универсальный аудит корректности переноса (Oracle / PostgreSQL).

Идея та же, что у do_etl: один процесс на все 4 направления и все таблицы,
параметры — из того же config.json по ключу tableNameEtlJobs+dbMaster+dbSlave.
Никаких doctype/iperson/medree в коде — всё, что отличается между таблицами,
вынесено в конфиг (filterClause, periodColumn, truncatePeriod, auditExcludeFields).

Логика
------
1. Берём из etl_jobs группы (period) этой линии, у которых isokaudit
   отличается от 1 (и не 4 — это зона ETL). См. GetBadAudit*.sql.
2. Для каждой группы:
   - выбираем записи за период из ведущего источника и из ведомой таблицы
     (только поля аудита: все перенесённые минус auditExcludeFields);
   - приводим типы к общему виду (Oracle NUMBER/DATE -> Postgres
     decimal/date) и сравниваем множества;
   - пишем результат в etl_jobs.isokaudit:
        1  — идентично;
       -4  — есть отличия / дубликаты;
       -2  — ошибка при проверке группы;
   - коммитим СРАЗУ после каждой группы. Это и есть защита от SIGTERM на
     5-м часу: при обрыве теряется только текущая группа, а следующий
     запуск добирает оставшиеся (isokaudit != 1) — отдельный костыль с
     двумя запусками в 17/18ч больше не нужен.

Особенность iperson (переносятся все поля, а сравнивать нужно не все)
решается опцией auditExcludeFields в config.json — список колонок,
которые аудит исключает из сравнения (join-поля из idoc/spdocper),
не влияя на перенос.
"""

from __future__ import annotations

import datetime
import decimal
import logging

from airflow.exceptions import (
    AirflowSkipException, AirflowException, AirflowFailException,
)

from Functions.functionsFile.loadConfig import LoadConfig
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Functions.functionsFile.structCheck import (
    StructCheckDataBase,
    StructCheckOracleQuery,
    StructCheckPostgresQuery,
)
from Functions.updateLog import UpdateLog
# Переиспользуем низкоуровневые утилиты ядра переноса — чтобы аудит и ETL
# одинаково подключались, фильтровали поля и строили списки колонок.
from Functions.do_etl import (
    _connect, _isPost, _pickSql, _loadStructure, _normalizePeriod,
    _resolveEtlPath,
    _appendFilter, _filterEtlFields, _bindName, _buildFieldsStr,
    _executeQuery, _configKey, classifyError, _asAndClause,
)
from Src.generalQueries import (
    structureCheckOrclSql,
    structureCheckPostSql,
    structureEmptyQuerySql,
    getBadAuditPostSql,
    getBadAuditOrclSql,
    auditRecordsMasterPostSql,
    auditRecordsMasterOrclSql,
    auditRecordsSlavePostSql,
    auditRecordsSlaveOrclSql,
    auditUpdatePostSql,
    auditUpdateOrclSql,
)

logger = logging.getLogger(__name__)

# Сколько отличающихся записей печатать в лог на группу.
_MAX_PRINT_DIFF = 100

# Граничная дата-сентинел для COALESCE: совпадает с тем, что в .sql.
_SENTINEL = "TO_DATE('1900-01-01', 'YYYY-MM-DD')"


class AuditScopeError(AirflowException):
    """Аудит отработал, но НЕ все группы идентичны: есть отличия (-4) и/или
    группы, которые не удалось проверить из-за per-group ошибки данных (-2).

    Статусы по группам уже записаны в etl_jobs (коммит per-group). Этот
    эксепшен лишь красит запуск 🟥 для видимости — линию НЕ морозит (аналог
    RecordScopeError в ETL). Зелёным аудит становится только когда все
    проверенные группы вернули 1. Подхватывается в runAudit:
    XCom error_class='record'.
    """
    pass


# ----------------------------------------------------------------------------
#                       Поля, участвующие в сравнении
# ----------------------------------------------------------------------------

def _asNameSet(value):
    """auditExcludeFields: принять строку 'a, b' ИЛИ список ['a', 'b'] и
    вернуть множество имён колонок в нижнем регистре. Канон — строка через
    запятую; список тоже принимается (легаси). Пусто -> пустое множество."""
    if not value:
        return set()
    if isinstance(value, str):
        value = value.split(",")
    return {f.strip().lower() for f in value if f.strip()}


def _auditFields(jsonStructFull, etlFields, auditExcludeFields):
    """Поля для сравнения = перенесённые поля минус auditExcludeFields.

    PK всегда остаётся (без него строки не сопоставить). Порядок колонок
    сохраняется — ведущая и ведомая структуры сопоставляются по позиции,
    как и в ETL.
    """
    fields = _filterEtlFields(jsonStructFull, etlFields)
    excluded = _asNameSet(auditExcludeFields)
    if not excluded:
        return fields
    return [f for f in fields
            if f[0].lower() not in excluded or f[3] == "Primary Key"]


# ----------------------------------------------------------------------------
#                       Построение SQL выборок за период
# ----------------------------------------------------------------------------

def _periodCond(dbType, periodColumn, truncatePeriod):
    """Условие WHERE по периоду с плейсхолдером :createdate / %(createdate)s.

    truncatePeriod=True (medree: dcalc содержит время) — сравниваем по дате.
    COALESCE с сентинелом обрабатывает группы с period IS NULL.
    """
    if truncatePeriod:
        expr = f"DATE(p.{periodColumn})" if _isPost(dbType) else f"TRUNC(p.{periodColumn})"
    else:
        expr = f"p.{periodColumn}"
    bind = _bindName(dbType, "createdate")
    return (f"COALESCE({expr}, {_SENTINEL}) = COALESCE({bind}, {_SENTINEL})")


def _buildMasterSql(cfg, selectSql, structMaster):
    fieldsStr = _buildFieldsStr(cfg["dbMaster"], structMaster, cfg["periodColumn"])
    cond = _periodCond(cfg["dbMaster"], cfg["periodColumn"], cfg["truncatePeriod"])
    tpl = _pickSql(cfg["dbMaster"], auditRecordsMasterPostSql, auditRecordsMasterOrclSql)
    return tpl.format(fields_str=fieldsStr, select_sql=selectSql, period_cond=cond)


def _buildSlaveSql(cfg, structSlave):
    fieldsStr = _buildFieldsStr(cfg["dbSlave"], structSlave, cfg["slavePeriodColumn"])
    cond = _periodCond(cfg["dbSlave"], cfg["slavePeriodColumn"], cfg["truncatePeriod"])
    # filterClauseSlave (тот же doctype-срез, что и при переносе) — на ведомую.
    # В скобках на случай OR внутри (склеиваем с условием по периоду через AND).
    filterClauseSlave = _asAndClause(cfg.get("filterClauseSlave"))
    if filterClauseSlave:
        cond += f" AND ({filterClauseSlave})"
    tpl = _pickSql(cfg["dbSlave"], auditRecordsSlavePostSql, auditRecordsSlaveOrclSql)
    return tpl.format(fields_str=fieldsStr, tablename=cfg["tableNameSlave"],
                      period_cond=cond)


# ----------------------------------------------------------------------------
#                   Приведение типов и сравнение множеств
# ----------------------------------------------------------------------------

def _canonicalizeOracleSet(rows, structOrcl, structPost):
    """Привести Oracle-строки к представлению Postgres, чтобы множества
    можно было сравнивать на равенство.

    У Oracle числа — float, у Postgres — Decimal; даты Oracle — datetime,
    у Postgres pure-date колонки — date. Без приведения set1 != set2 даже
    при идентичных данных. structOrcl и structPost идут параллельно
    (одна и та же логическая колонка по позиции).
    """
    result = set()
    for row in rows:
        converted = []
        for value, fOrcl, fPost in zip(row, structOrcl, structPost):
            typeOrcl = (fOrcl[1] or "").upper()
            scaleOrcl = fOrcl[2]
            typePost = (fPost[1] or "").lower()
            scalePost = fPost[2]
            if value is None:
                converted.append(None)
            elif typeOrcl == "NUMBER":
                converted.append(_quantize(value, scaleOrcl))
            elif typeOrcl in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR") \
                    and "numeric" in typePost:
                # На стороне Oracle строка, на стороне Postgres число.
                converted.append(_quantize(value, scalePost))
            elif typeOrcl == "DATE" and typePost == "date":
                # Чистая дата на обеих сторонах — сравниваем по дате.
                converted.append(value.date() if hasattr(value, "date") else value)
            else:
                converted.append(value)
        result.add(tuple(converted))
    return result


def _quantize(value, scale):
    """Decimal с фиксированным числом знаков после запятой (как в Postgres)."""
    template = "0" if not scale else "0." + "0" * int(scale)
    return decimal.Decimal(str(value)).quantize(decimal.Decimal(template))


def _toComparable(dbMaster, dbSlave, masterRows, slaveRows,
                  structMaster, structSlave):
    """Множества записей ведущей и ведомой, приведённые к общему виду.

    Приводим ту сторону, что на Oracle; сторона Postgres — эталон.
    Для одинаковых СУБД (Post->Post / Orcl->Orcl) приведение не нужно.
    """
    if dbMaster == "Orcl" and dbSlave == "Post":
        return (_canonicalizeOracleSet(masterRows, structMaster, structSlave),
                set(slaveRows))
    if dbMaster == "Post" and dbSlave == "Orcl":
        return (set(masterRows),
                _canonicalizeOracleSet(slaveRows, structSlave, structMaster))
    return set(masterRows), set(slaveRows)


def _printSample(prefix, rows):
    shown = 0
    for row in rows:
        if shown >= _MAX_PRINT_DIFF:
            logger.warning("  %s: показаны первые %d, всего %d",
                           prefix, _MAX_PRINT_DIFF, len(rows))
            break
        logger.warning("  %s: %s", prefix, row)
        shown += 1


def _evaluate(period, tableNameEtlJobs, tableNameSlave,
              masterRows, slaveRows, set1, set2):
    """Вернуть статус аудита группы: 1 (идентично) или -4 (отличия)."""
    if set1 == set2:
        if len(masterRows) == len(slaveRows) == len(set1):
            logger.info("Период %s: %s идентична (%d строк)",
                        period, tableNameEtlJobs, len(set1))
            return 1
        # множества равны, но есть дубликаты с одной из сторон
        if len(masterRows) > len(set1):
            logger.warning("Период %s: дубликаты в ведущей %s (%d против %d)",
                           period, tableNameEtlJobs, len(masterRows), len(set1))
        if len(slaveRows) > len(set2):
            logger.warning("Период %s: дубликаты в ведомой %s (%d против %d)",
                           period, tableNameSlave, len(slaveRows), len(set2))
        return -4

    onlyMaster = set1 - set2
    onlySlave = set2 - set1
    logger.warning(
        "Период %s: %s ОТЛИЧАЕТСЯ — только в ведущей %d, только в ведомой %d",
        period, tableNameEtlJobs, len(onlyMaster), len(onlySlave),
    )
    _printSample(f"только в ведущей {tableNameEtlJobs}", onlyMaster)
    _printSample(f"только в ведомой {tableNameSlave}", onlySlave)
    return -4


# ----------------------------------------------------------------------------
#                       Выбор групп и проверка одной группы
# ----------------------------------------------------------------------------

def _selectGroupsToAudit(cursor, dbMaster, tableNameEtlJobs):
    sqlTpl = _pickSql(dbMaster, getBadAuditPostSql, getBadAuditOrclSql)
    return _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs})


def _writeStatus(cfg, ctx, origPeriod, status):
    cursorMaster = ctx["cursorMaster"]
    cursorMaster.execute(
        _pickSql(cfg["dbMaster"], auditUpdatePostSql, auditUpdateOrclSql),
        {"ISOKAUDIT": status,
         "TABLENAME": cfg["tableNameEtlJobs"],
         "PERIOD": origPeriod},
    )


def _auditGroup(cfg, ctx, origPeriod, report):
    """Проверить одну группу (period) и записать результат. Коммит — здесь же,
    чтобы прерывание (SIGTERM) не откатывало уже проверенные группы.

    report — аккумулятор с ключами 'differing' и 'errored' (списки периодов),
    по которым _run в конце решает цвет квадрата. Системные ошибки
    (retryable/fatal — мёртвое соединение, битый SQL/структура) НЕ глотаем
    по группам: они одинаково сломают все группы, поэтому пробрасываем наверх,
    чтобы прогон сразу упал и ушёл в ретрай/заморозку. Глотаем (ставим -2 и
    идём дальше) только record-class — порча данных в конкретной группе."""
    dbMaster = cfg["dbMaster"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    tableNameSlave = cfg["tableNameSlave"]
    cursorMaster = ctx["cursorMaster"]
    cursorSlave = ctx["cursorSlave"]
    conMaster = ctx["conMaster"]
    conSlave = ctx["conSlave"]
    filterParams = cfg.get("filterParams") or {}
    period = _normalizePeriod(origPeriod)

    try:
        # filterParams нужны только ведущему источнику (selectSql); у ведомой
        # срез задан литеральным filterClauseSlave, лишние бинды Oracle не любит.
        cursorMaster.execute(ctx["masterSql"],
                             {"createdate": period, **filterParams})
        masterRows = cursorMaster.fetchall()
        cursorSlave.execute(ctx["slaveSql"], {"createdate": period})
        slaveRows = cursorSlave.fetchall()

        set1, set2 = _toComparable(dbMaster, cfg["dbSlave"], masterRows,
                                   slaveRows, ctx["structMaster"],
                                   ctx["structSlave"])
        status = _evaluate(period, tableNameEtlJobs, tableNameSlave,
                           masterRows, slaveRows, set1, set2)
        if status != 1:
            report["differing"].append(origPeriod)

        _writeStatus(cfg, ctx, origPeriod, status)
        UpdateLog(tableNameEtlJobs, dbMaster, "Audit",
                  cursorMaster, conMaster, len(set1), origPeriod, status)
        conMaster.commit()
    except Exception as err:
        _safeRollback(conMaster)
        _safeRollback(conSlave)
        # Системная ошибка — прерываем весь прогон, не плодим -2 по всем группам.
        if classifyError(err) in ("retryable", "fatal"):
            logger.error("Системная ошибка аудита на группе %s (%s) — "
                         "прерываю прогон: %s", origPeriod, tableNameEtlJobs, err)
            raise
        # record/неизвестный класс — порча данных в этой группе: -2 и дальше.
        logger.error("Ошибка данных группы %s (%s), помечаю -2: %s",
                     origPeriod, tableNameEtlJobs, err)
        try:
            _writeStatus(cfg, ctx, origPeriod, -2)
            UpdateLog(tableNameEtlJobs, dbMaster, "Audit",
                      cursorMaster, conMaster, 0, origPeriod, -2)
            conMaster.commit()
        except Exception as markErr:
            # не смогли даже записать -2 — это уже системно, прерываем прогон
            _safeRollback(conMaster)
            logger.error("Не удалось пометить группу %s как -2: %s",
                         origPeriod, markErr)
            raise
        report["errored"].append(origPeriod)


def _safeRollback(con):
    try:
        if con is not None:
            con.rollback()
    except Exception:
        pass


# ----------------------------------------------------------------------------
#                                  Точка входа
# ----------------------------------------------------------------------------

def Do_audit(tableNameMaster, dbMaster="Post", dbSlave="Post",
             tableNameEtlJobs=None, **overrides):
    """Универсальная точка входа аудита. Сигнатура повторяет Do_etl —
    параметры берутся из того же config.json по ключу
    tableNameEtlJobs+dbMaster+dbSlave."""
    config = LoadConfig(_configKey(tableNameEtlJobs or tableNameMaster,
                                   dbMaster, dbSlave))
    config.setdefault("tableNameMaster", tableNameMaster)
    config["dbMaster"] = dbMaster
    config["dbSlave"] = dbSlave
    config["tableNameEtlJobs"] = (tableNameEtlJobs or config["tableNameMaster"]
                                  or tableNameMaster)
    config.setdefault("periodColumn", "createdate")
    config.setdefault("slavePeriodColumn", config["periodColumn"])
    config.setdefault("etlFields", None)
    config.setdefault("auditExcludeFields", None)
    config.setdefault("filterClause", None)
    config.setdefault("filterClauseSlave", None)
    config.setdefault("filterParams", {})
    config.setdefault("truncatePeriod", False)
    config.update(overrides)
    return _run(config)


def _run(cfg):
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameMaster = cfg["tableNameMaster"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    tableNameSlave = cfg["tableNameSlave"]

    conMaster = _connect(dbMaster)
    conSlave = _connect(dbSlave)
    cursorMaster = conMaster.cursor()
    cursorSlave = conSlave.cursor()
    try:
        # 1. Структуры + поля аудита (перенесённые минус auditExcludeFields).
        structMasterFull = _loadStructure(cfg["structureMaster"], dbMaster)
        structSlaveFull = _loadStructure(cfg["structureSlave"], dbSlave)
        structMaster = _auditFields(structMasterFull, cfg["etlFields"],
                                    cfg["auditExcludeFields"])
        structSlave = _auditFields(structSlaveFull, cfg["etlFields"],
                                   cfg["auditExcludeFields"])
        if len(structMaster) != len(structSlave):
            logger.error("Разные размеры полей аудита %s и %s: %d vs %d",
                         tableNameEtlJobs, tableNameSlave,
                         len(structMaster), len(structSlave))
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "json")
            raise AirflowFailException(
                f"FLK: разные размеры полей аудита ({len(structMaster)} vs "
                f"{len(structSlave)}) — нужен человек"
            )

        # 2. Источник ведущей — sql или таблица; doctype-фильтр на источник.
        if cfg.get("selectSql"):
            selectSql = TakeOneQuery(_resolveEtlPath(cfg["selectSql"]))
        else:
            selectSql = structureEmptyQuerySql.format(tableNameMaster)
        selectSql = _appendFilter(selectSql, cfg.get("filterClause"))

        # 3. Однократная проверка структур (за период не меняются).
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
        if not StructCheckDataBase(
            structSlave, cursorSlave,
            _pickSql(dbSlave, structureCheckPostSql, structureCheckOrclSql),
            tableNameSlave,
        ):
            logger.error("Структура ведомой %s не совпадает", tableNameSlave)
            UpdateLog(tableNameEtlJobs, dbMaster, "FLK",
                      cursorMaster, conMaster, "ведомых")
            raise AirflowFailException(
                f"FLK: структура ведомой {tableNameSlave} не совпадает"
            )

        # 4. Контекст с готовыми SQL.
        ctx = {
            "conMaster": conMaster,
            "conSlave": conSlave,
            "cursorMaster": cursorMaster,
            "cursorSlave": cursorSlave,
            "structMaster": structMaster,
            "structSlave": structSlave,
            "masterSql": _buildMasterSql(cfg, selectSql, structMaster),
            "slaveSql": _buildSlaveSql(cfg, structSlave),
        }

        # 5. Группы на проверку.
        groups = _selectGroupsToAudit(cursorMaster, dbMaster, tableNameEtlJobs)
        if not groups:
            logger.info("Аудит %s: групп для проверки нет", tableNameEtlJobs)
            raise AirflowSkipException

        logger.info("Аудит %s: %d групп на проверку", tableNameEtlJobs, len(groups))
        report = {"differing": [], "errored": []}
        for tablename, period in groups:
            _auditGroup(cfg, ctx, period, report)

        differing, errored = report["differing"], report["errored"]
        if differing:
            logger.warning("Аудит %s: отличия в %d группах: %s",
                           tableNameEtlJobs, len(differing), differing)
        if errored:
            logger.error("Аудит %s: не проверено %d групп (ошибки данных, -2): %s",
                         tableNameEtlJobs, len(errored), errored)
        # Зелёным аудит становится ТОЛЬКО когда все проверенные группы — 1.
        # Любые -4/-2 → 🟥 (без заморозки линии), статусы уже в etl_jobs.
        if differing or errored:
            raise AuditScopeError(
                f"Аудит {tableNameEtlJobs}: отличий {len(differing)}, "
                f"не проверено {len(errored)} (см. etl_jobs.isokaudit)"
            )
        logger.info("Аудит %s: все %d групп идентичны", tableNameEtlJobs, len(groups))
    except (AirflowSkipException, AirflowFailException, AuditScopeError):
        raise
    except Exception as err:
        raise AirflowException(f"Аудит остановлен, ошибка: {err}") from err
    finally:
        for cur in (cursorMaster, cursorSlave):
            try:
                cur.close()
            except Exception:
                pass
        for con in (conMaster, conSlave):
            try:
                con.close()
            except Exception:
                pass
        logger.info("Соединения аудита (%s/%s) закрыты", dbMaster, dbSlave)
