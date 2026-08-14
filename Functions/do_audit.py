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

Что печатается
--------------
* по группе — строка с результатом, числом строк с каждой стороны и
  РАЗБИВКОЙ ВРЕМЕНИ по фазам (запрос/выборка каждой стороны, приведение
  типов, сравнение, запись статуса); в конце линии — сумма по фазам.
  Это диагностика «почему аудит идёт долго»: фаза, которая не зависит от
  размера группы, и есть фиксированная плата за группу
  (см. README, «Аудит идёт долго: с чего начать», и tools/audit_profile.py);
* при расхождении — записи, СОПОСТАВЛЕННЫЕ ПО ПЕРВИЧНОМУ КЛЮЧУ: для общего
  ключа перечисляются расходящиеся колонки по именам, для ключа с одной
  стороны так и сказано, что записи на другой стороне нет (см. _diffLines).
"""

from __future__ import annotations

import datetime
import decimal
import logging
import time

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
    _connect, _isPost, _pickSql, _loadStructure, _periodKey,
    _resolveEtlPath, ShutdownRequested, isShutdown,
    _appendFilter, _filterEtlFields, _buildFieldsStr,
    _executeQuery, _configKey, classifyError, _asAndClause,
    _periodSpec, _periodBinds, _periodBind,
    _periodSqlPair, _pickPeriodSql, logStatements,
    _phaseTimer, _newPhases, _addPhases, _phasesText, _phasesTotal,
    _periodCond as _etlPeriodCond,
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
    #excluded = {f.lower() for f in auditExcludeFields}
    return [f for f in fields
            if f[0].lower() not in excluded or f[3] == "Primary Key"]


# ----------------------------------------------------------------------------
#                       Построение SQL выборок за период
# ----------------------------------------------------------------------------

def _periodCond(dbType, periodColumn, truncatePeriod, nullGroup=False):
    """Условие WHERE по периоду с плейсхолдером :createdate / %(createdate)s.

    Период — свойство таблицы, а не режима (см. README): одна колонка-дата
    ЛИБО составной {"year": ..., "month": ...}. Строит его общая сборка из
    do_etl — те же условия и те же бинды, что и при переносе, включая
    отдельный вариант для NULL-группы (`колонка IS NULL`, без бинда).

    Своей копии условия здесь больше нет. Она появилась ради одной строчки
    (truncatePeriod для одиночной колонки), а стоила того, что правка
    sargable-условия в do_etl обошла бы аудит стороной — при том, что именно
    на аудите скан и был замечен.
    """
    return _etlPeriodCond(dbType, _periodSpec(periodColumn), "p", "createdate",
                          truncatePeriod, nullGroup)


def _buildMasterSql(cfg, selectSql, structMaster, nullGroup=False):
    fieldsStr = _buildFieldsStr(cfg["dbMaster"], structMaster, cfg["periodColumn"])
    cond = _periodCond(cfg["dbMaster"], cfg["periodColumn"], cfg["truncatePeriod"],
                       nullGroup)
    tpl = _pickSql(cfg["dbMaster"], auditRecordsMasterPostSql, auditRecordsMasterOrclSql)
    return tpl.format(fields_str=fieldsStr, select_sql=selectSql, period_cond=cond)


def _buildSlaveSql(cfg, structSlave, nullGroup=False):
    fieldsStr = _buildFieldsStr(cfg["dbSlave"], structSlave, cfg["slavePeriodColumn"])
    cond = _periodCond(cfg["dbSlave"], cfg["slavePeriodColumn"],
                       cfg["truncatePeriod"], nullGroup)
    # filterClauseSlave (тот же doctype-срез, что и при переносе) — на ведомую.
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


# ----------------------------------------------------------------------------
#            Отчёт о расхождениях: по первичному ключу, с именами колонок
# ----------------------------------------------------------------------------
# Раньше печатались просто две пачки строк-кортежей: 100 «только в ведущей» и
# 100 «только в ведомой», в произвольном (set) порядке и без единой подписи.
# А типовое расхождение — не «строки нет», а «строка есть, но обновилась
# неправильно»: тогда ОДНА и та же запись лежит в обеих пачках, и найти, каким
# полем они отличаются, можно было только глазами по длинному кортежу.
# Поэтому расхождения сопоставляются по первичному ключу и печатаются по одной
# записи на ключ: что за ключ, каких колонок касается расхождение и что в них с
# каждой стороны. Ключ, которого нет на другой стороне, так и называется.
#
# Сам ВЕРДИКТ группы этот отчёт не трогает: статус по-прежнему решает равенство
# множеств (см. _evaluate) — здесь только печать.

def _pkPositions(struct):
    """Позиции колонок первичного ключа в структуре аудита."""
    return [i for i, f in enumerate(struct) if f[3] == "Primary Key"]


def _fmt(value):
    """Значение для лога. Строки — через repr: иначе не видно ни хвостовых
    пробелов (частая причина расхождения CHAR/VARCHAR), ни пустой строки."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return repr(value)
    return str(value)


def _sortableKey(key):
    """Ключ сортировки PK, устойчивый к смеси типов и None.

    sorted() по сырым значениям падает с TypeError, стоит в ключе оказаться
    None или разнотипным значениям (Decimal и str в составном ключе). Здесь
    каждое значение превращается в тройку (разряд типа, число, текст) —
    сравнимую с любой другой, а порядок внутри одного типа остаётся
    естественным (числа как числа, а не как строки)."""
    out = []
    for value in key:
        if value is None:
            out.append((0, 0.0, ""))
        elif isinstance(value, bool):
            out.append((1, float(value), ""))
        elif isinstance(value, (int, float, decimal.Decimal)):
            out.append((1, float(value), ""))
        elif isinstance(value, (datetime.datetime, datetime.date)):
            out.append((2, 0.0, value.isoformat()))
        else:
            out.append((3, 0.0, str(value)))
    return out


def _colLabel(structMaster, structSlave, i):
    """Имя колонки для отчёта: 'IDRW' либо 'IDRW/idrw', если имена сторон
    различаются не только регистром (в ведущей-запросе это псевдоним)."""
    nameMaster = str(structMaster[i][0])
    nameSlave = str(structSlave[i][0]) if i < len(structSlave) else ""
    if nameSlave and nameSlave.lower() != nameMaster.lower():
        return f"{nameMaster}/{nameSlave}"
    return nameMaster


def _keyText(struct, pkPos, key):
    return ", ".join(f"{struct[i][0]}={_fmt(v)}" for i, v in zip(pkPos, key))


def _rowsByKey(rows, pkPos, wanted):
    """{ключ: [строки]} — ТОЛЬКО для ключей из wanted.

    Ограничение по wanted не косметическое: расхождение бывает и на всю
    группу (сотни тысяч строк с каждой стороны), а индекс нужен не по всем
    ключам — см. _diffLines, где wanted собирается из изменённых ключей и
    печатаемых."""
    out = {}
    for row in rows:
        key = tuple(row[i] for i in pkPos)
        if key in wanted:
            out.setdefault(key, []).append(row)
    return out


def _rowDiff(rowMaster, rowSlave, structMaster, structSlave, pkPos):
    """[(имя колонки, значение ведущей, значение ведомой)] по расходящимся полям."""
    skip = set(pkPos)
    return [(_colLabel(structMaster, structSlave, i), a, b)
            for i, (a, b) in enumerate(zip(rowMaster, rowSlave))
            if i not in skip and a != b]


def _diffLines(onlyMaster, onlySlave, structMaster, structSlave,
               nameMaster, nameSlave, limit=_MAX_PRINT_DIFF):
    """Строки отчёта о расхождениях группы (список, для лога).

    onlyMaster/onlySlave — уже приведённые к общему виду записи, которых нет
    на другой стороне (set1 - set2 и set2 - set1).
    """
    pkPos = _pkPositions(structMaster)
    if not pkPos:
        # Сопоставлять нечем (в полях аудита нет PK — так у medree). Печатаем
        # как раньше: две пачки строк, но хотя бы с шапкой из имён колонок.
        header = ", ".join(str(f[0]) for f in structMaster)
        lines = [f"колонки: {header}"]
        for prefix, rows in ((f"только в ведущей {nameMaster}", onlyMaster),
                             (f"только в ведомой {nameSlave}", onlySlave)):
            for row in list(rows)[:limit]:
                lines.append(f"{prefix}: {row}")
            if len(rows) > limit:
                lines.append(f"{prefix}: показаны первые {limit} из {len(rows)}")
        return lines

    keysMaster = {tuple(row[i] for i in pkPos) for row in onlyMaster}
    keysSlave = {tuple(row[i] for i in pkPos) for row in onlySlave}
    allKeys = sorted(keysMaster | keysSlave, key=_sortableKey)
    shown = allKeys[:limit]
    both = keysMaster & keysSlave
    # Индекс нужен по изменённым ключам (для перечня колонок — он считается по
    # ВСЕМ таким ключам) и по печатаемым (для подробностей). Односторонние
    # ключи за пределами shown в индекс не попадают: про них и сказать нечего,
    # кроме «нет на другой стороне».
    wanted = both | set(shown)
    byKeyMaster = _rowsByKey(onlyMaster, pkPos, wanted)
    byKeySlave = _rowsByKey(onlySlave, pkPos, wanted)

    lines = [
        f"расхождение по {len(allKeys)} ключам: изменены {len(both)}, "
        f"нет в ведомой {nameSlave} {len(keysMaster - keysSlave)}, "
        f"нет в ведущей {nameMaster} {len(keysSlave - keysMaster)}"
    ]

    # Перечень расходящихся колонок — по ВСЕМ изменённым ключам, а не только по
    # печатаемым. Иначе он врал бы ровно в том случае, ради которого нужен:
    # если первая сотня ключей по PK — это «нет в ведомой», то колонки,
    # ломающиеся у ключей за сотней, не попали бы в отчёт вовсе, и по логу
    # выходило бы, что расходящихся колонок нет.
    # Печатаются ВСЕ колонки, у которых есть хоть одно расхождение; порядок —
    # по убыванию частоты, отсечения по количеству нет.
    byColumn = {}
    for key in both:
        rowsM = byKeyMaster.get(key, [])
        rowsS = byKeySlave.get(key, [])
        if len(rowsM) != 1 or len(rowsS) != 1:
            continue        # дубликаты: сравнивать построчно нечего
        for column, _a, _b in _rowDiff(rowsM[0], rowsS[0],
                                       structMaster, structSlave, pkPos):
            byColumn[column] = byColumn.get(column, 0) + 1

    body = []
    for key in shown:
        rowsM = byKeyMaster.get(key, [])
        rowsS = byKeySlave.get(key, [])
        keyStr = _keyText(structMaster, pkPos, key)
        if rowsM and rowsS:
            if len(rowsM) > 1 or len(rowsS) > 1:
                body.append(f"{keyStr}: дубликаты — строк в ведущей "
                            f"{len(rowsM)}, в ведомой {len(rowsS)}")
                continue
            diff = _rowDiff(rowsM[0], rowsS[0], structMaster, structSlave, pkPos)
            detail = "; ".join(f"{column}: ведущая {_fmt(a)} ≠ ведомая {_fmt(b)}"
                               for column, a, b in diff)
            body.append(f"{keyStr}: отличаются поля ({len(diff)}) — {detail}")
        elif rowsM:
            extra = (f" (строк с этим ключом в ведущей {len(rowsM)})"
                     if len(rowsM) > 1 else "")
            body.append(f"{keyStr}: нет в ведомой {nameSlave}{extra}")
        else:
            extra = (f" (строк с этим ключом в ведомой {len(rowsS)})"
                     if len(rowsS) > 1 else "")
            body.append(f"{keyStr}: нет в ведущей {nameMaster} — лишняя строка "
                        f"в ведомой {nameSlave}")
    if byColumn:
        top = sorted(byColumn.items(), key=lambda kv: (-kv[1], kv[0]))
        lines.append(f"расходятся колонки (по всем {len(both)} изменённым "
                     f"ключам, по убыванию частоты): " +
                     ", ".join(f"{c} ({n})" for c, n in top))
    lines += body
    if len(allKeys) > limit:
        lines.append(f"показано ключей: {limit} из {len(allKeys)} "
                     f"(отсортированы по первичному ключу)")
    return lines


def _evaluate(period, tableNameEtlJobs, tableNameSlave,
              masterRows, slaveRows, set1, set2,
              structMaster=None, structSlave=None):
    """Вернуть статус аудита группы: 1 (идентично) или -4 (отличия)."""
    if set1 == set2:
        if len(masterRows) == len(slaveRows) == len(set1):
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
    if structMaster and structSlave:
        for line in _diffLines(onlyMaster, onlySlave, structMaster, structSlave,
                               tableNameEtlJobs, tableNameSlave):
            logger.warning("  %s", line)
    else:
        _printSample(f"только в ведущей {tableNameEtlJobs}", onlyMaster)
        _printSample(f"только в ведомой {tableNameSlave}", onlySlave)
    return -4


# ----------------------------------------------------------------------------
#                       Замеры времени по фазам (диагностика)
# ----------------------------------------------------------------------------
# Аудит iperson шёл часами, и до этих замеров нельзя было даже сказать, на что
# уходит время: группа на 2934 строки проверялась 14 с, а на 140 406 строк —
# 24 с. Основная часть НЕ зависела от объёма группы, то есть это была
# фиксированная плата за группу — и увидеть её удалось только по фазам. Так и
# нашлось, что COALESCE вокруг колонки периода отключал индекс (см.
# do_etl._periodCond). Механика замеров общая с переносом — она в do_etl.
_PHASE_LABELS = (
    ("masterExec", "запрос ведущей"),
    ("masterFetch", "выборка ведущей"),
    ("slaveExec", "запрос ведомой"),
    ("slaveFetch", "выборка ведомой"),
    ("convert", "приведение+множества"),
    ("compare", "сравнение+отчёт"),
    ("status", "статус+лог+коммит"),
)


def _newTiming():
    return _newPhases(_PHASE_LABELS)


def _addTiming(total, one, rows):
    _addPhases(total, one, _PHASE_LABELS, rows)


def _timingText(timing):
    return _phasesText(timing, _PHASE_LABELS)


def _timingTotal(timing):
    return _phasesTotal(timing, _PHASE_LABELS)


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
         "PERIOD": _periodBind(origPeriod)},
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
    # Гранулярность группы — та же, что у переноса (_periodKey): при
    # truncatePeriod=false период сравнивается точно, вместе со временем.
    # Безусловное приведение к дате (как было) для таких линий давало полночь,
    # которой в данных нет: обе выборки возвращали 0 строк, множества
    # совпадали — и группа помечалась как «идентична» (ложный зелёный).
    period = _periodKey(origPeriod, cfg["truncatePeriod"])

    one, _phase = _phaseTimer()
    try:
        # Бинды периода строятся отдельно для каждой стороны: у ведущей и
        # ведомой период может быть устроен по-разному (у одной составной
        # year+month, у другой одна колонка-дата).
        # filterParams нужны только ведущему источнику (selectSql); у ведомой
        # срез задан литеральным filterClauseSlave, лишние бинды Oracle не любит.
        cursorMaster.execute(
            _pickPeriodSql(ctx["masterSql"], period),
            {**_periodBinds(_periodSpec(cfg["periodColumn"]), period),
             **filterParams})
        _phase("masterExec")
        masterRows = cursorMaster.fetchall()
        _phase("masterFetch")
        cursorSlave.execute(
            _pickPeriodSql(ctx["slaveSql"], period),
            _periodBinds(_periodSpec(cfg["slavePeriodColumn"]), period))
        _phase("slaveExec")
        slaveRows = cursorSlave.fetchall()
        _phase("slaveFetch")

        set1, set2 = _toComparable(dbMaster, cfg["dbSlave"], masterRows,
                                   slaveRows, ctx["structMaster"],
                                   ctx["structSlave"])
        _phase("convert")
        status = _evaluate(period, tableNameEtlJobs, tableNameSlave,
                           masterRows, slaveRows, set1, set2,
                           ctx["structMaster"], ctx["structSlave"])
        _phase("compare")
        if status != 1:
            report["differing"].append(origPeriod)

        _writeStatus(cfg, ctx, origPeriod, status)
        UpdateLog(tableNameEtlJobs, dbMaster, "Audit",
                  cursorMaster, conMaster, len(set1), origPeriod, status)
        conMaster.commit()
        _phase("status")

        _addTiming(report["timing"], one, len(masterRows))
        logger.info("Период %s: %s %s — строк ведущая %d / ведомая %d; "
                    "%.1fс (%s)",
                    period, tableNameEtlJobs,
                    "идентична" if status == 1 else "ОТЛИЧАЕТСЯ",
                    len(masterRows), len(slaveRows),
                    _timingTotal(one), _timingText(one))
    except Exception as err:
        _safeRollback(conMaster)
        _safeRollback(conSlave)
        if isShutdown(err):
            # SIGTERM (деплой/перезапуск airflow) — не порча данных. Статус
            # группы НЕ трогаем: пометили бы -2, и человек потом разбирался бы
            # с несуществующей проблемой. Группа осталась непроверенной
            # (isokaudit != 1) и её доберёт следующий запуск.
            logging.warning("Аудит %s: SIGTERM на группе %s — останавливаюсь, "
                            "статус группы не меняю.", tableNameEtlJobs, origPeriod)
            raise ShutdownRequested(
                f"Аудит {tableNameEtlJobs}: SIGTERM на группе {origPeriod}, "
                f"проверенные группы сохранены."
            ) from err
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

    startedAt = time.perf_counter()
    conMaster = _connect(dbMaster)
    conSlave = _connect(dbSlave)
    cursorMaster = conMaster.cursor()
    cursorSlave = conSlave.cursor()
    connectSec = time.perf_counter() - startedAt
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

        setupSec = time.perf_counter() - startedAt - connectSec

        # 4. Контекст с готовыми SQL.
        ctx = {
            "conMaster": conMaster,
            "conSlave": conSlave,
            "cursorMaster": cursorMaster,
            "cursorSlave": cursorSlave,
            "structMaster": structMaster,
            "structSlave": structSlave,
            # По паре запросов на сторону: обычная группа и NULL-группа —
            # условия у них разные (см. do_etl._periodCond).
            "masterSql": _periodSqlPair(
                lambda nullGroup: _buildMasterSql(cfg, selectSql, structMaster,
                                                  nullGroup)),
            "slaveSql": _periodSqlPair(
                lambda nullGroup: _buildSlaveSql(cfg, structSlave, nullGroup)),
        }

        # Один раз за прогон — что именно уйдёт в БД. Сравнение с теми же
        # строками из лога переноса сразу показывает, чем запросы отличаются
        # (у аудита нет колонок из auditExcludeFields — и это меняет план).
        logStatements(
            f"Аудит {tableNameEtlJobs} ({dbMaster}->{dbSlave}): запросы этого "
            f"прогона. Значения уходят биндами; для NULL-группы условие "
            f"периода — `IS NULL` вместо `= бинд`.",
            [("выборка группы из ведущей", ctx["masterSql"]["row"]),
             ("выборка группы из ведомой", ctx["slaveSql"]["row"])])

        # 5. Группы на проверку.
        groupsAt = time.perf_counter()
        groups = _selectGroupsToAudit(cursorMaster, dbMaster, tableNameEtlJobs)
        groupsSec = time.perf_counter() - groupsAt
        if not groups:
            logger.info("Аудит %s: групп для проверки нет", tableNameEtlJobs)
            raise AirflowSkipException

        logger.info("Аудит %s: %d групп на проверку (подключение %.1fс, "
                    "структуры %.1fс, список групп %.1fс)",
                    tableNameEtlJobs, len(groups), connectSec, setupSec, groupsSec)
        report = {"differing": [], "errored": [], "timing": _newTiming()}
        for tablename, period in groups:
            _auditGroup(cfg, ctx, period, report)

        # Свод по фазам. Ради него замеры и заведены: одна строка в конце
        # отвечает на вопрос «что тормозит», не требуя читать лог по группам.
        timing = report["timing"]
        if timing["groups"]:
            logger.info(
                "Аудит %s: время по фазам за %d групп (%d строк ведущей) — %s; "
                "итого в группах %.1fс, в среднем %.1fс на группу",
                tableNameEtlJobs, timing["groups"], timing["rows"],
                _timingText(timing), _timingTotal(timing),
                _timingTotal(timing) / timing["groups"])

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
    except (AirflowSkipException, AirflowFailException, AuditScopeError,
            ShutdownRequested):
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