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
import os
import time
from collections import defaultdict
from decimal import Decimal

import psycopg2
import psycopg2.extras
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
    #dateSelectStatusOrclSql,
    #dateSelectStatusPostSql,
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
    #newDatesOrclSql,
    #newDatesPostSql,
    registerPeriodOrclSql,
    registerPeriodPostSql,
    # section_compare (mocheck / medree)
    dateSelectMasterPostSql,
    dateSelectMasterOrclSql,
    dateSelectSlavePostSql,
    dateSelectSlaveOrclSql,
    periodsFromIudPostSql,
    periodsFromIudOrclSql,
    periodBind,
    markPeriodIudPostSql,
    markPeriodIudOrclSql,
    periodsIsokAudit4PostSql,
    periodsIsokAudit4OrclSql,
)

from Src.fullPath import FULL_PATH

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
#                       Классификация ошибок по типу
# ----------------------------------------------------------------------------

class ShutdownRequested(AirflowException):
    """Процесс остановили извне (SIGTERM при деплое / перезапуске airflow), а не
    наткнулись на плохие данные.

    Ключевое отличие от RecordScopeError: НИЧЕГО не паркуем. Необработанные
    записи остаются в журнале с isetl=0 и уедут следующим запуском, поэтому
    после деплоя не нужно руками возвращать -1 в 0. runEtl превращает это
    в AirflowSkipException — прогон ⬜, линия не морозится и не краснеет.
    """
    pass


def isShutdown(err):
    """SIGTERM / остановка задачи, а не ошибка данных.

    Своего класса у этого исключения нет: airflow бросает
    AirflowException('Task received SIGTERM signal') прямо из обработчика
    сигнала, поэтому оно может прилететь в ЛЮБОЙ точке — в том числе внутри
    except-блока, который разбирает предыдущую ошибку. Отличаем по тексту и
    разматываем цепочку __cause__/__context__ (страховка от зацикливания —
    множество уже виденных).
    """
    seen = set()
    while err is not None and id(err) not in seen:
        seen.add(id(err))
        if isinstance(err, ShutdownRequested):
            return True
        if "sigterm" in str(err).lower():
            return True
        err = err.__cause__ or err.__context__
    return False


def _safeRollback(con):
    """Откат без шума: во время SIGTERM соединение уже может быть нерабочим."""
    try:
        if con is not None:
            con.rollback()
    except Exception:
        pass


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
    # SIGTERM — не ошибка данных и не поломка соединения: это внешняя
    # остановка, повторять/парковать нечего.
    if isShutdown(err):
        return "shutdown"
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

def _resolveEtlPath(path):
    """Путь к structure/sql из конфига.

    Абсолютный путь возвращается как есть (совместимость с прод-конфигом, где
    пути прибиты к /opt/.../etlFolderProd). Относительный — резолвится от
    {FULL_PATH}etlFolder/, поэтому один и тот же конфиг работает в любой
    среде (local/test/prod), а на dev-PC файлы лежат в etlFolder репозитория.
    """
    if not path or os.path.isabs(path):
        return path
    return f"{FULL_PATH}etlFolder/{path}"

def _loadStructure(path, dbType):
    path = _resolveEtlPath(path)
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


def _periodKey(value, truncatePeriod):
    """Канонический ключ ГРУППЫ для section_compare.

    Группы приезжают из трёх независимых источников (сравнение срезов
    master/slave, журнал etl_log_iud_row, isokaudit=4 в etl_jobs), и ключ у них
    обязан быть один и тот же: иначе одна и та же группа попадает в набор
    дважды в разном виде, а sorted() по смеси date и datetime падает с
    TypeError («can't compare datetime.datetime to datetime.date»).

    Гранулярность задаёт тот же флаг `truncatePeriod`, который решает, сравнивать
    ли период в SQL по дате (TRUNC/DATE) или точно — чтобы Python и SQL считали
    группой одно и то же:

      truncatePeriod=True  — группа это ДЕНЬ. Нужно, когда стороны хранят период
        по-разному: у iprkdept ведущая Oracle DATE (драйвер отдаёт datetime), а
        ведомая Postgres date (отдаёт date). Без приведения ключи не совпадут
        никогда, каждая группа считалась бы расходящейся и таблица
        перезаливалась бы целиком на каждом прогоне.

      truncatePeriod=False — группа это ТОЧНОЕ значение, вместе со временем. Так
        устроен medree: у dcalc есть время, и отдельным пересчётом (а значит
        отдельной группой) считается каждое конкретное значение. Тип при этом
        всё равно унифицируется (date -> datetime в полночь), поэтому Oracle
        DATE и Postgres timestamp сравниваются корректно.
    """
    return _normalizeCompareValue(value, truncatePeriod)


# ----------------------------------------------------------------------------
#     Период группы: одна колонка-дата ЛИБО составная (year[, month[, day]])
# ----------------------------------------------------------------------------
# Период — свойство ТАБЛИЦЫ, а не режима переноса: он может лежать в одной
# date-колонке (createdate, dcalc, ...) либо быть собран из нескольких числовых
# (year / year+month / year+month+day), причём у ведущей и ведомой — независимо
# (у одной составной, у другой цельный). Внутри Python период ВСЕГДА
# datetime.date; разложение на части происходит только на границе с SQL.
#
# Формат в конфиге (periodColumn / slavePeriodColumn):
#     "createdate"                                  — одна колонка (как раньше)
#     {"year": "god"}                               — только год  -> god-01-01
#     {"year": "year", "month": "month"}            — год+месяц   -> year-month-01
#     {"year": "y", "month": "m", "day": "d"}       — год+месяц+день

# NULL — ПОЛНОПРАВНАЯ группа, а не «нет группы». У expmed поле, которое служит
# периодом, какое-то время пустое, и это нормальное состояние данных: такие
# строки надо переносить, сверять и учитывать в etl_jobs наравне с прочими.
#
# Сравнивать NULL через `=` нельзя (NULL = NULL это не TRUE). Способов два, и
# место применения у них разное:
#
#   * ДАННЫЕ (ведущая/ведомая, миллионы строк) — отдельное условие на группу:
#     обычная идёт как `колонка = :бинд`, NULL-группа как `колонка IS NULL`
#     (см. _periodCond). Сентинел сюда НЕ лезет: COALESCE вокруг колонки
#     отключает индекс по периоду, и каждая группа обходилась полным сканом
#     (замеры аудита iperson: 11 с на группе в 2934 строки — столько же, сколько
#     на группе в 140 тысяч);
#   * СЛУЖЕБНЫЕ таблицы (etl_jobs, etl_log — сотни строк на линию) — прежний
#     COALESCE с сентинелом прямо в .sql. Там скан ничего не стоит, а один
#     запрос на оба случая проще двух.
#
# Важная деталь для служебных: в БИНД NULL не уходит НИКОГДА — подстановку
# делает Python (periodBind). Иначе пришлось бы гадать, каким типом драйвер
# пошлёт None: cx_Oracle по умолчанию шлёт его строкой, и COALESCE(строка, дата)
# падает с ORA-00932. Единственное место, где NULL уходит в БД как есть, —
# INSERT нового периода в etl_jobs, и там стоит явный CAST.
#
# Сам литерал и его python-двойник живут в Src.generalQueries — там же, где
# загружаются .sql, в которых он зашит, и оттуда же его берёт updateLog
# (periodBind). Локальных копий здесь больше нет: после перехода данных на
# `= :бинд` / `IS NULL` сентинел нужен только служебным .sql, а лишний алиас
# — это ещё одно место, где литерал может разъехаться.


def _periodSortKey(period):
    """Ключ сортировки, где NULL-группа — полноправный участник (идёт последней).

    Просто sorted() по смеси date и None падает с TypeError, а выкидывать None
    из списков нельзя: группа существует и её надо видеть в логах."""
    return (period is None, period)


def _periodSpec(value):
    """Нормализовать значение periodColumn в описание периода.

    Возвращает {"kind": "single", "column": ...} либо
    {"kind": "parts", "year": ..., "month": ... | None, "day": ... | None}.
    """
    if isinstance(value, dict):
        year = value.get("year") or value.get("YEAR")
        if not year:
            raise ValueError(
                "Составной период: обязателен ключ 'year' в periodColumn.")
        return {"kind": "parts", "year": year,
                "month": value.get("month") or value.get("MONTH"),
                "day": value.get("day") or value.get("DAY")}
    return {"kind": "single", "column": value}


def _qual(alias, column):
    return f"{alias}.{column}" if alias else column


def _periodExpr(dbType, spec, alias=None, truncate=False):
    """SQL-выражение, дающее ДАТУ периода (для SELECT/GROUP BY/JOIN).

    single: колонка как есть (или DATE()/TRUNC() при truncatePeriod — прежнее
    поведение). parts: сборка даты из частей (недостающие месяц/день = 1).
    """
    if spec["kind"] == "single":
        col = _qual(alias, spec["column"])
        if truncate:
            return f"DATE({col})" if _isPost(dbType) else f"TRUNC({col})"
        return col
    y = _qual(alias, spec["year"])
    m = _qual(alias, spec["month"]) if spec["month"] else "1"
    d = _qual(alias, spec["day"]) if spec["day"] else "1"
    if _isPost(dbType):
        return f"make_date({y}::int, {m}::int, {d}::int)"
    # Oracle: собираем через строку — надёжнее, чем арифметика по датам
    return (f"TO_DATE(TO_CHAR({y}) || '-' || LPAD(TO_CHAR({m}), 2, '0')"
            f" || '-' || LPAD(TO_CHAR({d}), 2, '0'), 'YYYY-MM-DD')")


def _periodCond(dbType, spec, alias=None, bind="createdate", truncate=False,
                nullGroup=False):
    """Условие WHERE «период = заданный».

    parts сравнивается ПО ЧАСТЯМ (year = :y AND month = :m) — это sargable и
    использует индексы, в отличие от сборки даты вокруг колонок.

    nullGroup=True — условие для NULL-ГРУППЫ: `колонка IS NULL`, БЕЗ бинда
    (для составного периода — все части IS NULL).

    ПОЧЕМУ ДВА ВАРИАНТА, А НЕ ОДИН С COALESCE. NULL-группа — полноправная
    группа, и когда-то она была сделана самым коротким способом: обе стороны
    под COALESCE с сентинелом, тогда `= :период` ловит и её. Стоило это
    индексов: колонка под функцией, и обычный индекс по периоду не применяется
    ни в Oracle, ни в Postgres — каждая группа обходилась полным сканом
    таблицы. Замеры аудита iperson: выборка ведомой 11.1 с на группе в 2934
    строки и 14.2 с на группе в 140 406 — время почти не зависит от объёма,
    то есть это скан, а не чтение строк.

    Поэтому условие теперь строится ПОД ГРУППУ: для обычной — голое
    `колонка = :бинд` (индекс работает), для NULL-группы — `колонка IS NULL`
    (бинда нет вовсе, и вопрос «каким типом драйвер пошлёт None» не
    возникает). Оба запроса готовятся один раз на линию, выбор — по значению
    периода (_pickPeriodSql), так что лишних разборов SQL не появляется.

    Смысл не изменился: COALESCE(x, s) = s ⟺ x IS NULL, а для непустого
    периода COALESCE(x, s) = v ⟺ x = v (сентинел в данных не встречается).
    """
    if spec["kind"] == "single":
        if nullGroup:
            # именно КОЛОНКА, а не TRUNC(колонка): TRUNC(NULL) это тоже NULL,
            # но под функцией индекс снова не применится
            return f"{_qual(alias, spec['column'])} IS NULL"
        if truncate:
            # ПОЛУИНТЕРВАЛ вместо TRUNC(колонка) = :период.
            #
            # Смысл тот же (для дат TRUNC(x) = d ⟺ d <= x < d+1), но колонка
            # остаётся ГОЛОЙ — а значит работает индекс по ней и, что важнее,
            # условие можно протолкнуть внутрь вложенного запроса. Под функцией
            # оба варианта закрыты: у EXPMED23 выборка одной группы обходила
            # весь EXPMED с 2020 года и стоила 7 секунд при 85 строках на
            # выходе; на 600 групп это часы.
            #
            # TRUNC/::date стоит на БИНДЕ, а не на колонке: бинд — константа,
            # индексу он не мешает, зато граница не поедет, если сверху придёт
            # значение со временем.
            col = _qual(alias, spec["column"])
            name = _bindName(dbType, bind)
            low = f"{name}::date" if _isPost(dbType) else f"TRUNC({name})"
            return f"({col} >= {low} AND {col} < {low} + 1)"
        expr = _periodExpr(dbType, spec, alias, truncate)
        return f"{expr} = " + _bindName(dbType, bind)
    columns = [(spec["year"], "y")]
    if spec["month"]:
        columns.append((spec["month"], "m"))
    if spec["day"]:
        columns.append((spec["day"], "d"))
    if nullGroup:
        return " AND ".join(f"{_qual(alias, col)} IS NULL" for col, _s in columns)
    return " AND ".join(f"{_qual(alias, col)} = " + _bindName(dbType, f"{bind}_{s}")
                        for col, s in columns)


def _periodBinds(spec, period, bind="createdate"):
    """Параметры для _periodCond по значению периода (date или None).

    Для NULL-группы параметров НЕТ вовсе: её условие — `IS NULL`, плейсхолдера
    в SQL нет, а лишние бинды Oracle не принимает (ORA-01036). Значит и
    вопроса, каким типом драйвер пошлёт None, больше не существует."""
    if period is None:
        return {}
    if spec["kind"] == "single":
        return {bind: period}
    period = _normalizePeriod(period)
    out = {f"{bind}_y": period.year}
    if spec["month"]:
        out[f"{bind}_m"] = period.month
    if spec["day"]:
        out[f"{bind}_d"] = period.day
    return out


def _periodBind(period):
    """То же для мест, где период биндится ОДНИМ параметром напрямую
    (etl_jobs: last_success_ts / isokaudit)."""
    return periodBind(period)


def _journalPeriodCond(dbType, period, bind="createdate", truncate=False):
    """Условие по периоду для ЖУРНАЛА etl_log_iud_row.

    Отдельно от etl_jobs: журнал — не «маленькая служебная таблица», в нём
    строка на каждое изменение ведущей, и COALESCE вокруг period обходился ему
    так же дорого, как таблицам данных (см. _periodCond). etl_jobs и etl_log
    остаются на сентинеле: там сотни строк на линию.

    truncate — ГРАНУЛЯРНОСТЬ ГРУППЫ, и она обязана совпадать с той, по которой
    группу выбрали (_periodKey). Иначе получается тихий вечный цикл: группу
    берут как TRUNC(period) журнала, а помечают обработанной по
    `period = <день>` — строка журнала со временем внутри дня под условие не
    попадает, остаётся isetl = 0, на следующем прогоне снова даёт ту же группу,
    и линия переносит её раз за разом. Ровно этот риск появился у expmed после
    перевода на section_compare_with_iud: truncatePeriod = true, а period в
    журнале — docexpdt как есть.
    """
    if period is None:
        return "period IS NULL"
    name = _bindName(dbType, bind)
    if truncate:
        # тот же полуинтервал, что и у данных (см. _periodCond): журнал —
        # самая большая служебная таблица, и функция вокруг period закрывала
        # индекс ровно так же
        low = f"{name}::date" if _isPost(dbType) else f"TRUNC({name})"
        return f"(period >= {low} AND period < {low} + 1)"
    return f"period = {name}"


def _journalPeriodBind(period, bind="createdate"):
    """Бинд к _journalPeriodCond: у NULL-группы плейсхолдера нет, и лишний
    параметр Oracle не примет (ORA-01036)."""
    return {} if period is None else {bind: period}


def _rowcount(cursor):
    """Сколько строк задел последний DML ('?' — драйвер не сказал)."""
    try:
        count = cursor.rowcount
    except Exception:
        return "?"
    return count if isinstance(count, int) and count >= 0 else "?"


def _periodColumns(spec):
    """Имена колонок периода (одна или несколько) — для поиска их в строке."""
    if spec["kind"] == "single":
        return [spec["column"]]
    return [c for c in (spec["year"], spec["month"], spec["day"]) if c]


def _periodFromRow(spec, row, columnNames):
    """Собрать период (date) из строки выборки ведущей по именам колонок.

    Регистронезависимо: Oracle отдаёт имена в ВЕРХНЕМ регистре, Postgres — в
    нижнем, а в конфиге они обычно записаны как в своей БД.
    """
    lower = [c.lower() for c in columnNames]

    def _val(name):
        return row[lower.index(name.lower())]

    if spec["kind"] == "single":
        return _normalizePeriod(_val(spec["column"]))
    # Пустой год — это NULL-группа, а не повод падать на int(None).
    if _val(spec["year"]) is None:
        return None
    year = int(_val(spec["year"]))
    month = int(_val(spec["month"])) if spec["month"] else 1
    day = int(_val(spec["day"])) if spec["day"] else 1
    return datetime.date(year, month, day)


def _normalizeCompareValue(value, truncate):
    """Нормализовать значение lastupdate для сравнения master/slave в section_compare.

    Разные драйверы возвращают одну и ту же дату по-разному: Oracle DATE →
    datetime.datetime (00:00:00), Postgres date → datetime.date. Без нормализации
    множества {datetime} и {date} никогда не равны → ложное «данные отличаются»
    → бесконечные обновления группы.

    По умолчанию (truncate=False) сохраняем точность timestamp: только унифицируем
    ТИП — date → datetime в полночь, так что date == datetime(00:00), но два разных
    времени по-прежнему различаются (реальный lastupdate не теряется).
    truncate=True (опция конфига truncateLastupdate): сравниваем lastupdate по ДАТЕ
    (день), время отбрасываем — для случаев, когда одна из БД хранит только дату.
    """
    if value is None:
        return None
    if truncate:
        return value.date() if isinstance(value, datetime.datetime) else value
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
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


def _asAndClause(value):
    """filterClause / filterClauseSlave / filterClauseMaster: принять строку
    ИЛИ список и вернуть единый предикат-строку.

    Канон — строка: между частями условия может быть и OR, и удобно видеть
    условие целиком. Список (легаси-форма) объединяется через AND. Пусто -> None.
    ВНИМАНИЕ: вызывающий код, приклеивая результат к своим условиям через AND,
    оборачивает его в скобки — иначе внутренний OR распарсится неверно.
    """
    if not value:
        return None
    if isinstance(value, (list, tuple)):
        return " AND ".join(value)
    return value


def _asColumns(value):
    """conflictExtra: принять строку 'a, b' ИЛИ список ['a', 'b'] и вернуть
    строку 'a, b' (или '' если пусто)."""
    if not value:
        return ""
    if isinstance(value, (list, tuple)):
        return ", ".join(value)
    return value


def _appendFilter(sql, filterClause):
    """Добавить пользовательский WHERE-фильтр к подзапросу.

    Используется, когда одна физическая таблица служит источником сразу
    нескольких логических групп (mocheck doctype-split): selectSql у них
    общий, а отличается только дополнительный фильтр.
    """
    filterClause = _asAndClause(filterClause)
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
                    conflictExtra, conflictWhere, mode):
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

    filterClauseSlave сюда НЕ передаётся ни на одной из СУБД — запись идёт
    строго по первичному ключу. Раньше на Oracle он приклеивался к ON у
    MERGE, и это давало две ошибки сразу:
      * ORA-38104, если фильтр стоит по переносимой колонке (eindexmo,
        "YEAR >= 2019"): Oracle запрещает обновлять колонки из ON, а YEAR
        есть и в UPDATE SET. Падало на КАЖДОЙ строке, независимо от данных;
      * строка, которая есть, но под фильтр не подходит, уходила в
        NOT MATCHED -> INSERT и падала дублем по PK (ORA-00001).
    Смысл ключа — «какие строки ведомой относятся к линии» при СРАВНЕНИИ
    (аудит) и УДАЛЕНИИ, а не ограничение записи по PK: совпал PK — это та
    же логическая строка, и перезаписать её правильно. В postgres-ветке так
    было всегда (срез там выражается через conflictExtra/conflictWhere),
    теперь обе СУБД ведут себя одинаково.
    """
    columns = _columnNames(structSlave)
    pkCols = _primaryKeys(structSlave)

    if _isPost(dbSlave):
        columnsStr = ", ".join(columns)
        valuesStr = ", ".join(["%s"] * len(columns))
        if mode in ("section", "section_compare", "section_compare_with_iud",
                    "delete_insert", "query_section"):
            # delete_insert тоже сначала удаляет (по idrw), query_section — по
            # (year, month), поэтому конфликта нет — простой INSERT без ON CONFLICT.
            return insertPostSql.format(
                tablename=tableNameSlave,
                columns_str=columnsStr,
                values_str=valuesStr,
            )
        extra = _asColumns(conflictExtra)
        primaryStr = ", ".join(pkCols) + (f", {extra}" if extra else "")
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
    fcs = _asAndClause(filterClauseSlave)
    if fcs:
        primaryCond += f" AND ({fcs})"
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
    periodExpr = _periodExpr(dbSlave, _periodSpec(slavePeriodColumn),
                             None, truncatePeriod)
    primaryCond = " AND ".join(
        f"{c} = " + _bindName(dbSlave, f"id{i}")
        for i, c in enumerate(pkColsSlave)
    )
    fcs = _asAndClause(filterClauseSlave)
    if fcs:
        primaryCond += f" AND ({fcs})"
    sqlTpl = _pickSql(dbSlave, slavePeriodsByIdPostSql, slavePeriodsByIdOrclSql)
    return sqlTpl.format(period_expr=periodExpr, tablename=tableNameSlave,
                         primary_cond=primaryCond)


# ----------------------------------------------------------------------------
#                     Замеры времени по фазам (диагностика)
# ----------------------------------------------------------------------------
# Один прогон по группе — это пять-шесть разных обращений к двум БД, а в логе
# между «обработка группы» и «найдено N записей» была одна пауза без единой
# подсказки, из чего она состоит. Замеры ничего не меняют в работе: только
# печатают, сколько заняла каждая фаза. Тот же приём уже окупился в аудите —
# именно так нашлось, что условие по периоду прячет колонку под функцию.

def logStatements(title, statements):
    """Напечатать ОДИН РАЗ за прогон, какой SQL реально уйдёт в БД.

    Печатается собранный текст с плейсхолдерами, без подстановки значений:
    вопрос, на который это отвечает, — «какой запрос выполняется», а не «с
    какими данными». Значения всё равно уходят биндами, и подставлять их в
    текст пришлось бы вручную и с риском переврать.

    Зачем. Запрос собирается из пяти кусков (шаблон, список полей, свой
    SELECT источника, условие периода, фильтр линии) — и понять по коду, что
    в итоге получилось, стоит дороже, чем прочитать готовый текст. На iperson
    именно так стало видно, чем выборка группы в переносе отличается от
    выборки аудита: списком полей, а из-за него планировщик по-разному
    обходится с соединением внутри источника.

    statements — [(подпись, sql)]; пустые пропускаются.
    """
    blocks = []
    for label, sql in statements:
        if not sql:
            continue
        blocks.append(f"--- {label} ---\n{_stripLeadingComments(sql)}")
    if blocks:
        logger.info("%s\n%s", title, "\n".join(blocks))


def _stripLeadingComments(sql):
    """Убрать шапку из `--`-комментариев шаблона: в файле она объясняет запрос,
    в логе — только мешает читать сам запрос."""
    lines = str(sql).strip().splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).strip()


# Что показывать из ctx и как подписывать. Порядок — как в работе линии.
_CTX_SQL_LABELS = (
    ("recordByIdSql", "выборка записи ведущей по ключу (iud / delete_insert)"),
    ("recordGroupSql", "выборка группы из ведущей"),
    ("deleteByIdSql", "удаление записи в ведомой по ключу"),
    ("deletePeriodSql", "удаление группы в ведомой"),
    ("slavePeriodsByIdSql", "группы ведомой по ключу (delete_insert)"),
    ("upsertSql", "запись строки в ведомую"),
)


def _ctxStatements(ctx, labels=_CTX_SQL_LABELS):
    """[(подпись, sql)] из ctx. У пар (обычная группа / NULL-группа) берётся
    вариант обычной — у NULL-группы отличается только условие периода."""
    out = []
    for key, label in labels:
        sql = ctx.get(key)
        if isinstance(sql, dict):
            out.append((label, sql.get("row")))
        else:
            out.append((label, sql))
    return out


def _phaseTimer():
    """(marks, phase): словарь замеров и функция-отсечка `phase('имя')`."""
    marks = {}
    state = {"at": time.perf_counter()}

    def phase(name):
        now = time.perf_counter()
        marks[name] = marks.get(name, 0.0) + (now - state["at"])
        state["at"] = now

    return marks, phase


def _phasesText(marks, labels):
    """'запрос ведущей 3.1с, выборка ведущей 0.0с, ...' по списку (имя, подпись).

    Фазы, которых в замерах нет, пропускаются: список фаз общий на несколько
    путей, и «проверка структуры 0.0с» там, где её не делают, читалась бы как
    «сделали мгновенно»."""
    return ", ".join(f"{label} {marks[name]:.1f}с"
                     for name, label in labels if name in marks)


def _phasesTotal(marks, labels):
    return sum(marks.get(name, 0.0) for name, _label in labels)


def _newPhases(labels):
    """Аккумулятор сумм по фазам (для свода в конце прогона)."""
    total = {name: 0.0 for name, _label in labels}
    total["groups"] = 0
    total["rows"] = 0
    return total


def _addPhases(total, marks, labels, rows=0):
    for name, _label in labels:
        total[name] += marks.get(name, 0.0)
    total["groups"] += 1
    total["rows"] += rows


# Фазы перезаливки ОДНОЙ группы — общие для массового обновления (isokaudit=4)
# и для section_compare: обе делают одно и то же в одном порядке.
_GROUP_PHASE_LABELS = (
    ("connect", "подключение к ведомой"),
    ("structCheck", "проверка структуры ведомой"),
    ("journal", "отметка журнала"),
    ("delete", "удаление группы в ведомой"),
    ("masterExec", "запрос группы к ведущей"),
    ("masterFetch", "выборка группы"),
    ("upsert", "заливка в ведомую"),
    ("commit", "etl_jobs+etl_log+коммит"),
)


def _periodSqlPair(build):
    """Пара готовых SQL: {'row': для обычной группы, 'null': для NULL-группы}.

    Условие по периоду у них разное (`= :бинд` против `IS NULL`, см.
    _periodCond), поэтому один текст на оба случая не годится. Готовятся оба
    сразу, один раз на линию — выбор делает _pickPeriodSql по значению группы.
    """
    return {"row": build(False), "null": build(True)}


def _pickPeriodSql(pair, period):
    """Взять из пары запрос под эту группу (period=None -> вариант IS NULL).

    Терпит и одиночный текст: линию могли собрать до появления пары."""
    if not isinstance(pair, dict):
        return pair
    return pair["null" if period is None else "row"]


def _buildDeletePeriodSql(dbSlave, tableNameSlave, slavePeriodColumn,
                          filterClauseSlave, truncatePeriod=False,
                          nullGroup=False):
    """SQL для удаления записей группы по периоду (одиночному или составному)."""
    spec = _periodSpec(slavePeriodColumn)
    cond = _periodCond(dbSlave, spec, None, "createdate", truncatePeriod,
                       nullGroup)
    fcs = _asAndClause(filterClauseSlave)
    if fcs:
        cond += f" AND ({fcs})"
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
    fcm = _asAndClause(filterClauseMaster)
    if fcm:
        primaryCond += f" AND ({fcm})"
    sqlTpl = _pickSql(dbMaster, recordSelectByIdPostSql, recordSelectByIdOrclSql)
    return sqlTpl.format(
        fields_str=fieldsStr,
        select_sql=selectSql,
        primary_cond=primaryCond,
    )


def _buildRecordGroupSql(dbMaster, selectSql, structMaster, periodColumn,
                         filterClauseMaster, truncatePeriod=False,
                         nullGroup=False):
    """Собрать SQL для выбора группы записей по периоду.

    truncatePeriod: если True, для Oracle применяется TRUNC(period),
                    для Postgres — DATE(period). Это нужно, когда в БД
                    колонка периода содержит время (например, dcalc в medree),
                    а в ETL процесс передается только дата.
    """
    fieldsStr = _buildFieldsStr(dbMaster, structMaster, periodColumn)
    spec = _periodSpec(periodColumn)
    cond = _periodCond(dbMaster, spec, "p", "createdate", truncatePeriod,
                       nullGroup)
    fcm = _asAndClause(filterClauseMaster)
    if fcm:
        cond += f" AND ({fcm})"
    sqlTpl = _pickSql(dbMaster, recordSelectGroupPostSql, recordSelectGroupOrclSql)
    return sqlTpl.format(
        fields_str=fieldsStr,
        select_sql=selectSql,
        period_cond=cond,
    )


# ----------------------------------------------------------------------------
#                              Группа ↔ периоды
# ----------------------------------------------------------------------------

'''
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
        {"tablename": tableNameEtlJobs},
    )
'''

def _registerAffectedPeriods(cursor, dbMaster, periods, tableNameEtlJobs):
    """Добавить в etl_jobs затронутые переносом группы (period), которых ещё нет.

    В отличие от _registerNewPeriods НЕ сканирует источник. Новый период
    появляется только когда вставлена строка с новым createdate, а любая
    такая вставка проходит через триггер и порождает событие в
    etl_log_iud_row. Значит достаточно зарегистрировать периоды, реально
    затронутые этим прогоном (для iud — периоды из лога, для delete_insert —
    периоды из данных). Их на прогон единицы, поэтому дешёвый
    INSERT ... WHERE NOT EXISTS на каждый дешевле полного прохода UNION.
    """
    if not periods:
        return
    sqlTpl = _pickSql(dbMaster, registerPeriodPostSql, registerPeriodOrclSql)
    for period in periods:
        cursor.execute(sqlTpl,
                       {"tablename": tableNameEtlJobs,
                        "period": period,
                        "period_cmp": _periodBind(period)})


def _selectGroupsForMassUpdate(cursor, dbMaster, tableNameEtlJobs):
    """Группы, которые требуют полного обновления (isokaudit = 4)."""
    sqlTpl = _pickSql(dbMaster, periodsIsokAudit4PostSql, periodsIsokAudit4OrclSql)
    return _executeQuery(
        cursor,
        sqlTpl,
        {"tablename": tableNameEtlJobs},
    )


def _selectIudWork(cursor, dbMaster, tableNameEtlJobs):
    """Один запрос к журналу -> (iudRecords, distinctIds).

    iudRecords  = [(id, idrw), ...] — все необработанные строки (для пометки
                  isetl).
    distinctIds = [(id, period, timeoper, oper), ...] — по одной на (period, id),
                  последняя по timeoper операция (дедуп в Python).

    Заменяет связку _selectIudRecords + _selectDistinctIds: без второго запроса
    и без IN-списка по idrw — поэтому при тысячах изменений нет ORA-01795
    (max 1000 expressions in a list).
    """
    sqlTpl = _pickSql(dbMaster, selectEtlIudPostSql, selectEtlIudOrclSql)
    rows = _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs})
    iudRecords = [(r[0], r[1]) for r in rows]
    # rows идут ORDER BY timeoper (возр.) — перезапись оставляет последнюю
    # операцию для каждой пары (period, id).
    latest = {}
    for rid, _idrw, period, timeoper, oper in rows:
        latest[(period, rid)] = (timeoper, oper)
    distinctIds = [(rid, period, timeoper, oper)
                   for (period, rid), (timeoper, oper) in latest.items()]
    return iudRecords, distinctIds


# ----------------------------------------------------------------------------
#         Батч-выборка строк ведущей/периодов ведомой по набору id
# ----------------------------------------------------------------------------

def _chunks(seq, size=1000):
    """Резать список на куски (Oracle: max 1000 выражений в IN)."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _bindInList(dbType, values, prefix):
    """(in_clause, params) для IN-выражения в нужном paramstyle.

    Postgres — %(p0)s,..., Oracle — :p0,...; значения биндятся по одному
    (как в одиночном `col = :id`), поэтому неявное приведение типа к колонке
    работает так же и индекс используется.
    """
    if _isPost(dbType):
        placeholders = ", ".join(f"%({prefix}{i})s" for i in range(len(values)))
    else:
        placeholders = ", ".join(f":{prefix}{i}" for i in range(len(values)))
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return placeholders, params


def _idKey(value):
    """Канонический ключ для сопоставления id из журнала (строка) и значения
    PK из строки выборки (int/Decimal/float/строка). Целые числа — без дробной
    части, чтобы '123' == 123 == Decimal('123')."""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, Decimal):
        return (str(value.to_integral_value())
                if value == value.to_integral_value() else str(value))
    return str(value).strip()


def _recIdKey(recId, pkCols):
    """Ключ для поиска в словарях префетча: одиночный PK нормализуем (_idKey),
    составной оставляем строкой 'pk1/pk2' как есть."""
    return _idKey(recId) if len(pkCols) == 1 else recId


def _masterRowsInChunks(ctx, cfg, ids):
    """Строки ведущей для набора id IN-запросами чанками по 1000 (fallback и
    путь для составного PK). Минус: тяжёлый исходник (UNION в MOCHECK.sql)
    пересчитывается на КАЖДЫЙ чанк — для тысяч id медленно. Быстрый путь
    (исходник считается 1-2 раза) — в _selectMasterRowsByIds."""
    ids = list(ids)
    if not ids:
        return []
    dbMaster = cfg["dbMaster"]
    pkCols = ctx["pkColsMaster"]
    fieldsStr = _buildFieldsStr(dbMaster, ctx["structMaster"], cfg["periodColumn"])
    selectSql = ctx["selectSql"]
    fcm = _asAndClause(cfg.get("filterClauseMaster"))
    filterParams = cfg.get("filterParams") or {}
    tpl = _pickSql(dbMaster, recordSelectByIdPostSql, recordSelectByIdOrclSql)
    cursor = ctx["cursorMaster"]
    rows = []

    if len(pkCols) == 1:
        pk = pkCols[0]
        for chunk in _chunks(ids):
            inClause, params = _bindInList(dbMaster, chunk, "v")
            cond = f"p.{pk} IN ({inClause})"
            if fcm:
                cond += f" AND ({fcm})"
            params.update(filterParams)
            cursor.execute(tpl.format(fields_str=fieldsStr, select_sql=selectSql,
                                      primary_cond=cond), params)
            rows.extend(cursor.fetchall())
    else:
        # Составной PK: IN по кортежам неудобен — идём по одному (редкий случай).
        for recId in ids:
            cond = " AND ".join(f"p.{c} = " + _bindName(dbMaster, f"id{i}")
                                for i, c in enumerate(pkCols))
            if fcm:
                cond += f" AND ({fcm})"
            params = {**_splitPkValue(recId, len(pkCols)), **filterParams}
            cursor.execute(tpl.format(fields_str=fieldsStr, select_sql=selectSql,
                                      primary_cond=cond), params)
            rows.extend(cursor.fetchall())
    return rows


def _resolveDependencyIds(ctx, cfg, column, values):
    """Строки ведущей, у которых <column> входит в values.

    -> [(значение колонки, ключ строки ведущей, период)].

    Сердце «зависимостей»: журнал принёс ключ ЧУЖОЙ таблицы, а работать надо с
    ключами ведущей. Спрашиваем об этом сам источник линии — тот же SELECT, из
    которого потом поедут данные, — и получаем ровно те строки, которые он
    отдаёт СЕЙЧАС. Никакой отдельной «карты связей» держать не нужно: связь уже
    описана запросом линии, а второе её описание рано или поздно разойдётся с
    первым.

    Значение колонки возвращаем вместе со строкой, чтобы знать, какая
    журнальная запись за какие строки отвечает: без этого зависимость нельзя
    закрыть честно (см. _selectDependencyWork).

    Период берём отсюда же, а НЕ из журнальной записи. Период чужой строки к
    группам этой линии отношения не имеет: он ушёл бы в etl_jobs, в регистрацию
    затронутых периодов и в аудит — линия отчитывалась бы о переносе не тех
    групп, которые перенесла. Ошибка при этом молчаливая: строки на месте, а
    сверка периодов не сходится.
    """
    values = list(values)
    if not values:
        return []
    dbMaster = cfg["dbMaster"]
    pkCols = ctx["pkColsMaster"]
    if len(pkCols) != 1:
        raise ValueError(
            "Зависимости поддержаны только для ведущей с ОДНОЙ колонкой ключа, "
            f"а тут {pkCols}. Составной ключ пришлось бы склеивать в строку — "
            "ровно та операция, из-за которой ключ линии iprkdept переехал на "
            "idrw (см. oracleSetup/11).")
    pk = pkCols[0]
    periodExpr = _periodExpr(dbMaster, _periodSpec(cfg["periodColumn"]),
                             "p", cfg.get("truncatePeriod"))
    fcm = _asAndClause(cfg.get("filterClauseMaster"))
    filterParams = cfg.get("filterParams") or {}
    cursor = ctx["cursorMaster"]
    out = []
    for chunk in _chunks(values):
        inClause, params = _bindInList(dbMaster, chunk, "v")
        cond = f"p.{column} IN ({inClause})"
        if fcm:
            cond += f" AND ({fcm})"
        params.update(filterParams)
        sql = (f"SELECT DISTINCT p.{column} AS dep_key, p.{pk} AS dep_id,\n"
               f"       {periodExpr} AS dep_period\n"
               f"FROM ( {ctx['selectSql']} ) p\nWHERE {cond}")
        cursor.execute(sql, params)
        out.extend((row[0], row[1], _normalizePeriod(row[2]))
                   for row in cursor.fetchall())
    return out


def _resolveDependencyCandidates(ctx, cfg, dep, values):
    """Строки ведущей, СВЯЗАННЫЕ с чужими ключами, — без условий запроса линии.

    -> [(значение колонки, ключ строки ведущей)].

    Спрашиваем саму таблицу, а не запрос линии, и в этом весь смысл. Запрос
    отдаёт только то, что подходит под условие СЕЙЧАС; когда DIRDT обнулили,
    строка из него исчезает — и по ней уже не узнать, что удалять из ведомой.
    Таблица связь помнит: `reqprepmo.reqprepsmo_idrw` никуда не делся.

    Разница между этим списком и тем, что отдал запрос, и есть «строки, которые
    надо снести»: связь осталась, а условию строка больше не отвечает.
    """
    values = list(values)
    if not values:
        return []
    dbMaster = cfg["dbMaster"]
    table = dep.get("masterTable") or cfg.get("tableNameMaster")
    if not table:
        raise ValueError(
            "Зависимость: не знаю, у какой таблицы спрашивать связанные строки. "
            "Укажите tableNameMaster у линии или masterTable у зависимости.")
    key = dep.get("masterKey") or ctx["pkColsMaster"][0]
    column = dep["column"]
    cursor = ctx["cursorMaster"]
    out = []
    for chunk in _chunks(values):
        inClause, params = _bindInList(dbMaster, chunk, "v")
        # filterClauseMaster здесь НЕ применяем намеренно: он часть условия
        # «строка подходит», а нам нужны как раз все связанные, включая уже не
        # подходящие. Иначе исчезнувшая строка не нашлась бы и тут.
        sql = (f"SELECT DISTINCT {column} AS dep_key, {key} AS dep_id\n"
               f"FROM {table}\nWHERE {column} IN ({inClause})")
        cursor.execute(sql, params)
        out.extend((row[0], row[1]) for row in cursor.fetchall())
    return out


def _dependencySpecs(cfg):
    """Нормализовать `iudDependencies` из конфига. -> [dict, ...].

    Обязательны два поля: `tablename` (метка в журнале) и `column` (колонка
    связи). Необязательные `masterTable` / `masterKey` нужны, когда связь надо
    искать не в `tableNameMaster` или ключ строки называется в таблице иначе,
    чем в запросе линии.

        "iudDependencies": [{"tablename": "fublin", "column": "reqprepsmo_idrw"}]
    """
    out = []
    for item in cfg.get("iudDependencies") or []:
        dep = dict(zip(("tablename", "column"), item)) if isinstance(
            item, (list, tuple)) else dict(item)
        dep.setdefault("tablename", dep.pop("tableName", None))
        if not dep.get("tablename") or not dep.get("column"):
            raise ValueError(
                "iudDependencies: у каждой зависимости обязательны 'tablename' "
                f"(метка в журнале) и 'column' (колонка связи), получено {item!r}")
        dep["tablename"] = str(dep["tablename"])
        dep["column"] = str(dep["column"])
        out.append(dep)
    return out


def _selectDependencyWork(cursor, cfg, ctx):
    """События ЧУЖИХ таблиц из журнала -> работа в терминах ведущей.

    -> (extraDistinct, missing, byDepIdrw):
        extraDistinct — [(recId, период, timeoper, 'IU')] для строк, которые
                        запрос линии отдаёт СЕЙЧАС;
        missing       — [recId] — связь есть, а запрос строку больше не отдаёт:
                        её надо снести из ведомой;
        byDepIdrw     — {idrw журнальной строки зависимости: {recId, ...}} —
                        кто за что отвечает.

    Разбор идёт «как delete_insert»: сравниваем два списка — что СВЯЗАНО с
    чужим ключом (спрашиваем таблицу) и что запрос отдаёт СЕЙЧАС. Первое минус
    второе — строки, переставшие подходить под условие; их удаляем. Иначе
    обнулённый DIRDT остался бы незамеченным: событие приходит, запрос по ключу
    молчит, строка в ведомой живёт дальше.

    Зачем отдельная бухгалтерия по byDepIdrw: одна чужая строка разворачивается
    в НЕСКОЛЬКО строк ведущей. Пометь её обработанной по первой удавшейся — и
    падение второй потеряется совсем: своих журнальных строк у той нет,
    парковать нечего. Поэтому зависимость закрывается, только когда доехали ВСЕ
    её строки.

    Тип операции чужой строки ('IU' или 'D') намеренно не различается, и это
    главное решение здесь. Триггер на чужой таблице остаётся глупым: он говорит
    «строка с таким ключом трогалась», и всё. Что из этого следует, выясняется
    сравнением двух списков. Разбирать в триггере, появилась строка в выборке
    или исчезла, значило бы продублировать в нём условие запроса линии со всеми
    его JOIN'ами.
    """
    deps = _dependencySpecs(cfg)
    if not deps:
        return [], [], {}

    sqlTpl = _pickSql(cfg["dbMaster"], selectEtlIudPostSql, selectEtlIudOrclSql)
    latest = {}                       # (период, recId) -> запись работы
    missing = {}                      # _idKey -> recId (как отдала таблица)
    byDepIdrw = {}

    for dep in deps:
        tablename, column = dep["tablename"], dep["column"]
        rows = _executeQuery(cursor, sqlTpl, {"tablename": tablename})
        if not rows:
            logger.info("Зависимость %s: в журнале ничего нет", tablename)
            continue

        idrwsByValue = defaultdict(list)
        for depId, idrw, _period, _timeoper, _oper in rows:
            idrwsByValue[_idKey(depId)].append(idrw)
            byDepIdrw.setdefault(idrw, set())
        values = list(idrwsByValue)

        live = _resolveDependencyIds(ctx, cfg, column, values)
        liveIds = {_idKey(recId) for _k, recId, _p in live}
        for depKey, recId, period in live:
            latest[(period, recId)] = (recId, period, ctx["currDt"], "IU")
            for idrw in idrwsByValue.get(_idKey(depKey), ()):
                byDepIdrw[idrw].add(recId)

        # Связь есть, а строки в выборке нет — значит она из неё ВЫПАЛА.
        gone = 0
        for depKey, recId in _resolveDependencyCandidates(ctx, cfg, dep, values):
            if _idKey(recId) in liveIds:
                continue
            missing[_idKey(recId)] = recId
            gone += 1
            for idrw in idrwsByValue.get(_idKey(depKey), ()):
                byDepIdrw[idrw].add(recId)

        logger.info(
            "Зависимость %s: событий %d, ключей %d, строк к переносу %d, "
            "выпавших из выборки %d", tablename, len(rows), len(values),
            len(live), gone)

    return list(latest.values()), list(missing.values()), byDepIdrw


def _fetchMasterRowsById(ctx, cfg, ids):
    """{ключ -> [строки ведущей]} для набора id. Ключ совместим с _recIdKey."""
    pkCols = ctx["pkColsMaster"]
    grouped = defaultdict(list)
    if len(pkCols) == 1:
        rows = _selectMasterRowsByIds(ctx, cfg, ids)
        colsLower = [c.lower() for c in _columnNames(ctx["structMaster"])]
        pkIdx = colsLower.index(pkCols[0].lower())
        for r in rows:
            grouped[_idKey(r[pkIdx])].append(r)
    else:
        for recId in ids:
            grouped[recId] = _selectMasterRowsByIds(ctx, cfg, [recId])
    return grouped


def _fetchSlavePeriodsById(cursorSlave, cfg, ctx, ids):
    """{ключ -> set(периодов)} — DISTINCT период ведомой по id, батч-запросами.

    Нужно delete_insert: реальные затронутые группы etl_jobs со стороны
    ведомой (до удаления). Раньше был SELECT на каждый id."""
    ids = list(ids)
    if not ids:
        return {}
    dbSlave = cfg["dbSlave"]
    pkCols = ctx["pkColsSlave"]
    tableNameSlave = cfg["tableNameSlave"]
    fcs = _asAndClause(cfg.get("filterClauseSlave"))
    periodExpr = _periodExpr(dbSlave, _periodSpec(cfg["slavePeriodColumn"]),
                             None, cfg.get("truncatePeriod"))
    grouped = defaultdict(set)

    def _run(idChunk):
        inClause, params = _bindInList(dbSlave, idChunk, "v")
        pk = pkCols[0]
        cond = f"{pk} IN ({inClause})"
        if fcs:
            cond += f" AND ({fcs})"
        sql = (f"SELECT DISTINCT {pk} AS idv, {periodExpr} AS createdate "
               f"FROM {tableNameSlave} WHERE {cond}")
        cursorSlave.execute(sql, params)
        for idv, period in cursorSlave.fetchall():
            grouped[_idKey(idv)].add(_normalizePeriod(period))

    if len(pkCols) == 1:
        for chunk in _chunks(ids):
            _run(chunk)
    else:
        # составной PK — по одному через готовый per-id SQL
        for recId in ids:
            cursorSlave.execute(ctx["slavePeriodsByIdSql"],
                                _splitPkValue(recId, len(pkCols)))
            acc = {_normalizePeriod(r[0]) for r in cursorSlave.fetchall()}
            grouped[recId] = acc
    return grouped

def _columnType(jsonStruct, name):
    nl = name.lower()
    for f in jsonStruct:
        if f[0].lower() == nl:
            return f[1] or ""
    return ""


def _isNumericType(dataType):
    t = (dataType or "").upper()
    return any(k in t for k in ("NUMBER", "NUMERIC", "INT", "DECIMAL",
                                "FLOAT", "DOUBLE", "REAL"))


# SYS.ODCINUMBERLIST/ODCIVARCHAR2LIST — VARRAY(32767); берём с запасом.
_ORA_COLL_MAX = 30000


def _selectMasterRowsByIds(ctx, cfg, ids):
    """Строки ведущей для набора id, считая тяжёлый исходник минимум раз.

    Postgres: ОДИН запрос `pk = ANY(:ids)` (без лимита на размер).
    Oracle: `pk IN (TABLE(:ids))` через табличную коллекцию SYS.ODCI*LIST,
    чанками до 32767 элементов — для 42k id это 2 прохода исходника вместо
    ~43 (по проходу на каждую 1000 при IN-списке). При недоступности типа
    коллекции — fallback на IN-чанки. Составной PK — тоже через IN-чанки.
    """
    ids = list(ids)
    if not ids:
        return []
    dbMaster = cfg["dbMaster"]
    pkCols = ctx["pkColsMaster"]
    if len(pkCols) != 1:
        return _masterRowsInChunks(ctx, cfg, ids)

    pk = pkCols[0]
    numeric = _isNumericType(_columnType(ctx["structMaster"], pk))
    fieldsStr = _buildFieldsStr(dbMaster, ctx["structMaster"], cfg["periodColumn"])
    selectSql = ctx["selectSql"]
    fcm = _asAndClause(cfg.get("filterClauseMaster"))
    filterParams = cfg.get("filterParams") or {}
    tpl = _pickSql(dbMaster, recordSelectByIdPostSql, recordSelectByIdOrclSql)
    cursor = ctx["cursorMaster"]

    if _isPost(dbMaster):
        arr = [int(i) for i in ids] if numeric else [str(i) for i in ids]
        cond = f"p.{pk} = ANY(%(ids)s)"
        if fcm:
            cond += f" AND ({fcm})"
        cursor.execute(tpl.format(fields_str=fieldsStr, select_sql=selectSql,
                                  primary_cond=cond), {"ids": arr, **filterParams})
        return cursor.fetchall()

    # Oracle — табличная коллекция (исходник считается 1 раз на чанк <=32767)
    con = ctx["conMaster"]
    typeName = "SYS.ODCINUMBERLIST" if numeric else "SYS.ODCIVARCHAR2LIST"
    try:
        collType = con.gettype(typeName)
    except Exception as err:
        logger.warning("Коллекция %s недоступна (%s) — fallback на IN-чанки",
                       typeName, err)
        return _masterRowsInChunks(ctx, cfg, ids)
    cond = f"p.{pk} IN (SELECT column_value FROM TABLE(:ids))"
    if fcm:
        cond += f" AND ({fcm})"
    sql = tpl.format(fields_str=fieldsStr, select_sql=selectSql, primary_cond=cond)
    rows = []
    for chunk in _chunks(ids, _ORA_COLL_MAX):
        obj = collType.newobject()
        obj.extend([int(i) for i in chunk] if numeric else [str(i) for i in chunk])
        cursor.execute(sql, {"ids": obj, **filterParams})
        rows.extend(cursor.fetchall())
    return rows

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
    marks, phase = _phaseTimer()
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()
        phase("connect")

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
        phase("structCheck")

        # isetl: 0 -> 1 в etl_log_iud_row для уже зафиксированных записей
        # этой группы — они будут перезалиты вместе со всей группой.
        cursorMaster.execute(
            _pickSql(dbMaster, etlStatusChangePostSql, etlStatusChangeOrclSql)
            .format(period_cond=_journalPeriodCond(
                dbMaster, createdate, truncate=cfg.get("truncatePeriod"))),
            {"tablename": tableNameEtlJobs,
             **_journalPeriodBind(createdate)},
        )
        logger.info("Журнал: отмечено обработанными %s строк группы %s",
                    _rowcount(cursorMaster), createdate)
        phase("journal")

        # 1) удалить все записи группы из ведомой. Параметры периода зависят от
        # того, одиночная колонка-дата или составная (year/month/day), а сам
        # запрос — от того, обычная это группа или NULL (см. _periodCond).
        cursorSlave.execute(
            _pickPeriodSql(ctx["deletePeriodSql"], createdate),
            _periodBinds(_periodSpec(cfg["slavePeriodColumn"]), createdate))
        logger.info("Ведомая: удалено %s строк группы %s",
                    _rowcount(cursorSlave), createdate)
        phase("delete")
        # 2) выбрать актуальные записи из ведущей и залить. Бинда tablename
        # здесь больше нет: он был нужен убранному JOIN'у с etl_jobs, а лишний
        # бинд Oracle не принимает (ORA-01036).
        cursorMaster.execute(
            _pickPeriodSql(ctx["recordGroupSql"], createdate),
            {**_periodBinds(_periodSpec(cfg["periodColumn"]), createdate),
             **(cfg.get("filterParams") or {})})
        phase("masterExec")
        records = cursorMaster.fetchall()
        phase("masterFetch")
        logger.info("Найдено %d записей для группы %s", len(records), createdate)

        _bulkUpsert(cursorSlave, dbSlave, ctx["upsertSql"],
                    ctx["structSlave"], records)
        phase("upsert")

        # 3) обновить etl_jobs (last_success_ts, isokaudit=0)
        cursorMaster.execute(
            _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
            {"LAST_SUCCESS_TS": ctx["currDt"],
             "TABLENAME": tableNameEtlJobs,
             "PERIOD": _periodBind(createdate)},
        )

        conMaster.commit()
        conSlave.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, len(records), createdate)
        phase("commit")
        logger.info("Группа %s успешно обработана: %d записей, %.1fс (%s)",
                    createdate, len(records),
                    _phasesTotal(marks, _GROUP_PHASE_LABELS),
                    _phasesText(marks, _GROUP_PHASE_LABELS))
    except Exception as err:
        if isShutdown(err):
            # SIGTERM — не ошибка группы. Статус НЕ трогаем: группа остаётся
            # с isokaudit=4 и будет перезалита следующим запуском. Пометили бы
            # -1 — пришлось бы руками возвращать 4.
            _safeRollback(conMaster)
            _safeRollback(conSlave)
            logger.warning("SIGTERM на группе %s — останавливаюсь, статус "
                           "группы не меняю (останется в очереди).", createdate)
            raise ShutdownRequested(
                f"Линия {tableNameEtlJobs}: SIGTERM на группе {createdate}, "
                f"группа осталась в очереди — чинить руками ничего не нужно."
            ) from err
        if conMaster:
            conMaster.rollback()
        if conSlave:
            conSlave.rollback()
        cursorMaster.execute(
            _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
            {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
             "PERIOD": _periodBind(createdate)},
        )
        conMaster.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, 0, createdate)
        logger.error("Ошибка обработки группы %s: %s", createdate, err)
        raise
    finally:
        if conSlave:
            conSlave.close()


# Сколько строк psycopg2 склеивает в один запрос к серверу (execute_batch).
_PG_BATCH_PAGE = 1000
# Сколько строк уходит в Oracle одним executemany (array DML).
_ORCL_BATCH_ROWS = 1000


def _bulkUpsert(cursorSlave, dbSlave, upsertSql, structSlave, records):
    """Вставка/обновление пачки записей в ведомой — ПАЧКОЙ, а не по строке.

    Postgres: execute_batch вместо executemany. psycopg2.executemany гоняет
    ОТДЕЛЬНЫЙ round-trip на каждую строку — на группах в ~90 тыс. строк
    (medree_prdisp) это часы вместо минут. execute_batch склеивает по
    _PG_BATCH_PAGE операторов в один запрос; SQL и параметры те же, поэтому
    работает одинаково и с INSERT, и с INSERT ... ON CONFLICT.

    Oracle: executemany (array DML) — один round-trip на пачку. Здесь был цикл
    по строкам, и замер это показал: заливка 29 999 записей iperson занимала
    106.7 с при том, что сама выборка группы — 0.4 с. Это ~3.5 мс на строку,
    то есть чистая сетевая цена round-trip, а не работа БД.

    Пачки по _ORCL_BATCH_ROWS: cx_Oracle под executemany выделяет буферы на всю
    пачку сразу, и группа в сотни тысяч строк одним вызовом съела бы память.

    Если пачка не прошла — повторяем её по строке. У Oracle-ведомой запись
    всегда идёт MERGE (см. _buildUpsertSql), то есть повтор уже применённых
    строк безопасен; зато ошибка называет конкретную строку, а не всю пачку.

    По итогам пишем ОДНУ строку в лог: сколько строк ушло пачками, сколько
    пачек пришлось повторять по одной и сколько это заняло. Без неё по фазе
    «заливка в ведомую» нельзя отличить «пачки не работают, всё идёт по
    строке» от «пачки работают, но столько времени тратит сама СУБД» —
    а лечится это совершенно по-разному.

    Для Oracle к этому добавлены времена первой и последней пачки и самая
    долгая. Все пачки одинаковые по размеру и по форме запроса, поэтому
    ровные времена означают постоянную цену строки (упирается в диски или
    в сопровождение индексов), а растущие от первой к последней — что цена
    поднимается по ходу заливки самой группы.
    """
    if not records:
        return
    started = time.monotonic()

    if _isPost(dbSlave):
        psycopg2.extras.execute_batch(cursorSlave, upsertSql, records,
                                      page_size=_PG_BATCH_PAGE)
        logger.info("Заливка: %d строк пачками по %d, %.1fс (%.2f мс/строка)",
                    len(records), _PG_BATCH_PAGE, time.monotonic() - started,
                    (time.monotonic() - started) * 1000.0 / len(records))
        return

    # Oracle: позиционные параметры :1 .. :N — те же, что и при построчной
    # записи, просто списком.
    fellBack = rowsFellBack = 0
    times = []          # секунды на пачку, по порядку
    for chunk in _chunks(records, _ORCL_BATCH_ROWS):
        rows = [{str(i + 1): value for i, value in enumerate(record)}
                for record in chunk]
        chunkStarted = time.monotonic()
        try:
            cursorSlave.executemany(upsertSql, rows)
        except Exception as err:
            fellBack += 1
            rowsFellBack += len(rows)
            logger.warning("Пачка из %d строк не прошла (%s: %s) — повторяю "
                           "по одной, чтобы стало видно, на какой именно",
                           len(rows), type(err).__name__, str(err)[:200])
            for params in rows:
                cursorSlave.execute(upsertSql, params)
        times.append(time.monotonic() - chunkStarted)
    spent = time.monotonic() - started
    logger.info("Заливка: %d строк, %d пачек по %d, повторено по строке %d "
                "пачек (%d строк), %.1fс (%.2f мс/строка); пачки: первая "
                "%.1fс, последняя %.1fс, самая долгая %.1fс",
                len(records), len(times), _ORCL_BATCH_ROWS, fellBack,
                rowsFellBack, spent, spent * 1000.0 / len(records),
                times[0], times[-1], max(times))


# ----------------------------------------------------------------------------
#     query_section: группы для перезаливки берутся из своего SQL (periodsSql)
# ----------------------------------------------------------------------------

def _processQuerySection(cfg, ctx):
    """Режим query_section (например, Medree_prdisp Oracle->Postgres).

    Отличие от других режимов ТОЛЬКО в источнике списка групп: их даёт
    пользовательский `periodsSql`, а не журнал etl_log_iud_row и не сравнение
    срезов. Дальше всё как везде: группа регистрируется в etl_jobs и
    перезаливается общим _processGroupUpdate (DELETE группы + заливка), который
    сам обновляет etl_jobs.last_success_ts/isokaudit и пишет etl_log.

    Форма строк periodsSql должна соответствовать periodColumn ведущей:
      * одиночная колонка-дата -> одна колонка с датой периода;
      * составной период       -> колонки year[, month[, day]] в этом порядке.
    """
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    dbMaster = cfg["dbMaster"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]

    periodsSqlPath = cfg.get("periodsSql")
    if not periodsSqlPath:
        raise AirflowFailException(
            "query_section: не задан periodsSql (запрос, возвращающий группы)")
    periodsSql = TakeOneQuery(_resolveEtlPath(periodsSqlPath))
    rows = _executeQuery(cursorMaster, periodsSql)
    if not rows:
        logger.info("query_section: periodsSql не вернул групп — нечего обновлять")
        raise AirflowSkipException

    spec = _periodSpec(cfg["periodColumn"])
    periods = sorted({_periodFromParts(spec, row) for row in rows},
                     key=_periodSortKey)
    logger.info("query_section: групп к обновлению — %d", len(periods))

    # Группы должны быть в etl_jobs — тогда их видит аудит и общий механизм
    # статусов (isokaudit), как у остальных режимов.
    _registerAffectedPeriods(cursorMaster, dbMaster, periods, tableNameEtlJobs)
    conMaster.commit()

    for period in periods:
        _processGroupUpdate(cfg, ctx, (period,))


def _periodFromParts(spec, row):
    """Период (date) из строки periodsSql: либо готовая дата, либо year/month/day."""
    if spec["kind"] == "single":
        return _normalizePeriod(row[0])
    year = int(row[0])
    month = int(row[1]) if spec["month"] and len(row) > 1 else 1
    day = int(row[2]) if spec["day"] and len(row) > 2 else 1
    return datetime.date(year, month, day)


# ----------------------------------------------------------------------------
#                       Индивидуальные обновления (etl_log_iud_row)
# ----------------------------------------------------------------------------

def _processIndividualUpdates(cfg, ctx, distinctIds, iudRecords,
                              byDepIdrw=None, depMissing=()):
    """Точечная синхронизация по событиям из etl_log_iud_row.

    ИЗМЕНЕНИЕ: Master-запрос может вернуть НЕСКОЛЬКО записей для одного id.
    Все записи обрабатываются атомарно: если одна упала — роллбек всей пачки.

    byDepIdrw — {idrw журнальной строки ЗАВИСИМОСТИ: {recId, ...}}. Такие строки
    помечаются не по ходу цикла, а в конце: одна чужая строка разворачивается в
    несколько строк ведущей, и закрыть её по первой удавшейся значило бы
    потерять падение второй — своих журнальных строк у той нет, парковать
    нечего.
    """
    distinctIds = list(distinctIds)
    if not distinctIds and not byDepIdrw:
        logger.info("Записи для индивидуального обновления отсутствуют")
        return

    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    action = "ETL_LOG_IUD_ROW"

    logger.info("Старт индивидуальных обновлений: %d записей", len(distinctIds))

    # idrw по recId (для пометки isetl) — O(1) вместо обхода iudRecords на каждый id.
    idrwsByRecId = defaultdict(list)
    for rid, idrw in iudRecords:
        idrwsByRecId[rid].append(idrw)
    pkKey = lambda recId: _recIdKey(recId, ctx["pkColsMaster"])

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

        # Строки, выпавшие из выборки по зависимости, разворачиваем в удаления
        # ЗДЕСЬ, а не раньше: период для них знает только ведомая — в ведущей
        # такой строки уже нет. Период нужен по-настоящему: по нему ведётся
        # etl_jobs, и удаление, приписанное чужой группе, испортило бы отчётность
        # обеим.
        if depMissing:
            slavePeriods = _fetchSlavePeriodsById(cursorSlave, cfg, ctx, depMissing)
            added = 0
            for recId in depMissing:
                for period in sorted(slavePeriods.get(pkKey(recId), ()),
                                     key=lambda p: (p is not None, p)):
                    distinctIds.append((recId, period, ctx["currDt"], "D"))
                    added += 1
            logger.info("Зависимости: к удалению из ведомой %d строк "
                        "(по %d ключам)", added, len(depMissing))

        groupsData = _groupSummary(distinctIds)
        # Префетч строк ведущей по ВСЕМ IU-id одним батчем (IN, чанки по 1000),
        # а не SELECT на каждую запись — главное ускорение при тысячах изменений.
        iuIds = [e[0] for e in distinctIds if e[3] == "IU"]
        masterById = _fetchMasterRowsById(ctx, cfg, iuIds)

        recordErrors = []  # [(recId, period, errStr), ...]
        shutdown = False   # получен SIGTERM -> выходим, ничего не паркуя

        for entry in distinctIds:
            recId, period, _timeoper, oper = entry[0], entry[1], entry[2], entry[3]

            try:
                pkParams = _splitPkValue(recId, len(ctx["pkColsMaster"]))

                if oper == "IU":
                    # строки уже выбраны батчем — берём из префетча
                    rowsDb = masterById.get(pkKey(recId), [])

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
                                #print(ctx["upsertSql"], params)
                                cursorSlave.execute(ctx["upsertSql"], params)

                else:  # 'D' — удаление
                    # Для DELETE удаляем ВСЕ записи с этим PK из slave
                    cursorSlave.execute(ctx["deleteByIdSql"], pkParams)
                    logger.info("Запись id=%s: удалена из ведомой", recId)

                # Коммитим только после успешной обработки ВСЕХ записей для этого id
                conSlave.commit()

                # Помечаем записи в etl_log_iud_row как обработанные
                idrws = idrwsByRecId.get(recId, [])
                _markEtlIud(cursorMaster, dbMaster, idrws, 1)
                conMaster.commit()

            except Exception as err:
                if isShutdown(err):
                    # SIGTERM (деплой / перезапуск airflow) — НЕ ошибка данных.
                    # Не паркуем: запись остаётся isetl=0 и уедет следующим
                    # запуском. Выходим сразу и минимумом действий: airflow шлёт
                    # SIGTERM повторно, и любая лишняя работа здесь ловит его
                    # снова (в проде это давало каскад ложных -1 и даже
                    # RecursionError внутри логирования).
                    _safeRollback(conSlave)
                    logger.warning(
                        "SIGTERM на id=%s — останавливаюсь. Запись НЕ "
                        "припаркована: останется в журнале (isetl=0) и уедет "
                        "следующим запуском.", recId)
                    shutdown = True
                    break

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
                idrws = idrwsByRecId.get(recId, [])
                _markEtlIud(cursorMaster, dbMaster, idrws, -1)
                conMaster.commit()
                recordErrors.append((recId, period, str(err)))
                _markFailGroup(groupsData, period)

        if shutdown:
            # Хвостовую бухгалтерию (регистрация периодов, etl_jobs, etl_log)
            # НЕ трогаем: соединения уже под сигналом, а любой лишний запрос
            # рискует упасть и превратить чистый ⬜ в 🟥. Записи, успевшие
            # переехать, помечены isetl=1 покомандно и не потеряются;
            # last_success_ts обновит следующий запуск.
            if recordErrors:
                logger.error(
                    "До SIGTERM припарковано %d записей (isetl=-1): %s. "
                    "Повторить: UPDATE etl_log_iud_row SET isetl=0 "
                    "WHERE tablename='%s' AND isetl=-1;",
                    len(recordErrors),
                    ", ".join(str(r[0]) for r in recordErrors[:5]),
                    tableNameEtlJobs)
            raise ShutdownRequested(
                f"Линия {tableNameEtlJobs}: получен SIGTERM, перенос прерван. "
                f"Необработанные записи остались в журнале (isetl=0) и уедут "
                f"следующим запуском — чинить руками ничего не нужно.")

        # Закрыть журнальные строки ЗАВИСИМОСТЕЙ — только теперь, когда видно
        # судьбу всех строк, которые по ним нашлись. Зависимость без единой
        # найденной строки тоже закрывается: работы по ней нет, а держать её в
        # журнале значит перебирать её каждый прогон вечно.
        if byDepIdrw:
            failed = {r[0] for r in recordErrors}
            done = [idrw for idrw, recIds in byDepIdrw.items()
                    if not (recIds & failed)]
            parked = [idrw for idrw, recIds in byDepIdrw.items()
                      if recIds & failed]
            _markEtlIud(cursorMaster, dbMaster, done, 1)
            _markEtlIud(cursorMaster, dbMaster, parked, -1)
            conMaster.commit()
            logger.info("Зависимости: закрыто %d журнальных строк, "
                        "припарковано %d", len(done), len(parked))

        # Зарегистрировать затронутые периоды (из лога) — без полного скана
        # источника: новые createdate приходят только вместе с событиями iud.
        _registerAffectedPeriods(cursorMaster, dbMaster,
                                 [g[0] for g in groupsData], tableNameEtlJobs)

        for groupId, count, status in groupsData:
            if status == "ok":
                cursorMaster.execute(
                    _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
                    {"LAST_SUCCESS_TS": ctx["currDt"],
                     "TABLENAME": tableNameEtlJobs,
                     "PERIOD": _periodBind(groupId)},
                )
                UpdateLog(tableNameEtlJobs, dbMaster, action,
                          cursorMaster, conMaster, count, groupId, 1)
            else:
                cursorMaster.execute(
                    _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
                    {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
                     "PERIOD": _periodBind(groupId)},
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

    Событийный, как iud (читает etl_log_iud_row по idrw), но на каждое событие
    id делает ОДНО И ТО ЖЕ, независимо от типа операции: удаляет ВСЕ строки
    этого id в ведомой и заливает то, что СЕЙЧАС отдаёт по нему ведущая. Если
    ведущая больше ничего не отдаёт (удалили последнюю строку) — ничего не
    вставляется. Это убирает «осиротевшие» строки при смене doctype (старый
    doctype=2 idrw=123 остаётся, когда запись переехала в doctype=3), чего
    upsert в режиме iud не ловит.

    Почему oper (IU/D) НЕ используется: у одного id может быть несколько строк
    ведущей. При разделении на IU/D событие 'D' по одной из них удаляло все
    строки id и не возвращало те, что в ведущей ещё есть — то есть терялись
    живые данные. Источник истины — текущее состояние ведущей, а не тип события.

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
    # Период строки ведущей: одна колонка-дата либо составной (year/month/day) —
    # собирается из строки по именам колонок (регистронезависимо: Oracle отдаёт
    # ВЕРХНИЙ регистр, Postgres — нижний).
    masterPeriodSpec = _periodSpec(cfg["periodColumn"])
    masterColNames = _columnNames(ctx["structMaster"])

    logger.info("Старт delete_insert: %d событий", len(distinctIds))
    okPeriods, failPeriods = set(), set()
    periodCount = defaultdict(int)
    recordErrors = []  # [(recId, errStr), ...]
    shutdown = False   # получен SIGTERM -> выходим, ничего не паркуя

    # idrw по recId (для пометки isetl) — O(1).
    idrwsByRecId = defaultdict(list)
    for rid, idrw in iudRecords:
        idrwsByRecId[rid].append(idrw)
    # Префетч строк ведущей по ВСЕМ затронутым id одним батчем.
    # ВАЖНО: берём все id, а НЕ только IU. Тип операции (IU/D) из журнала здесь
    # не используется — источником истины является текущее состояние ведущей
    # (см. комментарий в цикле ниже).
    allEventIds = [e[0] for e in distinctIds]
    masterById = _fetchMasterRowsById(ctx, cfg, allEventIds)
    mKey = lambda recId: _recIdKey(recId, ctx["pkColsMaster"])
    sKey = lambda recId: _recIdKey(recId, ctx["pkColsSlave"])

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

        # Префетч периодов ведомой по ВСЕМ id одним батчем (до удаления),
        # вместо SELECT DISTINCT createdate на каждый id.
        allIds = [e[0] for e in distinctIds]
        slavePeriodsById = _fetchSlavePeriodsById(cursorSlave, cfg, ctx, allIds)

        for entry in distinctIds:
            recId = entry[0]
            # реальные периоды удаляемых строк — из префетча ведомой (до удаления)
            affected = set(slavePeriodsById.get(sKey(recId)) or ())
            try:
                pkParamsSlave = _splitPkValue(recId, len(ctx["pkColsSlave"]))

                # Тип операции из журнала (IU/D) НЕ используется — и это принципиально.
                # Одному id может соответствовать НЕСКОЛЬКО строк ведущей (expmed:
                # разные doctype). Если разделять по oper, то событие 'D' на одной из
                # них удаляло все строки id и не возвращало те, что в ведущей ещё
                # есть. Поэтому всегда одинаково: удалить все строки id в ведомой и
                # залить то, что СЕЙЧАС отдаёт ведущая (её текущее состояние —
                # источник истины). Если ведущая по этому id больше ничего не
                # отдаёт (удалили последнюю строку) — просто не вставляем ничего.
                cursorSlave.execute(ctx["deleteByIdSql"], pkParamsSlave)

                rowsDb = masterById.get(mKey(recId), [])
                for rowDb in rowsDb:
                    if _isPost(dbSlave):
                        cursorSlave.execute(ctx["upsertSql"], rowDb)
                    else:
                        params = {str(i + 1): v for i, v in enumerate(rowDb)}
                        cursorSlave.execute(ctx["upsertSql"], params)
                    # реальный период вставляемой строки — из ведущей
                    p = _periodFromRow(masterPeriodSpec, rowDb, masterColNames)
                    if p is not None:
                        affected.add(p)
                        periodCount[p] += 1
                logger.info("idrw=%s: -all/+%d строк, периоды %s",
                            recId, len(rowsDb),
                            sorted(affected, key=_periodSortKey))

                conSlave.commit()

                idrws = idrwsByRecId.get(recId, [])
                _markEtlIud(cursorMaster, dbMaster, idrws, 1)
                conMaster.commit()

                okPeriods |= affected

            except Exception as err:
                if isShutdown(err):
                    # SIGTERM (деплой / перезапуск airflow) — не ошибка данных,
                    # см. одноимённую ветку в _processIndividualUpdates.
                    _safeRollback(conSlave)
                    logger.warning(
                        "SIGTERM на idrw=%s — останавливаюсь. Запись НЕ "
                        "припаркована: останется в журнале (isetl=0) и уедет "
                        "следующим запуском.", recId)
                    shutdown = True
                    break

                cls = classifyError(err) or "record"
                if conSlave:
                    conSlave.rollback()
                if cls != "record":
                    logger.error("Ошибка idrw=%s КЛАСС=%s — прерываю цикл: %s",
                                 recId, cls, err)
                    raise
                logger.error("RECORD-ошибка idrw=%s, паркую isetl=-1: %s",
                             recId, err)
                idrws = idrwsByRecId.get(recId, [])
                _markEtlIud(cursorMaster, dbMaster, idrws, -1)
                conMaster.commit()
                recordErrors.append((recId, str(err)))
                # затронутые этой записью периоды — под подозрение (-1)
                failPeriods |= affected

        if shutdown:
            # Хвостовую бухгалтерию не трогаем — см. _processIndividualUpdates.
            if recordErrors:
                logger.error(
                    "До SIGTERM припарковано %d idrw (isetl=-1): %s. "
                    "Повторить: UPDATE etl_log_iud_row SET isetl=0 "
                    "WHERE tablename='%s' AND isetl=-1;",
                    len(recordErrors),
                    ", ".join(str(r[0]) for r in recordErrors[:5]),
                    tableNameEtlJobs)
            raise ShutdownRequested(
                f"Линия {tableNameEtlJobs}: получен SIGTERM, перенос прерван. "
                f"Необработанные записи остались в журнале (isetl=0) и уедут "
                f"следующим запуском — чинить руками ничего не нужно.")

        # зарегистрировать реально затронутые периоды (из данных, без скана
        # источника) — затем обновить статус
        _registerAffectedPeriods(
            cursorMaster, dbMaster,
            # NULL-группу НЕ выбрасываем: она такая же группа, просто без даты.
            list(okPeriods | failPeriods), tableNameEtlJobs,
        )

        for period in okPeriods - failPeriods:
            cursorMaster.execute(
                _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
                {"LAST_SUCCESS_TS": ctx["currDt"],
                 "TABLENAME": tableNameEtlJobs,
                 "PERIOD": _periodBind(period)},
            )
            UpdateLog(tableNameEtlJobs, dbMaster, action,
                      cursorMaster, conMaster, periodCount.get(period, 0),
                      period, 1)
        for period in failPeriods:
            cursorMaster.execute(
                _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
                {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
                 "PERIOD": _periodBind(period)},
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
    """[(period, count, 'ok')] — для последующего пересчёта статусов.

    NULL-период УЧАСТВУЕТ наравне с остальными. Раньше он отбрасывался, и
    записи с пустым периодом переносились, но статус группы в etl_jobs не
    получали — то есть аудит о них не узнавал вовсе. Для expmed это не
    экзотика: поле-период какое-то время пустое, и это нормальное состояние
    данных. Сортировка идёт через _periodSortKey (обычный sorted на смеси date
    и None падает), а в SQL NULL сравнивается через COALESCE-сентинел."""
    counts = {}
    for entry in distinctIds:
        period = entry[1]
        counts[period] = counts.get(period, 0) + 1
    return [[period, counts[period], "ok"]
            for period in sorted(counts.keys(), key=_periodSortKey)]


def _markFailGroup(groupsData, period):
    for row in groupsData:
        if row[0] == period and row[2] == "ok":
            row[2] = "fail"
            break


# ----------------------------------------------------------------------------
#         Режим section_compare (mocheck / medree): сравнение master vs slave
# ----------------------------------------------------------------------------

def _buildMasterPeriodsSql(dbMaster, selectSql, periodColumn):
    """SQL среза ведущей: (период, lastupdate, счётчик)."""
    sqlTpl = _pickSql(dbMaster, dateSelectMasterPostSql, dateSelectMasterOrclSql)
    return sqlTpl.format(
        select_sql=selectSql,
        period_expr=_periodExpr(dbMaster, _periodSpec(periodColumn), "p"),
    )


def _buildSlavePeriodsSql(dbSlave, tableNameSlave, slavePeriodColumn,
                          filterClauseSlave):
    """SQL среза ведомой — то же, что у ведущей, плюс фильтр линии."""
    sqlTpl = _pickSql(dbSlave, dateSelectSlavePostSql, dateSelectSlaveOrclSql)
    return sqlTpl.format(
        tablename=tableNameSlave,
        period_expr=_periodExpr(dbSlave, _periodSpec(slavePeriodColumn)),
        filter=_asAndClause(filterClauseSlave) or "1=1",
    )


def _selectMasterPeriods(cursor, dbMaster, selectSql, periodColumn):
    """Уникальные (createdate, max(lastupdate)) на стороне ведущей.

    periodColumn — имя колонки группировки в источнике (createdate / dcalc /
    reqdt и т.п.).
    """
    return _executeQuery(
        cursor, _buildMasterPeriodsSql(dbMaster, selectSql, periodColumn))


def _selectSlavePeriods(cursor, dbSlave, tableNameSlave,
                        slavePeriodColumn, filterClauseSlave):
    """Уникальные (createdate, max(lastupdate)) на стороне ведомой."""
    return _executeQuery(
        cursor, _buildSlavePeriodsSql(dbSlave, tableNameSlave,
                                      slavePeriodColumn, filterClauseSlave))


def _selectIudPeriods(cursor, dbMaster, tableNameEtlJobs, idrwBefore):
    """Группы, по которым в etl_log_iud_row есть необработанное (isetl = 0).

    idrwBefore — ТА ЖЕ граница, по которой потом пойдёт пометка
    (_processSectionGroup). Одна граница на оба запроса — обязательное условие,
    иначе в работу берётся то, что помечено не будет:

      1) граница = MAX(idrw) снимается в начале прогона;
      2) журнал читается НЕСКОЛЬКИМИ запросами позже — и без этого условия
         возвращал в том числе записи, появившиеся уже после границы;
      3) их группы переносились;
      4) пометка (idrw <= граница) их не трогала — они оставались isetl = 0;
      5) следующий прогон делал те же группы второй раз.

    Попасть в это окно можно было простым `update` руками: даг ходит раз в
    минуту, и «повезло» ли изменению лечь между (1) и (2), было делом случая —
    отсюда и «иногда группа переносится дважды, а иногда нет».
    Записи, появившиеся после границы, теперь честно ждут следующего прогона.
    """
    sqlTpl = _pickSql(dbMaster, periodsFromIudPostSql, periodsFromIudOrclSql)
    rows = _executeQuery(cursor, sqlTpl, {"tablename": tableNameEtlJobs,
                                          "idrwBefore": idrwBefore})
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


def _diffPeriods(masterRows, slaveRows, truncateLastupdate=False,
                 truncatePeriod=False):
    """Группы, требующие обновления: нет на одной из сторон, разошлись множества
    lastupdate ЛИБО разошлось количество записей.

    Счётчик добавлен не для красоты: сравнение одних только lastupdate слепо к
    целому классу расхождений. Удалили строку, которая не держала максимум
    lastupdate, — множество значений на обеих сторонах то же самое, а данных
    больше нет. Или строка задвоилась с тем же lastupdate. Обе стороны отдают
    (период, lastupdate, счётчик пары) — см. DateSelectMaster*/DateSelectSlave*.

    Ключ-период приводится к канону через _periodKey (по truncatePeriod — день
    либо точное значение). Если SQL группировал точнее, чем Python (truncate
    только здесь), несколько SQL-строк схлопываются в один ключ — поэтому
    счётчики складываются, а не присваиваются.

    NULL-период — обычный ключ словаря и обычная группа: она сравнивается и
    обновляется наравне с датированными.
    """
    def _bucket(rows):
        buckets = {}
        for period, lu, cnt in rows:
            key = _periodKey(period, truncatePeriod)
            b = buckets.setdefault(key, {"lus": set(), "cnt": 0})
            b["cnt"] += int(cnt or 0)
            b["lus"].add(_normalizeCompareValue(lu, truncateLastupdate))
        return buckets

    masterMap = _bucket(masterRows)
    slaveMap = _bucket(slaveRows)

    diff = []
    # По объединению ключей, а не только по ведущей: группа, которой в источнике
    # уже нет, а в ведомой она осталась, тоже расхождение — раньше цикл шёл по
    # masterMap и такие группы не вычищались.
    for period in masterMap.keys() | slaveMap.keys():
        m = masterMap.get(period)
        sl = slaveMap.get(period)
        if m is None or sl is None:
            diff.append(period)
            continue
        if m["cnt"] != sl["cnt"] or m["lus"] != sl["lus"]:
            diff.append(period)
    if diff:
        logger.info("section_compare: расходятся группы %s",
                    sorted(diff, key=_periodSortKey))
    return diff


def _processSectionGroup(cfg, ctx, period, idrwBefore):
    """Полная перезаливка группы (createdate=period) с пометкой
    обработанных записей etl_log_iud_row.

    idrwBefore = None — журнал линией не используется (режим section_compare
    без журнала), помечать в нём нечего.
    """
    cursorMaster = ctx["cursorMaster"]
    conMaster = ctx["conMaster"]
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]
    action = "ETL_SECTION_COMPARE"

    logger.info("section_compare: группа %s (%s)", period, tableNameEtlJobs)

    conSlave = None
    marks, phase = _phaseTimer()
    try:
        conSlave = _connect(dbSlave)
        cursorSlave = conSlave.cursor()
        phase("connect")

        # 1) удалить группу на стороне ведомой (для NULL-группы — свой запрос)
        cursorSlave.execute(
            _pickPeriodSql(ctx["deletePeriodSql"], period),
            _periodBinds(_periodSpec(cfg["slavePeriodColumn"]), period))
        logger.info("Ведомая: удалено %s строк группы %s",
                    _rowcount(cursorSlave), period)
        phase("delete")

        # 2) выбрать актуальные записи из ведущей и залить (про tablename —
        # см. тот же шаг в _processGroupUpdate)
        cursorMaster.execute(
            _pickPeriodSql(ctx["recordGroupSql"], period),
            {**_periodBinds(_periodSpec(cfg["periodColumn"]), period),
             **(cfg.get("filterParams") or {})},
        )
        phase("masterExec")
        records = cursorMaster.fetchall()
        phase("masterFetch")
        logger.info("Найдено %d записей для %s", len(records), period)
        _bulkUpsert(cursorSlave, dbSlave, ctx["upsertSql"],
                    ctx["structSlave"], records)
        phase("upsert")

        # 3) пометить обработанными те записи журнала, что существовали
        # на момент старта (новые останутся для следующего запуска)
        if idrwBefore is not None:
            markSql = _pickSql(dbMaster, markPeriodIudPostSql,
                               markPeriodIudOrclSql)
            cursorMaster.execute(
                markSql.format(period_cond=_journalPeriodCond(
                    dbMaster, period, "period",
                    truncate=cfg.get("truncatePeriod"))),
                {"tablename": tableNameEtlJobs,
                 "idrwBefore": idrwBefore,
                 **_journalPeriodBind(period, "period")},
            )
            # Сколько строк журнала закрыто — единственный способ заметить, что
            # условие пометки разошлось с условием выбора группы: тогда здесь
            # будет 0, а группа вернётся на следующем прогоне.
            logger.info("Журнал: отмечено обработанными %s строк группы %s",
                        _rowcount(cursorMaster), period)
        phase("journal")

        # 4) обновить etl_jobs.last_success_ts
        cursorMaster.execute(
            _pickSql(dbMaster, etlUpdatePostSql, etlUpdateOrclSql),
            {"LAST_SUCCESS_TS": ctx["currDt"],
             "TABLENAME": tableNameEtlJobs,
             "PERIOD": _periodBind(period)},
        )

        conMaster.commit()
        conSlave.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, len(records), period)
        phase("commit")
        logger.info("Группа %s обработана (section_compare): %d записей, "
                    "%.1fс (%s)", period, len(records),
                    _phasesTotal(marks, _GROUP_PHASE_LABELS),
                    _phasesText(marks, _GROUP_PHASE_LABELS))
    except Exception as err:
        if isShutdown(err):
            # SIGTERM — не ошибка группы, статус не трогаем: расхождение
            # никуда не денется и группу подберёт следующее сравнение срезов.
            _safeRollback(conMaster)
            _safeRollback(conSlave)
            logger.warning("SIGTERM на группе %s — останавливаюсь, статус "
                           "группы не меняю.", period)
            raise ShutdownRequested(
                f"Линия {tableNameEtlJobs}: SIGTERM на группе {period} — "
                f"чинить руками ничего не нужно."
            ) from err
        if conMaster:
            conMaster.rollback()
        if conSlave:
            conSlave.rollback()
        cursorMaster.execute(
            _pickSql(dbMaster, etlErrorPostSql, etlErrorOrclSql),
            {"ISOKAUDIT": -1, "tablename": tableNameEtlJobs,
             "PERIOD": _periodBind(period)},
        )
        conMaster.commit()
        UpdateLog(tableNameEtlJobs, dbMaster, action,
                  cursorMaster, conMaster, 0, period)
        logger.error("Ошибка section_compare группы %s: %s", period, err)
        raise
    finally:
        if conSlave:
            conSlave.close()


def _runSectionCompare(cfg, ctx, selectSql, useIud=False):
    """Основной цикл section_compare: собрать группы и обработать.

    Источников групп два или три:
      1. сравнение срезов (period, MAX(lastupdate)) ведущей и ведомой;
      2. группы с isokaudit=4 в etl_jobs;
      3. ТОЛЬКО при useIud — сигналы из etl_log_iud_row.

    useIud задаётся режимом линии: `section_compare` журнал не читает вовсе
    (ему не нужен триггер, и предупреждать о нём не за что), а
    `section_compare_with_iud` читает и требует триггера. Раньше это был один
    режим: журнал опрашивали у всех, хотя пишут в него единицы, и линии вроде
    medree_cons получали замечание о ненужном им триггере.
    """
    dbMaster = cfg["dbMaster"]
    dbSlave = cfg["dbSlave"]
    tableNameEtlJobs = cfg["tableNameEtlJobs"]

    idrwBefore = (_maxIudIdrw(ctx["cursorMaster"], dbMaster, tableNameEtlJobs)
                  if useIud else None)

    # Срезы строятся здесь, а не в ctx, — покажем их тем же способом.
    logStatements(
        f"Линия {tableNameEtlJobs}: запросы сравнения срезов",
        [("срез ведущей (период, lastupdate, счётчик)",
          _buildMasterPeriodsSql(dbMaster, selectSql, cfg["periodColumn"])),
         ("срез ведомой",
          _buildSlavePeriodsSql(dbSlave, cfg["tableNameSlave"],
                                cfg["slavePeriodColumn"],
                                cfg.get("filterClauseSlave")))])

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

    # Гранулярность группы у линии одна на все три источника ниже (см. _periodKey).
    truncatePeriod = bool(cfg.get("truncatePeriod"))

    needUpdate = set(_diffPeriods(masterRows, slaveRows,
                                  cfg.get("truncateLastupdate"), truncatePeriod))

    # 2. сигналы из etl_log_iud_row (только режим section_compare_with_iud)
    if useIud:
        iudPeriods = _selectIudPeriods(ctx["cursorMaster"], dbMaster,
                                       tableNameEtlJobs, idrwBefore)
        needUpdate.update(_periodKey(p, truncatePeriod) for p in iudPeriods)
        logger.info("Журнал (idrw <= %s): групп с необработанными записями %d",
                    idrwBefore, len(set(iudPeriods)))

    # 3. явные группы с isokaudit=4 в etl_jobs
    sqlTpl = _pickSql(dbMaster, periodsIsokAudit4PostSql, periodsIsokAudit4OrclSql)
    explicit = _executeQuery(
        ctx["cursorMaster"], sqlTpl, {"tablename": tableNameEtlJobs},
    )
    needUpdate.update(_periodKey(row[0], truncatePeriod) for row in explicit)

    if not needUpdate:
        logger.info("section_compare %s: групп для обновления нет",
                    tableNameEtlJobs)
        raise AirflowSkipException

    logger.info("section_compare %s: %d групп для обновления: %s",
                tableNameEtlJobs, len(needUpdate),
                sorted(needUpdate, key=_periodSortKey))
    _registerAffectedPeriods(
        ctx["cursorMaster"], dbMaster,
        list(needUpdate), tableNameEtlJobs,
    )
    for period in sorted(needUpdate, key=_periodSortKey):
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
    # section_compare: обрезать ли lastupdate до даты при сравнении групп.
    # По умолчанию False — lastupdate сравнивается с точностью timestamp.
    config.setdefault("truncateLastupdate", False)
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
            raise AirflowFailException(
                f"FLK: разные размеры json-структур ({len(structMaster)} vs "
                f"{len(structSlave)}) — линия заморожена, нужен человек"
            )

        # 2. Источник ведущей — таблица или sql
        if cfg.get("selectSql"):
            selectSql = TakeOneQuery(_resolveEtlPath(cfg["selectSql"]))
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

        # 4. Регистрацию периодов делаем адресно — только когда есть работа.
        #    Холостые прогоны iud (а их большинство) больше не сканируют
        #    источник: новые группы дат не могут появиться без событий в
        #    etl_log_iud_row. section_compare регистрирует полным сканом ниже,
        #    так как ищет группы сравнением master/slave
        
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
            # Пара запросов на группу: обычную и NULL-группу (см. _periodCond).
            "deletePeriodSql": _periodSqlPair(lambda nullGroup: _buildDeletePeriodSql(
                dbSlave, cfg["tableNameSlave"],
                cfg["slavePeriodColumn"],
                cfg.get("filterClauseSlave"),
                cfg.get("truncatePeriod"),
                nullGroup,
            )),
            "recordByIdSql": _buildRecordByIdSql(
                dbMaster, selectSql, structMaster,
                _primaryKeys(structMaster),
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
                cfg.get("truncatePeriod"),  # <-- ДОБАВЛЕНО
            ),
            "recordGroupSql": _periodSqlPair(lambda nullGroup: _buildRecordGroupSql(
                dbMaster, selectSql, structMaster,
                cfg["periodColumn"],
                cfg.get("filterClauseMaster"),
                cfg.get("truncatePeriod"),
                nullGroup,
            )),
        }

        # 5a. Один раз за прогон — что именно уйдёт в БД.
        logStatements(
            f"Линия {tableNameEtlJobs} ({dbMaster}->{dbSlave}), режим "
            f"{cfg['mode']}: запросы этого прогона. Значения уходят биндами; "
            f"для NULL-группы условие периода — `IS NULL` вместо `= бинд`.",
            _ctxStatements(ctx))

        # 6. Диспатч по режиму
        mode = cfg["mode"]
        if mode in ("section_compare", "section_compare_with_iud"):
            # Универсальный режим срезов для mocheck / medree. Ищет группы
            # сравнением master/slave, поэтому ему нужны все периоды источника
            # в etl_jobs — регистрируем полным сканом (как и раньше).
            #_registerNewPeriods(cursorMaster, dbMaster, selectSql,
            #                    tableNameEtlJobs, cfg["periodColumn"])
            #conMaster.commit()
            # ...with_iud — тот же режим ПЛЮС журнал (нужен триггер на ведущей).
            _runSectionCompare(cfg, ctx, selectSql,
                               useIud=(mode == "section_compare_with_iud"))
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
            # Групп не было — работы нет, помечаем прогон пропуском (⬜), как
            # это делает iud с пустым журналом. Иначе холостые ночные запуски
            # (а их большинство) неотличимы от реальных переносов.
            if not groups:
                raise AirflowSkipException
            return

        if mode == "query_section":
            # Группы задаёт пользовательский periodsSql; перезаливаются тем же
            # _processGroupUpdate, что и isokaudit=4 (etl_jobs/etl_log ведутся).
            _processQuerySection(cfg, ctx)
            return

        # 7. Точечные обновления через etl_log_iud_row (mode='iud')
        iudRecords, distinctIds = _selectIudWork(
            cursorMaster, dbMaster, tableNameEtlJobs,
        )

        # 7.1. Зависимости: события ЧУЖИХ таблиц, разложенные в ключи ведущей.
        # Строка появляется в выборке из-за правки в соседней таблице, а на
        # самой ведущей при этом не меняется НИЧЕГО — её триггер молчит по
        # делу. Такую линию нельзя чинить триггером: пришлось бы повторить в
        # нём условие запроса вместе с JOIN'ами. Разбирается это здесь.
        depDistinct, depMissing, byDepIdrw = _selectDependencyWork(
            cursorMaster, cfg, ctx)
        if depDistinct or depMissing:
            if mode == "delete_insert":
                raise ValueError(
                    "iudDependencies пока поддержаны только в режиме iud, "
                    f"а у линии {tableNameEtlJobs} режим {mode}. Скажите — "
                    "сделаем: разница только в бухгалтерии пометок.")
            # Сверяем через _idKey: журнал отдаёт ключ строкой, источник —
            # числом, и без нормализации одна и та же строка поехала бы дважды.
            known = {(e[1], _idKey(e[0])) for e in distinctIds}
            distinctIds = list(distinctIds) + [
                e for e in depDistinct if (e[1], _idKey(e[0])) not in known]

        if not iudRecords and not byDepIdrw:
            logger.info("В etl_log_iud_row для %s ничего нет", tableNameEtlJobs)
            if not groups:
                raise AirflowSkipException
            return
        if mode == "delete_insert":
            _processDeleteInsert(cfg, ctx, distinctIds, iudRecords)
        else:
            _processIndividualUpdates(cfg, ctx, distinctIds, iudRecords,
                                      byDepIdrw=byDepIdrw, depMissing=depMissing)
    except (AirflowSkipException, AirflowFailException, RecordScopeError,
            ShutdownRequested):
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