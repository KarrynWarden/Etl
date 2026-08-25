# -*- coding: utf-8 -*-
"""Самопроверка изменённых мест переноса — без БД и без airflow.

    PYTHONPATH=$PWD python3 tools/etl_selftest.py

Зачем это отдельным файлом. Правки в do_etl делались по живым замерам, и
каждая проверялась разовым скриптом на заглушках драйверов. Разовый скрипт
живёт до конца дня; вопрос «а не сломали ли мы перенос, пока ускоряли
iperson» задаётся месяцами. Здесь собрано то же самое, но так, чтобы ответ
можно было получить в любой момент одной командой.

Проверяется НЕ то, что запросы быстрые, а то, что они те же самые по смыслу:
условие периода, границы журнала, форма записи в ведомую. Скорость меряется
логом на боевых данных, её тут воспроизвести нечем.

psycopg2 / cx_Oracle / airflow подменяются заглушками: на машине, где лежит
репозиторий, их обычно нет, а импортировать do_etl нужно целиком.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.environ.setdefault("ETL_FULL_PATH", ROOT + "/")


class _AnyError(types.ModuleType):
    """Модуль, у которого любой атрибут — класс исключения.

    Драйверы упоминаются в except-ветках (cx_Oracle.DatabaseError и т.п.),
    поэтому заглушка обязана отдавать классы, а не что попало.
    """

    def __getattr__(self, name):
        value = type(name, (Exception,), {})
        setattr(self, name, value)
        return value


def _stubDrivers():
    for name in ("psycopg2", "psycopg2.extras", "cx_Oracle"):
        sys.modules.setdefault(name, _AnyError(name))
    sys.modules["psycopg2"].extras = sys.modules["psycopg2.extras"]
    exceptions = types.ModuleType("airflow.exceptions")
    for name in ("AirflowFailException", "AirflowSkipException",
                 "AirflowException"):
        setattr(exceptions, name, type(name, (Exception,), {}))
    airflow = types.ModuleType("airflow")
    airflow.exceptions = exceptions
    sys.modules.setdefault("airflow", airflow)
    sys.modules.setdefault("airflow.exceptions", exceptions)


_stubDrivers()

from Functions import do_etl as E  # noqa: E402

logger = logging.getLogger(__name__)


# ─────────────────────────── заглушки курсоров ───────────────────────────

class Cursor:
    """Курсор, который только записывает, что ему передали."""

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []          # [(нормализованный sql, параметры), ...]

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def executemany(self, sql, rows):
        self.calls.append((sql, rows))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return (1,)


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _body(sql):
    """Текст запроса без строк-комментариев, в одну строку.

    В комментариях к .sql законно встречаются и COALESCE, и etl_jobs —
    искать запрещённое надо в самом операторе, иначе проверка ловит
    собственную документацию. Комментарии режутся ДО склейки переносов:
    после неё «--» съело бы весь оператор.
    """
    return " ".join(" ".join(line.split()) for line in sql.splitlines()
                    if line.strip() and not line.strip().startswith("--"))


# ───────────────────────────── сами проверки ─────────────────────────────

def _checkPeriodCond():
    """Условие периода не прячет колонку под функцию.

    Ради этого вся правка и делалась: COALESCE(col, sentinel) = :bind
    отключает обычный индекс и в Oracle, и в Postgres. Смысл сохранён:
    COALESCE(x, s) = s тождественно x IS NULL.
    """
    single = E._periodSpec("createdate")

    assert E._periodCond("Post", single) == "createdate = %(createdate)s"
    assert E._periodCond("Orcl", single) == "createdate = :createdate"
    assert E._periodCond("Post", single, alias="p") == \
        "p.createdate = %(createdate)s"

    # NULL-группа: отдельный запрос и НИ ОДНОГО бинда
    assert E._periodCond("Post", single, nullGroup=True) == "createdate IS NULL"
    assert E._periodCond("Orcl", single, nullGroup=True) == "createdate IS NULL"
    assert E._periodBinds(single, None) == {}
    assert E._periodBinds(single, dt.date(2026, 3, 1)) == \
        {"createdate": dt.date(2026, 3, 1)}

    for db in ("Post", "Orcl"):
        for nullGroup in (False, True):
            cond = E._periodCond(db, single, nullGroup=nullGroup)
            assert "COALESCE" not in cond.upper(), (db, nullGroup, cond)

    # составной период (year/month/day) — по колонкам, тоже без функций
    composite = E._periodSpec({"year": "YEAR", "month": "MO"})
    cond = E._periodCond("Orcl", composite, alias="p")
    assert cond == "p.YEAR = :createdate_y AND p.MO = :createdate_m", cond
    assert E._periodCond("Orcl", composite, alias="p", nullGroup=True) == \
        "p.YEAR IS NULL AND p.MO IS NULL"
    assert E._periodBinds(composite, dt.date(2026, 3, 1)) == \
        {"createdate_y": 2026, "createdate_m": 3}

    # truncatePeriod: суточная гранулярность выражается ПОЛУИНТЕРВАЛОМ, а не
    # функцией вокруг колонки. Смысл тот же (TRUNC(x) = d ⟺ d <= x < d+1), но
    # колонка остаётся голой: работает индекс, и условие можно протолкнуть
    # внутрь вложенного запроса. Под TRUNC выборка одной группы EXPMED23
    # обходила весь EXPMED с 2020 года — 7 секунд ради 85 строк.
    assert E._periodCond("Orcl", single, truncate=True) == (
        "(createdate >= TRUNC(:createdate) "
        "AND createdate < TRUNC(:createdate) + 1)")
    assert E._periodCond("Post", single, truncate=True) == (
        "(createdate >= %(createdate)s::date "
        "AND createdate < %(createdate)s::date + 1)")
    # граница считается от БИНДА (константа — индексу не мешает), а не от
    # колонки: иначе значение со временем сдвинуло бы окно
    for db in ("Orcl", "Post"):
        cond = E._periodCond(db, single, "p", truncate=True)
        assert "TRUNC(p." not in cond and "DATE(p." not in cond, cond
        assert cond.count("p.createdate") == 2, cond
    print("  условие периода: без COALESCE, NULL-группа без бинда — ок")


def _checkJournalCond():
    """Журнал помечается по той же гранулярности, по какой выбрана группа.

    Иначе строка журнала со временем внутри дня не закрывается, и группа
    возвращается на следующем прогоне — это и был двойной перенос.
    """
    day = dt.date(2026, 8, 13)
    assert E._journalPeriodCond("Post", day, "period", False) == \
        "period = %(period)s"
    assert E._journalPeriodCond("Orcl", day, "period", False) == \
        "period = :period"
    assert E._journalPeriodCond("Orcl", day, "period", True) == (
        "(period >= TRUNC(:period) AND period < TRUNC(:period) + 1)")
    assert E._journalPeriodCond("Post", day, "period", True) == (
        "(period >= %(period)s::date AND period < %(period)s::date + 1)")
    assert E._journalPeriodCond("Orcl", None, "period", True) == "period IS NULL"
    assert E._journalPeriodBind(None) == {}
    assert E._journalPeriodBind(day) == {"createdate": day}
    print("  журнал: гранулярность пометки совпадает с выбором группы — ок")


def _checkJournalBoundary():
    """Чтение журнала ограничено той же границей idrw, что и пометка.

    Без этого группа бралась в работу по записи, появившейся уже ПОСЛЕ
    границы, переносилась и не помечалась — следующий прогон делал ту же
    группу второй раз.
    """
    for sql in (E.periodsFromIudOrclSql, E.periodsFromIudPostSql):
        body = _body(sql)
        assert "idrw <= :idrwBefore" in body or \
               "idrw <= %(idrwBefore)s" in body, body
    for sql in (E.markPeriodIudOrclSql, E.markPeriodIudPostSql):
        body = _body(sql)
        assert "idrwBefore" in body, body
        assert "{period_cond}" in body, body
    print("  журнал: чтение и пометка идут по одной границе idrw — ок")


def _checkGroupSelect():
    """Выборка группы — без бесполезного JOIN с etl_jobs.

    Из etl_jobs не выбиралось ни одной колонки, а две его строки, попадающие
    в одну группу под COALESCE, задвоили бы КАЖДУЮ строку источника.
    """
    struct = [("IDRW", "NUMBER", 0, "Primary Key"),
              ("CREATEDATE", "DATE", None, None)]
    for db in ("Orcl", "Post"):
        for nullGroup in (False, True):
            sql = E._buildRecordGroupSql(db, "SELECT * FROM t", struct,
                                         "createdate", None, False, nullGroup)
            statement = _body(sql).split("SELECT", 1)[1]
            assert "etl_jobs" not in statement.lower(), (db, statement)
            assert "COALESCE" not in statement.upper(), (db, statement)
    print("  выборка группы: JOIN с etl_jobs убран — ок")


def _checkDeleteSql():
    """Удаление группы в ведомой: то же условие периода плюс фильтр линии."""
    pair = E._periodSqlPair(lambda nullGroup: E._buildDeletePeriodSql(
        "Post", "mocheck", "createdate", "doctype IN (2, 3)", False, nullGroup))

    normal = _body(E._pickPeriodSql(pair, dt.date(2026, 3, 1)))
    assert "createdate = %(createdate)s" in normal, normal
    assert "doctype IN (2, 3)" in normal, normal

    empty = _body(E._pickPeriodSql(pair, None))
    assert "createdate IS NULL" in empty, empty
    assert "doctype IN (2, 3)" in empty, empty
    print("  удаление группы: условие и фильтр ведомой на месте — ок")


def _checkBulkUpsert():
    """Запись в ведомую идёт пачками, и откат по строке считается честно.

    Пачки — не украшение: при 205 тыс. строк в группе построчная запись
    стоила бы порядка 3.5 мс на строку только на обмен с сервером.
    """
    records = [(i, None if i % 3 else "x") for i in range(2500)]

    class OracleCursor:
        def __init__(self, failEvery=0):
            self.batched = self.single = self.calls = 0
            self.failEvery = failEvery

        def executemany(self, sql, rows):
            self.calls += 1
            if self.failEvery and self.calls % self.failEvery == 0:
                raise ValueError("string data too large")
            self.batched += len(rows)

        def execute(self, sql, params):
            self.single += 1

    cursor = OracleCursor()
    E._bulkUpsert(cursor, "Orcl", "MERGE ...", [], records)
    assert (cursor.batched, cursor.single) == (2500, 0)

    # упавшая пачка повторяется по строке: у оракловой ведомой запись всегда
    # MERGE, поэтому повтор уже применённых строк безопасен
    cursor = OracleCursor(failEvery=2)
    E._bulkUpsert(cursor, "Orcl", "MERGE ...", [], records)
    assert (cursor.batched, cursor.single) == (1500, 1000)

    cursor = OracleCursor()
    E._bulkUpsert(cursor, "Orcl", "MERGE ...", [], [])
    assert cursor.calls == 0

    # форма параметров прежняя: позиционные :1 .. :N словарём
    seen = []

    class ShapeCursor(OracleCursor):
        def executemany(self, sql, rows):
            seen.extend(rows)

    E._bulkUpsert(ShapeCursor(), "Orcl", "MERGE ...", [], [(7, "a"), (8, None)])
    assert seen == [{"1": 7, "2": "a"}, {"1": 8, "2": None}], seen

    calls = []
    sys.modules["psycopg2.extras"].execute_batch = \
        lambda cur, sql, rows, page_size=None: calls.append((len(rows), page_size))
    E._bulkUpsert(object(), "Post", "INSERT ...", [], records)
    assert calls == [(2500, E._PG_BATCH_PAGE)], calls
    print("  заливка: пачки, откат по строке, форма биндов — ок")


def _checkGroupFlow():
    """Порядок действий при перезаливке группы не изменился.

    Удалить группу в ведомой -> выбрать её у ведущей -> залить -> отметить
    etl_jobs/etl_log. Проверяется на обычной группе и на NULL-группе.
    """
    cfg = {"dbMaster": "Orcl", "dbSlave": "Post", "tableNameEtlJobs": "EXPMED",
           "tableNameSlave": "mocheck", "periodColumn": "createdate",
           "slavePeriodColumn": "createdate", "truncatePeriod": True,
           "filterClauseSlave": "doctype IN (2, 3, 4)", "filterParams": {}}
    struct = [("IDRW", "NUMBER", 0, "Primary Key")]

    slaveCursor = Cursor()
    E._connect = lambda db: Connection(slaveCursor)
    E.UpdateLog = lambda *a, **k: None
    E.StructCheckDataBase = lambda *a, **k: True
    E._bulkUpsert = lambda *a, **k: None

    for period in (dt.date(2026, 3, 1), None):
        slaveCursor.calls.clear()
        masterCursor = Cursor(rows=[(1,)])
        ctx = {
            "currDt": dt.datetime(2026, 3, 2),
            "conMaster": Connection(masterCursor), "cursorMaster": masterCursor,
            "structMaster": struct,
            "structSlave": [("idrw", "numeric", 0, "Primary Key")],
            "upsertSql": "INSERT ...",
            "deletePeriodSql": E._periodSqlPair(lambda n: E._buildDeletePeriodSql(
                cfg["dbSlave"], cfg["tableNameSlave"], cfg["slavePeriodColumn"],
                cfg["filterClauseSlave"], cfg["truncatePeriod"], n)),
            "recordGroupSql": E._periodSqlPair(lambda n: E._buildRecordGroupSql(
                cfg["dbMaster"], "SELECT * FROM t", struct, cfg["periodColumn"],
                None, cfg["truncatePeriod"], n)),
        }
        E._processGroupUpdate(cfg, ctx, (period,))

        markSql, markBinds = masterCursor.calls[0]
        deleteSql, deleteBinds = slaveCursor.calls[0]
        groupSql, groupBinds = masterCursor.calls[1]
        markSql, deleteSql, groupSql = (_body(markSql), _body(deleteSql),
                                        _body(groupSql))
        assert "isetl = 1" in markSql, markSql
        if period is None:
            assert "period IS NULL" in markSql, markSql
            assert markBinds == {"tablename": "EXPMED"}
            assert "createdate IS NULL" in deleteSql and deleteBinds == {}
            assert "p.createdate IS NULL" in groupSql and groupBinds == {}
        else:
            assert "period >= TRUNC(:createdate)" in markSql, markSql
            assert "createdate >= %(createdate)s::date" in deleteSql, deleteSql
            assert deleteBinds == {"createdate": period}
            assert "p.createdate >= TRUNC(:createdate)" in groupSql, groupSql
            assert groupBinds == {"createdate": period}
        # tablename в выборке группы больше не биндится: он был нужен
        # убранному JOIN'у, а лишний бинд Oracle не принимает (ORA-01036)
        assert "tablename" not in groupBinds, groupBinds
        assert len(slaveCursor.calls) == 1, slaveCursor.calls
    print("  перезаливка группы: порядок действий и бинды — ок")


def _checkDependencies():
    """Зависимости: чужое событие -> ключи ведущей, с ЕЁ периодом.

    Проверяется то, из-за чего механика вообще заведена, и то, чем она опасна:
      1. метка чужой таблицы раскладывается в ключи ведущей запросом линии;
      2. период берётся из ВЕДУЩЕЙ, а не из журнальной записи — иначе линия
         отчиталась бы в etl_jobs о переносе не тех групп, и сверка периодов
         разошлась бы молча;
      3. журнальная строка зависимости отвечает за ВСЕ найденные по ней строки
         (иначе падение второй строки потерялось бы: своих журнальных строк у
         неё нет и парковать нечего).
    """
    print("  зависимости (iudDependencies)")

    # 1. разбор конфига
    assert E._dependencySpecs({}) == []
    one = E._dependencySpecs(
        {"iudDependencies": [{"tablename": "fublin", "column": "smo_idrw"}]})
    assert one == [{"tablename": "fublin", "column": "smo_idrw"}], one
    for bad in ({"tablename": "fublin"}, {"column": "x"}, {}):
        try:
            E._dependencySpecs({"iudDependencies": [bad]})
        except ValueError:
            pass
        else:
            raise AssertionError(f"принята кривая зависимость: {bad}")

    cfg = {
        "dbMaster": "Post", "dbSlave": "Post",
        "tableNameEtlJobs": "проба",
        "tableNameMaster": "src_table",
        "periodColumn": "createdate",
        "iudDependencies": [{"tablename": "fublin", "column": "smo_idrw"}],
    }
    struct = {"data": [
        {"column_name": "idrw", "data_type": "bigint", "is_primary_key": "Primary Key"},
        {"column_name": "createdate", "data_type": "date"},
    ]}

    # 2. журнал отдаёт ДВЕ строки по одному чужому ключу 77, источник —
    #    две строки ведущей с СОБСТВЕННЫМ периодом
    journalRows = [(77, 501, dt.date(2001, 1, 1), dt.datetime(2024, 5, 1), "IU"),
                   (77, 502, dt.date(2001, 1, 1), dt.datetime(2024, 5, 2), "IU")]
    resolved = [(77, 11, dt.date(2024, 3, 1)), (77, 12, dt.date(2024, 4, 1))]

    # Таблица помнит связь с ТРЕМЯ строками, а запрос отдаёт только две:
    # у третьей условие перестало выполняться (обнулили DIRDT).
    candidates = [(77, 11), (77, 12), (77, 13)]

    class _Cur(Cursor):
        """Журнал -> раскладка запросом -> связанные строки из таблицы."""

        def __init__(self):
            super().__init__()
            self.stage = 0
            self.sqls = []

        def execute(self, sql, params=None):
            self.sqls.append(sql)
            self.rows = (journalRows, resolved, candidates)[min(self.stage, 2)]
            self.stage += 1

    cur = _Cur()
    ctx = {"cursorMaster": cur, "pkColsMaster": ["idrw"], "structMaster": struct,
           "selectSql": "SELECT * FROM src", "currDt": dt.datetime(2024, 6, 1)}
    distinct, missing, byDepIdrw = E._selectDependencyWork(cur, cfg, ctx)

    periods = sorted(e[1] for e in distinct)
    assert periods == [dt.date(2024, 3, 1), dt.date(2024, 4, 1)], periods
    assert dt.date(2001, 1, 1) not in periods, \
        "период взят из журнальной записи — линия отчитается не о тех группах"
    assert sorted(e[0] for e in distinct) == [11, 12], distinct
    assert all(e[3] == "IU" for e in distinct), distinct
    assert byDepIdrw == {501: {11, 12, 13}, 502: {11, 12, 13}}, byDepIdrw

    # 2а. ГЛАВНОЕ: строка, выпавшая из выборки, идёт на удаление. Без этого
    # обнулённый DIRDT остался бы незамеченным — событие приходит, запрос по
    # ключу молчит, строка в ведомой живёт дальше.
    assert missing == [13], missing
    assert 13 not in [e[0] for e in distinct], distinct

    # связанные строки спрашиваются у ТАБЛИЦЫ, а не у запроса: запрос отдаёт
    # только подходящие, и выпавшую по нему уже не найти
    candSql = cur.sqls[-1]
    assert "FROM src_table" in candSql, candSql
    assert "SELECT * FROM src" not in candSql, candSql
    assert "smo_idrw AS dep_key" in candSql and "idrw AS dep_id" in candSql, candSql

    # 3. запрос раскладки идёт ПОВЕРХ запроса линии и тянет ключ вместе со
    #    строкой: без ключа не понять, какая журнальная строка чем закрыта
    resolveSql = cur.sqls[1]
    assert "FROM ( SELECT * FROM src ) p" in resolveSql, resolveSql
    assert "p.smo_idrw IN" in resolveSql, resolveSql
    assert "p.smo_idrw AS dep_key" in resolveSql, resolveSql
    assert "p.idrw AS dep_id" in resolveSql, resolveSql

    # 4. составной ключ ведущей — отказ, а не молчаливая склейка в строку
    try:
        E._resolveDependencyIds({**ctx, "pkColsMaster": ["a", "b"]}, cfg,
                                "smo_idrw", [1])
    except ValueError as err:
        assert "ОДНОЙ колонкой ключа" in str(err), err
    else:
        raise AssertionError("составной ключ ведущей принят")

    # 5. без зависимостей ничего не спрашивается вовсе
    quiet = Cursor()
    assert E._selectDependencyWork(quiet, {"dbMaster": "Post"}, ctx) == ([], [], {})
    assert quiet.calls == [], quiet.calls


def _selftest():
    print("проверка изменённых мест переноса (заглушки драйверов):")
    _checkDependencies()
    _checkPeriodCond()
    _checkJournalCond()
    _checkJournalBoundary()
    _checkGroupSelect()
    _checkDeleteSql()
    _checkBulkUpsert()
    _checkGroupFlow()
    print("etl_selftest OK")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING,
                        format="%(levelname)-7s %(message)s")
    _selftest()
