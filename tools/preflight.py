#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Готовность СЕГМЕНТА к запуску процессов — read-only отчёт.

Отвечает на вопрос «что в этой БД ещё не готово к текущей версии кода»:
служебные таблицы, триггеры ведущих, структуры ведущих/ведомых, залиты ли
ведомые. Нужен прежде всего при выкатке на прод, где БД старая и часть
объектов новых линий там ещё не заведена (см. deploy/README-prod.md).

Скрипт НИЧЕГО не пишет: только SELECT'ы и разбор SQL с `WHERE 1 = 0`.

Сегмент определяется тем, какой .env лежит в корне клона (см. Connect/).

    PYTHONPATH=$PWD python3 tools/preflight.py                # всё
    PYTHONPATH=$PWD python3 tools/preflight.py --no-sp        # без справочников
    PYTHONPATH=$PWD python3 tools/preflight.py MEDREE_PRDISPOrclPost PLANOMSOrclPost

Итог: строка «ГОТОВО» / «ЕСТЬ ЗАМЕЧАНИЯ» и код возврата 0/1 — скрипт можно
ставить в конвейер выкатки.
"""
import io
import os
import re
import sys
from contextlib import redirect_stdout

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
sys.path.insert(0, ROOT)

from Functions.functionsFile.loadConfig import assemble, resolvePath  # noqa: E402
from Functions.functionsFile.jsonLoad import JsonLoadOrcl, JsonLoadPost  # noqa: E402
from Functions.functionsFile.structCheck import (  # noqa: E402
    StructCheckDataBase,
    StructCheckOracleQuery,
    StructCheckPostgresQuery,
)
from Functions.functionsFile.takeOneQuery import TakeOneQuery  # noqa: E402
from Src.generalQueries import (  # noqa: E402
    selectEtlIudOrclSql,
    structureCheckOrclSql,
    structureCheckPostSql,
)

OK, WARN, BAD = "OK", "!!", "НЕТ"

# Схема служебных таблиц на Oracle прибита в .sql-файлах (koknaev.etl_jobs и
# т.д.). Не дублируем её константой — вычитываем из самого запроса, чтобы
# отчёт не разошёлся с тем, куда реально ходит рантайм.
_m = re.search(r"FROM\s+([A-Za-z0-9_]+)\.etl_log_iud_row", selectEtlIudOrclSql, re.I)
ORCL_SCHEMA = _m.group(1) if _m else "koknaev"


class Report:
    """Копит замечания, чтобы в конце сказать одним словом, готов сегмент или нет."""

    def __init__(self):
        self.problems = []

    def line(self, status, what, detail=""):
        print(f"  {status:>3}  {what}{('  — ' + detail) if detail else ''}")
        if status != OK:
            self.problems.append(f"{what}: {detail or status}")


# ─────────────────────────── подключения ───────────────────────────

class Segment:
    """Ленивые соединения с обеими БД сегмента (набор реквизитов MAIN)."""

    def __init__(self):
        self._con = {}
        self.failed = {}

    def cursor(self, dbType):
        if dbType in self.failed:
            return None
        if dbType not in self._con:
            try:
                from Connect import connectOracle, connectPostgres
                self._con[dbType] = (connectPostgres() if dbType == "Post"
                                     else connectOracle())
            except Exception as err:
                self.failed[dbType] = f"{type(err).__name__}: {str(err)[:200]}"
                return None
        return self._con[dbType].cursor()

    def close(self):
        for con in self._con.values():
            try:
                con.close()
            except Exception:
                pass


def _scalar(cur, sql, binds=None):
    """SELECT одного значения; None, если запрос не прошёл."""
    try:
        cur.execute(sql, binds or {})
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _exists(seg, dbType, name):
    """Есть ли таблица/вью (Oracle — с учётом схемы, Postgres — по search_path)."""
    cur = seg.cursor(dbType)
    if cur is None:
        return None
    if dbType == "Orcl":
        owner, _, bare = name.upper().rpartition(".")
        if owner:
            got = _scalar(cur, "SELECT COUNT(*) FROM all_objects WHERE owner = :o "
                               "AND object_name = :t AND object_type IN "
                               "('TABLE', 'VIEW', 'SYNONYM')",
                          {"o": owner, "t": bare})
        else:
            got = _scalar(cur, "SELECT COUNT(*) FROM all_objects WHERE object_name = :t "
                               "AND object_type IN ('TABLE', 'VIEW', 'SYNONYM')",
                          {"t": bare})
    else:
        got = _scalar(cur, "SELECT COUNT(*) FROM information_schema.tables "
                           "WHERE table_name = %(t)s", {"t": name.split(".")[-1].lower()})
    cur.close()
    return bool(got)


def _isEmpty(seg, dbType, name):
    """Пуста ли таблица (дёшево, без COUNT(*) по всей таблице).

    None — проверить не удалось (нет таблицы / нет прав).
    """
    cur = seg.cursor(dbType)
    if cur is None:
        return None
    sql = (f"SELECT 1 FROM {name} WHERE ROWNUM = 1" if dbType == "Orcl"
           else f"SELECT 1 FROM {name} LIMIT 1")
    try:
        cur.execute(sql)
        return cur.fetchone() is None
    except Exception:
        return None
    finally:
        cur.close()


def _hasTrigger(seg, dbType, tableName):
    """Есть ли на ведущей таблице ВКЛЮЧЁННЫЙ триггер (журнал etl_log_iud_row).

    Возвращает (естьВключённый, списокИмён) или (None, []) — если не проверить.
    """
    cur = seg.cursor(dbType)
    if cur is None:
        return None, []
    try:
        if dbType == "Orcl":
            owner, _, bare = tableName.upper().rpartition(".")
            sql = ("SELECT trigger_name, status FROM all_triggers "
                   "WHERE table_name = :t" + (" AND table_owner = :o" if owner else ""))
            cur.execute(sql, ({"t": bare, "o": owner} if owner else {"t": bare}))
            rows = cur.fetchall()
            enabled = [r[0] for r in rows if str(r[1]).upper() == "ENABLED"]
        else:
            cur.execute(
                "SELECT t.tgname, t.tgenabled FROM pg_trigger t "
                "JOIN pg_class c ON c.oid = t.tgrelid "
                "WHERE NOT t.tgisinternal AND c.relname = %(t)s",
                {"t": tableName.split(".")[-1].lower()})
            rows = cur.fetchall()
            enabled = [r[0] for r in rows if str(r[1]) != "D"]
        return bool(enabled), [r[0] for r in rows]
    except Exception:
        return None, []
    finally:
        cur.close()


def _etlJobsStat(seg, dbType, tableNameEtlJobs):
    """Сколько групп линии уже заведено в etl_jobs и в каком они состоянии."""
    cur = seg.cursor(dbType)
    if cur is None:
        return None
    table = f"{ORCL_SCHEMA}.etl_jobs" if dbType == "Orcl" else "etl_jobs"
    try:
        if dbType == "Orcl":
            cur.execute(f"SELECT COUNT(*), SUM(CASE WHEN isokaudit = 1 THEN 1 ELSE 0 END) "
                        f"FROM {table} WHERE tablename = :t", {"t": tableNameEtlJobs})
        else:
            cur.execute(f"SELECT COUNT(*), SUM(CASE WHEN isokaudit = 1 THEN 1 ELSE 0 END) "
                        f"FROM {table} WHERE tablename = %(t)s", {"t": tableNameEtlJobs})
        total, ok = cur.fetchone()
        return int(total or 0), int(ok or 0)
    except Exception:
        return None
    finally:
        cur.close()


# ─────────────────────────── проверки линий ───────────────────────────

def _direction(key):
    """(dbMaster, dbSlave) из хвоста ключа конфига: ...OrclPost / ...PostOrcl / ..."""
    for master in ("Orcl", "Post"):
        for slave in ("Orcl", "Post"):
            if key.endswith(master + slave):
                return master, slave
    return None, None


def _loadStruct(path, dbType):
    path = resolvePath(path)
    return JsonLoadPost(path) if dbType == "Post" else JsonLoadOrcl(path)


def _capture(fn, *args):
    """Выполнить проверку структуры, забрав её print'ы себе (они и есть диагноз)."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            ok = fn(*args)
        except Exception as err:
            return None, f"{type(err).__name__}: {str(err)[:200]}"
    noise = [ln for ln in buf.getvalue().splitlines()
             if ln.strip() and not ln.startswith("here is structure")]
    return ok, " / ".join(noise)[:300]


def checkLine(rep, seg, key, cfg):
    """Одна линия config: источник ведущей, ведомая, триггер, etl_jobs."""
    dbMaster, dbSlave = _direction(key)
    if dbMaster is None:
        rep.line(WARN, key, "не разобрал направление по имени ключа")
        return

    tableNameEtlJobs = key[: -len(dbMaster + dbSlave)]
    prefix = f"{key} [{dbMaster}->{dbSlave}]"

    if cfg.get("disabled"):
        print(f"  ---  {prefix}  — линия отключена в конфиге, пропуск")
        return

    curMaster = seg.cursor(dbMaster)
    curSlave = seg.cursor(dbSlave)
    if curMaster is None or curSlave is None:
        rep.line(BAD, prefix, "нет соединения с БД сегмента")
        return

    try:
        structMaster = _loadStruct(cfg["structureMaster"], dbMaster)
        structSlave = _loadStruct(cfg["structureSlave"], dbSlave)
    except Exception as err:
        rep.line(BAD, prefix, f"json-структуры: {type(err).__name__}: {err}")
        return

    if len(structMaster) != len(structSlave):
        rep.line(BAD, prefix, f"разные размеры json-структур: "
                              f"{len(structMaster)} vs {len(structSlave)}")

    # 1. источник ведущей: таблица или пользовательский SELECT
    if cfg.get("selectSql"):
        selectSql = TakeOneQuery(resolvePath(cfg["selectSql"]))
        checker = (StructCheckPostgresQuery if dbMaster == "Post"
                   else StructCheckOracleQuery)
        ok, detail = _capture(checker, structMaster, curMaster, selectSql)
        srcName = os.path.basename(cfg["selectSql"])
    else:
        tableNameMaster = cfg["tableNameMaster"]
        checkSql = structureCheckPostSql if dbMaster == "Post" else structureCheckOrclSql
        ok, detail = _capture(StructCheckDataBase, structMaster, curMaster,
                              checkSql, tableNameMaster)
        srcName = tableNameMaster

    rep.line(OK if ok else BAD, f"{prefix} ведущая {srcName}",
             "" if ok else (detail or "структура не совпадает с json"))

    # 2. ведомая
    checkSql = structureCheckPostSql if dbSlave == "Post" else structureCheckOrclSql
    ok, detail = _capture(StructCheckDataBase, structSlave, curSlave,
                          checkSql, cfg["tableNameSlave"])
    empty = _isEmpty(seg, dbSlave, cfg["tableNameSlave"])
    note = "" if ok else (detail or "структура не совпадает с json")
    if ok and empty:
        note = "таблица ПУСТА — нужен первичный залив"
    rep.line(OK if ok and not empty else (WARN if ok else BAD),
             f"{prefix} ведомая {cfg['tableNameSlave']}", note)

    # 3. триггер журнала на ведущей (режимы iud/section_compare им живут)
    mode = cfg.get("mode", "iud")
    if mode in ("iud", "section_compare", "delete_insert") and not cfg.get("selectSql"):
        has, names = _hasTrigger(seg, dbMaster, cfg["tableNameMaster"])
        if has is None:
            rep.line(WARN, f"{prefix} триггер на {cfg['tableNameMaster']}",
                     "не смог проверить (права на словарь?)")
        elif has:
            rep.line(OK, f"{prefix} триггер на {cfg['tableNameMaster']}",
                     ", ".join(names[:3]))
        else:
            rep.line(BAD, f"{prefix} триггер на {cfg['tableNameMaster']}",
                     "нет ВКЛЮЧЁННОГО триггера — журнал etl_log_iud_row "
                     "не наполняется, линия не увидит изменений")

    # 4. etl_jobs (лежит на стороне ВЕДУЩЕЙ)
    stat = _etlJobsStat(seg, dbMaster, tableNameEtlJobs)
    if stat is None:
        rep.line(WARN, f"{prefix} etl_jobs '{tableNameEtlJobs}'", "не смог посчитать")
    else:
        total, okCnt = stat
        rep.line(OK if total else WARN, f"{prefix} etl_jobs '{tableNameEtlJobs}'",
                 f"групп {total}, из них проверено аудитом {okCnt}" if total
                 else "групп нет — линия в этом сегменте ещё не работала")


def checkSpLine(rep, seg, key, cfg):
    """Линия справочника: ведомая на месте и SELECT ведущей разбирается."""
    dbMaster, dbSlave = _direction(key)
    if dbMaster is None:
        rep.line(WARN, key, "не разобрал направление по имени ключа")
        return
    prefix = f"{key} [{dbMaster}->{dbSlave}]"

    if _exists(seg, dbSlave, cfg["tableNameSlave"]) is False:
        rep.line(BAD, f"{prefix} ведомая {cfg['tableNameSlave']}", "таблицы нет")
        return

    cur = seg.cursor(dbMaster)
    if cur is None:
        rep.line(BAD, prefix, "нет соединения с ведущей")
        return
    sql = TakeOneQuery(resolvePath(cfg["selectSql"])).strip().rstrip(";")
    try:
        if dbMaster == "Orcl":
            cur.execute(f"SELECT * FROM ({sql}) WHERE 1 = 0")
        else:
            cur.execute(f"SELECT * FROM ({sql}) q LIMIT 0")
        rep.line(OK, prefix)
    except Exception as err:
        rep.line(BAD, f"{prefix} SELECT ведущей",
                 f"{type(err).__name__}: {str(err)[:200]}")
    finally:
        cur.close()


# ─────────────────────────── служебные объекты ───────────────────────────

def checkService(rep, seg, sides):
    """Служебные таблицы на тех сторонах, где они реально нужны."""
    for dbType in sorted(sides):
        names = ([f"{ORCL_SCHEMA}.etl_log_iud_row", f"{ORCL_SCHEMA}.etl_jobs",
                  f"{ORCL_SCHEMA}.etl_log"] if dbType == "Orcl"
                 else ["etl_log_iud_row", "etl_jobs", "etl_log"])
        for name in names:
            got = _exists(seg, dbType, name)
            if got is None:
                rep.line(WARN, f"{dbType}: {name}", "не смог проверить")
            else:
                rep.line(OK if got else BAD, f"{dbType}: {name}",
                         "" if got else "таблицы нет — заведи (etlFolder/queries/"
                                        "oracleSetup/01_create_etl_log_iud_row.sql "
                                        "и аналог на Postgres)")


def main(argv):
    only = [a for a in argv if not a.startswith("-")]
    withSp = "--no-sp" not in argv

    config = assemble("config")["data"]
    if only:
        missing = [k for k in only if k not in config]
        config = {k: v for k, v in config.items() if k in only}
        if missing:
            print("Нет таких линий в config:", ", ".join(missing))

    seg = Segment()
    rep = Report()

    print(f"Каталог конфигурации: {os.environ['ETL_FULL_PATH']}etlFolder")
    print(f"Схема служебных таблиц на Oracle: {ORCL_SCHEMA}\n")

    print("== Подключения (набор MAIN) ==")
    sides = set()
    for key in config:
        m, s = _direction(key)
        sides.update([x for x in (m, s) if x])
    if withSp:
        sides.update(["Orcl", "Post"])
    for dbType in sorted(sides):
        cur = seg.cursor(dbType)
        if cur is None:
            rep.line(BAD, f"{dbType} MAIN", seg.failed.get(dbType, ""))
        else:
            cur.close()
            rep.line(OK, f"{dbType} MAIN")
    if seg.failed:
        # Без соединения все остальные проверки выродятся в одинаковое «нет
        # соединения» на сотню строк — это не отчёт, а шум. Останавливаемся.
        print("\nНет соединения с БД сегмента — остальные проверки пропущены.")
        print("Проверь .env этого клона (шаблон: deploy/prod.env.example) и "
              "доступность БД: PYTHONPATH=$PWD python3 tools/check_db.py")
        seg.close()
        return 2

    print("\n== Служебные таблицы ==")
    checkService(rep, seg, sides)

    print(f"\n== Линии переноса ({len(config)}) ==")
    for key in sorted(config):
        checkLine(rep, seg, key, config[key])

    if withSp:
        spLines = {}
        for name in ("SpTableName", "SpOnce"):
            try:
                spLines.update({k: v for k, v in assemble(name)["data"].items()})
            except Exception as err:
                rep.line(WARN, f"конфиг {name}", f"{type(err).__name__}: {err}")
        print(f"\n== Справочники и разовый перенос ({len(spLines)}) ==")
        for key in sorted(spLines):
            checkSpLine(rep, seg, key, spLines[key])

    seg.close()

    print("\n" + "=" * 70)
    if rep.problems:
        print(f"ЕСТЬ ЗАМЕЧАНИЯ ({len(rep.problems)}):")
        for p in rep.problems:
            print("  -", p)
        return 1
    print("ГОТОВО: замечаний нет.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
