#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Профилировщик аудита — read-only: КУДА уходит время на группе.

Зачем. Аудит iperson идёт часами, и по логу видно только итог. Причём время
почти не зависит от объёма: группа на 525 строк проверяется ~15 с, а на
271 184 строки — ~32 с. Значит основная часть — фиксированная плата за каждую
группу, и прежде чем что-то ускорять, надо знать, ЧТО именно платится.

Скрипт берёт ту же линию из того же config.d, собирает ТЕ ЖЕ запросы, что и
Functions/do_audit (общий код, не копия), и прогоняет их по нескольким
реальным группам, замеряя фазы по отдельности:

    запрос ведущей / выборка ведущей / запрос ведомой / выборка ведомой /
    приведение типов и построение множеств / сравнение

НИЧЕГО НЕ ПИШЕТ: статусы в etl_jobs не трогаются, в etl_log не пишется,
в конце — rollback. Запускать можно на боевом сегменте.

    PYTHONPATH=$PWD python3 tools/audit_profile.py ipersonPostOrcl
    PYTHONPATH=$PWD python3 tools/audit_profile.py ipersonPostOrcl --groups 5
    PYTHONPATH=$PWD python3 tools/audit_profile.py ipersonPostOrcl --explain
    PYTHONPATH=$PWD python3 tools/audit_profile.py ipersonPostOrcl --probe
    PYTHONPATH=$PWD python3 tools/audit_profile.py ipersonPostOrcl --period 2026-03-01

Ключи:
  --groups N   сколько групп прогнать (по умолчанию 3). Группы берутся тем же
               запросом, что и у аудита (isokaudit НЕ в (1,4)); если таких нет,
               берутся любые группы линии из etl_jobs — профилировать можно и
               по здоровой линии.
  --period X   конкретная группа: дата ГГГГ-ММ-ДД, «null» (NULL-группа) либо
               год[-месяц[-день]] для составного периода.
  --probe      дополнительные замеры, разделяющие «плата за запрос» и «плата за
               строки»: тот же запрос под COUNT(*) (план тот же, строки не
               передаются) и повторный прогон (виден эффект кэша).
  --explain    план запроса ведущей и ведомой (Postgres — EXPLAIN ANALYZE,
               Oracle — EXPLAIN PLAN + DBMS_XPLAN). Именно он показывает, какая
               ЧАСТЬ запроса тяжёлая: например, соединение, которое считается
               целиком независимо от условия по периоду.
"""
import argparse
import datetime
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
sys.path.insert(0, ROOT)

from Functions.functionsFile.loadConfig import assemble  # noqa: E402
from Functions.functionsFile.takeOneQuery import TakeOneQuery  # noqa: E402
from Functions import do_audit as A  # noqa: E402
from Functions.do_etl import (  # noqa: E402
    _connect, _isPost, _pickSql, _loadStructure, _periodKey, _resolveEtlPath,
    _appendFilter, _executeQuery, _periodSpec, _periodBinds,
)
from Src.generalQueries import (  # noqa: E402
    structureEmptyQuerySql, getBadAuditPostSql, getBadAuditOrclSql,
)


def _direction(key):
    for master in ("Orcl", "Post"):
        for slave in ("Orcl", "Post"):
            if key.endswith(master + slave):
                return master, slave
    raise SystemExit(f"Не разобрал направление по имени ключа: {key}")


def _cfgFor(key):
    """Конфиг линии с теми же умолчаниями, что проставляет Do_audit."""
    data = assemble("config")["data"]
    if key not in data:
        similar = [k for k in data if k.lower() == key.lower()]
        hint = f" Есть ключ {similar[0]!r} — различие в регистре." if similar else ""
        raise SystemExit(f"Линия {key!r} не найдена в config.d.{hint}")
    cfg = dict(data[key])
    dbMaster, dbSlave = _direction(key)
    cfg["dbMaster"], cfg["dbSlave"] = dbMaster, dbSlave
    cfg["tableNameEtlJobs"] = key[: -len(dbMaster + dbSlave)]
    cfg.setdefault("tableNameMaster", cfg["tableNameEtlJobs"])
    cfg.setdefault("periodColumn", "createdate")
    cfg.setdefault("slavePeriodColumn", cfg["periodColumn"])
    cfg.setdefault("etlFields", None)
    cfg.setdefault("auditExcludeFields", None)
    cfg.setdefault("filterClause", None)
    cfg.setdefault("filterClauseSlave", None)
    cfg.setdefault("filterParams", {})
    cfg.setdefault("truncatePeriod", False)
    return cfg


def _parsePeriod(text):
    """'2026-03-01' -> date; 'null' -> None (NULL-группа); '2026-03' -> 1-е число."""
    text = (text or "").strip()
    if text.lower() in ("null", "none", ""):
        return None
    parts = text.replace("/", "-").split("-")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        raise SystemExit(f"Не разобрал период {text!r}: нужен ГГГГ-ММ-ДД или null.")
    while len(nums) < 3:
        nums.append(1)
    return datetime.date(*nums[:3])


def _anyGroupsSql(dbType):
    """Тот же запрос групп, но БЕЗ условия по isokaudit.

    Собирается из боевого текста (убираем строку с isokaudit), чтобы не
    заводить копию имени таблицы и схемы: на Oracle это koknaev.etl_jobs, и
    разъехавшаяся копия молча искала бы не там."""
    sql = _pickSql(dbType, getBadAuditPostSql, getBadAuditOrclSql)
    return "\n".join(line for line in sql.splitlines()
                     if "isokaudit" not in line.lower())


def _groupsOf(cursor, cfg, limit):
    """Группы линии: сначала те же, что взял бы аудит; если таких нет — любые."""
    key = cfg["tableNameEtlJobs"]
    rows = _executeQuery(cursor,
                         _pickSql(cfg["dbMaster"], getBadAuditPostSql,
                                  getBadAuditOrclSql),
                         {"tablename": key})
    source = "isokaudit НЕ в (1,4) — те же, что взял бы аудит"
    if not rows:
        rows = _executeQuery(cursor, _anyGroupsSql(cfg["dbMaster"]),
                             {"tablename": key})
        rows = rows[-limit:]        # ORDER BY period — берём последние
        source = "групп на проверку нет — взяты последние группы линии"
    return [r[1] for r in rows[:limit]], source


def _explain(cursor, dbType, sql, binds, title):
    print(f"\n--- план: {title} ---")
    try:
        if _isPost(dbType):
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS, TIMING) " + sql, binds)
            for row in cursor.fetchall():
                print("   ", row[0])
        else:
            cursor.execute("EXPLAIN PLAN FOR " + sql, binds)
            cursor.execute(
                "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY())")
            for row in cursor.fetchall():
                print("   ", row[0])
    except Exception as err:
        print(f"    (не удалось: {type(err).__name__}: {str(err)[:200]})")
        if not _isPost(dbType):
            print("    Oracle: нужен PLAN_TABLE и права на DBMS_XPLAN.")


def _countSql(dbType, sql):
    """Тот же запрос под COUNT(*): план тот же, строки клиенту не едут.
    Разница со временем полного запроса и есть цена передачи строк."""
    return f"SELECT COUNT(*) FROM (\n{sql}\n) prof"


def _dropCoalesce(sql, sentinel=A._SENTINEL):
    """Снять обёртку COALESCE(<выражение>, <сентинел>) -> <выражение>.

    Только для ПРОБЫ. Условие по периоду обёрнуто в COALESCE, чтобы NULL-группа
    сравнивалась как обычная (см. do_etl._periodCond). Побочный эффект: под
    функцией обычный индекс по колонке периода не применим — планировщику
    нужен индекс по тому же выражению. Проба выполняет тот же запрос без
    обёртки и показывает разницу во времени: если она в разы, узкое место
    найдено (и чинить его надо не здесь, а в сборке условия).
    Для NULL-группы проба бессмысленна — там без COALESCE ничего не найдётся.
    """
    needle = "COALESCE("
    tail = ", " + sentinel
    out, i = [], 0
    while True:
        start = sql.find(needle, i)
        if start < 0:
            out.append(sql[i:])
            return "".join(out)
        depth, k = 1, start + len(needle)
        while k < len(sql) and depth:
            if sql[k] == "(":
                depth += 1
            elif sql[k] == ")":
                depth -= 1
            k += 1
        inner = sql[start + len(needle):k - 1]
        if depth == 0 and inner.endswith(tail):
            out.append(sql[i:start])
            out.append(inner[:-len(tail)])
        else:
            out.append(sql[i:k])
        i = k


def _timeIt(fn):
    started = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - started


# ─────────────── самопроверка отчёта о расхождениях (без БД) ───────────────

def _selftest():
    """Проверить формат отчёта do_audit._diffLines на синтетических данных.

    Тут же его и видно глазами — не дожидаясь реального расхождения на боевом
    аудите. БД не нужна: _diffLines работает с уже выбранными строками."""
    import decimal
    D = decimal.Decimal
    structM = [("IDRW", "NUMBER", 0, "Primary Key"),
               ("SUMCHECK", "NUMBER", 2, None),
               ("LASTUPDATE", "DATE", None, None),
               ("ACCNO", "VARCHAR2", None, None)]
    structS = [("idrw", "numeric", 0, "Primary Key"),
               ("sumcheck", "numeric", 2, None),
               ("lastupdate", "timestamp", None, None),
               ("accno", "character varying", None, None)]
    lu1 = datetime.datetime(2026, 3, 1, 10, 0)
    lu2 = datetime.datetime(2026, 3, 2, 12, 30)
    # 101 и 102 обновились неверно, 103 не доехала, 104 лишняя в ведомой,
    # 105 отличается только хвостовым пробелом в строке
    onlyMaster = {(D("101"), D("100.00"), lu2, "A-1"),
                  (D("102"), D("55.00"), lu1, "A-2"),
                  (D("103"), D("10.00"), lu1, "A-3"),
                  (D("105"), D("7.00"), lu1, "A-5")}
    onlySlave = {(D("101"), D("90.00"), lu1, "A-1"),
                 (D("102"), D("50.00"), lu1, "A-2"),
                 (D("104"), D("1.00"), lu1, "A-4"),
                 (D("105"), D("7.00"), lu1, "A-5 ")}
    lines = A._diffLines(onlyMaster, onlySlave, structM, structS,
                         "EXPMED", "mocheck")
    print("Отчёт о расхождениях выглядит так:")
    for line in lines:
        print("   ", line)
    body = "\n".join(lines)
    assert "изменены 3" in lines[0] and "нет в ведомой mocheck 1" in lines[0]
    assert "нет в ведущей EXPMED 1" in lines[0]
    assert "IDRW=101: отличаются поля (2)" in body
    assert "SUMCHECK: ведущая 100.00 ≠ ведомая 90.00" in body
    assert "IDRW=103: нет в ведомой mocheck" in body
    assert "IDRW=104: нет в ведущей EXPMED" in body
    # строки печатаются через repr — иначе хвостовой пробел не отличить
    assert "ACCNO: ведущая 'A-5' ≠ ведомая 'A-5 '" in body
    assert "чаще всего расходятся колонки: SUMCHECK (2)" in body
    order = [l.split(":")[0] for l in lines if l.startswith("IDRW=")]
    assert order == ["IDRW=101", "IDRW=102", "IDRW=103", "IDRW=104",
                     "IDRW=105"], order
    short = A._diffLines(onlyMaster, onlySlave, structM, structS,
                         "EXPMED", "mocheck", limit=2)
    assert sum(1 for l in short if l.startswith("IDRW=")) == 2
    assert "показано ключей: 2 из 5" in "\n".join(short)

    # составной ключ, NULL в ключе и дубликаты по ключу
    structM2 = [("YEAR", "NUMBER", 0, "Primary Key"),
                ("CODE", "VARCHAR2", None, "Primary Key"),
                ("VAL", "NUMBER", 0, None)]
    structS2 = [("year", "numeric", 0, "Primary Key"),
                ("code", "character varying", None, "Primary Key"),
                ("val", "numeric", 0, None)]
    m2 = {(D("2026"), "a", D("1")), (None, "z", D("5")),
          (D("9"), "d", D("1")), (D("9"), "d", D("2"))}
    s2 = {(D("2026"), "a", D("2")), (None, "z", D("6")), (D("9"), "d", D("3"))}
    body2 = "\n".join(A._diffLines(m2, s2, structM2, structS2, "SRC", "dst"))
    assert "YEAR=NULL, CODE='z'" in body2, body2          # None не роняет сортировку
    assert "дубликаты — строк в ведущей 2, в ведомой 1" in body2, body2

    # структура без PK (medree): сопоставлять нечем — прежний вывод, но с шапкой
    structM3 = [("DCALC", "DATE", None, None), ("VAL", "NUMBER", 0, None)]
    structS3 = [("dcalc", "date", None, None), ("val", "numeric", 0, None)]
    body3 = "\n".join(A._diffLines({(datetime.date(2026, 1, 1), D("1"))},
                                   {(datetime.date(2026, 1, 1), D("2"))},
                                   structM3, structS3,
                                   "MEDREE_CONS", "medree_cons"))
    assert body3.startswith("колонки: DCALC, VAL"), body3

    # снятие COALESCE для пробы: и простая колонка, и выражение под TRUNC/DATE
    sent = A._SENTINEL
    got = _dropCoalesce(f"WHERE COALESCE(p.createdate, {sent}) = :createdate")
    assert got == "WHERE p.createdate = :createdate", got
    got = _dropCoalesce(f"WHERE COALESCE(DATE(p.dcalc), {sent}) = %(createdate)s")
    assert got == "WHERE DATE(p.dcalc) = %(createdate)s", got
    # чужой COALESCE (не про период) проба не трогает
    keep = "SELECT COALESCE(a, b) FROM t"
    assert _dropCoalesce(keep) == keep
    # составной период — обёртки нет, запрос остаётся как есть
    parts = "WHERE COALESCE(p.year, -1) = :createdate_y"
    assert _dropCoalesce(parts) == parts
    print("\nselftest OK")


def main(argv):
    if "--selftest" in argv:
        _selftest()
        return 0
    ap = argparse.ArgumentParser(
        description="Профиль аудита по фазам (ничего не пишет в БД).")
    ap.add_argument("line", help="ключ линии из config.d, напр. ipersonPostOrcl")
    ap.add_argument("--groups", type=int, default=3, help="сколько групп (3)")
    ap.add_argument("--period", help="конкретная группа (ГГГГ-ММ-ДД или null)")
    ap.add_argument("--probe", action="store_true",
                    help="дополнительно: COUNT(*) и повтор запроса")
    ap.add_argument("--explain", action="store_true", help="планы запросов")
    args = ap.parse_args(argv)

    cfg = _cfgFor(args.line)
    dbMaster, dbSlave = cfg["dbMaster"], cfg["dbSlave"]
    print(f"Линия {args.line}: {cfg['tableNameMaster']} ({dbMaster}) -> "
          f"{cfg['tableNameSlave']} ({dbSlave})")
    print(f"Каталог конфигурации: {os.environ['ETL_FULL_PATH']}etlFolder")

    conMaster, connectMasterSec = _timeIt(lambda: _connect(dbMaster))
    conSlave, connectSlaveSec = _timeIt(lambda: _connect(dbSlave))
    cursorMaster = conMaster.cursor()
    cursorSlave = conSlave.cursor()
    print(f"Подключение: ведущая {connectMasterSec:.2f}с, "
          f"ведомая {connectSlaveSec:.2f}с")

    try:
        structMaster = A._auditFields(_loadStructure(cfg["structureMaster"], dbMaster),
                                      cfg["etlFields"], cfg["auditExcludeFields"])
        structSlave = A._auditFields(_loadStructure(cfg["structureSlave"], dbSlave),
                                     cfg["etlFields"], cfg["auditExcludeFields"])
        if cfg.get("selectSql"):
            selectSql = TakeOneQuery(_resolveEtlPath(cfg["selectSql"]))
        else:
            selectSql = structureEmptyQuerySql.format(cfg["tableNameMaster"])
        selectSql = _appendFilter(selectSql, cfg.get("filterClause"))
        masterSql = A._buildMasterSql(cfg, selectSql, structMaster)
        slaveSql = A._buildSlaveSql(cfg, structSlave)
        excluded = A._asNameSet(cfg["auditExcludeFields"])
        print(f"Полей аудита: {len(structMaster)}"
              + (f" (исключены из сравнения: {', '.join(sorted(excluded))})"
                 if excluded else ""))
        print(f"Источник ведущей: "
              + (cfg["selectSql"] if cfg.get("selectSql")
                 else f"таблица {cfg['tableNameMaster']}"))

        if args.period:
            periods, source = [_parsePeriod(args.period)], "задана ключом --period"
        else:
            periods, source = _groupsOf(cursorMaster, cfg, args.groups)
        print(f"Групп для профиля: {len(periods)} ({source})")
        if not periods:
            print("В etl_jobs нет ни одной группы этой линии — профилировать нечего.")
            return 2

        specMaster = _periodSpec(cfg["periodColumn"])
        specSlave = _periodSpec(cfg["slavePeriodColumn"])
        totals = A._newTiming()

        for origPeriod in periods:
            period = _periodKey(origPeriod, cfg["truncatePeriod"])
            bindsMaster = {**_periodBinds(specMaster, period),
                           **(cfg.get("filterParams") or {})}
            bindsSlave = _periodBinds(specSlave, period)
            print("\n" + "=" * 70)
            print(f"Группа {period}")

            one = {}
            _, one["masterExec"] = _timeIt(
                lambda: cursorMaster.execute(masterSql, bindsMaster))
            masterRows, one["masterFetch"] = _timeIt(cursorMaster.fetchall)
            _, one["slaveExec"] = _timeIt(
                lambda: cursorSlave.execute(slaveSql, bindsSlave))
            slaveRows, one["slaveFetch"] = _timeIt(cursorSlave.fetchall)
            (set1, set2), one["convert"] = _timeIt(
                lambda: A._toComparable(dbMaster, dbSlave, masterRows, slaveRows,
                                        structMaster, structSlave))
            same, one["compare"] = _timeIt(lambda: set1 == set2)
            one["status"] = 0.0     # профиль ничего не пишет

            print(f"  строк: ведущая {len(masterRows)}, ведомая {len(slaveRows)}"
                  f" — {'совпадают' if same else 'ОТЛИЧАЮТСЯ'}")
            print(f"  {A._timingText(one)}")
            print(f"  итого {A._timingTotal(one):.2f}с")
            A._addTiming(totals, one, len(masterRows))

            if args.probe:
                masterFull = one["masterExec"] + one["masterFetch"]
                slaveFull = one["slaveExec"] + one["slaveFetch"]
                _, sec = _timeIt(lambda: cursorMaster.execute(
                    _countSql(dbMaster, masterSql), bindsMaster))
                cnt = cursorMaster.fetchone()[0]
                print(f"  проба: COUNT(*) по тому же запросу ведущей — "
                      f"{sec:.2f}с на {cnt} строк (строки клиенту не едут). "
                      f"Полный запрос+выборка был {masterFull:.2f}с — разница "
                      f"и есть цена передачи строк")
                _, sec2 = _timeIt(lambda: cursorMaster.execute(masterSql, bindsMaster))
                cursorMaster.fetchall()
                print(f"  проба: повтор того же запроса ведущей — {sec2:.2f}с "
                      f"(было {masterFull:.2f}с; разница — кэш, а не работа)")
                if period is None:
                    print("  проба без COALESCE пропущена: группа NULL — без "
                          "обёртки условие не нашло бы ничего")
                else:
                    for label, cur, sql, binds, was in (
                            ("ведущей", cursorMaster, masterSql, bindsMaster,
                             masterFull),
                            ("ведомой", cursorSlave, slaveSql, bindsSlave,
                             slaveFull)):
                        plain = _dropCoalesce(sql)
                        if plain == sql:
                            continue
                        try:
                            _, sec3 = _timeIt(lambda: cur.execute(plain, binds))
                            rows = cur.fetchall()
                        except Exception as err:
                            print(f"  проба без COALESCE ({label}) не удалась: "
                                  f"{type(err).__name__}: {str(err)[:150]}")
                            continue
                        print(f"  проба: запрос {label} БЕЗ обёртки COALESCE "
                              f"вокруг периода — {sec3:.2f}с на {len(rows)} "
                              f"строк (с обёрткой было {was:.2f}с). Разница в "
                              f"разы = условие не даёт использовать индекс по "
                              f"колонке периода")

            if args.explain:
                _explain(cursorMaster, dbMaster, masterSql, bindsMaster,
                         "ведущая")
                _explain(cursorSlave, dbSlave, slaveSql, bindsSlave, "ведомая")

        print("\n" + "=" * 70)
        if totals["groups"]:
            print(f"ИТОГО по {totals['groups']} группам "
                  f"({totals['rows']} строк ведущей):")
            total = A._timingTotal(totals)
            for name, label in A._PHASE_LABELS:
                if name == "status":
                    continue
                share = (100.0 * totals[name] / total) if total else 0.0
                print(f"  {label:<24} {totals[name]:7.2f}с  {share:5.1f}%")
            print(f"  {'ВСЕГО':<24} {total:7.2f}с   "
                  f"({total / totals['groups']:.2f}с на группу)")
            print("\nЧитать так: фаза, которая почти не меняется от размера "
                  "группы, и есть фиксированная плата за группу. Если это "
                  "«запрос ведущей/ведомой» — смотри --explain: план покажет, "
                  "какая часть считается целиком, независимо от периода.")
        return 0
    finally:
        for con in (conMaster, conSlave):
            try:
                con.rollback()      # профиль ничего не пишет — фиксировать нечего
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
