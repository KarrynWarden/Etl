#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Триггеры IUD на ведущих таблицах: генерация DDL, проверка в БД, применение.

Зачем модуль. Режимы `iud`, `delete_insert` и (дополнительно) `section_compare`
работают по журналу `etl_log_iud_row`: строки в него пишет ТРИГГЕР на ведущей
таблице. Триггер живёт в БД, а не в репозитории, поэтому «линия собрана» и
«линия работает» — разные вещи: без триггера прогон честно отчитывается
пропуском (⬜) и молча не переносит ничего. Здесь три функции:

  1. build_trigger()  — собрать DDL триггера по описанию линии (Oracle и
     Postgres, одиночный и составной PK, одиночный и составной период);
  2. check_targets()  — сходить в БД и проверить, что триггеры на месте,
     включены, валидны и пишут ПРАВИЛЬНЫЙ `tablename` (регистр значим!);
  3. apply_trigger()  — выполнить собранный DDL в БД.

Что именно проверяется — см. _evaluate(): наличие, включённость, валидность
(Oracle INVALID = ORA-04098 на каждом INSERT в ведущую), события
INSERT/UPDATE/DELETE, FOR EACH ROW, точное совпадение литерала `tablename`
(сравнение с `etl_jobs.tablename` в SQL регистрозависимое), упоминание колонки
периода и колонок PK.

Самопроверка без БД:  python3 tools/trigger_builder.py --selftest
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import dag_builder as B          # noqa: E402
from tools import sp_builder as SP          # noqa: E402

# Имя журнала, куда пишет триггер. По умолчанию — ровно то, что читает рантайм:
#   Oracle   — koknaev.etl_log_iud_row (см. queries/general/newEtl/SelectEtlIudOrcl.sql);
#   Postgres — etl_user.etl_log_iud_row (рантайм читает без схемы, через search_path;
#              так же пишет пример из oracleSetup/03_example_triggers.sql).
JOURNAL_DEFAULT = {"Orcl": "koknaev.etl_log_iud_row",
                   "Post": "etl_user.etl_log_iud_row"}
JOURNAL_BARE = "etl_log_iud_row"
JOURNAL_COLUMNS = ("idrw", "tablename", "timeoper", "oper", "period", "id", "isetl")

# Режимы, которым журнал (а значит триггер) НУЖЕН.
#   iud / delete_insert  — единственный источник событий;
#   section_compare      — журнал дополняет сравнение срезов (без него линия
#                          работает, но реагирует медленнее — только по срезу).
# section и query_section журнал не читают: список групп даёт isokaudit=4 либо
# свой periodsSql, триггер таким линиям не нужен.
TRIGGER_MODES = ("iud", "delete_insert", "section_compare")
TRIGGER_MODES_OPTIONAL = ("section_compare",)

# Максимальная длина идентификатора: Oracle 11g — 30, Postgres — 63.
_MAX_IDENT = {"Orcl": 30, "Post": 63}


# ─────────────────────────── имена объектов ───────────────────────────

def _sanitize(name):
    return re.sub(r"[^A-Za-z0-9_]", "_", str(name or "")).strip("_")


def _fit(prefix, core, suffix, limit):
    """prefix+core+suffix, укоротив СЕРЕДИНУ до лимита длины идентификатора."""
    room = limit - len(prefix) - len(suffix)
    if room < 1:
        raise ValueError("Слишком длинные префикс/суффикс имени объекта.")
    return prefix + core[:room] + suffix


def trigger_name(tablename, db="Orcl"):
    """Имя триггера линии: tr_<tablename>_after_iud (как в 02_trigger_template.sql).
    Для Oracle обрезается до 30 символов — иначе ORA-00972 на длинных именах
    (напр. medree_structure_stacionar)."""
    core = _sanitize(tablename).lower()
    if not core:
        raise ValueError("Не задано имя линии (tablename) для имени триггера.")
    return _fit("tr_", core, "_after_iud", _MAX_IDENT.get(db, 63))


def function_name(tablename):
    """Имя функции триггера (только Postgres): tr_etl_<tablename>_after_iud_func."""
    core = _sanitize(tablename).lower()
    if not core:
        raise ValueError("Не задано имя линии (tablename) для имени функции.")
    return _fit("tr_etl_", core, "_after_iud_func", _MAX_IDENT["Post"])


def needs_trigger(mode):
    return (mode or "iud") in TRIGGER_MODES


# ─────────────────────── выражения периода и PK ───────────────────────

def _period_parts(period_column):
    """dict-период -> [(ключ, колонка)] в порядке year/month/day; иначе None."""
    if not isinstance(period_column, dict):
        return None
    parts = [(k, period_column.get(k)) for k in ("year", "month", "day")]
    parts = [(k, v) for k, v in parts if v]
    return parts or None


def _period_expr(db, period_column, side):
    """Выражение периода для триггера. side: 'new' | 'old'.

    Одна колонка-дата подставляется как есть (журнальный `period` — DATE, и
    неявное приведение timestamp->date делает сама СУБД). Составной период
    (year[/month[/day]]) собирается в дату первого числа — ровно так, как его
    понимает ETL (см. README, «Период группы»).
    """
    parts = _period_parts(period_column)
    if db == "Orcl":
        ref = f":{side}."
        if not parts:
            if not period_column:
                return "NULL"
            return f"{ref}{period_column}"
        got = dict(parts)
        y = f"TO_CHAR({ref}{got['year']})"
        m = (f"LPAD(TO_CHAR(NVL({ref}{got['month']}, 1)), 2, '0')"
             if "month" in got else "'01'")
        d = (f"LPAD(TO_CHAR(NVL({ref}{got['day']}, 1)), 2, '0')"
             if "day" in got else "'01'")
        return (f"TO_DATE({y} || '-' || {m} || '-' || {d}, 'YYYY-MM-DD')")
    ref = f"{side}."
    if not parts:
        if not period_column:
            return "NULL"
        return f"{ref}{period_column}"
    got = dict(parts)
    m = f"coalesce({ref}{got['month']}, 1)::int" if "month" in got else "1"
    d = f"coalesce({ref}{got['day']}, 1)::int" if "day" in got else "1"
    return f"make_date({ref}{got['year']}::int, {m}, {d})"


def _pk_expr(db, pk_columns, side):
    """Выражение id для журнала. Составной PK склеивается через '/' — так его
    разбирает do_etl (см. README, «Составной PK»)."""
    if not pk_columns:
        raise ValueError(
            "Не отмечен первичный ключ: без PK триггер не сможет записать id "
            "изменённой строки. Отметь PK в таблице колонок.")
    if db == "Orcl":
        items = [f"TO_CHAR(:{side}.{c})" for c in pk_columns]
    else:
        items = [f"{side}.{c}::text" for c in pk_columns]
    return " || '/' || ".join(items)


# ─────────────────────────── генерация DDL ───────────────────────────

def _orcl_ddl(name, table, tablename, journal, period_column, pk_columns):
    pk_new, pk_old = _pk_expr("Orcl", pk_columns, "new"), _pk_expr("Orcl", pk_columns, "old")
    per_new, per_old = _period_expr("Orcl", period_column, "new"), \
        _period_expr("Orcl", period_column, "old")
    # Сравнение периодов через NVL: NULL <> NULL в Oracle не TRUE, иначе смена
    # периода с NULL на дату не породила бы 'D' по старой группе.
    zero = "TO_DATE('1900-01-01', 'YYYY-MM-DD')"
    return f"""CREATE OR REPLACE TRIGGER {name}
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW
DECLARE
    p_id     VARCHAR2(200);
    p_oper   VARCHAR2(2);
    p_period DATE;
BEGIN
    IF INSERTING THEN
        p_id     := {pk_new};
        p_oper   := 'IU';
        p_period := {per_new};
    ELSIF UPDATING THEN
        p_id     := {pk_new};
        p_oper   := 'IU';
        p_period := {per_new};
        -- Сменился PK или период — старую строку ведомой нужно удалить.
        IF {pk_old} <> {pk_new}
           OR NVL({per_old}, {zero}) <> NVL({per_new}, {zero}) THEN
            INSERT INTO {journal}(tablename, timeoper, oper, period, id, isetl)
            VALUES ('{tablename}', systimestamp, 'D', {per_old}, {pk_old}, 0);
        END IF;
    ELSIF DELETING THEN
        p_id     := {pk_old};
        p_oper   := 'D';
        p_period := {per_old};
    END IF;

    INSERT INTO {journal}(tablename, timeoper, oper, period, id, isetl)
    VALUES ('{tablename}', systimestamp, p_oper, p_period, p_id, 0);
END;"""


def _post_ddl(name, func, table, tablename, journal, period_column, pk_columns):
    pk_new, pk_old = _pk_expr("Post", pk_columns, "new"), _pk_expr("Post", pk_columns, "old")
    per_new, per_old = _period_expr("Post", period_column, "new"), \
        _period_expr("Post", period_column, "old")
    # SECURITY DEFINER: журнал обычно в чужой схеме (etl_user), а писать в него
    # должен любой, кто меняет ведущую. RETURN NULL — для AFTER-триггера
    # возвращаемое значение игнорируется.
    fn = f"""CREATE OR REPLACE FUNCTION {func}()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    p_id     text;
    p_oper   varchar(2);
    p_period date;
BEGIN
    IF TG_OP = 'DELETE' THEN
        p_id     := {pk_old};
        p_oper   := 'D';
        p_period := {per_old};
    ELSE
        p_id     := {pk_new};
        p_oper   := 'IU';
        p_period := {per_new};
        -- Сменился PK или период — старую строку ведомой нужно удалить.
        IF TG_OP = 'UPDATE'
           AND ({pk_old} <> {pk_new}
                OR coalesce({per_old}, '1900-01-01'::date)
                <> coalesce({per_new}, '1900-01-01'::date)) THEN
            INSERT INTO {journal}(tablename, timeoper, oper, period, id, isetl)
            VALUES ('{tablename}', clock_timestamp(), 'D', {per_old}, {pk_old}, 0);
        END IF;
    END IF;

    INSERT INTO {journal}(tablename, timeoper, oper, period, id, isetl)
    VALUES ('{tablename}', clock_timestamp(), p_oper, p_period, p_id, 0);
    RETURN NULL;
END;
$function$"""
    drop = f"DROP TRIGGER IF EXISTS {name} ON {table}"
    # EXECUTE PROCEDURE (а не FUNCTION): синтаксис принимают все версии, включая
    # PostgreSQL 10 и старше, где EXECUTE FUNCTION ещё не поддерживается.
    trg = (f"CREATE TRIGGER {name}\nAFTER INSERT OR UPDATE OR DELETE ON {table}\n"
           f"FOR EACH ROW EXECUTE PROCEDURE {func}()")
    return [fn, drop, trg]


def build_trigger(db, table, tablename, period_column, pk_columns, journal=None):
    """Собрать DDL триггера IUD. Возвращает dict:

        name       — имя триггера
        func       — имя функции (Postgres) или None
        journal    — куда пишется журнал
        statements — список ОТДЕЛЬНЫХ операторов (их и выполняет apply_trigger)
        text       — тот же DDL для показа/сохранения в .sql (Oracle-блоки
                     разделены '/', как ждут SQL*Plus и DBeaver)
    """
    if db not in ("Orcl", "Post"):
        raise ValueError(f"Неизвестная БД: {db!r}.")
    table = (table or "").strip()
    if not table:
        raise ValueError("Не задана ведущая таблица — не на что ставить триггер.")
    tablename = (tablename or "").strip()
    if not tablename:
        raise ValueError("Не задано имя линии (tablename) — триггеру нечего писать "
                         "в журнал.")
    journal = (journal or JOURNAL_DEFAULT[db]).strip()
    name = trigger_name(tablename, db)
    if db == "Orcl":
        stmts = [_orcl_ddl(name, table, tablename, journal, period_column, pk_columns)]
        text = stmts[0] + "\n/\n"
        func = None
    else:
        func = function_name(tablename)
        stmts = _post_ddl(name, func, table, tablename, journal,
                          period_column, pk_columns)
        text = ";\n\n".join(stmts) + ";\n"
    header = (f"-- Триггер IUD для линии {tablename} ({db}).\n"
              f"-- Ведущая: {table}; журнал: {journal}.\n"
              f"-- Сгенерирован конструктором (tools/trigger_builder.py); правки\n"
              f"-- руками при пересборке линии будут перезаписаны.\n")
    return {"name": name, "func": func, "journal": journal,
            "statements": stmts, "text": header + text}


# Путь версионируемой копии DDL — общий с ядром (его пишет build_all).
trigger_sql_rel = B.trigger_sql_rel


# ─────────────────────────── применение в БД ───────────────────────────

def _connect(db, cred="MAIN"):
    from Connect import connectPostgres, connectOracle  # noqa: E402
    return connectPostgres(cred) if db == "Post" else connectOracle(cred)


def apply_trigger(db, statements, cred="MAIN"):
    """Выполнить операторы DDL в БД. Возвращает список выполненного (для лога).
    Ошибка пробрасывается — вызывающий показывает её пользователю."""
    conn = _connect(db, cred)
    done = []
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
            done.append(stmt.strip().splitlines()[0])
        conn.commit()
    finally:
        conn.close()
    return done


# ─────────────────────────── проверка в БД ───────────────────────────

_ORCL_TRIGGERS_SQL = """
SELECT t.owner, t.trigger_name, t.table_owner, t.table_name, t.status,
       t.trigger_type, t.triggering_event, t.trigger_body
FROM all_triggers t
WHERE UPPER(t.table_name) = :tname
"""

_ORCL_TRIGGER_STATUS_SQL = """
SELECT status FROM all_objects
WHERE object_type = 'TRIGGER' AND owner = :own AND object_name = :nm
"""

_POST_TRIGGERS_SQL = """
SELECT n.nspname, t.tgname, c.relname, t.tgenabled,
       pg_get_triggerdef(t.oid), np.nspname || '.' || p.proname, p.prosrc
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_proc p ON p.oid = t.tgfoid
JOIN pg_namespace np ON np.oid = p.pronamespace
WHERE NOT t.tgisinternal AND lower(c.relname) = %(tname)s
"""


def _split_table(table):
    """'KOKNAEV.PRBDIR' -> ('KOKNAEV', 'PRBDIR'); 'prbdir' -> (None, 'prbdir')."""
    parts = str(table or "").replace('"', "").split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


def _fetch_triggers(db, cursor, table):
    """Триггеры на таблице -> список dict (единый вид для обоих диалектов)."""
    schema, bare = _split_table(table)
    out = []
    if db == "Orcl":
        cursor.execute(_ORCL_TRIGGERS_SQL, {"tname": bare.upper()})
        for (own, nm, tbl_own, tbl, status, ttype, events, body) in cursor.fetchall():
            if schema and tbl_own and tbl_own.upper() != schema.upper():
                continue
            out.append({"name": nm, "owner": own, "table": f"{tbl_own}.{tbl}",
                        "enabled": (status or "").upper() == "ENABLED",
                        "events": (events or ""), "row_level": "EACH ROW" in (ttype or ""),
                        "after": "AFTER" in (ttype or "").upper(),
                        "body": body or "", "func": None, "valid": None,
                        "definition": f"{ttype} / {events}"})
        for t in out:
            cursor.execute(_ORCL_TRIGGER_STATUS_SQL,
                           {"own": t["owner"], "nm": t["name"]})
            row = cursor.fetchone()
            t["valid"] = (row[0].upper() == "VALID") if row and row[0] else None
    else:
        cursor.execute(_POST_TRIGGERS_SQL, {"tname": bare.lower()})
        for (nsp, nm, tbl, enabled, tdef, func, src) in cursor.fetchall():
            if schema and nsp and nsp.lower() != schema.lower():
                continue
            up = (tdef or "").upper()
            out.append({"name": nm, "owner": nsp, "table": f"{nsp}.{tbl}",
                        # tgenabled: 'D' — отключён, остальное (O/R/A) — работает
                        "enabled": (enabled or "O") != "D",
                        "events": up, "row_level": "FOR EACH ROW" in up,
                        "after": " AFTER " in f" {up} ",
                        "body": src or "", "func": func, "valid": True,
                        "definition": tdef or ""})
    return out


def _evaluate(db, found, tablename, period_column, pk_columns):
    """Разобрать найденные триггеры. -> (status, problems, notes, matched).

    status: 'ok' | 'warn' | 'error' | 'missing'."""
    problems, notes = [], []
    journal_trgs = [t for t in found if JOURNAL_BARE in (t["body"] or "").lower()]
    if not journal_trgs:
        if found:
            notes.append("на таблице есть триггеры (" +
                         ", ".join(t["name"] for t in found) +
                         "), но ни один не пишет в " + JOURNAL_BARE)
        return "missing", ["триггера, пишущего в " + JOURNAL_BARE + ", нет"], notes, []

    literal = f"'{tablename}'"
    exact = [t for t in journal_trgs if literal in (t["body"] or "")]
    if not exact:
        loose = [t for t in journal_trgs
                 if literal.lower() in (t["body"] or "").lower()]
        if loose:
            problems.append(
                f"tablename в триггере записан в другом регистре — ожидается "
                f"{literal}. Сравнение с etl_jobs.tablename регистрозависимое, "
                f"ETL не увидит эти события")
            exact = loose
        else:
            problems.append(f"ни один триггер не пишет tablename = {literal} "
                            f"(нашлись: " +
                            ", ".join(t["name"] for t in journal_trgs) + ")")
            return "error", problems, notes, journal_trgs

    for t in exact:
        if not t["enabled"]:
            problems.append(f"{t['name']}: триггер ОТКЛЮЧЁН (DISABLED)")
        if t["valid"] is False:
            problems.append(f"{t['name']}: триггер INVALID — любое изменение "
                            f"ведущей падает (ORA-04098). Диагностика: "
                            f"ALTER TRIGGER {t['name']} COMPILE; "
                            f"SELECT text FROM user_errors WHERE name = "
                            f"'{t['name'].upper()}'")
        ev = (t["events"] or "").upper()
        missing_ev = [e for e in ("INSERT", "UPDATE", "DELETE") if e not in ev]
        if missing_ev:
            problems.append(f"{t['name']}: не отслеживает " +
                            ", ".join(missing_ev))
        if not t["row_level"]:
            problems.append(f"{t['name']}: не построчный (нет FOR EACH ROW) — "
                            f"события отдельных строк не попадут в журнал")
        if not t["after"]:
            notes.append(f"{t['name']}: не AFTER-триггер — проверь порядок "
                         f"относительно фиксации данных")
        body_low = (t["body"] or "").lower()
        for col in _period_columns(period_column):
            if col and col.lower() not in body_low:
                notes.append(f"{t['name']}: колонка периода {col} в теле не "
                             f"упоминается — период в журнале может быть не тем")
        for col in (pk_columns or []):
            if col.lower() not in body_low:
                notes.append(f"{t['name']}: колонка PK {col} в теле не "
                             f"упоминается — id строки может быть неполным")
        if "isetl" not in body_low:
            notes.append(f"{t['name']}: в теле нет isetl — проверь, что журнал "
                         f"пишется с isetl = 0")

    status = "error" if problems else ("warn" if notes else "ok")
    return status, problems, notes, exact


def _period_columns(period_column):
    parts = _period_parts(period_column)
    if parts:
        return [v for _k, v in parts]
    return [period_column] if period_column else []


# --- общие проверки по БД (журнал, служебные таблицы) ---

def _check_db_objects(db, cursor, journal):
    """Наличие журнала и служебных таблиц. -> (status, list[str])."""
    msgs, bad = [], False
    j_schema, _j = _split_table(journal)
    if db == "Orcl":
        cursor.execute("SELECT owner FROM all_tables WHERE table_name = :n",
                       {"n": JOURNAL_BARE.upper()})
        owners = [r[0] for r in cursor.fetchall()]
        if not owners:
            return "error", [f"таблицы {JOURNAL_BARE} не видно — журнал не создан "
                             f"или нет прав (etlFolder/queries/oracleSetup/"
                             f"01_create_etl_log_iud_row.sql)"]
        msgs.append(f"{JOURNAL_BARE}: есть (схемы: {', '.join(owners)})")
        if j_schema and j_schema.upper() not in [o.upper() for o in owners]:
            bad = True
            msgs.append(f"⚠ в схеме {j_schema} журнала нет — триггеры пишут не туда")
        own = (j_schema or owners[0]).upper()
        cursor.execute("SELECT column_name FROM all_tab_columns "
                       "WHERE owner = :o AND table_name = :n",
                       {"o": own, "n": JOURNAL_BARE.upper()})
        cols = {r[0].lower() for r in cursor.fetchall()}
        miss = [c for c in JOURNAL_COLUMNS if c not in cols]
        if miss:
            bad = True
            msgs.append("⚠ в журнале нет колонок: " + ", ".join(miss))
        cursor.execute("SELECT sequence_owner FROM all_sequences "
                       "WHERE sequence_name = :n",
                       {"n": "ETL_LOG_IUD_ROW_SEQ"})
        if not cursor.fetchall():
            msgs.append("⚠ последовательности etl_log_iud_row_seq не видно — "
                        "если idrw заполняется ею, INSERT в журнал упадёт")
        cursor.execute(
            "SELECT owner, status FROM all_objects "
            "WHERE object_type = 'TRIGGER' AND object_name = :n",
            {"n": "TRG_ETL_LOG_IUD_ROW_BI"})
        rows = cursor.fetchall()
        if rows:
            invalid = [o for o, st in rows if (st or "").upper() != "VALID"]
            if invalid:
                bad = True
                msgs.append("⚠ trg_etl_log_iud_row_bi INVALID (схемы: " +
                            ", ".join(invalid) + ") — ORA-04098 на любом INSERT "
                            "в журнал, а значит и на любом изменении ведущих")
            else:
                msgs.append("trg_etl_log_iud_row_bi: VALID")
        for tbl in ("etl_jobs", "etl_log"):
            cursor.execute("SELECT owner FROM all_tables WHERE table_name = :n",
                           {"n": tbl.upper()})
            if not cursor.fetchall():
                bad = True
                msgs.append(f"⚠ таблицы {tbl} не видно")
    else:
        cursor.execute("SELECT table_schema FROM information_schema.tables "
                       "WHERE table_name = %(n)s", {"n": JOURNAL_BARE})
        schemas = [r[0] for r in cursor.fetchall()]
        if not schemas:
            return "error", [f"таблицы {JOURNAL_BARE} не видно — журнал не создан "
                             f"или нет прав"]
        msgs.append(f"{JOURNAL_BARE}: есть (схемы: {', '.join(schemas)})")
        if j_schema and j_schema.lower() not in [s.lower() for s in schemas]:
            bad = True
            msgs.append(f"⚠ в схеме {j_schema} журнала нет — триггеры пишут не туда")
        cursor.execute("SELECT column_name FROM information_schema.columns "
                       "WHERE table_name = %(n)s AND table_schema = %(s)s",
                       {"n": JOURNAL_BARE, "s": (j_schema or schemas[0])})
        cols = {r[0].lower() for r in cursor.fetchall()}
        miss = [c for c in JOURNAL_COLUMNS if c not in cols]
        if miss:
            bad = True
            msgs.append("⚠ в журнале нет колонок: " + ", ".join(miss))
        for tbl in ("etl_jobs", "etl_log"):
            cursor.execute("SELECT table_schema FROM information_schema.tables "
                           "WHERE table_name = %(n)s", {"n": tbl})
            if not cursor.fetchall():
                bad = True
                msgs.append(f"⚠ таблицы {tbl} не видно")
    return ("warn" if bad else "ok"), msgs


# ─────────────────────── что вообще надо проверять ───────────────────────

def trigger_targets(include_sp=True):
    """Список линий с данными, нужными для проверки/генерации триггера.

    Элемент: key, kind ('etl'|'sp:regular'|'sp:once'), db_master, table_master,
    tablename (значение для журнала), mode, period_column, pk_columns,
    needs (нужен ли триггер), note (что помешало собрать данные).
    """
    out = []
    for key, body in sorted(B._all_config_bodies().items()):
        line, dbm, _dbs = B.split_key(key)
        mode = body.get("mode", "iud")
        pk, note = [], None
        try:
            pk = [c["column_name"] for c in B._cols_from_struct(body["structureMaster"])
                  if c.get("is_primary_key")]
        except Exception as e:
            note = f"структуру ведущей прочитать не удалось: {type(e).__name__}: {e}"
        if not pk and note is None:
            note = "в структуре ведущей не отмечен ни один PK"
        out.append({
            "key": key, "kind": "etl", "db_master": dbm,
            "table_master": body.get("tableNameMaster", line),
            "tablename": line, "mode": mode,
            "period_column": body.get("periodColumn"),
            "pk_columns": pk, "needs": needs_trigger(mode),
            "disabled": bool(body.get("disabled")), "note": note,
        })
    if include_sp:
        for kind in ("regular", "once"):
            for key in SP.list_sp_lines(kind):
                try:
                    data = SP.load_sp_line(kind, key)
                except Exception:
                    continue
                out.append({
                    "key": key, "kind": f"sp:{kind}", "db_master": data["db_master"],
                    "table_master": data.get("master_table") or "",
                    "tablename": data.get("master_label") or key,
                    "mode": "sp", "period_column": None, "pk_columns": [],
                    "needs": False, "disabled": bool(data.get("disabled")),
                    # Справочник и разовый перенос переносятся ПОЛНОЙ перезаливкой
                    # (DELETE FROM ведомой + INSERT всех строк) — журнал
                    # etl_log_iud_row они не читают, поэтому триггер им не нужен.
                    "note": "полная перезаливка — журнал не используется",
                })
    return out


def check_targets(targets, creds=None, journals=None, log=None):
    """Сходить в БД и проверить триггеры по списку линий.

    creds:    {'Orcl': 'MAIN', 'Post': 'MAIN'} — наборы реквизитов (.env);
    journals: {'Orcl': ..., 'Post': ...} — переопределить журнал;
    log:      callable(str) для прогресса (UI показывает его вживую).

    Возвращает (results, db_reports). results — по линии:
      key, kind, db, table, tablename, mode, status, problems, notes, found.
    status: ok | warn | error | missing | skip | fail.
    db_reports: {db: {'status':..., 'messages': [...], 'cred':..., 'journal':...}}.
    """
    creds = dict(creds or {})
    journals = dict(journals or {})
    log = log or (lambda _m: None)
    results, db_reports = [], {}

    by_db = {}
    for t in targets:
        by_db.setdefault(t["db_master"], []).append(t)

    for db, items in sorted(by_db.items()):
        cred = creds.get(db, "MAIN")
        journal = journals.get(db) or JOURNAL_DEFAULT.get(db, JOURNAL_BARE)
        log(f"Подключаюсь к {db} (реквизиты {cred})…")
        try:
            conn = _connect(db, cred)
        except Exception as e:
            db_reports[db] = {"status": "fail", "cred": cred, "journal": journal,
                              "messages": [f"подключиться не удалось: "
                                           f"{type(e).__name__}: {e}"]}
            for t in items:
                results.append(dict(t, db=db, status="fail",
                                    problems=["нет подключения к БД"], notes=[],
                                    found=[]))
            continue
        try:
            cur = conn.cursor()
            try:
                st, msgs = _check_db_objects(db, cur, journal)
            except Exception as e:
                st, msgs = "fail", [f"проверка служебных объектов не удалась: "
                                    f"{type(e).__name__}: {e}"]
            db_reports[db] = {"status": st, "cred": cred, "journal": journal,
                              "messages": msgs}
            for t in items:
                if not t["needs"]:
                    results.append(dict(t, db=db, status="skip", problems=[],
                                        notes=[t.get("note") or
                                               "режим не использует журнал"],
                                        found=[]))
                    continue
                log(f"Проверяю {t['key']} ({t['table_master']})…")
                try:
                    found = _fetch_triggers(db, cur, t["table_master"])
                    status, problems, notes, matched = _evaluate(
                        db, found, t["tablename"], t["period_column"],
                        t["pk_columns"])
                except Exception as e:
                    results.append(dict(t, db=db, status="fail",
                                        problems=[f"{type(e).__name__}: {e}"],
                                        notes=[], found=[]))
                    continue
                if t.get("note"):
                    notes = list(notes) + [t["note"]]
                if t["mode"] in TRIGGER_MODES_OPTIONAL and status == "missing":
                    # section_compare живёт и без журнала (сравнение срезов),
                    # поэтому это не ошибка, а замечание.
                    status = "warn"
                    problems, notes = [], list(notes) + [
                        "триггера нет; режим section_compare работает и без него "
                        "(по сравнению срезов), но реагирует только на срезе"]
                results.append(dict(t, db=db, status=status, problems=problems,
                                    notes=notes,
                                    found=[f["name"] for f in matched] or
                                          [f["name"] for f in found]))
        finally:
            conn.close()
    order = {"error": 0, "missing": 1, "fail": 2, "warn": 3, "ok": 4, "skip": 5}
    results.sort(key=lambda r: (order.get(r["status"], 9), r["key"]))
    return results, db_reports


# ─────────────────────────── самопроверка (без БД) ───────────────────────────

def _selftest():
    # 1) Oracle, одиночный PK и одиночный период
    t = build_trigger("Orcl", "koknaev.eindexmo", "EINDEXMO", "createdate", ["idrw"])
    assert t["name"] == "tr_eindexmo_after_iud", t["name"]
    ddl = t["statements"][0]
    assert "AFTER INSERT OR UPDATE OR DELETE ON koknaev.eindexmo" in ddl
    assert "VALUES ('EINDEXMO', systimestamp, p_oper, p_period, p_id, 0)" in ddl
    assert "koknaev.etl_log_iud_row" in ddl
    assert "TO_CHAR(:new.idrw)" in ddl and "TO_CHAR(:old.idrw)" in ddl
    assert t["text"].rstrip().endswith("/")

    # 2) Oracle, составной PK и составной период
    t2 = build_trigger("Orcl", "PLANOMS", "PLANOMS",
                       {"year": "YEAR", "month": "MO"}, ["IDRW1", "IDRW2"])
    d2 = t2["statements"][0]
    assert "TO_CHAR(:new.IDRW1) || '/' || TO_CHAR(:new.IDRW2)" in d2
    assert "LPAD(TO_CHAR(NVL(:new.MO, 1)), 2, '0')" in d2 and "'01'" in d2

    # 3) Postgres: функция + пересоздание триггера
    t3 = build_trigger("Post", "public.reqprepmo", "reqprepmo", "createdate", ["idrw"])
    assert t3["func"] == "tr_etl_reqprepmo_after_iud_func", t3["func"]
    assert len(t3["statements"]) == 3
    assert t3["statements"][0].startswith("CREATE OR REPLACE FUNCTION")
    assert t3["statements"][1] == "DROP TRIGGER IF EXISTS tr_reqprepmo_after_iud ON public.reqprepmo"
    assert "EXECUTE PROCEDURE" in t3["statements"][2]
    assert "new.idrw::text" in t3["statements"][0]
    assert "etl_user.etl_log_iud_row" in t3["statements"][0]
    t3b = build_trigger("Post", "t", "t2", {"year": "y", "month": "m", "day": "d"},
                        ["a", "b"])
    assert "make_date(new.y::int, coalesce(new.m, 1)::int, coalesce(new.d, 1)::int)" \
        in t3b["statements"][0]
    assert "new.a::text || '/' || new.b::text" in t3b["statements"][0]

    # 4) длинные имена: Oracle обрезается до 30, Postgres — нет
    long_name = "medree_structure_stacionar"
    assert len(trigger_name(long_name, "Orcl")) <= 30
    assert trigger_name(long_name, "Post") == f"tr_{long_name}_after_iud"

    # 5) без PK — понятная ошибка, а не молча битый триггер
    try:
        build_trigger("Orcl", "t", "t", "createdate", [])
        raise AssertionError("ожидалась ошибка про отсутствующий PK")
    except ValueError as e:
        assert "первичный ключ" in str(e)

    # 6) режимы, которым триггер нужен
    assert needs_trigger("iud") and needs_trigger("delete_insert")
    assert not needs_trigger("section") and not needs_trigger("query_section")

    # 7) разбор результатов проверки
    body_ok = ("insert into etl_log_iud_row(tablename, timeoper, oper, period, id, "
               "isetl) values ('EINDEXMO', systimestamp, 'IU', :new.createdate, "
               "TO_CHAR(:new.idrw), 0)")
    good = [{"name": "TR_E", "owner": "K", "table": "K.E", "enabled": True,
             "events": "INSERT OR UPDATE OR DELETE", "row_level": True,
             "after": True, "body": body_ok, "func": None, "valid": True,
             "definition": ""}]
    st, pr, no, matched = _evaluate("Orcl", good, "EINDEXMO", "createdate", ["idrw"])
    assert st == "ok", (st, pr, no)
    assert len(matched) == 1
    # чужой регистр tablename — ошибка (сравнение регистрозависимое)
    st2, pr2, _n2, _m2 = _evaluate("Orcl", good, "eindexmo", "createdate", ["idrw"])
    assert st2 == "error" and "регистре" in pr2[0], (st2, pr2)
    # нет триггера с журналом
    st3, pr3, _n3, _m3 = _evaluate("Orcl", [], "EINDEXMO", "createdate", ["idrw"])
    assert st3 == "missing", st3
    # отключён и не полный набор событий
    bad = [dict(good[0], enabled=False, events="INSERT", row_level=False,
                valid=False)]
    st4, pr4, _n4, _m4 = _evaluate("Orcl", bad, "EINDEXMO", "createdate", ["idrw"])
    assert st4 == "error"
    assert any("ОТКЛЮЧЁН" in p for p in pr4) and any("INVALID" in p for p in pr4)
    assert any("UPDATE, DELETE" in p for p in pr4)
    assert any("FOR EACH ROW" in p for p in pr4)

    # 8) имя таблицы со схемой
    assert _split_table("KOKNAEV.PRBDIR") == ("KOKNAEV", "PRBDIR")
    assert _split_table("prbdir") == (None, "prbdir")
    assert trigger_sql_rel("eindexmoOrclPost") == "queries/triggers/eindexmoOrclPost.sql"

    # 9) полный проход check_targets на подставном курсоре: порядок колонок в
    #    выборках (all_triggers / pg_trigger) ломается молча, поэтому проверяем
    #    сборку результата целиком, без БД.
    _orig_connect = globals()["_connect"]

    class _Cur(object):
        def __init__(self, db):
            self.db, self.rows = db, []

        def execute(self, sql, params=None):
            p = params or {}
            low = sql.lower()
            if "all_triggers" in low:
                self.rows = [("KOKNAEV", "TR_EINDEXMO_AFTER_IUD", "KOKNAEV",
                              "EINDEXMO", "ENABLED", "AFTER EACH ROW",
                              "INSERT OR UPDATE OR DELETE", body_ok)]
            elif "pg_trigger" in low:
                self.rows = [("public", "tr_p_after_iud", "p", "O",
                              "CREATE TRIGGER tr_p_after_iud AFTER INSERT OR DELETE "
                              "OR UPDATE ON public.p FOR EACH ROW EXECUTE PROCEDURE f()",
                              "public.f",
                              "insert into etl_user.etl_log_iud_row(tablename, oper, "
                              "period, id, isetl) values ('p', 'IU', new.createdate, "
                              "new.idrw::text, 0)")]
            elif "all_objects" in low:
                self.rows = ([("KOKNAEV", "VALID")] if "n" in p else [("VALID",)])
            elif "all_sequences" in low:
                self.rows = [("KOKNAEV",)]
            elif "all_tab_columns" in low or "information_schema.columns" in low:
                self.rows = [(c,) for c in JOURNAL_COLUMNS]
            elif "all_tables" in low or "information_schema.tables" in low:
                self.rows = [("koknaev" if self.db == "Orcl" else "etl_user",)]
            else:
                self.rows = []

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.rows[0] if self.rows else None

    class _Conn(object):
        def __init__(self, db):
            self.db = db

        def cursor(self):
            return _Cur(self.db)

        def close(self):
            pass

    globals()["_connect"] = lambda db, cred="MAIN": _Conn(db)
    try:
        targets = [
            {"key": "EINDEXMOOrclPost", "kind": "etl", "db_master": "Orcl",
             "table_master": "KOKNAEV.EINDEXMO", "tablename": "EINDEXMO",
             "mode": "iud", "period_column": "createdate", "pk_columns": ["idrw"],
             "needs": True, "note": None},
            {"key": "SECTIONOrclPost", "kind": "etl", "db_master": "Orcl",
             "table_master": "KOKNAEV.X", "tablename": "X", "mode": "section",
             "period_column": "dcalc", "pk_columns": ["idrw"], "needs": False,
             "note": None},
            {"key": "pPostOrcl", "kind": "etl", "db_master": "Post",
             "table_master": "public.p", "tablename": "p", "mode": "iud",
             "period_column": "createdate", "pk_columns": ["idrw"], "needs": True,
             "note": None},
        ]
        res, reps = check_targets(targets)
        by = {r["key"]: r for r in res}
        assert by["EINDEXMOOrclPost"]["status"] == "ok", by["EINDEXMOOrclPost"]
        assert by["EINDEXMOOrclPost"]["found"] == ["TR_EINDEXMO_AFTER_IUD"]
        assert by["SECTIONOrclPost"]["status"] == "skip"
        assert by["pPostOrcl"]["status"] == "ok", by["pPostOrcl"]
        assert reps["Orcl"]["status"] == "ok", reps["Orcl"]
        assert reps["Post"]["status"] == "ok", reps["Post"]
        assert any("etl_log_iud_row: есть" in m for m in reps["Orcl"]["messages"])
        # проблемные статусы идут первыми — с них и начинают разбираться
        assert [r["status"] for r in res] == sorted(
            [r["status"] for r in res],
            key=lambda s: {"error": 0, "missing": 1, "fail": 2, "warn": 3,
                           "ok": 4, "skip": 5}[s])
    finally:
        globals()["_connect"] = _orig_connect
    print("trigger_builder selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
