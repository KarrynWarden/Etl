#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Триггеры на ведущих таблицах: генерация DDL, проверка в БД, применение.

Зачем модуль. Половина линии живёт не в репозитории, а в БД — это ТРИГГЕР на
ведущей таблице. Триггеров два разных вида, и путать их нельзя:

  • **журнальный** (`etl_log_iud_row`) — для сложных линий в режимах `iud`,
    `delete_insert`, `section_compare_with_iud`. Пишет строку на каждое
    изменение; без него прогон честно отчитывается пропуском (⬜) и молча не
    переносит ничего;
  • **аудитный** (`etl_jobs.isokaudit = 0`) — для СПРАВОЧНИКОВ и разового
    переноса. Даг `SpEtlNew` берёт работу из `etl_jobs` (`isokaudit = 0`), а
    ноль туда ставит именно триггер на ведущей. Без него справочник не
    перенесётся НИКОГДА — при этом ни одна ошибка нигде не появится.

Функции:

  1. build_trigger()       — DDL журнального триггера по описанию линии (Oracle
     и Postgres, одиночный и составной PK, одиночный и составной период);
  2. build_audit_trigger() — DDL аудитного триггера справочника;
  3. check_targets()       — сходить в БД и проверить, что триггеры на месте,
     включены, валидны и пишут ПРАВИЛЬНЫЙ литерал (регистр значим!);
  4. apply_trigger()       — выполнить собранный DDL в БД;
  5. guess_from_sql()      — «предположить» колонки ведущей, когда линия
     построена не на таблице, а на своём SELECT (см. ниже).

Что именно проверяется — см. _evaluate() и _evaluate_audit(): наличие,
включённость, валидность (Oracle INVALID = ORA-04098 на каждом INSERT в
ведущую), события INSERT/UPDATE/DELETE, FOR EACH ROW, точное совпадение
литерала (сравнение с `etl_jobs.tablename` в SQL регистрозависимое), упоминание
колонки периода и колонок PK.

**Свой SELECT (кастомный источник).** Если линия построена на `selectSql`, то
`periodColumn`/PK — это имена колонок ЗАПРОСА, а не ведущей таблицы:
в `MOCHECK.sql` период `createdate` — это `dschet` из TFINSCHET, а PK `idrw` —
это `id` из PODCHECK. Проверять тело триггера по ним нельзя: получались ложные
замечания «колонка CREATEDATE в теле не упоминается». Поэтому такие линии
разбираются отдельно: guess_from_sql() находит выражение, стоящее за
псевдонимом, и дальше и проверка, и DDL идут уже по нему; если разобрать
запрос не удалось — проверка колонок молча пропускается с честной пометкой
«источник — свой SELECT».

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

bare = B.bare                                # 'KOKNAEV.PRBDIR' -> 'PRBDIR'

# Имя журнала, куда пишет триггер. По умолчанию — ровно то, что читает рантайм:
#   Oracle   — koknaev.etl_log_iud_row (см. queries/general/newEtl/SelectEtlIudOrcl.sql);
#   Postgres — etl_user.etl_log_iud_row (рантайм читает без схемы, через search_path;
#              так же пишет пример из oracleSetup/03_example_triggers.sql).
JOURNAL_DEFAULT = {"Orcl": "koknaev.etl_log_iud_row",
                   "Post": "etl_user.etl_log_iud_row"}
JOURNAL_BARE = "etl_log_iud_row"
JOURNAL_COLUMNS = ("idrw", "tablename", "timeoper", "oper", "period", "id", "isetl")

# Таблица заданий: её isokaudit = 0 — единственный сигнал «перенеси справочник».
JOBS_DEFAULT = {"Orcl": "koknaev.etl_jobs", "Post": "etl_user.etl_jobs"}
JOBS_BARE = "etl_jobs"

# Режимы, которым журнал (а значит триггер) НУЖЕН.
#   iud / delete_insert       — единственный источник событий;
#   section_compare_with_iud  — сравнение срезов ПЛЮС журнал.
# section, query_section и section_compare журнал не читают: список групп даёт
# сравнение срезов, isokaudit=4 либо свой periodsSql — триггер им не нужен.
TRIGGER_MODES = ("iud", "delete_insert", "section_compare_with_iud")
# Режимы, где журнал желателен, но не обязателен. После разделения
# section_compare на две штуки таких не осталось: кто хочет журнал — берёт
# `section_compare_with_iud` и тогда триггер обязателен. Константа оставлена:
# на неё смотрит UI, и она понадобится, если такой режим появится снова.
TRIGGER_MODES_OPTIONAL = ()

# Все режимы переноса — один список на конструктор, проверку и подсказки.
ALL_MODES = ("iud", "section", "section_compare", "section_compare_with_iud",
             "delete_insert", "query_section")

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


def audit_trigger_name(table, db="Orcl"):
    """Имя аудитного триггера справочника: tr_<таблица>_iud (как TR_CALENDAR_IUD
    и tr_eindexstru_iud, которые уже стоят в БД)."""
    core = _sanitize(bare(table)).lower()
    if not core:
        raise ValueError("Не задана таблица для имени аудитного триггера.")
    return _fit("tr_", core, "_iud", _MAX_IDENT.get(db, 63))


# Общая функция аудитных триггеров Postgres — ровно та, что уже развёрнута в
# БД (метка таблицы приходит аргументом TG_ARGV[0], поэтому функция одна на все
# справочники).
AUDIT_FUNC_POST = "tr_etl_table_iud_func"


def needs_trigger(mode):
    return (mode or "iud") in TRIGGER_MODES


# ─────────────────────── выражения периода и PK ───────────────────────

def _raw_expr(value):
    """dict-выражение {'expr': ..., 'columns': [...]} -> (expr, columns); иначе None.

    Такую форму отдаёт guess_from_sql() для кастомного источника: за псевдонимом
    запроса может стоять не колонка, а выражение
    (`TO_DATE(YEAR||'-01-01','YYYY-MM-DD') createdate` в MOCHECK.sql)."""
    if isinstance(value, dict) and value.get("expr"):
        return str(value["expr"]), list(value.get("columns") or [])
    return None


def _period_parts(period_column):
    """dict-период -> [(ключ, колонка)] в порядке year/month/day; иначе None."""
    if not isinstance(period_column, dict) or _raw_expr(period_column):
        return None
    parts = [(k, period_column.get(k)) for k in ("year", "month", "day")]
    parts = [(k, v) for k, v in parts if v]
    return parts or None


def _side_ref(db, side):
    return f":{side}." if db == "Orcl" else f"{side}."


def _expr_for_side(db, value, side):
    """Готовое выражение (dict с 'expr') -> текст со ссылками на строку триггера."""
    raw = _raw_expr(value)
    if not raw:
        return None
    expr, columns = raw
    return _prefix_columns(expr, columns or _expr_columns(expr),
                           _side_ref(db, side))


def _period_expr(db, period_column, side):
    """Выражение периода для триггера. side: 'new' | 'old'.

    Одна колонка-дата подставляется как есть (журнальный `period` — DATE, и
    неявное приведение timestamp->date делает сама СУБД). Составной период
    (year[/month[/day]]) собирается в дату первого числа — ровно так, как его
    понимает ETL (см. README, «Период группы»). Форма {'expr': ...} — готовое
    выражение из своего SELECT (см. guess_from_sql).
    """
    ready = _expr_for_side(db, period_column, side)
    if ready:
        return ready
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
    разбирает do_etl (см. README, «Составной PK»). Элемент списка — имя колонки
    либо {'expr': ...} (готовое выражение из своего SELECT)."""
    if not pk_columns:
        raise ValueError(
            "Не отмечен первичный ключ: без PK триггер не сможет записать id "
            "изменённой строки. Отметь PK в таблице колонок.")
    items = []
    for c in pk_columns:
        ready = _expr_for_side(db, c, side)
        if ready is None:
            ref = f"{_side_ref(db, side)}{c}"
            items.append(f"TO_CHAR({ref})" if db == "Orcl" else f"{ref}::text")
        else:
            items.append(f"TO_CHAR({ready})" if db == "Orcl"
                         else f"({ready})::text")
    return " || '/' || ".join(items)


# ──────────────── разбор своего SELECT: «предположить триггер» ────────────────
# Линия может строиться не на таблице, а на своём запросе (`selectSql`). Тогда
# имена в конфиге — это ПСЕВДОНИМЫ запроса, и на ведущей таких колонок нет:
#     ... dschet createdate ... FROM TFINSCHET      -> период это dschet
#     ... id     idrw       ... FROM PODCHECK       -> ключ   это id
#     ... TO_DATE(YEAR||'-01-01','YYYY-MM-DD') createdate ...
# Разбор здесь намеренно грубый (регулярки, а не парсер SQL): он не обязан
# понимать любой запрос — он обязан либо дать ответ, либо честно сказать
# «не разобрал». Всё, что он находит, показывается человеку ДО применения DDL.

# Слова, которые не могут быть псевдонимом/колонкой в хвосте выражения.
_NOT_ALIAS = {"END", "NULL", "ASC", "DESC", "DISTINCT", "ALL", "IS", "NOT",
              "AND", "OR", "ELSE", "THEN", "WHEN", "CASE", "BY", "FROM"}
# Ключевые слова и типовые функции — не колонки.
_SQL_WORDS = {
    "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "BY", "JOIN", "LEFT",
    "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "AND", "OR", "NOT", "IN",
    "IS", "NULL", "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT",
    "UNION", "ALL", "BETWEEN", "LIKE", "EXISTS", "WITH", "PARTITION", "OVER",
    "ASC", "DESC", "DUAL", "INTERVAL", "DATE", "TIMESTAMP", "CAST", "AT",
}
_AGGREGATES = ("MAX", "MIN", "SUM", "AVG", "COUNT")

_IDENT = r"[A-Za-z_][A-Za-z0-9_$#]*"
_UNION_RE = re.compile(r"\bUNION\s+ALL\b|\bUNION\b|\bINTERSECT\b|\bMINUS\b"
                       r"|\bEXCEPT\b", re.I)
_FROM_ITEM_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(%s(?:\.%s)?)(\s+(?!ON\b|WHERE\b|JOIN\b|LEFT\b|RIGHT\b"
    r"|INNER\b|GROUP\b|ORDER\b|UNION\b|USING\b|FULL\b|CROSS\b)(%s))?"
    % (_IDENT, _IDENT, _IDENT), re.I)


def _mask(sql):
    """Копия запроса той же длины, где литералы и комментарии забиты пробелами.

    По ней ищутся ключевые слова и скобки: иначе UNION внутри строки-константы
    или закомментированная скобка ломают разбор."""
    out = list(sql)
    i, n = 0, len(sql)
    while i < n:
        if sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            for k in range(i, min(j + 1, n)):
                out[k] = " "
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            for k in range(i, j):
                out[k] = " "
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i)
            j = n if j < 0 else j + 2
            for k in range(i, j):
                out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def _depths(masked):
    """Глубина вложенности скобок в каждой позиции (у самих скобок — внешняя)."""
    out, d = [], 0
    for ch in masked:
        if ch == "(":
            out.append(d)
            d += 1
        elif ch == ")":
            d = max(0, d - 1)
            out.append(d)
        else:
            out.append(d)
    return out


def split_union_branches(sql):
    """Разбить запрос на ветки UNION [ALL] верхнего уровня."""
    masked = _mask(sql or "")
    depth = _depths(masked)
    parts, last = [], 0
    for m in _UNION_RE.finditer(masked):
        if depth[m.start()] != 0:
            continue
        parts.append(sql[last:m.start()])
        last = m.end()
    parts.append(sql[last:])
    return [p for p in (s.strip() for s in parts) if p]


def _split_commas(text):
    """Разбить по запятым верхнего уровня (внутри скобок запятые не считаются)."""
    masked = _mask(text)
    depth = _depths(masked)
    parts, last = [], 0
    for i, ch in enumerate(masked):
        if ch == "," and depth[i] == 0:
            parts.append(text[last:i])
            last = i + 1
    parts.append(text[last:])
    return [p for p in (s.strip() for s in parts) if p]


def _split_alias(item):
    """'dschet createdate' -> ('dschet', 'createdate'); 'a.ID' -> ('a.ID', 'ID')."""
    item = " ".join(str(item).split())
    m = re.match(r"^(%s)(?:\.(%s))?$" % (_IDENT, _IDENT), item)
    if m:
        return item, (m.group(2) or m.group(1))
    m = re.search(r"\s+AS\s+\"?(%s)\"?$" % _IDENT, item, re.I)
    if m:
        return item[:m.start()].strip(), m.group(1)
    m = re.search(r"[\s)]\"?(%s)\"?$" % _IDENT, item)
    if m and m.group(1).upper() not in _NOT_ALIAS:
        return item[:m.start() + 1].strip(), m.group(1)
    return item, None


def select_items(branch):
    """Список (выражение, псевдоним) из SELECT-списка ветки. [] — не разобрал."""
    masked = _mask(branch)
    depth = _depths(masked)
    ms = re.search(r"\bSELECT\b", masked, re.I)
    if not ms or depth[ms.start()] != 0:
        return []
    base = depth[ms.start()]
    end = None
    for mf in re.finditer(r"\bFROM\b", masked, re.I):
        if mf.start() > ms.end() and depth[mf.start()] == base:
            end = mf.start()
            break
    if end is None:
        return []
    body = branch[ms.end():end]
    body = re.sub(r"^\s*(DISTINCT|ALL)\b", "", body, flags=re.I)
    return [_split_alias(p) for p in _split_commas(body)]


def from_items(branch, any_depth=False):
    """[(таблица, псевдоним|None)] из FROM/JOIN ветки.

    По умолчанию — только верхний уровень: если ведущая спрятана в подзапросе,
    колонки SELECT-списка ей всё равно не принадлежат. any_depth=True нужен
    там, где важен лишь сам факт «запрос читает эту таблицу» (справочники: их
    SELECT часто обёрнут подзапросом ради row_number)."""
    masked = _mask(branch)
    depth = _depths(masked)
    out = []
    for m in _FROM_ITEM_RE.finditer(masked):
        if not any_depth and depth[m.start()] != 0:
            continue
        table = branch[m.start(1):m.end(1)]
        alias = branch[m.start(3):m.end(3)] if m.group(3) else None
        out.append((table, alias))
    return out


def _sql_idents(expr):
    """[(начало, конец, текст)] идентификаторов выражения — без функций и слов SQL."""
    masked = _mask(expr)
    out = []
    for m in re.finditer(r"%s(?:\.%s)?" % (_IDENT, _IDENT), masked):
        word = expr[m.start():m.end()]
        tail = masked[m.end():].lstrip()
        if tail.startswith("("):            # вызов функции, а не колонка
            continue
        if word.upper() in _SQL_WORDS:
            continue
        out.append((m.start(), m.end(), word))
    return out


def _expr_columns(expr):
    """Имена колонок, встречающихся в выражении (без квалификаторов)."""
    seen, out = set(), []
    for _s, _e, word in _sql_idents(expr):
        name = word.split(".")[-1]
        if name.lower() in seen:
            continue
        seen.add(name.lower())
        out.append(name)
    return out


def _prefix_columns(expr, columns, prefix):
    """Подставить `:new.` / `new.` перед колонками выражения (и снять квалификатор).

    `MAX(m.idrw)` + prefix ':new.' -> `MAX(:new.idrw)`; строки-константы и имена
    функций не трогаются."""
    want = {str(c).split(".")[-1].lower() for c in (columns or [])}
    pieces, last = [], 0
    for start, end, word in _sql_idents(expr):
        name = word.split(".")[-1]
        if name.lower() not in want:
            continue
        pieces.append(expr[last:start])
        pieces.append(prefix + name)
        last = end
    pieces.append(expr[last:])
    return "".join(pieces)


def _unwrap_aggregate(expr):
    """`MAX(m.idrw)` -> ('m.idrw', True). В строке триггера агрегата нет."""
    changed = False
    while True:
        m = re.match(r"^(?:%s)\s*\((.*)\)$" % "|".join(_AGGREGATES),
                     expr.strip(), re.I | re.S)
        if not m:
            return expr.strip(), changed
        inner = m.group(1)
        masked = _mask(inner)
        # скобки внутри должны быть сбалансированы, иначе это MAX(a) + MAX(b)
        if masked.count("(") != masked.count(")"):
            return expr.strip(), changed
        expr, changed = inner, True


def _resolve_expr(expr, table, aliases):
    """Привести выражение запроса к виду, годному для триггера.

    Возвращает (значение, замечания). Значение — имя колонки (строка), словарь
    {'expr','columns'} для сложного выражения либо None, если выражение к строке
    ведущей не относится (константа, колонка ДРУГОЙ таблицы из JOIN)."""
    notes = []
    expr, agg = _unwrap_aggregate(expr)
    if agg:
        notes.append(f"в запросе значение агрегировано ({expr}) — в триггере "
                     f"берётся сама колонка строки")
    idents = _sql_idents(expr)
    if not idents:
        return None, notes + [f"за псевдонимом стоит константа ({expr}) — "
                              f"колонку ведущей по нему не определить"]
    own = {a.lower() for a in aliases}
    own.add(bare(table).lower())
    foreign = sorted({w.split(".")[0] for _s, _e, w in idents
                      if "." in w and w.split(".")[0].lower() not in own})
    if foreign:
        return None, notes + [
            "выражение ссылается на другую таблицу запроса (" +
            ", ".join(foreign) + ") — такую колонку триггер на " +
            f"{table} прочитать не может"]
    columns = _expr_columns(expr)
    if len(idents) == 1 and re.match(r"^%s(?:\.%s)?$" % (_IDENT, _IDENT),
                                     expr.strip()):
        return columns[0], notes
    return {"expr": expr.strip(), "columns": columns}, notes


def guess_from_sql(sql_text, table, aliases=(), filter_clause=None):
    """Что стоит за псевдонимами своего SELECT для ведущей `table`.

    Возвращает dict:
        branches      — сколько веток UNION ссылаются на эту таблицу;
        values        — {псевдоним: колонка | {'expr','columns'}};
        missing       — псевдонимы, которых в запросе не нашлось;
        notes         — что стоит показать человеку;
        ok            — удалось ли определить хоть что-то.
    Ничего не выдумывает: не нашёл — говорит, что не нашёл.
    """
    res = {"branches": 0, "values": {}, "missing": [], "notes": [],
           "ok": False, "table": table}
    if not sql_text or not table:
        res["notes"].append("нет текста запроса или имени ведущей таблицы")
        return res
    want = bare(table).lower()
    branches = split_union_branches(sql_text)
    all_items = [select_items(b) for b in branches]
    # Имена колонок РЕЗУЛЬТАТА UNION даёт первая ветка, а дальше сопоставление
    # идёт ПО ПОРЯДКУ: в MOCHECK.sql позиция 'idrw' у PODCHECK названа 'id', а
    # 'createdate' у EXPMED — 'createate' (опечатка в исходном запросе). Поэтому
    # ищем сначала позицию псевдонима, и только потом — псевдоним в самой ветке.
    width = len(all_items[0]) if all_items else 0
    picked, joined = [], []
    for branch, items in zip(branches, all_items):
        froms = from_items(branch)
        if not any(bare(t).lower() == want for t, _a in froms):
            continue
        if not items:
            continue
        # Псевдонимы ИМЕННО ведущей: `mc.IDRW` из присоединённой MEDCHECK
        # строке триггера на EXPMED недоступен, и предполагать его нельзя.
        entry = (items, [a for t, a in froms if a and bare(t).lower() == want])
        # Ведущая ветки — ПЕРВАЯ таблица FROM. Если наша таблица подцеплена
        # через JOIN (как MEDCHECK в ветках EXPMED), строки этой ветки к ней не
        # относятся: такие ветки берём, только если своих не нашлось вовсе.
        (picked if bare(froms[0][0]).lower() == want else joined).append(entry)
    if not picked and joined:
        picked = joined
        res["notes"].append(
            f"{table} встречается в запросе только как присоединённая таблица "
            f"(JOIN) — колонки предположены по этим веткам, проверь глазами")
    res["branches"] = len(picked)
    if not picked:
        res["notes"].append(
            f"в запросе не нашлось ветки с FROM {table} — разобрать псевдонимы "
            f"не по чему")
        return res
    if len(picked) > 1 and filter_clause:
        res["notes"].append(
            f"веток с {table} несколько ({len(picked)}); фильтр линии "
            f"«{filter_clause}» здесь не разбирается — сверь колонки глазами")

    def _note(text):
        if text not in res["notes"]:
            res["notes"].append(text)

    def _index_of(alias):
        for items in all_items:
            for i, (_expr, al) in enumerate(items):
                if al and al.lower() == str(alias).lower():
                    return i
        return None

    def _lookup(alias):
        """Значение псевдонима по всем подходящим веткам (+ расхождения)."""
        idx = _index_of(alias)
        found, seen = None, []
        for items, aliases in picked:
            exprs = []
            if idx is not None and len(items) == width:
                exprs.append(items[idx][0])
            else:
                exprs += [e for e, al in items
                          if al and al.lower() == str(alias).lower()]
            for expr in exprs:
                value, notes = _resolve_expr(expr, table, aliases)
                for n in notes:
                    _note(f"{alias}: {n}")
                if value is None:
                    continue
                key = value if isinstance(value, str) else value["expr"]
                if key.lower() not in [s.lower() for s in seen]:
                    seen.append(key)
                if found is None:
                    found = value
        if len(seen) > 1:
            _note(f"{alias}: в разных ветках запроса разные выражения (" +
                  ", ".join(seen) + f") — взято первое ({seen[0]})")
        return found

    for alias in aliases or ():
        if not alias or alias in res["values"]:
            continue
        got = _lookup(alias)
        if got is None:
            res["missing"].append(alias)
        else:
            res["values"][alias] = got
    res["ok"] = bool(res["values"])
    return res


def guess_line_columns(sql_text, table, period_column, pk_columns,
                       filter_clause=None):
    """Предположение в терминах линии: period_column и pk_columns для триггера.

    К результату guess_from_sql добавляет ключи `period` и `pk` уже в тех
    формах, которые принимает build_trigger (строка, dict год/месяц/день либо
    {'expr': ...})."""
    parts = _period_parts(period_column)
    if parts:
        p_aliases = [v for _k, v in parts]
    elif isinstance(period_column, str) and period_column:
        p_aliases = [period_column]
    else:
        p_aliases = []
    pk_aliases = [c for c in (pk_columns or []) if isinstance(c, str)]
    res = guess_from_sql(sql_text, table, p_aliases + pk_aliases, filter_clause)
    vals = res["values"]
    if parts:
        got = {}
        for key, alias in parts:
            value = vals.get(alias)
            if isinstance(value, str):
                got[key] = value
            elif value:
                res["notes"].append(
                    f"{alias}: за псевдонимом стоит выражение "
                    f"({value['expr']}) — часть составного периода так не "
                    f"собрать, впиши колонку руками")
        res["period"] = got or None
    else:
        res["period"] = vals.get(p_aliases[0]) if p_aliases else None
    res["pk"] = [vals[a] for a in pk_aliases if vals.get(a)]
    res["ok"] = bool(res["period"] or res["pk"])
    return res


def guess_columns_text(guess):
    """Человекочитаемая сводка предположения (для UI и отчёта)."""
    def _one(v):
        if isinstance(v, str):
            return v
        if isinstance(v, dict) and v.get("expr"):
            return v["expr"]
        if isinstance(v, dict):
            return "+".join(str(x) for x in v.values())
        return "—"
    parts = []
    if guess.get("period"):
        parts.append("период " + _one(guess["period"]))
    if guess.get("pk"):
        parts.append("PK " + ", ".join(_one(v) for v in guess["pk"]))
    if guess.get("missing"):
        parts.append("не нашлось: " + ", ".join(guess["missing"]))
    return "; ".join(parts) or "ничего определить не удалось"


def guess_columns(guess):
    """Все колонки ведущей, которые упоминает предположение (для проверки тела)."""
    out = []
    for value in [guess.get("period")] + list(guess.get("pk") or []):
        if isinstance(value, str):
            out.append(value)
        elif isinstance(value, dict) and value.get("expr"):
            out.extend(value.get("columns") or _expr_columns(value["expr"]))
        elif isinstance(value, dict):
            out.extend(str(v) for v in value.values() if v)
    seen, uniq = set(), []
    for c in out:
        if c.lower() in seen:
            continue
        seen.add(c.lower())
        uniq.append(c)
    return uniq


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


def build_audit_trigger(db, table, dependence, jobs=None):
    """DDL аудитного триггера СПРАВОЧНИКА (etl_jobs.isokaudit = 0).

    Справочник переносится не по журналу, а по заданию: даг `SpEtlNew` берёт
    `SELECT DISTINCT tablename FROM etl_jobs WHERE isokaudit = 0` и гоняет все
    линии с такой `dependence`. Ноль туда ставит этот триггер — без него
    справочник не обновится никогда и ни одной ошибки при этом не будет.

    `dependence` — метка зависимости линии (по умолчанию имя ведущей); ровно её
    даг сравнивает с `etl_jobs.tablename`, поэтому регистр значим.
    """
    if db not in ("Orcl", "Post"):
        raise ValueError(f"Неизвестная БД: {db!r}.")
    table = (table or "").strip()
    if not table:
        raise ValueError("Не задана таблица справочника — не на что ставить триггер.")
    dependence = (dependence or "").strip()
    if not dependence:
        raise ValueError("Не задана зависимость (tablename в etl_jobs) — "
                         "триггеру нечего помечать.")
    jobs = (jobs or JOBS_DEFAULT[db]).strip()
    name = audit_trigger_name(table, db)
    header = (f"-- Аудитный триггер справочника {dependence} ({db}).\n"
              f"-- Ведущая: {table}; задание: {jobs}.\n"
              f"-- Ставит isokaudit = 0 — это и есть сигнал «перенеси справочник».\n")
    if db == "Orcl":
        stmt = f"""CREATE OR REPLACE TRIGGER {name}
BEFORE INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {jobs}
       SET isokaudit = 0, last_success_ts = sysdate
     WHERE tablename = '{dependence}';
END;"""
        return {"name": name, "func": None, "jobs": jobs, "kind": "audit",
                "statements": [stmt], "text": header + stmt + "\n/\n"}
    fn = f"""CREATE OR REPLACE FUNCTION {AUDIT_FUNC_POST}()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    param text := TG_ARGV[0];
BEGIN
    UPDATE {jobs} SET isokaudit = 0, last_success_ts = clock_timestamp()
     WHERE tablename = param;
    IF TG_OP = 'DELETE' THEN
        RETURN old;
    END IF;
    RETURN new;
END;
$function$"""
    drop = f"DROP TRIGGER IF EXISTS {name} ON {table}"
    trg = (f"CREATE TRIGGER {name}\n"
           f"BEFORE INSERT OR UPDATE OR DELETE ON {table}\n"
           f"FOR EACH ROW EXECUTE PROCEDURE {AUDIT_FUNC_POST}('{dependence}')")
    stmts = [fn, drop, trg]
    return {"name": name, "func": AUDIT_FUNC_POST, "jobs": jobs, "kind": "audit",
            "statements": stmts, "text": header + ";\n\n".join(stmts) + ";\n"}


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


def _common_problems(t, problems, notes):
    """Проблемы, общие для журнального и аудитного триггера."""
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
        problems.append(f"{t['name']}: не отслеживает " + ", ".join(missing_ev))
    if not t["row_level"]:
        problems.append(f"{t['name']}: не построчный (нет FOR EACH ROW) — "
                        f"события отдельных строк не попадут в журнал")


def _evaluate(db, found, tablename, period_columns, pk_columns,
              source_note=None):
    """Разобрать найденные журнальные триггеры. -> (status, problems, notes, matched).

    period_columns / pk_columns — имена колонок ВЕДУЩЕЙ (для линии на своём
    SELECT их даёт guess_line_columns; пустой список = проверку тела по
    колонкам не делаем). source_note — пояснение про источник линии.

    status: 'ok' | 'warn' | 'error' | 'missing'."""
    problems, notes = [], []
    if source_note:
        notes.append(source_note)
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
        _common_problems(t, problems, notes)
        if not t["after"]:
            notes.append(f"{t['name']}: не AFTER-триггер — проверь порядок "
                         f"относительно фиксации данных")
        body_low = (t["body"] or "").lower()
        for col in (period_columns or []):
            if col and col.lower() not in body_low:
                notes.append(f"{t['name']}: колонка периода {col} в теле не "
                             f"упоминается — период в журнале может быть не тем")
        for col in (pk_columns or []):
            if col and col.lower() not in body_low:
                notes.append(f"{t['name']}: колонка PK {col} в теле не "
                             f"упоминается — id строки может быть неполным")
        if "isetl" not in body_low:
            notes.append(f"{t['name']}: в теле нет isetl — проверь, что журнал "
                         f"пишется с isetl = 0")

    status = "error" if problems else ("warn" if notes else "ok")
    return status, problems, notes, exact


def _evaluate_audit(db, found, dependence, jobs_row=None):
    """Разобрать аудитные триггеры справочника. -> (status, problems, notes, matched).

    Ищем триггер, который на изменение ведущей ставит `etl_jobs.isokaudit = 0`
    для СВОЕЙ метки зависимости. У Postgres метка живёт не в теле функции (она
    общая на все справочники), а в аргументе триггера — поэтому смотрим и
    определение тоже. jobs_row: есть ли строка dependence в etl_jobs
    (None — не проверяли)."""
    problems, notes = [], []
    audit_trgs = [t for t in found
                  if JOBS_BARE in (t["body"] or "").lower()
                  and "isokaudit" in ((t["body"] or "") +
                                      (t["definition"] or "")).lower()]
    if not audit_trgs:
        if found:
            notes.append("на таблице есть триггеры (" +
                         ", ".join(t["name"] for t in found) +
                         f"), но ни один не ставит {JOBS_BARE}.isokaudit = 0")
        return ("missing",
                [f"нет триггера, который помечает {JOBS_BARE}.isokaudit = 0 — "
                 f"справочник не будет переноситься (даг берёт работу только "
                 f"оттуда)"], notes, [])

    literal = f"'{dependence}'"
    exact = [t for t in audit_trgs
             if literal in ((t["body"] or "") + (t["definition"] or ""))]
    if not exact:
        both = [((t["body"] or "") + (t["definition"] or "")).lower()
                for t in audit_trgs]
        if any(literal.lower() in b for b in both):
            problems.append(
                f"метка зависимости записана в другом регистре — ожидается "
                f"{literal}: сравнение с etl_jobs.tablename регистрозависимое, "
                f"даг справочников такую пометку не увидит")
            exact = audit_trgs
        else:
            problems.append(
                f"триггер есть, но помечает не {literal} (нашлись: " +
                ", ".join(t["name"] for t in audit_trgs) +
                ") — проверь «Зависимость» линии")
            return "error", problems, notes, audit_trgs

    for t in exact:
        _common_problems(t, problems, notes)
    if jobs_row is False:
        problems.append(
            f"в {JOBS_BARE} нет строки tablename = {literal}: UPDATE из "
            f"триггера не изменит ни одной строки и справочник не поедет. "
            f"Заведи строку (period/last_success_ts любые, isokaudit = 0)")
    status = "error" if problems else ("warn" if notes else "ok")
    return status, problems, notes, exact


def _period_columns(period_column):
    """Имена колонок периода — одной строкой, составным периодом или выражением."""
    raw = _raw_expr(period_column)
    if raw:
        return raw[1] or _expr_columns(raw[0])
    parts = _period_parts(period_column)
    if parts:
        return [v for _k, v in parts]
    return [period_column] if isinstance(period_column, str) and period_column else []


# --- общие проверки по БД (журнал, служебные таблицы) ---

def _check_db_objects(db, cursor, journal, jobs=None):
    """Наличие журнала и служебных таблиц. -> (status, list[str], info).

    info['jobs'] — найденная таблица заданий (со схемой): по ней проверяются
    строки зависимостей справочников."""
    msgs, bad = [], False
    info = {"jobs": jobs or JOBS_DEFAULT.get(db, JOBS_BARE)}
    j_schema, _j = _split_table(journal)
    if db == "Orcl":
        cursor.execute("SELECT owner FROM all_tables WHERE table_name = :n",
                       {"n": JOURNAL_BARE.upper()})
        owners = [r[0] for r in cursor.fetchall()]
        if not owners:
            return "error", [f"таблицы {JOURNAL_BARE} не видно — журнал не создан "
                             f"или нет прав (etlFolder/queries/oracleSetup/"
                             f"01_create_etl_log_iud_row.sql)"], info
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
        has_seq = bool(cursor.fetchall())
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
        elif not has_seq:
            # Ругаемся, только если idrw заполнять НЕЧЕМ: ни своей
            # последовательности, ни BI-триггера. Само по себе отсутствие
            # etl_log_iud_row_seq ничего не значит — колонка может быть
            # identity или иметь DEFAULT с другой последовательностью, и
            # безусловное предупреждение было ложной тревогой.
            msgs.append("⚠ не видно ни последовательности etl_log_iud_row_seq, "
                        "ни триггера trg_etl_log_iud_row_bi — проверь, чем "
                        "заполняется idrw (identity / DEFAULT), иначе INSERT в "
                        "журнал упадёт")
        for tbl in ("etl_jobs", "etl_log"):
            cursor.execute("SELECT owner FROM all_tables WHERE table_name = :n",
                           {"n": tbl.upper()})
            rows = cursor.fetchall()
            if not rows:
                bad = True
                msgs.append(f"⚠ таблицы {tbl} не видно")
            elif tbl == JOBS_BARE:
                info["jobs"] = f"{rows[0][0]}.{JOBS_BARE}"
    else:
        cursor.execute("SELECT table_schema FROM information_schema.tables "
                       "WHERE table_name = %(n)s", {"n": JOURNAL_BARE})
        schemas = [r[0] for r in cursor.fetchall()]
        if not schemas:
            return "error", [f"таблицы {JOURNAL_BARE} не видно — журнал не создан "
                             f"или нет прав"], info
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
            rows = cursor.fetchall()
            if not rows:
                bad = True
                msgs.append(f"⚠ таблицы {tbl} не видно")
            elif tbl == JOBS_BARE:
                info["jobs"] = f"{rows[0][0]}.{JOBS_BARE}"
    return ("warn" if bad else "ok"), msgs, info


def _jobs_row_exists(db, cursor, jobs, tablename):
    """Есть ли в etl_jobs строка с такой меткой (None — проверить не вышло).

    Для справочника это половина работоспособности: триггер ставит isokaudit=0
    UPDATE-ом, а UPDATE по несуществующей строке не делает ничего."""
    try:
        if db == "Orcl":
            cursor.execute(f"SELECT COUNT(*) FROM {jobs} WHERE tablename = :n",
                           {"n": tablename})
        else:
            cursor.execute(f"SELECT COUNT(*) FROM {jobs} WHERE tablename = %(n)s",
                           {"n": tablename})
        row = cursor.fetchone()
        return bool(row and row[0])
    except Exception:
        return None


# ─────────────────────── что вообще надо проверять ───────────────────────

def _read_etl_sql(rel):
    """Текст .sql линии из etlFolder (или None, если файла нет)."""
    if not rel:
        return None
    try:
        return B._read_text(os.path.join(B.ETLFOLDER, rel))
    except OSError:
        return None


def sp_dependence(data, key):
    """Метка, которую даг справочников сравнивает с etl_jobs.tablename.

    Ровно как в dags/SpDagNew.py: явная `dependence`, иначе метка ведущей из
    ключа, у которой отброшена цифра 1/2 на конце (SPEINDEX1 -> SPEINDEX —
    так две линии одной таблицы делят одну зависимость)."""
    dep = (data.get("dependence") or "").strip()
    if dep:
        return dep
    label = data.get("master_label") or B.split_key(key)[0]
    if label and label[-1] in ("1", "2"):
        label = label[:-1]
    return label


def sp_master_tables(data):
    """Таблицы, на которые справочнику имеет смысл ставить аудитный триггер.

    Обычный справочник — своя ведущая. Справочник на своём SELECT — ПЕРВАЯ
    таблица каждой ветки запроса (метка линии таблицей не является). Фрагмент
    вообще без selectSql читает ведущую целиком — значит, ведущую и смотрим."""
    text = (data.get("select_sql_text") or "").strip()
    if data.get("src_mode") != "custom" or not text:
        table = (data.get("master_table") or "").strip()
        return [table] if table else []
    seen, out = set(), []
    for branch in split_union_branches(text):
        items = from_items(branch) or from_items(branch, any_depth=True)
        if not items:
            continue
        table = items[0][0]
        if table.lower() in seen:
            continue
        seen.add(table.lower())
        out.append(table)
    return out


def trigger_targets(include_sp=True, include_disabled=False):
    """Список линий с данными, нужными для проверки/генерации триггера.

    Элемент: key, kind ('etl'|'sp:regular'|'sp:once'), db_master, table_master,
    tables (что смотреть в БД), tablename (литерал в журнале / метка
    зависимости), mode, check ('iud'|'audit'|'none'), period_column,
    pk_columns, guess (разбор своего SELECT или None), needs, disabled, note.

    include_disabled=False — архивные линии (флаг `disabled` либо даг в
    dags/_archived/) в список не попадают вовсе: их не обслуживает ни один даг,
    и требовать от них триггер бессмысленно — это был чистый шум в отчёте.
    """
    out = []
    archived = set()
    try:
        archived = set(B.list_archived_lines())
    except Exception:
        pass
    for key, body in sorted(B._all_config_bodies().items()):
        line, dbm, _dbs = B.split_key(key)
        mode = body.get("mode", "iud")
        disabled = bool(body.get("disabled")) or key in archived
        if disabled and not include_disabled:
            continue
        pk, note = [], None
        try:
            pk = [c["column_name"] for c in B._cols_from_struct(body["structureMaster"])
                  if c.get("is_primary_key")]
        except Exception as e:
            note = f"структуру ведущей прочитать не удалось: {type(e).__name__}: {e}"
        if not pk and note is None and needs_trigger(mode):
            note = "в структуре ведущей не отмечен ни один PK"
        table = body.get("tableNameMaster", line)
        # Тот же дефолт, что и у рантайма (Do_etl: periodColumn -> createdate),
        # иначе у половины линий период «не задан» и проверять нечего.
        period = body.get("periodColumn") or "createdate"
        # Свой SELECT: имена в конфиге — псевдонимы ЗАПРОСА, а не колонки
        # ведущей. Разбираем запрос и дальше живём по предположению.
        guess = None
        select_rel = body.get("selectSql")
        if select_rel and needs_trigger(mode):
            text = _read_etl_sql(select_rel)
            if text:
                guess = guess_line_columns(text, table, period, pk,
                                           body.get("filterClause"))
                guess["source"] = select_rel
        out.append({
            "key": key, "kind": "etl", "db_master": dbm,
            "table_master": table, "tables": [table] if table else [],
            "tablename": line, "mode": mode,
            "check": "iud" if needs_trigger(mode) else "none",
            "period_column": period, "pk_columns": pk,
            "select_sql": select_rel, "guess": guess,
            "needs": needs_trigger(mode),
            "disabled": disabled, "archived": key in archived, "note": note,
        })
    if include_sp:
        for kind in ("regular", "once"):
            for key in SP.list_sp_lines(kind):
                try:
                    data = SP.load_sp_line(kind, key)
                except Exception:
                    continue
                disabled = bool(data.get("disabled"))
                if disabled and not include_disabled:
                    continue
                tables = sp_master_tables(data)
                # Разовый перенос запускают руками — регулярного сигнала ему не
                # нужно. Регулярный справочник живёт ТОЛЬКО аудитным триггером.
                check = "audit" if kind == "regular" else "none"
                note = None
                if check == "audit" and not tables:
                    note = ("не удалось понять, на какой таблице должен стоять "
                            "триггер: линия на своём SELECT, а FROM в нём не "
                            "разобрался")
                elif check == "none":
                    note = "разовый перенос — запускается вручную, триггер не нужен"
                out.append({
                    "key": key, "kind": f"sp:{kind}", "db_master": data["db_master"],
                    "table_master": tables[0] if tables else
                                    (data.get("master_table") or ""),
                    "tables": tables,
                    # Даг справочников сравнивает с etl_jobs.tablename именно
                    # `dependence` (по умолчанию — метка ведущей).
                    "tablename": sp_dependence(data, key),
                    "mode": "sp", "check": check,
                    "period_column": None, "pk_columns": [],
                    "select_sql": data.get("select_sql"), "guess": None,
                    "needs": check != "none", "disabled": disabled,
                    "archived": False, "note": note,
                })
    return out


def effective_columns(target):
    """(колонки периода, колонки PK) ведущей для проверки тела и сборки DDL.

    Для обычной линии это то, что в конфиге. Для линии на своём SELECT — то,
    что удалось предположить по запросу (см. guess_line_columns): в конфиге
    там лежат ПСЕВДОНИМЫ запроса, а в триггере должны стоять колонки ведущей.
    """
    guess = target.get("guess")
    if guess and guess.get("ok"):
        return guess.get("period"), list(guess.get("pk") or [])
    if target.get("select_sql"):
        return None, []
    return target.get("period_column"), list(target.get("pk_columns") or [])


def _source_note(target):
    """Пояснение про кастомный источник (или None)."""
    rel = target.get("select_sql")
    if not rel:
        return None
    guess = target.get("guess")
    if guess and guess.get("ok"):
        return (f"источник — свой SELECT ({rel}); колонки ведущей предположены "
                f"по нему: {guess_columns_text(guess)}")
    return (f"источник — свой SELECT ({rel}): в конфиге псевдонимы запроса, а "
            f"не колонки ведущей — тело триггера по ним не проверяю")


def _check_journal(db, triggers_of, target):
    """Проверка журнального триггера линии. -> (status, problems, notes, matched, found)."""
    found = triggers_of(target["table_master"])
    period, pk = effective_columns(target)
    guess = target.get("guess")
    if guess:
        # Кастомный источник: тело сверяем не с псевдонимами конфига, а с тем,
        # что стоит за ними в запросе (одним списком — колонки могут прийти из
        # выражения вроде TO_DATE(YEAR||'-01-01', ...)).
        status, problems, notes, matched = _evaluate(
            db, found, target["tablename"], [], [], _source_note(target))
        body_cols = guess_columns(guess)
        for t in matched:
            body_low = (t["body"] or "").lower()
            miss = [c for c in body_cols if c.lower() not in body_low]
            if miss:
                notes.append(f"{t['name']}: предположенные по запросу колонки "
                             f"({', '.join(miss)}) в теле не упоминаются — "
                             f"сверь триггер с запросом")
        notes.extend(guess.get("notes") or [])
    else:
        status, problems, notes, matched = _evaluate(
            db, found, target["tablename"], _period_columns(period), pk,
            _source_note(target))
    return status, problems, notes, matched, found


def _check_audit(db, triggers_of, cursor, jobs, target):
    """Проверка аудитного триггера справочника (etl_jobs.isokaudit = 0)."""
    tables = target.get("tables") or ([target["table_master"]]
                                      if target.get("table_master") else [])
    if not tables:
        return ("error", ["не на чем проверять: не определена таблица справочника"],
                [], [], [])
    row = _jobs_row_exists(db, cursor, jobs, target["tablename"]) if jobs else None
    best = None
    seen = []
    for table in tables:
        found = triggers_of(table)
        seen.extend(found)
        st, problems, notes, matched = _evaluate_audit(
            db, found, target["tablename"], row)
        rank = {"ok": 0, "warn": 1, "error": 2, "missing": 3}.get(st, 4)
        if best is None or rank < best[0]:
            best = (rank, st, problems, notes, matched, found)
        if rank == 0:
            break
    _rank, status, problems, notes, matched, found = best
    if len(tables) > 1:
        notes.append("линия на своём SELECT; смотрел таблицы: " +
                     ", ".join(tables))
    return status, problems, notes, matched, (found or seen)


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
            info = {"jobs": JOBS_DEFAULT.get(db, JOBS_BARE)}
            try:
                st, msgs, info = _check_db_objects(db, cur, journal)
            except Exception as e:
                st, msgs = "fail", [f"проверка служебных объектов не удалась: "
                                    f"{type(e).__name__}: {e}"]
            db_reports[db] = {"status": st, "cred": cred, "journal": journal,
                              "jobs": info.get("jobs"), "messages": msgs}
            cache = {}          # таблица -> её триггеры (одна выборка на таблицу)

            def _triggers(table):
                key = str(table).lower()
                if key not in cache:
                    cache[key] = _fetch_triggers(db, cur, table)
                return cache[key]

            for t in items:
                if t.get("check", "iud" if t.get("needs") else "none") == "none":
                    results.append(dict(t, db=db, status="skip", problems=[],
                                        notes=[t.get("note") or
                                               "режим не использует журнал"],
                                        found=[]))
                    continue
                log(f"Проверяю {t['key']} ({t['table_master']})…")
                try:
                    if t["check"] == "audit":
                        status, problems, notes, matched, found = _check_audit(
                            db, _triggers, cur, info.get("jobs"), t)
                    else:
                        status, problems, notes, matched, found = _check_journal(
                            db, _triggers, t)
                except Exception as e:
                    results.append(dict(t, db=db, status="fail",
                                        problems=[f"{type(e).__name__}: {e}"],
                                        notes=[], found=[]))
                    continue
                if t.get("note"):
                    notes = list(notes) + [t["note"]]
                if t["mode"] in TRIGGER_MODES_OPTIONAL and status == "missing":
                    # Режим, которому журнал только помогает: отсутствие
                    # триггера — замечание, а не ошибка.
                    status = "warn"
                    problems, notes = [], list(notes) + [
                        f"триггера нет; режим {t['mode']} работает и без него, "
                        f"но реагирует медленнее"]
                results.append(dict(t, db=db, status=status, problems=problems,
                                    notes=notes,
                                    found=[f["name"] for f in matched] or
                                          [f["name"] for f in found]))
        finally:
            conn.close()
    _spread_dependence(results)
    order = {"error": 0, "missing": 1, "fail": 2, "warn": 3, "ok": 4, "skip": 5}
    results.sort(key=lambda r: (order.get(r["status"], 9), r["key"]))
    return results, db_reports


def _spread_dependence(results):
    """Справочники одной зависимости обновляются вместе — значит, и триггер им
    нужен один.

    Даг справочников берёт из etl_jobs МЕТКУ (`dependence`) и гоняет ВСЕ линии
    с такой меткой. Поэтому если по метке SPACC триггер стоит на spacc, то
    линия SPPAYCONT (её ведущие — sptf/spsmo, а метка та же SPACC) поедет
    вместе с ней, и требовать от неё собственный триггер неправильно."""
    covered = {}
    for r in results:
        if r.get("check") == "audit" and r["status"] in ("ok", "warn"):
            covered.setdefault((r["db"], r["tablename"]),
                               (r["key"], r["table_master"]))
    for r in results:
        if r.get("check") != "audit" or r["status"] != "missing":
            continue
        owner = covered.get((r["db"], r["tablename"]))
        if not owner:
            continue
        key, table = owner
        r["status"] = "ok"
        r["problems"] = []
        r["notes"] = list(r["notes"]) + [
            f"своего триггера нет, но зависимость «{r['tablename']}» помечает "
            f"триггер на {table} (линия {key}) — даг обновит обе линии сразу"]


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

    # 6) режимы, которым триггер нужен. section_compare разделён надвое:
    #    «чистое» сравнение срезов журнал не читает, журнальный вариант — читает.
    assert needs_trigger("iud") and needs_trigger("delete_insert")
    assert needs_trigger("section_compare_with_iud")
    assert not needs_trigger("section_compare")
    assert not needs_trigger("section") and not needs_trigger("query_section")

    # 7) разбор результатов проверки
    body_ok = ("insert into etl_log_iud_row(tablename, timeoper, oper, period, id, "
               "isetl) values ('EINDEXMO', systimestamp, 'IU', :new.createdate, "
               "TO_CHAR(:new.idrw), 0)")
    good = [{"name": "TR_E", "owner": "K", "table": "K.E", "enabled": True,
             "events": "INSERT OR UPDATE OR DELETE", "row_level": True,
             "after": True, "body": body_ok, "func": None, "valid": True,
             "definition": ""}]
    st, pr, no, matched = _evaluate("Orcl", good, "EINDEXMO", ["createdate"],
                                    ["idrw"])
    assert st == "ok", (st, pr, no)
    assert len(matched) == 1
    # чужой регистр tablename — ошибка (сравнение регистрозависимое)
    st2, pr2, _n2, _m2 = _evaluate("Orcl", good, "eindexmo", ["createdate"],
                                   ["idrw"])
    assert st2 == "error" and "регистре" in pr2[0], (st2, pr2)
    # нет триггера с журналом
    st3, pr3, _n3, _m3 = _evaluate("Orcl", [], "EINDEXMO", ["createdate"], ["idrw"])
    assert st3 == "missing", st3
    # отключён и не полный набор событий
    bad = [dict(good[0], enabled=False, events="INSERT", row_level=False,
                valid=False)]
    st4, pr4, _n4, _m4 = _evaluate("Orcl", bad, "EINDEXMO", ["createdate"], ["idrw"])
    assert st4 == "error"
    assert any("ОТКЛЮЧЁН" in p for p in pr4) and any("INVALID" in p for p in pr4)
    assert any("UPDATE, DELETE" in p for p in pr4)
    assert any("FOR EACH ROW" in p for p in pr4)

    # 7a) свой SELECT: за псевдонимами запроса стоят другие колонки ведущей.
    #     Проверка по псевдонимам давала ложное «колонка CREATEDATE не
    #     упоминается» — теперь колонки берутся из самого запроса.
    sql = ("SELECT idrw, UPDT createdate FROM MEDCHECK\n"
           "UNION ALL\n"
           "SELECT id, TO_DATE(YEAR||'-01-01','YYYY-MM-DD') createdate "
           "FROM PODCHECK WHERE typehelp = 4\n"
           "UNION ALL\n"
           "SELECT idrw, dschet FROM TFINSCHET")
    g = guess_line_columns(sql, "TFINSCHET", "createdate", ["idrw"])
    assert g["period"] == "dschet" and g["pk"] == ["idrw"], g
    g2 = guess_line_columns(sql, "PODCHECK", "createdate", ["idrw"])
    # позиция важнее имени: в ветке PODCHECK колонка idrw названа id
    assert g2["pk"] == ["id"], g2
    assert g2["period"] == {"expr": "TO_DATE(YEAR||'-01-01','YYYY-MM-DD')",
                            "columns": ["YEAR"]}, g2
    assert guess_columns(g2) == ["YEAR", "id"], guess_columns(g2)
    d_guess = build_trigger("Orcl", "PODCHECK", "PODCHECK", g2["period"],
                            g2["pk"])["statements"][0]
    assert "TO_DATE(:new.YEAR||'-01-01','YYYY-MM-DD')" in d_guess, d_guess
    assert "TO_CHAR(:new.id)" in d_guess, d_guess
    # чужая таблица запроса в триггер не попадает
    g3 = guess_line_columns("SELECT mc.IDRW idrw, m.updt createdate FROM EXPMED m "
                            "LEFT JOIN MEDCHECK mc ON mc.id = m.id", "EXPMED",
                            "createdate", ["idrw"])
    assert g3["pk"] == [] and "другую таблицу" in " ".join(g3["notes"]), g3
    # таблицы, которой в запросе нет, честно говорим «не по чему»
    g4 = guess_line_columns(sql, "NOSUCH", "createdate", ["idrw"])
    assert not g4["ok"] and "не нашлось ветки" in " ".join(g4["notes"]), g4

    # 7b) аудитный триггер справочника (etl_jobs.isokaudit = 0)
    a_orcl = build_audit_trigger("Orcl", "KOKNAEV.CALENDAR", "CALENDAR")
    assert a_orcl["name"] == "tr_calendar_iud", a_orcl["name"]
    assert "koknaev.etl_jobs" in a_orcl["statements"][0]
    assert "isokaudit = 0" in a_orcl["statements"][0]
    assert "tablename = 'CALENDAR'" in a_orcl["statements"][0]
    a_post = build_audit_trigger("Post", "public.eindexstru", "EINDEXSTRU")
    assert a_post["func"] == AUDIT_FUNC_POST
    assert a_post["statements"][1] == \
        "DROP TRIGGER IF EXISTS tr_eindexstru_iud ON public.eindexstru"
    assert "tr_etl_table_iud_func('EINDEXSTRU')" in a_post["statements"][2]
    audit_body = ("update etl_jobs set isokaudit = 0, last_success_ts = sysdate "
                  "where tablename = 'CALENDAR'")
    a_found = [dict(good[0], name="TR_CALENDAR_IUD", body=audit_body)]
    ast, apr, _an, amt = _evaluate_audit("Orcl", a_found, "CALENDAR", True)
    assert ast == "ok" and len(amt) == 1, (ast, apr)
    # нет строки в etl_jobs — UPDATE ничего не пометит
    ast2, apr2, _n, _m = _evaluate_audit("Orcl", a_found, "CALENDAR", False)
    assert ast2 == "error" and any("нет строки" in p for p in apr2), apr2
    # чужая метка
    ast3, apr3, _n, _m = _evaluate_audit("Orcl", a_found, "SPMKB", True)
    assert ast3 == "error" and any("помечает не" in p for p in apr3), apr3
    # журнальный триггер аудитным не считается
    ast4, apr4, _n, _m = _evaluate_audit("Orcl", good, "EINDEXMO", True)
    assert ast4 == "missing", ast4
    # Postgres: метка живёт в аргументе триггера, а не в теле общей функции
    p_found = [{"name": "tr_e_iud", "owner": "public", "table": "public.e",
                "enabled": True, "events": "INSERT OR UPDATE OR DELETE",
                "row_level": True, "after": False, "valid": True, "func": "f",
                "body": "update etl_user.etl_jobs set isokaudit = 0 where "
                        "tablename = param",
                "definition": "CREATE TRIGGER tr_e_iud BEFORE INSERT OR UPDATE OR "
                              "DELETE ON public.e FOR EACH ROW EXECUTE FUNCTION "
                              "tr_etl_table_iud_func('EINDEXSTRU')"}]
    ast5, apr5, _n, _m = _evaluate_audit("Post", p_found, "EINDEXSTRU", True)
    assert ast5 == "ok", (ast5, apr5)

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
                if (p.get("tname") or "").upper() == "CALENDAR":
                    self.rows = [("KOKNAEV", "TR_CALENDAR_IUD", "KOKNAEV",
                                  "CALENDAR", "ENABLED", "BEFORE EACH ROW",
                                  "INSERT OR UPDATE OR DELETE", audit_body)]
                elif (p.get("tname") or "").upper() == "SPMKB":
                    self.rows = []
                else:
                    self.rows = [("KOKNAEV", "TR_EINDEXMO_AFTER_IUD", "KOKNAEV",
                                  "EINDEXMO", "ENABLED", "AFTER EACH ROW",
                                  "INSERT OR UPDATE OR DELETE", body_ok)]
            elif "count(*)" in low and "etl_jobs" in low:
                # строка зависимости справочника в etl_jobs
                self.rows = [(0,)] if p.get("n") == "SPMKB" else [(1,)]
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
        def _t(**kw):
            base = {"kind": "etl", "check": "iud", "tables": [], "guess": None,
                    "select_sql": None, "period_column": "createdate",
                    "pk_columns": ["idrw"], "needs": True, "note": None,
                    "disabled": False}
            base.update(kw)
            base.setdefault("tables", [base["table_master"]])
            return base

        targets = [
            _t(key="EINDEXMOOrclPost", db_master="Orcl",
               table_master="KOKNAEV.EINDEXMO", tablename="EINDEXMO", mode="iud"),
            _t(key="SECTIONOrclPost", db_master="Orcl", table_master="KOKNAEV.X",
               tablename="X", mode="section", period_column="dcalc",
               check="none", needs=False),
            _t(key="pPostOrcl", db_master="Post", table_master="public.p",
               tablename="p", mode="iud"),
            # справочник: свой вид триггера (etl_jobs.isokaudit = 0)
            _t(key="CALENDAROrclPost", kind="sp:regular", db_master="Orcl",
               table_master="KOKNAEV.CALENDAR", tables=["KOKNAEV.CALENDAR"],
               tablename="CALENDAR", mode="sp", check="audit",
               period_column=None, pk_columns=[]),
            # справочник без триггера и без строки в etl_jobs
            _t(key="SPMKBOrclPost", kind="sp:regular", db_master="Orcl",
               table_master="KOKNAEV.SPMKB", tables=["KOKNAEV.SPMKB"],
               tablename="SPMKB", mode="sp", check="audit",
               period_column=None, pk_columns=[]),
        ]
        res, reps = check_targets(targets)
        by = {r["key"]: r for r in res}
        assert by["EINDEXMOOrclPost"]["status"] == "ok", by["EINDEXMOOrclPost"]
        assert by["EINDEXMOOrclPost"]["found"] == ["TR_EINDEXMO_AFTER_IUD"]
        assert by["SECTIONOrclPost"]["status"] == "skip"
        assert by["pPostOrcl"]["status"] == "ok", by["pPostOrcl"]
        assert by["CALENDAROrclPost"]["status"] == "ok", by["CALENDAROrclPost"]
        assert by["CALENDAROrclPost"]["found"] == ["TR_CALENDAR_IUD"]
        assert by["SPMKBOrclPost"]["status"] == "missing", by["SPMKBOrclPost"]
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

    # 10) реальные линии репозитория: архивные в проверку не попадают, а линия
    #     на своём SELECT приносит с собой разбор запроса.
    keys = {t["key"] for t in trigger_targets()}
    assert "PODCHECK3OrclPost" not in keys, "архивная линия не должна проверяться"
    with_all = {t["key"] for t in trigger_targets(include_disabled=True)}
    assert "PODCHECK3OrclPost" in with_all
    pod = [t for t in trigger_targets() if t["key"] == "PODCHECKOrclPost"]
    if pod:
        period, pk = effective_columns(pod[0])
        assert pk == ["id"], pk        # в MOCHECK.sql idrw линии PODCHECK — это id
    print("trigger_builder selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
