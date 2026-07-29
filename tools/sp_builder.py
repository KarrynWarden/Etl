#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ядро конструктора ETL справочников и разового переноса (без UI).

В отличие от «сложного» ETL (tools/dag_builder.py — свой даг + структуры на
каждую линию), справочники и разовый перенос устроены проще: один общий даг
итерирует записи-фрагменты, а каждая запись описывает лишь
    tableNameSlave + Select.sql (из ведущей) + Add.sql (INSERT в ведомую).
Логика переноса — полная перезаливка (DELETE FROM slave + INSERT всех строк).

Два типа линий (kind):
  • 'regular' — регулярный справочник. Фрагменты в etlFolder/SpTableName.d/,
    их гоняет даг SpEtlNew (dags/SpDagNew.py) по расписанию.
  • 'once'    — разовый перенос. Фрагменты в etlFolder/SpOnce.d/, их гоняет
    даг SpEtlOnce (dags/SpOnce.py) по ручному триггеру, ПАРАЛЛЕЛЬНО по таблицам.

Ключ фрагмента = <МЕТКА_ВЕДУЩЕЙ><DbMaster><DbSlave>, напр. 'SPMKBOrclPost',
'ipersonactOrclPost' — так его разбирают Sp-даги (срез [-8:-4]/[-4:]).

Что делает конструктор:
  1. Либо пользователь называет ведущую таблицу — тогда Select.sql генерится
     автоматически как `SELECT <колонки> FROM <ведущая>` (в порядке маппинга),
     а Add.sql — как `INSERT INTO <ведомая> (<колонки>) VALUES ...`.
  2. Либо пользователь даёт свой SELECT-запрос — тогда его колонки снимаются
     из курсора (snap_query_columns), сопоставляются со столбцами ведомой, и
     генерится только Add.sql (INSERT в ведомую). Select.sql = текст запроса.

Сопоставление колонок — ПОЗИЦИОННОЕ: строки SELECT ложатся в INSERT по порядку.
Регистр имён берётся из БД как есть (Oracle — ВЕРХНИЙ, Postgres — нижний);
авто-сопоставление (dag_builder.auto_match) сверяет имена без учёта регистра.

Плейсхолдеры INSERT зависят от диалекта ведомой (как их ждёт Functions.spEtlNew):
  • ведомая Orcl  -> cursor.executemany(sql, rows):  VALUES (:1, :2, ...)
  • ведомая Post  -> psycopg2.extras.execute_values(sql, rows):  VALUES %s

Отключение таблицы из регулярного справочника — флаг "disabled": true во
фрагменте (даг SpEtlNew её пропускает). Это НЕ удаление: снятие флага возвращает
таблицу в обработку. См. set_sp_disabled / list_disabled_sp_lines.

Самопроверка без БД:  python3 tools/sp_builder.py --selftest
"""
import json
import os
import re

from tools import dag_builder as B  # noqa: E402

ETLFOLDER = B.ETLFOLDER

# kind -> имя конфига/каталога фрагментов (assemble(<name>) читает <name>.d/*.json)
SP_KIND_CONFIG = {"regular": "SpTableName", "once": "SpOnce"}


def _sp_dir(kind):
    return os.path.join(ETLFOLDER, SP_KIND_CONFIG[kind] + ".d")


def sp_key(master_label, db_master, db_slave):
    """Ключ фрагмента: <МЕТКА><DbMaster><DbSlave>. Метка — имя ведущей без схемы
    и без спецсимволов (латиница/цифры/подчёркивание)."""
    label = re.sub(r"[^A-Za-z0-9_]", "", B.bare(str(master_label)))
    return f"{label}{db_master}{db_slave}"


# ─────────────────────────── снятие колонок ───────────────────────────

# Невидимые/типографские символы, которые приезжают вместе с текстом, если SELECT
# копировали из мессенджера, Word, Confluence и т.п. Для СУБД это мусор в теле
# запроса: Oracle отвечает ORA-00911 «invalid character», Postgres — syntax error.
# Глазами в поле ввода они неотличимы от обычного пробела/дефиса/кавычки, поэтому
# ищем их сами и говорим, где именно.
_BAD_CHARS = {
    " ": "неразрывный пробел",
    " ": "неразрывный пробел (figure space)",
    " ": "узкий неразрывный пробел",
    "​": "нулевой пробел",
    "‌": "нулевой несоединитель",
    "﻿": "BOM / неразрывный нулевой пробел",
    "–": "короткое тире (вместо дефиса)",
    "—": "длинное тире (вместо дефиса)",
    "‘": "типографская кавычка ‘",
    "’": "типографская кавычка ’",
    "“": "типографская кавычка “",
    "”": "типографская кавычка ”",
}


def _check_sql_chars(stmt):
    """Найти в тексте запроса символы, которые СУБД не переварит.

    Возвращает None, если всё чисто, иначе — готовое человекочитаемое описание
    с номером строки и позицией (СУБД в таких случаях says только «invalid
    character», не показывая где).
    """
    for lineno, line in enumerate(stmt.splitlines(), 1):
        for pos, ch in enumerate(line, 1):
            name = _BAD_CHARS.get(ch)
            if name is None and ord(ch) < 32 and ch != "\t":
                name = f"управляющий символ U+{ord(ch):04X}"
            if name:
                return (f"строка {lineno}, позиция {pos}: {name} "
                        f"(U+{ord(ch):04X}). Похоже, запрос копировали из "
                        f"документа/мессенджера — перенаберите этот символ "
                        f"вручную.")
    return None


def _strip_terminators(sql):
    """Убрать хвостовые ';' и пробелы (в т.ч. вперемешку: ';\\n;')."""
    stmt = sql.strip()
    while stmt.endswith(";"):
        stmt = stmt[:-1].strip()
    return stmt


def snap_query_columns(db, sql, cred="MAIN"):
    """Снять список колонок, которые вернёт SELECT-запрос (без выборки строк).

    Оборачивает запрос в подзапрос с «нулевым» лимитом и читает cursor.description.
    Возвращает список словарей в том же формате, что snap_structure (внутренние
    ключи в нижнем регистре), но data_type/data_scale/PK пустые — тип из описания
    курсора не нужен для сопоставления по имени. Регистр имён — как отдаёт БД
    (Oracle ВЕРХНИЙ, Postgres нижний).

    ВНИМАНИЕ к алиасу подзапроса: он НЕ должен начинаться с подчёркивания.
    Oracle требует, чтобы неквотированный идентификатор начинался с буквы, и на
    '_sp_probe' отвечал ORA-00911 «invalid character» — то есть снятие колонок
    по своему SELECT не работало на Oracle вообще, независимо от самого запроса.
    'sp_probe' валиден и в Oracle, и в Postgres.
    """
    from Connect import connectPostgres, connectOracle  # noqa: E402

    stmt = _strip_terminators(sql)
    if not stmt:
        raise ValueError("Пустой SELECT-запрос.")
    bad = _check_sql_chars(stmt)
    if bad:
        raise ValueError(f"В тексте запроса недопустимый символ — {bad}")

    if db == "Post":
        probe = f"SELECT * FROM (\n{stmt}\n) AS sp_probe LIMIT 0"
    else:
        probe = f"SELECT * FROM (\n{stmt}\n) sp_probe WHERE ROWNUM < 1"

    conn = connectPostgres(cred) if db == "Post" else connectOracle(cred)
    try:
        cur = conn.cursor()
        cur.execute(probe)
        desc = cur.description or []
    finally:
        conn.close()

    if not desc:
        raise ValueError("Запрос не вернул ни одной колонки (проверь SELECT).")
    return [{"column_name": d[0], "data_type": "", "data_scale": None,
             "is_primary_key": None} for d in desc]


# ─────────────────────────── генерация SQL ───────────────────────────

def build_sp_select(master_table, master_cols):
    """`SELECT c1, c2, ... FROM <ведущая>` — колонки в переданном порядке."""
    if not master_cols:
        raise ValueError("Нет колонок для SELECT.")
    cols = ",\n       ".join(master_cols)
    return f"SELECT {cols}\nFROM {master_table}\n"


def build_sp_add(slave_table, slave_cols, db_slave):
    """INSERT в ведомую. Плейсхолдеры под диалект (см. модульную docstring)."""
    if not slave_cols:
        raise ValueError("Нет колонок для INSERT.")
    cols = ", ".join(slave_cols)
    if db_slave == "Orcl":
        binds = ", ".join(f":{i}" for i in range(1, len(slave_cols) + 1))
        return f"INSERT INTO {slave_table} ({cols})\nVALUES ({binds})\n"
    # Postgres: execute_values подставит строки вместо единственного %s
    return f"INSERT INTO {slave_table} ({cols})\nVALUES %s\n"


def build_sp_all(spec):
    """spec -> (files, key). files: [(относительный_путь, содержимое)].

    Обязательные ключи spec:
      kind ('regular'|'once'), master_table, slave_table, db_master, db_slave,
      pairs (список (master_name, slave_name) — уже в нужном порядке),
      select_mode ('table' | 'custom').
    Для select_mode='custom' нужен select_sql_text (текст запроса), и КАЖДАЯ
    колонка запроса должна быть сопоставлена (позиционная вставка не терпит дыр).
    Необязательные: master_label, sql_dir_name, dependence, disabled, doc,
      struct_dir_name (переопределить путь sql в режиме правки).
    """
    kind = spec.get("kind", "regular")
    if kind not in SP_KIND_CONFIG:
        raise ValueError(f"Неизвестный тип линии: {kind!r}.")
    dbm, dbs = spec["db_master"], spec["db_slave"]
    master_table = str(spec.get("master_table", "")).strip()
    slave_table = str(spec["slave_table"]).strip()
    if not slave_table:
        raise ValueError("Не задана ведомая таблица.")

    master_label = spec.get("master_label") or B.bare(master_table)
    key = sp_key(master_label, dbm, dbs)
    if len(key) <= 8:
        raise ValueError("Слишком короткое имя ведущей для ключа линии.")

    pairs = spec.get("pairs") or []
    select_mode = spec.get("select_mode", "table")

    if select_mode == "custom":
        # своё SELECT: порядок колонок задаёт запрос — сопоставить надо все
        if not pairs or any(s is None for _m, s in pairs):
            raise ValueError(
                "Для своего SELECT сопоставь КАЖДУЮ колонку запроса со столбцом "
                "ведомой — вставка идёт по порядку, пропуски недопустимы."
            )
        s_order = [s for _m, s in pairs]
        select_text = (spec.get("select_sql_text") or "").strip()
        if not select_text:
            raise ValueError("Пустой SELECT-запрос.")
        select_content = select_text + ("" if select_text.endswith("\n") else "\n")
    else:
        if not master_table:
            raise ValueError("Не задана ведущая таблица.")
        m_order = [m for m, s in pairs if m and s]
        s_order = [s for m, s in pairs if m and s]
        if not s_order:
            raise ValueError("Не сопоставлено ни одной колонки.")
        select_content = build_sp_select(master_table, m_order)

    add_content = build_sp_add(slave_table, s_order, dbs)

    sql_name = spec.get("sql_dir_name") or re.sub(r"[^A-Za-z0-9_]", "",
                                                  str(master_label)) or "sp"
    select_rel = f"queries/sp/{sql_name}/Select.sql"
    add_rel = f"queries/sp/{sql_name}/Add.sql"

    body = {"tableNameSlave": slave_table, "addSql": add_rel, "selectSql": select_rel}
    if select_mode == "custom":
        # Режим источника ОБЯЗАН лежать в конфиге: по одному тексту Select.sql
        # его не восстановить. Режим «из таблицы» генерирует ровно такой же
        # `SELECT c1, c2 FROM t`, какой человек пишет руками, когда ему нужен
        # тот же набор колонок, — тексты неотличимы. Без этого флага правка
        # такой линии открывалась как «из таблицы», своё SELECT не показывалось,
        # а «Снять колонки» подтягивало ВСЕ столбцы ведущей, затирая
        # сознательно суженный список.
        body["srcMode"] = "custom"
    if spec.get("dependence"):
        body["dependence"] = str(spec["dependence"]).strip()
    if spec.get("doc"):
        body["_doc"] = spec["doc"]
    if spec.get("disabled"):
        body["disabled"] = True
    # разовый перенос: 'append' — дополнять ведомую без очистки (актуально только
    # для kind='once'; регулярный справочник всегда полная перезаливка)
    if kind == "once" and spec.get("append"):
        body["append"] = True

    fragment = {key: body}
    files = [
        (f"etlFolder/{select_rel}", select_content),
        (f"etlFolder/{add_rel}", add_content),
        (f"etlFolder/{SP_KIND_CONFIG[kind]}.d/{key}.json",
         json.dumps(fragment, ensure_ascii=False, indent=2) + "\n"),
    ]
    return files, key


# ─────────────────────── существующие линии / правка ───────────────────────

def list_sp_lines(kind):
    """Все ключи линий указанного типа (из SpTableName.d / SpOnce.d)."""
    d = _sp_dir(kind)
    keys = []
    if os.path.isdir(d):
        for f in sorted(os.listdir(d)):
            if not f.endswith(".json"):
                continue
            try:
                obj = B._read_json(os.path.join(d, f))
            except Exception:
                continue
            keys.extend(obj.keys())
    return sorted(keys)


def _find_sp_fragment(kind, key):
    """(body, path, obj) фрагмента линии в её каталоге."""
    d = _sp_dir(kind)
    for f in sorted(os.listdir(d)):
        if not f.endswith(".json"):
            continue
        path = os.path.join(d, f)
        try:
            obj = B._read_json(path)
        except Exception:
            continue
        if key in obj:
            return obj[key], path, obj
    raise KeyError(f"Линия '{key}' не найдена в {SP_KIND_CONFIG[kind]}.d.")


def list_active_sp_lines(kind):
    """Линии, которые НЕ отключены (участвуют в регулярном переносе)."""
    return [k for k in list_sp_lines(kind)
            if not _find_sp_fragment(kind, k)[0].get("disabled")]


def list_disabled_sp_lines(kind):
    """Отключённые линии (disabled=true) — их даг пропускает."""
    return [k for k in list_sp_lines(kind)
            if _find_sp_fragment(kind, k)[0].get("disabled")]


def set_sp_disabled(kind, key, value):
    """Проставить (True) или снять (False) флаг disabled у линии.

    Отключение = таблица временно НЕ переносится регулярным дагом справочников,
    но конфиг и SQL остаются на месте — включить обратно можно в любой момент.
    """
    _body, path, obj = _find_sp_fragment(kind, key)
    if value:
        obj[key]["disabled"] = True
    else:
        obj[key].pop("disabled", None)
    with B._group_writable():
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(obj, fp, ensure_ascii=False, indent=2)
            fp.write("\n")
    return obj[key].get("disabled", False)


def move_sp_line(key, from_kind, to_kind):
    """Перевести линию между типами (разовый <-> регулярный) без пересборки.

    Частый сценарий: справочник держали в разовом переносе (once) ради данных для
    разработки, а потом переводят на регулярный (regular). Формат фрагмента и пути
    к SQL одинаковы, поэтому перенос — это просто перемещение фрагмента из
    <from>.d/ в <to>.d/; SQL-файлы (queries/sp/...) остаются на месте. Возвращает
    путь нового фрагмента.
    """
    if from_kind == to_kind:
        raise ValueError("Исходный и целевой типы совпадают.")
    if from_kind not in SP_KIND_CONFIG or to_kind not in SP_KIND_CONFIG:
        raise ValueError("Неизвестный тип линии.")
    body, from_path, from_obj = _find_sp_fragment(from_kind, key)
    if key in list_sp_lines(to_kind):
        raise ValueError(
            f"Линия '{key}' уже есть среди «{SP_KIND_CONFIG[to_kind]}» — "
            "перенос создал бы дубликат ключа.")
    to_path = os.path.join(_sp_dir(to_kind), f"{key}.json")
    with B._group_writable():
        os.makedirs(_sp_dir(to_kind), exist_ok=True)
        with open(to_path, "w", encoding="utf-8") as fp:
            json.dump({key: body}, fp, ensure_ascii=False, indent=2)
            fp.write("\n")
        # убрать ключ из исходного каталога (файл целиком — если в нём только он)
        if len(from_obj) <= 1:
            os.remove(from_path)
        else:
            from_obj.pop(key, None)
            with open(from_path, "w", encoding="utf-8") as fp:
                json.dump(from_obj, fp, ensure_ascii=False, indent=2)
                fp.write("\n")
    return to_path


def _all_sql_dirs(exclude=None):
    """Множество каталогов queries/sp/<...>, на которые ссылаются ДРУГИЕ линии
    (обоих типов). exclude — (kind, key) исключить из подсчёта. Нужно, чтобы при
    удалении линии не снести SQL, который делит с ней другая линия."""
    dirs = set()
    for kind in SP_KIND_CONFIG:
        d = _sp_dir(kind)
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if not f.endswith(".json"):
                continue
            try:
                obj = B._read_json(os.path.join(d, f))
            except Exception:
                continue
            for k, body in obj.items():
                if exclude and exclude == (kind, k):
                    continue
                for rel in (body.get("selectSql"), body.get("addSql")):
                    if rel:
                        dirs.add(os.path.dirname(os.path.abspath(
                            os.path.join(ETLFOLDER, rel))))
    return dirs


def sp_line_targets(kind, key):
    """Что физически будет удалено при удалении линии — для предпросмотра в UI.
    Возвращает (fragment_path_или_ключ, sql_dir_или_None, sql_dir_shared: bool)."""
    body, path, obj = _find_sp_fragment(kind, key)
    sql_dir = None
    for rel in (body.get("selectSql"), body.get("addSql")):
        if rel:
            sql_dir = os.path.dirname(os.path.abspath(os.path.join(ETLFOLDER, rel)))
    shared = bool(sql_dir and sql_dir in _all_sql_dirs(exclude=(kind, key)))
    frag_desc = path if len(obj) <= 1 else f"{path} (ключ {key})"
    return frag_desc, sql_dir, shared


def delete_sp_line(kind, key, remove_sql=True):
    """Удалить линию справочника/разового переноса НАСОВСЕМ.

    Удаляет фрагмент (файл целиком, если в нём один ключ; иначе только ключ) и,
    если remove_sql, каталог его SQL (queries/sp/<...>) — но лишь когда он не
    используется другой линией и лежит строго под queries/sp (страховка).
    Возвращает список удалённых путей.
    """
    body, path, obj = _find_sp_fragment(kind, key)
    removed = []
    sql_dir = None
    for rel in (body.get("selectSql"), body.get("addSql")):
        if rel:
            sql_dir = os.path.dirname(os.path.abspath(os.path.join(ETLFOLDER, rel)))
    shared = bool(sql_dir and sql_dir in _all_sql_dirs(exclude=(kind, key)))
    sp_root = os.path.abspath(os.path.join(ETLFOLDER, "queries", "sp")) + os.sep

    with B._group_writable():
        if len(obj) <= 1:
            os.remove(path)
            removed.append(path)
        else:
            obj.pop(key, None)
            with open(path, "w", encoding="utf-8") as fp:
                json.dump(obj, fp, ensure_ascii=False, indent=2)
                fp.write("\n")
            removed.append(f"{path} (ключ {key})")
        if (remove_sql and sql_dir and not shared and os.path.isdir(sql_dir)
                and (sql_dir + os.sep).startswith(sp_root)):
            import shutil
            shutil.rmtree(sql_dir)
            removed.append(sql_dir)
    return removed


def load_sp_line(kind, key):
    """Собрать спецификацию существующей линии для формы (режим правки)."""
    body, _path, _obj = _find_sp_fragment(kind, key)
    line, dbm, dbs = B.split_key(key)

    def _text(rel):
        if not rel:
            return ""
        try:
            return B._read_text(os.path.join(ETLFOLDER, rel))
        except OSError:
            return ""

    select_rel = body.get("selectSql")
    select_text = _text(select_rel)
    add_text = _text(body.get("addSql"))
    # если Select.sql — это простой `SELECT ... FROM <master>`, режим 'table'
    master_from = _master_from_select(select_text)
    # Режим источника: сохранённый флаг — источник истины. У фрагментов,
    # созданных до его появления, флага нет — там остаётся прежняя эвристика
    # «простой SELECT ... FROM t => из таблицы» (она может ошибиться на своём
    # запросе без WHERE; такую линию достаточно один раз пересохранить).
    src_mode = body.get("srcMode") or ("table" if master_from else "custom")
    # текущее сопоставление колонок — прямо из сохранённых SQL (без обращения к БД)
    master_cols, slave_cols, pairs = restore_mapping(select_text, add_text)
    return {
        "key": key, "kind": kind,
        "master_label": line, "db_master": dbm, "db_slave": dbs,
        "master_table": master_from or line,
        "slave_table": body.get("tableNameSlave", ""),
        "dependence": body.get("dependence", ""),
        "disabled": bool(body.get("disabled")),
        "append": bool(body.get("append")),
        "doc": body.get("_doc", ""),
        "src_mode": src_mode,
        "select_sql": select_rel, "select_sql_text": select_text,
        "add_sql": body.get("addSql"), "add_sql_text": add_text,
        "master_cols": master_cols, "slave_cols": slave_cols, "pairs": pairs,
        "sql_dir_name": (os.path.basename(os.path.dirname(select_rel))
                         if select_rel else None),
    }


_SELECT_FROM_RE = re.compile(
    r"^\s*SELECT\b.*?\bFROM\s+([A-Za-z0-9_.\"]+)\s*$", re.S | re.I)


def _master_from_select(text):
    """Достать имя ведущей из простого `SELECT ... FROM <table>` (или None,
    если запрос сложнее — тогда это «своё SELECT»)."""
    if not text:
        return None
    m = _SELECT_FROM_RE.match(text.strip())
    return m.group(1) if m else None


def _split_top_level(s):
    """Разбить список через запятую по верхнему уровню (запятые внутри скобок
    не считаются разделителями): 'a, f(x, y), b' -> ['a', 'f(x, y)', 'b']."""
    parts, depth, cur = [], 0, ""
    for ch in s:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return [p for p in parts if p]


_SELECT_LIST_RE = re.compile(r"\bSELECT\b(.*?)\bFROM\b", re.S | re.I)
_INSERT_COLS_RE = re.compile(
    r"\bINSERT\s+INTO\s+([A-Za-z0-9_.\"]+)\s*\(([^)]*)\)\s*VALUES", re.S | re.I)


def parse_select_columns(text):
    """Список колонок из SELECT-списка простого `SELECT c1, c2, ... FROM ...`.

    Для сгенерированного запроса «из таблицы» вернёт чистые имена колонок. Для
    своего SELECT — выражения как есть (напр. 'a.ID'); если распарсить нельзя
    (нет FROM, '*' и т.п.) — пустой список."""
    if not text:
        return []
    m = _SELECT_LIST_RE.search(text)
    if not m:
        return []
    cols = _split_top_level(m.group(1))
    if any(c == "*" or c.endswith(".*") for c in cols):
        return []
    return cols


def parse_insert_columns(text):
    """(таблица, [колонки]) из `INSERT INTO t (c1, c2, ...) VALUES ...`.
    Если распарсить нельзя — (None, [])."""
    if not text:
        return None, []
    m = _INSERT_COLS_RE.search(text)
    if not m:
        return None, []
    table = m.group(1)
    cols = [c for c in _split_top_level(m.group(2))]
    return table, cols


def _cols_as_dicts(names):
    return [{"column_name": n, "data_type": "", "data_scale": None,
             "is_primary_key": None} for n in names]


def restore_mapping(select_text, add_text):
    """Восстановить (master_cols, slave_cols, pairs) из сохранённых SQL — без БД.

    Позиционное сопоставление: SELECT-колонка i <-> INSERT-колонка i. Если число
    колонок в SELECT и INSERT не совпало (или SELECT не распарсился), метки
    ведущей заменяются заглушками «(колонка N)», а порядок берётся из INSERT —
    так текущее сопоставление всё равно видно, пусть и без имён из ведущей.
    Возвращает пустые списки, если INSERT распарсить не удалось.
    """
    _tbl, ins_cols = parse_insert_columns(add_text)
    if not ins_cols:
        return [], [], []
    sel_cols = parse_select_columns(select_text)
    if len(sel_cols) != len(ins_cols):
        sel_cols = [f"(колонка {i + 1})" for i in range(len(ins_cols))]
    pairs = list(zip(sel_cols, ins_cols))
    return _cols_as_dicts(sel_cols), _cols_as_dicts(ins_cols), pairs


# ─────────────────────────── самопроверка (без БД) ───────────────────────────

def _selftest():
    master_cols = [
        {"column_name": "ID", "data_type": "NUMBER", "data_scale": 0, "is_primary_key": "Primary Key"},
        {"column_name": "NAME", "data_type": "VARCHAR2", "data_scale": None, "is_primary_key": None},
        {"column_name": "CODE", "data_type": "VARCHAR2", "data_scale": None, "is_primary_key": None},
    ]
    slave_cols = [
        {"column_name": "id", "data_type": "numeric", "data_scale": 0, "is_primary_key": "Primary Key"},
        {"column_name": "name", "data_type": "text", "data_scale": None, "is_primary_key": None},
        {"column_name": "code", "data_type": "text", "data_scale": None, "is_primary_key": None},
    ]
    sugg, unmatched = B.auto_match(master_cols, slave_cols)
    assert sugg == ["id", "name", "code"], sugg          # регистр не мешает
    assert unmatched == [], unmatched

    pairs = list(zip([c["column_name"] for c in master_cols], sugg))
    # 1) режим «по таблице»: справочник Orcl -> Post
    spec = {
        "kind": "regular", "master_table": "KOKNAEV.SPMKB", "slave_table": "spmkb",
        "db_master": "Orcl", "db_slave": "Post", "select_mode": "table", "pairs": pairs,
    }
    files, key = build_sp_all(spec)
    assert key == "SPMKBOrclPost", key
    d = dict(files)
    assert d["etlFolder/queries/sp/SPMKB/Select.sql"].startswith("SELECT ID,"), \
        d["etlFolder/queries/sp/SPMKB/Select.sql"]
    assert "FROM KOKNAEV.SPMKB" in d["etlFolder/queries/sp/SPMKB/Select.sql"]
    add = d["etlFolder/queries/sp/SPMKB/Add.sql"]
    assert add == "INSERT INTO spmkb (id, name, code)\nVALUES %s\n", add   # Post -> %s
    frag = json.loads(d["etlFolder/SpTableName.d/SPMKBOrclPost.json"])
    assert frag["SPMKBOrclPost"]["tableNameSlave"] == "spmkb"
    assert frag["SPMKBOrclPost"]["selectSql"] == "queries/sp/SPMKB/Select.sql"

    # 2) ведомая Oracle -> плейсхолдеры :1,:2,:3
    spec2 = dict(spec, master_table="spacc", slave_table="KOKNAEV.spacc",
                 db_master="Post", db_slave="Orcl", master_label="SPACC")
    files2, key2 = build_sp_all(spec2)
    assert key2 == "SPACCPostOrcl", key2
    add2 = dict(files2)["etlFolder/queries/sp/SPACC/Add.sql"]
    assert add2 == "INSERT INTO KOKNAEV.spacc (id, name, code)\nVALUES (:1, :2, :3)\n", add2

    # 3) свой SELECT: все колонки должны быть сопоставлены
    spec3 = {
        "kind": "once", "master_table": "", "slave_table": "ipersonact",
        "db_master": "Orcl", "db_slave": "Post", "master_label": "ipersonact",
        "select_mode": "custom",
        "select_sql_text": "SELECT a.ID, a.NAME FROM KOKNAEV.IPERSON a WHERE a.ACT = 1",
        "pairs": [("ID", "id"), ("NAME", "name")],
    }
    files3, key3 = build_sp_all(spec3)
    assert key3 == "ipersonactOrclPost", key3
    d3 = dict(files3)
    assert "etlFolder/SpOnce.d/ipersonactOrclPost.json" in d3
    assert d3["etlFolder/queries/sp/ipersonact/Select.sql"].startswith("SELECT a.ID")
    # пропуск сопоставления в custom -> ошибка
    try:
        build_sp_all(dict(spec3, pairs=[("ID", "id"), ("NAME", None)]))
        raise AssertionError("ожидалась ошибка о неполном сопоставлении")
    except ValueError:
        pass

    # 4) разбор ведущей из простого SELECT
    assert _master_from_select("SELECT ID, NAME\nFROM KOKNAEV.SPMKB\n") == "KOKNAEV.SPMKB"
    assert _master_from_select("SELECT a.ID FROM T a WHERE a.x=1") is None

    # 5) парсинг колонок из сохранённых SQL (режим правки без БД)
    assert parse_select_columns("SELECT ID,\n       NAME,\n       CODE\nFROM T\n") == \
        ["ID", "NAME", "CODE"]
    assert parse_insert_columns("INSERT INTO spmkb (id, name, code)\nVALUES %s\n") == \
        ("spmkb", ["id", "name", "code"])
    assert parse_insert_columns("INSERT INTO KOKNAEV.spacc (id, name)\nVALUES (:1, :2)\n") == \
        ("KOKNAEV.spacc", ["id", "name"])
    # верхнеуровневые запятые (функции с запятыми внутри не рвутся)
    assert parse_select_columns("SELECT a.id, coalesce(x, 0) v FROM t") == \
        ["a.id", "coalesce(x, 0) v"]
    # восстановление сопоставления: SELECT[i] <-> INSERT[i]
    mc, sc, pr = restore_mapping("SELECT ID, NAME, CODE\nFROM T\n",
                                 "INSERT INTO spmkb (id, name, code)\nVALUES %s\n")
    assert [c["column_name"] for c in mc] == ["ID", "NAME", "CODE"]
    assert [c["column_name"] for c in sc] == ["id", "name", "code"]
    assert pr == [("ID", "id"), ("NAME", "name"), ("CODE", "code")]
    # рассинхрон числа колонок -> метки-заглушки, порядок из INSERT
    mc2, sc2, pr2 = restore_mapping("SELECT * FROM T",
                                    "INSERT INTO t (a, b)\nVALUES (:1, :2)\n")
    assert [c["column_name"] for c in sc2] == ["a", "b"]
    assert [c["column_name"] for c in mc2] == ["(колонка 1)", "(колонка 2)"]
    # нераспарсиваемый INSERT -> пусто
    assert restore_mapping("", "") == ([], [], [])

    # 6) перевод линии между типами (в изолированном каталоге)
    import tempfile
    global ETLFOLDER
    _saved = ETLFOLDER
    try:
        ETLFOLDER = tempfile.mkdtemp()
        os.makedirs(os.path.join(ETLFOLDER, "SpOnce.d"))
        _frag = {"FOOOrclPost": {"tableNameSlave": "foo",
                                 "addSql": "queries/sp/FOO/Add.sql",
                                 "selectSql": "queries/sp/FOO/Select.sql"}}
        with open(os.path.join(ETLFOLDER, "SpOnce.d", "FOOOrclPost.json"), "w") as fp:
            json.dump(_frag, fp)
        move_sp_line("FOOOrclPost", "once", "regular")
        assert list_sp_lines("once") == [] and list_sp_lines("regular") == ["FOOOrclPost"]
        move_sp_line("FOOOrclPost", "regular", "once")
        assert list_sp_lines("regular") == [] and list_sp_lines("once") == ["FOOOrclPost"]

        # удаление: фрагмент + SQL-каталог (лежащий под queries/sp) сносятся
        os.makedirs(os.path.join(ETLFOLDER, "queries", "sp", "FOO"))
        for fn in ("Select.sql", "Add.sql"):
            with open(os.path.join(ETLFOLDER, "queries", "sp", "FOO", fn), "w") as fp:
                fp.write("x")
        delete_sp_line("once", "FOOOrclPost")
        assert list_sp_lines("once") == []
        assert not os.path.isdir(os.path.join(ETLFOLDER, "queries", "sp", "FOO"))
    finally:
        ETLFOLDER = _saved

    # 7) флаг append только для разового (kind=once)
    _p = [("A", "a"), ("B", "b")]
    f_once, _ = build_sp_all({"kind": "once", "master_table": "", "slave_table": "t",
                              "db_master": "Orcl", "db_slave": "Post", "master_label": "T",
                              "select_mode": "custom", "select_sql_text": "SELECT A,B FROM X",
                              "pairs": _p, "append": True})
    assert json.loads(dict(f_once)["etlFolder/SpOnce.d/TOrclPost.json"])["TOrclPost"]["append"] is True
    f_reg, _ = build_sp_all({"kind": "regular", "master_table": "M", "slave_table": "t",
                             "db_master": "Orcl", "db_slave": "Post", "master_label": "T",
                             "select_mode": "table", "pairs": _p, "append": True})
    assert "append" not in json.loads(dict(f_reg)["etlFolder/SpTableName.d/TOrclPost.json"])["TOrclPost"]
    print("sp_builder selftest OK")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
