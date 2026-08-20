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
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:                 # чтобы работал прямой запуск --selftest
    sys.path.insert(0, ROOT)

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
# Снятие колонок по своему SELECT (и проверка текста запроса на типографский
# мусор) живёт в ядре сложного ETL — оно нужно обоим конструкторам, а для
# сложной линии ещё и с типами (snap_query_structure). Имена оставлены
# прежними: на них ссылаются старые вызовы и самопроверки.
snap_query_columns = B.snap_query_columns
_check_sql_chars = B._check_sql_chars
_strip_terminators = B._strip_terminators


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
    # Имя ключа принимаем оба. load_sp_line отдаёт режим источника как
    # `src_mode` (так он назван и в конфиге — srcMode), а здесь исторически
    # читался `select_mode`, который кладёт форма создания. Пока обе стороны
    # заполняла одна и та же форма, это не всплывало; стоило передать сюда
    # спецификацию, прочитанную load_sp_line, — и линия со своим SELECT молча
    # пересобиралась как «из таблицы»: рукописный запрос затирался
    # сгенерированным `SELECT c1, c2 FROM t`, а srcMode пропадал из конфига.
    # То есть правка ЛЮБОГО поля такой линии стирала её запрос.
    select_mode = spec.get("select_mode") or spec.get("src_mode") or "table"

    if select_mode == "all":
        # Источник — ВСЯ ведущая таблица, своего Select.sql у линии нет.
        # Это законная настройка, а не недоделка: без ключа selectSql рантайм
        # подставляет `SELECT * FROM <ведущая>` (dags/SpDagNew.py, dags/SpOnce.py).
        # Так заведён CALENDAR — справочник в одну колонку, ради которого
        # отдельный файл запроса не нужен.
        # Конструктор этого режима не знал и на пересборке ВЫДУМЫВАЛ Select.sql
        # с заглушками «(колонка N)» вместо имён, попутно дописывая selectSql
        # в конфиг, — то есть ломал рабочую линию.
        s_order = [s for _m, s in pairs if s]
        if not s_order:
            raise ValueError("Не сопоставлено ни одной колонки ведомой.")
        select_content = None
    elif select_mode == "custom":
        # своё SELECT: порядок колонок задаёт запрос — сопоставить надо все.
        #
        # Проверка стоит ЗДЕСЬ, на пути записи, а не отдельным предупреждением
        # на вкладке колонок: висящее предупреждение читается как требование
        # переписать запрос, а требование ровно одно и совсем другое — рантайм
        # кладёт строку выборки в INSERT без проекции, так что колонок в SELECT
        # должно быть столько же, сколько в INSERT. Отсюда и два выхода, оба
        # законные: дать колонке пару — или убрать её (кнопкой в строке; из
        # запроса она вырежется точечно, remove_select_column).
        if not pairs or any(s is None for _m, s in pairs):
            orphans = [str(m) for m, s in pairs if s is None] if pairs else []
            raise ValueError(
                "Для своего SELECT сопоставь КАЖДУЮ колонку запроса со столбцом "
                "ведомой — вставка идёт по порядку, пропуски недопустимы"
                + (f". Без пары: {', '.join(orphans)}" if orphans else "")
                + ". Ненужную колонку можно убрать кнопкой в её строке — "
                  "из запроса она уйдёт вместе с парой."
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

    body = {"tableNameSlave": slave_table, "addSql": add_rel}
    if select_content is not None:
        body["selectSql"] = select_rel
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
    files = []
    if select_content is not None:
        files.append((f"etlFolder/{select_rel}", select_content))
    files += [
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
    # Нет ключа selectSql вовсе — это режим «вся таблица» (см. build_sp_all).
    # Проверять его надо ПЕРВЫМ: у такой линии нет текста, по которому
    # эвристика ниже могла бы что-то определить, и она давала «custom».
    if not select_rel:
        src_mode = "all"
    else:
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
        # Каталог SQL берём из ЛЮБОГО из двух путей — они всегда лежат рядом.
        # Раньше смотрели только на selectSql, и у линии, где его нет (в
        # конфиге один addSql — так заведён CALENDAR), имя каталога терялось.
        # Пересборка такой линии уводила файлы в каталог по МЕТКЕ линии
        # (queries/sp/CALENDAR вместо queries/sp/calendar) — то есть создавала
        # второй комплект рядом, а рабочий оставляла сиротой.
        "sql_dir_name": (os.path.basename(os.path.dirname(
            select_rel or body.get("addSql") or "")) or None),
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


def _split_top_level_spans(s):
    """То же, что _split_top_level, но с границами каждого куска в исходной
    строке: [(начало, конец), ...]. Нужно, чтобы править ОДИН элемент списка
    точечно, не пересобирая и не переформатируя остальные."""
    spans, depth, start = [], 0, 0
    for i, ch in enumerate(s):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            spans.append((start, i))
            start = i + 1
    spans.append((start, len(s)))
    # обрезаем пробелы по краям, пустые куски выбрасываем
    out = []
    for a, b in spans:
        piece = s[a:b]
        left = len(piece) - len(piece.lstrip())
        right = len(piece) - len(piece.rstrip())
        if piece.strip():
            out.append((a + left, b - right))
    return out


# Хвостовой псевдоним: «… kod» или «… AS kod». Имя — обычный идентификатор либо
# в кавычках (в кавычках регистр значим, поэтому их сохраняем как есть).
_ALIAS_TAIL_RE = re.compile(
    r"(?:\s+AS)?\s+(\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_$#]*)\s*$", re.I)
_BARE_NAME_RE = re.compile(r"^(?:\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_$#.]*)$")
# слова, которые псевдонимом не бывают: иначе «CAST(x AS int)» или «… END»
# приняли бы за имя выходной колонки
_NOT_ALIAS = {"end", "null", "distinct", "all", "unique"}


def rename_select_column(text, index, new_name):
    """Переименовать ВЫХОДНУЮ колонку №index (с нуля) в SELECT-списке.

    Одно правило на все случаи, поэтому «правка колонки» значит одно и то же и
    для сгенерированного запроса, и для написанного руками:

      code                 -> kod              голое имя заменяется целиком
                                               (в запросе «из таблицы» это и
                                               есть выбор другой колонки);
      m.code kod           -> m.code naim      меняется ТОЛЬКО псевдоним,
      m.code AS kod        -> m.code AS naim   выражение остаётся вашим;
      TRUNC(dt)            -> TRUNC(dt) naim   псевдонима не было — добавляется.

    Возвращает новый текст запроса. Если разобрать список не удалось (нет FROM,
    '*', индекс за пределами) — возвращает текст БЕЗ ИЗМЕНЕНИЙ: молча испортить
    чужой запрос хуже, чем не переименовать.
    """
    new_name = (new_name or "").strip()
    if not text or not new_name:
        return text
    m = _SELECT_LIST_RE.search(text)
    if not m:
        return text
    body = m.group(1)
    spans = _split_top_level_spans(body)
    if not (0 <= index < len(spans)):
        return text
    a, b = spans[index]
    item = body[a:b]
    if item == "*" or item.endswith(".*"):
        return text

    if _BARE_NAME_RE.match(item):
        replacement = new_name                      # голое имя — целиком
    else:
        tail = _ALIAS_TAIL_RE.search(item)
        if tail and tail.group(1).strip('"').lower() not in _NOT_ALIAS:
            replacement = item[:tail.start(1)] + new_name
        else:
            replacement = f"{item} {new_name}"      # псевдонима не было
    return text[:m.start(1) + a] + replacement + text[m.start(1) + b:]


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


def remove_select_column(text, index):
    """Убрать выходную колонку №index (с нуля) из SELECT-списка.

    Почему это приходится делать, даже когда запрос «заперт». Рантайм
    справочника кладёт строки из SELECT в INSERT БЕЗ ПРОЕКЦИИ:

        rows = cursor.fetchmany(...)          # весь кортеж как есть
        cursor2.executemany(addAllSpSql, rows)

    Значит число колонок в SELECT обязано совпадать с числом в INSERT.
    Оставить в запросе лишнюю колонку «просто так» нельзя: Oracle ответит
    «not all variables bound», Postgres — «INSERT has more expressions than
    target columns», и так каждый прогон.

    Поэтому убранная колонка уходит и из запроса — но ТОЧЕЧНО: вырезается один
    элемент списка, остальной текст (выражения, переносы, WHERE) остаётся байт
    в байт. Разобрать не вышло — текст возвращается как есть.
    """
    if not text:
        return text
    m = _SELECT_LIST_RE.search(text)
    if not m:
        return text
    body = m.group(1)
    spans = _split_top_level_spans(body)
    if not (0 <= index < len(spans)) or len(spans) < 2:
        return text          # последнюю колонку не убираем: SELECT без списка
    if any(body[a:b] == "*" or body[a:b].endswith(".*") for a, b in spans):
        return text

    a, b = spans[index]
    # Съедаем и разделитель — тот, что со стороны соседа, чтобы не осталось
    # висящей запятой и чтобы отступ следующей строки не поехал.
    if index + 1 < len(spans):
        cut_a, cut_b = a, spans[index + 1][0]
    else:
        cut_a, cut_b = spans[index - 1][1], b
    return text[:m.start(1) + cut_a] + text[m.start(1) + cut_b:]


def output_name(item):
    """Имя ВЫХОДНОЙ колонки для элемента SELECT-списка.

    'code' -> 'code';  'm.code kod' -> 'kod';  'm.code AS kod' -> 'kod';
    'TRUNC(dt)' -> 'TRUNC(dt)' (имени нет — показываем выражение).

    Нужно, чтобы колонки назывались одинаково, откуда бы ни пришли: снятие по
    запросу берёт имена у драйвера (то есть псевдонимы), а чтение с диска
    разбирает текст. Без этого одна и та же колонка звалась то 'kod', то
    'm.code kod' — в зависимости от того, открыли линию заново или только что
    сняли структуру.
    """
    item = (item or "").strip()
    if not item or _BARE_NAME_RE.match(item):
        return item
    tail = _ALIAS_TAIL_RE.search(item)
    if tail and tail.group(1).strip('"').lower() not in _NOT_ALIAS:
        return tail.group(1)
    return item


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
    # Имена ВЫХОДНЫХ колонок, а не выражения целиком: см. output_name.
    sel_cols = [output_name(c) for c in parse_select_columns(select_text)]
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
    # ── правка колонки = правка запроса, одним правилом ──────────────────────
    # Режим правки ОДИН: «поменял колонку в списке — поменялась колонка
    # запроса» должно значить одно и то же и для сгенерированного запроса, и
    # для написанного руками. Разница только в том, ЧТО именно меняется в
    # тексте, и решает это сам текст, а не «режим линии».
    for sql, idx, name, want, note in (
        ("SELECT code, name FROM t\n", 0, "kod",
         "SELECT kod, name FROM t\n", "голое имя заменяется целиком"),
        ("SELECT m.code kod, b FROM t m\n", 0, "naim",
         "SELECT m.code naim, b FROM t m\n", "меняется только псевдоним"),
        ("SELECT m.code AS kod, b FROM t m\n", 0, "naim",
         "SELECT m.code AS naim, b FROM t m\n", "форма AS сохраняется"),
        ("SELECT TRUNC(dt), b FROM t\n", 0, "dat",
         "SELECT TRUNC(dt) dat, b FROM t\n", "псевдонима не было — добавляется"),
        ("SELECT f(a, b) x, c FROM t\n", 0, "y",
         "SELECT f(a, b) y, c FROM t\n", "запятая в скобках не сбивает разбор"),
        ("SELECT CASE WHEN a=1 THEN 2 ELSE 3 END, c FROM t\n", 0, "flag",
         "SELECT CASE WHEN a=1 THEN 2 ELSE 3 END flag, c FROM t\n",
         "END не принят за псевдоним"),
        # разобрать нельзя — текст возвращается КАК ЕСТЬ: молча испортить чужой
        # запрос хуже, чем не переименовать
        ("SELECT * FROM t\n", 0, "x", "SELECT * FROM t\n", "звёздочка не трогается"),
        ("SELECT a, b FROM t\n", 9, "x", "SELECT a, b FROM t\n", "индекс за пределами"),
        ("не запрос вовсе", 0, "x", "не запрос вовсе", "не разобралось"),
    ):
        got = rename_select_column(sql, idx, name)
        assert got == want, (note, got)

    # имя ВЫХОДНОЙ колонки — одно и то же, откуда бы колонки ни пришли:
    # снятие по запросу берёт имена у драйвера (псевдонимы), чтение с диска
    # разбирает текст. Без output_name одна колонка звалась то 'kod', то
    # 'm.code kod' — смотря открыли линию заново или только что сняли структуру
    for item, want in (("code", "code"), ("m.code kod", "kod"),
                       ("m.code AS kod", "kod"), ("TRUNC(dt)", "TRUNC(dt)"),
                       ('m."Mixed" mc', "mc"), ("CASE WHEN a=1 THEN 2 END",
                                                "CASE WHEN a=1 THEN 2 END")):
        assert output_name(item) == want, (item, output_name(item))

    # круг: переименовал -> разобрал -> имя стало новым, выражение цело
    sel = "SELECT m.code kod,\n       TRUNC(m.dt)\nFROM t m\nWHERE m.dend IS NULL\n"
    add = "INSERT INTO t (kod, dat)\nVALUES %s\n"
    sel = rename_select_column(sel, 1, "data_nach")
    _m, _s, pairs = restore_mapping(sel, add)
    assert [p[0] for p in pairs] == ["kod", "data_nach"], pairs
    assert "TRUNC(m.dt)" in sel and "WHERE m.dend IS NULL" in sel, sel

    # ── убранная колонка уходит и из запроса ────────────────────────────────
    # Не «на всякий случай»: рантайм справочника отдаёт строки выборки в INSERT
    # без проекции, поэтому лишняя колонка в SELECT — ошибка привязки на каждом
    # прогоне, а не безобидное лишнее поле. Вырезается ровно один элемент,
    # остальной текст обязан остаться байт в байт — иначе «убрал одну колонку»
    # показывалось бы в предпросмотре переписанным запросом.
    for sql, idx, want, note in (
        ("SELECT a, b, c FROM t\n", 1, "SELECT a, c FROM t\n", "середина списка"),
        ("SELECT a, b, c FROM t\n", 0, "SELECT b, c FROM t\n", "первая"),
        ("SELECT a, b, c FROM t\n", 2, "SELECT a, b FROM t\n", "последняя"),
        ("SELECT f(a, b) x, c FROM t\n", 0, "SELECT c FROM t\n",
         "запятая в скобках не сбивает разбор"),
        ("SELECT m.code kod,\n       m.name AS naim,\n       TRUNC(m.dt) dat\n"
         "FROM t m\nWHERE m.dend IS NULL\n", 1,
         "SELECT m.code kod,\n       TRUNC(m.dt) dat\n"
         "FROM t m\nWHERE m.dend IS NULL\n", "раскладка и WHERE целы"),
        # разобрать нельзя — текст как есть
        ("SELECT * FROM t\n", 0, "SELECT * FROM t\n", "звёздочка не трогается"),
        ("SELECT a FROM t\n", 0, "SELECT a FROM t\n", "последнюю колонку не убираем"),
        ("SELECT a, b FROM t\n", 9, "SELECT a, b FROM t\n", "индекс за пределами"),
        ("не запрос вовсе", 0, "не запрос вовсе", "не разобралось"),
    ):
        got = remove_select_column(sql, idx)
        assert got == want, (note, got)

    # круг: убрал колонку -> разобрал -> её нет ни в парах, ни в запросе,
    # а число колонок SELECT и INSERT сошлось (иначе рантайм упадёт)
    sel = "SELECT m.code kod,\n       m.name naim,\n       TRUNC(m.dt) dat\nFROM t m\n"
    sel = remove_select_column(sel, 1)
    add = build_sp_add("t", ["kod", "dat"], "Post")
    _m, _s, pairs = restore_mapping(sel, add)
    assert [p[0] for p in pairs] == ["kod", "dat"], pairs
    assert len(parse_select_columns(sel)) == len(parse_insert_columns(add)), sel

    print("sp_builder selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
