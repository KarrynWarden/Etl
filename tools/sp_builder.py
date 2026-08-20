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
  2. Либо пользователь даёт свой SELECT-запрос — тогда Select.sql это список
     сопоставленных колонок, НАДСТРОЕННЫЙ над его запросом:
         SELECT kod, naim
           FROM ( <запрос пользователя дословно> ) src
     Ровно так же берёт данные сложный ETL (`SELECT {fields_str} FROM
     ( {select_sql} ) p`, etlFolder/queries/general/newEtl/*.sql), только там
     обёртку собирает рантайм из json-структуры, а здесь — конструктор, потому
     что у справочника структур нет.

     Смысл обёртки: переносятся ТОЛЬКО сопоставленные колонки и берутся ПО
     ИМЕНИ. Значит лишняя колонка в запросе безвредна, порядок в нём ничего не
     решает, а убрать колонку из линии можно, не тронув чужой текст. Раньше
     Select.sql был текстом запроса как есть, состав и порядок обязаны были
     совпадать с INSERT (рантайм кладёт строку выборки без проекции), и правка
     сопоставления лезла в запрос.

Сопоставление колонок — ПОЗИЦИОННОЕ: строки SELECT-СПИСКА ложатся в INSERT по
порядку. Для обёрнутого запроса это внешний список, который собирает сам
конструктор, поэтому разойтись они не могут.
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


# Обёртка вокруг рукописного запроса. Ровно то же, что делает сложный ETL:
#     SELECT {fields_str}
#     FROM ( {select_sql} ) p
# (etlFolder/queries/general/newEtl/RecordSelectGroupPost.sql и соседние) —
# только там её собирает рантайм из json-структуры, а здесь конструктор, потому
# что у справочника структур нет: линия описана двумя SQL.
_WRAP_HEAD = "  FROM (\n"
_WRAP_TAIL = "\n) src\n"


def build_sp_select_over(inner_sql, master_cols):
    """`SELECT c1, c2 FROM ( <ваш запрос> ) src` — колонки берутся ПО ИМЕНИ.

    Зачем обёртка. Рантайм справочника кладёт строку выборки в INSERT без
    проекции, поэтому раньше состав и ПОРЯДОК колонок запроса были обязаны
    совпадать с INSERT — а значит любая правка сопоставления лезла в чужой
    текст: убрал колонку из пары, и её приходилось вырезать из SELECT. Обёртка
    снимает это целиком: список колонок собирает конструктор, запрос внутри
    остаётся байт в байт, лишние колонки в нём никому не мешают, порядок
    неважен.

    Текст вставляется ДОСЛОВНО и без отступов — так `unwrap_select` достаёт его
    обратно точно таким же, и «запрос не менялся» значит именно это.
    """
    if not master_cols:
        raise ValueError("Нет колонок для SELECT.")
    inner = (inner_sql or "").strip()
    if not inner:
        raise ValueError("Пустой SELECT-запрос.")
    # Точка с запятой законна в конце файла, но не внутри подзапроса.
    inner = inner.rstrip().rstrip(";").rstrip()
    cols = ",\n       ".join(master_cols)
    return (
        "-- Список колонок собирает конструктор: они берутся ПО ИМЕНИ из запроса\n"
        "-- ниже и идут один в один с Add.sql. Поэтому в самом запросе порядок\n"
        "-- колонок ничего не решает, а лишние колонки безвредны.\n"
        f"SELECT {cols}\n" + _WRAP_HEAD + inner + _WRAP_TAIL
    )


def unwrap_select(text):
    """Достать рукописный запрос из обёртки. None — обёртки нет.

    Обратная к build_sp_select_over и обязана быть ТОЧНОЙ: по этому тексту
    человек правит свой запрос, и вернуться на диск он должен без единого
    изменённого пробела.
    """
    if not text:
        return None
    mask = _mask(text)
    at = mask.find(_WRAP_HEAD)
    if at < 0:
        return None
    start = at + len(_WRAP_HEAD)
    # Ищем скобку, парную открывающей, — по маске, чтобы скобки внутри строк и
    # комментариев не сбивали счёт.
    depth = 1
    for i in range(start, len(mask)):
        if mask[i] == "(":
            depth += 1
        elif mask[i] == ")":
            depth -= 1
            if depth == 0:
                break
    else:
        return None
    if text[i:] != _WRAP_TAIL.lstrip("\n") or text[i - 1] != "\n":
        return None                      # закрывается не концом файла — не наша
    return text[start:i - 1]


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


def _ident_key(item):
    """Ключ сравнения имени. У НЕэкранированного идентификатора регистр не
    значим ни в Oracle, ни в Postgres, поэтому `KOKNAEV.SPACC` и
    `KOKNAEV.spacc` — одно и то же имя; в кавычках регистр значим и остаётся.
    Выражение (не имя) сравнивается как есть — в нём могут быть литералы, где
    регистр решает всё."""
    s = (item or "").strip()
    if _BARE_NAME_RE.match(s) or _QUALIFIED_NAME_RE.match(s):
        return ".".join(p if p.startswith('"') else p.lower()
                        for p in _NAME_SEG_RE.findall(s))
    return s


def _select_key(text):
    return ([_ident_key(c) for c in parse_select_columns(text)],
            _ident_key(_master_from_select(text) or ""))


def _insert_key(text):
    table, cols = parse_insert_columns(text)
    return _ident_key(table or ""), [_ident_key(c) for c in cols]


def _keep_if_same(current, fresh, parse):
    """Оставить текущий текст, если он значит ТО ЖЕ, что свежесобранный.

    Файлы писаны руками и разложены по-своему: у SPMKB все семнадцать колонок в
    одну строку, а генератор ставит перенос после каждой. Без этой проверки
    правка любого поля линии показывала бы в предпросмотре переписанный SQL —
    правку, которой человек не делал, — и приучала бы снимать галочки не глядя.

    Сравниваются не тексты, а смысл: список колонок (и таблица). Разошлись —
    берём свежий.
    """
    if not (current or "").strip():
        return fresh
    try:
        return current if parse(current) == parse(fresh) else fresh
    except Exception:
        return fresh


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
        # Своё SELECT. Переносятся ТОЛЬКО сопоставленные колонки, и берутся они
        # по имени — конструктор надстраивает над вашим запросом список колонок
        # (build_sp_select_over). Поэтому лишняя колонка в запросе безвредна, а
        # порядок в нём ничего не решает.
        #
        # Раньше требовалось сопоставить каждую колонку запроса, и убрать
        # ненужную можно было только вырезав её из текста: рантайм кладёт строку
        # выборки в INSERT без проекции, так что состав и порядок обязаны были
        # совпадать. То есть правка сопоставления лезла в чужой текст — а
        # «запертый» запрос при этом всё равно менялся.
        m_order = [m for m, s in pairs if m and s]
        s_order = [s for m, s in pairs if m and s]
        if not s_order:
            raise ValueError("Не сопоставлено ни одной колонки.")
        inner = (spec.get("select_sql_text") or "").strip()
        if not inner:
            raise ValueError("Пустой SELECT-запрос.")
        # Обёртка выбирает колонки ПО ИМЕНИ, поэтому имя обязано быть — и
        # обязано найтись в запросе. Без проверки линия ушла бы на диск и упала
        # на первом же прогоне с «invalid identifier», а виноватым назначили бы
        # перенос.
        nameless = [m for m in m_order if not _BARE_NAME_RE.match(m)]
        if nameless:
            raise ValueError(
                "Колонки без имени выбрать из запроса нельзя: "
                + ", ".join(nameless)
                + ". Задайте им псевдоним в запросе (кнопка «дать имя» в строке "
                  "колонки) — или снимите с них сопоставление.")
        avail = [output_name(c) for c in parse_select_columns(inner)]
        if avail:                      # '*' и неразобранный запрос не проверяем
            lost = [m for m in m_order if m not in avail]
            if lost:
                raise ValueError(
                    "В запросе нет таких колонок: " + ", ".join(lost)
                    + ". Доступны: " + ", ".join(avail) + ".")
            dup = sorted({m for m in m_order if avail.count(m) > 1})
            if dup:
                raise ValueError(
                    "В запросе несколько колонок с одним именем: "
                    + ", ".join(dup)
                    + " — какую из них брать, неоднозначно. Дайте им разные "
                      "псевдонимы.")
        select_content = build_sp_select_over(inner, m_order)
    else:
        if not master_table:
            raise ValueError("Не задана ведущая таблица.")
        m_order = [m for m, s in pairs if m and s]
        s_order = [s for m, s in pairs if m and s]
        if not s_order:
            raise ValueError("Не сопоставлено ни одной колонки.")
        select_content = _keep_if_same(
            spec.get("select_sql_text"),
            build_sp_select(master_table, m_order), _select_key)

    add_content = _keep_if_same(
        spec.get("add_sql_text"), build_sp_add(slave_table, s_order, dbs),
        _insert_key)

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
    select_file = _text(select_rel)
    add_text = _text(body.get("addSql"))
    # Форме отдаём ВАШ запрос, а не файл целиком: внешний список колонок собирает
    # конструктор, и правит его человек на вкладке колонок, а не руками в тексте.
    inner = unwrap_select(select_file)
    select_text = select_file if inner is None else inner
    # если Select.sql — это простой `SELECT ... FROM <master>`, режим 'table'
    master_from = _master_from_select(select_file)
    # Режим источника: сохранённый флаг — источник истины. У фрагментов,
    # созданных до его появления, флага нет — там остаётся прежняя эвристика
    # «простой SELECT ... FROM t => из таблицы» (она может ошибиться на своём
    # запросе без WHERE; такую линию достаточно один раз пересохранить).
    # Нет ключа selectSql вовсе — это режим «вся таблица» (см. build_sp_all).
    # Проверять его надо ПЕРВЫМ: у такой линии нет текста, по которому
    # эвристика ниже могла бы что-то определить, и она давала «custom».
    if not select_rel:
        src_mode = "all"
    elif inner is not None:
        src_mode = "custom"          # обёртка бывает только у своего запроса
    else:
        src_mode = body.get("srcMode") or (
            "table" if _looks_generated(select_file) else "custom")
    # текущее сопоставление колонок — прямо из сохранённых SQL (без обращения к БД)
    master_cols, slave_cols, pairs = restore_mapping(select_file, add_text)
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
    """Достать имя ведущей из простого `SELECT ... FROM <table>` (или None)."""
    if not text:
        return None
    m = _SELECT_FROM_RE.match(text.strip())
    return m.group(1) if m else None


def _looks_generated(text):
    """Похоже ли на запрос, СОБРАННЫЙ конструктором из списка колонок.

    Нужно только для старых фрагментов, где нет ключа srcMode: по одному тексту
    режим не восстановить, и раньше хватало «простой SELECT ... FROM t». Этого
    мало. `SELECT IDROW IDRW, MO, ... FROM KOKNAEV.SPSTRUCTURE` подходит под тот
    признак, но собран руками — в нём переименование, и колонки IDRW в таблице
    нет. Признай такую линию «собранной из колонок», и первая же пересборка
    напишет `SELECT IDRW, ...`, то есть сломает работающий перенос.

    Поэтому требование строгое: КАЖДЫЙ элемент списка — голое имя. Всё
    остальное — псевдонимы, выражения, DISTINCT — писал человек, и трогать это
    конструктор не вправе.
    """
    if not _master_from_select(text):
        return False
    if re.search(r"\bSELECT\s+(?:DISTINCT|UNIQUE|ALL)\b", _mask(text), re.I):
        return False                 # конструктор такого не пишет — значит рука
    cols = parse_select_columns(text)
    return bool(cols) and all(_BARE_NAME_RE.match(c) for c in cols)


# Комментарии, строковые литералы, имена в кавычках. Внутри них бывает что
# угодно — запятая, скобка, слово SELECT, — и разбор об это спотыкался.
_MASKED_RE = re.compile(r"--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'|\"[^\"]*\"", re.S)


def _mask(text):
    """Копия текста ТОЙ ЖЕ длины: содержимое комментариев, строковых литералов
    и имён в кавычках заменено пробелами.

    Границы (список колонок, запятые верхнего уровня, глубина скобок) ищутся по
    маске, а куски берутся из ИСХОДНОГО текста по тем же смещениям — ради этого
    длина и сохраняется. Без маски разбор ломался на живых файлах:

      * `when ffinsource = '1,2,3,4' then 1` — запятые внутри литерала делили
        одну колонку на четыре, число колонок не сходилось с INSERT, и вся
        линия показывалась заглушками «(колонка N)» (SPEINDEXFORM);
      * `-- на проде этот SELECT отрабатывал` — слово SELECT в комментарии
        забирало разбор себе, и «списком колонок» становился хвост комментария
        (разовый перенос SPEINDEX).

    Кавычки остаются на месте: по ним видно границы имени, а содержимое для
    поиска границ неважно. Комментарий гасится целиком, вместе со скобками.
    """
    out = list(text)
    for m in _MASKED_RE.finditer(text):
        a, b = m.span()
        keep = 0 if text[a] in "-/" else 1     # у литерала кавычки сохраняем
        for i in range(a + keep, b - keep):
            out[i] = " "
    return "".join(out)


_SELECT_LIST_RE = re.compile(r"\bSELECT\b(.*?)\bFROM\b", re.S | re.I)
# DISTINCT / UNIQUE / ALL — свойство всей выборки, а не первая колонка. Пока
# слово считалось её частью, колонка звалась «distinct t.code», а удаление
# первой колонки унесло бы с ней и сам DISTINCT — молча, вместе со смыслом
# запроса.
_SET_QUANT_RE = re.compile(r"\s*(?:DISTINCT|UNIQUE|ALL)\b", re.I)
_INSERT_COLS_RE = re.compile(
    r"\bINSERT\s+INTO\s+([A-Za-z0-9_.\"]+)\s*\(([^)]*)\)\s*VALUES", re.S | re.I)


def _select_list(text):
    """(маска, начало, конец) списка колонок: после SELECT [DISTINCT] и до FROM.
    None — разобрать не удалось."""
    if not text:
        return None
    mask = _mask(text)
    m = _SELECT_LIST_RE.search(mask)
    if not m:
        return None
    a, b = m.span(1)
    quant = _SET_QUANT_RE.match(mask, a, b)
    if quant:
        a = quant.end()
    return mask, a, b


def _column_spans(text):
    """Границы каждого элемента SELECT-списка в ИСХОДНОМ тексте: [(нач, кон)].

    Одна точка входа на весь разбор: и на чтение имён, и на точечную правку. Без
    неё «прочитать колонку» и «поправить колонку» считали границы порознь, и
    достаточно было одной запятой в литерале, чтобы они разошлись.
    """
    sel = _select_list(text)
    if not sel:
        return []
    mask, a, b = sel
    return [(a + x, a + y) for x, y in _split_top_level_spans(mask[a:b])]


def parse_select_columns(text):
    """Список колонок из SELECT-списка простого `SELECT c1, c2, ... FROM ...`.

    Для сгенерированного запроса «из таблицы» вернёт чистые имена колонок. Для
    своего SELECT — выражения как есть (напр. 'a.ID'); если распарсить нельзя
    (нет FROM, '*' и т.п.) — пустой список."""
    cols = [text[a:b] for a, b in _column_spans(text)]
    if any(c == "*" or c.endswith(".*") for c in cols):
        return []
    return cols


def _split_top_level_spans(s):
    """Разбить список через запятую по верхнему уровню, вернув границы кусков:
    'a, f(x, y), b' -> [(0,1), (3,11), (13,14)]. Запятые внутри скобок
    разделителями не считаются.

    Границы, а не сами куски: так один элемент списка правится точечно, не
    пересобирая и не переформатируя остальные. Строку сюда передают
    ЗАМАСКИРОВАННУЮ (см. _mask) — иначе запятая внутри литерала делит колонку
    пополам."""
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
_NAME_SEG = r"\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_$#]*"
_BARE_NAME_RE = re.compile(rf"^(?:{_NAME_SEG})$")
# Имя с уточнением — `m.code`, `s.t.code`. Отдельно от голого, потому что это
# ВЫРАЖЕНИЕ, а не просто выбор колонки: переименовать его надо псевдонимом
# (`m.code kod`), иначе замена целиком выбросит из запроса псевдоним таблицы, а
# показывать в списке колонок надо последнюю часть — ровно так колонку назовёт
# сама БД.
_QUALIFIED_NAME_RE = re.compile(rf"^(?:{_NAME_SEG})(?:\.(?:{_NAME_SEG}))+$")
_NAME_SEG_RE = re.compile(_NAME_SEG)
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
      m.code               -> m.code kod       имя с уточнением — выражение:
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
    spans = _column_spans(text)
    if not (0 <= index < len(spans)):
        return text
    a, b = spans[index]
    item = text[a:b]
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
    return text[:a] + replacement + text[b:]


def parse_insert_columns(text):
    """(таблица, [колонки]) из `INSERT INTO t (c1, c2, ...) VALUES ...`.
    Если распарсить нельзя — (None, [])."""
    if not text:
        return None, []
    # По маске — ровно как SELECT: у половины файлов над INSERT висит шапка
    # комментария, и слово INSERT в ней не должно забирать разбор себе.
    mask = _mask(text)
    m = _INSERT_COLS_RE.search(mask)
    if not m:
        return None, []
    a, b = m.span(2)
    return text[m.start(1):m.end(1)], [
        text[a + x:a + y] for x, y in _split_top_level_spans(mask[a:b])]


def output_name(item):
    """Имя ВЫХОДНОЙ колонки для элемента SELECT-списка.

    'code' -> 'code';  'm.code kod' -> 'kod';  'm.code AS kod' -> 'kod';
    'm.code' -> 'code' (так колонку назовёт и сама БД);
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
    if _QUALIFIED_NAME_RE.match(item):
        return _NAME_SEG_RE.findall(item)[-1]
    tail = _ALIAS_TAIL_RE.search(item)
    if tail and tail.group(1).strip('"').lower() not in _NOT_ALIAS:
        return tail.group(1)
    return item


def _cols_as_dicts(names):
    return [{"column_name": n, "data_type": "", "data_scale": None,
             "is_primary_key": None} for n in names]


def restore_mapping(select_text, add_text):
    """Восстановить (master_cols, slave_cols, pairs) из сохранённых SQL — без БД.

    Сопоставление позиционное: i-я колонка SELECT-СПИСКА ложится в i-ю колонку
    INSERT. Для обёрнутого запроса «SELECT-список» — это внешний список,
    собранный конструктором; сам запрос внутри может отдавать сколько угодно
    колонок в любом порядке, и на сопоставление это не влияет.

    master_cols — что ДОСТУПНО (колонки запроса), pairs — что ВЫБРАНО. Пока
    обёртки не было, это было одно и то же, и разница ни на что не влияла; с
    обёрткой она и есть весь смысл: выбрать можно не всё.

    Если число колонок внешнего списка и INSERT не совпало (или SELECT не
    распарсился), метки ведущей заменяются заглушками «(колонка N)», а порядок
    берётся из INSERT — так текущее сопоставление всё равно видно, пусть и без
    имён. Возвращает пустые списки, если INSERT распарсить не удалось.
    """
    _tbl, ins_cols = parse_insert_columns(add_text)
    if not ins_cols:
        return [], [], []
    # Имена ВЫХОДНЫХ колонок, а не выражения целиком: см. output_name.
    chosen = [output_name(c) for c in parse_select_columns(select_text)]
    inner = unwrap_select(select_text)
    avail = ([output_name(c) for c in parse_select_columns(inner)]
             if inner is not None else list(chosen))
    if len(chosen) != len(ins_cols):
        chosen = [f"(колонка {i + 1})" for i in range(len(ins_cols))]
    pairs = list(zip(chosen, ins_cols))
    # Доступные — объединение: колонка, выбранная в паре, обязана быть в списке,
    # даже если запрос внутри разобрать не удалось (`SELECT *`).
    for name in chosen:
        if name not in avail:
            avail.append(name)
    return _cols_as_dicts(avail), _cols_as_dicts(ins_cols), pairs


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

    # 3) свой SELECT: запрос уходит на диск внутри обёртки, колонки — сверху
    inner3 = "SELECT a.ID, a.NAME FROM KOKNAEV.IPERSON a WHERE a.ACT = 1"
    spec3 = {
        "kind": "once", "master_table": "", "slave_table": "ipersonact",
        "db_master": "Orcl", "db_slave": "Post", "master_label": "ipersonact",
        "select_mode": "custom", "select_sql_text": inner3,
        "pairs": [("ID", "id"), ("NAME", "name")],
    }
    files3, key3 = build_sp_all(spec3)
    assert key3 == "ipersonactOrclPost", key3
    d3 = dict(files3)
    assert "etlFolder/SpOnce.d/ipersonactOrclPost.json" in d3
    sel3 = d3["etlFolder/queries/sp/ipersonact/Select.sql"]
    assert unwrap_select(sel3) == inner3, sel3
    assert parse_select_columns(sel3) == ["ID", "NAME"], sel3
    # Колонка без пары больше НЕ ошибка: переносятся только сопоставленные, а
    # лишняя в запросе никому не мешает — она просто не попадает во внешний
    # список. Раньше здесь был отказ, и убрать колонку можно было лишь вырезав
    # её из чужого текста.
    part = dict(build_sp_all(dict(spec3, pairs=[("ID", "id"), ("NAME", None)]))[0])
    assert parse_select_columns(
        part["etlFolder/queries/sp/ipersonact/Select.sql"]) == ["ID"]
    assert part["etlFolder/queries/sp/ipersonact/Add.sql"] == \
        "INSERT INTO ipersonact (id)\nVALUES %s\n"

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

    # ── разбор не спотыкается о комментарии, литералы и DISTINCT ─────────────
    # Всё это взято из живых файлов, и каждое ломало разбор так, что линия
    # показывалась заглушками «(колонка N)» — то есть конструктор говорил «я не
    # знаю, что тут за колонки» про совершенно обычный запрос.
    lit = ("SELECT code, name, period,\ncase \nwhen f = '1,2,3,4' then 1\n"
           "when f = '2,3,4' then 3\nend as ffinsource, \n'', idrw from t\n")
    assert parse_select_columns(lit) == [
        "code", "name", "period",
        "case \nwhen f = '1,2,3,4' then 1\nwhen f = '2,3,4' then 3\nend as ffinsource",
        "''", "idrw"], parse_select_columns(lit)

    commented = ("-- на проде этот SELECT отрабатывал\n"
                 "-- порядок колонок обязан совпадать с Add.sql\n"
                 "SELECT code, name\n  FROM speindex\n")
    assert parse_select_columns(commented) == ["code", "name"], \
        parse_select_columns(commented)
    assert parse_insert_columns(
        "-- вставка в ведомую, INSERT INTO чего-то другого тут нет\n"
        "INSERT INTO t (a, b)\nVALUES %s\n") == ("t", ["a", "b"])

    # DISTINCT — свойство выборки, а не первая колонка
    for sql, want in (
        ("SELECT DISTINCT code, name FROM t\n", ["code", "name"]),
        ("SELECT distinct 0 fil, cont FROM t\n", ["0 fil", "cont"]),
        ("select unique a, b from t\n", ["a", "b"]),
    ):
        assert parse_select_columns(sql) == want, parse_select_columns(sql)
    # …и остаётся на месте, что бы с колонками ни делали
    assert rename_select_column("SELECT DISTINCT a, b FROM t\n", 0, "kod") == \
        "SELECT DISTINCT kod, b FROM t\n"

    # имя ВЫХОДНОЙ колонки — одно и то же, откуда бы колонки ни пришли:
    # снятие по запросу берёт имена у драйвера (псевдонимы), чтение с диска
    # разбирает текст. Без output_name одна колонка звалась то 'kod', то
    # 'm.code kod' — смотря открыли линию заново или только что сняли структуру
    for item, want in (("code", "code"), ("m.code kod", "kod"),
                       ("m.code AS kod", "kod"), ("TRUNC(dt)", "TRUNC(dt)"),
                       ('m."Mixed" mc', "mc"), ("CASE WHEN a=1 THEN 2 END",
                                                "CASE WHEN a=1 THEN 2 END"),
                       # имя с уточнением: так его назовёт и сама БД
                       ("m.code", "code"), ('m."Mixed"', '"Mixed"'),
                       ('"account"', '"account"')):
        assert output_name(item) == want, (item, output_name(item))
    # …а переименование такого имени идёт псевдонимом: замена целиком выбросила
    # бы из запроса псевдоним таблицы, и запрос перестал бы работать
    assert rename_select_column("SELECT m.code, b FROM t m\n", 0, "kod") == \
        "SELECT m.code kod, b FROM t m\n"

    # круг: переименовал -> разобрал -> имя стало новым, выражение цело
    sel = "SELECT m.code kod,\n       TRUNC(m.dt)\nFROM t m\nWHERE m.dend IS NULL\n"
    add = "INSERT INTO t (kod, dat)\nVALUES %s\n"
    sel = rename_select_column(sel, 1, "data_nach")
    _m, _s, pairs = restore_mapping(sel, add)
    assert [p[0] for p in pairs] == ["kod", "data_nach"], pairs
    assert "TRUNC(m.dt)" in sel and "WHERE m.dend IS NULL" in sel, sel

    # ── обёртка: список колонок сверху, ваш запрос внутри ───────────────────
    # Главное свойство — ТОЧНЫЙ круг. По этому тексту человек правит свой
    # запрос, и вернуться на диск он обязан без единого изменённого пробела:
    # «запрос не менялся» значит именно это, а не «почти не менялся».
    inner = ("select distinct t.code kod, t.\"name\" naim, -- лишняя, но пусть\n"
             "       trunc(t.dt) dat\n"
             "  from kok.spr t\n where t.dend is null")
    wrapped = build_sp_select_over(inner, ["kod", "dat"])
    assert unwrap_select(wrapped) == inner, repr(unwrap_select(wrapped))
    assert parse_select_columns(wrapped) == ["kod", "dat"], wrapped
    assert wrapped.endswith("\n) src\n"), wrapped
    # запрос внутри лежит дословно — ни отступов, ни переносов от себя
    assert "\n" + inner + "\n) src\n" in wrapped, wrapped
    # у обычного файла обёртки нет, и выдумывать её нельзя
    assert unwrap_select("SELECT a, b FROM t\n") is None
    assert unwrap_select("") is None
    # ') src' внутри запроса не принимается за конец обёртки
    tricky = "select * from (select 1 x\n) src\nwhere x = 1"
    assert unwrap_select(build_sp_select_over(tricky, ["x"])) == tricky

    # сопоставление читается по ВНЕШНЕМУ списку, а доступные колонки — по
    # внутреннему запросу: в этом вся разница, ради которой обёртка и заведена
    add = build_sp_add("spr", ["kod", "dat"], "Post")
    master, slave, pairs = restore_mapping(wrapped, add)
    assert [c["column_name"] for c in master] == ["kod", "naim", "dat"], master
    assert [c["column_name"] for c in slave] == ["kod", "dat"], slave
    assert pairs == [("kod", "kod"), ("dat", "dat")], pairs

    # круг через build_sp_all: убрали пару — запрос внутри БАЙТ В БАЙТ прежний,
    # изменился только внешний список
    base = {"kind": "regular", "master_table": "kok.spr", "slave_table": "spr",
            "db_master": "Orcl", "db_slave": "Post", "master_label": "SPR",
            "select_mode": "custom", "select_sql_text": inner}
    f_all, _k = build_sp_all(dict(base, pairs=[("kod", "kod"), ("naim", "naim"),
                                               ("dat", "dat")]))
    f_less, _k = build_sp_all(dict(base, pairs=[("kod", "kod"), ("dat", "dat")]))
    sel_all = dict(f_all)["etlFolder/queries/sp/SPR/Select.sql"]
    sel_less = dict(f_less)["etlFolder/queries/sp/SPR/Select.sql"]
    assert unwrap_select(sel_all) == unwrap_select(sel_less) == inner
    assert parse_select_columns(sel_less) == ["kod", "dat"], sel_less
    assert len(parse_select_columns(sel_less)) == \
        len(parse_insert_columns(dict(f_less)["etlFolder/queries/sp/SPR/Add.sql"])[1])

    # колонку, которой в запросе нет, выбрать нельзя — иначе линия уедет на диск
    # и упадёт на первом прогоне с «invalid identifier»
    for bad_pairs, needle in (
        ([("kod", "kod"), ("nosuch", "x")], "нет таких колонок"),
        ([("kod", "kod"), ("trunc(t.dt)", "x")], "без имени"),
    ):
        try:
            build_sp_all(dict(base, pairs=bad_pairs))
        except ValueError as e:
            assert needle in str(e), e
        else:
            raise AssertionError(f"ожидался отказ: {bad_pairs}")

    # эвристика режима у старых фрагментов: «собрано конструктором» — это когда
    # КАЖДЫЙ элемент голое имя. Псевдоним `IDROW IDRW` писал человек, и пересборка
    # такой линии из имён дала бы `SELECT IDRW, ...` — колонки, которой в
    # таблице нет.
    assert _looks_generated("SELECT ID, NAME\nFROM KOKNAEV.SPR\n")
    assert not _looks_generated("SELECT IDROW IDRW, MO FROM KOKNAEV.SPSTRUCTURE\n")
    assert not _looks_generated("SELECT DISTINCT a, b FROM t\n")
    assert not _looks_generated("SELECT a FROM t WHERE x = 1\n")

    print("sp_builder selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
