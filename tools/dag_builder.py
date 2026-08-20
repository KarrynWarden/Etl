#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ядро генератора ETL-линии (без UI).

Делает три вещи, на которые раньше уходила ручная работа:
  1. snap_structure() — снимает структуру таблицы прямо из БД (тем же
     StructureCheck-запросом, что и рантайм-проверка), в формате json-структур;
     snap_query_structure() — то же по СВОЕМУ SELECT-запросу (приоритетнее
     таблицы: рантайм тоже проверяет структуру по запросу, а не по таблице).
  2. auto_match()     — предлагает сопоставление колонок master->slave по имени
     (имена в двух БД могут отличаться — остальное поправит человек в UI).
  3. build_all()/write_files() — генерит из спецификации три артефакта линии:
        etlFolder/structures/<master>/<master>.json   (ведущая, в порядке маппинга)
        etlFolder/structures/<master>/<slave>.json     (ведомая, в ТОМ ЖЕ порядке)
        etlFolder/config.d/<key>.json                  (фрагмент конфига линии)
        dags/<DagId>.py                                (даг)
     Сопоставление колонок — ПОЗИЦИОННОЕ (do_etl сверяет длины и переносит по
     индексу), поэтому оба json пишутся в одном согласованном порядке.
     Вместо своего дага линию можно положить в СОСТАВНОЙ (`group_dag_id`):
     один файл на несколько линий со списком LINES, который конструктор читает
     и дописывает (см. build_group_dag_py / parse_group_dag). Так собраны
     линии одного источника — например, все doctype mocheck.

UI (ipywidgets) вызывает эти функции; сам по себе модуль консольно-тестируемый.
Запуск проверки шаблонов без БД:  python3 tools/dag_builder.py --selftest
"""
import contextlib
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ETLFOLDER = os.path.join(ROOT, "etlFolder")
QUERIES_GENERAL = os.path.join(ETLFOLDER, "queries", "general")

# Ключи json-структуры — РАЗНЫЕ по диалектам (так их читает jsonLoad.py).
PG_KEYS = ("column_name", "data_type", "data_scale", "is_primary_key")
ORCL_KEYS = ("COLUMN_NAME", "DATA_TYPE", "DATA_SCALE", "IS_PRIMARY_KEY")


def keys_for(db):
    return PG_KEYS if db == "Post" else ORCL_KEYS


def _read_text(path):
    """Прочитать текстовый файл терпимо к кодировке. Файлы (даги/конфиги/sql)
    могли быть сохранены Windows-редактором в UTF-16 или с BOM — строгий utf-8
    на таком падает 'invalid start byte 0xff'. Порядок: BOM → utf-8 → cp1251."""
    with open(path, "rb") as fp:
        raw = fp.read()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1251", errors="replace")


def _read_json(path):
    return json.loads(_read_text(path))


@contextlib.contextmanager
def _group_writable():
    """На время создания файлов ставим umask 002 → новые файлы 664, каталоги 775
    (груп-записываемые). В связке с setgid-каталогами группы etldev это значит,
    что файлы, созданные приложением под пользователем jupyter, остаются
    редактируемыми и удаляемыми с dev-ПК (пользователь devel, та же группа).
    Без этого umask по умолчанию (022) даёт 644/755 — и devel не может их удалить."""
    old = os.umask(0o002)
    try:
        yield
    finally:
        os.umask(old)


def bare(table):
    """Имя без схемы: 'KOKNAEV.PRBDIR' -> 'PRBDIR'."""
    return table.split(".")[-1]


# ─────────────────────────── снятие структуры из БД ───────────────────────────

def snap_structure(db, table, cred="MAIN"):
    """Снять структуру таблицы из БД -> список словарей с ключами под диалект.

    db: 'Post' | 'Orcl'. cred: имя набора реквизитов (.env): MAIN, A56, ...
    Connect импортируется лениво — модуль грузится и без драйверов БД.
    """
    from Connect import connectPostgres, connectOracle  # noqa: E402

    sqlfile = "StructureCheckPost.sql" if db == "Post" else "StructureCheckOrcl.sql"
    with open(os.path.join(QUERIES_GENERAL, sqlfile), encoding="utf-8") as fp:
        sql = fp.read()

    conn = connectPostgres(cred) if db == "Post" else connectOracle(cred)
    try:
        cur = conn.cursor()
        cur.execute(sql, {"TABLENAME": bare(table)})
        rows = cur.fetchall()  # (name, data_type, data_scale, is_primary_key)
    finally:
        conn.close()

    if not rows:
        raise ValueError(
            f"Структура пуста: таблица '{bare(table)}' не найдена в {db} "
            f"(набор реквизитов {cred}) или у пользователя нет прав."
        )

    out = []
    for name, dtype, scale, pk in rows:
        out.append({
            "column_name": name, "data_type": dtype,
            "data_scale": scale,
            # нормализуем PK: 'Primary Key' либо null (Oracle отдаёт '' для не-PK)
            "is_primary_key": "Primary Key" if pk == "Primary Key" else None,
        })
    return out


# ─────────────────── снятие структуры по своему SELECT ───────────────────
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
    с номером строки и позицией (СУБД в таких случаях говорит только «invalid
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


def _probe_sql(db, stmt):
    """Обёртка «дай только описание колонок, строк не надо».

    ВНИМАНИЕ к алиасу подзапроса: он НЕ должен начинаться с подчёркивания.
    Oracle требует, чтобы неквотированный идентификатор начинался с буквы, и на
    '_sp_probe' отвечал ORA-00911 «invalid character». 'sp_probe' валиден и в
    Oracle, и в Postgres.
    """
    if db == "Post":
        return f"SELECT * FROM (\n{stmt}\n) AS sp_probe LIMIT 0"
    return f"SELECT * FROM (\n{stmt}\n) sp_probe WHERE ROWNUM < 1"


def _query_description(db, sql, cred="MAIN"):
    """[(имя колонки, тип)] по своему SELECT — без выборки строк.

    Тип приводится к тому же виду, в котором его вычисляет рантайм-проверка
    структуры источника, чтобы снятая структура проходила её как есть."""
    from Connect import connectPostgres, connectOracle  # noqa: E402
    from Functions.functionsFile.structCheck import ORACLE_CURSOR_TYPES  # noqa: E402

    stmt = _strip_terminators(sql)
    if not stmt:
        raise ValueError("Пустой SELECT-запрос.")
    bad = _check_sql_chars(stmt)
    if bad:
        raise ValueError(f"В тексте запроса недопустимый символ — {bad}")

    conn = connectPostgres(cred) if db == "Post" else connectOracle(cred)
    try:
        cur = conn.cursor()
        if db == "Post":
            # Имена типов Postgres берём так же, как рантайм-проверка структуры
            # (StructCheckPostgresQuery): typname из pg_type по oid из описания.
            cur.execute("SELECT oid, typname FROM pg_type")
            types = {oid: name for oid, name in cur.fetchall()}
            cur.execute(_probe_sql(db, stmt))
            desc = [(d[0], types.get(d[1], "unknown")) for d in (cur.description or [])]
        else:
            cur.execute(_probe_sql(db, stmt))
            desc = [(d[0], ORACLE_CURSOR_TYPES.get(str(d[1]), "UNKNOWN"))
                    for d in (cur.description or [])]
    finally:
        conn.close()

    if not desc:
        raise ValueError("Запрос не вернул ни одной колонки (проверь SELECT).")
    return desc


def snap_query_columns(db, sql, cred="MAIN"):
    """Список колонок своего SELECT-запроса — только имена (типы пустые).
    Используется конструктором справочников: там json-структуры не пишутся."""
    return [{"column_name": name, "data_type": "", "data_scale": None,
             "is_primary_key": None} for name, _t in _query_description(db, sql, cred)]


def snap_query_structure(db, sql, cred="MAIN"):
    """Структура своего SELECT-запроса с ТИПАМИ — для json-структуры ведущей.

    Типы снимаются по тому же словарю, каким рантайм сверяет структуру источника
    (StructCheckOracleQuery / StructCheckPostgresQuery), поэтому снятая структура
    проходит проверку как есть. Тип константы (`null CHECKDIR`, `2 CHECKDIR`)
    драйвер определить не может — там оказывается 'UNKNOWN' / 'unknown'; такие
    колонки рантайм сверяет только по имени, а тип можно дописать руками в UI.
    PK из описания курсора не виден — его подмешивает merge_table_pk().
    """
    return [{"column_name": name, "data_type": dtype, "data_scale": None,
             "is_primary_key": None} for name, dtype in _query_description(db, sql, cred)]


def unknown_type(dtype):
    """Тип, который драйвер не смог определить (константа в запросе)."""
    return str(dtype or "").strip().lower() in ("", "unknown")


def merge_table_pk(query_cols, table_cols):
    """Подмешать в колонки запроса признак PK (и scale) из структуры таблицы.

    Запрос знает имена и типы, но не знает первичный ключ — а он нужен и для
    переноса, и для триггера. Сопоставляем по имени без учёта регистра; типы НЕ
    трогаем: рантайм сверяет структуру источника по описанию курсора, поэтому
    тип таблицы там был бы неверным ориентиром.
    """
    by_norm = {}
    for c in table_cols or []:
        by_norm.setdefault(str(c["column_name"]).lower(), c)
    out = []
    for c in query_cols:
        src = by_norm.get(str(c["column_name"]).lower())
        merged = dict(c)
        if src:
            merged["is_primary_key"] = src.get("is_primary_key")
            if merged.get("data_scale") is None:
                merged["data_scale"] = src.get("data_scale")
        out.append(merged)
    return out


# ─────────────────────────── сопоставление колонок ───────────────────────────

def _norm(name):
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def auto_match(master_cols, slave_cols):
    """Предложить slave-колонку для каждой master-колонки по нормализованному имени.

    Возвращает (suggestions, slave_unmatched):
      suggestions    — список slave-имён (или None) в порядке master_cols;
      slave_unmatched — slave-колонки, которым не нашлось пары (для подсказки в UI).
    """
    slave_by_norm = {}
    for c in slave_cols:
        slave_by_norm.setdefault(_norm(c["column_name"]), c["column_name"])

    suggestions, used = [], set()
    for m in master_cols:
        match = slave_by_norm.get(_norm(m["column_name"]))
        if match is not None and match not in used:
            suggestions.append(match)
            used.add(match)
        else:
            suggestions.append(None)

    slave_unmatched = [c["column_name"] for c in slave_cols
                       if c["column_name"] not in used]
    return suggestions, slave_unmatched


# ─────────────────────────── сборка артефактов ───────────────────────────

_INTERNAL = ("column_name", "data_type", "data_scale", "is_primary_key")


def _to_dialect(col, db):
    """Внутренний словарь (нижний регистр ключей) -> ключи под диалект БД."""
    return {dk: col[ik] for dk, ik in zip(keys_for(db), _INTERNAL)}


def _struct_json(cols, db):
    data = [_to_dialect(c, db) for c in cols]
    return json.dumps({"data": data}, ensure_ascii=False, indent=2) + "\n"


def _ordered_pair(master_cols, slave_cols, pairs):
    """pairs: список (master_name, slave_name). Вернуть два списка словарей
    колонок в согласованном (позиционном) порядке, только сопоставленные."""
    m_by = {c["column_name"]: c for c in master_cols}
    s_by = {c["column_name"]: c for c in slave_cols}
    m_out, s_out = [], []
    for mn, sn in pairs:
        if mn is None or sn is None:
            continue
        m_out.append(m_by[mn])
        s_out.append(s_by[sn])
    return m_out, s_out


def times_to_cron(times):
    """['11:50','13:50','20:50'] -> '50 11,13,20 * * *'. Требует одинаковой минуты
    у всех времён (как во всех текущих дагах); иначе — подсказка про cron."""
    pairs = []
    for t in times:
        t = str(t).strip()
        if not t:
            continue
        hh, _, mm = t.partition(":")
        try:
            h, m = int(hh), int(mm)
        except ValueError:
            raise ValueError(f"Неверное время '{t}'. Формат — ЧЧ:ММ, например 11:50.")
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError(f"Время вне диапазона: '{t}'.")
        pairs.append((h, m))
    if not pairs:
        raise ValueError("Не заданы времена запуска.")
    minutes = sorted({m for _, m in pairs})
    hours = sorted({h for h, _ in pairs})
    if len(minutes) != 1:
        raise ValueError(
            "У времён разные минуты — одним простым расписанием не выразить. "
            "Используй режим «Cron-выражение» (например '20 11 * * *,50 13 * * *' "
            "нельзя — нужно одно выражение)."
        )
    return f"{minutes[0]} {','.join(str(h) for h in hours)} * * *"


def build_schedule_expr(spec):
    """Литерал Python для schedule_interval из spec.

    schedule_kind: 'interval' (по умолч.) | 'times' | 'cron'.
      interval -> dt.timedelta(minutes=schedule_minutes)
      times    -> cron из списка schedule_times (['11:50', ...])
      cron     -> строка schedule_cron как есть.
    """
    kind = spec.get("schedule_kind", "interval")
    if kind == "cron":
        cron = (spec.get("schedule_cron") or "").strip()
        if not cron:
            raise ValueError("Пустое cron-выражение.")
        return repr(cron)
    if kind == "times":
        return repr(times_to_cron(spec.get("schedule_times") or []))
    minutes = int(spec.get("schedule_minutes", 1) or 1)
    if minutes < 1:
        raise ValueError("Период в минутах должен быть ≥ 1.")
    return f"dt.timedelta(minutes={minutes})"


def build_dag_py(dag_id, line_name, table_master, db_master, db_slave,
                 tags, schedule_expr="dt.timedelta(minutes=1)", retry_mode="frequent",
                 note=None, task_id=None):
    """Свой даг линии.

    note — свободный текст пользователя (зачем эта линия, чем особенна). Он
    попадает в конец docstring после маркера и ПЕРЕЖИВАЕТ перезапись, как у
    составного дага. Без этого правка любого поля линии затирала рукописный
    docstring шаблонным однострочником: у MedreeprdispOrclPost так терялось
    двадцать строк описания двухчастного процесса — и терялось молча, потому
    что даг после этого работает точно так же.

    task_id — имя задачи в даге. У существующего дага берётся ИЗ ФАЙЛА, потому
    что вся история запусков в Airflow висит именно на нём: даги писались
    руками и звали задачу do_etl_medree_prdisp, а формула даёт
    do_etl_MEDREE_PRDISP (имя линии для Oracle-ведущей — ВЕРХНИМ регистром).
    Перезапись с новым именем — это НОВАЯ задача: прежняя остаётся в базе
    Airflow отдельной строкой, её логи и статусы к новой не относятся."""
    tags_repr = ", ".join(repr(t) for t in tags)
    task_id = task_id or f"do_etl_{line_name}"
    return f'''"""DAG: {db_master}->{db_slave} для {line_name}.{_note_block(note)}"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import DEFAULT_ARGS, configureLogger, makeEtlOperator, addFreezeWatcher

with DAG(
    dag_id="{dag_id}",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=[{tags_repr}],
    schedule_interval={schedule_expr},
    catchup=False,
) as dag:
    configureLogger()
    task = makeEtlOperator(
        "{task_id}",
        tableNameMaster="{table_master}", dbMaster="{db_master}", dbSlave="{db_slave}",
        tableNameEtlJobs="{line_name}",
        retryMode="{retry_mode}",
    )
    addFreezeWatcher([task], retryMode="{retry_mode}")
'''


# ─────────────────── составной даг: несколько линий одним файлом ───────────────────
# Одна ведущая-запрос часто кормит СРАЗУ НЕСКОЛЬКО линий (mocheck: MEDCHECK,
# EXPMED, PODCHECK, TFOUTSCHET, TFINSCHET из общего MOCHECK.sql). Отдельный даг
# на каждую такую линию — это N параллельных прогонов по одному и тому же
# источнику; вручную их складывали в ОДИН даг с несколькими задачами, а
# конструктор так не умел и всегда писал свой даг на линию. Теперь умеет:
# составной даг — обычный файл в dags/ со списком LINES, который конструктор
# читает и дописывает.

GROUP_MARK = "# dagbuilder: составной даг (список линий ниже правит конструктор)"
GROUP_LINES_VAR = "LINES"
# Заметка человека внутри составного дага. Всё, что в docstring ПОСЛЕ этой
# строки, конструктор считает текстом пользователя: читает при разборе и
# кладёт обратно при перезаписи. Без неё «объяснение, зачем этот даг» жило
# только в дагах, написанных руками, — и стоило файлу стать машинным, как
# первая же дописанная линия стирала его начисто.
GROUP_NOTE_MARK = "─── заметка (правится руками, конструктор её сохраняет) ───"
# Ключ конфига линии: в каком составном даге она перечислена. Явная запись
# нужна потому, что обратный поиск (пробежать даги и найти линию в списке)
# работает только для дагов НАШЕГО формата: линия в чужом даге выглядела как
# линия вообще без дага, и правка такой линии молча заводила ей второй,
# собственный даг — то есть тот же перенос начинал идти дважды.
GROUP_DAG_KEY = "groupDag"

# Ключи конфига, за которые отвечают ПОЛЯ ФОРМЫ: их build_all собирает сам.
# Всё остальное из конфига проходит насквозь через extra — см. load_line.
_FORM_CONFIG_KEYS = frozenset((
    "_doc", "tableNameMaster", "tableNameSlave", "structureMaster",
    "structureSlave", "periodColumn", "slavePeriodColumn", "mode",
    "selectSql", "periodsSql", GROUP_DAG_KEY,
))


def _note_block(note):
    """Хвост docstring с заметкой пользователя ('' — заметки нет).

    Из текста вычищаются сам маркер (иначе при следующем разборе заметка
    «съела» бы саму себя) и тройная кавычка, которая закрыла бы docstring."""
    note = (note or "").replace(GROUP_NOTE_MARK, "").replace('"""', "'''").strip()
    return f"\n{GROUP_NOTE_MARK}\n{note}\n" if note else ""


def build_group_dag_py(dag_id, lines, tags, schedule_expr="dt.timedelta(minutes=1)",
                       retry_mode="frequent", note=None):
    """DAG на НЕСКОЛЬКО линий: по задаче на каждую, общий freeze-watcher.

    lines — [(tableNameEtlJobs, dbMaster, dbSlave)]; порядок сохраняется.
    note — свободный текст (зачем этот даг, что за линии): попадает в конец
    docstring после маркера и переживает перезапись.
    Линии, убранные в архив (флаг `disabled`), даг пропускает сам —
    lineEnabled() читает конфиг в рантайме, поэтому архивация такой линии
    работает без правки файла."""
    tags_repr = ", ".join(repr(t) for t in tags)
    items = "\n".join(f"    ({line!r}, {dbm!r}, {dbs!r})," for line, dbm, dbs in lines)
    return f'''"""DAG: составной перенос — несколько линий одним дагом.

Линии перечислены в {GROUP_LINES_VAR}: по задаче на линию, все читают свои
настройки из etlFolder/config.d. Так собирают линии одного источника
(например, все doctype mocheck из общего MOCHECK.sql), чтобы не плодить
одинаковые даги и не гонять один и тот же запрос параллельно.

Файл пишет конструктор: правки в списке линий, расписании и тегах делаются
через него (иначе следующая дописанная линия их затрёт). Свободный текст
ниже маркера — исключение: его конструктор переносит в новый файл как есть.
{_note_block(note)}"""
import datetime as dt

from airflow.models import DAG

from Functions._dagHelpers import (DEFAULT_ARGS, configureLogger, makeEtlOperator,
                                   addFreezeWatcher, lineEnabled)

{GROUP_MARK}
{GROUP_LINES_VAR} = [
{items}
]

with DAG(
    dag_id="{dag_id}",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=[{tags_repr}],
    schedule_interval={schedule_expr},
    catchup=False,
) as dag:
    configureLogger()
    tasks = [
        makeEtlOperator(
            f"do_etl_{{line}}",
            tableNameMaster=line, dbMaster=dbm, dbSlave=dbs,
            tableNameEtlJobs=line, retryMode="{retry_mode}",
        )
        for line, dbm, dbs in {GROUP_LINES_VAR}
        if lineEnabled(line, dbm, dbs)
    ]
    if tasks:
        addFreezeWatcher(tasks, retryMode="{retry_mode}")
'''


def parse_group_dag(path):
    """Разобрать составной даг конструктора. -> dict(lines, tags, ...) либо None.

    None означает «файл не в нашем формате» (например, написанный руками
    MocheckOrclPost со своей структурой) — такой мы не переписываем, а честно
    просим дописать линию руками."""
    try:
        txt = _read_text(path)
    except OSError:
        return None
    if GROUP_MARK not in txt:
        return None
    try:
        import ast
        tree = ast.parse(txt)
    except SyntaxError:
        return None
    lines = None
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if GROUP_LINES_VAR not in names:
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, SyntaxError):
            return None
        lines = [tuple(str(x) for x in item) for item in value
                 if isinstance(item, (list, tuple)) and len(item) == 3]
    if lines is None:
        return None
    res = _parse_dag_file(path)
    res["lines"] = lines
    res["note"] = _docstring_note(tree)
    res["dag_id"] = os.path.splitext(os.path.basename(path))[0]
    return res


def _docstring_note(tree):
    """Заметка пользователя из docstring СОСТАВНОГО дага ('' — её нет).

    Строго по маркеру: в составном даге под шапкой лежит ещё и описание
    формата, которое пишет сам конструктор, — принять его за пользовательский
    текст значило бы дублировать его при каждой перезаписи."""
    import ast
    doc = ast.get_docstring(tree) or ""
    if GROUP_NOTE_MARK not in doc:
        return ""
    return doc.split(GROUP_NOTE_MARK, 1)[1].strip()


def _own_dag_note(tree):
    """То же для дага ОДНОЙ линии.

    У такого дага конструктор пишет ровно одну строку docstring — шапку вида
    «DAG: Orcl->Post для X.». Значит всё, что есть кроме неё, написано руками
    и обязано пережить перезапись. Маркер тоже понимаем: даги, созданные уже
    с ним, разбираются так же, как составные.

    Без этого правка любого поля линии подменяла рукописное описание шаблонным
    однострочником — молча, потому что даг после такой замены работает точно
    так же. У MedreeprdispOrclPost так терялось двадцать строк про двухчастный
    процесс: где живёт часть 1, почему большинство запусков — пропуски и
    откуда берётся isokaudit = 4."""
    import ast
    doc = ast.get_docstring(tree) or ""
    if GROUP_NOTE_MARK in doc:
        return doc.split(GROUP_NOTE_MARK, 1)[1].strip()
    rest = doc.split("\n", 1)[1] if "\n" in doc else ""
    return rest.strip()


def list_group_dags(include_foreign=True):
    """{dag_id: (path, разбор|None)} — составные даги, которые видит конструктор.

    include_foreign — показывать и написанные руками (у них разбор None): их
    видно в списке, но переписывать конструктор не станет."""
    out = {}
    for f in _all_dag_files():
        parsed = parse_group_dag(f)
        name = os.path.splitext(os.path.basename(f))[0]
        if parsed:
            out[name] = (f, parsed)
        elif include_foreign and _looks_multiline_dag(f):
            out[name] = (f, None)
    return out


def _looks_multiline_dag(path):
    """Похоже ли, что в даге НЕСКОЛЬКО линий (для списка «куда добавить»)."""
    try:
        txt = _read_text(path)
    except OSError:
        return False
    return txt.count("makeEtlOperator") >= 1 and (
        "for " in txt and "makeEtlOperator" in txt)


def group_dag_path(group_id):
    """Путь файла составного дага по имени (в dags/ либо в архиве)."""
    for base in (_dags_dir(), _archive_dir()):
        p = os.path.join(base, f"{group_id}.py")
        if os.path.exists(p):
            return p
    return os.path.join(_dags_dir(), f"{group_id}.py")


def group_dag_of(key):
    """В каком составном даге упомянута линия. -> (dag_id, path) или (None, None).

    Источник истины — ключ `groupDag` в конфиге самой линии: он верен и для
    дага, который конструктор не разбирает, и когда файла дага ещё нет.
    Для линий, заведённых до появления ключа, остаётся прежний способ —
    поиск линии в списках составных дагов нашего формата.
    """
    try:
        body, _ = _find_config_body(key)
    except (KeyError, OSError, ValueError):
        body = {}
    named = str(body.get(GROUP_DAG_KEY) or "").strip()
    if named:
        return named, group_dag_path(named)
    line, dbm, dbs = split_key(key)
    for name, (path, parsed) in list_group_dags(include_foreign=False).items():
        if (line, dbm, dbs) in parsed["lines"]:
            return name, path
    return None, None


def group_dag_drop_line(key, group_id=None):
    """Убрать линию из списка составного дага. -> путь или None.

    group_id — из какого дага убирать; по умолчанию берётся текущий (по
    конфигу линии). Явно его передают, когда конфиг уже переписан на НОВОЕ
    размещение, а вычеркнуть линию надо из прежнего.
    None возвращается и когда дага нет, и когда он не нашего формата: чужой
    файл конструктор не переписывает, про такой случай зовущий говорит вслух.
    """
    line, dbm, dbs = split_key(key)
    if group_id:
        name, path = group_id, group_dag_path(group_id)
    else:
        name, path = group_dag_of(key)
    if not path or not os.path.exists(path):
        return None
    parsed = parse_group_dag(path)
    if parsed is None:
        return None
    lines = [t for t in parsed["lines"] if t != (line, dbm, dbs)]
    content = build_group_dag_py(
        name, lines, parsed["tags"],
        build_schedule_expr({"schedule_kind": parsed["schedule_kind"],
                             "schedule_minutes": parsed["schedule_minutes"],
                             "schedule_cron": parsed["schedule_cron"]}),
        parsed["retry_mode"], parsed.get("note"))
    with _group_writable():
        with open(path, "w", encoding="utf-8") as fp:
            fp.write(content)
    return path


def build_all(spec):
    """spec -> список (относительный_путь, содержимое). Ничего не пишет на диск.

    Обязательные ключи spec:
      table_master, table_slave, db_master, db_slave,
      master_cols, slave_cols (списки словарей из snap_structure),
      pairs (список (master_name, slave_name)),
      period_column, slave_period_column.
    Необязательные:
      line_name (по умолч. bare(table_master) в регистре ведущей БД:
                 Oracle — ВЕРХНИЙ, Postgres — нижний, см. to_db_case),
      dag_id    (по умолч. CamelCase(line_name)+db_master+db_slave),
      mode ('iud' по умолч.), tags, schedule_minutes, retry_mode,
      doc (строка _doc), extra (dict доп. ключей конфига: filterClause,
           filterClauseSlave, selectSql, conflictExtra, conflictWhere,
           truncatePeriod, etlFields ...).
    """
    tm, ts = spec["table_master"], spec["table_slave"]
    dbm, dbs = spec["db_master"], spec["db_slave"]
    m_bare, s_bare = bare(tm), bare(ts)
    # Имя линии = tableNameEtlJobs = значение tablename в etl_jobs /
    # etl_log_iud_row, и оно же префикс ключа конфига. Сравнение с этой колонкой
    # в SQL РЕГИСТРОЗАВИСИМОЕ, а пишет её триггер ведущей — то есть регистр
    # должен быть регистром ВЕДУЩЕЙ БД (Oracle — ВЕРХНИЙ, Postgres — нижний).
    # Безусловный .lower() здесь давал для oracle-ведущих ключ вида
    # 'medree_consOrclPost' при том, что даг спрашивает 'MEDREE_CONSOrclPost':
    # перенос падал с «Конфиг для ключа ... не найден», а аудит (он берёт имя
    # из ключа) молча не находил ни одной группы в etl_jobs.
    line = spec.get("line_name") or to_db_case(m_bare, dbm)
    dag_id = spec.get("dag_id") or default_dag_id(line, dbm, dbs)
    key = f"{line}{dbm}{dbs}"

    m_out, s_out = _ordered_pair(spec["master_cols"], spec["slave_cols"], spec["pairs"])
    if not m_out:
        raise ValueError("Не сопоставлено ни одной колонки.")
    if len(m_out) != len(s_out):
        raise ValueError("Внутренняя ошибка: длины master/slave не равны.")

    # Первичный ключ: master — источник истины, ведомая колонка той же позиции
    # помечается так же (составной PK = несколько помеченных колонок).
    for mc, sc in zip(m_out, s_out):
        is_pk = "Primary Key" if mc.get("is_primary_key") else None
        mc["is_primary_key"] = is_pk
        sc["is_primary_key"] = is_pk

    # В режиме правки сохраняем ИСХОДНЫЕ пути структур (для таблиц с несколькими
    # линиями имена кастомные — иначе файлы уехали бы не туда). В новом — по схеме.
    struct_dir = f"structures/{m_bare}"
    master_struct_rel = spec.get("struct_master_rel") or f"{struct_dir}/{m_bare}.json"
    slave_struct_rel = spec.get("struct_slave_rel") or f"{struct_dir}/{s_bare}.json"

    out_files = []
    group_id = (spec.get("group_dag_id") or "").strip()

    # Ключи-умолчания (tableNameMaster = имя линии, mode = "iud") в конфигах
    # написаны не везде: рантайм подставляет их сам (do_etl setdefault), и
    # корпус ровно пополам — 17 конфигов с tableNameMaster, 12 без. Значит
    # решать, писать ли ключ, должен САМ конфиг: был — останется, не было — не
    # появится. Иначе предпросмотр линии, в которой ничего не трогали, показывал
    # правку конфига, и человеку приходилось гадать, что именно он задел.
    # У новой линии прошлого нет — там ключи пишутся явно, как и раньше.
    had = spec.get("config_keys")
    explicit = (lambda name: True) if had is None else (lambda name: name in had)

    fragment = {key: {}}
    body = fragment[key]
    if spec.get("doc"):
        body["_doc"] = spec["doc"]
    if tm != line or explicit("tableNameMaster"):
        body["tableNameMaster"] = tm
    body["tableNameSlave"] = ts
    body["structureMaster"] = master_struct_rel
    body["structureSlave"] = slave_struct_rel
    # Пустой период не пишем ВОВСЕ: рантайм подставляет 'createdate' через
    # setdefault, а он не срабатывает на явном null — в SQL уехало бы
    # COALESCE(None, ...). Нет значения — пусть работает умолчание.
    if spec.get("period_column"):
        body["periodColumn"] = spec["period_column"]
    if spec.get("slave_period_column"):
        body["slavePeriodColumn"] = spec["slave_period_column"]
    mode = spec.get("mode") or "iud"
    if mode != "iud" or explicit("mode"):
        body["mode"] = mode
    # Принадлежность линии составному дагу — в её же конфиге: по нему
    # конструктор потом узнаёт, что своего дага у линии нет (см. group_dag_of).
    if group_id:
        body[GROUP_DAG_KEY] = re.sub(r"[^A-Za-z0-9_]", "", group_id)
    for k, v in (spec.get("extra") or {}).items():
        if v not in (None, "", (), [], {}):
            body[k] = v

    # SQL-запрос ведущей: пользователь вставляет ТЕКСТ — мы сами создаём .sql
    # и прописываем путь к нему в selectSql (а не просим указать готовый файл).
    # Текст сохраняется как есть, нормализуется только хвост: отступ ПЕРВОЙ
    # строки тоже принадлежит запросу (в MOCHECK.sql ветки UNION сдвинуты), а
    # общий .strip() его срезал — и правка любой линии показывала «изменение»
    # первой строки общего файла.
    sql_text = spec.get("select_sql_text") or ""
    if sql_text.strip():
        # ПУТЬ ФАЙЛА В РЕЖИМЕ ПРАВКИ БЕРЁТСЯ ИЗ ЛИНИИ, а не выводится из её
        # имени — ровно как struct_master_rel/struct_slave_rel выше, и по той
        # же причине: запрос бывает ОБЩИМ на несколько линий.
        #
        # У линий mocheck источник один — queries/customQueries/MOCHECK.sql, и
        # правка запроса обязана менять его всем (это описано в README как
        # свойство схемы). Имя же, выведенное из линии, дало бы для EXPMED23
        # файл queries/customQueries/EXPMED23.sql: предпросмотр предлагал
        # создать ЧАСТНУЮ КОПИЮ общего запроса и переставить конфиг на неё.
        # Линия после такой записи переставала следовать правкам MOCHECK.sql,
        # причём молча — расхождение всплыло бы только на данных.
        sql_rel = spec.get("select_sql_rel") or spec.get("select_sql")
        if not sql_rel:
            sql_name = re.sub(r"[^A-Za-z0-9_]", "",
                              spec.get("select_sql_name") or line) or line
            sql_rel = f"queries/customQueries/{sql_name}.sql"
        out_files.append((f"etlFolder/{sql_rel}", sql_text.rstrip() + "\n"))
        body["selectSql"] = sql_rel

    # periodsSql — ТОЛЬКО для режима query_section: список групп для перезаливки
    # берёт из него один этот режим (см. do_etl._runQuerySection), остальные
    # читают журнал/сравнивают срезы. Поэтому в других режимах ключ не пишется и
    # файл не создаётся, даже если текст в форме остался от предыдущего режима.
    periods_text = spec.get("periods_sql_text") or ""
    if mode == "query_section":
        if not periods_text.strip():
            raise ValueError(
                "Режим query_section: не заполнен «SQL периодов» — без него линии "
                "неоткуда взять список групп для перезаливки.")
        # тот же принцип, что и с selectSql: путь существующей линии важнее
        # имени, выведенного из её названия
        periods_rel = spec.get("periods_sql_rel") or spec.get("periods_sql")
        if not periods_rel:
            pname = re.sub(r"[^A-Za-z0-9_]", "",
                           spec.get("periods_sql_name") or f"{line}_periods") \
                or f"{line}_periods"
            periods_rel = f"queries/customQueries/{pname}.sql"
        out_files.append((f"etlFolder/{periods_rel}", periods_text.rstrip() + "\n"))
        body["periodsSql"] = periods_rel

    # DDL триггера ведущей. Сам триггер живёт в БД (его ставит кнопка «Создать
    # триггер»), а файл — версионируемая копия: видно, что именно поставлено, и
    # можно открыть в DBeaver. Текст готовит UI (tools/trigger_builder), ядро
    # только пишет его рядом с линией.
    trigger_text = (spec.get("trigger_sql_text") or "").strip()
    if trigger_text:
        out_files.append((f"etlFolder/{trigger_sql_rel(key)}",
                          trigger_text + ("" if trigger_text.endswith("\n") else "\n")))

    tags = spec.get("tags") or [f"{dbm}{dbs}", line, "DbSync"]
    if group_id:
        # Линия живёт в СОСТАВНОМ даге: своего файла у неё нет, вместо него
        # переписывается список линий общего дага (расписание и теги — его,
        # а не линии: у дага они одни на всех).
        dag_rel, dag_py = _group_dag_file(group_id, line, dbm, dbs, tags, spec)
    else:
        # Правим СУЩЕСТВУЮЩИЙ файл дага, если он известен: имя файла и dag_id
        # внутри совпадают не всегда (dags/IpersonOrclPost.py объявляет
        # dag_id="IpersonPostOrcl"), и вывод пути из dag_id создал бы ВТОРОЙ
        # файл с тем же dag_id — Airflow на такую пару ругается и не берёт ни
        # один из них. Смена dag_id в форме сбрасывает эту привязку (UI шлёт
        # dag_file_rel только пока имя дага не трогали).
        dag_rel = spec.get("dag_file_rel") or f"dags/{dag_id}.py"
        # Свой даг под именем СОСТАВНОГО затёр бы его вместе со всеми линиями,
        # которые в нём перечислены. Это не гипотеза: имя составного дага
        # подставляется в поле dag_id само, стоит открыть такую линию в правке.
        exists = os.path.join(ROOT, dag_rel)
        other = parse_group_dag(exists) if os.path.exists(exists) else None
        if other:
            raise ValueError(
                f"{dag_rel} — составной даг ({len(other['lines'])} "
                f"линий: {', '.join(l for l, _m, _s in other['lines'])}). Своим "
                f"дагом линии он стать не может — файл был бы затёрт. Либо "
                f"выбери «В составной даг», либо задай другой dag_id.")
        dag_py = build_dag_py(dag_id, line, tm, dbm, dbs, tags,
                              build_schedule_expr(spec),
                              spec.get("retry_mode", "frequent"),
                              spec.get("note"), spec.get("task_id"))

    out_files += [
        (f"etlFolder/{master_struct_rel}", _struct_json(m_out, dbm)),
        (f"etlFolder/{slave_struct_rel}", _struct_json(s_out, dbs)),
        (f"etlFolder/config.d/{key}.json",
         json.dumps(fragment, ensure_ascii=False, indent=2) + "\n"),
        (dag_rel, dag_py),
    ]
    return out_files


def _group_dag_file(group_id, line, dbm, dbs, tags, spec):
    """(путь, содержимое) составного дага с добавленной линией."""
    group_id = re.sub(r"[^A-Za-z0-9_]", "", group_id)
    if not group_id:
        raise ValueError("Пустое имя составного дага.")
    path = os.path.join(_dags_dir(), f"{group_id}.py")
    parsed = parse_group_dag(path) if os.path.exists(path) else None
    if os.path.exists(path) and parsed is None:
        raise ValueError(
            f"Даг dags/{group_id}.py написан руками — конструктор его не "
            f"переписывает, чтобы не потерять вашу логику. Добавьте линию в "
            f"него сами (в списке линий дага: ('{line}', '{dbm}', '{dbs}')), а "
            f"здесь выберите другой даг или сборку со своим дагом.")
    lines = list(parsed["lines"]) if parsed else []
    if (line, dbm, dbs) not in lines:
        lines.append((line, dbm, dbs))
    if parsed:
        # Расписание/теги/ретраи у составного дага общие — берём его, а не формы:
        # иначе правка одной линии молча меняла бы поведение всех остальных.
        # Заметка человека в docstring по той же причине переезжает как есть.
        schedule_expr = build_schedule_expr(parsed)
        content = build_group_dag_py(group_id, lines, parsed["tags"],
                                     schedule_expr, parsed["retry_mode"],
                                     parsed.get("note"))
    else:
        content = build_group_dag_py(group_id, lines, tags,
                                     build_schedule_expr(spec),
                                     spec.get("retry_mode", "frequent"))
    return f"dags/{group_id}.py", content


def trigger_sql_rel(key):
    """Путь (относительно etlFolder) к версионируемой копии DDL триггера линии."""
    return f"queries/triggers/{key}.sql"


def default_dag_id(line, dbm, dbs):
    """Имя DAG по умолчанию: имя линии без не-буквенно-цифровых символов,
    первая буква заглавная, плюс направление. Напр. prbdir+Post+Orcl -> PrbdirPostOrcl."""
    return re.sub(r"[^a-z0-9]", "", line.lower()).capitalize() + dbm + dbs


def default_period_column(cols):
    """Колонка-период по умолчанию. Без учёта регистра (Oracle отдаёт ИМЕНА в
    ВЕРХНЕМ регистре, поэтому прямой поиск 'createdate' не находил 'CREATEDATE'):
    createdate -> первая колонка с типом дата/время -> первая с 'date' в имени ->
    первая колонка."""
    if not cols:
        return None
    for c in cols:
        if c["column_name"].lower() == "createdate":
            return c["column_name"]
    for c in cols:
        if any(w in str(c.get("data_type") or "").lower() for w in ("date", "timestamp")):
            return c["column_name"]
    for c in cols:
        if "date" in c["column_name"].lower():
            return c["column_name"]
    return cols[0]["column_name"]


# ─────────────────────── существующие линии / теги (для UI) ───────────────────────

_DB_NAMES = ("Post", "Orcl")


def split_key(key):
    """'prbdirPostOrcl' -> ('prbdir', 'Post', 'Orcl'). Направление кодируется
    суффиксом из двух фиксированных имён БД."""
    for dbm in _DB_NAMES:
        for dbs in _DB_NAMES:
            suf = dbm + dbs
            if key.endswith(suf) and len(key) > len(suf):
                return key[:-len(suf)], dbm, dbs
    return key, "Post", "Orcl"


def _dag_files():
    d = os.path.join(ROOT, "dags")
    if not os.path.isdir(d):
        return []
    return [os.path.join(d, f) for f in sorted(os.listdir(d))
            if f.endswith(".py") and not f.startswith("__")]


def existing_tags():
    """Все теги из dags/*.py — чтобы UI подсказывал уже использованные.
    Новый тег попадёт сюда автоматически после сохранения дага с этим тегом."""
    tags = set()
    pat = re.compile(r"tags\s*=\s*\[(.*?)\]", re.S)
    for f in _dag_files():
        try:
            txt = _read_text(f)
        except OSError:
            continue
        m = pat.search(txt)
        if m:
            tags.update(re.findall(r"""['"]([^'"]+)['"]""", m.group(1)))
    return sorted(tags)


def existing_lines():
    """Имена всех линий (ключей) из etlFolder/config.d/*.json — для режима правки."""
    d = os.path.join(ETLFOLDER, "config.d")
    keys = []
    if os.path.isdir(d):
        for f in sorted(os.listdir(d)):
            if not f.endswith(".json"):
                continue
            try:
                obj = _read_json(os.path.join(d, f))
            except Exception:
                continue
            keys.extend(obj.keys())
    return sorted(keys)


def _find_config_body(key):
    d = os.path.join(ETLFOLDER, "config.d")
    for f in sorted(os.listdir(d)):
        if not f.endswith(".json"):
            continue
        path = os.path.join(d, f)
        try:
            obj = _read_json(path)
        except Exception:
            continue
        if key in obj:
            return obj[key], path
    raise KeyError(f"Линия '{key}' не найдена в config.d.")


def _norm_col(c):
    def g(*ks):
        for k in ks:
            if k in c:
                return c[k]
        return None
    return {
        "column_name": g("column_name", "COLUMN_NAME"),
        "data_type": g("data_type", "DATA_TYPE"),
        "data_scale": g("data_scale", "DATA_SCALE"),
        "is_primary_key": "Primary Key"
        if g("is_primary_key", "IS_PRIMARY_KEY") == "Primary Key" else None,
    }


def _cols_from_struct(rel):
    obj = _read_json(os.path.join(ETLFOLDER, rel))
    return [_norm_col(c) for c in obj.get("data", [])]


ARCHIVE_DIRNAME = "_archived"


def _dags_dir():
    return os.path.join(ROOT, "dags")


def _archive_dir():
    return os.path.join(_dags_dir(), ARCHIVE_DIRNAME)


def _all_dag_files():
    """Файлы дагов и в dags/, и в архиве dags/_archived/."""
    files = list(_dag_files())
    ad = _archive_dir()
    if os.path.isdir(ad):
        files += [os.path.join(ad, f) for f in sorted(os.listdir(ad))
                  if f.endswith(".py") and not f.startswith("__")]
    return files


def _resolve_dag_path(line, table_master, dbm, dbs):
    """Найти РЕАЛЬНЫЙ файл дага линии (а не угадать по формуле) — иначе правка
    дага, названного не по формуле (другой регистр и т.п.), создала бы дубль.
    Ищет и в dags/, и в архиве. Возвращает (path, dag_id, archived)."""
    cand = default_dag_id(line, dbm, dbs)
    for base, arch in ((_dags_dir(), False), (_archive_dir(), True)):
        p = os.path.join(base, f"{cand}.py")
        if os.path.exists(p):
            return p, cand, arch
    p_line = re.compile(r'tableNameEtlJobs\s*=\s*[\'"]%s[\'"]' % re.escape(line))
    p_tm = re.compile(r'tableNameMaster\s*=\s*[\'"]%s[\'"]' % re.escape(table_master))
    p_dbm = re.compile(r'dbMaster\s*=\s*[\'"]%s[\'"]' % re.escape(dbm))
    p_dbs = re.compile(r'dbSlave\s*=\s*[\'"]%s[\'"]' % re.escape(dbs))
    fallback = None
    for f in _all_dag_files():
        try:
            txt = _read_text(f)
        except OSError:
            continue
        name = os.path.splitext(os.path.basename(f))[0]
        arch = os.path.dirname(os.path.abspath(f)) == os.path.abspath(_archive_dir())
        if p_line.search(txt):
            return f, name, arch
        if fallback is None and p_tm.search(txt) and p_dbm.search(txt) and p_dbs.search(txt):
            fallback = (f, name, arch)
    return fallback or (os.path.join(_dags_dir(), f"{cand}.py"), cand, False)


def _parse_dag_file(path):
    """Расписание / retryMode / теги / заметка из файла дага (best-effort)."""
    res = {"schedule_kind": "interval", "schedule_minutes": 1, "schedule_cron": "",
           "retry_mode": "frequent", "tags": [], "note": "",
           "dag_id": "", "task_id": ""}
    try:
        txt = _read_text(path)
    except OSError:
        return res
    # dag_id берём ИЗ ФАЙЛА, а не из его имени. Они совпадают не всегда:
    # dags/IpersonOrclPost.py объявляет dag_id="IpersonPostOrcl". Для Airflow
    # dag_id — это идентификатор задачи со всей её историей запусков, и подмена
    # его именем файла при перезаписи завела бы НОВЫЙ даг, а прежний остался бы
    # висеть без расписания.
    m = re.search(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", txt)
    if m:
        res["dag_id"] = m.group(1)
    # Имя задачи — тоже из файла и по той же причине: на нём висит история
    # запусков. Берём первое: у дага одной линии задача одна (составной даг
    # разбирается отдельно, через LINES).
    # Закомментированные куски в дагах есть (в ReqprepmoMocheckPostPost лежит
    # старый вариант вызова), и принять имя задачи из комментария значило бы
    # закрепить то, чего в даге нет.
    live = "\n".join(l for l in txt.splitlines() if not l.lstrip().startswith("#"))
    m = re.search(r"makeEtlOperator\(\s*['\"]([^'\"]+)['\"]", live)
    if m:
        res["task_id"] = m.group(1)
    m = re.search(r"retryMode\s*=\s*['\"](\w+)['\"]", txt)
    if m:
        res["retry_mode"] = m.group(1)
    m = re.search(r"tags\s*=\s*\[(.*?)\]", txt, re.S)
    if m:
        res["tags"] = re.findall(r"""['"]([^'"]+)['"]""", m.group(1))
    m = re.search(r"schedule_interval\s*=\s*(.+),\s*$", txt, re.M)
    if m:
        expr = m.group(1).strip()
        tmd = re.search(r"timedelta\(minutes\s*=\s*(\d+)\)", expr)
        if tmd:
            res["schedule_kind"], res["schedule_minutes"] = "interval", int(tmd.group(1))
        else:
            res["schedule_kind"] = "cron"
            res["schedule_cron"] = expr.strip().strip("'\"")
    # Заметка пользователя из docstring — её надо вернуть в build_all, иначе
    # пересборка линии затрёт рукописное описание шаблонной строкой.
    try:
        import ast
        res["note"] = _own_dag_note(ast.parse(txt))
    except Exception:
        pass
    return res


def load_line(key):
    """Собрать спецификацию существующей линии для заполнения формы (режим правки).
    Колонки и сопоставление берутся из сохранённых structures (в их парном порядке);
    расписание/retry/теги — из файла дага."""
    line, dbm, dbs = split_key(key)
    body, _ = _find_config_body(key)
    master_cols = _cols_from_struct(body["structureMaster"])
    slave_cols = _cols_from_struct(body["structureSlave"])
    pairs = list(zip([c["column_name"] for c in master_cols],
                     [c["column_name"] for c in slave_cols]))
    table_master = body.get("tableNameMaster", line)
    # Линия может жить в СОСТАВНОМ даге — тогда расписание, теги и ретраи у неё
    # общие с остальными линиями этого дага, и брать их надо оттуда.
    group_id, group_path = group_dag_of(key)
    if group_path:
        # dag_id у линии в составном даге ПУСТОЙ, а не имя этого дага: поле
        # формы «dag_id» — про СВОЙ даг линии. С именем составного там линия,
        # переключённая на «свой даг», записала бы свой даг поверх общего —
        # и вместе с ним потеряла бы все остальные линии этого дага.
        dag_path, dag_id, archived = group_path, "", False
    else:
        dag_path, dag_id, archived = _resolve_dag_path(line, table_master, dbm, dbs)
    sched = _parse_dag_file(dag_path)
    # Файл дага и dag_id внутри него — два РАЗНЫХ факта, и оба надо сохранить:
    # имя файла определяет, какой файл перезапишется, dag_id — какую задачу
    # Airflow при этом продолжит вести. У архивных путь не закрепляем: правка
    # архивной линии по-прежнему кладёт даг в dags/ (это и есть восстановление).
    dag_file_rel, task_id = "", ""
    if dag_id and os.path.exists(dag_path):
        task_id = sched.get("task_id", "")
        if not archived:
            dag_id = sched.get("dag_id") or dag_id
            dag_file_rel = os.path.relpath(dag_path, ROOT).replace(os.sep, "/")

    # extra — ВСЁ, что конфиг знает сверх полей формы, а не заранее известный
    # список. Список молча терял ключи, которых в нём нет: у трёх отключённых
    # линий стоит "disabled": true, и перезапись конфига (правка любого поля,
    # хоть комментария) снимала флаг — линия включалась обратно сама, без
    # единого слова в предпросмотре. Здесь любой ключ, который ставит не форма,
    # переживает пересборку, включая те, что появятся позже.
    extra = {k: v for k, v in body.items() if k not in _FORM_CONFIG_KEYS}
    select_sql = body.get("selectSql")
    select_sql_text = ""
    if select_sql:
        try:
            select_sql_text = _read_text(os.path.join(ETLFOLDER, select_sql))
        except OSError:
            select_sql_text = ""
    periods_sql = body.get("periodsSql")
    periods_sql_text = ""
    if periods_sql:
        try:
            periods_sql_text = _read_text(os.path.join(ETLFOLDER, periods_sql))
        except OSError:
            periods_sql_text = ""

    return {
        "key": key, "line_name": line, "dag_id": dag_id,
        "dag_file_rel": dag_file_rel, "task_id": task_id,
        # какие ключи конфиг РЕАЛЬНО содержит — чтобы пересборка не дописывала
        # умолчания в конфиг, который прекрасно жил без них
        "config_keys": sorted(body),
        "group_dag_id": group_id,
        "table_master": body.get("tableNameMaster", line),
        "table_slave": body.get("tableNameSlave", ""),
        "db_master": dbm, "db_slave": dbs,
        "mode": body.get("mode", "iud"),
        "period_column": body.get("periodColumn"),
        "slave_period_column": body.get("slavePeriodColumn"),
        "doc": body.get("_doc", ""),
        "master_cols": master_cols, "slave_cols": slave_cols, "pairs": pairs,
        "struct_master_rel": body["structureMaster"],
        "struct_slave_rel": body["structureSlave"],
        "extra": extra,
        "select_sql": select_sql, "select_sql_text": select_sql_text,
        "periods_sql": periods_sql, "periods_sql_text": periods_sql_text,
        "tags": sched["tags"], "retry_mode": sched["retry_mode"],
        "note": sched.get("note", ""),
        "schedule_kind": sched["schedule_kind"],
        "schedule_minutes": sched["schedule_minutes"],
        "schedule_cron": sched["schedule_cron"],
    }


# ─────────────────────── архив дагов (скрыть / восстановить) ───────────────────────
# «Архив» = убрать даг из видимости Airflow без удаления: файл дага переезжает в
# dags/_archived/, который Airflow не парсит (через dags/.airflowignore), плюс на
# линии ставится skipAudit (иначе AuditDag продолжал бы её аудировать). Восстановление
# возвращает файл назад и снимает skipAudit. Конфиг и структуры остаются на месте.

def ensure_airflowignore():
    """Гарантировать, что Airflow игнорирует папку архива (dags/.airflowignore)."""
    path = os.path.join(_dags_dir(), ".airflowignore")
    rule = ARCHIVE_DIRNAME + "/"
    lines = []
    if os.path.exists(path):
        lines = _read_text(path).splitlines()
        if any(l.strip() in (rule, ARCHIVE_DIRNAME) for l in lines):
            return path
    with open(path, "a", encoding="utf-8") as fp:
        if lines and lines[-1].strip():
            fp.write("\n")
        fp.write(rule + "\n")
    return path


def _set_skip_audit(key, value):
    """Проставить (True) или снять (False) skipAudit у линии в её config.d-файле."""
    body, path = _find_config_body(key)
    obj = _read_json(path)
    if value:
        obj[key]["skipAudit"] = True
    else:
        obj[key].pop("skipAudit", None)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(obj, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def _set_line_flag(key, name, value):
    """Проставить (True) или снять (False) булев флаг у линии в config.d."""
    body, path = _find_config_body(key)
    obj = _read_json(path)
    if value:
        obj[key][name] = True
    else:
        obj[key].pop(name, None)
    with _group_writable():
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(obj, fp, ensure_ascii=False, indent=2)
            fp.write("\n")


def line_disabled(key):
    """Линия отключена флагом disabled (для линий без собственного файла дага —
    их обслуживает общий даг-итератор, например MocheckOrclPost)."""
    body, _ = _find_config_body(key)
    return bool(body.get("disabled"))


def _line_dag_info(key):
    body, _ = _find_config_body(key)
    line, dbm, dbs = split_key(key)
    tm = body.get("tableNameMaster", line)
    return _resolve_dag_path(line, tm, dbm, dbs)  # (path, dag_id, archived)


def _is_own_dag_file(path, line, table_master=None, dbm=None, dbs=None):
    """Файл дага обслуживает ТОЛЬКО эту линию — его можно удалить при переезде
    линии в составной даг.

    Проверка нужна, потому что _resolve_dag_path умеет отвечать «похожим»
    файлом (совпали ведущая и направление), а сносить чужой даг из-за похожести
    нельзя. Опознаём сборку конструктора: ровно один ВЫЗОВ makeEtlOperator (имя
    встречается ещё и в импорте), не составной даг, и линия названа явно —
    через tableNameEtlJobs либо, когда его нет (даг сделан до появления ключа),
    через tableNameMaster с тем же направлением."""
    try:
        txt = _read_text(path)
    except OSError:
        return False
    if GROUP_MARK in txt or txt.count("makeEtlOperator(") != 1:
        return False
    if re.search(r'tableNameEtlJobs\s*=', txt):
        return bool(re.search(
            r'tableNameEtlJobs\s*=\s*[\'"]%s[\'"]' % re.escape(line), txt))
    if not table_master:
        return False
    return all(re.search(r'%s\s*=\s*[\'"]%s[\'"]' % (k, re.escape(v)), txt)
               for k, v in (("tableNameMaster", table_master),
                            ("dbMaster", dbm or ""), ("dbSlave", dbs or "")))


def line_placement(key):
    """Где сейчас живёт даг линии: (составной даг | None, путь своего дага | None).

    Свой даг возвращается, только если файл существует и обслуживает ровно эту
    линию (см. _is_own_dag_file)."""
    gname, _gpath = group_dag_of(key)
    if gname:
        return gname, None
    try:
        body, _ = _find_config_body(key)
        path, _dag_id, _arch = _line_dag_info(key)
    except (KeyError, OSError, ValueError):
        return None, None
    line, dbm, dbs = split_key(key)
    if os.path.exists(path) and _is_own_dag_file(
            path, line, body.get("tableNameMaster", line), dbm, dbs):
        return None, path
    return None, None


def retire_old_placement(key, old_group, old_own_dag, new_group):
    """Убрать следы ПРЕЖНЕГО размещения линии после смены «свой даг ↔ составной».

    Сборка линии знает только про новое размещение: она допишет линию в
    составной даг либо создаст ей свой. Прежнее при этом никуда не девается —
    и линия начинает переноситься ДВАЖДЫ, из двух дагов сразу. Здесь прежнее и
    вычёркивается: линия убирается из старого составного дага, а свой даг
    удаляется, если линия уехала в составной.

    Возвращает список описаний сделанного (пусто — размещение не менялось)."""
    notes = []
    new_group = (new_group or "").strip()
    old_group = (old_group or "").strip()
    if old_group and old_group != new_group:
        path = group_dag_drop_line(key, old_group)
        if path:
            notes.append(f"{os.path.relpath(path, ROOT)}: линия убрана из списка")
        else:
            notes.append(
                f"dags/{old_group}.py: конструктор его не переписывает (нет "
                f"файла либо он не нашего формата) — убери строку линии оттуда "
                f"сам, иначе она будет переноситься дважды")
    if new_group:
        if old_own_dag and os.path.exists(old_own_dag):
            with _group_writable():
                os.remove(old_own_dag)
            notes.append(f"{os.path.relpath(old_own_dag, ROOT)}: удалён — линия "
                         f"переехала в составной даг {new_group}")
        else:
            # Файл дага у линии был, но он не только про неё (или написан не
            # конструктором) — удалять такой нельзя, а молчать нельзя тем более.
            try:
                path, _dag_id, _arch = _line_dag_info(key)
            except (KeyError, OSError, ValueError):
                path = None
            if path and os.path.exists(path):
                notes.append(
                    f"{os.path.relpath(path, ROOT)}: остался на месте — "
                    f"конструктор его не разбирает. Проверь, не гоняет ли он ту "
                    f"же линию: теперь она есть и в {new_group}")
    return notes


def line_has_own_dag(key):
    """Есть ли у линии СВОЙ файл дага. У линий mocheck-семейства (PODCHECK3 и
    т.п.) его нет — они перечислены внутри общего дага-итератора, поэтому
    архивируются флагом disabled, а не переносом файла."""
    path, _dag_id, archived = _line_dag_info(key)
    return os.path.exists(path) or archived


def list_archived_lines():
    """Линии, убранные из работы: даг в архиве ЛИБО стоит disabled."""
    out = []
    for k in existing_lines():
        try:
            if _line_dag_info(k)[2] or line_disabled(k):
                out.append(k)
        except Exception:
            continue
    return out


def list_active_lines():
    """Линии в работе: даг не в архиве и нет disabled."""
    archived = set(list_archived_lines())
    return [k for k in existing_lines() if k not in archived]


def archive_line(key):
    """Убрать линию из работы БЕЗ удаления. Возвращает описание того, что сделано.

    Если у линии есть свой файл дага — он уезжает в dags/_archived/ (Airflow его
    не парсит). Если своего дага нет (линия перечислена в общем даге-итераторе,
    как PODCHECK3 в MocheckOrclPost) — ставится флаг `disabled`, и даг-итератор
    такую линию пропускает. В обоих случаях дополнительно ставится skipAudit.
    """
    path, dag_id, archived = _line_dag_info(key)
    if archived or line_disabled(key):
        raise ValueError(f"Линия '{key}' уже убрана из работы.")
    if not os.path.exists(path):
        # своего дага нет — отключаем флагом (иначе линию нельзя было убрать)
        with _group_writable():
            _set_line_flag(key, "disabled", True)
            _set_skip_audit(key, True)
        return f"{key} (disabled; свой даг отсутствует — линия в общем даге)"
    with _group_writable():
        os.makedirs(_archive_dir(), exist_ok=True)
        ensure_airflowignore()
        os.replace(path, os.path.join(_archive_dir(), f"{dag_id}.py"))
        _set_skip_audit(key, True)
    return dag_id


def restore_line(key):
    """Вернуть линию в работу (из архива / снять disabled) + снять skipAudit."""
    path, dag_id, archived = _line_dag_info(key)
    if line_disabled(key):
        with _group_writable():
            _set_line_flag(key, "disabled", False)
            _set_skip_audit(key, False)
        return f"{key} (снят disabled)"
    if not archived:
        raise ValueError(f"Линия '{key}' не в архиве.")
    dst = os.path.join(_dags_dir(), f"{dag_id}.py")
    if os.path.exists(dst):
        raise FileExistsError(f"Активный даг с таким именем уже есть: dags/{dag_id}.py")
    with _group_writable():
        os.replace(path, dst)
        _set_skip_audit(key, False)
    return dag_id


def _same_on_disk(path, content):
    """Лежит ли на диске то же САМОЕ — с точностью до незначащего.

    Сравнение не побайтовое, иначе «ничего не менял, а переписалось всё»:
      * json (структуры, фрагменты конфига) — сравниваем разобранное значение.
        Структуры mocheck набиты руками табами и пробелами вокруг двоеточий;
        генератор пишет json.dumps(indent=2), и правка ОДНОЙ линии
        переформатировала бы общий файл целиком, показывая в diff 150 строк
        «изменений», которых нет;
      * .sql — сравниваем с точностью до РАСКЛАДКИ: переводы строк и отступы в
        одном операторе ничего не значат, а различаются постоянно. Файлы
        queries/sp/*/Select.sql писались прежним форматом генератора (весь
        список колонок одной строкой), нынешний пишет по колонке на строку —
        и пересборка любой линии справочника показывала «изменилось 2 файла»
        там, где не изменилось ничего. Пятьдесят семь линий из пятидесяти
        восьми выглядели грязными, а правка одного поля переписывала боевые
        .sql чужим форматированием.
        Значащие правки при этом видны: пробелы схлопываются между лексемами,
        но не внутри них, поэтому `a=1` и `a = 1` остаются разными текстами.
    """
    if not os.path.exists(path):
        return False
    try:
        old = _read_text(path)
    except OSError:
        return False
    if old == content:
        return True
    if path.endswith(".json"):
        try:
            return json.loads(old) == json.loads(content)
        except ValueError:
            return False
    if path.endswith(".sql"):
        return " ".join(old.split()) == " ".join(content.split())
    return False


def unchanged_files(files):
    """Из [(relpath, content)] — те, что на диске УЖЕ такие же.

    Спрашивать надо ДО write_files: после записи «пропущенные» от «записанных»
    не отличить. UI печатает этот список, чтобы правка линии была видна ровно
    в том объёме, в каком она сделана."""
    return [rel for rel, content in files
            if _same_on_disk(os.path.join(ROOT, rel), content)]


def write_files(files, overwrite=False, validate="config", force=(),
                skip_unchanged=True):
    """Записать [(relpath, content)] под ROOT. Без overwrite не трогает
    существующие файлы (чтобы не затереть чужую линию). Возвращает список
    записанных абсолютных путей. В конце валидирует сборку конфига.

    validate: имя конфига для проверки сборки после записи — 'config'
    (сложный ETL), 'SpTableName' (справочники) или 'SpOnce' (разовый перенос).
    None — не валидировать.
    force: пути, которые перезаписываются ВСЕГДА (составной даг: новая линия
    дописывается в существующий файл — это не «затирание чужого»).
    skip_unchanged: файл, содержимое которого уже совпадает с диском, НЕ
    переписывается и в результат не попадает. Это и есть «правка меняет только
    изменённое»: сборка всегда выдаёт ПОЛНЫЙ комплект файлов линии, и без этого
    правка одного поля трогала дату у структур, дага и общего SQL, а в коммит
    линии попадали файлы, в которых ничего не менялось (для общих — MOCHECK.sql,
    структуры mocheck — это ещё и лишний повод для конфликта с чужой правкой).
    Сравнение текстовое: файл, сохранённый в другой кодировке, но с тем же
    текстом, считается неизменившимся и остаётся в своей кодировке."""
    written = []
    force = set(force or ())
    with _group_writable():
        for rel, content in files:
            path = os.path.join(ROOT, rel)
            if os.path.exists(path):
                if not overwrite and rel not in force:
                    raise FileExistsError(
                        f"Файл уже существует: {rel}. Включи overwrite или "
                        f"выбери другое имя."
                    )
                if skip_unchanged and _same_on_disk(path, content):
                    continue
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fp:
                fp.write(content)
            written.append(path)

    # Валидация: конфиг должен собраться (ловит дубль ключа линии / битый json)
    if validate:
        os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
        import sys
        sys.path.insert(0, ROOT)
        from Functions.functionsFile.loadConfig import assemble
        assemble(validate)
    return written


# ─────────────────────── приведение имени к диалекту БД ───────────────────────

def to_db_case(name, db):
    """Привести имя таблицы к регистру диалекта: Oracle — ВЕРХНИЙ, Postgres —
    нижний. Регистр меняется по каждой части через точку (схема и имя отдельно),
    сами разделители и содержимое кавычек не трогаются.

    'spmkb' + Orcl -> 'SPMKB';  'KOKNAEV.SPMKB' + Post -> 'koknaev.spmkb'.
    Кавычки сохраняем как есть (в них регистр значим — не корёжим).

    То же правило живёт в интерфейсе (tools/webui/src/dbCase.js) — там оно
    стоит за кнопкой «привести к регистру БД». Расходиться им нельзя: имя
    линии, полученное этим правилом, сравнивается с etl_jobs.tablename
    ДОСЛОВНО. Набор проверочных случаев один на обе стороны, см. _selftest.
    """
    name = (name or "").strip()
    if not name:
        return ""
    f = str.upper if db == "Orcl" else str.lower
    out = []
    for part in name.split("."):
        # Открывающей кавычки достаточно: имя набирают вживую, и пока вторая
        # кавычка не поставлена, портить набранное нельзя. Незакрытая кавычка
        # в SQL всё равно ошибка — поднимать регистр в ней незачем.
        if part.startswith('"'):
            out.append(part)          # кавычки -> регистр значим, не меняем
        else:
            out.append(f(part))
    return ".".join(out)


# ─────────────────────────── git: коммит и пуш ───────────────────────────

def current_branch(root=ROOT):
    """Текущая ветка рабочего клона (или None, если detached HEAD/не git)."""
    import subprocess
    try:
        p = subprocess.run(["git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"],
                           capture_output=True, text=True)
    except OSError:
        return None
    br = (p.stdout or "").strip()
    return br if (p.returncode == 0 and br and br != "HEAD") else None


GIT_AREAS = ("etlFolder", "dags")   # что конструктор трогает — этим и ограничиваем git


def _git_runner(root):
    """Вернуть функцию run(*git-args) -> (rc, output) с безопасным окружением.

    GIT_TERMINAL_PROMPT=0: не зависать на интерактивном запросе логина/пароля
    (веб-сессия Voilà) — вместо этого git сразу падает с понятной ошибкой.
    """
    import subprocess
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")

    def run(*args, timeout=120):
        try:
            p = subprocess.run(["git", "-C", root, *args], capture_output=True,
                               text=True, env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, (f"git {args[0]}: превышен таймаут ({timeout}с) — вероятно, "
                         "git ждёт учётные данные. Настрой креды для origin у "
                         "пользователя сервиса или выложи обычным деплоем.")
        return p.returncode, ((p.stdout or "") + (p.stderr or "")).strip()

    return run


def _commit_and_push(run, branch, message):
    """Коммит уже проиндексированного + push. Возвращает (ok, log)."""
    log = []
    rc, out = run("commit", "-m", message)
    if rc != 0:
        # Нечего коммитить — не ошибка (проиндексированное идентично истории).
        low = out.lower()
        if any(s in low for s in ("nothing to commit", "no changes added to commit",
                                  "nothing added to commit", "ничего для коммита")):
            return True, "git: коммитить нечего (изменений нет)."
        return False, f"git commit не удался:\n{out}"
    log.append(f"git commit: {out.splitlines()[0] if out else 'ok'}")

    rc, out = run("push", "origin", "HEAD")
    if rc != 0:
        return False, ("\n".join(log) + f"\n\ngit push НЕ удался (ветка {branch}):\n{out}"
                       "\n\nИзменения сохранены локально — выложи их обычным деплоем "
                       "(local/dev-push.sh / deploy-test.sh).")
    log.append(f"git push origin {branch}: ok\n{out}".rstrip())
    return True, "\n".join(log)


def git_status_short(root=ROOT, areas=GIT_AREAS):
    """Краткий статус (`git status --porcelain`) по областям конструктора —
    что реально изменено на диске и попадёт в «просто запушить». '' если чисто."""
    run = _git_runner(root)
    rc, out = run("status", "--porcelain", "--", *areas)
    return out if rc == 0 else ""


def git_commit_push(paths, message, root=ROOT, areas=GIT_AREAS, include_saved=True):
    """Проиндексировать файлы линии и запушить текущую ветку в origin.

    include_saved=True (умолчание) — вместе с новыми файлами коммитятся ВСЕ уже
    сохранённые изменения в областях конструктора (etlFolder/, dags/), то есть
    ровно то же, что берёт «Просто запушить».

    ПОЧЕМУ ТАК. Раньше индексировались ТОЛЬКО пути текущей формы. Типовой
    сценарий — собрать несколько связанных линий («Создать файлы» на каждой) и
    выложить их одним коммитом — молча ломался: уезжала лишь последняя линия, а
    предыдущие оставались несохранёнными в рабочей копии. Дальше их не видел ни
    dev-pull на ПК, ни хук деплоя (он обновляет Jupyter-клон только ff-only,
    а грязная копия это блокирует), и обнаруживались они уже как «несохранённые
    правки» с предложением их затереть.

    Что уедет — показывает диалог подтверждения в UI (git_status_short), поэтому
    случайно прихватить чужую незакоммиченную работу в общем клоне нельзя:
    список видно до нажатия «Да».

    include_saved=False — прежнее узкое поведение (только перечисленные пути).

    Возвращает (ok, log). Ошибки НЕ бросаются — текст возвращается для показа в
    UI (на сервере у пользователя сервиса могут быть или не быть креды к origin).
    """
    run = _git_runner(root)
    branch = current_branch(root)
    if not branch:
        return False, ("git: не удалось определить ветку (detached HEAD или это не "
                       "git-клон). Закоммить/запушь вручную.")
    if include_saved:
        rc, out = run("add", "-A", "--", *areas)
        if rc != 0:
            return False, f"git add не удался:\n{out}"
    # Пути формы индексируем явно и в этом случае: они почти всегда лежат внутри
    # areas (тогда это no-op), но линия может ссылаться на файл вне их.
    rel = [os.path.relpath(p, root) for p in paths]
    if rel:
        rc, out = run("add", "--", *rel)
        if rc != 0:
            return False, f"git add не удался:\n{out}"
    return _commit_and_push(run, branch, message)


def git_push_saved(message, root=ROOT, areas=GIT_AREAS):
    """«Просто запушить»: закоммитить и запушить ВСЕ уже сохранённые на диске
    изменения в областях конструктора (add -A по etlFolder/ и dags/), НЕ трогая
    несохранённую форму. Возвращает (ok, log)."""
    run = _git_runner(root)
    branch = current_branch(root)
    if not branch:
        return False, ("git: не удалось определить ветку (detached HEAD или это не "
                       "git-клон). Закоммить/запушь вручную.")
    rc, out = run("add", "-A", "--", *areas)
    if rc != 0:
        return False, f"git add не удался:\n{out}"
    return _commit_and_push(run, branch, message)


# ─────────────────────────── удаление линии насовсем ───────────────────────────

def _all_config_bodies():
    """key -> body по всем фрагментам config.d (для проверки общих файлов)."""
    d = os.path.join(ETLFOLDER, "config.d")
    out = {}
    if os.path.isdir(d):
        for f in sorted(os.listdir(d)):
            if not f.endswith(".json"):
                continue
            try:
                obj = _read_json(os.path.join(d, f))
            except Exception:
                continue
            out.update(obj)
    return out


def _ref_shared(rel, exclude_key):
    """Ссылается ли на файл `rel` (структура/selectSql) ещё какая-то линия, кроме
    exclude_key. Структуры общие у таблицы с несколькими направлениями — их нельзя
    удалять, пока жива хоть одна линия."""
    if not rel:
        return False
    for k, body in _all_config_bodies().items():
        if k == exclude_key:
            continue
        if rel in (body.get("structureMaster"), body.get("structureSlave"),
                   body.get("selectSql"), body.get("periodsSql")):
            return True
    return False


def shared_line_files(key, rels):
    """Кто ЕЩЁ ссылается на эти файлы линии. -> {rel: [ключи других линий]}.

    Структуры, свой SELECT и SQL периодов у линий одного источника общие
    (у mocheck на пяти линиях один MOCHECK.sql и одна пара структур). Правка
    такого файла из формы одной линии достаётся всем сразу — и это ровно то,
    ради чего его и правят, но сказать об этом надо вслух: молча переписанный
    общий запрос выглядит как «поменял у себя»."""
    bodies = _all_config_bodies()
    out = {}
    for rel in rels:
        if not rel:
            continue
        users = sorted(k for k, body in bodies.items()
                       if k != key and rel in (body.get("structureMaster"),
                                               body.get("structureSlave"),
                                               body.get("selectSql"),
                                               body.get("periodsSql")))
        if users:
            out[rel] = users
    return out


def line_delete_targets(key):
    """Список путей, которые удалит delete_line(key) — для предпросмотра в UI."""
    body, path = _find_config_body(key)
    line, dbm, dbs = split_key(key)
    tm = body.get("tableNameMaster", line)
    dagpath, _dag_id, _arch = _resolve_dag_path(line, tm, dbm, dbs)
    obj = _read_json(path)
    targets = [path if len(obj) <= 1 else f"{path} (ключ {key})"]
    gname, gpath = group_dag_of(key)
    if gname:
        targets.append(
            f"{gpath} (линия будет убрана из списка, файл останется)"
            if parse_group_dag(gpath) else
            f"{gpath} — НЕ нашего формата: строку линии убери сам")
    elif os.path.exists(dagpath):
        targets.append(dagpath)
    for rel in (body.get("selectSql"), body.get("periodsSql"),
                trigger_sql_rel(key),
                body.get("structureMaster"), body.get("structureSlave")):
        if rel and not _ref_shared(rel, key):
            p = os.path.join(ETLFOLDER, rel)
            if os.path.exists(p):
                targets.append(p)
    return targets


# ─────────────────────────── переименование линии ───────────────────────────
# Имя линии (tableNameEtlJobs) — это НЕ подпись на форме. Оно живёт в трёх
# местах сразу, и два из них в базе:
#
#   файлы   ключ фрагмента config.d, значение tableNameEtlJobs в даге, имя DDL
#           триггера;
#   БД      триггер ведущей ПИШЕТ это имя в etl_log_iud_row.tablename, а перенос
#           и аудит ищут по нему же в etl_jobs.tablename. Сравнение дословное.
#
# Отсюда правило: переименование — это МИГРАЦИЯ, а не правка поля. Поменяй
# только файлы — и линия замолчит: триггер продолжит писать старое имя, перенос
# станет искать новое и находить ноль строк. Ошибки при этом не будет ни одной,
# что хуже всего.
#
# Поэтому rename_plan отдаёт сразу три вещи: новые файлы, список того, что надо
# снести, и SQL для базы. Ни одну из них нельзя делать в отрыве от остальных.

def rename_plan(key, new_line=None, new_dag_id=None):
    """Что произойдёт при переименовании линии. Ничего не меняет.

    -> {new_key, files, remove, sql, warnings, needs_trigger}
    """
    line, dbm, dbs = split_key(key)
    spec = load_line(key)
    new_line = (new_line or line).strip()
    if not new_line:
        raise ValueError("Пустое имя линии.")
    if new_line == line and (new_dag_id or spec.get("dag_id")) == spec.get("dag_id"):
        raise ValueError("Ни имя линии, ни dag_id не изменились — переименовывать нечего.")

    new_key = f"{new_line}{dbm}{dbs}"
    if new_key != key and new_key in set(existing_lines()):
        raise ValueError(f"Линия {new_key} уже есть — выберите другое имя.")

    spec = dict(spec, line_name=new_line)
    if new_dag_id is not None and new_dag_id.strip():
        if new_dag_id.strip() != spec.get("dag_id"):
            # файл дага поедет под новым именем — прежнюю привязку снимаем
            spec = dict(spec, dag_id=new_dag_id.strip(), dag_file_rel="")
    # DDL триггера назван по ключу линии: под новым ключом это новый файл.
    spec.pop("trigger_sql_text", None)

    files = build_all(spec)

    # ── что снести ───────────────────────────────────────────────────────────
    body, path = _find_config_body(key)
    obj = _read_json(path)
    remove = [f"{_relroot(path)}" if len(obj) <= 1 else f"{_relroot(path)} (ключ {key})"]

    gname, gpath = group_dag_of(key)
    old_dag = None
    if not gname:
        old_dag, _id, _arch = _resolve_dag_path(line, spec["table_master"], dbm, dbs)
        new_dag_rel = next((rel for rel, _c in files if rel.startswith("dags/")), None)
        if old_dag and os.path.exists(old_dag) and \
                _relroot(old_dag) != new_dag_rel:
            remove.append(_relroot(old_dag))
        else:
            old_dag = None

    old_trigger = os.path.join(ETLFOLDER, trigger_sql_rel(key))
    if new_key != key and os.path.exists(old_trigger):
        remove.append(_relroot(old_trigger))

    # ── что сделать в БД ─────────────────────────────────────────────────────
    # Импорт ЛЕНИВЫЙ: trigger_builder импортирует нас, и наверху это был бы
    # цикл. Здесь модуль уже загружен целиком, поэтому всё честно.
    from tools import trigger_builder as T

    warnings, sql = [], ""
    if new_key != key:
        jobs = T.JOBS_DEFAULT[dbm]
        journal = T.JOURNAL_DEFAULT[dbm]
        sql = (
            f"-- Переименование линии {line} -> {new_line} ({dbm} -> {dbs}).\n"
            f"-- Выполнять В ВЕДУЩЕЙ БД ({dbm}), одной транзакцией, ПОСЛЕ записи файлов\n"
            f"-- и ДО следующего запуска дага.\n"
            f"--\n"
            f"-- 1. Группы переноса. Без этого section-режимы и аудит не найдут\n"
            f"--    ни одной группы: сравнение с tablename дословное.\n"
            f"UPDATE {jobs}\n"
            f"   SET tablename = '{new_line}'\n"
            f" WHERE tablename = '{line}';\n"
            f"\n"
            f"-- 2. Непереваренные события журнала (isetl = 0). Оставить под старым\n"
            f"--    именем — значит потерять правки, которые ещё не перенесены.\n"
            f"UPDATE {journal}\n"
            f"   SET tablename = '{new_line}'\n"
            f" WHERE tablename = '{line}';\n"
            f"\n"
            f"COMMIT;\n"
        )
        warnings.append(
            f"ТРИГГЕР ведущей пишет имя линии в журнал жёстко строкой. Пока он не "
            f"пересоздан под '{new_line}', новые правки будут уходить под старым "
            f"именем, а перенос — искать новое и находить ноль. Ошибки при этом "
            f"не будет: линия просто замолчит. DDL берите на вкладке «Триггеры».")
    if gname:
        warnings.append(
            f"Линия в составном даге {gname}: в его списке LINES она "
            f"переписывается, файл дага общий и не трогается.")
    if spec.get("dag_id") and old_dag:
        warnings.append(
            f"История запусков в Airflow останется за прежним дагом: dag_id — это "
            f"идентификатор задачи, а не подпись. Прежний файл будет удалён, "
            f"поэтому даг исчезнет из списка вместе со своими запусками.")

    return {"new_key": new_key,
            "files": files,
            "remove": remove,
            "sql": sql,
            "warnings": warnings,
            "needs_trigger": bool(sql) and T.needs_trigger(spec.get("mode", "iud"))}


def rename_apply(key, files, remove):
    """Записать новые файлы и снести перечисленное старое. Возвращает отчёт.

    Порядок важен: сначала ПИШЕМ, потом удаляем. Упади запись на полпути —
    старая линия ещё цела, и вернуться есть куда."""
    written = write_files(_files_as_pairs(files), overwrite=True)
    dropped = []
    line, dbm, dbs = split_key(key)
    with _group_writable():
        for rel in remove:
            rel = rel.split(" (")[0]
            path = os.path.join(ROOT, rel)
            if rel.endswith(".json") and "config.d" in rel:
                obj = _read_json(path) if os.path.exists(path) else {}
                if key in obj and len(obj) > 1:
                    obj.pop(key)
                    with open(path, "w", encoding="utf-8") as fp:
                        json.dump(obj, fp, ensure_ascii=False, indent=2)
                        fp.write("\n")
                    dropped.append(f"{rel} (ключ {key})")
                    continue
            if os.path.exists(path):
                os.remove(path)
                dropped.append(rel)
    gname, _gpath = group_dag_of(key)
    if gname:
        group_dag_drop_line(key, gname)
        dropped.append(f"{gname}: старая строка убрана из списка линий")
    return {"written": [_relroot(p) for p in written], "removed": dropped}


def _files_as_pairs(files):
    """[[rel, content]] из JSON -> [(rel, content)]."""
    return [(f[0], f[1]) if isinstance(f, (list, tuple)) else (f["path"], f["content"])
            for f in files]


def _relroot(path):
    """Путь относительно корня репозитория."""
    try:
        return os.path.relpath(path, ROOT).replace(os.sep, "/")
    except ValueError:
        return path


def delete_line(key, remove_struct=True):
    """Удалить линию сложного ETL НАСОВСЕМ: фрагмент config.d, файл дага (из dags/
    или архива) и — если не используются другими линиями — selectSql и структуры.
    Возвращает список удалённых путей. Пустые каталоги структур прибираются."""
    body, path = _find_config_body(key)
    line, dbm, dbs = split_key(key)
    tm = body.get("tableNameMaster", line)
    dagpath, _dag_id, _arch = _resolve_dag_path(line, tm, dbm, dbs)
    # Спрашиваем ДО правки конфига: принадлежность составному дагу записана в
    # нём же (ключ groupDag), и после удаления фрагмента её было бы не узнать.
    gname, gpath = group_dag_of(key)
    obj = _read_json(path)
    removed = []
    with _group_writable():
        if len(obj) <= 1:
            os.remove(path)
            removed.append(path)
        else:
            obj.pop(key, None)
            with open(path, "w", encoding="utf-8") as fp:
                json.dump(obj, fp, ensure_ascii=False, indent=2)
                fp.write("\n")
            removed.append(f"{path} (ключ {key})")
        # Линия из составного дага: файл общий, удалять его нельзя — вычёркиваем
        # только её строку из списка линий.
        if gname:
            dropped = group_dag_drop_line(key, gname)
            removed.append(f"{dropped} (линия убрана из списка)" if dropped else
                           f"{gpath} — не нашего формата, строку линии убери сам")
        elif os.path.exists(dagpath):
            os.remove(dagpath)
            removed.append(dagpath)
        # periodsSql и DDL триггера принадлежат линии (имя файла = ключ линии),
        # но _ref_shared всё равно спросим — вдруг на них ссылается ещё кто-то.
        refs = [body.get("selectSql"), body.get("periodsSql"), trigger_sql_rel(key)]
        if remove_struct:
            refs += [body.get("structureMaster"), body.get("structureSlave")]
        for rel in refs:
            if rel and not _ref_shared(rel, key):
                p = os.path.join(ETLFOLDER, rel)
                if os.path.exists(p):
                    os.remove(p)
                    removed.append(p)
                parent = os.path.dirname(p)
                try:
                    if os.path.isdir(parent) and not os.listdir(parent):
                        os.rmdir(parent)
                        removed.append(parent)
                except OSError:
                    pass
    return removed


# ─────────────────────────── самопроверка (без БД) ───────────────────────────

def _selftest():
    master_cols = [
        {"column_name": "id", "data_type": "numeric", "data_scale": 0, "is_primary_key": "Primary Key"},
        {"column_name": "name_full", "data_type": "character varying", "data_scale": None, "is_primary_key": None},
        {"column_name": "createdate", "data_type": "date", "data_scale": None, "is_primary_key": None},
    ]
    # snap_structure всегда отдаёт ключи в нижнем регистре (внутреннее
    # представление); имена колонок Oracle при этом сами по себе в верхнем.
    slave_cols = [
        {"column_name": "ID", "data_type": "NUMBER", "data_scale": 0, "is_primary_key": "Primary Key"},
        {"column_name": "FULLNAME", "data_type": "VARCHAR2", "data_scale": None, "is_primary_key": None},
        {"column_name": "CREATEDATE", "data_type": "DATE", "data_scale": None, "is_primary_key": None},
    ]
    sugg, unmatched = auto_match(master_cols, slave_cols)
    assert sugg == ["ID", None, "CREATEDATE"], sugg          # name_full != FULLNAME
    assert unmatched == ["FULLNAME"], unmatched
    # человек поправил: name_full -> FULLNAME
    pairs = [("id", "ID"), ("name_full", "FULLNAME"), ("createdate", "CREATEDATE")]
    spec = {
        "table_master": "demo", "table_slave": "KOKNAEV.DEMO",
        "db_master": "Post", "db_slave": "Orcl",
        "master_cols": master_cols, "slave_cols": slave_cols, "pairs": pairs,
        "period_column": "createdate", "slave_period_column": "CREATEDATE",
        "doc": "demo Post->Orcl",
    }
    files = build_all(spec)
    got = {rel for rel, _ in files}
    assert got == {
        "etlFolder/structures/demo/demo.json",
        "etlFolder/structures/demo/DEMO.json",
        "etlFolder/config.d/demoPostOrcl.json",
        "dags/DemoPostOrcl.py",
    }, got
    frag = json.loads(dict(files)["etlFolder/config.d/demoPostOrcl.json"])
    assert "demoPostOrcl" in frag
    assert frag["demoPostOrcl"]["structureMaster"] == "structures/demo/demo.json"
    assert frag["demoPostOrcl"]["periodColumn"] == "createdate"
    # порядок ведомой согласован с ведущей
    slave_written = json.loads(dict(files)["etlFolder/structures/demo/DEMO.json"])
    assert [c["COLUMN_NAME"] for c in slave_written["data"]] == ["ID", "FULLNAME", "CREATEDATE"]
    dag = dict(files)["dags/DemoPostOrcl.py"]
    assert 'dag_id="DemoPostOrcl"' in dag
    assert 'tableNameMaster="demo"' in dag and 'tableNameEtlJobs="demo"' in dag

    # periodsSql пишется ТОЛЬКО в режиме query_section
    spec_iud = dict(spec, periods_sql_text="SELECT year, month FROM t")
    assert not [rel for rel, _ in build_all(spec_iud) if "periods" in rel]
    assert "periodsSql" not in json.loads(
        dict(build_all(spec_iud))["etlFolder/config.d/demoPostOrcl.json"])["demoPostOrcl"]
    spec_qs = dict(spec_iud, mode="query_section")
    files_qs = dict(build_all(spec_qs))
    assert "etlFolder/queries/customQueries/demo_periods.sql" in files_qs, files_qs.keys()
    assert json.loads(files_qs["etlFolder/config.d/demoPostOrcl.json"]) \
        ["demoPostOrcl"]["periodsSql"] == "queries/customQueries/demo_periods.sql"
    # query_section без SQL периодов — понятная ошибка, а не молча битая линия
    try:
        build_all(dict(spec, mode="query_section"))
        raise AssertionError("ожидалась ошибка про пустой SQL периодов")
    except ValueError as e:
        assert "query_section" in str(e)

    # DDL триггера кладётся рядом с линией
    files_trg = dict(build_all(dict(spec, trigger_sql_text="CREATE OR REPLACE TRIGGER x")))
    assert files_trg["etlFolder/queries/triggers/demoPostOrcl.sql"] == \
        "CREATE OR REPLACE TRIGGER x\n"

    # структура по запросу: тип-константа помечается как неопределённый,
    # PK подмешивается из структуры таблицы
    qcols = [{"column_name": "id", "data_type": "numeric", "data_scale": None,
              "is_primary_key": None},
             {"column_name": "checkdir", "data_type": "unknown", "data_scale": None,
              "is_primary_key": None}]
    assert not unknown_type("numeric") and unknown_type("unknown") and unknown_type("")
    merged = merge_table_pk(qcols, master_cols)
    assert merged[0]["is_primary_key"] == "Primary Key"   # id — PK таблицы
    assert merged[1]["is_primary_key"] is None            # checkdir'а в таблице нет
    assert merged[0]["data_type"] == "numeric"             # тип запроса не подменён

    # составной даг: своего файла у линии нет, вместо него — общий со списком
    files_g = dict(build_all(dict(spec, group_dag_id="MyGroupPostOrcl")))
    assert "dags/DemoPostOrcl.py" not in files_g, files_g.keys()
    gdag = files_g["dags/MyGroupPostOrcl.py"]
    assert "('demo', 'Post', 'Orcl')," in gdag, gdag
    assert 'dag_id="MyGroupPostOrcl"' in gdag
    assert "lineEnabled(line, dbm, dbs)" in gdag
    # принадлежность даге записана в конфиге линии — по ней её потом и находим
    assert json.loads(files_g["etlFolder/config.d/demoPostOrcl.json"]) \
        ["demoPostOrcl"][GROUP_DAG_KEY] == "MyGroupPostOrcl"
    assert GROUP_DAG_KEY not in json.loads(
        dict(build_all(spec))["etlFolder/config.d/demoPostOrcl.json"])["demoPostOrcl"]
    # …и он читается обратно, чтобы дописать в него следующую линию
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        gpath = os.path.join(tmp, "MyGroupPostOrcl.py")
        with open(gpath, "w", encoding="utf-8") as fp:
            fp.write(gdag)
        parsed = parse_group_dag(gpath)
        assert parsed and parsed["lines"] == [("demo", "Post", "Orcl")], parsed
        assert parsed["note"] == ""
        with open(gpath, "w", encoding="utf-8") as fp:
            fp.write("# руками написанный даг\nmakeEtlOperator()\n")
        assert parse_group_dag(gpath) is None    # чужой формат не трогаем

        # заметка человека переживает перезапись дословно
        note = "Тут про doctype:\n  * 1 — MEDCHECK;\n  * 2,3,4 — EXPMED."
        with open(gpath, "w", encoding="utf-8") as fp:
            fp.write(build_group_dag_py("MyGroupPostOrcl",
                                        [("demo", "Post", "Orcl")],
                                        ["a"], note=note))
        parsed = parse_group_dag(gpath)
        assert parsed["note"] == note, parsed["note"]
        again = build_group_dag_py("MyGroupPostOrcl", parsed["lines"],
                                   parsed["tags"], build_schedule_expr(parsed),
                                   parsed["retry_mode"], parsed["note"])
        assert again == _read_text(gpath)        # круг замкнулся байт в байт
        # маркер внутри самой заметки не должен «съесть» её при следующем разборе
        with open(gpath, "w", encoding="utf-8") as fp:
            fp.write(build_group_dag_py("MyGroupPostOrcl", parsed["lines"], ["a"],
                                        note=f"до {GROUP_NOTE_MARK} после"))
        assert parse_group_dag(gpath)["note"] == "до  после"

    # запись: файл с тем же содержимым не переписывается
    with tempfile.TemporaryDirectory() as tmp:
        rel = "etlFolder/structures/demo/demo.json"
        p = os.path.join(tmp, rel)
        os.makedirs(os.path.dirname(p))
        with open(p, "w", encoding="utf-8") as fp:
            fp.write("СТАРОЕ")
        global ROOT
        root_was, ROOT = ROOT, tmp
        try:
            assert unchanged_files([(rel, "СТАРОЕ")]) == [rel]
            assert unchanged_files([(rel, "НОВОЕ")]) == []
            assert write_files([(rel, "СТАРОЕ")], overwrite=True,
                               validate=None) == []
            assert write_files([(rel, "НОВОЕ")], overwrite=True,
                               validate=None) == [p]
            assert _read_text(p) == "НОВОЕ"
        finally:
            ROOT = root_was

    # ── ПЕРЕСБОРКА НЕ ПРИДУМЫВАЕТ ПРАВОК ─────────────────────────────────────
    # Открыть линию и нажать «Предпросмотр», ничего не тронув, обязано давать
    # «менять нечего». Каждая проверка ниже — про случай, где это ломалось, и
    # ломалось молча: файл менялся, а понять, что именно ты задел, было нельзя.

    # dag_id и task_id держат ИСТОРИЮ ЗАПУСКОВ Airflow и берутся из файла, а
    # не выводятся из имени линии
    dag_txt = build_dag_py("IpersonPostOrcl", "iperson", "iperson", "Post", "Orcl",
                           ["PostOrcl"], task_id="do_etl_iperson")
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "IpersonOrclPost.py")   # имя файла ≠ dag_id, так бывает
        with open(p, "w", encoding="utf-8") as fp:
            fp.write(dag_txt)
        got = _parse_dag_file(p)
        assert got["dag_id"] == "IpersonPostOrcl", got
        assert got["task_id"] == "do_etl_iperson", got
        # закомментированный вызов не должен подменять живое имя задачи
        with open(p, "a", encoding="utf-8") as fp:
            fp.write('    #task = makeEtlOperator("do_etl_OLD")\n')
        assert _parse_dag_file(p)["task_id"] == "do_etl_iperson"
    # без подсказки имя задачи по-прежнему выводится из линии
    assert '"do_etl_iperson"' in build_dag_py("X", "iperson", "iperson", "Post",
                                              "Orcl", [])

    # ключи-умолчания пишутся ровно так, как их держит сам конфиг
    base = dict(spec, mode="iud", table_master="demo", line_name="demo")
    lean = dict(build_all(dict(base, config_keys=["tableNameSlave"])))
    body = json.loads(lean["etlFolder/config.d/demoPostOrcl.json"])["demoPostOrcl"]
    assert "mode" not in body and "tableNameMaster" not in body, body
    full = dict(build_all(dict(base, config_keys=["mode", "tableNameMaster"])))
    body = json.loads(full["etlFolder/config.d/demoPostOrcl.json"])["demoPostOrcl"]
    assert body["mode"] == "iud" and body["tableNameMaster"] == "demo", body
    # у новой линии прошлого нет — там ключи пишутся явно
    body = json.loads(dict(build_all(base))["etlFolder/config.d/demoPostOrcl.json"])
    assert body["demoPostOrcl"]["mode"] == "iud"
    # неумолчальное значение пишется всегда
    body = json.loads(dict(build_all(dict(base, mode="section", config_keys=[])))
                      ["etlFolder/config.d/demoPostOrcl.json"])
    assert body["demoPostOrcl"]["mode"] == "section"

    # чужие ключи конфига (disabled!) проходят насквозь: перезапись конфига
    # отключённой линии не смеет её включить
    body = json.loads(dict(build_all(dict(base, extra={"disabled": True})))
                      ["etlFolder/config.d/demoPostOrcl.json"])
    assert body["demoPostOrcl"]["disabled"] is True, body
    assert "disabled" not in _FORM_CONFIG_KEYS

    # ── приведение имени к диалекту БД ───────────────────────────────────────
    # Этот же набор случаев проверяет интерфейс (tools/webui/src/dbCase.js):
    # правило одно, и разойтись сторонам нельзя — имя линии, полученное им,
    # сравнивается с etl_jobs.tablename дословно.
    for name, db, want in (
        ("spmkb", "Orcl", "SPMKB"),
        ("SPMKB", "Post", "spmkb"),
        ("KOKNAEV.SPMKB", "Post", "koknaev.spmkb"),
        ("koknaev.iprkdept", "Orcl", "KOKNAEV.IPRKDEPT"),
        ('sch."MixedCase"', "Orcl", 'SCH."MixedCase"'),
        ('sch."MixedCase"', "Post", 'sch."MixedCase"'),
        # незакрытая кавычка: имя набирают вживую, и до второй кавычки
        # содержимое трогать нельзя
        ('sch."Mix', "Orcl", 'SCH."Mix'),
        ("  spmkb  ", "Orcl", "SPMKB"),
        ("", "Orcl", ""),
        (None, "Post", ""),
    ):
        assert to_db_case(name, db) == want, (name, db, to_db_case(name, db))
    print("selftest OK")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
