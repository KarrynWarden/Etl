#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ядро генератора ETL-линии (без UI).

Делает три вещи, на которые раньше уходила ручная работа:
  1. snap_structure() — снимает структуру таблицы прямо из БД (тем же
     StructureCheck-запросом, что и рантайм-проверка), в формате json-структур.
  2. auto_match()     — предлагает сопоставление колонок master->slave по имени
     (имена в двух БД могут отличаться — остальное поправит человек в UI).
  3. build_all()/write_files() — генерит из спецификации три артефакта линии:
        etlFolder/structures/<master>/<master>.json   (ведущая, в порядке маппинга)
        etlFolder/structures/<master>/<slave>.json     (ведомая, в ТОМ ЖЕ порядке)
        etlFolder/config.d/<key>.json                  (фрагмент конфига линии)
        dags/<DagId>.py                                (даг)
     Сопоставление колонок — ПОЗИЦИОННОЕ (do_etl сверяет длины и переносит по
     индексу), поэтому оба json пишутся в одном согласованном порядке.

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
                 tags, schedule_expr="dt.timedelta(minutes=1)", retry_mode="frequent"):
    tags_repr = ", ".join(repr(t) for t in tags)
    return f'''"""DAG: {db_master}->{db_slave} для {line_name}."""
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
        "do_etl_{line_name}",
        tableNameMaster="{table_master}", dbMaster="{db_master}", dbSlave="{db_slave}",
        tableNameEtlJobs="{line_name}",
        retryMode="{retry_mode}",
    )
    addFreezeWatcher([task], retryMode="{retry_mode}")
'''


def build_all(spec):
    """spec -> список (относительный_путь, содержимое). Ничего не пишет на диск.

    Обязательные ключи spec:
      table_master, table_slave, db_master, db_slave,
      master_cols, slave_cols (списки словарей из snap_structure),
      pairs (список (master_name, slave_name)),
      period_column, slave_period_column.
    Необязательные:
      line_name (по умолч. bare(table_master).lower()),
      dag_id    (по умолч. CamelCase(line_name)+db_master+db_slave),
      mode ('iud' по умолч.), tags, schedule_minutes, retry_mode,
      doc (строка _doc), extra (dict доп. ключей конфига: filterClause,
           filterClauseSlave, selectSql, conflictExtra, conflictWhere,
           truncatePeriod, etlFields ...).
    """
    tm, ts = spec["table_master"], spec["table_slave"]
    dbm, dbs = spec["db_master"], spec["db_slave"]
    m_bare, s_bare = bare(tm), bare(ts)
    line = spec.get("line_name") or m_bare.lower()
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

    fragment = {key: {}}
    body = fragment[key]
    if spec.get("doc"):
        body["_doc"] = spec["doc"]
    body["tableNameMaster"] = tm
    body["tableNameSlave"] = ts
    body["structureMaster"] = master_struct_rel
    body["structureSlave"] = slave_struct_rel
    body["periodColumn"] = spec["period_column"]
    body["slavePeriodColumn"] = spec["slave_period_column"]
    body["mode"] = spec.get("mode", "iud")
    for k, v in (spec.get("extra") or {}).items():
        if v not in (None, "", (), [], {}):
            body[k] = v

    # SQL-запрос ведущей: пользователь вставляет ТЕКСТ — мы сами создаём .sql
    # и прописываем путь к нему в selectSql (а не просим указать готовый файл).
    sql_text = (spec.get("select_sql_text") or "").strip()
    if sql_text:
        sql_name = re.sub(r"[^A-Za-z0-9_]", "", spec.get("select_sql_name") or line) or line
        sql_rel = f"queries/customQueries/{sql_name}.sql"
        out_files.append((f"etlFolder/{sql_rel}",
                          sql_text + ("" if sql_text.endswith("\n") else "\n")))
        body["selectSql"] = sql_rel

    tags = spec.get("tags") or [f"{dbm}{dbs}", line, "DbSync"]
    dag_py = build_dag_py(dag_id, line, tm, dbm, dbs, tags,
                          build_schedule_expr(spec),
                          spec.get("retry_mode", "frequent"))

    out_files += [
        (f"etlFolder/{master_struct_rel}", _struct_json(m_out, dbm)),
        (f"etlFolder/{slave_struct_rel}", _struct_json(s_out, dbs)),
        (f"etlFolder/config.d/{key}.json",
         json.dumps(fragment, ensure_ascii=False, indent=2) + "\n"),
        (f"dags/{dag_id}.py", dag_py),
    ]
    return out_files


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
    """Расписание / retryMode / теги из файла дага (best-effort)."""
    res = {"schedule_kind": "interval", "schedule_minutes": 1, "schedule_cron": "",
           "retry_mode": "frequent", "tags": []}
    try:
        txt = _read_text(path)
    except OSError:
        return res
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
    dag_path, dag_id, _arch = _resolve_dag_path(line, table_master, dbm, dbs)
    sched = _parse_dag_file(dag_path)

    extra = {k: body[k] for k in
             ("filterClause", "filterClauseSlave", "conflictExtra",
              "conflictWhere", "truncatePeriod", "auditExcludeFields", "skipAudit")
             if k in body}
    select_sql = body.get("selectSql")
    select_sql_text = ""
    if select_sql:
        try:
            select_sql_text = _read_text(os.path.join(ETLFOLDER, select_sql))
        except OSError:
            select_sql_text = ""

    return {
        "key": key, "line_name": line, "dag_id": dag_id,
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
        "tags": sched["tags"], "retry_mode": sched["retry_mode"],
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


def _line_dag_info(key):
    body, _ = _find_config_body(key)
    line, dbm, dbs = split_key(key)
    tm = body.get("tableNameMaster", line)
    return _resolve_dag_path(line, tm, dbm, dbs)  # (path, dag_id, archived)


def list_archived_lines():
    """Линии (ключи), чей даг сейчас в архиве."""
    return [k for k in existing_lines() if _line_dag_info(k)[2]]


def list_active_lines():
    """Линии (ключи), чей даг активен (не в архиве)."""
    return [k for k in existing_lines() if not _line_dag_info(k)[2]]


def archive_line(key):
    """Убрать даг линии из Airflow (в архив) + skipAudit. Возвращает dag_id."""
    path, dag_id, archived = _line_dag_info(key)
    if archived:
        raise ValueError(f"Линия '{key}' уже в архиве.")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл дага для '{key}' не найден ({path}).")
    with _group_writable():
        os.makedirs(_archive_dir(), exist_ok=True)
        ensure_airflowignore()
        os.replace(path, os.path.join(_archive_dir(), f"{dag_id}.py"))
        _set_skip_audit(key, True)
    return dag_id


def restore_line(key):
    """Вернуть даг линии из архива в Airflow + снять skipAudit. Возвращает dag_id."""
    path, dag_id, archived = _line_dag_info(key)
    if not archived:
        raise ValueError(f"Линия '{key}' не в архиве.")
    dst = os.path.join(_dags_dir(), f"{dag_id}.py")
    if os.path.exists(dst):
        raise FileExistsError(f"Активный даг с таким именем уже есть: dags/{dag_id}.py")
    with _group_writable():
        os.replace(path, dst)
        _set_skip_audit(key, False)
    return dag_id


def write_files(files, overwrite=False):
    """Записать [(relpath, content)] под ROOT. Без overwrite не трогает
    существующие файлы (чтобы не затереть чужую линию). Возвращает список
    записанных абсолютных путей. В конце валидирует сборку конфига."""
    written = []
    with _group_writable():
        for rel, content in files:
            path = os.path.join(ROOT, rel)
            if os.path.exists(path) and not overwrite:
                raise FileExistsError(
                    f"Файл уже существует: {rel}. Включи overwrite или выбери другое имя."
                )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fp:
                fp.write(content)
            written.append(path)

    # Валидация: конфиг должен собраться (ловит дубль ключа линии / битый json)
    os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
    import sys
    sys.path.insert(0, ROOT)
    from Functions.functionsFile.loadConfig import assemble
    assemble("config")
    return written


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
    print("selftest OK")


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
