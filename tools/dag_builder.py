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


def build_dag_py(dag_id, line_name, table_master, db_master, db_slave,
                 tags, schedule_minutes=1, retry_mode="frequent"):
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
    schedule_interval=dt.timedelta(minutes={schedule_minutes}),
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
    dag_id = spec.get("dag_id") or (re.sub(r"[^a-z0-9]", "", line.lower()).capitalize()
                                    + dbm + dbs)
    key = f"{line}{dbm}{dbs}"

    m_out, s_out = _ordered_pair(spec["master_cols"], spec["slave_cols"], spec["pairs"])
    if not m_out:
        raise ValueError("Не сопоставлено ни одной колонки.")
    if len(m_out) != len(s_out):
        raise ValueError("Внутренняя ошибка: длины master/slave не равны.")

    struct_dir = f"structures/{m_bare}"
    master_struct_rel = f"{struct_dir}/{m_bare}.json"
    slave_struct_rel = f"{struct_dir}/{s_bare}.json"

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

    tags = spec.get("tags") or [f"{dbm}{dbs}", line, "DbSync"]
    dag_py = build_dag_py(dag_id, line, tm, dbm, dbs, tags,
                          spec.get("schedule_minutes", 1),
                          spec.get("retry_mode", "frequent"))

    return [
        (f"etlFolder/{master_struct_rel}", _struct_json(m_out, dbm)),
        (f"etlFolder/{slave_struct_rel}", _struct_json(s_out, dbs)),
        (f"etlFolder/config.d/{key}.json",
         json.dumps(fragment, ensure_ascii=False, indent=2) + "\n"),
        (f"dags/{dag_id}.py", dag_py),
    ]


def write_files(files, overwrite=False):
    """Записать [(relpath, content)] под ROOT. Без overwrite не трогает
    существующие файлы (чтобы не затереть чужую линию). Возвращает список
    записанных абсолютных путей. В конце валидирует сборку конфига."""
    written = []
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
