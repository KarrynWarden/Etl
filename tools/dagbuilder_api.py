#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HTTP-API конструктора ETL-линий — замена интерфейсу на ipywidgets/Voilà.

    /opt/jupyter/bin/python tools/dagbuilder_api.py --port 8085
    python3 tools/dagbuilder_api.py --selftest          # без БД и без сервера

Почему tornado, а не Flask/FastAPI. Сервер офлайновый: каждая новая зависимость
это .whl, привезённый флешкой. Tornado уже стоит в окружении JupyterLab
(/opt/jupyter), потому что это его собственный веб-сервер, — значит API не
добавляет к развёртыванию ни одного пакета. Он же раздаёт собранный фронтенд,
так что отдельный веб-сервер тоже не нужен: снаружи всё закрывает тот же
Apache, что сейчас проксирует Voilà.

Устройство. Логика конструктора живёт в tools/dag_builder.py и
tools/trigger_builder.py и про HTTP ничего не знает — здесь только тонкий слой
поверх них: разбор параметров, единый конверт ответа и запуск в пуле потоков.
Поэтому маршрут — обычная функция (params: dict) -> JSON-совместимый объект, а
не класс; их и проверяет --selftest, минуя сеть.

Конверт ответа одинаковый у всех маршрутов:
    успех   200 {"ok": true,  "result": ...}
    ошибка  4xx {"ok": false, "error": "ValueError", "detail": "текст"}
Именно из-за этого затевался переезд: в ноутбуке ошибка уезжала текстом в конец
потока вывода и терялась, а здесь она приходит структурой и показывается там,
где нажали кнопку.

БЕЗОПАСНОСТЬ. Конструктор пишет файлы в репозиторий, делает git push и ходит в
боевые БД. Поэтому по умолчанию слушаем 127.0.0.1: наружу отдаёт Apache, у
которого есть авторизация. Открыть на все интерфейсы можно только явным
--host 0.0.0.0 — чтобы это было решением, а не умолчанием.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import dag_builder as B      # noqa: E402
from tools import trigger_builder as T  # noqa: E402
from tools import sp_builder as SP      # noqa: E402
from tools import git_ops as G          # noqa: E402

logger = logging.getLogger("dagbuilder_api")

# Каталог собранного фронтенда. Собирается на машине с интернетом (npm), сюда
# приезжает готовым — на сервере ни node, ни npm не нужны.
WEBUI_DIST = os.path.join(ROOT, "tools", "webui", "dist")


# ──────────────────────────── разбор параметров ────────────────────────────

class BadRequest(ValueError):
    """Ошибка во ВХОДНЫХ данных — отвечаем 400, а не 500."""


def _need(params, *names):
    """Достать обязательные параметры или сказать, каких не хватает."""
    missing = [n for n in names if params.get(n) in (None, "")]
    if missing:
        raise BadRequest("не заданы обязательные параметры: " + ", ".join(missing))
    return [params[n] for n in names]


def _db(value):
    if value not in ("Orcl", "Post"):
        raise BadRequest(f"db должно быть 'Orcl' или 'Post', получено {value!r}")
    return value


def _pairs(value):
    """[[master, slave], ...] из JSON -> [(master, slave), ...]."""
    if not isinstance(value, list):
        raise BadRequest("pairs должен быть списком пар [master, slave]")
    out = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise BadRequest(f"пара должна быть [master, slave], получено {item!r}")
        out.append((item[0], item[1]))
    return out


def _files(value):
    """[[relpath, content], ...] из JSON -> [(relpath, content), ...]."""
    if not isinstance(value, list):
        raise BadRequest("files должен быть списком пар [путь, содержимое]")
    return [(f[0], f[1]) for f in value]


# ─────────────────────────────── маршруты ───────────────────────────────
# Каждый — (params: dict) -> JSON-совместимый объект. Ни один не знает про
# tornado, поэтому все проверяются вызовом напрямую (см. _selftest).

def _line_summary(key, bodies, archived):
    """Краткие сведения о линии — то, по чему интерфейс фильтрует список.

    Собирается из конфига (дёшево) плюс разбор файла дага ради тегов и
    расписания. Полную спецификацию (структуры, сопоставление) отдаёт /line —
    её тянуть на весь список незачем.
    """
    line, dbm, dbs = B.split_key(key)
    body = bodies.get(key) or {}
    group_id, group_path = B.group_dag_of(key)
    table_master = body.get("tableNameMaster", line)
    if group_path:
        dag_path, own_dag = group_path, None
    else:
        dag_path, _dag_id, _arch = B._resolve_dag_path(line, table_master, dbm, dbs)
        own_dag = _rel(dag_path)
    tags, schedule = [], None
    try:
        sched = B._parse_dag_file(dag_path)
        tags = sched.get("tags") or []
        schedule = {"kind": sched.get("schedule_kind"),
                    "minutes": sched.get("schedule_minutes"),
                    "cron": sched.get("schedule_cron"),
                    "retry_mode": sched.get("retry_mode")}
    except Exception:                       # дага нет / написан руками
        pass
    return {
        "key": key, "line": line,
        "db_master": dbm, "db_slave": dbs,
        "direction": f"{dbm}→{dbs}",
        "table_master": table_master,
        "table_slave": body.get("tableNameSlave", ""),
        "mode": body.get("mode", "iud"),
        "group_dag": group_id or None,
        "own_dag": own_dag,
        "archived": key in archived,
        "disabled": bool(body.get("disabled")),
        "skip_audit": bool(body.get("skipAudit")),
        "needs_trigger": T.needs_trigger(body.get("mode", "iud")),
        "tags": tags,
        "schedule": schedule,
    }


def r_lines(params):
    """Все линии сложного ETL со сведениями для фильтров списка."""
    bodies = B._all_config_bodies()
    archived = set(B.list_archived_lines())
    lines = [_line_summary(k, bodies, archived) for k in B.existing_lines()]
    return {
        "lines": lines,
        "directions": sorted({l["direction"] for l in lines}),
        "modes": sorted({l["mode"] for l in lines}),
        "tags": sorted({t for l in lines for t in l["tags"]}),
    }


def _rel(path):
    """Путь относительно корня репозитория. Абсолютные пути сервера в ответе
    интерфейсу не нужны и только мешают их читать."""
    if not path:
        return path
    try:
        return os.path.relpath(path, ROOT)
    except ValueError:            # другой диск на Windows — отдаём как есть
        return path


def r_line(params):
    """Спецификация линии для формы правки."""
    (key,) = _need(params, "key")
    spec = B.load_line(key)
    group, own = B.line_placement(key)
    return {"spec": spec,
            "placement": {"group_dag": group, "own_dag": _rel(own)}}


def r_tags(params):
    """Теги, уже использованные в дагах — для подсказки."""
    return {"tags": B.existing_tags()}


def r_group_dags(params):
    """Составные даги. Написанные руками помечены parsed=false: конструктор их
    видит, но перезаписывать не станет."""
    out = []
    for dag_id, (path, parsed) in sorted(B.list_group_dags().items()):
        out.append({"dag_id": dag_id, "path": path, "parsed": parsed is not None})
    return {"group_dags": out}


def r_git_status(params):
    """Состояние рабочего клона — его видно в шапке интерфейса."""
    return {"branch": B.current_branch(), "status": B.git_status_short()}


def r_defaults(params):
    """Значения по умолчанию для новой линии."""
    table, dbm, dbs = _need(params, "table", "db_master", "db_slave")
    _db(dbm), _db(dbs)
    line = B.to_db_case(B.bare(table), dbm)
    return {"line_name": line,
            "dag_id": B.default_dag_id(line, dbm, dbs),
            "key": f"{line}{dbm}{dbs}"}


def r_snap_structure(params):
    """Снять структуру таблицы из БД."""
    db, table = _need(params, "db", "table")
    cols = B.snap_structure(_db(db), table, params.get("cred") or "MAIN")
    # У таблицы масштаб и ключ видны достоверно — в отличие от снимка по
    # запросу, см. r_snap_query_structure
    return {"columns": cols,
            "period_column": B.default_period_column(cols),
            "scale_known": True, "from_query": False}


def r_snap_query_structure(params):
    """Снять структуру своего SELECT — типы берутся так же, как их сверяет
    рантайм, поэтому снятая структура проходит проверку как есть.

    Если передана `table` — подмешиваем из неё DATA_SCALE и признак ключа.
    Из описания курсора их не видно ВОВСЕ: snap_query_structure всегда отдаёт
    data_scale=None и is_primary_key=None. Без подмешивания снятие структуры у
    линии со своим запросом выглядело так, будто в базе всё поменялось: у
    EXPMED23 в сохранённой структуре NUMBER(0), а свежий снимок показывал
    NUMBER — и разбор предлагал «поправить» почти каждую колонку. То же с
    ключом: он пропадал у всех колонок разом.
    """
    db, sql = _need(params, "db", "sql")
    db = _db(db)
    cred = params.get("cred") or "MAIN"
    cols = B.snap_query_structure(db, sql, cred)
    table = (params.get("table") or "").strip()
    merged = False
    if table:
        try:
            cols = B.merge_table_pk(cols, B.snap_structure(db, table, cred))
            merged = True
        except Exception as err:
            # Таблицы может не быть вовсе (запрос по нескольким источникам) —
            # это не ошибка снятия, просто масштаб и ключ останутся неизвестны.
            logger.info("подмешать структуру таблицы %s не вышло: %s", table, err)
    unknown = [c for c in cols
               if B.unknown_type(c.get("data_type") or c.get("DATA_TYPE"))]
    return {"columns": cols,
            "period_column": B.default_period_column(cols),
            # Масштаб и ключ достоверны только если их удалось подмешать из
            # таблицы. Интерфейсу это нужно знать: «неизвестно» и «пусто» —
            # разные вещи, и выдавать первое за второе значит показывать
            # изменения там, где их нет.
            "scale_known": merged,
            "from_query": True,
            # Тип константы в запросе драйвер определить не может — такие
            # колонки интерфейс обязан показать и попросить указать тип руками.
            "unknown_types": [c.get("column_name") or c.get("COLUMN_NAME")
                              for c in unknown]}


def r_merge_table_pk(params):
    """Подмешать признак PK из структуры таблицы в колонки запроса."""
    query_cols, table_cols = _need(params, "query_cols", "table_cols")
    return {"columns": B.merge_table_pk(query_cols, table_cols)}


def r_match(params):
    """Предложить сопоставление колонок ведущей и ведомой."""
    master_cols, slave_cols = _need(params, "master_cols", "slave_cols")
    suggestions, unmatched = B.auto_match(master_cols, slave_cols)
    return {"suggestions": suggestions, "slave_unmatched": unmatched}


def _preview_body(files):
    """Тело ответа предпросмотра: собранное + то, что лежит на диске СЕЙЧАС.

    Старое содержимое отдаётся вместе с новым, чтобы интерфейс мог показать
    построчную разницу, а не два полотна текста рядом. Без неё «изменится» —
    это приглашение вычитывать двести строк дага глазами в поисках правки,
    которая на деле в одной строке."""
    out = []
    created = []
    for rel, content in files:
        path = os.path.join(B.ROOT, rel)
        old = None
        if os.path.exists(path):
            try:
                old = B._read_text(path)
            except OSError:
                old = None
        else:
            created.append(rel)
        out.append({"path": rel, "content": content, "old": old})
    return {"files": out, "unchanged": B.unchanged_files(files),
            # Отдельно от «изменится»: у отключённых линий (PODCHECK3,
            # PODCHECK4) файла дага нет вовсе, и пересборка предлагает его
            # СОЗДАТЬ. В общем списке это выглядело как правка существующего.
            "created": created}


def r_preview(params):
    """Собрать файлы линии, НИЧЕГО не записывая.

    Отдельный маршрут от записи намеренно: предпросмотр обязан быть
    безопасным, чтобы в него можно было заглядывать сколько угодно.
    """
    (spec,) = _need(params, "spec")
    spec = dict(spec)
    if "pairs" in spec:
        spec["pairs"] = _pairs(spec["pairs"])
    files = B.build_all(spec)
    return _preview_body(files)


def r_write(params):
    """Записать собранные файлы на диск."""
    (files,) = _need(params, "files")
    files = _files(files)
    written = B.write_files(files,
                            overwrite=bool(params.get("overwrite")),
                            force=tuple(params.get("force") or ()),
                            skip_unchanged=params.get("skip_unchanged", True))
    return {"written": [_rel(p) for p in written],
            "unchanged": B.unchanged_files(files)}


def r_shared_files(params):
    """Кто ЕЩЁ ссылается на файлы линии — предупреждение перед правкой общего."""
    key, rels = _need(params, "key", "rels")
    return {"shared": B.shared_line_files(key, rels)}


def r_delete_targets(params):
    """Что именно удалит удаление линии — для подтверждения."""
    (key,) = _need(params, "key")
    return {"targets": [_rel(p) for p in B.line_delete_targets(key)]}


# Маршрутов «поставить флаг одним запросом» здесь БОЛЬШЕ НЕТ — ни set-flag для
# сложного ETL, ни sp/set-disabled для справочников. Они писали файл на диск
# сразу по щелчку переключателя, и это был единственный способ изменить
# репозиторий мимо связки «Предпросмотр» → «Записать»: задел мышью, не заметил,
# перезагрузил страницу — а линия уже отключена в конфиге, и узнать об этом
# можно было только из git diff.
#
# Оба флага (disabled, skipAudit) — обычные ключи конфига: их собирает
# build_all, они видны в предпросмотре как изменение файла и попадают на диск
# той же кнопкой, что и всё остальное. Функции B._set_line_flag /
# SP.set_sp_disabled остались — ими пользуется прежний интерфейс на Voilà
# (tools/dag_builder_ui.py).


def r_rename_plan(params):
    """Что произойдёт при переименовании линии. НИЧЕГО не меняет.

    Отдаёт три вещи сразу — новые файлы, список того, что снести, и SQL для
    базы, — потому что по отдельности они бессмысленны: имя линии живёт и в
    файлах, и в etl_jobs, и в том, что пишет триггер. Поменяй только файлы, и
    линия замолчит без единой ошибки.
    """
    (key,) = _need(params, "key")
    plan = B.rename_plan(key, params.get("new_line"), params.get("new_dag_id"))
    body = _preview_body(plan["files"])
    return dict(plan, **body)


def r_rename_apply(params):
    """Записать новые файлы линии и снести старые."""
    key, files, remove = _need(params, "key", "files", "remove")
    return B.rename_apply(key, files, remove)


def r_archive(params):
    (key,) = _need(params, "key")
    return {"done": B.archive_line(key)}


def r_restore(params):
    (key,) = _need(params, "key")
    return {"done": B.restore_line(key)}


def r_delete(params):
    (key,) = _need(params, "key")
    return {"done": B.delete_line(key, remove_struct=params.get("remove_struct", True))}


def r_git_push(params):
    """Закоммитить и запушить. paths пуст — пушим всё уже сохранённое."""
    message = params.get("message") or "конструктор: правка линии"
    paths = params.get("paths") or []
    if paths:
        return {"done": B.git_commit_push(paths, message)}
    return {"done": B.git_push_saved(message)}


def r_versions(params):
    """Версии: история ветки конструктора и выкладки прода (теги prod-*).

    Одним ответом обе стороны — вопрос «до какой версии откатить» задают,
    глядя сразу на тест и на прод: что уже уехало, а что ещё нет.
    """
    limit = params.get("limit") or 40
    scope = params.get("scope") or "areas"
    paths = None if scope == "all" else list(G.AREAS)
    return {"branch": B.current_branch(),
            "versions": G.versions(limit=int(limit), paths=paths),
            "prod_tags": G.prod_tags(limit=int(limit))}


def r_rollback_plan(params):
    """Что вернёт откат до версии. Ничего не трогает."""
    (ref,) = _need(params, "ref")
    plan = G.rollback_plan(ref, areas=_rollback_areas(params))
    return dict(_preview_body(plan["files"]),
                ref=plan["ref"], resolved=plan["resolved"],
                subject=plan.get("subject", ""), date=plan.get("date", ""),
                remove=plan["remove"], dirty=plan["dirty"],
                areas=plan["areas"], note=plan.get("note", ""))


def _rollback_areas(params):
    """Что откат имеет право трогать.

    По умолчанию — только хозяйство конструктора (etlFolder/, dags/). Код
    (Functions/, Src/, tools/) сюда не входит, и не по осторожности: из tools/
    прямо сейчас исполняется сам конструктор, и подменять его себе под ногами
    по кнопке в браузере — не то, чего ждёт нажимающий. Такой откат делается
    обычным деплоем.
    """
    if params.get("include_code"):
        raise BadRequest(
            "Откат кода (Functions/, Src/, tools/) из интерфейса не делается: "
            "конструктор запущен из этого же дерева и подменил бы сам себя. "
            "Такой откат — обычным деплоем с dev-ПК.")
    return G.AREAS


def r_rollback_apply(params):
    """Откатить НОВЫМ КОММИТОМ поверх и запушить."""
    (ref,) = _need(params, "ref")
    ok, log = G.rollback_apply(ref, areas=_rollback_areas(params))
    return {"ok": ok, "log": log}


def r_prod_status(params):
    """Может ли конструктор выложить на прод — и если нет, то почему."""
    return G.prod_status(probe=params.get("probe", True))


def r_prod_diff(params):
    """Чем прод отличается от теста. Читает, ничего не меняет."""
    return G.prod_diff()


def r_prod_deploy(params):
    """Выложить на прод. from_ref = тег prod-* — это откат прода.

    Всю работу делает deploy/deploy-prod.sh: он же гоняет гейт check-dags.sh по
    тому дереву, что уедет, он же ставит тег, он же повторяет push при сетевых
    сбоях. Второй реализации выкладки здесь нет намеренно — две прошлые
    разошлись и уронили прод (см. README, «Почему гейт именно в pre-receive»).
    """
    return G.prod_deploy(from_ref=params.get("from_ref"),
                         lines=params.get("lines"))


def r_trigger_targets(params):
    """Линии, которым нужен триггер, с предположенными колонками ведущей."""
    targets = T.trigger_targets(include_disabled=bool(params.get("include_disabled")))
    out = []
    for t in targets:
        period, pk = T.effective_columns(t)
        out.append({"key": t["key"], "table": t["table_master"],
                    "tablename": t["tablename"], "db": t["db_master"],
                    "mode": t["mode"], "needs": t["needs"],
                    "period_column": period, "pk_columns": pk,
                    "note": t.get("note")})
    return {"targets": out}


def r_trigger_build(params):
    """Собрать DDL журнального триггера (в БД ничего не выполняется)."""
    db, table, tablename = _need(params, "db", "table", "tablename")
    targets = params.get("targets")
    if targets:
        targets = [(t[0], t[1]) for t in targets]
    built = T.build_trigger(_db(db), table, tablename,
                            params.get("period_column"),
                            params.get("pk_columns") or [],
                            journal=params.get("journal"),
                            targets=targets)
    return {"name": built["name"], "func": built["func"],
            "journal": built["journal"], "text": built["text"],
            "statements": built["statements"]}


def r_trigger_check(params):
    """Что РЕАЛЬНО стоит в БД против того, что нужно линии.

    check_targets отдаёт ПАРУ (список по линиям, отчёт по каждой БД), а не
    словарь. Раньше эта пара уезжала в ответ как есть: интерфейс искал в ней
    строку по ключу линии, не находил и честно писал «не сверялось» — при
    успешном 200 и непустом теле. Здесь она раскладывается по местам: results
    ключуется линией, db_reports идёт отдельно (в нём живёт самое частое —
    «подключиться не удалось», и без него сверка выглядит просто молчащей)."""
    (keys,) = _need(params, "keys")
    targets = [t for t in T.trigger_targets(include_disabled=True)
               if t["key"] in set(keys)]
    if not targets:
        raise BadRequest(f"среди линий не нашлось ни одной из {keys}")
    results, db_reports = T.check_targets(targets)
    return {"results": {r["key"]: r for r in results},
            "db_reports": db_reports}


# ─────────── справочники и разовый перенос (tools/sp_builder.py) ───────────
# Отдельный от сложного ETL мир со своими понятиями, и смешивать их нельзя:
#   * регулярный справочник ('regular') переносится не по расписанию, а по
#     заданию: даг SpEtlNew берёт из etl_jobs строки с isokaudit = 0 и гоняет
#     все линии с такой `dependence`. Ноль туда ставит АУДИТНЫЙ триггер на
#     ведущей — без него справочник не обновится никогда и ни одной ошибки при
#     этом не будет;
#   * разовый перенос ('once') запускают руками, ему сигнал не нужен вовсе.
# Линия описывается не структурами, а парой готовых SQL (Select.sql / Add.sql),
# поэтому сопоставление колонок восстанавливается из них же, без обращения к БД.

def _sp_kind(value):
    if value not in ("regular", "once"):
        raise BadRequest(f"kind должен быть 'regular' или 'once', получено {value!r}")
    return value


def r_sp_lines(params):
    """Линии справочников и разового переноса — обоих типов сразу."""
    out = []
    for kind in ("regular", "once"):
        disabled = set(SP.list_disabled_sp_lines(kind))
        for key in SP.list_sp_lines(kind):
            line, dbm, dbs = B.split_key(key)
            out.append({"key": key, "kind": kind, "label": line,
                        "db_master": dbm, "db_slave": dbs,
                        "direction": f"{dbm}→{dbs}",
                        "disabled": key in disabled})
    return {"lines": out,
            "directions": sorted({l["direction"] for l in out})}


def r_sp_line(params):
    """Спецификация линии справочника для формы правки."""
    kind, key = _need(params, "kind", "key")
    return {"spec": SP.load_sp_line(_sp_kind(kind), key)}


def r_sp_preview(params):
    """Собрать файлы линии справочника, ничего не записывая."""
    (spec,) = _need(params, "spec")
    spec = dict(spec)
    if "pairs" in spec:
        # у справочника пара может быть неполной (колонка без ведомой) —
        # None здесь законен, в отличие от сложного ETL
        spec["pairs"] = [(p[0], p[1]) for p in spec["pairs"]]
    files, key = SP.build_sp_all(spec)
    return dict(_preview_body(files), key=key)


def r_sp_parse_sql(params):
    """SQL справочника -> какие колонки ДОСТУПНЫ с каждой стороны.

    Половина связи «правлю запрос — вижу колонки». Отдаёт именно доступное, а не
    сопоставление: с обёрткой (build_sp_select_over) выбор колонок живёт в
    парах, а не в тексте запроса, и переразбор текста не вправе его менять.
    Раньше здесь возвращались ещё и пары, и правка запроса молча перекладывала
    сопоставление заново.

    Имена — ВЫХОДНЫЕ (output_name): ровно так колонку назовёт БД, и ровно так
    её вернёт «Снять по запросу». Иначе одна колонка звалась бы то `kod`, то
    `m.code kod` — смотря чем её сегодня прочитали.
    """
    select_text = params.get("select_sql_text") or ""
    inner = SP.unwrap_select(select_text)
    if inner is not None:
        select_text = inner
    master = [SP.output_name(c) for c in SP.parse_select_columns(select_text)]
    _table, slave = SP.parse_insert_columns(params.get("add_sql_text") or "")
    return {"master_cols": SP._cols_as_dicts(master),
            "slave_cols": SP._cols_as_dicts(slave)}


def r_sp_rename_select_column(params):
    """Переименовать выходную колонку №index в SELECT справочника.

    Ради ОДНОГО правила правки: «поменял колонку в списке — поменялась колонка
    запроса» должно работать и для сгенерированного запроса, и для написанного
    руками. Голое имя заменяется целиком, у выражения меняется только
    псевдоним, а если его не было — добавляется. Разобрать не вышло (нет FROM,
    '*') — текст возвращается как есть: молча испортить чужой запрос хуже, чем
    не переименовать.
    """
    text, name = _need(params, "select_sql_text", "new_name")
    index = params.get("index")
    if not isinstance(index, int) or index < 0:
        raise BadRequest(f"index должен быть неотрицательным целым, получено {index!r}")
    return {"select_sql_text": SP.rename_select_column(text, index, name)}


def r_sp_build_sql(params):
    """Колонки и сопоставление -> SQL справочника. Вторая половина связи.

    Собирает теми же функциями, что и запись на диск, — текст в форме и текст в
    файле обязаны совпадать байт в байт, иначе предпросмотр показывал бы
    изменение сразу после того, как человек ничего не менял.

    Select собирается только в режиме «выбранные колонки»: в режиме «своё
    SELECT» запрос принадлежит человеку, и перезаписывать его нельзя.
    """
    db_slave = _db(params.get("db_slave") or "Post")
    pairs = _pairs(params.get("pairs") or [])
    slave_table = (params.get("slave_table") or "").strip()
    master_table = (params.get("master_table") or "").strip()
    mode = params.get("src_mode") or "table"

    # Текст, который сейчас в форме: если состав колонок в нём тот же, оставляем
    # его КАК ЕСТЬ. Файлы писались руками и разложены по-своему — у SPMKB все
    # семнадцать колонок в одну строку, — а генератор ставит перенос после
    # каждой. Без этой проверки выбор пары у одной колонки переписывал бы весь
    # запрос ради переносов строк, и предпросмотр показывал бы правку, которой
    # человек не делал.
    def keep_if_same(current, fresh, parse):
        if not current:
            return fresh
        try:
            return current if parse(current) == parse(fresh) else fresh
        except Exception:
            return fresh

    out = {}
    m_order = [m for m, s in pairs if m and s]
    s_order = [s for m, s in pairs if (mode == "custom" or m) and s]
    if mode != "custom" and master_table and m_order:
        out["select_sql_text"] = keep_if_same(
            params.get("select_sql_text"),
            SP.build_sp_select(master_table, m_order),
            SP.parse_select_columns)
    if slave_table and s_order:
        out["add_sql_text"] = keep_if_same(
            params.get("add_sql_text"),
            SP.build_sp_add(slave_table, s_order, db_slave),
            lambda t: SP.parse_insert_columns(t))
    return out


def r_sp_move(params):
    """Перевести линию между типами: разовый <-> регулярный."""
    key, from_kind, to_kind = _need(params, "key", "from_kind", "to_kind")
    _sp_kind(from_kind), _sp_kind(to_kind)
    if from_kind == to_kind:
        raise BadRequest("исходный и целевой тип совпадают — переводить некуда")
    return {"done": SP.move_sp_line(key, from_kind, to_kind)}


def r_sp_delete_targets(params):
    """Что удалится вместе с линией справочника.

    sp_line_targets отдаёт ТРОЙКУ, а не список путей: описание фрагмента,
    каталог SQL и признак «этот каталог нужен ещё кому-то». Последнее важно:
    общий каталог удаление не трогает, и человек должен видеть это ДО того,
    как нажмёт «удалить», а не выяснять потом, почему файлы остались.
    """
    kind, key = _need(params, "kind", "key")
    fragment, sql_dir, shared = SP.sp_line_targets(_sp_kind(kind), key)
    # у фрагмента с несколькими ключами описание вида "путь (ключ X)" —
    # относительным делаем только сам путь
    head, sep, tail = str(fragment).partition(" ")
    return {"fragment": _rel(head) + sep + tail,
            "sql_dir": _rel(sql_dir),
            "sql_dir_shared": shared}


def r_sp_delete(params):
    kind, key = _need(params, "kind", "key")
    return {"done": SP.delete_sp_line(_sp_kind(kind), key,
                                      remove_sql=params.get("remove_sql", True))}


def r_sp_audit_trigger(params):
    """DDL аудитного триггера справочника — того, что ставит isokaudit = 0."""
    db, table, dependence = _need(params, "db", "table", "dependence")
    built = T.build_audit_trigger(_db(db), table, dependence,
                                  jobs=params.get("jobs"))
    return {"name": built["name"], "text": built["text"],
            "statements": built["statements"]}


GET_ROUTES = {
    "lines": r_lines,
    "line": r_line,
    "tags": r_tags,
    "group-dags": r_group_dags,
    "git/status": r_git_status,
    "git/versions": r_versions,
    "defaults": r_defaults,
    "triggers/targets": r_trigger_targets,
    "sp/lines": r_sp_lines,
    "sp/line": r_sp_line,
}

POST_ROUTES = {
    "snap-structure": r_snap_structure,
    "snap-query-structure": r_snap_query_structure,
    "merge-table-pk": r_merge_table_pk,
    "match": r_match,
    "preview": r_preview,
    "write": r_write,
    "shared-files": r_shared_files,
    "delete-targets": r_delete_targets,
    "rename-plan": r_rename_plan,
    "rename-apply": r_rename_apply,
    "archive": r_archive,
    "restore": r_restore,
    "delete": r_delete,
    "git/push": r_git_push,
    "git/rollback-plan": r_rollback_plan,
    "git/rollback-apply": r_rollback_apply,
    "prod/status": r_prod_status,
    "prod/diff": r_prod_diff,
    "prod/deploy": r_prod_deploy,
    "triggers/build": r_trigger_build,
    "triggers/check": r_trigger_check,
    "sp/preview": r_sp_preview,
    "sp/parse-sql": r_sp_parse_sql,
    "sp/build-sql": r_sp_build_sql,
    "sp/rename-select-column": r_sp_rename_select_column,
    "sp/move": r_sp_move,
    "sp/delete-targets": r_sp_delete_targets,
    "sp/delete": r_sp_delete,
    "sp/audit-trigger": r_sp_audit_trigger,
}


def dispatch(method, route, params):
    """Выполнить маршрут. -> (http-код, тело ответа).

    Никакого tornado: ровно эту функцию дёргает и сервер, и самопроверка.
    """
    table = GET_ROUTES if method == "GET" else POST_ROUTES
    fn = table.get(route)
    if fn is None:
        known = ", ".join(sorted(table))
        return 404, {"ok": False, "error": "UnknownRoute",
                     "detail": f"нет маршрута {method} /api/{route}. Есть: {known}"}
    try:
        return 200, {"ok": True, "result": fn(params or {})}
    except BadRequest as err:
        return 400, {"ok": False, "error": "BadRequest", "detail": str(err)}
    except Exception as err:                       # noqa: BLE001
        # Ошибку логики (нет линии, не сошлись структуры, упал git) отдаём
        # текстом, а трассировку пишем в лог сервиса: в интерфейсе она не
        # нужна, а в journalctl — обязательна.
        logger.error("%s /api/%s: %s", method, route, traceback.format_exc())
        return 400, {"ok": False, "error": type(err).__name__, "detail": str(err)}


# ──────────────────────────────── сервер ────────────────────────────────

def make_app():
    """Собрать tornado-приложение. Импорт tornado здесь, а не наверху: без него
    работает и --selftest, и импорт модуля."""
    import tornado.ioloop
    import tornado.web

    class ApiHandler(tornado.web.RequestHandler):
        def set_default_headers(self):
            self.set_header("Content-Type", "application/json; charset=utf-8")

        def _finish(self, code, body):
            self.set_status(code)
            self.finish(json.dumps(body, ensure_ascii=False, default=str))

        async def get(self, route):
            params = {k: self.get_argument(k) for k in self.request.arguments}
            code, body = await self._run("GET", route, params)
            self._finish(code, body)

        async def post(self, route):
            try:
                params = json.loads(self.request.body or b"{}")
            except ValueError as err:
                return self._finish(400, {"ok": False, "error": "BadRequest",
                                          "detail": f"тело не JSON: {err}"})
            code, body = await self._run("POST", route, params)
            self._finish(code, body)

        async def _run(self, method, route, params):
            # Снятие структуры ходит в Oracle и занимает секунды, git push —
            # тоже. В основном цикле это заморозило бы весь сервис, поэтому
            # маршруты идут в пуле потоков.
            loop = tornado.ioloop.IOLoop.current()
            return await loop.run_in_executor(None, dispatch, method, route, params)

    class SpaHandler(tornado.web.StaticFileHandler):
        """Статика фронтенда. Неизвестный путь отдаёт index.html — иначе
        переход по ссылке внутри приложения давал бы 404.

        Но подменять index.html'ом ФАЙЛ (что-нибудь.js, .css) нельзя. Именно на
        этом получался пустой белый экран без единого сообщения: имя собранного
        файла содержит хэш содержимого (index-05412b86.js) и меняется при каждой
        пересборке. Стоит index.html и assets/ разъехаться — при переносе на
        флешке, при недотянутом git pull — браузер просил старый .js, получал в
        ответ HTML со статусом 200, не мог его исполнить и оставлял страницу
        пустой. Ни ошибки, ни намёка: страница ведь «загрузилась».

        Теперь такой запрос честно отдаёт 404, и в консоли браузера видно, чего
        не хватает. А чтобы это ловилось ещё раньше, целостность dist
        проверяется при старте — см. _dist_problems."""
        def validate_absolute_path(self, root, absolute_path):
            if not os.path.isfile(absolute_path):
                if os.path.splitext(absolute_path)[1]:
                    raise tornado.web.HTTPError(404)
                absolute_path = os.path.join(root, "index.html")
            return super().validate_absolute_path(root, absolute_path)

    handlers = [(r"/api/(.*)", ApiHandler)]
    if os.path.isdir(WEBUI_DIST):
        handlers.append((r"/(.*)", SpaHandler,
                         {"path": WEBUI_DIST, "default_filename": "index.html"}))
    return tornado.web.Application(handlers, compress_response=True)


def _dist_problems(dist=None):
    """Чего не хватает собранному фронтенду. [] — всё на месте.

    Проверка дешёвая и делается на старте, потому что расплата за пропуск
    дорогая: страница открывается ПУСТОЙ, без ошибки и без намёка, и выглядит
    это как «сервис не работает». Достаточно, чтобы index.html ссылался на
    файл, которого рядом нет, — а имена файлов сборки содержат хэш и меняются
    при каждой пересборке, так что разъехаться им проще простого."""
    dist = dist or WEBUI_DIST
    if not os.path.isdir(dist):
        return [f"{os.path.relpath(dist, ROOT)} нет — фронтенд не собран "
                f"(npm --prefix tools/webui run build)"]
    index = os.path.join(dist, "index.html")
    if not os.path.isfile(index):
        return [f"{os.path.relpath(index, ROOT)} нет — сборка неполная"]
    try:
        with open(index, encoding="utf-8") as fp:
            html = fp.read()
    except OSError as err:
        return [f"index.html не читается: {err}"]
    out = []
    for ref in re.findall(r'(?:src|href)\s*=\s*"([^"]+)"', html):
        if ref.startswith(("http://", "https://", "//", "data:")):
            continue
        target = os.path.join(dist, ref.lstrip("./").split("?", 1)[0])
        if not os.path.isfile(target):
            out.append(f"index.html ссылается на {ref}, а файла нет — "
                       f"страница откроется пустой. Пересоберите фронтенд "
                       f"или дотяните tools/webui/dist целиком")
    return out


def serve(host, port):
    import tornado.ioloop
    app = make_app()
    app.listen(port, address=host)
    where = "весь мир" if host == "0.0.0.0" else host
    logger.info("конструктор слушает %s:%s (доступ: %s)", host, port, where)
    for problem in _dist_problems():
        logger.warning("ФРОНТЕНД: %s", problem)
    tornado.ioloop.IOLoop.current().start()


# ────────────────────────────── самопроверка ──────────────────────────────

def _selftest():
    """Проверка маршрутизации и конверта ответа. Без БД, без сети, без tornado."""
    # 1) каждый маршрут — вызываемый, имена не пересекаются между методами
    for name, fn in list(GET_ROUTES.items()) + list(POST_ROUTES.items()):
        assert callable(fn), name
    both = set(GET_ROUTES) & set(POST_ROUTES)
    assert not both, f"маршрут объявлен и в GET, и в POST: {both}"

    # 2) неизвестный маршрут — 404 и перечень доступных, а не пустой ответ
    code, body = dispatch("GET", "нет-такого", {})
    assert code == 404 and body["ok"] is False, body
    assert "lines" in body["detail"], body

    # 3) не хватает параметра — 400 с именем параметра, а не 500
    code, body = dispatch("GET", "line", {})
    assert code == 400 and body["error"] == "BadRequest", body
    assert "key" in body["detail"], body

    # 4) ошибка логики — 400 и текст, трассировка в ответ не попадает
    code, body = dispatch("POST", "snap-structure", {"db": "Mysql", "table": "t"})
    assert code == 400 and "Orcl" in body["detail"], body
    assert "Traceback" not in json.dumps(body), body

    # 5) чтение репозитория работает без БД — самые частые маршруты интерфейса
    code, body = dispatch("GET", "lines", {})
    assert code == 200, body
    lines = body["result"]["lines"]
    assert lines, "не нашлось ни одной линии в config.d"
    one = {l["key"]: l for l in lines}["iprkdeptOrclPost"]
    assert one["direction"] == "Orcl→Post", one
    assert one["mode"] == "iud" and one["needs_trigger"] is True, one
    assert one["own_dag"] == "dags/IprkdeptOrclPost.py", one
    # у линии составного дага заполнен group_dag, а своего дага нет
    grouped = {l["key"]: l for l in lines}["EXPMED23OrclPost"]
    assert grouped["group_dag"] == "MocheckOrclPost" and not grouped["own_dag"], grouped
    # справочники для фильтров непустые — без них фильтровать нечем
    assert body["result"]["directions"] and body["result"]["modes"], body["result"]

    # ПРОЧИТАТЬ И СОБРАТЬ ОБРАТНО = НИЧЕГО НЕ ПОМЕНЯТЬ. Проверяем на живых
    # линиях, а не на выдуманной: ломалось это именно на особенностях реальных —
    # общий MOCHECK.sql у EXPMED23 (пересборка предлагала завести его частную
    # копию и переставить на неё конфиг) и конфиг без ключей-умолчаний у
    # PLANOMS. Открыл линию, нажал «Предпросмотр», ничего не тронув — должно
    # быть «менять нечего»; иначе непонятно, что из показанного твоё.
    for key in ("EXPMED23OrclPost", "PLANOMSOrclPost", "iprkdeptOrclPost"):
        code, body = dispatch("GET", "line", {"key": key})
        assert code == 200, body
        code, body = dispatch("POST", "preview", {"spec": body["result"]["spec"]})
        assert code == 200, body
        unchanged = set(body["result"]["unchanged"])
        moved = [f["path"] for f in body["result"]["files"]
                 if f["path"] not in unchanged and not f["path"].startswith("dags/")]
        assert not moved, f"{key}: пересборка меняет нетронутое — {moved}"

    # предпросмотр отдаёт и то, что лежит на диске: без старого текста
    # интерфейсу нечего подсвечивать, и «изменится» снова превращается в
    # полотно текста, которое надо вычитывать глазами
    code, body = dispatch("GET", "line", {"key": "iprkdeptOrclPost"})
    code, body = dispatch("POST", "preview", {"spec": body["result"]["spec"]})
    assert code == 200, body
    files = body["result"]["files"]
    assert files and all("old" in f for f in files), files[:1]
    on_disk = [f for f in files if f["path"] not in set(body["result"]["created"])]
    assert on_disk and all(f["old"] for f in on_disk), "старое содержимое не прочиталось"

    # сверка триггеров: ответ ключуется ЛИНИЕЙ. Раньше сюда как есть уезжала
    # пара (список, отчёт), интерфейс искал в ней ключ линии, не находил и
    # писал «не сверялось» — при успешном 200 и непустом теле
    import inspect
    src = inspect.getsource(r_trigger_check)
    assert "results, db_reports = T.check_targets" in src, src
    assert len(inspect.signature(T.check_targets).parameters) >= 1
    assert "return results, db_reports" in inspect.getsource(T.check_targets)

    code, body = dispatch("GET", "tags", {})
    assert code == 200 and isinstance(body["result"]["tags"], list), body

    code, body = dispatch("GET", "group-dags", {})
    assert code == 200, body
    names = [g["dag_id"] for g in body["result"]["group_dags"]]
    assert "MocheckOrclPost" in names, names

    code, body = dispatch("GET", "defaults",
                          {"table": "KOKNAEV.IPRKDEPT", "db_master": "Orcl",
                           "db_slave": "Post"})
    assert code == 200, body
    assert body["result"]["key"] == "IPRKDEPTOrclPost", body["result"]

    # 6) спецификация существующей линии читается целиком
    code, body = dispatch("GET", "line", {"key": "iprkdeptOrclPost"})
    assert code == 200, body
    assert body["result"]["spec"]["mode"] == "iud", body["result"]["spec"]["mode"]
    # путь дага отдаётся ОТНОСИТЕЛЬНЫМ: абсолютные пути сервера интерфейсу не
    # нужны, а в интерфейсе их ещё и неудобно читать
    own = body["result"]["placement"]["own_dag"]
    assert own == "dags/IprkdeptOrclPost.py", own

    # 6b) правка одного поля меняет РОВНО один файл — то, ради чего в
    #     конструкторе появился skip_unchanged. Проверяем через API целиком.
    spec = body["result"]["spec"]
    _code, out = dispatch("POST", "preview", {"spec": spec})
    assert not [f for f in out["result"]["files"]
                if f["path"] not in out["result"]["unchanged"]], "линия «поехала» без правок"
    _code, out = dispatch("POST", "preview", {"spec": dict(spec, mode="section_compare")})
    changed = [f["path"] for f in out["result"]["files"]
               if f["path"] not in out["result"]["unchanged"]]
    assert changed == ["etlFolder/config.d/iprkdeptOrclPost.json"], changed

    # 7) сборка триггера не ходит в БД и отдаёт готовый текст
    code, body = dispatch("POST", "triggers/build",
                          {"db": "Orcl", "table": "EXPMED", "tablename": "EXPMED",
                           "period_column": "docexpdt", "pk_columns": ["idrw"],
                           "targets": [["EXPMED23", "docexpdt"],
                                       ["EXPMED4", "docpenaltydt"]]})
    assert code == 200, body
    assert "EXPMED23" in body["result"]["text"] and "EXPMED4" in body["result"]["text"]

    # 8) ответ всегда сериализуется в JSON — иначе сервер отдал бы 500 уже
    #    после того, как работа сделана (например, git push прошёл)
    for method, route, params in (("GET", "lines", {}),
                                  ("GET", "group-dags", {}),
                                  ("GET", "triggers/targets", {})):
        _code, out = dispatch(method, route, params)
        json.dumps(out, ensure_ascii=False, default=str)

    # 8b) справочники и разовый перенос — свой мир, свои маршруты
    code, body = dispatch("GET", "sp/lines", {})
    assert code == 200, body
    spLines = body["result"]["lines"]
    assert spLines, "не нашлось ни одной линии справочника"
    kinds = {l["kind"] for l in spLines}
    assert kinds <= {"regular", "once"}, kinds
    sample = spLines[0]
    code, body = dispatch("GET", "sp/line",
                          {"kind": sample["kind"], "key": sample["key"]})
    assert code == 200, body
    spec = body["result"]["spec"]
    # сопоставление колонок у справочника восстанавливается ИЗ SQL, без БД —
    # если оно пустое, форма правки откроется ни с чем
    assert spec["pairs"], (sample["key"], spec)
    assert spec["src_mode"] in ("all", "table", "custom"), spec["src_mode"]

    # Линия БЕЗ своего Select.sql — законная настройка: рантайм подставляет
    # SELECT * FROM ведущей. Проверяем, что такая линия читается как "all" и
    # пересобирается БЕЗ файла запроса: конструктор когда-то выдумывал его с
    # заглушками «(колонка N)» и дописывал selectSql в конфиг, ломая линию.
    allMode = [l for l in spLines
               if dispatch("GET", "sp/line", {"kind": l["kind"], "key": l["key"]})[1]
               ["result"]["spec"]["src_mode"] == "all"]
    if allMode:
        one = allMode[0]
        spec2 = dispatch("GET", "sp/line", {"kind": one["kind"], "key": one["key"]})[1]
        spec2 = spec2["result"]["spec"]
        code, body = dispatch("POST", "sp/preview", {"spec": dict(spec2, kind=one["kind"])})
        assert code == 200, body
        paths = [f["path"] for f in body["result"]["files"]]
        assert not any(p.endswith("Select.sql") for p in paths), paths
        assert any(p.endswith("Add.sql") for p in paths), paths

    # пересборка того же самого не должна ничего менять на диске
    code, body = dispatch("POST", "sp/preview", {"spec": spec})
    assert code == 200, body
    assert body["result"]["key"] == sample["key"], body["result"]["key"]

    # неизвестный тип линии — понятная ошибка, а не падение внутри sp_builder
    code, body = dispatch("GET", "sp/line", {"kind": "хз", "key": sample["key"]})
    assert code == 400 and "regular" in body["detail"], body

    # 8.5) ВЕРСИИ, ОТКАТ, ПРОД — всё, что читает, проверяется на живом репозитории.
    code, body = dispatch("GET", "git/versions", {"limit": 5})
    assert code == 200 and body["result"]["versions"], body
    first = body["result"]["versions"][0]
    assert first["sha"] and first["date"] and first["subject"], first

    # План отката до HEAD пуст — и это не мелочь: «откатить туда, где мы уже
    # стоим» не должно предлагать ни одного файла, иначе первый же откат
    # покажет весь репозиторий как изменённый.
    code, body = dispatch("POST", "git/rollback-plan", {"ref": "HEAD"})
    assert code == 200, body
    assert body["result"]["files"] == [] and body["result"]["remove"] == [], body["result"]

    # Откат кода из интерфейса запрещён явно: конструктор запущен из этого же
    # дерева и подменил бы сам себя.
    code, body = dispatch("POST", "git/rollback-plan",
                          {"ref": "HEAD", "include_code": True})
    assert code == 400 and "сам себя" in body["detail"], body

    # Несуществующая версия — 400 с человеческим текстом, а не трассировка
    code, body = dispatch("POST", "git/rollback-plan", {"ref": "нет-такой"})
    assert code == 400 and "Не нашёл версию" in body["detail"], body

    # Состояние прода читается без сети (probe=false) и не падает там, где
    # remote 'prod' не настроен — на dev-ПК его нет ни у кого.
    code, body = dispatch("POST", "prod/status", {"probe": False})
    assert code == 200 and body["result"]["script"] is True, body

    # 9) ФРОНТЕНД И API ГОВОРЯТ ОБ ОДНОМ И ТОМ ЖЕ.
    #
    # Интерфейс собирается отдельно и на другой машине, поэтому опечатка в
    # имени маршрута там не ловится ничем: сборка проходит, а в браузере
    # кнопка молча отвечает 404. Здесь мы вычитываем имена прямо из клиента
    # (tools/webui/src/api.js) и требуем, чтобы каждый существовал.
    client = os.path.join(ROOT, "tools", "webui", "src", "api.js")
    if os.path.exists(client):
        import re
        with open(client, encoding="utf-8") as fp:
            text = fp.read()
        used = re.findall(r"call\(\s*'(GET|POST)'\s*,\s*'([^']+)'", text)
        assert used, "в клиенте не нашлось ни одного вызова call(...) — проверка ослепла"
        for method, route in used:
            table = GET_ROUTES if method == "GET" else POST_ROUTES
            assert route in table, (
                f"интерфейс зовёт {method} /api/{route}, а такого маршрута нет. "
                f"Есть: {', '.join(sorted(table))}")

    # ── создание линии справочника с нуля ────────────────────────────────────
    # Ровно то, что собирает форма создания: пустая спецификация с
    # заполненными полями, никакого чтения с диска. Проверяем, что из неё
    # получается полный комплект — два SQL и фрагмент — и что все три файла
    # НОВЫЕ: запись новой линии идёт с overwrite=false и обязана падать, если
    # ключ занят, а не съедать чужие файлы.
    new_sp = {"kind": "regular", "master_label": "SPSELFTEST",
              "db_master": "Orcl", "db_slave": "Post",
              "master_table": "KOKNAEV.SPSELFTEST", "slave_table": "spselftest",
              "dependence": "SPSELFTEST", "src_mode": "table",
              "pairs": [["ID", "id"], ["NAME", "name"]]}
    code, body = dispatch("POST", "sp/preview", {"spec": new_sp})
    assert code == 200, body
    res = body["result"]
    assert res["key"] == "SPSELFTESTOrclPost", res["key"]
    paths = [f["path"] for f in res["files"]]
    assert len(paths) == 3 and set(paths) == set(res["created"]), (paths, res["created"])
    assert any(p.endswith("Select.sql") for p in paths), paths
    assert any(p.endswith("Add.sql") for p in paths), paths
    assert any("SpTableName.d/" in p for p in paths), paths
    # разовый перенос — тот же путь, только фрагмент уезжает в SpOnce.d
    code, body = dispatch("POST", "sp/preview", {"spec": dict(new_sp, kind="once")})
    assert code == 200, body
    assert any("SpOnce.d/" in f["path"] for f in body["result"]["files"]), body["result"]
    # и ничего из этого на диск не попало: предпросмотр обязан быть безопасным
    assert not os.path.exists(os.path.join(
        B.ROOT, "etlFolder", "SpTableName.d", "SPSELFTESTOrclPost.json"))

    # ── свой запрос и колонки помогают друг другу, а не спорят ───────────────
    # Сценарий целиком: линия живёт в режиме «выбранные колонки», человек
    # пишет СВОЙ запрос и снимает колонки по нему. Ожидается: имена берутся из
    # псевдонимов запроса, масштаб и признак ключа подмешиваются из таблицы,
    # запрос после этого принадлежит человеку и не пересобирается, а INSERT
    # собирается по парам. БД здесь не нужна — подменяем оба снятия.
    from unittest import mock
    q_cols = [{"column_name": n, "data_type": "", "data_scale": None,
               "is_primary_key": None} for n in ("kod", "naimenovanie", "idrw")]
    t_cols = [{"column_name": n, "data_type": t, "data_scale": s,
               "is_primary_key": pk}
              for n, t, s, pk in (("kod", "VARCHAR2", None, "Primary Key"),
                                  ("naimenovanie", "VARCHAR2", None, None),
                                  ("idrw", "NUMBER", 0, None))]
    with mock.patch.object(B, "snap_query_structure", return_value=q_cols), \
         mock.patch.object(B, "snap_structure", return_value=t_cols):
        code, body = dispatch("POST", "snap-query-structure",
                              {"db": "Orcl", "sql": "SELECT 1 FROM dual",
                               "table": "KOKNAEV.SPMKB"})
        assert code == 200, body
        got = body["result"]
        assert [c["column_name"] for c in got["columns"]] == \
            ["kod", "naimenovanie", "idrw"], got["columns"]
        # ради этого всё и затевалось: без подмешивания масштаб приходил
        # пустым, и снятие показывало NUMBER там, где в структуре NUMBER(0)
        assert got["scale_known"] is True, got
        assert got["columns"][2]["data_scale"] == 0, got["columns"][2]
        assert got["columns"][0]["is_primary_key"] == "Primary Key", got["columns"][0]

        # а без имени таблицы подмешивать неоткуда — и это честно помечается,
        # чтобы разбор не выдавал «неизвестно» за «поменялось»
        code, body = dispatch("POST", "snap-query-structure",
                              {"db": "Orcl", "sql": "SELECT 1 FROM dual"})
        assert code == 200 and body["result"]["scale_known"] is False, body

    # запрос теперь свой — конструктор его не пересобирает, INSERT собирает
    code, body = dispatch("POST", "sp/build-sql",
                          {"db_slave": "Post", "master_table": "KOKNAEV.SPMKB",
                           "slave_table": "spmkb", "src_mode": "custom",
                           "pairs": [["kod", "kod"], ["idrw", "idrw"]],
                           "select_sql_text": "SELECT code kod FROM spmkb\n"})
    assert code == 200, body
    assert "select_sql_text" not in body["result"], body["result"]
    assert "INSERT INTO spmkb (kod, idrw)" in body["result"]["add_sql_text"]

    # ── связь «колонки ↔ SQL» у справочника ──────────────────────────────────
    # Линия описана двумя запросами, и оба конца связи обязаны сходиться:
    # разобрать сохранённый SQL и собрать его обратно — значит получить то же
    # самое. Иначе правка колонок молча переписывала бы запрос.
    code, body = dispatch("GET", "sp/line", {"kind": "regular", "key": "SPMKBOrclPost"})
    assert code == 200, body
    sp = body["result"]["spec"]
    code, body = dispatch("POST", "sp/parse-sql",
                          {"select_sql_text": sp["select_sql_text"],
                           "add_sql_text": sp["add_sql_text"]})
    assert code == 200, body
    # Разбор отдаёт ДОСТУПНОЕ, а не выбранное: сопоставление живёт в парах, и
    # переразбор текста не вправе его перекладывать. Здесь запрос собран из
    # колонок, поэтому доступное и выбранное совпадают.
    got = [c["column_name"] for c in body["result"]["master_cols"]]
    assert got == [p[0] for p in sp["pairs"]], got
    assert [c["column_name"] for c in body["result"]["slave_cols"]] == \
        [p[1] for p in sp["pairs"]]
    assert "pairs" not in body["result"], body["result"]

    build_args = {"db_slave": sp["db_slave"], "master_table": sp["master_table"],
                  "slave_table": sp["slave_table"], "src_mode": sp["src_mode"],
                  "select_sql_text": sp["select_sql_text"],
                  "add_sql_text": sp["add_sql_text"]}
    code, body = dispatch("POST", "sp/build-sql", dict(build_args, pairs=sp["pairs"]))
    assert code == 200, body
    # состав колонок тот же — рукописную раскладку не трогаем. У SPMKB все
    # семнадцать колонок в одной строке, а генератор ставит перенос после
    # каждой: перепиши их тут, и выбор пары у одной колонки показывал бы в
    # предпросмотре переписанный целиком запрос
    assert body["result"]["select_sql_text"] == sp["select_sql_text"]
    assert body["result"]["add_sql_text"] == sp["add_sql_text"]
    # а вот убранная колонка обязана перестроить оба запроса
    code, body = dispatch("POST", "sp/build-sql",
                          dict(build_args, pairs=sp["pairs"][:-1]))
    assert code == 200, body
    assert body["result"]["select_sql_text"] != sp["select_sql_text"]
    assert sp["pairs"][-1][1] not in body["result"]["add_sql_text"]

    # ── переименование линии ─────────────────────────────────────────────────
    # План обязан быть полным: имя линии живёт и в файлах, и в базе. Поменяй
    # одно — линия замолчит без единой ошибки, и это самый дорогой исход.
    code, body = dispatch("POST", "rename-plan",
                          {"key": "iprkdeptOrclPost", "new_line": "IPRKDEPT_NEW"})
    assert code == 200, body
    plan = body["result"]
    assert plan["new_key"] == "IPRKDEPT_NEWOrclPost", plan["new_key"]
    paths = [f["path"] for f in plan["files"]]
    assert "etlFolder/config.d/IPRKDEPT_NEWOrclPost.json" in paths, paths
    # старый фрагмент И старый DDL триггера обязаны попасть в снос: фрагмент —
    # иначе линия задвоится, DDL — он назван по ключу линии
    assert any("config.d/iprkdeptOrclPost.json" in r for r in plan["remove"]), plan["remove"]
    assert any("triggers/iprkdeptOrclPost.sql" in r for r in plan["remove"]), plan["remove"]
    # в даге меняется ИМЯ ЛИНИИ, но не имя задачи: на task_id висит история
    # запусков Airflow, и терять её при переименовании линии незачем
    dag = next(f for f in plan["files"] if f["path"].startswith("dags/"))
    assert 'tableNameEtlJobs="IPRKDEPT_NEW"' in dag["content"], dag["content"][:400]
    assert '"do_etl_iprkdept"' in dag["content"], "имя задачи не должно меняться"
    # SQL для базы — обе таблицы, иначе группы и непереваренные события
    # останутся под старым именем
    assert "etl_jobs" in plan["sql"] and "etl_log_iud_row" in plan["sql"], plan["sql"]
    assert "'IPRKDEPT_NEW'" in plan["sql"] and "'iprkdept'" in plan["sql"], plan["sql"]
    assert plan["warnings"] and any("ТРИГГЕР" in w for w in plan["warnings"]), plan["warnings"]
    # план НИЧЕГО не пишет
    assert not os.path.exists(os.path.join(
        B.ROOT, "etlFolder", "config.d", "IPRKDEPT_NEWOrclPost.json"))

    # занятое имя — отказ, а не молчаливое слияние двух линий в одну
    code, body = dispatch("POST", "rename-plan",
                          {"key": "iprkdeptOrclPost", "new_line": "IPERSON"})
    assert code == 200 or "уже есть" in body.get("detail", ""), body

    # ── что вообще меняет репозиторий ────────────────────────────────────────
    # Правка обязана доходить до диска ОДНОЙ дорогой: «Предпросмотр» → человек
    # видит разницу → «Записать». Маршруты ниже — исключения, и каждое здесь не
    # случайно: write — это и есть кнопка записи; archive/restore/delete/sp-move
    # двигают и удаляют файлы, собрать их в предпросмотр нечем, поэтому в
    # интерфейсе каждый спрашивает подтверждение; git/push отправляет уже
    # записанное.
    #
    # Список закрытый: новый пишущий маршрут придётся вписать сюда руками, и
    # это тот момент, когда стоит спросить себя, почему он не проходит через
    # предпросмотр. Так ушли set-flag и sp/set-disabled — переключатели,
    # писавшие конфиг сразу по щелчку.
    writing = {"write", "archive", "restore", "delete", "sp/delete", "sp/move",
               "git/push", "rename-apply",
               # откат и выкладка — тоже запись, только не в файл, а в историю:
               # rollback-apply коммитит и пушит в test, prod/deploy собирает
               # выкладку и пушит в прод. Оба спрашивают подтверждение.
               "git/rollback-apply", "prod/deploy"}
    # …а все ОСТАЛЬНЫЕ маршруты не трогают ни диск, ни историю: считают,
    # разбирают, показывают. Перечислены поимённо, и это принципиально.
    read_only = {
        # показывают, что будет удалено/переименовано/выложено
        "delete-targets", "sp/delete-targets", "rename-plan",
        "git/rollback-plan", "prod/status", "prod/diff",
        # читают БД
        "snap-structure", "snap-query-structure", "merge-table-pk",
        # считают в памяти
        "match", "preview", "sp/preview", "shared-files",
        "sp/parse-sql", "sp/build-sql", "sp/rename-select-column",
        "triggers/build", "triggers/check", "sp/audit-trigger",
    }
    # Список ЗАКРЫТЫЙ в обе стороны: каждый POST-маршрут обязан быть ровно в
    # одном из двух наборов. Раньше проверка искала в имени слова вроде
    # «delete» или «push» — и git/rollback-apply с prod/deploy прошли её
    # насквозь: ни одного такого слова в них не было. То есть самый опасный
    # маршрут (push в прод) остался незамеченным ровно тем, что заведено
    # замечать опасные. Догадка по имени такой ошибки не ловит никогда —
    # объявление ловит всегда: новый маршрут просто не даст пройти проверке,
    # пока автор не скажет, пишет он или нет.
    unclassified = set(POST_ROUTES) - writing - read_only
    assert not unclassified, (
        f"маршруты {sorted(unclassified)} не отнесены ни к пишущим, ни к "
        f"читающим. Впишите каждый в writing или read_only в этом файле — это "
        f"тот момент, когда стоит спросить себя, почему маршрут не проходит "
        f"через предпросмотр")
    both = writing & read_only
    assert not both, f"маршруты {sorted(both)} перечислены в обоих наборах"
    stale = (writing | read_only) - set(POST_ROUTES)
    assert not stale, (
        f"маршрутов {sorted(stale)} больше нет — уберите их из списков, иначе "
        f"проверка начнёт покрывать несуществующее")
    for name in ("set-flag", "sp/set-disabled"):
        assert name not in POST_ROUTES, (
            f"маршрут {name} писал конфиг сразу по щелчку переключателя — "
            f"единственный способ изменить репозиторий мимо кнопки «Записать»")

    # ── формы линии пересобираются при смене линии ───────────────────────────
    # У React состояние живёт в СМОНТИРОВАННОМ компоненте. Перешёл на другую
    # линию без key — форма остаётся той же, и вместе с ней имя в
    # «Переименовать», готовый план, список «что будет удалено», незакрытый
    # разбор снятых структур. Всё это относится к прежней линии, а кнопки рядом
    # работают уже с новой: список удаления от линии A и включал кнопку, и
    # оставался на экране — а удалялась по ней линия B.
    #
    # Лечится одним key на форме. Проверка грубая (по тексту), но другой тут
    # нет: JSX отсюда не исполнить, а цена пропажи — потерянная не та линия.
    for path, needle in (
        (("tools", "webui", "src", "App.jsx"), r"<LineForm\s+key=\{selected\}"),
        (("tools", "webui", "src", "SpPage.jsx"), r"<SpForm\s+key=\{"),
    ):
        f = os.path.join(ROOT, *path)
        if os.path.exists(f):
            with open(f, encoding="utf-8") as fp:
                text = fp.read()
            assert re.search(needle, text), (
                f"в {path[-1]} пропал key на форме линии: состояние прежней линии "
                f"переживёт переход на другую, и кнопки начнут относиться не к той")

    # ── страница колонок не правит чужой запрос ──────────────────────────────
    # Единственная её правка в тексте — псевдоним безымянной колонке
    # (onNameMaster), и та через отдельное окно с подтверждением. Всё остальное
    # — выбор, снятие, удаление — живёт в парах: внешний список конструктор
    # соберёт сам. Стоит вернуть сюда правку текста, и «запертый SELECT» снова
    # начнёт меняться сам по себе, ради чего замок и заводили.
    editor = os.path.join(ROOT, "tools", "webui", "src", "SpMappingEditor.jsx")
    if os.path.exists(editor):
        with open(editor, encoding="utf-8") as fp:
            text = fp.read()
        assert re.search(r"const removeRow = \(i\) =>\s*onChange\(\{ pairs:", text), (
            "в SpMappingEditor удаление колонки снова лезет куда-то помимо пар: "
            "убрать колонку из линии — это убрать пару, запрос при этом не "
            "меняется вовсе")
        # читать текст запроса ей можно (по нему видно, есть ли он вообще),
        # а вот ОТДАВАТЬ новый — уже правка: `select_sql_text:` как ключ объекта
        assert "onNameMaster" in text and "select_sql_text:" not in text, (
            "страница колонок начала править текст запроса напрямую — правка "
            "текста живёт на вкладке SQL, здесь только псевдоним по кнопке")

    # ── пересборка любой линии справочника ничего не теряет ──────────────────
    # Обёртка (SELECT колонки FROM ( ваш запрос ) src) меняет ФОРМУ файла у линий
    # со своим запросом: файл станет другим при первой же записи такой линии.
    # Значит нужен постоянный, а не разовый ответ на вопрос «а не потеряется ли
    # там что-нибудь». Он такой: для КАЖДОЙ линии пересборка даёт либо тот же
    # файл байт в байт, либо ровно обёртку вокруг текущего текста — запрос
    # внутри дословный, список колонок тот же и в том же порядке.
    for kind in ("regular", "once"):
        for key in SP.list_sp_lines(kind):
            spec = SP.load_sp_line(kind, key)
            try:
                files, _k = SP.build_sp_all(dict(spec, kind=kind))
            except ValueError:
                continue          # линия требует правки (см. текст ошибки в форме)
            for rel, fresh in files:
                if not rel.endswith("Select.sql"):
                    continue
                path = os.path.join(ROOT, rel)
                if not os.path.exists(path):
                    continue
                with open(path, encoding="utf-8") as fp:
                    old = fp.read()
                if old == fresh:
                    continue
                inner = SP.unwrap_select(fresh)
                assert inner is not None and inner == old.strip().rstrip(";").rstrip(), (
                    f"{key}: пересборка изменила ТЕКСТ запроса, а не только "
                    f"список колонок — так рукописный SELECT и теряют")
                assert ([SP.output_name(c) for c in SP.parse_select_columns(fresh)]
                        == [SP.output_name(c) for c in SP.parse_select_columns(old)]), (
                    f"{key}: пересборка изменила состав или порядок колонок — "
                    f"вставка идёт позиционно, значения лягут не в свои столбцы")

    # ── замок стоит там, где на него натыкаются ──────────────────────────────
    # Переключатель один (SqlLock), но показан в ДВУХ местах: на вкладке SQL,
    # где лежит текст запроса, и на вкладке колонок, где его правят походя —
    # переименованием и регистром. Первая версия жила только в колонках, а на
    # вкладке SQL вместо него стояло окошко с кнопкой «Запрос мой, не
    # пересобирать» — тот же смысл под другим именем, из-за чего управление
    # читалось как два разных, да ещё и не на своих местах.
    for path in (("tools", "webui", "src", "SpPage.jsx"),
                 ("tools", "webui", "src", "SpMappingEditor.jsx")):
        f = os.path.join(ROOT, *path)
        if os.path.exists(f):
            with open(f, encoding="utf-8") as fp:
                text = fp.read()
            assert "SqlLock" in text, (
                f"в {path[-1]} пропал переключатель замка: он должен быть и там, "
                f"где запрос лежит, и там, где его правят не глядя")

    # ── регистр имени таблицы: правило одно на обе стороны ───────────────────
    # to_db_case живёт и в питоне (им получается имя линии), и в интерфейсе
    # (tools/webui/src/dbCase.js — за кнопкой «привести к регистру БД»).
    # Расходиться им нельзя: имя линии сравнивается с etl_jobs.tablename
    # ДОСЛОВНО. Запустить здесь JS нечем — на сервере нет node, — поэтому
    # проверяем, что правило на месте целиком: разбор по точкам, кавычки не
    # трогаются, Oracle вверх / Postgres вниз.
    case_js = os.path.join(ROOT, "tools", "webui", "src", "dbCase.js")
    if os.path.exists(case_js):
        with open(case_js, encoding="utf-8") as fp:
            text = fp.read()
        for token in ("split('.')", "startsWith('\"')", "toUpperCase", "toLowerCase",
                      "'Orcl'", "export function toDbCase",
                      # без него «привести к регистру» переписывало бы и
                      # выражения: TRUNC(dt) -> TRUNC(DT)
                      "export function isPlainName"):
            assert token in text, (
                f"в dbCase.js пропало {token!r} — правило регистра разошлось с "
                f"dag_builder.to_db_case, а имя линии сравнивается с "
                f"etl_jobs.tablename дословно")

    # ── собранный фронтенд ───────────────────────────────────────────────────
    # Проверка на разъехавшиеся index.html и assets/. Стоит им разойтись —
    # страница открывается ПУСТОЙ, без ошибки, и виноватым назначается сервер.
    assert not _dist_problems(), _dist_problems()
    with tempfile.TemporaryDirectory() as tmp:
        assert _dist_problems(tmp), "пустой каталог должен считаться проблемой"
        with open(os.path.join(tmp, "index.html"), "w", encoding="utf-8") as fp:
            fp.write('<script src="./assets/index-DEADBEEF.js"></script>')
        problems = _dist_problems(tmp)
        assert problems and "index-DEADBEEF.js" in problems[0], problems

    print("dagbuilder_api selftest OK")


def main(argv):
    parser = argparse.ArgumentParser(description="API конструктора ETL-линий")
    parser.add_argument("--host", default="127.0.0.1",
                        help="по умолчанию только локально; наружу отдаёт Apache")
    parser.add_argument("--port", type=int, default=8085)
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")
    if args.selftest:
        return _selftest()
    serve(args.host, args.port)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
