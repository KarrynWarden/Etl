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
    return {"columns": cols,
            "period_column": B.default_period_column(cols)}


def r_snap_query_structure(params):
    """Снять структуру своего SELECT — типы берутся так же, как их сверяет
    рантайм, поэтому снятая структура проходит проверку как есть."""
    db, sql = _need(params, "db", "sql")
    cols = B.snap_query_structure(_db(db), sql, params.get("cred") or "MAIN")
    unknown = [c for c in cols
               if B.unknown_type(c.get("data_type") or c.get("DATA_TYPE"))]
    return {"columns": cols,
            "period_column": B.default_period_column(cols),
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
    "archive": r_archive,
    "restore": r_restore,
    "delete": r_delete,
    "git/push": r_git_push,
    "triggers/build": r_trigger_build,
    "triggers/check": r_trigger_check,
    "sp/preview": r_sp_preview,
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
               "git/push"}
    # …а эти только ПОКАЗЫВАЮТ, что будет удалено, — их и зовут перед
    # подтверждением удаления
    read_only = {"delete-targets", "sp/delete-targets"}
    unexpected = {r for r in POST_ROUTES
                  if r not in writing and r not in read_only and (
                      "delete" in r or "set" in r or "write" in r
                      or "move" in r or "push" in r or "archive" in r)}
    assert not unexpected, (
        f"маршруты {sorted(unexpected)} похожи на пишущие, но не перечислены. "
        f"Если они правда меняют репозиторий — им нужно подтверждение в "
        f"интерфейсе; если нет — переименуйте, чтобы не путать")
    for name in ("set-flag", "sp/set-disabled"):
        assert name not in POST_ROUTES, (
            f"маршрут {name} писал конфиг сразу по щелчку переключателя — "
            f"единственный способ изменить репозиторий мимо кнопки «Записать»")

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
