#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Интерфейс генератора ETL-линии для JupyterLab (ipywidgets).

Надстройка над tools/dag_builder.py. Два режима:
  • «Создать новый»     — с нуля: заполнить форму → снять структуры из БД →
    поправить сопоставление колонок → предпросмотр → создать файлы.
  • «Редактировать»     — выбрать существующую линию, подгрузить её настройки и
    сопоставление, поменять что нужно → сохранить поверх.

Использование (в ячейке ноутбука):
    from tools.dag_builder_ui import launch
    launch()
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import dag_builder as B  # noqa: E402

_NONE = "— нет —"

# Метки расписания -> внутренний вид (kind в dag_builder.build_schedule_expr)
_SCHED_KINDS = {
    "Интервал: каждые N минут": "interval",
    "В заданные часы (одна минута)": "times",
    "Cron-выражение (вручную)": "cron",
}
_KIND_TO_LABEL = {v: k for k, v in _SCHED_KINDS.items()}


def launch():
    import ipywidgets as W
    from IPython.display import display

    state = {"master_cols": [], "slave_cols": [], "rows": [], "row_widgets": [],
             "period_w": None, "slave_period_w": None,
             "struct_master_rel": None, "struct_slave_rel": None,
             "tags_auto": True, "_tags_guard": False, "_opts_guard": False}

    # ── режим работы ──
    work_mode = W.ToggleButtons(
        options=[("➕ Создать новый", "new"), ("✏️ Редактировать", "edit"),
                 ("🗄 Архив", "archive")],
        value="new", style={"button_width": "170px"})
    edit_pick = W.Dropdown(description="Линия", options=B.existing_lines(),
                           layout=W.Layout(width="380px"),
                           style={"description_width": "90px"})
    btn_load = W.Button(description="Загрузить линию", icon="upload",
                        layout=W.Layout(width="200px"))
    edit_box = W.HBox([edit_pick, btn_load])

    # ── панель архива (скрыть/восстановить даг без удаления) ──
    arch_active = W.Dropdown(description="Активная линия", options=[],
                             layout=W.Layout(width="380px"),
                             style={"description_width": "120px"})
    btn_archive = W.Button(description="🗄 В архив", button_style="warning",
                           layout=W.Layout(width="180px"))
    arch_archived = W.Dropdown(description="В архиве", options=[],
                               layout=W.Layout(width="380px"),
                               style={"description_width": "120px"})
    btn_restore = W.Button(description="♻ Восстановить", button_style="success",
                           layout=W.Layout(width="180px"))
    arch_help = W.HTML(
        "<span style='color:#888'>Архив прячет даг от Airflow <b>без удаления</b>: "
        "файл уезжает в <code>dags/_archived/</code> (Airflow его не парсит) и на линию "
        "ставится <code>skipAudit</code>. Конфиг и структуры остаются. Изменения вступают "
        "в силу <b>после деплоя</b>. Восстановление возвращает всё назад.</span>")
    archive_box = W.VBox([arch_help, W.HBox([arch_active, btn_archive]),
                          W.HBox([arch_archived, btn_restore])])

    def _refresh_archive_lists():
        arch_active.options = B.list_active_lines()
        arch_archived.options = B.list_archived_lines()

    # ── шапка-форма ──
    tm = W.Text(description="Ведущая", placeholder="prbdir  или  KOKNAEV.PRBDIR",
                layout=W.Layout(width="380px"), style={"description_width": "110px"})
    ts = W.Text(description="Ведомая", placeholder="KOKNAEV.PRBDIR",
                layout=W.Layout(width="380px"), style={"description_width": "110px"})
    dbm = W.Dropdown(description="БД ведущей", options=["Post", "Orcl"], value="Post",
                     layout=W.Layout(width="220px"), style={"description_width": "110px"})
    dbs = W.Dropdown(description="БД ведомой", options=["Orcl", "Post"], value="Orcl",
                     layout=W.Layout(width="220px"), style={"description_width": "110px"})
    # «Пользователь БД» = под каким набором реквизитов (.env) ходим в базу при
    # СНЯТИИ структуры. Выпадающий список (показывает варианты сразу по клику) +
    # пункт «своё…», открывающий поле ручного ввода имени набора.
    _USER_SENT = "✏ своё…"

    def _user_field(desc, opts=("MAIN", "A56")):
        dd = W.Dropdown(description=desc, options=list(opts) + [_USER_SENT], value=opts[0],
                        layout=W.Layout(width="300px"), style={"description_width": "150px"})
        txt = W.Text(placeholder="имя набора из .env", layout=W.Layout(width="190px"))
        txt.layout.display = "none"
        dd.observe(lambda _:
                   setattr(txt.layout, "display", "" if dd.value == _USER_SENT else "none"),
                   names="value")

        def get():
            return (txt.value.strip() or "MAIN") if dd.value == _USER_SENT else dd.value
        return W.HBox([dd, txt]), get

    user_m_box, user_m_get = _user_field("Пользователь ведущей")
    user_s_box, user_s_get = _user_field("Пользователь ведомой")
    line = W.Text(description="Имя линии", placeholder="по умолч. = имя ведущей",
                  layout=W.Layout(width="380px"), style={"description_width": "110px"})
    dag = W.Text(description="dag_id", placeholder="по умолч. авто (см. ниже)",
                 layout=W.Layout(width="380px"), style={"description_width": "110px"})
    dag_preview = W.HTML("")
    mode = W.Dropdown(description="mode", value="iud",
                      options=["iud", "section", "section_compare", "delete_insert"],
                      layout=W.Layout(width="280px"), style={"description_width": "110px"})
    doc = W.Text(description="Комментарий", placeholder="_doc: краткое описание линии",
                 layout=W.Layout(width="640px"), style={"description_width": "110px"})

    # ── ретраи ──
    retry = W.Dropdown(
        description="Режим ретраев",
        options=[("frequent — частый запуск, свой FSM-ретрай", "frequent"),
                 ("rare — редкий запуск, ретраи делает airflow", "rare")],
        value="frequent", layout=W.Layout(width="520px"),
        style={"description_width": "110px"})

    # ── расписание ──
    sched_kind = W.Dropdown(description="Расписание", options=list(_SCHED_KINDS),
                            value="Интервал: каждые N минут",
                            layout=W.Layout(width="420px"),
                            style={"description_width": "110px"})
    sched_minutes = W.IntText(description="каждые, мин", value=1,
                              layout=W.Layout(width="220px"),
                              style={"description_width": "110px"})
    sched_times = W.Text(description="часы:мин", placeholder="11:50, 13:50, 20:50",
                         layout=W.Layout(width="420px"),
                         style={"description_width": "110px"})
    sched_cron = W.Text(description="cron", placeholder="50 11,13,20 * * *",
                        layout=W.Layout(width="420px"),
                        style={"description_width": "110px"})
    sched_help = W.HTML("<span style='color:#888'>Интервал — для frequent. "
                        "«Часы:мин» — несколько запусков в день с одинаковой минутой "
                        "(напр. 11:50, 13:50, 20:50). Cron — для сложных случаев.</span>")
    sched_inputs = W.VBox([sched_minutes])

    def _on_sched_kind(_=None):
        kind = _SCHED_KINDS[sched_kind.value]
        sched_inputs.children = {
            "interval": [sched_minutes], "times": [sched_times], "cron": [sched_cron],
        }[kind]
    sched_kind.observe(_on_sched_kind, names="value")

    # ── теги (несколько; из существующих + новые) ──
    # По умолчанию проставляются 3 тега: направление, имя таблицы, базовый DbSync.
    # Авто-теги обновляются, пока пользователь не тронул поле руками.
    tags_input = W.TagsInput(value=[], allow_duplicates=False,
                             layout=W.Layout(width="640px"))
    # Dropdown показывает существующие теги сразу по клику (без ввода буквы).
    tag_pick = W.Dropdown(options=[""] + B.existing_tags(),
                          layout=W.Layout(width="300px"))
    btn_add_tag = W.Button(description="＋ выбранный", layout=W.Layout(width="130px"))
    # Отдельное поле для РУЧНОГО ввода нового тега (Dropdown не даёт печатать).
    tag_new = W.Text(placeholder="новый тег вручную", layout=W.Layout(width="220px"))
    btn_add_new = W.Button(description="＋ новый", layout=W.Layout(width="110px"))

    def _add_tag_value(t):
        t = (t or "").strip()
        if t:
            tags_input.value = list(dict.fromkeys(list(tags_input.value) + [t]))

    def _add_tag(_):
        _add_tag_value(tag_pick.value)
        tag_pick.value = ""

    def _add_new(_):
        _add_tag_value(tag_new.value)
        tag_new.value = ""
    btn_add_tag.on_click(_add_tag)
    btn_add_new.on_click(_add_new)

    def _default_tags():
        ln = line.value.strip() or (B.bare(tm.value.strip()).lower() if tm.value.strip() else "")
        t = [f"{dbm.value}{dbs.value}"]
        if ln:
            t.append(ln)
        t.append("DbSync")
        return list(dict.fromkeys(t))

    def _refresh_default_tags(_=None):
        if state.get("tags_auto", True) and work_mode.value == "new":
            state["_tags_guard"] = True
            tags_input.value = _default_tags()
            state["_tags_guard"] = False

    def _on_tags_change(_):
        if not state.get("_tags_guard"):
            state["tags_auto"] = False     # пользователь правил теги — авто больше не вмешивается
    tags_input.observe(_on_tags_change, names="value")
    for _w in (tm, line, dbm, dbs):
        _w.observe(_refresh_default_tags, names="value")

    # ── дополнительно (необязательные ключи конфига), по-русски ──
    f_filter = W.Text(description="Фильтр ведущей", layout=W.Layout(width="640px"),
                      style={"description_width": "180px"})
    f_filter_s = W.Text(description="Фильтр ведомой", layout=W.Layout(width="640px"),
                        style={"description_width": "180px"})
    f_sql = W.Textarea(description="SQL ведущей (текст)",
                       placeholder="Вставь текст SELECT-запроса. Файл .sql создастся сам, "
                                   "путь к нему пропишется в selectSql.",
                       layout=W.Layout(width="760px", height="120px"),
                       style={"description_width": "180px"})
    f_sql_name = W.Text(description="Имя файла .sql", placeholder="по умолч. = имя линии",
                        layout=W.Layout(width="500px"),
                        style={"description_width": "180px"})
    f_confl = W.Text(description="Конфликт: доп. колонки", layout=W.Layout(width="640px"),
                     style={"description_width": "180px"})
    f_confl_w = W.Text(description="Конфликт: условие WHERE", layout=W.Layout(width="640px"),
                       style={"description_width": "180px"})
    f_trunc = W.Checkbox(description="Сравнивать период по дате (truncatePeriod)",
                         value=False, indent=False)
    f_skip_audit = W.Checkbox(description="Не аудировать линию (skipAudit)",
                              value=False, indent=False)
    f_audit_excl = W.Text(description="Не сверять поля (аудит)",
                          placeholder="через запятую: updatedate, hash",
                          layout=W.Layout(width="640px"),
                          style={"description_width": "180px"})
    adv_help = W.HTML(
        "<span style='color:#888'>"
        "<b>Фильтр ведущей/ведомой</b> — доп. условие WHERE (например <code>doctype = 7</code>).<br>"
        "<b>SQL ведущей</b> — если перенос идёт не из таблицы, а из своего запроса.<br>"
        "<b>Конфликт: доп. колонки</b> — лишние колонки в ON CONFLICT (upsert).<br>"
        "<b>Конфликт: условие WHERE</b> — для частичного уникального индекса.<br>"
        "<b>truncatePeriod</b> — сравнивать период по дате, без времени.<br>"
        "<b>skipAudit</b> — исключить линию из общего аудита (AuditAll).<br>"
        "<b>Не сверять поля</b> — auditExcludeFields: колонки, которые аудит игнорирует "
        "(часто меняющиеся служебные поля).</span>")
    advanced = W.Accordion(children=[W.VBox(
        [adv_help, f_filter, f_filter_s, f_sql, f_sql_name, f_confl, f_confl_w,
         f_trunc, f_skip_audit, f_audit_excl])])
    advanced.set_title(0, "Дополнительно (необязательно)")
    advanced.selected_index = None

    # ── зоны вывода ──
    period_box = W.HBox([])
    map_title = W.HTML("")
    hide_unmapped = W.Checkbox(description="Скрывать непривязанные", value=False,
                               indent=False)
    # шапка над колонками: какая таблица/БД слева (ведущая) и справа (ведомая) —
    # чтобы не гадать по регистру, из какой таблицы колонка
    map_head = W.HBox([], layout=W.Layout(align_items="center",
                                          border_bottom="2px solid #bbb", padding="2px"))
    map_box = W.VBox([], layout=W.Layout(max_height="420px", overflow="auto",
                                         border="1px solid #ddd", padding="4px"))
    out = W.Output()

    btn_snap = W.Button(description="1) Снять структуры из БД", button_style="primary",
                        icon="download", layout=W.Layout(width="260px"))
    btn_prev = W.Button(description="2) Предпросмотр", icon="eye",
                        layout=W.Layout(width="200px"))
    btn_make = W.Button(description="3) Создать файлы", button_style="success",
                        icon="check", layout=W.Layout(width="220px"))

    def _log(msg, clear=True):
        with out:
            if clear:
                out.clear_output()
            print(msg)

    # ── живой предпросмотр dag_id ──
    def _update_dag_preview(_=None):
        ln = (line.value.strip() or B.bare(tm.value.strip()).lower()) if tm.value.strip() \
            or line.value.strip() else ""
        if dag.value.strip():
            dag_preview.value = (f"<span style='color:#888'>dag_id (задан вручную): "
                                 f"<code>{dag.value.strip()}</code></span>")
        elif ln:
            dag_preview.value = ("<span style='color:#888'>будет создан dag_id: "
                                 f"<code>{B.default_dag_id(ln, dbm.value, dbs.value)}</code> "
                                 "(имя линии + направление)</span>")
        else:
            dag_preview.value = ("<span style='color:#888'>dag_id появится, когда "
                                 "заполнишь ведущую/имя линии</span>")
    for w in (tm, line, dag, dbm, dbs):
        w.observe(_update_dag_preview, names="value")
    _update_dag_preview()

    # ── режим: что показывать (new/edit — форма сборки; archive — панель архива) ──
    def _on_work_mode(_=None):
        m = work_mode.value
        edit_box.layout.display = "" if m == "edit" else "none"
        archive_box.layout.display = "" if m == "archive" else "none"
        build_area.layout.display = "none" if m == "archive" else ""
        btn_make.description = ("3) Сохранить изменения" if m == "edit"
                                else "3) Создать файлы")
        if m == "new":
            # уходя из правки, забыть исходные пути структур, чтобы новая линия
            # не записалась поверх чужих файлов; вернуть авто-теги
            state["struct_master_rel"] = state["struct_slave_rel"] = None
            state["tags_auto"] = True
            _refresh_default_tags()
        elif m == "archive":
            _refresh_archive_lists()
    work_mode.observe(_on_work_mode, names="value")

    # ── фильтр строк: показывать только привязанные (для проверки) ──
    def _apply_row_filter(_=None):
        widgets = state.get("row_widgets", [])
        if hide_unmapped.value:
            map_box.children = [w for (m, dd, pk), w in zip(state["rows"], widgets)
                                if dd.value != _NONE]
        else:
            map_box.children = widgets
    hide_unmapped.observe(_apply_row_filter, names="value")

    # ── связь 1:1: в каждой выпадашке оставляем только ещё не занятые колонки ──
    # ведомой (плюс собственный текущий выбор), чтобы одну ведомую колонку нельзя
    # было назначить дважды и список был короче.
    def _refresh_slave_options():
        if state.get("_opts_guard"):
            return
        all_s = [c["column_name"] for c in state["slave_cols"]]
        used = {dd.value for _m, dd, _p in state["rows"] if dd.value != _NONE}
        state["_opts_guard"] = True
        try:
            for _m, dd, _p in state["rows"]:
                cur = dd.value
                opts = [_NONE] + [s for s in all_s if s == cur or s not in used]
                if list(dd.options) != opts:
                    dd.options = opts
                    dd.value = cur            # сохранить выбор строки
        finally:
            state["_opts_guard"] = False

    def _on_pair_change(_=None):
        _refresh_slave_options()
        _apply_row_filter()

    # ── отрисовка таблицы сопоставления колонок ──
    def _render_mapping(mcols, scols, pair_map=None, pk_names=None,
                        period_m=None, period_s=None):
        # шапка: явно подписываем, какая таблица/БД слева и справа
        m_lbl = (tm.value.strip() or "ведущая")
        s_lbl = (ts.value.strip() or "ведомая")
        map_head.children = [
            W.HTML(f"<b>ВЕДУЩАЯ</b> · <code>{m_lbl}</code> "
                   f"<span style='color:#888'>[{dbm.value}]</span>",
                   layout=W.Layout(width="324px")),
            W.HTML(f"<b>ВЕДОМАЯ</b> · <code>{s_lbl}</code> "
                   f"<span style='color:#888'>[{dbs.value}]</span>",
                   layout=W.Layout(width="280px")),
            W.HTML("<b>PK</b>", layout=W.Layout(width="80px")),
        ]
        slave_opts = [_NONE] + [c["column_name"] for c in scols]
        rows, widgets = [], []
        for mcol in mcols:
            name = mcol["column_name"]
            sval = pair_map.get(name) if pair_map else None
            is_pk = (name in pk_names) if pk_names is not None \
                else bool(mcol.get("is_primary_key"))
            lbl = W.HTML(f"<code>{name}</code> "
                         f"<span style='color:#888'>{mcol['data_type']}</span>",
                         layout=W.Layout(width="300px"))
            dd = W.Dropdown(options=slave_opts, value=sval if sval in slave_opts else _NONE,
                            layout=W.Layout(width="280px"))
            dd.observe(_on_pair_change, names="value")
            pk = W.Checkbox(value=is_pk, description="PK", indent=False,
                            layout=W.Layout(width="80px"))
            rows.append((mcol, dd, pk))
            # увеличенная высота строки + разделитель, чтобы строки не сливались
            widgets.append(W.HBox(
                [lbl, W.HTML("→", layout=W.Layout(width="24px")), dd, pk],
                layout=W.Layout(align_items="center", min_height="42px",
                                padding="4px 2px", border_bottom="1px solid #eee")))
        state["rows"] = rows
        state["row_widgets"] = widgets
        _refresh_slave_options()   # сразу убрать занятые из чужих списков
        _apply_row_filter()

        m_names = [c["column_name"] for c in mcols]
        s_names = [c["column_name"] for c in scols]
        # дефолт колонки-периода: createdate без учёта регистра (Oracle отдаёт
        # CREATEDATE), иначе колонка с типом дата/время — а не первая по алфавиту
        pm = period_m if period_m in m_names else B.default_period_column(mcols)
        ps = period_s if period_s in s_names else B.default_period_column(scols)
        state["period_w"] = W.Dropdown(description="periodColumn", options=m_names, value=pm,
                                        style={"description_width": "130px"},
                                        layout=W.Layout(width="330px"))
        state["slave_period_w"] = W.Dropdown(description="slavePeriodColumn", options=s_names,
                                              value=ps, style={"description_width": "150px"},
                                              layout=W.Layout(width="360px"))
        period_box.children = [state["period_w"], state["slave_period_w"]]

    def _collect_pairs():
        pairs = []
        for mcol, dd, _pk in state["rows"]:
            sval = None if dd.value == _NONE else dd.value
            pairs.append((mcol["column_name"], sval))
        return pairs

    def _apply_pk():
        # перенести галочки PK обратно в словари колонок (build_all зеркалит на ведомую)
        for mcol, _dd, pk in state["rows"]:
            mcol["is_primary_key"] = "Primary Key" if pk.value else None

    def _build_spec():
        _apply_pk()
        extra = {
            "filterClause": f_filter.value.strip() or None,
            "filterClauseSlave": f_filter_s.value.strip() or None,
            "conflictExtra": f_confl.value.strip() or None,
            "conflictWhere": f_confl_w.value.strip() or None,
        }
        if f_trunc.value:
            extra["truncatePeriod"] = True
        if f_skip_audit.value:
            extra["skipAudit"] = True
        excl = [c.strip() for c in f_audit_excl.value.replace(";", ",").split(",") if c.strip()]
        if excl:
            extra["auditExcludeFields"] = excl

        spec = {
            "table_master": tm.value.strip(), "table_slave": ts.value.strip(),
            "db_master": dbm.value, "db_slave": dbs.value,
            "line_name": line.value.strip() or None,
            "dag_id": dag.value.strip() or None,
            "mode": mode.value,
            "master_cols": state["master_cols"], "slave_cols": state["slave_cols"],
            "pairs": _collect_pairs(),
            "period_column": state["period_w"].value if state["period_w"] else None,
            "slave_period_column": state["slave_period_w"].value if state["slave_period_w"] else None,
            "tags": list(tags_input.value) or None,
            "retry_mode": retry.value,
            "doc": doc.value.strip() or None,
            "extra": extra,
            "select_sql_text": f_sql.value.strip() or None,
            "select_sql_name": f_sql_name.value.strip() or None,
            "struct_master_rel": state["struct_master_rel"],
            "struct_slave_rel": state["struct_slave_rel"],
        }
        # расписание
        kind = _SCHED_KINDS[sched_kind.value]
        spec["schedule_kind"] = kind
        if kind == "interval":
            spec["schedule_minutes"] = sched_minutes.value
        elif kind == "times":
            spec["schedule_times"] = [t.strip() for t in
                                      sched_times.value.replace(";", ",").replace(" ", ",").split(",")
                                      if t.strip()]
        else:
            spec["schedule_cron"] = sched_cron.value.strip()
        return spec

    def _current_mapping():
        """Текущее сопоставление из формы: {master_name: slave_name|None} и набор PK."""
        pm = {}
        pk = set()
        for mcol, dd, pkw in state["rows"]:
            nm = mcol["column_name"]
            pm[nm] = None if dd.value == _NONE else dd.value
            if pkw.value:
                pk.add(nm)
        return pm, pk

    # ── снять структуры из БД ──
    def on_snap(_):
        try:
            if not tm.value.strip() or not ts.value.strip():
                _log("Заполни имена ведущей и ведомой таблиц."); return
            edit = work_mode.value == "edit"
            # в правке сохраняем текущие связки/PK/период, чтобы пересъём не снёс их
            prev_pairs, prev_pk = (_current_mapping() if edit and state["rows"]
                                   else ({}, None))
            prev_pm = state["period_w"].value if (edit and state["period_w"]) else None
            prev_ps = state["slave_period_w"].value if (edit and state["slave_period_w"]) else None

            _log("Снимаю структуры из БД…")
            mcols = B.snap_structure(dbm.value, tm.value.strip(), user_m_get())
            scols = B.snap_structure(dbs.value, ts.value.strip(), user_s_get())
            state["master_cols"], state["slave_cols"] = mcols, scols

            if edit:
                # НЕ авто-матчим: новые колонки остаются непривязанными (их могли
                # намеренно не включать), старые связки сохраняем как есть.
                slave_names = {c["column_name"] for c in scols}
                pair_map = {n: (s if s in slave_names else None)
                            for n, s in prev_pairs.items()}
                _render_mapping(mcols, scols, pair_map=pair_map, pk_names=prev_pk,
                                period_m=prev_pm, period_s=prev_ps)
                new_cols = [c["column_name"] for c in mcols if c["column_name"] not in prev_pairs]
                note = (f"Пересъём (правка): ведущая {len(mcols)} / ведомая {len(scols)}. "
                        "Существующие связки и PK сохранены.")
                if new_cols:
                    note += (" <b>Новые колонки (привяжи вручную при необходимости):</b> "
                             + ", ".join(f"<code>{c}</code>" for c in new_cols))
                map_title.value = note
                _log("Структуры обновлены, связки сохранены. Привяжи новые поля и «Сохранить».")
            else:
                sugg, unmatched = B.auto_match(mcols, scols)
                pair_map = {m["column_name"]: s for m, s in zip(mcols, sugg)}
                _render_mapping(mcols, scols, pair_map=pair_map)
                matched = sum(1 for s in sugg if s)
                note = (f"Сопоставление колонок: ведущая {len(mcols)} / ведомая {len(scols)}, "
                        f"авто-совпало {matched}.")
                if unmatched:
                    note += (" <b>Без пары в ведомой:</b> "
                             + ", ".join(f"<code>{u}</code>" for u in unmatched))
                map_title.value = note
                _log("Структуры сняты. Поправь выпадашки и галочки PK, затем «Предпросмотр».")
        except Exception as e:
            _log(f"Ошибка снятия структур: {type(e).__name__}: {e}")

    # ── загрузить существующую линию (режим правки) ──
    def on_load(_):
        try:
            key = edit_pick.value
            if not key:
                _log("Выбери линию из списка."); return
            data = B.load_line(key)
            tm.value = data["table_master"]
            ts.value = data["table_slave"]
            dbm.value = data["db_master"]
            dbs.value = data["db_slave"]
            line.value = data["line_name"]
            dag.value = data["dag_id"]
            mode.value = data["mode"] if data["mode"] in mode.options else "iud"
            doc.value = data["doc"] or ""
            retry.value = data["retry_mode"] if data["retry_mode"] in ("frequent", "rare") \
                else "frequent"
            tags_input.value = list(data["tags"])

            # расписание
            sched_kind.value = _KIND_TO_LABEL.get(data["schedule_kind"],
                                                  "Интервал: каждые N минут")
            if data["schedule_kind"] == "interval":
                sched_minutes.value = data["schedule_minutes"]
            elif data["schedule_kind"] == "cron":
                sched_cron.value = data["schedule_cron"]
            _on_sched_kind()

            ex = data["extra"]
            f_filter.value = ex.get("filterClause", "") or ""
            f_filter_s.value = ex.get("filterClauseSlave", "") or ""
            f_confl.value = ex.get("conflictExtra", "") or ""
            f_confl_w.value = ex.get("conflictWhere", "") or ""
            f_trunc.value = bool(ex.get("truncatePeriod"))
            f_skip_audit.value = bool(ex.get("skipAudit"))
            ae = ex.get("auditExcludeFields")
            if isinstance(ae, str):
                ae = [x.strip() for x in ae.split(",") if x.strip()]
            f_audit_excl.value = ", ".join(ae) if ae else ""
            f_sql.value = data["select_sql_text"] or ""
            f_sql_name.value = (os.path.splitext(os.path.basename(data["select_sql"]))[0]
                                if data.get("select_sql") else "")

            state["master_cols"], state["slave_cols"] = data["master_cols"], data["slave_cols"]
            state["struct_master_rel"] = data["struct_master_rel"]
            state["struct_slave_rel"] = data["struct_slave_rel"]
            pair_map = dict(data["pairs"])
            pk_names = {c["column_name"] for c in data["master_cols"]
                        if c.get("is_primary_key")}
            _render_mapping(data["master_cols"], data["slave_cols"], pair_map=pair_map,
                            pk_names=pk_names, period_m=data["period_column"],
                            period_s=data["slave_period_column"])
            map_title.value = (f"Загружена линия <code>{key}</code>. Колонки — из сохранённых "
                               "структур. Чтобы подтянуть новые колонки из БД — нажми «Снять "
                               "структуры».")
            _log(f"Линия «{key}» загружена. Поменяй что нужно и нажми «Сохранить изменения».")
        except Exception as e:
            _log(f"Ошибка загрузки линии: {type(e).__name__}: {e}")

    def on_preview(_):
        try:
            if not state["rows"]:
                _log("Сначала сними структуры (кнопка 1) или загрузи линию."); return
            files = B.build_all(_build_spec())
            with out:
                out.clear_output()
                for rel, content in files:
                    print("=" * 70); print(rel); print("-" * 70); print(content)
        except Exception as e:
            _log(f"Ошибка предпросмотра: {type(e).__name__}: {e}")

    def on_make(_):
        try:
            if not state["rows"]:
                _log("Сначала сними структуры (кнопка 1) или загрузи линию."); return
            files = B.build_all(_build_spec())
            # в режиме правки перезаписываем существующие файлы намеренно
            written = B.write_files(files, overwrite=(work_mode.value == "edit"))
            with out:
                out.clear_output()
                print("Готово (и конфиг успешно собрался):")
                for p in written:
                    print("  ", os.path.relpath(p, ROOT))
                print("\nДальше выкатить на тест:")
                print("  sh deploy/deploy-test.sh \"новая линия\"")
        except FileExistsError as e:
            _log(f"{e}\nТакая линия уже есть. Чтобы изменить её — переключись в режим "
                 f"«✏️ Редактировать» и выбери её из списка.")
        except Exception as e:
            _log(f"Ошибка создания: {type(e).__name__}: {e}")

    # ── архив: убрать в архив / восстановить ──
    def on_archive(_):
        try:
            key = arch_active.value
            if not key:
                _log("Выбери активную линию."); return
            did = B.archive_line(key)
            _refresh_archive_lists()
            edit_pick.options = B.existing_lines()
            _log(f"Линия «{key}» (даг {did}) убрана в архив: файл в dags/_archived/, "
                 "выставлен skipAudit. Вступит в силу после деплоя. Восстановить — тут же.")
        except Exception as e:
            _log(f"Ошибка архивации: {type(e).__name__}: {e}")

    def on_restore(_):
        try:
            key = arch_archived.value
            if not key:
                _log("Выбери линию из архива."); return
            did = B.restore_line(key)
            _refresh_archive_lists()
            edit_pick.options = B.existing_lines()
            _log(f"Линия «{key}» (даг {did}) восстановлена. Вступит в силу после деплоя.")
        except Exception as e:
            _log(f"Ошибка восстановления: {type(e).__name__}: {e}")

    btn_snap.on_click(on_snap)
    btn_prev.on_click(on_preview)
    btn_make.on_click(on_make)
    btn_load.on_click(on_load)
    btn_archive.on_click(on_archive)
    btn_restore.on_click(on_restore)

    # форма сборки (new/edit) — прячется целиком в режиме архива
    build_area = W.VBox([
        W.HBox([tm, ts]), W.HBox([dbm, dbs]), W.HBox([user_m_box, user_s_box]),
        W.HBox([line, dag]), dag_preview,
        W.HBox([mode, retry]), doc,
        W.HTML("<b>Расписание и ретраи</b>"), sched_kind, sched_inputs, sched_help,
        W.HTML("<b>Теги</b> (несколько). Из списка — выбери и «＋ выбранный». "
               "Новый — впиши в поле и «＋ новый»:"),
        tags_input, W.HBox([tag_pick, btn_add_tag, tag_new, btn_add_new]),
        advanced, btn_snap,
        W.HTML("<hr>"),
        W.HTML("<b>Колонки</b> (ведущая → ведомая; отметь PK, составной ключ — "
               "несколько галочек):"),
        period_box, W.HBox([map_title, hide_unmapped]), map_head, map_box,
        W.HBox([btn_prev, btn_make]),
    ])

    _refresh_default_tags()   # проставить 3 тега по умолчанию
    _on_work_mode()           # выставить видимость по текущему режиму

    display(W.VBox([
        W.HTML("<h3>Генератор ETL-линии</h3>"),
        work_mode, edit_box, archive_box, W.HTML("<hr>"),
        build_area, W.HTML("<hr>"), out,
    ]))
