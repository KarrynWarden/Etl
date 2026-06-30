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

    state = {"master_cols": [], "slave_cols": [], "rows": [],
             "period_w": None, "slave_period_w": None,
             "struct_master_rel": None, "struct_slave_rel": None}

    # ── режим работы ──
    work_mode = W.ToggleButtons(
        options=[("➕ Создать новый", "new"), ("✏️ Редактировать", "edit")],
        value="new", style={"button_width": "180px"})
    edit_pick = W.Dropdown(description="Линия", options=B.existing_lines(),
                           layout=W.Layout(width="380px"),
                           style={"description_width": "90px"})
    btn_load = W.Button(description="Загрузить линию", icon="upload",
                        layout=W.Layout(width="200px"))
    edit_box = W.HBox([edit_pick, btn_load])

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
    # СНЯТИИ структуры. Combobox: можно выбрать из списка или вписать своё имя.
    user_m = W.Combobox(description="Пользователь ведущей", value="MAIN",
                        options=["MAIN", "A56"], ensure_option=False,
                        layout=W.Layout(width="320px"),
                        style={"description_width": "150px"})
    user_s = W.Combobox(description="Пользователь ведомой", value="MAIN",
                        options=["MAIN", "A56"], ensure_option=False,
                        layout=W.Layout(width="320px"),
                        style={"description_width": "150px"})
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
    tags_input = W.TagsInput(value=[], allow_duplicates=False,
                             layout=W.Layout(width="640px"))
    tag_pick = W.Combobox(placeholder="выбрать из существующих или вписать новый",
                          options=B.existing_tags(), ensure_option=False,
                          layout=W.Layout(width="420px"))
    btn_add_tag = W.Button(description="＋ тег", layout=W.Layout(width="100px"))

    def _add_tag(_):
        t = tag_pick.value.strip()
        if t:
            tags_input.value = list(dict.fromkeys(list(tags_input.value) + [t]))
            tag_pick.value = ""
    btn_add_tag.on_click(_add_tag)

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
    adv_help = W.HTML(
        "<span style='color:#888'>"
        "<b>Фильтр ведущей/ведомой</b> — доп. условие WHERE (например <code>doctype = 7</code>).<br>"
        "<b>SQL ведущей</b> — если перенос идёт не из таблицы, а из своего запроса.<br>"
        "<b>Конфликт: доп. колонки</b> — лишние колонки в ON CONFLICT (upsert).<br>"
        "<b>Конфликт: условие WHERE</b> — для частичного уникального индекса.<br>"
        "<b>truncatePeriod</b> — сравнивать период по дате, без времени.</span>")
    advanced = W.Accordion(children=[W.VBox(
        [adv_help, f_filter, f_filter_s, f_sql, f_sql_name, f_confl, f_confl_w, f_trunc])])
    advanced.set_title(0, "Дополнительно (необязательно)")
    advanced.selected_index = None

    # ── зоны вывода ──
    period_box = W.HBox([])
    map_title = W.HTML("")
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

    # ── режим: показывать селектор линии только в правке ──
    def _on_work_mode(_=None):
        edit_box.layout.display = "" if work_mode.value == "edit" else "none"
        btn_make.description = ("3) Сохранить изменения" if work_mode.value == "edit"
                                else "3) Создать файлы")
        if work_mode.value == "new":
            # уходя из правки, забыть исходные пути структур, чтобы новая линия
            # не записалась поверх чужих файлов
            state["struct_master_rel"] = state["struct_slave_rel"] = None
    work_mode.observe(_on_work_mode, names="value")

    # ── отрисовка таблицы сопоставления колонок ──
    def _render_mapping(mcols, scols, pair_map=None, pk_names=None,
                        period_m=None, period_s=None):
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
            pk = W.Checkbox(value=is_pk, description="PK", indent=False,
                            layout=W.Layout(width="80px"))
            rows.append((mcol, dd, pk))
            # увеличенная высота строки + разделитель, чтобы строки не сливались
            widgets.append(W.HBox(
                [lbl, W.HTML("→", layout=W.Layout(width="24px")), dd, pk],
                layout=W.Layout(align_items="center", min_height="42px",
                                padding="4px 2px", border_bottom="1px solid #eee")))
        state["rows"] = rows
        map_box.children = widgets

        m_names = [c["column_name"] for c in mcols]
        s_names = [c["column_name"] for c in scols]
        pm = period_m if period_m in m_names else \
            ("createdate" if "createdate" in m_names else (m_names[0] if m_names else None))
        ps = period_s if period_s in s_names else \
            ("createdate" if "createdate" in s_names else (s_names[0] if s_names else None))
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

    # ── снять структуры из БД ──
    def on_snap(_):
        try:
            if not tm.value.strip() or not ts.value.strip():
                _log("Заполни имена ведущей и ведомой таблиц."); return
            _log("Снимаю структуры из БД…")
            mcols = B.snap_structure(dbm.value, tm.value.strip(), user_m.value.strip() or "MAIN")
            scols = B.snap_structure(dbs.value, ts.value.strip(), user_s.value.strip() or "MAIN")
            state["master_cols"], state["slave_cols"] = mcols, scols

            sugg, unmatched = B.auto_match(mcols, scols)
            pair_map = {m["column_name"]: s for m, s in zip(mcols, sugg)}
            _render_mapping(mcols, scols, pair_map=pair_map)

            if not tags_input.value:
                tags_input.value = [f"{dbm.value}{dbs.value}",
                                    line.value.strip() or B.bare(tm.value.strip()).lower(),
                                    "DbSync"]

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

    btn_snap.on_click(on_snap)
    btn_prev.on_click(on_preview)
    btn_make.on_click(on_make)
    btn_load.on_click(on_load)

    _on_work_mode()  # скрыть селектор линии в режиме «новый»

    header = W.VBox([
        W.HTML("<h3>Генератор ETL-линии</h3>"),
        work_mode, edit_box, W.HTML("<hr>"),
        W.HBox([tm, ts]), W.HBox([dbm, dbs]), W.HBox([user_m, user_s]),
        W.HBox([line, dag]), dag_preview,
        W.HBox([mode, retry]), doc,
        W.HTML("<b>Расписание и ретраи</b>"), sched_kind, sched_inputs, sched_help,
        W.HTML("<b>Теги</b> (несколько; новый появится в списке после сохранения):"),
        tags_input, W.HBox([tag_pick, btn_add_tag]),
        advanced, btn_snap,
    ])
    mapping = W.VBox([W.HTML("<b>Колонки</b> (ведущая → ведомая; отметь PK, "
                             "составной ключ — несколько галочек):"),
                      period_box, map_title, map_box,
                      W.HBox([btn_prev, btn_make])])
    display(W.VBox([header, W.HTML("<hr>"), mapping, W.HTML("<hr>"), out]))
