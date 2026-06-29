#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Интерфейс генератора ETL-линии для JupyterLab (ipywidgets).

Тонкая надстройка над tools/dag_builder.py: форма + снятие структур из БД +
визуальное сопоставление колонок выпадашками (имена в двух БД могут отличаться —
сопоставляешь мышкой, без вырезания JSON) + предпросмотр и запись 4 файлов.

Использование (в ячейке ноутбука):
    from tools.dag_builder_ui import launch
    launch()
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import dag_builder as B  # noqa: E402

_NONE = "— нет —"


def launch():
    import ipywidgets as W
    from IPython.display import display

    state = {"master_cols": [], "slave_cols": [], "rows": []}

    # ── шапка-форма ──
    tm = W.Text(description="Ведущая", placeholder="prbdir  или  KOKNAEV.PRBDIR",
                layout=W.Layout(width="380px"), style={"description_width": "90px"})
    ts = W.Text(description="Ведомая", placeholder="KOKNAEV.PRBDIR",
                layout=W.Layout(width="380px"), style={"description_width": "90px"})
    dbm = W.Dropdown(description="dbMaster", options=["Post", "Orcl"], value="Post",
                     layout=W.Layout(width="220px"), style={"description_width": "90px"})
    dbs = W.Dropdown(description="dbSlave", options=["Orcl", "Post"], value="Orcl",
                     layout=W.Layout(width="220px"), style={"description_width": "90px"})
    cred = W.Dropdown(description="Реквизиты", options=["MAIN", "A56"], value="MAIN",
                      layout=W.Layout(width="220px"), style={"description_width": "90px"})
    line = W.Text(description="Имя линии", placeholder="по умолч. = имя ведущей",
                  layout=W.Layout(width="380px"), style={"description_width": "90px"})
    dag = W.Text(description="dag_id", placeholder="по умолч. авто",
                 layout=W.Layout(width="380px"), style={"description_width": "90px"})
    mode = W.Dropdown(description="mode", value="iud",
                      options=["iud", "section", "section_compare", "delete_insert"],
                      layout=W.Layout(width="280px"), style={"description_width": "90px"})
    tags = W.Text(description="Теги", placeholder="через запятую (по умолч. авто)",
                  layout=W.Layout(width="380px"), style={"description_width": "90px"})
    sched = W.IntText(description="Период, мин", value=1,
                      layout=W.Layout(width="220px"), style={"description_width": "90px"})
    doc = W.Text(description="_doc", placeholder="комментарий к линии",
                 layout=W.Layout(width="640px"), style={"description_width": "90px"})

    # ── дополнительно (необязательные ключи конфига) ──
    f_filter = W.Text(description="filterClause", layout=W.Layout(width="640px"),
                      style={"description_width": "130px"})
    f_filter_s = W.Text(description="filterClauseSlave", layout=W.Layout(width="640px"),
                        style={"description_width": "130px"})
    f_select = W.Text(description="selectSql", placeholder="queries/customQueries/<...>.sql",
                      layout=W.Layout(width="640px"), style={"description_width": "130px"})
    f_confl = W.Text(description="conflictExtra", layout=W.Layout(width="640px"),
                     style={"description_width": "130px"})
    f_confl_w = W.Text(description="conflictWhere", layout=W.Layout(width="640px"),
                       style={"description_width": "130px"})
    f_trunc = W.Checkbox(description="truncatePeriod", value=False)
    advanced = W.Accordion(children=[W.VBox(
        [f_filter, f_filter_s, f_select, f_confl, f_confl_w, f_trunc])])
    advanced.set_title(0, "Дополнительно (необязательно)")
    advanced.selected_index = None

    # ── зоны вывода ──
    period_box = W.HBox([])
    map_title = W.HTML("")
    map_box = W.VBox([], layout=W.Layout(max_height="360px", overflow="auto",
                                         border="1px solid #ddd", padding="4px"))
    out = W.Output()

    btn_snap = W.Button(description="1) Снять структуры из БД", button_style="primary",
                        icon="download", layout=W.Layout(width="240px"))
    btn_prev = W.Button(description="2) Предпросмотр", icon="eye",
                        layout=W.Layout(width="200px"))
    btn_make = W.Button(description="3) Создать файлы", button_style="success",
                        icon="check", layout=W.Layout(width="200px"))
    overwrite = W.Checkbox(description="перезаписать существующие", value=False)

    def _log(msg, clear=True):
        with out:
            if clear:
                out.clear_output()
            print(msg)

    def _collect_pairs():
        pairs = []
        for mcol, dd in state["rows"]:
            sval = None if dd.value == _NONE else dd.value
            pairs.append((mcol["column_name"], sval))
        return pairs

    def _build_spec():
        period = state["period_w"].value
        slave_period = state["slave_period_w"].value
        extra = {
            "filterClause": f_filter.value.strip() or None,
            "filterClauseSlave": f_filter_s.value.strip() or None,
            "selectSql": f_select.value.strip() or None,
            "conflictExtra": f_confl.value.strip() or None,
            "conflictWhere": f_confl_w.value.strip() or None,
        }
        if f_trunc.value:
            extra["truncatePeriod"] = True
        tag_list = [t.strip() for t in tags.value.split(",") if t.strip()] or None
        return {
            "table_master": tm.value.strip(), "table_slave": ts.value.strip(),
            "db_master": dbm.value, "db_slave": dbs.value,
            "line_name": line.value.strip() or None,
            "dag_id": dag.value.strip() or None,
            "mode": mode.value,
            "master_cols": state["master_cols"], "slave_cols": state["slave_cols"],
            "pairs": _collect_pairs(),
            "period_column": period, "slave_period_column": slave_period,
            "tags": tag_list, "schedule_minutes": sched.value,
            "doc": doc.value.strip() or None, "extra": extra,
        }

    def on_snap(_):
        try:
            if not tm.value.strip() or not ts.value.strip():
                _log("Заполни имена ведущей и ведомой таблиц."); return
            _log("Снимаю структуры из БД…")
            mcols = B.snap_structure(dbm.value, tm.value.strip(), cred.value)
            scols = B.snap_structure(dbs.value, ts.value.strip(), cred.value)
            state["master_cols"], state["slave_cols"] = mcols, scols

            sugg, unmatched = B.auto_match(mcols, scols)
            slave_opts = [_NONE] + [c["column_name"] for c in scols]

            rows, widgets = [], []
            for mcol, s in zip(mcols, sugg):
                pk = " 🔑" if mcol["is_primary_key"] else ""
                lbl = W.HTML(f"<code>{mcol['column_name']}</code> "
                             f"<span style='color:#888'>{mcol['data_type']}{pk}</span>",
                             layout=W.Layout(width="320px"))
                dd = W.Dropdown(options=slave_opts, value=s if s else _NONE,
                                layout=W.Layout(width="300px"))
                rows.append((mcol, dd))
                widgets.append(W.HBox([lbl, W.HTML("→"), dd]))
            state["rows"] = rows
            map_box.children = widgets

            # выпадашки колонки-периода
            m_names = [c["column_name"] for c in mcols]
            s_names = [c["column_name"] for c in scols]
            pc = "createdate" if "createdate" in m_names else m_names[0]
            spc = "createdate" if "createdate" in s_names else s_names[0]
            state["period_w"] = W.Dropdown(description="periodColumn", options=m_names,
                                           value=pc, style={"description_width": "120px"},
                                           layout=W.Layout(width="320px"))
            state["slave_period_w"] = W.Dropdown(description="slavePeriodColumn",
                                                 options=s_names, value=spc,
                                                 style={"description_width": "140px"},
                                                 layout=W.Layout(width="340px"))
            period_box.children = [state["period_w"], state["slave_period_w"]]

            matched = sum(1 for s in sugg if s)
            note = (f"Сопоставление колонок: ведущая {len(mcols)} / ведомая {len(scols)}, "
                    f"авто-совпало {matched}.")
            if unmatched:
                note += (" <b>Без пары в ведомой:</b> "
                         + ", ".join(f"<code>{u}</code>" for u in unmatched))
            map_title.value = note
            _log("Структуры сняты. Поправь выпадашки, где имена не совпали, "
                 "затем «Предпросмотр».")
        except Exception as e:
            _log(f"Ошибка снятия структур: {type(e).__name__}: {e}")

    def on_preview(_):
        try:
            if not state["rows"]:
                _log("Сначала сними структуры (кнопка 1)."); return
            files = B.build_all(_build_spec())
            with out:
                out.clear_output()
                for rel, content in files:
                    print("=" * 70)
                    print(rel)
                    print("-" * 70)
                    print(content)
        except Exception as e:
            _log(f"Ошибка предпросмотра: {type(e).__name__}: {e}")

    def on_make(_):
        try:
            if not state["rows"]:
                _log("Сначала сними структуры (кнопка 1)."); return
            files = B.build_all(_build_spec())
            written = B.write_files(files, overwrite=overwrite.value)
            with out:
                out.clear_output()
                print("Создано (и конфиг успешно собрался):")
                for p in written:
                    print("  ", os.path.relpath(p, ROOT))
                print("\nДальше выкатить на тест:")
                print("  sh deploy/deploy-test.sh \"новая линия\"")
        except FileExistsError as e:
            _log(f"{e}\nЕсли правишь существующую линию — поставь галочку "
                 f"«перезаписать существующие».")
        except Exception as e:
            _log(f"Ошибка создания: {type(e).__name__}: {e}")

    btn_snap.on_click(on_snap)
    btn_prev.on_click(on_preview)
    btn_make.on_click(on_make)

    header = W.VBox([
        W.HTML("<h3>Генератор ETL-линии</h3>"),
        W.HBox([tm, ts]), W.HBox([dbm, dbs, cred]),
        W.HBox([line, dag]), W.HBox([mode, sched]), doc,
        advanced, btn_snap,
    ])
    mapping = W.VBox([W.HTML("<b>Колонки</b> (ведущая → ведомая):"),
                      period_box, map_title, map_box,
                      W.HBox([btn_prev, btn_make, overwrite])])
    display(W.VBox([header, W.HTML("<hr>"), mapping, W.HTML("<hr>"), out]))
