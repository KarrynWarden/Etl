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


def _rel(p):
    """Путь относительно корня репо для читаемого вывода. Терпит хвост вида
    ' (ключ X)' у строк-описаний удаляемого."""
    p = str(p)
    first = p.split(" ", 1)[0]
    if os.path.isabs(first):
        return os.path.relpath(first, ROOT) + p[len(first):]
    return p


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_MODE_HELP_HTML = """
<div style='max-height:420px;overflow:auto;padding:8px;line-height:1.45'>
<b>Режимы переноса (ключ <code>mode</code> в конфиге линии)</b>

<p style='margin:10px 0 4px'><b><code>iud</code></b> — точечные обновления
(по умолчанию).<br>
Триггер на ведущей пишет каждое изменение в <code>etl_log_iud_row</code>
(<code>oper IN ('IU','D')</code>, <code>isetl=0</code>). Процесс читает
необработанные строки и точечно переносит их в ведомую через UPSERT/MERGE,
затем ставит <code>isetl=1</code>.<br>
<i>Когда:</i> обычная таблица с триггером, изменений немного, нужен быстрый
перенос по факту события. <i>Требует:</i> триггер на ведущей.</p>

<p style='margin:10px 0 4px'><b><code>section</code></b> — срез только по
требованию.<br>
Обрабатываются ТОЛЬКО группы, у которых в <code>etl_jobs</code> проставлен
<code>isokaudit=4</code>. Группа перезаливается целиком (DELETE периода +
заливка).<br>
<i>Когда:</i> нужен ручной триггер массового переобновления периода, автоматика
не нужна.</p>

<p style='margin:10px 0 4px'><b><code>section_compare</code></b> — срез со
сравнением (mocheck, medree).<br>
Сравнивает пары <code>(period, MAX(lastupdate))</code> на ведущей и ведомой,
добавляет группы из <code>etl_log_iud_row</code> и с <code>isokaudit=4</code>;
каждую расходящуюся группу перезаливает целиком.<br>
<i>Когда:</i> данные меняются пачками задним числом, и надёжнее сверять срез,
чем ловить каждое событие. <i>Дорого:</i> сканирует источник.</p>

<p style='margin:10px 0 4px'><b><code>delete_insert</code></b> — событийный, но
«один id = несколько строк» (expmed).<br>
Как <code>iud</code> читает <code>etl_log_iud_row</code>, но на каждое событие
<code>idrw</code> делает DELETE всех строк этого <code>idrw</code> в ведомой и
(для <code>IU</code>) INSERT актуальных.<br>
<i>Когда:</i> одна запись-источник даёт несколько строк ведомой и при смене
атрибута (напр. <code>doctype</code>) остаются «осиротевшие» строки, которые
upsert не убирает.</p>

<p style='margin:10px 0 4px'><b><code>query_section</code></b> — группы из
своего запроса (Medree_prdisp).<br>
Список групп берётся из пользовательского <code>periodsSql</code> (возвращает
пары <code>year, month</code>), каждая группа обновляется полной перезаписью:
DELETE <code>(year, month)</code> в ведомой + заливка той же группы из ведущей.
Задаётся ещё <code>periodYearColumn</code>/<code>periodMonthColumn</code>
(по умолч. <code>year</code>/<code>month</code>).<br>
<i>Когда:</i> периода-даты нет (только year+month), а какие периоды пересчитаны —
знает ваш SQL. <code>etl_jobs</code> режим не ведёт → ставьте
<code>skipAudit</code>. <i>Не требует</i> триггера на ведущей.</p>
</div>
"""

_CRON_HELP_HTML = """
<div style='max-height:420px;overflow:auto;padding:8px;line-height:1.45'>
<b>Как заполнять cron</b>

<p style='margin:8px 0 4px'>Пять полей через пробел:</p>
<pre style='margin:4px 0'>┌ минута (0-59)
│ ┌ час (0-23)
│ │ ┌ день месяца (1-31)
│ │ │ ┌ месяц (1-12)
│ │ │ │ ┌ день недели (0-6, 0 = воскресенье)
│ │ │ │ │
*  *  *  *  *</pre>

<p style='margin:8px 0 4px'><b>Символы:</b>
<code>*</code> — любое значение;
<code>,</code> — перечисление (<code>11,13,20</code>);
<code>-</code> — диапазон (<code>9-18</code>);
<code>/</code> — шаг (<code>*/15</code> — каждые 15).</p>

<p style='margin:8px 0 4px'><b>Примеры:</b></p>
<table style='border-collapse:collapse'>
<tr><td style='padding:2px 10px 2px 0'><code>*/5 * * * *</code></td><td>каждые 5 минут</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>0 * * * *</code></td><td>каждый час в :00</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>50 11,13,20 * * *</code></td><td>в 11:50, 13:50 и 20:50</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>0 3 * * *</code></td><td>каждый день в 03:00</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>30 2 * * 1-5</code></td><td>по будням в 02:30</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>0 0 1 * *</code></td><td>1-го числа каждого месяца в 00:00</td></tr>
<tr><td style='padding:2px 10px 2px 0'><code>0 8-18/2 * * *</code></td><td>с 8 до 18 каждые 2 часа</td></tr>
</table>

<p style='margin:10px 0 4px'><b>Важно:</b> одно поле = ОДНО выражение. Нельзя
задать «11:20 и 13:50» одной строкой (разные минуты) — для этого либо режим
«В заданные часы» (одинаковая минута), либо два DAG'а.</p>

<p style='margin:8px 0 4px'><b>Часовой пояс</b> — как настроен Airflow
(<code>timezone</code> в конфиге/DAG'ах проекта — Asia/Yekaterinburg).
Проверить, что вы имели в виду, удобно на crontab.guru.</p>
</div>
"""


def _help_toggle(W, html, width="820px"):
    """Кнопка «ℹ» + сворачиваемая панель со справкой.

    Настоящего модального окна в ipywidgets нет (и в Voilà оно бы не всплыло),
    поэтому справка раскрывается прямо под полем — работает одинаково в Jupyter
    и Voilà. Возвращает (btn, box)."""
    btn = W.Button(description="", icon="info-circle", tooltip="Подробнее",
                   layout=W.Layout(width="42px"))
    box = W.HTML(html, layout=W.Layout(display="none", width=width,
                                       border="1px solid #17a2b8", margin="4px 0"))

    def _toggle(_):
        box.layout.display = "none" if box.layout.display == "" else ""
    btn.on_click(_toggle)
    return btn, box


def _spinner_html(text):
    """HTML с CSS-спиннером. Анимация чисто клиентская (@keyframes), поэтому
    крутится даже пока ядро занято синхронным git push — мгновенный фидбек, что
    операция идёт, и не создаётся ощущение «кнопка не нажалась»."""
    return ("<div style='display:flex;align-items:center;gap:8px;color:#333'>"
            "<span style='display:inline-block;width:16px;height:16px;"
            "border:3px solid #cfd8dc;border-top-color:#1976d2;border-radius:50%;"
            "animation:ccspin 0.8s linear infinite'></span>"
            f"<b>{_esc(text)}</b>"
            "<style>@keyframes ccspin{to{transform:rotate(360deg)}}</style></div>")


def _push_only_controls(W, out, message_fn=None):
    """Кнопка «Просто запушить» + подтверждение: коммит+push УЖЕ сохранённых на
    диске изменений (etlFolder/, dags/), без генерации файлов из текущей формы.
    Показывает список изменений перед выкладкой."""
    message_fn = message_fn or (lambda: "dagbuilder: выложить сохранённые изменения")
    info = W.HTML()
    btn_yes = W.Button(description="✅ Да, запушить сохранённое", button_style="danger",
                       layout=W.Layout(width="260px"))
    btn_no = W.Button(description="Отмена", layout=W.Layout(width="120px"))
    confirm = W.VBox([info, W.HBox([btn_yes, btn_no])],
                     layout=W.Layout(display="none", border="1px solid #17a2b8",
                                     padding="6px", margin="4px 0"))
    # button_style="info" — «Просто запушить» выделено цветом как важное действие
    btn = W.Button(description="Просто запушить", icon="upload", button_style="info",
                   layout=W.Layout(width="200px"))

    def _show(_):
        changed = B.git_status_short()
        if not changed.strip():
            with out:
                out.clear_output()
                print("Нечего пушить: в etlFolder/ и dags/ нет сохранённых изменений.")
            return
        br = B.current_branch() or "— (detached)"
        info.value = (
            "<b>Запушить сохранённые изменения?</b> Будет <code>git commit</code> и "
            f"<code>git push origin {br}</code>. Текущая несохранённая форма НЕ "
            "затрагивается. Будет выложено:"
            f"<pre style='max-height:220px;overflow:auto'>{_esc(changed)}</pre>")
        confirm.layout.display = ""

    def _hide(_):
        confirm.layout.display = "none"

    def _go(_):
        from IPython.display import display, HTML
        confirm.layout.display = "none"
        # блокируем кнопки и показываем спиннер, пока идёт push (несколько секунд)
        for b in (btn, btn_yes, btn_no):
            b.disabled = True
        with out:
            out.clear_output()
            display(HTML(_spinner_html("Выкладываю в git — подождите…")))
        try:
            ok, log = B.git_push_saved(message_fn())
        finally:
            for b in (btn, btn_yes, btn_no):
                b.disabled = False
        with out:
            out.clear_output()
            print("✅ Запушено:" if ok else "⚠ Не запушено:")
            print(log)

    btn.on_click(_show)
    btn_yes.on_click(_go)
    btn_no.on_click(_hide)
    return btn, confirm


def _delete_controls(W, out, list_fn, targets_fn, delete_fn, after_fn, label="линию"):
    """Выпадашка + кнопка «🗑 Удалить» + двухшаговое подтверждение со списком
    файлов, которые будут удалены. list_fn()->ключи, targets_fn(key)->пути,
    delete_fn(key)->удалённые, after_fn() дергается после удаления (обновить
    связанные списки). Возвращает (pick, btn, confirm, refresh_fn)."""
    pick = W.Dropdown(description="Линия", options=[],
                      layout=W.Layout(width="380px"),
                      style={"description_width": "90px"})
    btn = W.Button(description=f"🗑 Удалить {label}", button_style="danger",
                   layout=W.Layout(width="240px"))
    info = W.HTML()
    btn_yes = W.Button(description="⚠️ Да, удалить насовсем", button_style="danger",
                       layout=W.Layout(width="260px"))
    btn_no = W.Button(description="Отмена", layout=W.Layout(width="120px"))
    confirm = W.VBox([info, W.HBox([btn_yes, btn_no])],
                     layout=W.Layout(display="none", border="2px solid #dc3545",
                                     padding="6px", margin="4px 0"))

    def _refresh():
        pick.options = list_fn()

    def _show(_):
        key = pick.value
        if not key:
            with out:
                out.clear_output(); print("Выбери линию для удаления.")
            return
        try:
            targets = targets_fn(key)
        except Exception as e:
            with out:
                out.clear_output(); print(f"Не удалось собрать список: {type(e).__name__}: {e}")
            return
        lst = "\n".join("  " + _rel(t) for t in targets) or "  (файлы не найдены)"
        info.value = (
            f"<b>Удалить линию «{_esc(key)}» НАСОВСЕМ?</b> Будут удалены файлы:"
            f"<pre style='max-height:220px;overflow:auto'>{_esc(lst)}</pre>"
            "Действие необратимо на диске (пока не запушено — восстановимо из git). "
            "Общие структуры/SQL, используемые другими линиями, не трогаются.")
        confirm.layout.display = ""

    def _hide(_):
        confirm.layout.display = "none"

    def _go(_):
        confirm.layout.display = "none"
        key = pick.value
        try:
            removed = delete_fn(key)
        except Exception as e:
            with out:
                out.clear_output(); print(f"Ошибка удаления: {type(e).__name__}: {e}")
            return
        _refresh()
        after_fn()
        with out:
            out.clear_output()
            print(f"Линия «{key}» удалена. Удалено:")
            for r in removed:
                print("  ", _rel(r))
            print("\nВыложи удаление: кнопка «Просто запушить» или обычный деплой.")

    btn.on_click(_show)
    btn_yes.on_click(_go)
    btn_no.on_click(_hide)
    return pick, btn, confirm, _refresh


def _publish_controls(W, out, do_write, message_fn):
    """Кнопка «создать и запушить» + подтверждение (защита от мисклика).

    do_write() пишет файлы линии и возвращает список абсолютных путей (может
    бросить исключение). message_fn() даёт текст коммита. Возвращает
    (btn_publish, confirm_box) — их размещает вызывающий UI. После подтверждения
    файлы пишутся и делается git commit + push текущей ветки (B.git_commit_push).
    """
    info = W.HTML()
    btn_yes = W.Button(description="✅ Да, выложить в git", button_style="danger",
                       layout=W.Layout(width="240px"))
    btn_no = W.Button(description="Отмена", layout=W.Layout(width="120px"))
    confirm = W.VBox([info, W.HBox([btn_yes, btn_no])],
                     layout=W.Layout(display="none", border="1px solid #e0a800",
                                     padding="6px", margin="4px 0"))
    btn_pub = W.Button(description="Создать и запушить", icon="cloud-upload",
                       button_style="warning", layout=W.Layout(width="260px"))

    def _show(_):
        br = B.current_branch() or "— (detached / не git)"
        info.value = (
            "<b>Выложить в git?</b> Файлы будут созданы, затем "
            f"<code>git commit</code> и <code>git push origin {br}</code>. "
            "Это выкатывает изменения на удалёнку — сверься с предпросмотром. "
            "Нажми «Да», только если уверен.")
        confirm.layout.display = ""

    def _hide(_):
        confirm.layout.display = "none"

    def _go(_):
        from IPython.display import display, HTML
        confirm.layout.display = "none"
        for b in (btn_pub, btn_yes, btn_no):
            b.disabled = True
        with out:
            out.clear_output()
            display(HTML(_spinner_html("Создаю файлы и выкладываю в git — подождите…")))
        try:
            written = do_write()
        except Exception as e:
            for b in (btn_pub, btn_yes, btn_no):
                b.disabled = False
            with out:
                out.clear_output()
                print(f"Не выложено — ошибка при создании файлов: "
                      f"{type(e).__name__}: {e}")
            return
        try:
            ok, log = B.git_commit_push(written, message_fn())
        finally:
            for b in (btn_pub, btn_yes, btn_no):
                b.disabled = False
        with out:
            out.clear_output()
            print("Файлы созданы:")
            for p in written:
                print("  ", os.path.relpath(p, ROOT))
            print("\n" + ("✅ Выложено в git:" if ok
                          else "⚠ Файлы созданы, но НЕ выложены:"))
            print(log)

    btn_pub.on_click(_show)
    btn_yes.on_click(_go)
    btn_no.on_click(_hide)
    return btn_pub, confirm


def _complex_ui():
    """Построить виджет конструктора «сложного» ETL (ведёт себя как раньше).
    Возвращает VBox — его встраивают во вкладку в launch()."""
    import ipywidgets as W

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
                      options=["iud", "section", "section_compare", "delete_insert",
                               "query_section"],
                      layout=W.Layout(width="280px"), style={"description_width": "110px"})
    mode_info_btn, mode_info_box = _help_toggle(W, _MODE_HELP_HTML)
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
    cron_info_btn, cron_info_box = _help_toggle(W, _CRON_HELP_HTML)
    sched_help = W.HTML("<span style='color:#888'>Интервал — для frequent. "
                        "«Часы:мин» — несколько запусков в день с одинаковой минутой "
                        "(напр. 11:50, 13:50, 20:50). Cron — для сложных случаев "
                        "(жми ℹ рядом с полем cron).</span>")
    sched_inputs = W.VBox([sched_minutes])

    def _on_sched_kind(_=None):
        kind = _SCHED_KINDS[sched_kind.value]
        sched_inputs.children = {
            "interval": [sched_minutes],
            "times": [sched_times],
            # у cron — кнопка ℹ со шпаргалкой по формату
            "cron": [W.HBox([sched_cron, cron_info_btn]), cron_info_box],
        }[kind]
    sched_kind.observe(_on_sched_kind, names="value")

    # ── теги (несколько; из существующих + новые) ──
    # По умолчанию проставляются 3 тега: направление, имя таблицы, базовый DbSync.
    # Авто-теги обновляются, пока пользователь не тронул поле руками.
    # Видимая рамка — иначе на белом фоне поле тегов незаметно. Новый тег вводится
    # прямо здесь: печатаешь и жмёшь Enter.
    tags_input = W.TagsInput(value=[], allow_duplicates=False,
                             layout=W.Layout(width="640px", min_height="34px",
                                             border="1px solid #ccc", padding="2px"))
    # Dropdown показывает существующие теги сразу по клику (без ввода буквы).
    tag_pick = W.Dropdown(options=[""] + B.existing_tags(),
                          layout=W.Layout(width="320px"))
    btn_add_tag = W.Button(description="+ выбранный", layout=W.Layout(width="140px"))

    def _add_tag(_):
        t = (tag_pick.value or "").strip()
        if t:
            tags_input.value = list(dict.fromkeys(list(tags_input.value) + [t]))
            tag_pick.value = ""
    btn_add_tag.on_click(_add_tag)

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
    # ── поля режима query_section (группы (year, month) из своего запроса) ──
    f_periods = W.Textarea(description="SQL периодов (query_section)",
                           placeholder="Запрос групп для перезаливки. Возвращает две "
                                       "колонки: year, month. Файл .sql создастся сам, "
                                       "путь пропишется в periodsSql.",
                           layout=W.Layout(width="760px", height="100px"),
                           style={"description_width": "180px"})
    f_period_year = W.Text(description="Колонка year", value="year",
                           layout=W.Layout(width="360px"),
                           style={"description_width": "180px"})
    f_period_month = W.Text(description="Колонка month", value="month",
                            layout=W.Layout(width="360px"),
                            style={"description_width": "180px"})
    adv_help = W.HTML(
        "<span style='color:#888'>"
        "<b>Фильтр ведущей/ведомой</b> — доп. условие WHERE (например <code>doctype = 7</code>).<br>"
        "<b>SQL ведущей</b> — если перенос идёт не из таблицы, а из своего запроса.<br>"
        "<b>Конфликт: доп. колонки</b> — лишние колонки в ON CONFLICT (upsert).<br>"
        "<b>Конфликт: условие WHERE</b> — для частичного уникального индекса.<br>"
        "<b>truncatePeriod</b> — сравнивать период по дате, без времени.<br>"
        "<b>skipAudit</b> — исключить линию из общего аудита (AuditAll).<br>"
        "<b>Не сверять поля</b> — auditExcludeFields: колонки, которые аудит игнорирует.<br>"
        "<b>SQL периодов / Колонка year / month</b> — только для mode=<code>query_section</code>: "
        "запрос возвращает пары (year, month), каждая группа перезаливается целиком "
        "(DELETE (year,month) + заливка).</span>")
    advanced = W.Accordion(children=[W.VBox(
        [adv_help, f_filter, f_filter_s, f_sql, f_sql_name, f_confl, f_confl_w,
         f_trunc, f_skip_audit, f_audit_excl,
         f_periods, W.HBox([f_period_year, f_period_month])])])
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

    btn_snap = W.Button(description="Снять структуры из БД", button_style="primary",
                        icon="download", layout=W.Layout(width="260px"))
    btn_prev = W.Button(description="Предпросмотр", icon="eye",
                        layout=W.Layout(width="200px"))
    btn_make = W.Button(description="Создать файлы", button_style="success",
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
        btn_make.description = ("Сохранить изменения" if m == "edit"
                                else "Создать файлы")
        if m == "new":
            # уходя из правки, забыть исходные пути структур, чтобы новая линия
            # не записалась поверх чужих файлов; вернуть авто-теги
            state["struct_master_rel"] = state["struct_slave_rel"] = None
            state["tags_auto"] = True
            _refresh_default_tags()
        elif m == "archive":
            _refresh_archive_lists()
            del_refresh()
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
        if mode.value == "query_section":
            extra["periodYearColumn"] = f_period_year.value.strip() or "year"
            extra["periodMonthColumn"] = f_period_month.value.strip() or "month"

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
            "periods_sql_text": f_periods.value.strip() or None,
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
            f_periods.value = data.get("periods_sql_text") or ""
            f_period_year.value = ex.get("periodYearColumn", "year") or "year"
            f_period_month.value = ex.get("periodMonthColumn", "month") or "month"

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
                _log("Сначала сними структуры («Снять структуры из БД») или загрузи линию."); return
            files = B.build_all(_build_spec())
            with out:
                out.clear_output()
                for rel, content in files:
                    print("=" * 70); print(rel); print("-" * 70); print(content)
        except Exception as e:
            _log(f"Ошибка предпросмотра: {type(e).__name__}: {e}")

    def _write_current():
        """Собрать и записать файлы линии. Возвращает список путей. Бросает при
        пустом сопоставлении/конфликте имён — вызывающий показывает ошибку."""
        if not state["rows"]:
            raise RuntimeError("Сначала сними структуры («Снять структуры из БД») или загрузи линию.")
        files = B.build_all(_build_spec())
        # в режиме правки перезаписываем существующие файлы намеренно
        return B.write_files(files, overwrite=(work_mode.value == "edit"))

    def on_make(_):
        try:
            written = _write_current()
            with out:
                out.clear_output()
                print("Готово (и конфиг успешно собрался):")
                for p in written:
                    print("  ", os.path.relpath(p, ROOT))
                print("\nДальше выкатить на тест:")
                print("  sh deploy/deploy-test.sh \"новая линия\"")
                print("(или кнопка «Создать и запушить» — сразу в git)")
        except FileExistsError as e:
            _log(f"{e}\nТакая линия уже есть. Чтобы изменить её — переключись в режим "
                 f"«✏️ Редактировать» и выбери её из списка.")
        except Exception as e:
            _log(f"Ошибка создания: {type(e).__name__}: {e}")

    def _commit_msg():
        ln = line.value.strip() or (B.bare(tm.value.strip()) if tm.value.strip()
                                    else "линия")
        return f"dagbuilder: {'правка' if work_mode.value == 'edit' else 'новая'} линия {ln}"

    btn_pub, pub_confirm = _publish_controls(W, out, _write_current, _commit_msg)
    btn_push_only, push_only_confirm = _push_only_controls(W, out)
    btn_pub.layout.margin = "0 0 0 40px"   # «Создать и запушить» — сбоку, отдельно

    # удаление линии насовсем (живёт в панели архива — рядом со «скрыть»)
    def _after_delete_complex():
        edit_pick.options = B.existing_lines()
        _refresh_archive_lists()
    del_pick, del_btn, del_confirm, del_refresh = _delete_controls(
        W, out, B.existing_lines, B.line_delete_targets, B.delete_line,
        _after_delete_complex, label="линию ETL")
    del_help = W.HTML(
        "<span style='color:#888'><b>Удалить насовсем</b> (не архив): убирает фрагмент "
        "config.d, файл дага и — если не используются другими линиями — структуры и "
        "selectSql. Затем выложи «Просто запушить» или деплоем.</span>")
    # отдельная «Просто запушить» для панели архива (один виджет-кнопку нельзя
    # разместить в двух контейнерах) — иначе после архива/удаления её тут нет
    arch_btn_push, arch_push_confirm = _push_only_controls(W, out)
    archive_box.children = tuple(archive_box.children) + (
        W.HTML("<hr>"), del_help, W.HBox([del_pick, del_btn]), del_confirm,
        W.HTML("<hr>"),
        W.HTML("<b>Выложить сделанное</b> (архив/восстановление/удаление меняют "
               "файлы на диске — их нужно запушить):"),
        W.HBox([arch_btn_push]), arch_push_confirm)

    # автозаполнение парного имени таблицы с приведением регистра под БД
    btn_case = W.Button(description="⇄ имя по регистру БД", icon="magic",
                        layout=W.Layout(width="230px"))
    case_hint = W.HTML("<span style='color:#888'>заполнит второе имя по первому, "
                       "приведя регистр под БД (Oracle — ВЕРХНИЙ, Postgres — нижний). "
                       "Схему при необходимости допиши вручную.</span>")

    def _fill_paired_name(_):
        m, s = tm.value.strip(), ts.value.strip()
        if m and not s:
            ts.value = B.to_db_case(m, dbs.value)
        elif s and not m:
            tm.value = B.to_db_case(s, dbm.value)
        elif m:
            ts.value = B.to_db_case(m, dbs.value)   # оба заданы -> ведомая по ведущей
        else:
            _log("Заполни хотя бы одно имя таблицы, потом жми «⇄ имя по регистру БД».")
    btn_case.on_click(_fill_paired_name)

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
        W.HBox([tm, ts]), W.HBox([btn_case, case_hint]),
        W.HBox([dbm, dbs]), W.HBox([user_m_box, user_s_box]),
        W.HBox([line, dag]), dag_preview,
        W.HBox([mode, mode_info_btn, retry]), mode_info_box, doc,
        W.HTML("<b>Расписание и ретраи</b>"), sched_kind, sched_inputs, sched_help,
        W.HTML("<b>Теги</b> (несколько). Новый тег впиши прямо в поле ниже и нажми "
               "Enter. Существующий — выбери из списка и «+ выбранный»:"),
        tags_input, W.HBox([tag_pick, btn_add_tag]),
        advanced, btn_snap,
        W.HTML("<hr>"),
        W.HTML("<b>Колонки</b> (ведущая → ведомая; отметь PK, составной ключ — "
               "несколько галочек):"),
        period_box, W.HBox([map_title, hide_unmapped]), map_head, map_box,
        W.HBox([btn_prev, btn_make, btn_push_only, btn_pub]),
        pub_confirm, push_only_confirm,
    ])

    _refresh_default_tags()   # проставить 3 тега по умолчанию
    _on_work_mode()           # выставить видимость по текущему режиму

    return W.VBox([
        W.HTML("<h3>Сложный ETL (свой даг на линию)</h3>"),
        work_mode, edit_box, archive_box, W.HTML("<hr>"),
        build_area, W.HTML("<hr>"), out,
    ])


def _sp_ui():
    """Конструктор ETL справочников и разового переноса (delete+insert).

    Общая логика на оба типа (regular/once); отличается лишь каталог фрагмента
    и даг, который их гоняет. Возвращает VBox для вкладки в launch()."""
    import ipywidgets as W
    from tools import sp_builder as SP  # noqa: E402

    state = {"master_cols": [], "slave_cols": [], "rows": [], "row_widgets": [],
             "_opts_guard": False}

    # ── тип линии и режим работы ──
    kind = W.ToggleButtons(
        options=[("📚 Справочник (регулярный)", "regular"),
                 ("🔂 Разовый перенос", "once")],
        value="regular", style={"button_width": "230px"})
    work_mode = W.ToggleButtons(
        options=[("➕ Создать", "new"), ("✏️ Редактировать", "edit"),
                 ("🚦 Вкл/выкл таблицу", "toggle")],
        value="new", style={"button_width": "190px"})

    # ── правка: выбор существующей линии ──
    edit_pick = W.Dropdown(description="Линия", options=[],
                           layout=W.Layout(width="380px"),
                           style={"description_width": "90px"})
    btn_load = W.Button(description="Загрузить линию", icon="upload",
                        layout=W.Layout(width="200px"))
    edit_box = W.HBox([edit_pick, btn_load])

    # ── панель включения/выключения (не удаляя) ──
    tgl_active = W.Dropdown(description="Включённая", options=[],
                            layout=W.Layout(width="380px"),
                            style={"description_width": "110px"})
    btn_disable = W.Button(description="🚫 Отключить", button_style="warning",
                           layout=W.Layout(width="180px"))
    tgl_disabled = W.Dropdown(description="Отключённая", options=[],
                              layout=W.Layout(width="380px"),
                              style={"description_width": "110px"})
    btn_enable = W.Button(description="✅ Включить", button_style="success",
                          layout=W.Layout(width="180px"))
    tgl_help = W.HTML(
        "<span style='color:#888'>Отключение <b>не удаляет</b> таблицу: во фрагмент "
        "ставится <code>disabled: true</code>, и регулярный даг справочников "
        "(<code>SpEtlNew</code>) её пропускает. Конфиг и SQL остаются — включить "
        "обратно можно тут же. Действует <b>после деплоя</b>.</span>")
    # перевод линии между типами (разовый <-> регулярный) без пересборки
    move_pick = W.Dropdown(description="Линия", options=[],
                           layout=W.Layout(width="380px"),
                           style={"description_width": "110px"})
    btn_move = W.Button(description="→ Перевести", button_style="info",
                        layout=W.Layout(width="260px"))
    move_help = W.HTML(
        "<span style='color:#888'><b>Перевод между типами без пересборки.</b> "
        "Частый случай: справочник держали в <b>разовом</b> переносе ради данных "
        "для разработки, а теперь нужен <b>регулярный</b> режим. Фрагмент переезжает "
        "в другой каталог, SQL (queries/sp/…) остаётся на месте — заново настраивать "
        "перенос не нужно.</span>")
    toggle_box = W.VBox([tgl_help, W.HBox([tgl_active, btn_disable]),
                         W.HBox([tgl_disabled, btn_enable]),
                         W.HTML("<hr>"), move_help, W.HBox([move_pick, btn_move])])

    def _refresh_toggle_lists():
        tgl_active.options = SP.list_active_sp_lines(kind.value)
        tgl_disabled.options = SP.list_disabled_sp_lines(kind.value)
        move_pick.options = SP.list_sp_lines(kind.value)
        other = "разовый" if kind.value == "regular" else "регулярный"
        btn_move.description = f"→ Перевести в {other}"

    # ── шапка-форма ──
    tm = W.Text(description="Ведущая", placeholder="SPMKB  или  KOKNAEV.SPMKB",
                layout=W.Layout(width="380px"), style={"description_width": "110px"})
    ts = W.Text(description="Ведомая", placeholder="spmkb  или  KOKNAEV.spmkb",
                layout=W.Layout(width="380px"), style={"description_width": "110px"})
    dbm = W.Dropdown(description="БД ведущей", options=["Orcl", "Post"], value="Orcl",
                     layout=W.Layout(width="220px"), style={"description_width": "110px"})
    dbs = W.Dropdown(description="БД ведомой", options=["Post", "Orcl"], value="Post",
                     layout=W.Layout(width="220px"), style={"description_width": "110px"})

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

    line = W.Text(description="Метка линии", placeholder="по умолч. = имя ведущей",
                  layout=W.Layout(width="380px"), style={"description_width": "110px"})
    key_preview = W.HTML("")
    dependence = W.Text(description="Зависимость", placeholder="напр. SPACC (необязательно)",
                        layout=W.Layout(width="380px"), style={"description_width": "110px"})
    doc = W.Text(description="Комментарий", placeholder="_doc: краткое описание",
                 layout=W.Layout(width="640px"), style={"description_width": "110px"})

    # ── источник SELECT ──
    src_mode = W.ToggleButtons(
        options=[("Из таблицы (авто SELECT)", "table"),
                 ("Свой SELECT-запрос", "custom")],
        value="table", style={"button_width": "230px"})
    src_help = W.HTML(
        "<span style='color:#888'><b>Из таблицы</b> — назови ведущую, SELECT и INSERT "
        "соберутся сами по сопоставленным колонкам. <b>Свой SELECT</b> — вставь запрос; "
        "колонки снимутся из курсора, сопоставишь их со столбцами ведомой, получишь "
        "INSERT. Регистр имён из БД (Oracle — ВЕРХНИЙ, Postgres — нижний) учитывается "
        "автоматически.</span>")
    f_sql = W.Textarea(description="SELECT ведущей",
                       placeholder="SELECT a.ID, a.NAME FROM KOKNAEV.IPERSON a WHERE a.ACT = 1",
                       layout=W.Layout(width="760px", height="120px"),
                       style={"description_width": "110px"})
    sql_box = W.VBox([f_sql])

    # режим разового переноса: перенести (очистить+залить) / дополнить (без очистки)
    once_mode = W.ToggleButtons(
        options=[("Перенести (очистить + залить)", "replace"),
                 ("Дополнить (без очистки)", "append")],
        value="replace", style={"button_width": "260px"})
    once_mode_help = W.HTML(
        "<span style='color:#888'><b>Перенести</b> — ведомая очищается и заливается "
        "заново. <b>Дополнить</b> — ведомая НЕ очищается, строки дописываются (SELECT "
        "обычно с WHERE на нужную часть). Типичный сценарий: сперва «Перенести», затем "
        "тем же переносом «Дополнить» с другим SELECT — ведомая и INSERT те же, "
        "меняется только запрос.</span>")
    once_mode_box = W.VBox([W.HTML("<b>Режим разового переноса</b>"),
                            once_mode, once_mode_help])

    def _on_src_mode(_=None):
        custom = src_mode.value == "custom"
        sql_box.layout.display = "" if custom else "none"
        # В режиме «свой SELECT» имя ведущей нужно лишь как метка линии (ключ),
        # структуру берём из запроса. Подсказываем это подписью поля.
        tm.description = "Метка (ведущая)" if custom else "Ведущая"
        tm.placeholder = ("имя для ключа линии, напр. ipersonact" if custom
                          else "SPMKB  или  KOKNAEV.SPMKB")
    src_mode.observe(_on_src_mode, names="value")

    # ── зоны сопоставления/вывода ──
    map_title = W.HTML("")
    hide_unmapped = W.Checkbox(description="Скрывать непривязанные", value=False,
                               indent=False)
    map_head = W.HBox([], layout=W.Layout(align_items="center",
                                          border_bottom="2px solid #bbb", padding="2px"))
    map_box = W.VBox([], layout=W.Layout(max_height="420px", overflow="auto",
                                         border="1px solid #ddd", padding="4px"))
    out = W.Output()

    btn_snap = W.Button(description="Снять колонки", button_style="primary",
                        icon="download", layout=W.Layout(width="260px"))
    btn_prev = W.Button(description="Предпросмотр", icon="eye",
                        layout=W.Layout(width="200px"))
    btn_make = W.Button(description="Создать файлы", button_style="success",
                        icon="check", layout=W.Layout(width="220px"))

    def _log(msg, clear=True):
        with out:
            if clear:
                out.clear_output()
            print(msg)

    # ── предпросмотр ключа линии ──
    def _update_key_preview(_=None):
        lbl = line.value.strip() or (B.bare(tm.value.strip()) if tm.value.strip() else "")
        if lbl:
            key_preview.value = ("<span style='color:#888'>ключ линии: "
                                 f"<code>{SP.sp_key(lbl, dbm.value, dbs.value)}</code></span>")
        else:
            key_preview.value = ("<span style='color:#888'>ключ появится, когда "
                                 "заполнишь ведущую/метку</span>")
    for _w in (tm, line, dbm, dbs):
        _w.observe(_update_key_preview, names="value")

    # ── связь 1:1 в выпадашках (не назначать одну ведомую дважды) ──
    def _refresh_slave_options():
        if state.get("_opts_guard"):
            return
        all_s = [c["column_name"] for c in state["slave_cols"]]
        used = {dd.value for _m, dd in state["rows"] if dd.value != _NONE}
        state["_opts_guard"] = True
        try:
            for _m, dd in state["rows"]:
                cur = dd.value
                opts = [_NONE] + [s for s in all_s if s == cur or s not in used]
                if list(dd.options) != opts:
                    dd.options = opts
                    dd.value = cur
        finally:
            state["_opts_guard"] = False

    def _apply_row_filter(_=None):
        widgets = state.get("row_widgets", [])
        if hide_unmapped.value:
            map_box.children = [w for (m, dd), w in zip(state["rows"], widgets)
                                if dd.value != _NONE]
        else:
            map_box.children = widgets
    hide_unmapped.observe(_apply_row_filter, names="value")

    def _on_pair_change(_=None):
        _refresh_slave_options()
        _apply_row_filter()

    def _render_mapping(mcols, scols, pair_map=None):
        m_lbl = (tm.value.strip() or "запрос ведущей")
        s_lbl = (ts.value.strip() or "ведомая")
        map_head.children = [
            W.HTML(f"<b>ВЕДУЩАЯ</b> · <code>{m_lbl}</code> "
                   f"<span style='color:#888'>[{dbm.value}]</span>",
                   layout=W.Layout(width="324px")),
            W.HTML(f"<b>ВЕДОМАЯ</b> · <code>{s_lbl}</code> "
                   f"<span style='color:#888'>[{dbs.value}]</span>",
                   layout=W.Layout(width="280px")),
        ]
        slave_opts = [_NONE] + [c["column_name"] for c in scols]
        rows, widgets = [], []
        for mcol in mcols:
            name = mcol["column_name"]
            sval = pair_map.get(name) if pair_map else None
            dtype = mcol.get("data_type") or ""
            lbl = W.HTML(f"<code>{name}</code> <span style='color:#888'>{dtype}</span>",
                         layout=W.Layout(width="300px"))
            dd = W.Dropdown(options=slave_opts,
                            value=sval if sval in slave_opts else _NONE,
                            layout=W.Layout(width="280px"))
            dd.observe(_on_pair_change, names="value")
            rows.append((mcol, dd))
            widgets.append(W.HBox(
                [lbl, W.HTML("→", layout=W.Layout(width="24px")), dd],
                layout=W.Layout(align_items="center", min_height="40px",
                                padding="4px 2px", border_bottom="1px solid #eee")))
        state["rows"] = rows
        state["row_widgets"] = widgets
        _refresh_slave_options()
        _apply_row_filter()

    def _collect_pairs():
        return [(mcol["column_name"], None if dd.value == _NONE else dd.value)
                for mcol, dd in state["rows"]]

    def _build_spec():
        return {
            "kind": kind.value,
            "master_table": tm.value.strip(),
            "slave_table": ts.value.strip(),
            "db_master": dbm.value, "db_slave": dbs.value,
            "master_label": line.value.strip() or None,
            "select_mode": src_mode.value,
            "select_sql_text": f_sql.value.strip() or None,
            "pairs": _collect_pairs(),
            "dependence": dependence.value.strip() or None,
            "append": (kind.value == "once" and once_mode.value == "append"),
            "doc": doc.value.strip() or None,
        }

    # ── снять колонки ──
    def on_snap(_):
        try:
            if not ts.value.strip():
                _log("Заполни имя ведомой таблицы."); return
            if src_mode.value == "custom":
                if not f_sql.value.strip():
                    _log("Вставь SELECT-запрос ведущей."); return
                _log("Снимаю колонки запроса и ведомой из БД…")
                mcols = SP.snap_query_columns(dbm.value, f_sql.value.strip(), user_m_get())
            else:
                if not tm.value.strip():
                    _log("Заполни имя ведущей таблицы."); return
                _log("Снимаю структуры ведущей и ведомой из БД…")
                mcols = B.snap_structure(dbm.value, tm.value.strip(), user_m_get())
            scols = B.snap_structure(dbs.value, ts.value.strip(), user_s_get())
            state["master_cols"], state["slave_cols"] = mcols, scols

            sugg, unmatched = B.auto_match(mcols, scols)
            pair_map = {m["column_name"]: s for m, s in zip(mcols, sugg)}
            _render_mapping(mcols, scols, pair_map=pair_map)
            matched = sum(1 for s in sugg if s)
            note = (f"Колонки: ведущая {len(mcols)} / ведомая {len(scols)}, "
                    f"авто-совпало {matched}.")
            if src_mode.value == "custom":
                note += (" <b>Важно:</b> для своего SELECT сопоставь <b>каждую</b> "
                         "колонку запроса — вставка идёт по порядку.")
            if unmatched:
                note += (" Без пары в ведомой: "
                         + ", ".join(f"<code>{u}</code>" for u in unmatched))
            map_title.value = note
            _log("Колонки сняты. Поправь сопоставление и жми «Предпросмотр».")
        except Exception as e:
            _log(f"Ошибка снятия колонок: {type(e).__name__}: {e}")

    # ── загрузить существующую линию ──
    def on_load(_):
        try:
            key = edit_pick.value
            if not key:
                _log("Выбери линию из списка."); return
            data = SP.load_sp_line(kind.value, key)
            tm.value = data["master_table"]
            ts.value = data["slave_table"]
            dbm.value = data["db_master"]
            dbs.value = data["db_slave"]
            line.value = data["master_label"]
            dependence.value = data["dependence"] or ""
            doc.value = data["doc"] or ""
            # если Select.sql — простой SELECT ... FROM t, это режим «из таблицы»
            src_mode.value = "table" if SP._master_from_select(data["select_sql_text"]) \
                else "custom"
            f_sql.value = data["select_sql_text"] if src_mode.value == "custom" else ""
            once_mode.value = "append" if data.get("append") else "replace"
            _on_src_mode()

            # Текущее сопоставление колонок — из сохранённых Select.sql/Add.sql,
            # БЕЗ обращения к БД. Пользователь сразу видит, что и куда льётся.
            mcols, scols, pairs = (data.get("master_cols") or [],
                                   data.get("slave_cols") or [],
                                   data.get("pairs") or [])
            state["master_cols"], state["slave_cols"] = mcols, scols
            if mcols and scols:
                _render_mapping(mcols, scols, pair_map=dict(pairs))
                map_title.value = (
                    f"Загружена линия <code>{key}</code>. Показано <b>текущее</b> "
                    "сопоставление из сохранённого SQL. Правки колонок сохранятся как "
                    "есть; чтобы подтянуть новые столбцы из БД — нажми «Снять колонки».")
            else:
                state["rows"] = []
                map_box.children, map_head.children = [], []
                map_title.value = (
                    f"Загружена линия <code>{key}</code>. Сопоставление из SQL "
                    "распарсить не удалось — нажми «Снять колонки», чтобы собрать его "
                    "из БД.")

            # Показать текущие SELECT и INSERT (то, что реально лежит в файлах линии).
            with out:
                out.clear_output()
                print(f"Линия «{key}» загружена.\n")
                if data.get("select_sql"):
                    print("=" * 70)
                    print(f"ТЕКУЩИЙ SELECT ведущей  ({data['select_sql']})")
                    print("-" * 70)
                    print(data.get("select_sql_text") or "(файл пуст/не найден)")
                    print()
                if data.get("add_sql"):
                    print("=" * 70)
                    print(f"ТЕКУЩИЙ INSERT в ведомую  ({data['add_sql']})")
                    print("-" * 70)
                    print(data.get("add_sql_text") or "(файл пуст/не найден)")
                    print()
                print("Поправь что нужно и нажми «Сохранить изменения».")
        except Exception as e:
            _log(f"Ошибка загрузки линии: {type(e).__name__}: {e}")

    def on_preview(_):
        try:
            if not state["rows"]:
                _log("Сначала сними колонки («Снять колонки»)."); return
            files, key = SP.build_sp_all(_build_spec())
            with out:
                out.clear_output()
                print(f"Ключ линии: {key}\n")
                for rel, content in files:
                    print("=" * 70); print(rel); print("-" * 70); print(content)
        except Exception as e:
            _log(f"Ошибка предпросмотра: {type(e).__name__}: {e}")

    def _write_current():
        """Собрать и записать файлы sp-линии. Возвращает список путей."""
        if not state["rows"]:
            raise RuntimeError("Сначала сними колонки («Снять колонки»).")
        files, _key = SP.build_sp_all(_build_spec())
        validate = SP.SP_KIND_CONFIG[kind.value]
        written = B.write_files(files, overwrite=(work_mode.value == "edit"),
                                validate=validate)
        edit_pick.options = SP.list_sp_lines(kind.value)
        return written

    def on_make(_):
        try:
            written = _write_current()
            with out:
                out.clear_output()
                print(f"Готово (конфиг {SP.SP_KIND_CONFIG[kind.value]} собрался):")
                for p in written:
                    print("  ", os.path.relpath(p, ROOT))
                print("\nДальше выкатить на тест:")
                print("  sh deploy/deploy-test.sh \"справочник/разовый перенос\"")
                print("(или кнопка «Создать и запушить» — сразу в git)")
        except FileExistsError as e:
            _log(f"{e}\nТакая линия уже есть. Переключись в «✏️ Редактировать».")
        except Exception as e:
            _log(f"Ошибка создания: {type(e).__name__}: {e}")

    def _commit_msg():
        lbl = line.value.strip() or (B.bare(tm.value.strip()) if tm.value.strip()
                                     else "линия")
        typ = "справочник" if kind.value == "regular" else "разовый"
        verb = "правка" if work_mode.value == "edit" else "новая"
        return f"dagbuilder ({typ}): {verb} {lbl}"

    btn_pub, pub_confirm = _publish_controls(W, out, _write_current, _commit_msg)

    # автозаполнение парного имени таблицы с приведением регистра под БД
    btn_case = W.Button(description="⇄ имя по регистру БД", icon="magic",
                        layout=W.Layout(width="230px"))
    case_hint = W.HTML("<span style='color:#888'>заполни одно имя и нажми — второе "
                       "подставится с регистром своей БД (Oracle — ВЕРХНИЙ, "
                       "Postgres — нижний). Схему при необходимости допиши.</span>")

    def _fill_paired_name(_):
        m, s = tm.value.strip(), ts.value.strip()
        if m and not s:
            ts.value = B.to_db_case(m, dbs.value)
        elif s and not m:
            tm.value = B.to_db_case(s, dbm.value)
        elif m:
            ts.value = B.to_db_case(m, dbs.value)
        else:
            _log("Заполни хотя бы одно имя таблицы, потом жми «⇄ имя по регистру БД».")
    btn_case.on_click(_fill_paired_name)

    # ── вкл/выкл ──
    def on_disable(_):
        try:
            key = tgl_active.value
            if not key:
                _log("Выбери включённую линию."); return
            SP.set_sp_disabled(kind.value, key, True)
            _refresh_toggle_lists()
            edit_pick.options = SP.list_sp_lines(kind.value)
            _log(f"Линия «{key}» отключена (disabled=true): регулярный даг её пропустит. "
                 "Вступит в силу после деплоя. Включить обратно — тут же.")
        except Exception as e:
            _log(f"Ошибка отключения: {type(e).__name__}: {e}")

    def on_enable(_):
        try:
            key = tgl_disabled.value
            if not key:
                _log("Выбери отключённую линию."); return
            SP.set_sp_disabled(kind.value, key, False)
            _refresh_toggle_lists()
            edit_pick.options = SP.list_sp_lines(kind.value)
            _log(f"Линия «{key}» снова включена. Вступит в силу после деплоя.")
        except Exception as e:
            _log(f"Ошибка включения: {type(e).__name__}: {e}")

    def on_move(_):
        try:
            key = move_pick.value
            if not key:
                _log("Выбери линию для перевода."); return
            to_kind = "once" if kind.value == "regular" else "regular"
            SP.move_sp_line(key, kind.value, to_kind)
            _refresh_toggle_lists()
            edit_pick.options = SP.list_sp_lines(kind.value)
            other = "разовый" if to_kind == "once" else "регулярный"
            _log(f"Линия «{key}» переведена в «{other}». Файлы изменены локально — "
                 "выложи деплоем/пушем. (SQL остались на месте.)")
        except Exception as e:
            _log(f"Ошибка перевода: {type(e).__name__}: {e}")

    btn_snap.on_click(on_snap)
    btn_prev.on_click(on_preview)
    btn_make.on_click(on_make)
    btn_load.on_click(on_load)
    btn_disable.on_click(on_disable)
    btn_enable.on_click(on_enable)
    btn_move.on_click(on_move)

    # push-only для формы сборки и отдельный — для панели вкл/выкл (свой виджет:
    # один и тот же кнопка-виджет нельзя разместить в двух контейнерах)
    btn_push_only, push_only_confirm = _push_only_controls(W, out)
    tgl_btn_push, tgl_push_confirm = _push_only_controls(W, out)
    btn_pub.layout.margin = "0 0 0 40px"   # «Создать и запушить» — сбоку, отдельно

    # удаление sp-линии насовсем (в панели вкл/выкл)
    def _sp_list():
        return SP.list_sp_lines(kind.value)

    def _sp_targets(key):
        frag, sql_dir, shared = SP.sp_line_targets(kind.value, key)
        items = [frag]
        if sql_dir and shared:
            items.append(f"{sql_dir}  (общий с другой линией — НЕ будет удалён)")
        elif sql_dir:
            items.append(sql_dir)
        return items

    def _sp_delete(key):
        return SP.delete_sp_line(kind.value, key)

    def _after_sp_delete():
        edit_pick.options = SP.list_sp_lines(kind.value)
        _refresh_toggle_lists()

    sp_del_pick, sp_del_btn, sp_del_confirm, sp_del_refresh = _delete_controls(
        W, out, _sp_list, _sp_targets, _sp_delete, _after_sp_delete,
        label="линию справочника")
    sp_del_help = W.HTML(
        "<span style='color:#888'><b>Удалить насовсем:</b> убирает фрагмент линии и "
        "её SQL (queries/sp/…), если он не используется другой линией. Затем выложи "
        "«Просто запушить» ниже или деплоем.</span>")
    toggle_box.children = tuple(toggle_box.children) + (
        W.HTML("<hr>"), sp_del_help, W.HBox([sp_del_pick, sp_del_btn]), sp_del_confirm,
        W.HTML("<hr>"),
        W.HTML("<b>Выложить сделанное</b> (вкл/выкл, перевод, удаление меняют файлы "
               "на диске — их нужно запушить):"),
        W.HBox([tgl_btn_push]), tgl_push_confirm)

    # ── видимость по режимам ──
    def _on_mode(_=None):
        m = work_mode.value
        edit_box.layout.display = "" if m == "edit" else "none"
        toggle_box.layout.display = "" if m == "toggle" else "none"
        build_area.layout.display = "none" if m == "toggle" else ""
        btn_make.description = ("Сохранить изменения" if m == "edit"
                                else "Создать файлы")
        if m == "edit":
            edit_pick.options = SP.list_sp_lines(kind.value)
        elif m == "toggle":
            _refresh_toggle_lists()
            sp_del_refresh()
    work_mode.observe(_on_mode, names="value")

    def _on_kind(_=None):
        # зависимость и режим разового — зависят от типа линии
        dependence.layout.display = "" if kind.value == "regular" else "none"
        once_mode_box.layout.display = "" if kind.value == "once" else "none"
        if work_mode.value == "edit":
            edit_pick.options = SP.list_sp_lines(kind.value)
        elif work_mode.value == "toggle":
            _refresh_toggle_lists()
            sp_del_refresh()
        _update_key_preview()
    kind.observe(_on_kind, names="value")

    build_area = W.VBox([
        W.HBox([tm, ts]), W.HBox([btn_case, case_hint]),
        W.HBox([dbm, dbs]), W.HBox([user_m_box, user_s_box]),
        W.HBox([line, dependence]), key_preview, doc,
        W.HTML("<b>Источник данных ведущей</b>"), src_mode, src_help, sql_box,
        once_mode_box,
        btn_snap,
        W.HTML("<hr>"),
        W.HTML("<b>Сопоставление колонок</b> (ведущая → ведомая; порядок = порядок "
               "вставки):"),
        W.HBox([map_title, hide_unmapped]), map_head, map_box,
        W.HBox([btn_prev, btn_make, btn_push_only, btn_pub]),
        pub_confirm, push_only_confirm,
    ])

    _on_src_mode()
    _on_kind()
    _on_mode()
    _update_key_preview()

    return W.VBox([
        W.HTML("<h3>Справочники и разовый перенос (delete + insert)</h3>"),
        kind, work_mode, edit_box, toggle_box, W.HTML("<hr>"),
        build_area, W.HTML("<hr>"), out,
    ])


def launch():
    """Показать конструктор: две вкладки — «Сложный ETL» и «Справочники / разовый».

    Это единая точка входа; веб-конструктор (Voilà) запускает именно её через
    tools/dagbuilder_app.ipynb, поэтому обновление этого модуля обновляет и
    веб-версию (после перезапуска сервиса etl-dagbuilder)."""
    import ipywidgets as W
    from IPython.display import display

    tabs = W.Tab([_complex_ui(), _sp_ui()])
    tabs.set_title(0, "Сложный ETL")
    tabs.set_title(1, "Справочники / разовый")
    display(tabs)
