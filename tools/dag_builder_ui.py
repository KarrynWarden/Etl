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
from tools import trigger_builder as T  # noqa: E402

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
Групп с <code>isokaudit=4</code> нет — прогон завершается пропуском (⬜).<br>
<i>Когда:</i> какие периоды пересчитаны, знает не ETL, а тот, кто их пересчитал:
ручной триггер массового переобновления либо джоб <b>внутри</b> ведущей БД,
который сам ставит <code>isokaudit=4</code> (так работает Medree_prdisp).</p>

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
<code>idrw</code> делает одно и то же <b>независимо от типа операции</b>: DELETE
всех строк этого <code>idrw</code> в ведомой + INSERT того, что <b>сейчас</b>
отдаёт по нему ведущая (если ничего — не вставляет). Тип события (IU/D) не
используется: иначе <code>D</code> по одной из нескольких строк id сносил бы и
живые.<br>
<i>Когда:</i> одна запись-источник даёт несколько строк ведомой и при смене
атрибута (напр. <code>doctype</code>) остаются «осиротевшие» строки, которые
upsert не убирает.</p>

<p style='margin:10px 0 4px'><b><code>query_section</code></b> — группы из
своего запроса.<br>
Отличается от других режимов ТОЛЬКО источником списка групп: его даёт
пользовательский <code>periodsSql</code> (а не журнал и не сравнение срезов).
Каждая группа перезаливается целиком (DELETE группы + заливка), при этом
<code>etl_jobs</code> и <code>etl_log</code> ведутся так же, как в остальных
режимах. Колонки <code>periodsSql</code> должны соответствовать периоду ведущей:
одна колонка-дата либо <code>year[, month[, day]]</code>.<br>
<i>Когда:</i> какие периоды пересчитаны — знает ваш SQL, а триггера на ведущей
нет и проставить <code>isokaudit=4</code> некому. Если пересчитывающий джоб
может пометить сам — берите <code>section</code>, он проще.</p>

<p style='margin:12px 0 4px;padding-top:6px;border-top:1px solid #ddd'>
<b>Период группы — свойство таблицы, а не режима.</b> В ЛЮБОМ режиме период
может быть одной колонкой-датой (<code>createdate</code>, <code>dcalc</code>…)
либо собираться из нескольких колонок (<code>year</code>,
<code>year+month</code>, <code>year+month+day</code>) — причём у ведущей и
ведомой независимо. Настраивается в блоке «Период группы» под таблицей колонок.</p>
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


def _period_picker(W, title, names, cols, current=None):
    """Виджет выбора периода стороны: одна колонка-дата ИЛИ составная (year/month/day).

    Период — свойство ТАБЛИЦЫ, а не режима переноса, и у ведущей с ведомой он
    может быть устроен по-разному (у одной cоставной year+month, у другой цельный
    createdate). Поэтому такой выбор есть у каждой стороны отдельно.

    current — текущее значение из конфига: строка (одна колонка) либо dict
    {"year":..., "month":..., "day":...}. Возвращает dict с ключом "box" (виджет)
    и "value" (функция, отдающая строку или dict для конфига).
    """
    parts = current if isinstance(current, dict) else None
    kind = W.ToggleButtons(
        options=[("Одна колонка-дата", "single"), ("Составная (год/месяц/день)", "parts")],
        value="parts" if parts else "single",
        style={"button_width": "210px"})

    single_default = (current if isinstance(current, str) and current in names
                      else B.default_period_column(cols))
    single = W.Dropdown(description="колонка", options=names or [""],
                        value=single_default if single_default in names else
                        (names[0] if names else ""),
                        style={"description_width": "90px"},
                        layout=W.Layout(width="330px"))

    def _dd(label, key, required=False):
        opts = names if required else [_NONE] + names
        val = (parts or {}).get(key)
        return W.Dropdown(description=label, options=opts or [_NONE],
                          value=val if val in opts else (opts[0] if opts else _NONE),
                          style={"description_width": "70px"},
                          layout=W.Layout(width="300px"))

    year = _dd("год", "year", required=True)
    month = _dd("месяц", "month")
    day = _dd("день", "day")
    parts_box = W.VBox([year, month, day])
    hint = W.HTML("<span style='color:#888'>Недостающие месяц/день считаются "
                  "равными 1: год → ГГГГ-01-01, год+месяц → ГГГГ-ММ-01.</span>")

    def _on_kind(_=None):
        single.layout.display = "" if kind.value == "single" else "none"
        parts_box.layout.display = "" if kind.value == "parts" else "none"
        hint.layout.display = "" if kind.value == "parts" else "none"
    kind.observe(_on_kind, names="value")
    _on_kind()

    def value():
        if kind.value == "single":
            return single.value
        out = {"year": year.value}
        if month.value != _NONE:
            out["month"] = month.value
        if day.value != _NONE:
            out["day"] = day.value
        return out

    box = W.VBox([W.HTML(f"<b>{title}</b>"), kind, single, parts_box, hint],
                 layout=W.Layout(border="1px solid #ddd", padding="6px",
                                 margin="4px 0", width="600px"))
    return {"box": box, "value": value}


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


def _push_only_controls(W, out, message_fn=None, on_pushed=None):
    """Кнопка «Просто запушить» + подтверждение: коммит+push УЖЕ сохранённых на
    диске изменений (etlFolder/, dags/), без генерации файлов из текущей формы.
    Показывает список изменений перед выкладкой.

    on_pushed() дёргается ТОЛЬКО после успешного push — им очищают поле
    сообщения коммита, чтобы следующая выкладка не уехала под чужим текстом."""
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
        if ok and on_pushed:
            on_pushed()
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


_STATUS_ICON = {"ok": "✅", "warn": "⚠️", "error": "❌", "missing": "❌",
                "fail": "🔌", "skip": "⬜"}
_STATUS_WORD = {"ok": "в порядке", "warn": "замечания", "error": "ошибка",
                "missing": "нет триггера", "fail": "не проверено",
                "skip": "не требуется"}


def _trigger_report_text(results, db_reports):
    """Текстовый отчёт проверки триггеров (для зоны вывода)."""
    lines = []
    for db, rep in sorted(db_reports.items()):
        lines.append(f"=== {db} (реквизиты {rep['cred']}, журнал {rep['journal']}) "
                     f"{_STATUS_ICON.get(rep['status'], '')}")
        for m in rep["messages"]:
            lines.append("    " + m)
    for r in results:
        lines.append("")
        lines.append(f"{_STATUS_ICON.get(r['status'], '')} {r['key']} "
                     f"[{r['db']} · {r['mode']}] ведущая {r['table_master'] or '—'} "
                     f"→ tablename '{r['tablename']}': {_STATUS_WORD.get(r['status'])}")
        if r.get("found"):
            lines.append("    найдено: " + ", ".join(r["found"]))
        for p in r.get("problems") or []:
            lines.append("    ❌ " + p)
        for n in r.get("notes") or []:
            lines.append("    · " + n)
    return "\n".join(lines)


def _trigger_controls(W, out, ctx_fn, save_option=True, mode_note_fn=None, extra=()):
    """Блок «Триггер на ведущей»: показать DDL, проверить в БД, создать/обновить.

    Триггер — вторая половина линии: без него режимы по журналу
    (`iud`, `delete_insert`) не видят изменений и молча ничего не переносят.
    Раньше его приходилось делать отдельно, руками по шаблону — поэтому блок
    висит прямо в форме сборки, рядом с кнопками создания линии.

    ctx_fn() -> dict(db, table, tablename, period_column, pk_columns, cred,
                     mode, key). Если данных ещё нет — бросает RuntimeError с
    текстом, который и показывается пользователю.
    mode_note_fn() -> строка-замечание про режим (или '').
    Возвращает dict: box (виджет), ddl_text() (текст DDL или None — если
    сохранять не просили/собрать не удалось), refresh().
    """
    info = W.HTML()
    note = W.HTML()
    journal = W.Text(description="Журнал", placeholder="по умолчанию — как читает ETL",
                     layout=W.Layout(width="420px"),
                     style={"description_width": "70px"})
    chk_save = W.Checkbox(
        value=True, indent=False,
        description="Сохранять DDL в etlFolder/queries/triggers/<ключ>.sql")
    btn_show = W.Button(description="Показать DDL", icon="code",
                        layout=W.Layout(width="190px"))
    btn_check = W.Button(description="Проверить в БД", icon="search",
                         layout=W.Layout(width="190px"))
    btn_make = W.Button(description="Создать / обновить триггер", icon="bolt",
                        button_style="warning", layout=W.Layout(width="250px"))
    c_info = W.HTML()
    c_yes = W.Button(description="✅ Да, выполнить DDL в БД", button_style="danger",
                     layout=W.Layout(width="260px"))
    c_no = W.Button(description="Отмена", layout=W.Layout(width="120px"))
    confirm = W.VBox([c_info, W.HBox([c_yes, c_no])],
                     layout=W.Layout(display="none", border="2px solid #dc3545",
                                     padding="6px", margin="4px 0"))

    def _build():
        """(ddl, ctx) — бросает исключение с понятным текстом."""
        ctx = ctx_fn()
        ddl = T.build_trigger(ctx["db"], ctx["table"], ctx["tablename"],
                              ctx.get("period_column"), ctx.get("pk_columns") or [],
                              journal.value.strip() or None)
        return ddl, ctx

    def refresh(_=None):
        note.value = (mode_note_fn() or "") if mode_note_fn else ""
        try:
            ddl, ctx = _build()
        except Exception as e:
            info.value = ("<span style='color:#888'>DDL появится, когда будут "
                          f"известны ведущая, имя линии и PK: {_esc(str(e))}</span>")
            return
        pk = ", ".join(ctx.get("pk_columns") or []) or "—"
        info.value = (
            f"триггер <code>{_esc(ddl['name'])}</code> на <code>{_esc(ctx['table'])}</code> "
            f"[{ctx['db']}] → журнал <code>{_esc(ddl['journal'])}</code>, "
            f"<code>tablename = '{_esc(ctx['tablename'])}'</code>, PK: <code>{_esc(pk)}</code>"
            + (f", функция <code>{_esc(ddl['func'])}</code>" if ddl["func"] else ""))

    def _show(_):
        try:
            ddl, ctx = _build()
        except Exception as e:
            with out:
                out.clear_output(); print(f"DDL собрать не удалось: {e}")
            return
        with out:
            out.clear_output()
            print("=" * 70)
            print(f"DDL ТРИГГЕРА ведущей ({ctx['db']})")
            print("-" * 70)
            print(ddl["text"])

    def _check(_):
        from IPython.display import display, HTML
        try:
            ddl, ctx = _build()
        except Exception as e:
            with out:
                out.clear_output(); print(f"Проверять нечего: {e}")
            return
        with out:
            out.clear_output()
            display(HTML(_spinner_html("Смотрю триггер в БД — подождите…")))
        target = {"key": ctx.get("key") or ctx["tablename"], "kind": "etl",
                  "db_master": ctx["db"], "table_master": ctx["table"],
                  "tablename": ctx["tablename"], "mode": ctx.get("mode", "iud"),
                  "period_column": ctx.get("period_column"),
                  "pk_columns": ctx.get("pk_columns") or [],
                  # проверяем по запросу пользователя даже в режимах, которым
                  # журнал не нужен: он сам решил посмотреть, что там стоит
                  "needs": True, "note": None}
        try:
            results, db_reports = T.check_targets(
                [target], creds={ctx["db"]: ctx.get("cred", "MAIN")},
                journals={ctx["db"]: journal.value.strip() or None})
        except Exception as e:
            with out:
                out.clear_output(); print(f"Проверка не удалась: {type(e).__name__}: {e}")
            return
        with out:
            out.clear_output()
            print(_trigger_report_text(results, db_reports))
            if results and results[0]["status"] in ("missing", "error"):
                if not T.needs_trigger(ctx.get("mode", "iud")):
                    print(f"\nПри этом режим {ctx['mode']} журнал не читает — "
                          f"триггер этой линии не требуется, проверка показана "
                          f"как есть.")
                else:
                    print("\nПоправить — кнопка «Создать / обновить триггер» "
                          "(DDL можно посмотреть заранее).")

    def _ask(_):
        try:
            ddl, ctx = _build()
        except Exception as e:
            with out:
                out.clear_output(); print(f"DDL собрать не удалось: {e}")
            return
        c_info.value = (
            f"<b>Выполнить DDL в БД {ctx['db']}</b> (реквизиты "
            f"{_esc(ctx.get('cred', 'MAIN'))})? Триггер "
            f"<code>{_esc(ddl['name'])}</code> на <code>{_esc(ctx['table'])}</code> "
            "будет создан или ЗАМЕНЁН (CREATE OR REPLACE). Это изменение в "
            "боевой БД, не в репозитории:"
            f"<pre style='max-height:220px;overflow:auto'>{_esc(ddl['text'])}</pre>")
        confirm.layout.display = ""

    def _hide(_):
        confirm.layout.display = "none"

    def _go(_):
        from IPython.display import display, HTML
        confirm.layout.display = "none"
        try:
            ddl, ctx = _build()
        except Exception as e:
            with out:
                out.clear_output(); print(f"DDL собрать не удалось: {e}")
            return
        for b in (btn_make, c_yes, c_no):
            b.disabled = True
        with out:
            out.clear_output()
            display(HTML(_spinner_html("Выполняю DDL в БД — подождите…")))
        try:
            done = T.apply_trigger(ctx["db"], ddl["statements"], ctx.get("cred", "MAIN"))
        except Exception as e:
            with out:
                out.clear_output()
                print(f"Триггер НЕ создан: {type(e).__name__}: {e}")
                print("\nЧастые причины: нет прав на ведущую таблицу или на журнал "
                      "(нужен GRANT INSERT на etl_log_iud_row), либо триггер должен "
                      "создаваться под владельцем таблицы.")
            return
        finally:
            for b in (btn_make, c_yes, c_no):
                b.disabled = False
        with out:
            out.clear_output()
            print(f"✅ Триггер {ddl['name']} создан/обновлён в {ctx['db']}. Выполнено:")
            for d in done:
                print("  ", d)
            # Файл с DDL пишется не здесь, а при сборке линии — говорим прямо,
            # чтобы «создал триггер» не путали с «сохранил в репозиторий».
            print("\nВ БД изменения уже применены. Копия DDL попадёт в репозиторий "
                  "при «Создать файлы» / «Создать и запушить»."
                  if (save_option and chk_save.value) else
                  "\nВ БД изменения уже применены. В репозиторий этот DDL не "
                  "сохраняется.")

    btn_show.on_click(_show)
    btn_check.on_click(_check)
    btn_make.on_click(_ask)
    c_yes.on_click(_go)
    c_no.on_click(_hide)

    def ddl_text():
        """Текст DDL для записи рядом с линией (None — если не нужно/не собрать)."""
        if save_option and not chk_save.value:
            return None
        try:
            return _build()[0]["text"]
        except Exception:
            return None

    children = [W.HTML("<b>Триггер на ведущей</b> — вторая половина линии: "
                       "события в <code>etl_log_iud_row</code> пишет он, "
                       "без него режимы по журналу молча ничего не переносят."),
                note, info] + list(extra) + [journal]
    if save_option:
        children.append(chk_save)
    children += [W.HBox([btn_show, btn_check, btn_make]), confirm]
    box = W.VBox(children, layout=W.Layout(border="1px solid #17a2b8",
                                           padding="6px", margin="4px 0"))
    return {"box": box, "ddl_text": ddl_text, "refresh": refresh,
            "journal": journal, "save": chk_save}


def _publish_controls(W, out, do_write, message_fn, on_pushed=None):
    """Кнопка «создать и запушить» + подтверждение (защита от мисклика).

    do_write() пишет файлы линии и возвращает список абсолютных путей (может
    бросить исключение). message_fn() даёт текст коммита. Возвращает
    (btn_publish, confirm_box) — их размещает вызывающий UI. После подтверждения
    файлы пишутся и делается git commit + push текущей ветки (B.git_commit_push).
    on_pushed() дёргается ТОЛЬКО после успешного push (см. _push_only_controls).
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
        # Коммит забирает и уже сохранённые изменения (см. B.git_commit_push):
        # показываем их СПИСКОМ до нажатия «Да», чтобы прихватить чужую работу
        # в общем клоне можно было только осознанно.
        changed = B.git_status_short()
        extra = (
            "Вместе с текущей линией уедут уже сохранённые изменения "
            f"(кнопка «Создать файлы» без пуша):<pre style='max-height:200px;"
            f"overflow:auto'>{_esc(changed)}</pre>"
            if changed.strip() else
            "Других сохранённых изменений нет — уедет только текущая линия.")
        info.value = (
            "<b>Выложить в git?</b> Файлы будут созданы, затем "
            f"<code>git commit</code> и <code>git push origin {br}</code>. "
            f"{extra}"
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
        if ok and on_pushed:
            on_pushed()
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

    state = {"master_cols": [], "slave_cols": [], "rows": [],
             "period_m_w": None, "period_s_w": None,
             "struct_master_rel": None, "struct_slave_rel": None,
             "tags_auto": True, "_tags_guard": False, "_opts_guard": False}

    # ── режим работы ──
    work_mode = W.ToggleButtons(
        options=[("➕ Создать новый", "new"), ("✏️ Редактировать", "edit"),
                 ("🗄 Архив", "archive")],
        value="new", style={"button_width": "170px"})
    # Только линии В РАБОТЕ: убранные в архив править незачем — правка всё равно
    # не доедет до Airflow, пока линию не восстановишь (см. _refresh_edit_list).
    edit_pick = W.Dropdown(description="Линия", options=B.list_active_lines(),
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

    def _refresh_edit_list():
        """Пересобрать список линий для правки — ТОЛЬКО линии в работе.

        Архивная линия (даг в dags/_archived/ либо флаг `disabled`) в списке
        править нечего: Airflow её не парсит, и правка доедет только после
        «Восстановить». Раньше здесь стоял existing_lines(), поэтому убранная в
        архив линия оставалась вариантом выбора и выглядела как живая.
        Выбор сохраняем, если линия ещё в списке (иначе Dropdown сбрасывает
        value на первый вариант при любой перезаписи options).
        """
        cur = edit_pick.value
        opts = B.list_active_lines()
        edit_pick.options = opts
        if cur in opts:
            edit_pick.value = cur

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
    line = W.Text(description="Имя линии",
                  placeholder="по умолч. = имя ведущей в регистре её БД",
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
        ln = line.value.strip() or (B.to_db_case(B.bare(tm.value.strip()), dbm.value)
                                    if tm.value.strip() else "")
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
    # ── поле режима query_section (список групп задаёт свой запрос) ──
    # Показывается ТОЛЬКО при mode=query_section: список групп из своего запроса
    # читает один этот режим, в остальных ключ periodsSql не пишется вообще —
    # видимое поле там сбивало с толку («заполнил, а оно не работает»).
    f_periods = W.Textarea(description="SQL периодов (query_section)",
                           placeholder="Запрос групп для перезаливки. Колонки должны "
                                       "соответствовать периоду ведущей: одна колонка-дата "
                                       "либо year[, month[, day]]. Файл .sql создастся сам, "
                                       "путь пропишется в periodsSql.",
                           layout=W.Layout(width="760px", height="100px"),
                           style={"description_width": "180px"})
    periods_box = W.VBox([f_periods])
    adv_help = W.HTML(
        "<span style='color:#888'>"
        "<b>Фильтр ведущей/ведомой</b> — доп. условие WHERE (например <code>doctype = 7</code>).<br>"
        "<b>SQL ведущей</b> — если перенос идёт не из таблицы, а из своего запроса.<br>"
        "<b>Конфликт: доп. колонки</b> — лишние колонки в ON CONFLICT (upsert).<br>"
        "<b>Конфликт: условие WHERE</b> — для частичного уникального индекса.<br>"
        "<b>truncatePeriod</b> — сравнивать период по дате, без времени.<br>"
        "<b>skipAudit</b> — исключить линию из общего аудита (AuditAll).<br>"
        "<b>Не сверять поля</b> — auditExcludeFields: колонки, которые аудит игнорирует.<br>"
        "<b>SQL периодов</b> — появляется только при mode=<code>query_section</code> "
        "(другие режимы его не читают): запрос возвращает список групп для "
        "перезаливки. Колонки должны соответствовать периоду ведущей (см. блок "
        "«Период группы» ниже): одна колонка-дата либо year[, month[, day]].</span>")
    advanced = W.Accordion(children=[W.VBox(
        [adv_help, f_filter, f_filter_s, f_sql, f_sql_name, f_confl, f_confl_w,
         f_trunc, f_skip_audit, f_audit_excl, periods_box])])
    advanced.set_title(0, "Дополнительно (необязательно)")
    advanced.selected_index = None

    # ── зоны вывода ──
    # VBox (не HBox): подсказка идёт СТРОКОЙ СВЕРХУ, а две панели периода — под
    # ней в ряд. В HBox длинный текст подсказки съедал ширину и панели уезжали
    # далеко вправо.
    period_box = W.VBox([])
    map_title = W.HTML("")
    hide_unmapped = W.Checkbox(description="Скрывать непривязанные", value=False,
                               indent=False)
    # ── ручное добавление колонок ──
    btn_add_mrow = W.Button(description="➕ колонка ведущей", icon="plus",
                            layout=W.Layout(width="220px"))
    new_scol = W.Text(placeholder="колонка ведомой", layout=W.Layout(width="180px"))
    new_scol_type = W.Text(placeholder="её тип", layout=W.Layout(width="140px"))
    btn_add_scol = W.Button(description="➕ колонка ведомой", icon="plus",
                            layout=W.Layout(width="200px"))
    manual_help = W.HTML(
        "<span style='color:#888'>Колонки можно править руками: имя и тип ведущей — "
        "прямо в строке, <b>🗑</b> убирает колонку из линии, кнопки ниже добавляют новую. "
        "Тип нужен, когда его не отдаёт источник: у константы в запросе "
        "(<code>null CHECKDIR</code>, <code>2 CHECKDIR</code>) тип определить неоткуда, "
        "и он подсвечен как <b>неизвестный</b>. Такие колонки рантайм сверяет только по "
        "имени, поэтому вписанный тип ничего не сломает; надёжнее всего задать тип прямо "
        "в запросе — <code>CAST(NULL AS NUMBER) CHECKDIR</code>.</span>")
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
        ln = (line.value.strip()
              or B.to_db_case(B.bare(tm.value.strip()), dbm.value)) \
            if tm.value.strip() or line.value.strip() else ""
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
        elif m == "edit":
            # перечитываем на входе в правку: линию могли заархивировать
            # (или восстановить) в этой же сессии либо правкой файлов на диске
            _refresh_edit_list()
        elif m == "archive":
            _refresh_archive_lists()
            del_refresh()
    work_mode.observe(_on_work_mode, names="value")

    # ── фильтр строк: показывать только привязанные (для проверки) ──
    def _apply_row_filter(_=None):
        rows = state.get("rows", [])
        if hide_unmapped.value:
            map_box.children = [r["box"] for r in rows if r["dd"].value != _NONE]
        else:
            map_box.children = [r["box"] for r in rows]
    hide_unmapped.observe(_apply_row_filter, names="value")

    # ── связь 1:1: в каждой выпадашке оставляем только ещё не занятые колонки ──
    # ведомой (плюс собственный текущий выбор), чтобы одну ведомую колонку нельзя
    # было назначить дважды и список был короче.
    def _refresh_slave_options():
        if state.get("_opts_guard"):
            return
        all_s = [c["column_name"] for c in state["slave_cols"]]
        used = {r["dd"].value for r in state["rows"] if r["dd"].value != _NONE}
        state["_opts_guard"] = True
        try:
            for r in state["rows"]:
                cur = r["dd"].value
                opts = [_NONE] + [s for s in all_s if s == cur or s not in used]
                if list(r["dd"].options) != opts:
                    r["dd"].options = opts
                    r["dd"].value = cur       # сохранить выбор строки
        finally:
            state["_opts_guard"] = False

    def _on_pair_change(_=None):
        _refresh_slave_options()
        _apply_row_filter()

    def _on_rows_changed():
        """Состав колонок изменился руками — обновить и списки периода."""
        _on_pair_change()
        _refresh_period_keep()

    def _mark_type(row):
        """Подсветить тип, который источник не отдал (константа в запросе).
        Пустой/unknown — рамка внимания; как только вписали своё — снимаем."""
        unknown = B.unknown_type(row["type"].value)
        row["type"].layout.border = "2px solid #e0a800" if unknown else ""

    def _make_row(mcol, sval=None, is_pk=False):
        """Строка сопоставления: имя и тип ведущей правятся руками, 🗑 убирает
        колонку из линии (в json и в перенос она не попадёт)."""
        name_w = W.Text(value=str(mcol.get("column_name") or ""),
                        placeholder="имя колонки", layout=W.Layout(width="190px"))
        type_w = W.Text(value=str(mcol.get("data_type") or ""),
                        placeholder="тип", layout=W.Layout(width="140px"))
        all_s = [c["column_name"] for c in state["slave_cols"]]
        opts = [_NONE] + all_s
        dd = W.Dropdown(options=opts, value=sval if sval in opts else _NONE,
                        layout=W.Layout(width="240px"))
        dd.observe(_on_pair_change, names="value")
        pk = W.Checkbox(value=bool(is_pk), description="PK", indent=False,
                        layout=W.Layout(width="70px"))
        # PK входит в DDL триггера — держим блок «Триггер» в актуальном виде
        pk.observe(lambda _c: trg["refresh"](), names="value")
        rm = W.Button(description="", icon="trash", tooltip="убрать колонку из линии",
                      layout=W.Layout(width="42px"))
        row = {"name": name_w, "type": type_w, "dd": dd, "pk": pk,
               "scale": mcol.get("data_scale")}
        # увеличенная высота строки + разделитель, чтобы строки не сливались
        row["box"] = W.HBox(
            [name_w, type_w, W.HTML("→", layout=W.Layout(width="24px")), dd, pk, rm],
            layout=W.Layout(align_items="center", min_height="42px",
                            padding="4px 2px", border_bottom="1px solid #eee"))
        type_w.observe(lambda _c: _mark_type(row), names="value")
        _mark_type(row)

        def _remove(_):
            state["rows"] = [r for r in state["rows"] if r is not row]
            _on_rows_changed()
        rm.on_click(_remove)
        return row

    def _add_master_row(_=None):
        if not state["rows"] and not state["slave_cols"]:
            _log("Сначала сними структуры — потом можно добавлять колонки руками.")
            return
        state["rows"] = list(state["rows"]) + [
            _make_row({"column_name": "", "data_type": ""})]
        _on_rows_changed()
    btn_add_mrow.on_click(_add_master_row)

    def _add_slave_col(_=None):
        """Добавить колонку ведомой руками — если её не отдала БД (только что
        создали, нет прав на словарь и т.п.). Тип обязателен: json ведомой
        сверяется с information_schema по паре (имя, тип)."""
        nm = new_scol.value.strip()
        if not nm:
            _log("Впиши имя колонки ведомой."); return
        if any(c["column_name"] == nm for c in state["slave_cols"]):
            _log(f"Колонка ведомой «{nm}» уже есть в списке."); return
        state["slave_cols"] = list(state["slave_cols"]) + [{
            "column_name": nm, "data_type": new_scol_type.value.strip(),
            "data_scale": None, "is_primary_key": None}]
        new_scol.value = new_scol_type.value = ""
        _on_rows_changed()
        _log(f"Колонка ведомой «{nm}» добавлена в список — выбери её в нужной строке.")
    btn_add_scol.on_click(_add_slave_col)

    # ── отрисовка таблицы сопоставления колонок ──
    def _render_mapping(mcols, scols, pair_map=None, pk_names=None,
                        period_m=None, period_s=None):
        # шапка: явно подписываем, какая таблица/БД слева и справа
        m_lbl = (tm.value.strip() or "ведущая")
        s_lbl = (ts.value.strip() or "ведомая")
        map_head.children = [
            W.HTML(f"<b>ВЕДУЩАЯ</b> · <code>{m_lbl}</code> "
                   f"<span style='color:#888'>[{dbm.value}] имя / тип</span>",
                   layout=W.Layout(width="354px")),
            W.HTML(f"<b>ВЕДОМАЯ</b> · <code>{s_lbl}</code> "
                   f"<span style='color:#888'>[{dbs.value}]</span>",
                   layout=W.Layout(width="264px")),
            W.HTML("<b>PK</b>", layout=W.Layout(width="70px")),
            W.HTML("", layout=W.Layout(width="42px")),
        ]
        state["slave_cols"] = list(scols)
        rows = []
        for mcol in mcols:
            name = mcol["column_name"]
            sval = pair_map.get(name) if pair_map else None
            is_pk = (name in pk_names) if pk_names is not None \
                else bool(mcol.get("is_primary_key"))
            rows.append(_make_row(mcol, sval, is_pk))
        state["rows"] = rows
        _refresh_slave_options()   # сразу убрать занятые из чужих списков
        _apply_row_filter()

        _render_period(period_m, period_s)
        trg["refresh"]()

    def _render_period(period_m, period_s):
        """Панели «Период группы» по ТЕКУЩИМ колонкам формы (включая правленные
        руками) — период выбирается из того, что реально попадёт в структуры."""
        mcols = _collect_master_cols()
        scols = list(state["slave_cols"])
        m_names = [c["column_name"] for c in mcols]
        s_names = [c["column_name"] for c in scols]
        state["period_m_w"] = _period_picker(W, "Период ВЕДУЩЕЙ", m_names, mcols, period_m)
        state["period_s_w"] = _period_picker(W, "Период ВЕДОМОЙ", s_names, scols, period_s)
        period_box.children = [
            W.HTML("<b>Период группы</b> — по нему идёт перезаливка и учёт в "
                   "<code>etl_jobs</code>. Может быть одной колонкой-датой ИЛИ "
                   "собираться из year[/month[/day]] — <i>у ведущей и ведомой "
                   "независимо</i>."),
            # две панели рядом, слева направо, сразу под подсказкой
            W.HBox([state["period_m_w"]["box"], state["period_s_w"]["box"]],
                   layout=W.Layout(align_items="flex-start")),
        ]

    def _refresh_period_keep():
        """Пересобрать панели периода после ручной правки состава колонок,
        сохранив выбранное значение."""
        cur_m = state["period_m_w"]["value"]() if state["period_m_w"] else None
        cur_s = state["period_s_w"]["value"]() if state["period_s_w"] else None
        _render_period(cur_m, cur_s)

    def _collect_master_cols():
        """Колонки ведущей ИЗ ФОРМЫ (имя/тип правятся руками, удалённые строки
        исключены). Это и есть источник json-структуры ведущей."""
        out = []
        for r in state["rows"]:
            name = r["name"].value.strip()
            if not name:
                continue
            out.append({"column_name": name,
                        "data_type": r["type"].value.strip(),
                        "data_scale": r["scale"],
                        "is_primary_key": "Primary Key" if r["pk"].value else None})
        return out

    def _collect_pairs():
        pairs = []
        for r in state["rows"]:
            name = r["name"].value.strip()
            if not name:
                continue
            pairs.append((name, None if r["dd"].value == _NONE else r["dd"].value))
        return pairs

    def _check_rows():
        """Понятные ошибки до сборки: пустые/повторяющиеся имена, пустые типы."""
        names = [r["name"].value.strip() for r in state["rows"]]
        dupes = sorted({n for n in names if n and names.count(n) > 1})
        if dupes:
            raise RuntimeError("Колонки ведущей повторяются: " + ", ".join(dupes) +
                               ". Убери лишние строки (🗑) или переименуй.")
        empty = [r["name"].value.strip() for r in state["rows"]
                 if r["name"].value.strip() and not r["type"].value.strip()
                 and r["dd"].value != _NONE]
        if empty:
            raise RuntimeError(
                "Не заполнен тип у колонок: " + ", ".join(empty) +
                ". Впиши тип в строке (или убери колонку из линии) — он попадёт в "
                "json-структуру, по которой рантайм сверяет источник.")

    def _build_spec():
        _check_rows()
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
            "master_cols": _collect_master_cols(), "slave_cols": state["slave_cols"],
            "pairs": _collect_pairs(),
            "period_column": state["period_m_w"]["value"]() if state["period_m_w"] else None,
            "slave_period_column": state["period_s_w"]["value"]() if state["period_s_w"] else None,
            "tags": list(tags_input.value) or None,
            "retry_mode": retry.value,
            "doc": doc.value.strip() or None,
            "extra": extra,
            "select_sql_text": f_sql.value.strip() or None,
            "select_sql_name": f_sql_name.value.strip() or None,
            "periods_sql_text": f_periods.value.strip() or None,
            "struct_master_rel": state["struct_master_rel"],
            "struct_slave_rel": state["struct_slave_rel"],
            # версионируемая копия DDL триггера (галочка в блоке «Триггер»)
            "trigger_sql_text": trg["ddl_text"](),
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

    # ── блок «Триггер на ведущей» ──
    def _line_name():
        return (line.value.strip()
                or (B.to_db_case(B.bare(tm.value.strip()), dbm.value)
                    if tm.value.strip() else ""))

    def _trigger_ctx():
        ln = _line_name()
        if not ln:
            raise RuntimeError("не заполнена ведущая / имя линии")
        if not tm.value.strip():
            raise RuntimeError("не заполнена ведущая таблица")
        pk = [r["name"].value.strip() for r in state["rows"]
              if r["pk"].value and r["name"].value.strip()]
        if not pk:
            raise RuntimeError("не отмечен PK в таблице колонок")
        return {"db": dbm.value, "table": tm.value.strip(), "tablename": ln,
                "period_column": (state["period_m_w"]["value"]()
                                  if state["period_m_w"] else None),
                "pk_columns": pk, "cred": user_m_get(), "mode": mode.value,
                "key": f"{ln}{dbm.value}{dbs.value}"}

    def _trigger_mode_note():
        if T.needs_trigger(mode.value):
            if mode.value in T.TRIGGER_MODES_OPTIONAL:
                return ("<span style='color:#8a6d3b'>режим "
                        f"<code>{mode.value}</code> журналом только дополняется: "
                        "линия работает и без триггера (по сравнению срезов), "
                        "но с ним реагирует на каждое изменение.</span>")
            return ("<span style='color:#8a6d3b'>режим "
                    f"<code>{mode.value}</code> работает ТОЛЬКО по журналу — "
                    "без триггера перенос не увидит ни одного изменения.</span>")
        return ("<span style='color:#888'>режим "
                f"<code>{mode.value}</code> журнал не читает "
                "(группы даёт isokaudit=4 либо свой SQL периодов) — триггер этой "
                "линии не нужен, галочка сохранения снята автоматически.</span>")

    trg = _trigger_controls(W, out, _trigger_ctx, save_option=True,
                            mode_note_fn=_trigger_mode_note)

    def _current_mapping():
        """Текущее состояние формы: {master: slave|None}, набор PK и {master: тип}
        (ручные типы нужно сохранить при пересъёме — источник их не отдаёт)."""
        pm, pk, types = {}, set(), {}
        for r in state["rows"]:
            nm = r["name"].value.strip()
            if not nm:
                continue
            pm[nm] = None if r["dd"].value == _NONE else r["dd"].value
            types[nm] = r["type"].value.strip()
            if r["pk"].value:
                pk.add(nm)
        return pm, pk, types

    def _snap_note_html(notes):
        if not notes:
            return ""
        return ("<div style='margin-top:4px;color:#8a6d3b'>· " +
                "<br>· ".join(notes) + "</div>")

    def _snap_master(prev_types=None):
        """Снять структуру ведущей. ПРИОРИТЕТ — свой запрос («SQL ведущей»), и
        только если его нет — таблица.

        Так же поступает рантайм: при заданном selectSql он сверяет структуру по
        описанию курсора этого запроса, а не по таблице. Раньше структура всегда
        снималась с таблицы, поэтому колонки запроса, которых в таблице нет
        (`null CHECKDIR`), в структуру просто не попадали, и перенос их не видел.

        Возвращает (колонки, заметки для пользователя).
        """
        notes = []
        sql_text = f_sql.value.strip()
        if not sql_text:
            if not tm.value.strip():
                raise RuntimeError("Заполни имя ведущей таблицы.")
            return B.snap_structure(dbm.value, tm.value.strip(), user_m_get()), notes
        mcols = B.snap_query_structure(dbm.value, sql_text, user_m_get())
        notes.append("структура ведущей снята <b>по своему запросу</b> "
                     "(«SQL ведущей»), а не по таблице")
        # Первичный ключ из описания курсора не виден — берём из таблицы ведущей.
        if tm.value.strip():
            try:
                tcols = B.snap_structure(dbm.value, tm.value.strip(), user_m_get())
                mcols = B.merge_table_pk(mcols, tcols)
            except Exception as e:
                notes.append(f"PK из таблицы {tm.value.strip()} подтянуть не удалось "
                             f"({type(e).__name__}) — отметь PK галочками вручную")
        # Ручные типы (их источник не отдаёт) переносим на новую съёмку.
        if prev_types:
            for c in mcols:
                old = prev_types.get(c["column_name"])
                if old and B.unknown_type(c["data_type"]):
                    c["data_type"] = old
        unknown = [c["column_name"] for c in mcols if B.unknown_type(c["data_type"])]
        if unknown:
            notes.append("тип не определился у " +
                         ", ".join(f"<code>{c}</code>" for c in unknown) +
                         " (константа в запросе) — впиши его в строке вручную либо "
                         "задай в запросе через CAST")
        return mcols, notes

    # ── снять структуры из БД ──
    def on_snap(_):
        try:
            if not ts.value.strip():
                _log("Заполни имя ведомой таблицы."); return
            edit = work_mode.value == "edit"
            # в правке сохраняем текущие связки/PK/период/типы, чтобы пересъём их не снёс
            prev_pairs, prev_pk, prev_types = (_current_mapping() if state["rows"]
                                               else ({}, None, {}))
            if not edit:
                prev_pairs, prev_pk = {}, None
            # период сохраняем при пересъёме структур (строка либо dict)
            prev_pm = state["period_m_w"]["value"]() if (edit and state["period_m_w"]) else None
            prev_ps = state["period_s_w"]["value"]() if (edit and state["period_s_w"]) else None

            _log("Снимаю структуры из БД…")
            mcols, snap_notes = _snap_master(prev_types)
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
                map_title.value = note + _snap_note_html(snap_notes)
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
                map_title.value = note + _snap_note_html(snap_notes)
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
                # DDL триггера показываем ВСЕГДА — даже когда его не сохраняют в
                # репозиторий: увидеть, что именно поедет в БД, нужно до нажатия
                # «Создать триггер».
                if not any(rel.startswith("etlFolder/queries/triggers/")
                           for rel, _c in files):
                    print("=" * 70)
                    print("DDL ТРИГГЕРА ведущей (в репозиторий НЕ сохраняется)")
                    print("-" * 70)
                    try:
                        ctx = _trigger_ctx()
                        print(T.build_trigger(
                            ctx["db"], ctx["table"], ctx["tablename"],
                            ctx["period_column"], ctx["pk_columns"],
                            trg["journal"].value.strip() or None)["text"])
                    except Exception as e:
                        print(f"(собрать не удалось: {e})")
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

    # Своё сообщение коммита. Пусто — подставляется автоописание (как было).
    commit_msg = W.Text(
        description="Коммит", placeholder="по умолчанию — автоописание правки",
        layout=W.Layout(width="560px"), style={"description_width": "90px"})
    commit_hint = W.HTML(
        "<span style='color:#888'>Текст коммита для обеих кнопок ниже. Пусто — "
        "подставится автоматическое описание.</span>")

    def _commit_msg():
        typed = commit_msg.value.strip()
        if typed:
            return typed
        ln = line.value.strip() or (B.bare(tm.value.strip()) if tm.value.strip()
                                    else "линия")
        return f"dagbuilder: {'правка' if work_mode.value == 'edit' else 'новая'} линия {ln}"

    def _clear_commit_msg():
        commit_msg.value = ""

    btn_pub, pub_confirm = _publish_controls(W, out, _write_current, _commit_msg,
                                             _clear_commit_msg)
    btn_push_only, push_only_confirm = _push_only_controls(W, out, _commit_msg,
                                                           _clear_commit_msg)
    btn_pub.layout.margin = "0 0 0 40px"   # «Создать и запушить» — сбоку, отдельно

    # удаление линии насовсем (живёт в панели архива — рядом со «скрыть»)
    def _after_delete_complex():
        _refresh_edit_list()
        _refresh_archive_lists()
    del_pick, del_btn, del_confirm, del_refresh = _delete_controls(
        W, out, B.existing_lines, B.line_delete_targets, B.delete_line,
        _after_delete_complex, label="линию ETL")
    del_help = W.HTML(
        "<span style='color:#888'><b>Удалить насовсем</b> (не архив): убирает фрагмент "
        "config.d, файл дага и — если не используются другими линиями — структуры и "
        "selectSql. Затем выложи «Просто запушить» или деплоем.</span>")
    # отдельная «Просто запушить» для панели архива (один виджет-кнопку нельзя
    # разместить в двух контейнерах) — иначе после архива/удаления её тут нет.
    # Поле коммита здесь тоже своё: форма сборки в режиме архива скрыта целиком,
    # поэтому её поле было бы не видно.
    arch_commit_msg = W.Text(
        description="Коммит", placeholder="по умолчанию — автоописание",
        layout=W.Layout(width="560px"), style={"description_width": "90px"})

    def _arch_commit_msg():
        return (arch_commit_msg.value.strip()
                or "dagbuilder: архив / восстановление / удаление линий")

    arch_btn_push, arch_push_confirm = _push_only_controls(
        W, out, _arch_commit_msg, lambda: setattr(arch_commit_msg, "value", ""))
    archive_box.children = tuple(archive_box.children) + (
        W.HTML("<hr>"), del_help, W.HBox([del_pick, del_btn]), del_confirm,
        W.HTML("<hr>"),
        W.HTML("<b>Выложить сделанное</b> (архив/восстановление/удаление меняют "
               "файлы на диске — их нужно запушить):"),
        arch_commit_msg, W.HBox([arch_btn_push]), arch_push_confirm)

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
            _refresh_edit_list()
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
            _refresh_edit_list()
            _log(f"Линия «{key}» (даг {did}) восстановлена. Вступит в силу после деплоя.")
        except Exception as e:
            _log(f"Ошибка восстановления: {type(e).__name__}: {e}")

    btn_snap.on_click(on_snap)
    btn_prev.on_click(on_preview)
    btn_make.on_click(on_make)
    btn_load.on_click(on_load)
    btn_archive.on_click(on_archive)
    btn_restore.on_click(on_restore)

    # ── видимость полей, зависящих от режима переноса ──
    def _on_mode(_=None):
        # «SQL периодов» читает ТОЛЬКО query_section — в остальных режимах поля
        # нет вовсе, чтобы его не заполняли впустую.
        qs = mode.value == "query_section"
        periods_box.layout.display = "" if qs else "none"
        if qs:
            advanced.selected_index = 0     # раскрыть: без этого поля не найти
        # DDL триггера кладём в репозиторий только для режимов, которые его
        # используют; галочку всегда можно вернуть руками.
        trg["save"].value = T.needs_trigger(mode.value)
        trg["refresh"]()
    mode.observe(_on_mode, names="value")
    for _w in (tm, line, dbm, dbs):
        _w.observe(lambda _c: trg["refresh"](), names="value")

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
        manual_help,
        W.HBox([btn_add_mrow, W.HTML("&nbsp;&nbsp;"), new_scol, new_scol_type,
                btn_add_scol]),
        W.HTML("<hr>"), trg["box"],
        W.HTML("<hr>"), commit_hint, commit_msg,
        W.HBox([btn_prev, btn_make, btn_push_only, btn_pub]),
        pub_confirm, push_only_confirm,
    ])

    _refresh_default_tags()   # проставить 3 тега по умолчанию
    _on_mode()                # спрятать «SQL периодов» вне query_section
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

    state = {"master_cols": [], "slave_cols": [], "rows": [], "snap_count": 0,
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
    # ── ручное добавление/удаление колонок ──
    btn_add_mrow = W.Button(description="➕ колонка ведущей", icon="plus",
                            layout=W.Layout(width="220px"))
    new_scol = W.Text(placeholder="колонка ведомой", layout=W.Layout(width="200px"))
    btn_add_scol = W.Button(description="➕ колонка ведомой", icon="plus",
                            layout=W.Layout(width="200px"))
    manual_help = W.HTML(
        "<span style='color:#888'>Строки можно править руками: имя (или выражение) "
        "ведущей — прямо в строке, <b>🗑</b> убирает колонку из переноса, кнопки ниже "
        "добавляют новую. В режиме <b>«Свой SELECT»</b> порядок и количество строк "
        "должны в точности совпадать с колонками запроса — вставка позиционная; "
        "в режиме «из таблицы» из этих имён собирается SELECT.</span>")
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
        used = {r["dd"].value for r in state["rows"] if r["dd"].value != _NONE}
        state["_opts_guard"] = True
        try:
            for r in state["rows"]:
                cur = r["dd"].value
                opts = [_NONE] + [s for s in all_s if s == cur or s not in used]
                if list(r["dd"].options) != opts:
                    r["dd"].options = opts
                    r["dd"].value = cur
        finally:
            state["_opts_guard"] = False

    def _apply_row_filter(_=None):
        rows = state.get("rows", [])
        if hide_unmapped.value:
            map_box.children = [r["box"] for r in rows if r["dd"].value != _NONE]
        else:
            map_box.children = [r["box"] for r in rows]
    hide_unmapped.observe(_apply_row_filter, names="value")

    def _on_pair_change(_=None):
        _refresh_slave_options()
        _apply_row_filter()

    def _make_row(mcol, sval=None):
        """Строка сопоставления: имя/выражение ведущей правится руками, 🗑 убирает
        колонку из переноса, ➕ добавляет новую (см. _add_master_row)."""
        name_w = W.Text(value=str(mcol.get("column_name") or ""),
                        placeholder="колонка или выражение ведущей",
                        layout=W.Layout(width="300px"))
        all_s = [c["column_name"] for c in state["slave_cols"]]
        opts = [_NONE] + all_s
        dd = W.Dropdown(options=opts, value=sval if sval in opts else _NONE,
                        layout=W.Layout(width="280px"))
        dd.observe(_on_pair_change, names="value")
        rm = W.Button(description="", icon="trash", tooltip="убрать колонку из переноса",
                      layout=W.Layout(width="42px"))
        row = {"name": name_w, "dd": dd}
        row["box"] = W.HBox(
            [name_w, W.HTML("→", layout=W.Layout(width="24px")), dd, rm],
            layout=W.Layout(align_items="center", min_height="40px",
                            padding="4px 2px", border_bottom="1px solid #eee"))

        def _remove(_):
            state["rows"] = [r for r in state["rows"] if r is not row]
            _on_pair_change()
        rm.on_click(_remove)
        return row

    def _add_master_row(_=None):
        if not state["slave_cols"]:
            _log("Сначала сними колонки — потом можно добавлять их руками.")
            return
        state["rows"] = list(state["rows"]) + [_make_row({"column_name": ""})]
        _on_pair_change()

    def _add_slave_col(_=None):
        """Добавить колонку ведомой руками (если её не отдала БД)."""
        nm = new_scol.value.strip()
        if not nm:
            _log("Впиши имя колонки ведомой."); return
        if any(c["column_name"] == nm for c in state["slave_cols"]):
            _log(f"Колонка ведомой «{nm}» уже есть в списке."); return
        state["slave_cols"] = list(state["slave_cols"]) + [{
            "column_name": nm, "data_type": "", "data_scale": None,
            "is_primary_key": None}]
        new_scol.value = ""
        _on_pair_change()
        _log(f"Колонка ведомой «{nm}» добавлена в список — выбери её в нужной строке.")

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
        state["slave_cols"] = list(scols)
        state["rows"] = [_make_row(m, (pair_map or {}).get(m["column_name"]))
                         for m in mcols]
        _refresh_slave_options()
        _apply_row_filter()

    def _collect_pairs():
        out = []
        for r in state["rows"]:
            name = r["name"].value.strip()
            if not name:
                continue
            out.append((name, None if r["dd"].value == _NONE else r["dd"].value))
        return out

    def _custom_count_warning():
        """В режиме «свой SELECT» вставка позиционная: число строк должно совпадать
        с числом колонок запроса, иначе значения уедут не в те столбцы."""
        if src_mode.value != "custom" or not state.get("snap_count"):
            return None
        rows = len([r for r in state["rows"] if r["name"].value.strip()])
        if rows != state["snap_count"]:
            return (f"⚠ В форме {rows} колонок, а запрос отдавал "
                    f"{state['snap_count']}. Вставка позиционная — если запрос не "
                    f"менялся, лишние строки уведут значения в другие столбцы. "
                    f"Проверь состав или пересними колонки.")
        return None

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

    # ── блок «Триггер на ведущей» ──
    # Справочник и разовый перенос журнал НЕ читают (полная перезаливка), поэтому
    # своего триггера им не требуется. Блок здесь по другой причине: та же таблица
    # часто участвует ещё и в сложной линии (или журнал по ней ведут ради истории),
    # и тогда ставить триггер удобнее сразу, не уходя из формы.
    trg_table = W.Text(description="Таблица", placeholder="по умолчанию — ведущая",
                       layout=W.Layout(width="420px"),
                       style={"description_width": "70px"})
    trg_name_in = W.Text(description="tablename", placeholder="по умолчанию — метка линии",
                         layout=W.Layout(width="420px"),
                         style={"description_width": "70px"})
    trg_pk = W.Text(description="PK", placeholder="колонки через запятую, напр. idrw",
                    layout=W.Layout(width="420px"),
                    style={"description_width": "70px"})
    trg_period = W.Text(description="Период", placeholder="колонка-дата, напр. createdate",
                        layout=W.Layout(width="420px"),
                        style={"description_width": "70px"})

    def _trigger_ctx():
        tbl = trg_table.value.strip() or (tm.value.strip()
                                          if src_mode.value == "table" else "")
        if not tbl:
            raise RuntimeError(
                "не задана таблица (в режиме «свой SELECT» имя ведущей — только "
                "метка линии, впиши реальную таблицу в поле «Таблица»)")
        lbl = trg_name_in.value.strip() or line.value.strip() or B.bare(tm.value.strip())
        if not lbl:
            raise RuntimeError("не задано имя линии (tablename)")
        pk = [c.strip() for c in trg_pk.value.replace(";", ",").split(",") if c.strip()]
        if not pk:
            raise RuntimeError("не заданы колонки PK")
        return {"db": dbm.value, "table": tbl, "tablename": lbl,
                "period_column": trg_period.value.strip() or None,
                "pk_columns": pk, "cred": user_m_get(), "mode": "iud",
                "key": SP.sp_key(lbl, dbm.value, dbs.value)}

    def _trigger_mode_note():
        return ("<span style='color:#888'>Перенос справочника/разовый — "
                "<b>полная перезаливка</b> (DELETE ведомой + INSERT всех строк), "
                "журнал <code>etl_log_iud_row</code> он не читает: <b>для самого "
                "справочника триггер не нужен</b>. Ставь его, если эта же таблица "
                "участвует в сложной линии — тогда <code>tablename</code> должен "
                "совпадать с её <code>tableNameEtlJobs</code>.</span>")

    trg = _trigger_controls(W, out, _trigger_ctx, save_option=False,
                            mode_note_fn=_trigger_mode_note,
                            extra=(trg_table, trg_name_in, trg_pk, trg_period))

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
            # запомнить, сколько колонок отдал источник: если строки правили
            # руками, при сборке предупредим о рассинхроне со «своим SELECT»
            state["snap_count"] = len(mcols)

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
            # Режим источника берём из конфига (load_sp_line сам откатывается
            # на эвристику для фрагментов, сохранённых до появления флага).
            src_mode.value = data.get("src_mode") or "table"
            # Текст SELECT показываем ВСЕГДА, даже в режиме «из таблицы»:
            # переключение тумблера не должно терять сохранённый запрос, а
            # видеть, что реально лежит в Select.sql, полезно в любом режиме.
            f_sql.value = data["select_sql_text"] or ""
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
                warn = _custom_count_warning()
                if warn:
                    print(warn + "\n")
                for rel, content in files:
                    print("=" * 70); print(rel); print("-" * 70); print(content)
                # DDL триггера — в предпросмотре виден всегда (в файлы линии
                # справочника он не пишется: перенос идёт полной перезаливкой)
                print("=" * 70)
                print("DDL ТРИГГЕРА ведущей (в репозиторий НЕ сохраняется)")
                print("-" * 70)
                try:
                    ctx = _trigger_ctx()
                    print(T.build_trigger(
                        ctx["db"], ctx["table"], ctx["tablename"],
                        ctx["period_column"], ctx["pk_columns"],
                        trg["journal"].value.strip() or None)["text"])
                except Exception as e:
                    print(f"(собрать не удалось: {e})")
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
            warn = _custom_count_warning()
            written = _write_current()
            with out:
                out.clear_output()
                if warn:
                    print(warn + "\n")
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

    # Своё сообщение коммита. Пусто — подставляется автоописание (как было).
    commit_msg = W.Text(
        description="Коммит", placeholder="по умолчанию — автоописание правки",
        layout=W.Layout(width="560px"), style={"description_width": "90px"})
    commit_hint = W.HTML(
        "<span style='color:#888'>Текст коммита для обеих кнопок ниже. Пусто — "
        "подставится автоматическое описание.</span>")

    def _commit_msg():
        typed = commit_msg.value.strip()
        if typed:
            return typed
        lbl = line.value.strip() or (B.bare(tm.value.strip()) if tm.value.strip()
                                     else "линия")
        typ = "справочник" if kind.value == "regular" else "разовый"
        verb = "правка" if work_mode.value == "edit" else "новая"
        return f"dagbuilder ({typ}): {verb} {lbl}"

    def _clear_commit_msg():
        commit_msg.value = ""

    btn_pub, pub_confirm = _publish_controls(W, out, _write_current, _commit_msg,
                                             _clear_commit_msg)

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
    btn_add_mrow.on_click(_add_master_row)
    btn_add_scol.on_click(_add_slave_col)

    # push-only для формы сборки и отдельный — для панели вкл/выкл (свой виджет:
    # один и тот же кнопка-виджет нельзя разместить в двух контейнерах).
    # У панели вкл/выкл своё поле коммита: форма сборки там скрыта, её поле
    # было бы не видно.
    tgl_commit_msg = W.Text(
        description="Коммит", placeholder="по умолчанию — автоописание",
        layout=W.Layout(width="560px"), style={"description_width": "90px"})

    def _tgl_commit_msg():
        return (tgl_commit_msg.value.strip()
                or "dagbuilder: вкл/выкл, перевод, удаление линий")

    btn_push_only, push_only_confirm = _push_only_controls(W, out, _commit_msg,
                                                           _clear_commit_msg)
    tgl_btn_push, tgl_push_confirm = _push_only_controls(
        W, out, _tgl_commit_msg, lambda: setattr(tgl_commit_msg, "value", ""))
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
        tgl_commit_msg, W.HBox([tgl_btn_push]), tgl_push_confirm)

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
        manual_help,
        W.HBox([btn_add_mrow, W.HTML("&nbsp;&nbsp;"), new_scol, btn_add_scol]),
        W.HTML("<hr>"), trg["box"],
        W.HTML("<hr>"), commit_hint, commit_msg,
        W.HBox([btn_prev, btn_make, btn_push_only, btn_pub]),
        pub_confirm, push_only_confirm,
    ])

    for _w in (tm, line, dbm, dbs, src_mode):
        _w.observe(lambda _c: trg["refresh"](), names="value")

    _on_src_mode()
    _on_kind()
    _on_mode()
    _update_key_preview()
    trg["refresh"]()

    return W.VBox([
        W.HTML("<h3>Справочники и разовый перенос (delete + insert)</h3>"),
        kind, work_mode, edit_box, toggle_box, W.HTML("<hr>"),
        build_area, W.HTML("<hr>"), out,
    ])


_TRIGGERS_HELP_HTML = """
<div style='max-height:420px;overflow:auto;padding:8px;line-height:1.45'>
<b>Что проверяет кнопка «Проверить»</b>

<p style='margin:8px 0 4px'>Линия собрана в репозитории, а половина её работы
живёт в БД: события в <code>etl_log_iud_row</code> пишет ТРИГГЕР на ведущей.
Если его нет, режимы <code>iud</code> и <code>delete_insert</code> честно
отчитываются пропуском (⬜) и не переносят ничего — по логам это выглядит как
«всё хорошо». Проверка ходит в БД и смотрит:</p>

<ul style='margin:4px 0 4px 18px'>
<li><b>служебные объекты</b> — журнал <code>etl_log_iud_row</code> (и его
колонки), <code>etl_jobs</code>, <code>etl_log</code>; на Oracle ещё
последовательность <code>etl_log_iud_row_seq</code> и триггер
<code>trg_etl_log_iud_row_bi</code> — если он INVALID, ORA-04098 валит любое
изменение любой ведущей;</li>
<li><b>есть ли триггер</b> на ведущей таблице линии и пишет ли он в журнал;</li>
<li><b>tablename</b> — тот ли литерал. Сравнение с <code>etl_jobs.tablename</code>
регистрозависимое: <code>'eindexmo'</code> вместо <code>'EINDEXMO'</code> — это
молчаливый простой линии, поэтому чужой регистр помечается ошибкой;</li>
<li><b>состояние</b> — включён (не DISABLED) и валиден (не INVALID);</li>
<li><b>полнота</b> — INSERT, UPDATE и DELETE, построчный (FOR EACH ROW);</li>
<li><b>тело</b> — упоминаются ли колонка периода, колонки PK и
<code>isetl</code>.</li>
</ul>

<p style='margin:8px 0 4px'><b>Статусы:</b> ✅ в порядке · ⚠️ замечания (работать
будет, но проверь) · ❌ ошибка или триггера нет · 🔌 не удалось проверить
(нет подключения/прав) · ⬜ триггер не требуется.</p>

<p style='margin:8px 0 4px'><b>Кому триггер не нужен.</b> Режимы
<code>section</code> и <code>query_section</code> журнал не читают (группы даёт
<code>isokaudit = 4</code> либо свой SQL периодов). Справочники и разовый
перенос — тем более: они перезаливают ведомую целиком.
<code>section_compare</code> живёт и без триггера (сравнивает срезы), но с ним
реагирует на каждое изменение — поэтому его отсутствие здесь замечание, а не
ошибка.</p>

<p style='margin:8px 0 4px'><b>Создать или поправить триггер</b> — на вкладке
самой линии («Сложный ETL» / «Справочники»), блок «Триггер на ведущей»: там
DDL, проверка и кнопка создания. Массовой кнопки «добавить всем» тут пока нет
намеренно — сперва посмотреть глазами.</p>

<p style='margin:8px 0 4px'><b>Реквизиты.</b> Рантайм ходит в БД под набором
<code>MAIN</code> — проверка по умолчанию тоже. Другой набор нужен, если
ведущие лежат под вторым логином (A56).</p>
</div>
"""


def _triggers_ui():
    """Вкладка «Триггеры»: проверить в БД, что триггеры всех линий на месте.

    Отдельная вкладка (а не кнопка в форме линии) — потому что вопрос «всё ли в
    БД готово» задают ко ВСЕМУ хозяйству сразу, а не к одной линии. Сюда же
    потом лягут массовые действия (добавить/исправить всем)."""
    import ipywidgets as W

    _USER_SENT = "✏ своё…"

    def _cred_field(desc, opts=("MAIN", "A56")):
        dd = W.Dropdown(description=desc, options=list(opts) + [_USER_SENT],
                        value=opts[0], layout=W.Layout(width="280px"),
                        style={"description_width": "150px"})
        txt = W.Text(placeholder="имя набора из .env", layout=W.Layout(width="190px"))
        txt.layout.display = "none"
        dd.observe(lambda _:
                   setattr(txt.layout, "display", "" if dd.value == _USER_SENT else "none"),
                   names="value")

        def get():
            return (txt.value.strip() or "MAIN") if dd.value == _USER_SENT else dd.value
        return W.HBox([dd, txt]), get

    cred_o_box, cred_o_get = _cred_field("Реквизиты Oracle")
    cred_p_box, cred_p_get = _cred_field("Реквизиты Postgres")
    j_orcl = W.Text(description="Журнал Oracle",
                    placeholder=T.JOURNAL_DEFAULT["Orcl"],
                    layout=W.Layout(width="420px"),
                    style={"description_width": "150px"})
    j_post = W.Text(description="Журнал Postgres",
                    placeholder=T.JOURNAL_DEFAULT["Post"],
                    layout=W.Layout(width="420px"),
                    style={"description_width": "150px"})
    inc_sp = W.Checkbox(value=True, indent=False,
                        description="Показывать справочники и разовый перенос "
                                    "(им триггер не нужен)")
    inc_skip = W.Checkbox(value=False, indent=False,
                          description="Показывать линии, которым триггер не требуется")
    help_btn, help_box = _help_toggle(W, _TRIGGERS_HELP_HTML)
    btn_check = W.Button(description="Проверить", button_style="primary",
                         icon="search", layout=W.Layout(width="220px"))
    summary = W.HTML()
    table = W.HTML()
    ddl_pick = W.Dropdown(description="Линия", options=[],
                          layout=W.Layout(width="420px"),
                          style={"description_width": "90px"})
    btn_ddl = W.Button(description="Показать DDL", icon="code",
                       layout=W.Layout(width="190px"))
    ddl_box = W.VBox([W.HTML("<span style='color:#888'>DDL триггера для линии из "
                             "проверки — посмотреть, чего не хватает. Создать его "
                             "можно на вкладке линии.</span>"),
                      W.HBox([ddl_pick, btn_ddl])],
                     layout=W.Layout(display="none"))
    out = W.Output()
    state = {"results": [], "targets": {}, "db_reports": {}}

    def _row_html(r):
        icon = _STATUS_ICON.get(r["status"], "")
        color = {"ok": "#2e7d32", "warn": "#8a6d3b", "error": "#c62828",
                 "missing": "#c62828", "fail": "#6a1b9a",
                 "skip": "#888"}.get(r["status"], "#333")
        details = "".join(
            f"<div style='color:#c62828'>❌ {_esc(p)}</div>"
            for p in (r.get("problems") or []))
        details += "".join(f"<div style='color:#888'>· {_esc(n)}</div>"
                           for n in (r.get("notes") or []))
        if r.get("found"):
            details = (f"<div style='color:#888'>найдено: "
                       f"{_esc(', '.join(r['found']))}</div>" + details)
        return (
            "<tr style='border-bottom:1px solid #eee'>"
            f"<td style='padding:4px 8px;white-space:nowrap'>{icon} "
            f"<b style='color:{color}'>{_esc(_STATUS_WORD.get(r['status'], ''))}</b></td>"
            f"<td style='padding:4px 8px'><code>{_esc(r['key'])}</code></td>"
            f"<td style='padding:4px 8px;white-space:nowrap'>{_esc(r['db'])} · "
            f"{_esc(r['mode'])}</td>"
            f"<td style='padding:4px 8px'><code>{_esc(r['table_master'] or '—')}</code></td>"
            f"<td style='padding:4px 8px'><code>{_esc(r['tablename'])}</code></td>"
            f"<td style='padding:4px 8px'>{details or '—'}</td></tr>")

    def _render(results, db_reports):
        shown = [r for r in results
                 if r["status"] != "skip" or inc_skip.value]
        counts = {}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        parts = [f"{_STATUS_ICON.get(k, '')} {_STATUS_WORD.get(k, k)}: <b>{v}</b>"
                 for k, v in sorted(counts.items(),
                                    key=lambda kv: list(_STATUS_WORD).index(kv[0])
                                    if kv[0] in _STATUS_WORD else 99)]
        db_lines = []
        for db, rep in sorted(db_reports.items()):
            msgs = "<br>".join(_esc(m) for m in rep["messages"])
            db_lines.append(
                f"<div style='margin-top:4px'>{_STATUS_ICON.get(rep['status'], '')} "
                f"<b>{_esc(db)}</b> <span style='color:#888'>(реквизиты "
                f"{_esc(rep['cred'])}, журнал {_esc(rep['journal'])})</span><br>"
                f"<span style='color:#555'>{msgs}</span></div>")
        summary.value = ("<div><b>Итог:</b> " + " · ".join(parts) + "</div>" +
                         "".join(db_lines))
        table.value = (
            "<div style='overflow-x:auto'><table style='border-collapse:collapse;"
            "font-size:13px'><tr style='border-bottom:2px solid #bbb'>"
            "<th style='padding:4px 8px;text-align:left'>Статус</th>"
            "<th style='padding:4px 8px;text-align:left'>Линия</th>"
            "<th style='padding:4px 8px;text-align:left'>БД · режим</th>"
            "<th style='padding:4px 8px;text-align:left'>Ведущая</th>"
            "<th style='padding:4px 8px;text-align:left'>tablename</th>"
            "<th style='padding:4px 8px;text-align:left'>Что нашли</th></tr>" +
            "".join(_row_html(r) for r in shown) + "</table></div>")
        problem = [r["key"] for r in results
                   if r["status"] in ("missing", "error", "warn")]
        opts = problem or [r["key"] for r in results if r["needs"]]
        ddl_pick.options = opts
        # Dropdown, созданный с пустым options, при их появлении оставляет value
        # None — выбираем первую проблемную линию сами, иначе кнопка «Показать
        # DDL» отвечает «сначала нажми Проверить» на уже проверенном списке.
        if opts and ddl_pick.value is None:
            ddl_pick.value = opts[0]
        ddl_box.layout.display = "" if opts else "none"

    def on_check(_):
        from IPython.display import display, HTML
        btn_check.disabled = True
        with out:
            out.clear_output()
            display(HTML(_spinner_html("Собираю линии и смотрю триггеры в БД…")))
        try:
            targets = T.trigger_targets(include_sp=inc_sp.value)
            state["targets"] = {t["key"]: t for t in targets}
            results, db_reports = T.check_targets(
                targets,
                creds={"Orcl": cred_o_get(), "Post": cred_p_get()},
                journals={"Orcl": j_orcl.value.strip() or None,
                          "Post": j_post.value.strip() or None})
            state["results"], state["db_reports"] = results, db_reports
            _render(results, db_reports)
            with out:
                out.clear_output()
                print(_trigger_report_text(results, db_reports))
        except Exception as e:
            with out:
                out.clear_output()
                print(f"Проверка не удалась: {type(e).__name__}: {e}")
        finally:
            btn_check.disabled = False

    def on_ddl(_):
        key = ddl_pick.value
        t = state["targets"].get(key)
        with out:
            out.clear_output()
            if not t:
                print("Сначала нажми «Проверить»."); return
            j = {"Orcl": j_orcl.value.strip(), "Post": j_post.value.strip()}
            try:
                ddl = T.build_trigger(t["db_master"], t["table_master"],
                                      t["tablename"], t["period_column"],
                                      t["pk_columns"],
                                      j.get(t["db_master"]) or None)
            except Exception as e:
                print(f"DDL собрать не удалось: {e}")
                return
            print("=" * 70)
            print(f"{key}: DDL триггера ({t['db_master']})")
            print("-" * 70)
            print(ddl["text"])
            print("Выполнить его можно на вкладке линии — блок «Триггер на ведущей».")

    def _on_filter(_=None):
        # перерисовать уже полученный результат, не ходя в БД заново
        if state["results"]:
            _render(state["results"], state["db_reports"])
    inc_skip.observe(_on_filter, names="value")

    btn_check.on_click(on_check)
    btn_ddl.on_click(on_ddl)

    return W.VBox([
        W.HTML("<h3>Триггеры в БД</h3>"),
        W.HTML("<span style='color:#888'>Линия живёт в двух местах: конфиг и даг — "
               "в репозитории, а события изменений пишет <b>триггер в БД</b>. "
               "Здесь видно, всё ли для линий и справочников на месте.</span>"),
        W.HBox([cred_o_box, cred_p_box]),
        W.HBox([j_orcl, j_post]),
        inc_sp, inc_skip,
        W.HBox([btn_check, help_btn]), help_box,
        summary, table, ddl_box, W.HTML("<hr>"), out,
    ])


def launch():
    """Показать конструктор: три вкладки — «Сложный ETL», «Справочники / разовый»
    и «Триггеры» (проверка того, что есть в БД).

    Это единая точка входа; веб-конструктор (Voilà) запускает именно её через
    tools/dagbuilder_app.ipynb, поэтому обновление этого модуля обновляет и
    веб-версию (после перезапуска сервиса etl-dagbuilder)."""
    import ipywidgets as W
    from IPython.display import display

    tabs = W.Tab([_complex_ui(), _sp_ui(), _triggers_ui()])
    tabs.set_title(0, "Сложный ETL")
    tabs.set_title(1, "Справочники / разовый")
    tabs.set_title(2, "Триггеры")
    display(tabs)
