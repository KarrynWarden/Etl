"""Даги-процедуры: сборка обвязки конструктором, тело — руками.

Даг-процедура — это не перенос. У него нет структур, сопоставления колонок и
режимов; у него есть расписание, соединения и КОД, который человек пишет сам.
Поэтому конструктор здесь делит файл на две части и владеет только одной:

    обвязка          — dag_id, теги, расписание, ретраи, соединения, task_id.
                       Её собирает этот модуль, и правится она формой.
    тело функции     — ваш python. Конструктор его читает, кладёт в редактор и
                       записывает обратно ДОСЛОВНО, ни во что не вмешиваясь.

Почему не «сгенерировать всё»: A56ProceduresFIX_DEND — двести строк логики с
курсорами по двум базам, и никакая форма её не опишет. Почему не «дать редактор
на весь файл»: тогда расписание и теги правятся текстом, а это ровно тот класс
правок, из-за которых даг перестаёт парситься и исчезает из airflow молча.

РАЗБОР ЧУЖОГО ФАЙЛА. Даг, написанный руками (A56*, A61*), конструктор не
трогает: у него нет маркера, и `load_proc` возвращает `generated=False`. Такой
файл интерфейс показывает целиком и только читает — обвязку в нём разбирает
`envelope_of`, чтобы человек хотя бы видел расписание и теги, не вычитывая
двести строк.

ПРОВЕРКА СИНТАКСИСА. Собранный файл компилируется до записи (`compile`), и
ошибка возвращается с номером строки. Даг с SyntaxError не просто «не
работает» — он пропадает из списка airflow, и по интерфейсу airflow этого не
видно вовсе.
"""
import argparse
import ast
import datetime as dt
import os
import re
import sys
import textwrap

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAGS = os.path.join(ROOT, "dags")

# Маркер «этот файл собран конструктором». По нему и только по нему решается,
# можно ли файл перезаписывать формой. Отсутствие маркера — файл человека.
PROC_MARK = "# dagbuilder: даг-процедура (обвязку правит конструктор, тело — ваше)"

# Заметка человека в docstring. Всё после этой строки конструктор считает
# пользовательским текстом: читает при разборе и кладёт обратно при записи.
PROC_NOTE_MARK = "─── заметка (правится руками, конструктор её сохраняет) ───"

# Соединения, которые можно подключить галочкой. Больше в проекте нет —
# список закрытый намеренно: опечатка в имени даёт ImportError, а он на этапе
# парсинга дага выглядит как «даг исчез».
CONNECTIONS = ("DbConnectOrcl", "DbConnectPost", "DbConnectA56Orcl",
               "DbConnectA56Post")

TASK_ID_DEFAULT = "do_etl_procedures"
FUNC_NAME = "do_etl_procedures"

# Тело по умолчанию для новой процедуры: не «pass», а рабочий каркас с тем, что
# в этом проекте забывают чаще всего — commit и закрытие соединения.
BODY_DEFAULT = '''con = None
try:
    con = DbConnectPost()
    cursor = con.cursor()
    cursor.execute("call схема.пакет.процедура()")
    con.commit()
finally:
    if con is None:
        logging.info("соединение не было открыто")
    else:
        con.close()
        logging.info("соединение закрыто")'''


class BadSpec(Exception):
    """Ошибка, которую показывают человеку текстом, а не трассировкой."""


# ──────────────────────────── проверки полей ────────────────────────────

_DAG_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_TASK_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_spec(spec):
    """Проверить поля формы. -> нормализованный spec, либо BadSpec.

    Проверяется то, что молча ломает даг: имя, из которого получится
    несуществующий модуль, чужое соединение, отрицательные ретраи. Расписание
    проверяется отдельно — оно выражение python, и судить о нём по регулярке
    нельзя (см. check_schedule).
    """
    dag_id = (spec.get("dag_id") or "").strip()
    if not _DAG_ID_RE.match(dag_id):
        raise BadSpec(
            "dag_id должен начинаться с буквы и состоять из латиницы, цифр и "
            "подчёркиваний — это ещё и имя файла: получено {!r}".format(dag_id))

    task_id = (spec.get("task_id") or TASK_ID_DEFAULT).strip()
    if not _TASK_ID_RE.match(task_id):
        raise BadSpec("task_id: {!r} — недопустимое имя задачи".format(task_id))

    conns = tuple(spec.get("connections") or ())
    unknown = [c for c in conns if c not in CONNECTIONS]
    if unknown:
        raise BadSpec("нет таких соединений: {}. Есть: {}".format(
            ", ".join(unknown), ", ".join(CONNECTIONS)))

    retries = spec.get("retries", 2)
    delay = spec.get("retry_delay_min", 5)
    runs = spec.get("max_active_runs", 1)
    for name, value in (("retries", retries), ("retry_delay_min", delay),
                        ("max_active_runs", runs)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise BadSpec("{}: ожидается целое ≥ 0, получено {!r}".format(
                name, value))
    if runs < 1:
        raise BadSpec("max_active_runs должен быть не меньше 1")

    schedule = (spec.get("schedule") or "").strip()
    check_schedule(schedule)

    body = spec.get("body")
    body = BODY_DEFAULT if body is None else body
    check_body(body)

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "tags": [str(t).strip() for t in (spec.get("tags") or []) if str(t).strip()],
        "schedule": schedule,
        "retries": retries,
        "retry_delay_min": delay,
        "max_active_runs": runs,
        "connections": list(conns),
        "doc": spec.get("doc") or "",
        "note": spec.get("note") or "",
        "body": body,
    }


def check_schedule(expr):
    """Расписание — это python-выражение, и проверять его надо как выражение.

    В файле оно стоит как есть: `dt.timedelta(minutes=10)`, `'50 5,7,13 * * *'`,
    `None`. Регуляркой такое не разобрать, а ошибка в нём роняет весь модуль на
    импорте — то есть даг пропадает из airflow, а не «идёт не по расписанию».
    Поэтому просто просим python его разобрать.
    """
    if not expr:
        raise BadSpec("не задано расписание (schedule_interval)")
    try:
        ast.parse(expr, mode="eval")
    except SyntaxError as err:
        raise BadSpec("расписание не разбирается как выражение python: "
                      "{}\n    {}".format(err.msg, expr))
    return expr


def check_body(body):
    """Тело функции обязано быть разбираемым python — до записи, не после.

    Собираем его в фиктивную функцию с тем же отступом, что и в готовом файле:
    ошибка отступа так тоже ловится, а номер строки остаётся тем же, что видит
    человек в редакторе (первая строка тела = строка 1).
    """
    if not (body or "").strip():
        raise BadSpec("тело функции пустое — нечего выполнять")
    text = "def _(*a, **kw):\n" + textwrap.indent(_dedent(body), "    ")
    try:
        ast.parse(text)
    except SyntaxError as err:
        line = (err.lineno or 2) - 1        # минус строка `def _(...)`
        raise BadSpec("тело функции: {} (строка {})".format(err.msg, line))
    return body


def _dedent(text):
    """Убрать общий отступ, не трогая пустые строки и относительные сдвиги."""
    return textwrap.dedent(text.replace("\t", "    ")).rstrip()


# ──────────────────────────────── сборка ────────────────────────────────

def _note_block(note):
    """Хвост docstring с заметкой пользователя ('' — заметки нет).

    Из текста вычищаются сам маркер (иначе следующий разбор «съел» бы заметку
    сам собой) и тройная кавычка, которая закрыла бы docstring раньше времени.
    """
    note = (note or "").replace(PROC_NOTE_MARK, "").replace('"""', "'''").strip()
    return "\n{}\n{}\n".format(PROC_NOTE_MARK, note) if note else ""


def build_proc_py(spec):
    """Собрать текст файла дага-процедуры из проверенного spec."""
    spec = check_spec(spec)
    tags = ", ".join(repr(t) for t in spec["tags"])
    conns = spec["connections"]
    conn_import = ("from Connect import {}\n".format(", ".join(conns))
                   if conns else "")
    doc = (spec["doc"] or "запуск процедуры по расписанию").replace('"""', "'''")
    body = textwrap.indent(_dedent(spec["body"]), "    ")

    return '''"""DAG-процедура: {doc}

{mark_note}Обвязку ниже — расписание, теги, ретраи, соединения — пишет конструктор.
Тело {func}() принадлежит вам: конструктор читает его в редактор и кладёт
обратно дословно, ни во что не вмешиваясь.

Задача идёт в пуле Etl (buildOperator), то есть НЕ стартует, пока идёт аудит:
задача-замок etl_lock занимает пул целиком. Для процедуры, которая правит
таблицы линий, это и нужно — иначе аудит поймает её на середине правки. Для
процедуры, к переносам не относящейся, это просто ожидание в очереди.{note}"""
{mark}
import datetime as dt
import logging

from airflow.models import DAG

{conn_import}from Functions._dagHelpers import DEFAULT_ARGS, buildOperator, configureLogger

args = {{**DEFAULT_ARGS,
        "retries": {retries},
        "retry_delay": dt.timedelta(minutes={delay})}}


def {func}(**context):
{body}


with DAG(
    dag_id="{dag_id}",
    default_args=args,
    max_active_runs={runs},
    tags=[{tags}],
    schedule_interval={schedule},
    catchup=False,
) as dag:
    configureLogger()
    buildOperator("{task_id}", {func})
'''.format(
        doc=doc,
        mark_note="",
        mark=PROC_MARK,
        func=FUNC_NAME,
        note=_note_block(spec["note"]),
        conn_import=conn_import,
        retries=spec["retries"],
        delay=spec["retry_delay_min"],
        body=body,
        dag_id=spec["dag_id"],
        runs=spec["max_active_runs"],
        tags=tags,
        schedule=spec["schedule"],
        task_id=spec["task_id"],
    )


def build_all(spec):
    """-> [(относительный путь, содержимое)] — как у остальных сборщиков.

    Компилируем результат ЗДЕСЬ, а не после записи: даг с SyntaxError не
    «работает неправильно», он исчезает из списка airflow, и по интерфейсу
    airflow причину не найти.
    """
    text = build_proc_py(spec)
    rel = "dags/{}.py".format(check_spec(spec)["dag_id"])
    try:
        compile(text, rel, "exec")
    except SyntaxError as err:
        raise BadSpec("собранный файл не компилируется — {} (строка {}). "
                      "Так быть не должно: сообщите об этом.".format(
                          err.msg, err.lineno))
    return [(rel, text)]


# ──────────────────────────────── разбор ────────────────────────────────

def _dag_call(tree):
    """Найти вызов `DAG(...)` в файле. -> ast.Call или None."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = node.func
            if isinstance(name, ast.Name) and name.id == "DAG":
                return node
            if isinstance(name, ast.Attribute) and name.attr == "DAG":
                return node
    return None


def _kw(call, name):
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _literal(node, default=None):
    if node is None:
        return default
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        return default


def _segment(lines, node):
    """Исходный текст узла — ровно его, без имени ключа и запятой соседа.

    Резать по строкам нельзя: `schedule_interval=dt.timedelta(minutes=10),`
    вернуло бы всю строку целиком, и в форму попало бы `schedule_interval=` со
    всем прочим. Нужны колонки, а они в ast байтовые — поэтому режем байты и
    декодируем обратно, иначе русский комментарий выше по строке сдвинул бы
    срез.
    """
    if node is None or getattr(node, "end_lineno", None) is None:
        return ""
    first, last = node.lineno - 1, node.end_lineno - 1
    if first == last:
        raw = lines[first].encode("utf-8")[node.col_offset:node.end_col_offset]
        return raw.decode("utf-8", "replace")
    head = lines[first].encode("utf-8")[node.col_offset:].decode("utf-8", "replace")
    tail = lines[last].encode("utf-8")[:node.end_col_offset].decode("utf-8", "replace")
    return "\n".join([head] + lines[first + 1:last] + [tail])


def envelope_of(text):
    """Обвязка любого дага: dag_id, теги, расписание, ретраи, task_id.

    Работает и на чужих файлах — там она нужна для чтения: человек должен
    видеть расписание, не вычитывая двести строк.
    """
    out = {"dag_id": "", "tags": [], "schedule": "", "retries": None,
           "retry_delay_min": None, "max_active_runs": 1,
           "task_id": "", "connections": [], "doc": ""}
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return out
    lines = text.split("\n")

    out["doc"] = (ast.get_docstring(tree) or "").strip()

    call = _dag_call(tree)
    if call is not None:
        dag_id = _kw(call, "dag_id")
        if dag_id is None and call.args:
            dag_id = call.args[0]
        out["dag_id"] = _literal(dag_id, "") or ""
        out["tags"] = list(_literal(_kw(call, "tags"), []) or [])
        out["max_active_runs"] = _literal(_kw(call, "max_active_runs"), 1) or 1
        out["schedule"] = _segment(lines, _kw(call, "schedule_interval")).strip()
        if not out["schedule"]:
            out["schedule"] = _segment(lines, _kw(call, "schedule")).strip()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "Connect":
            out["connections"] = [a.name for a in node.names]
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                name = _literal(key)
                if name == "retries":
                    out["retries"] = _literal(value)
                if name == "retry_delay":
                    out["retry_delay_min"] = _minutes(value)
        if isinstance(node, ast.Call) and not out["task_id"]:
            func = node.func
            named = (getattr(func, "id", "") or getattr(func, "attr", ""))
            if named in ("buildOperator", "PythonOperator"):
                task = _kw(node, "task_id")
                if task is None and node.args:
                    task = node.args[0]
                out["task_id"] = _literal(task, "") or ""
    return out


def _minutes(node):
    """`dt.timedelta(minutes=5)` -> 5. Иначе None — угадывать не станем."""
    if not isinstance(node, ast.Call):
        return None
    if getattr(node.func, "attr", "") != "timedelta":
        return None
    value = _kw(node, "minutes")
    return _literal(value)


def _body_of(text):
    """Текст тела функции do_etl_procedures без общего отступа ('' — нет её)."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return ""
    lines = text.split("\n")
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == FUNC_NAME:
            first = node.body[0]
            start = first.lineno - 1
            # docstring функции в тело не входит: он часть обвязки
            end = node.end_lineno
            return _dedent("\n".join(lines[start:end]))
    return ""


def _note_of(doc):
    """Заметка человека из docstring ('' — её там нет)."""
    if PROC_NOTE_MARK not in doc:
        return ""
    return doc.split(PROC_NOTE_MARK, 1)[1].strip()


def is_generated(text):
    return PROC_MARK in text


def load_proc(dag_id, root=None):
    """Открыть даг-процедуру для правки.

    Собранный конструктором отдаётся полями формы плюс телом; чужой — целиком
    текстом, и обвязка в нём только для чтения. Различие не косметическое:
    перезаписать чужой файл формой значит потерять всё, чего форма не знает.
    """
    root = root or ROOT
    path = os.path.join(root, "dags", "{}.py".format(dag_id))
    if not os.path.exists(path):
        raise BadSpec("нет такого дага: dags/{}.py".format(dag_id))
    with open(path, encoding="utf-8") as fp:
        text = fp.read()

    env = envelope_of(text)
    generated = is_generated(text)
    doc = env["doc"]
    out = {
        "dag_id": env["dag_id"] or dag_id,
        "generated": generated,
        "path": "dags/{}.py".format(dag_id),
        "source": text,
        "tags": env["tags"],
        "schedule": env["schedule"],
        "retries": env["retries"] if env["retries"] is not None else 2,
        "retry_delay_min": (env["retry_delay_min"]
                            if env["retry_delay_min"] is not None else 5),
        "max_active_runs": env["max_active_runs"],
        "task_id": env["task_id"] or TASK_ID_DEFAULT,
        "connections": env["connections"],
        "note": _note_of(doc),
        "doc": doc.split(PROC_NOTE_MARK)[0].split("\n")[0]
                  .replace("DAG-процедура: ", "").strip(),
        "body": _body_of(text) if generated else "",
    }
    return out


def list_procs(root=None):
    """Все даги-процедуры: собранные конструктором и написанные руками.

    Признак — тег `procedures`, а не имя файла: имена в проекте разные
    (A56ProceduresFERZL_LOAD, A61ProceduresScanLogProlong), а тег ставят все.
    """
    root = root or ROOT
    dags = os.path.join(root, "dags")
    out = []
    if not os.path.isdir(dags):
        return out
    for name in sorted(os.listdir(dags)):
        if not name.endswith(".py") or name.startswith("_"):
            continue
        path = os.path.join(dags, name)
        try:
            with open(path, encoding="utf-8") as fp:
                text = fp.read()
        except OSError:
            continue
        env = envelope_of(text)
        if "procedures" not in env["tags"] and not is_generated(text):
            continue
        out.append({
            "dag_id": env["dag_id"] or name[:-3],
            "file": name,
            "generated": is_generated(text),
            "tags": env["tags"],
            "schedule": env["schedule"],
            "task_id": env["task_id"],
        })
    return out


def defaults():
    """Заготовка новой процедуры — то, что подставляется в пустую форму."""
    return {
        "dag_id": "",
        "task_id": TASK_ID_DEFAULT,
        "tags": ["procedures"],
        "schedule": "dt.timedelta(minutes=60)",
        "retries": 2,
        "retry_delay_min": 5,
        "max_active_runs": 1,
        "connections": ["DbConnectPost"],
        "doc": "",
        "note": "",
        "body": BODY_DEFAULT,
        "known_connections": list(CONNECTIONS),
    }


# ────────────────────────────── самопроверка ──────────────────────────────

def _selftest():
    import tempfile

    spec = {
        "dag_id": "A99ProceduresProba",
        "tags": ["A99", "procedures", "DbSync"],
        "schedule": "dt.timedelta(minutes=10)",
        "retries": 3,
        "retry_delay_min": 7,
        "max_active_runs": 1,
        "connections": ["DbConnectPost", "DbConnectOrcl"],
        "doc": "проба пера",
        "note": "это моя заметка,\nв две строки",
        "body": 'con = DbConnectPost()\ncursor = con.cursor()\ncursor.execute("call x()")\ncon.commit()',
    }
    files = build_all(spec)
    assert len(files) == 1, files
    rel, text = files[0]
    assert rel == "dags/A99ProceduresProba.py", rel
    compile(text, rel, "exec")            # обязано компилироваться

    # обвязка читается обратно ТОЧНО такой, какой её задали: иначе форма при
    # открытии показала бы не то, что в файле, и первая же запись это закрепила
    env = envelope_of(text)
    assert env["dag_id"] == spec["dag_id"], env
    assert env["tags"] == spec["tags"], env
    assert env["schedule"] == "dt.timedelta(minutes=10)", env
    assert env["retries"] == 3 and env["retry_delay_min"] == 7, env
    assert env["task_id"] == TASK_ID_DEFAULT, env
    assert set(env["connections"]) == set(spec["connections"]), env

    # тело возвращается ДОСЛОВНО — на этом держится всё разделение
    assert _body_of(text) == _dedent(spec["body"]), repr(_body_of(text))
    assert _note_of(env["doc"]) == spec["note"], repr(_note_of(env["doc"]))

    # круг замкнут: разобрали -> собрали -> тот же текст
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "dags"))
        with open(os.path.join(tmp, "dags", "A99ProceduresProba.py"), "w",
                  encoding="utf-8") as fp:
            fp.write(text)
        loaded = load_proc("A99ProceduresProba", root=tmp)
        assert loaded["generated"] is True, loaded
        assert loaded["body"] == _dedent(spec["body"]), loaded["body"]
        assert loaded["note"] == spec["note"], loaded["note"]
        assert loaded["doc"] == "проба пера", loaded["doc"]
        again = build_all(loaded)[0][1]
        assert again == text, "круг не замкнулся — правка меняла бы файл сама"

        # ЧУЖОЙ файл: обвязку читаем, но собранным не считаем и тела не берём
        chuzhoy = ('"""руками написанный даг"""\n'
                   "import datetime as dt\n"
                   "from airflow.models import DAG\n"
                   "from airflow.operators.python_operator import PythonOperator\n"
                   "def do_etl_procedures():\n    pass\n"
                   "with DAG(dag_id='A56ProceduresRuchnoy', tags=['A56','procedures'],\n"
                   "         max_active_runs=1, schedule_interval='50 5 * * *',\n"
                   "         catchup=False) as dag:\n"
                   "    PythonOperator(task_id='do_etl_procedures',\n"
                   "                   python_callable=do_etl_procedures, dag=dag)\n")
        with open(os.path.join(tmp, "dags", "A56ProceduresRuchnoy.py"), "w",
                  encoding="utf-8") as fp:
            fp.write(chuzhoy)
        alien = load_proc("A56ProceduresRuchnoy", root=tmp)
        assert alien["generated"] is False, alien
        assert alien["body"] == "", "тело чужого дага попало в форму"
        assert alien["schedule"] == "'50 5 * * *'", alien["schedule"]
        assert alien["tags"] == ["A56", "procedures"], alien
        assert alien["task_id"] == "do_etl_procedures", alien
        assert alien["source"] == chuzhoy, "чужой файл отдан не дословно"

        listed = {p["dag_id"]: p for p in list_procs(root=tmp)}
        assert set(listed) == {"A99ProceduresProba", "A56ProceduresRuchnoy"}, listed
        assert listed["A99ProceduresProba"]["generated"] is True
        assert listed["A56ProceduresRuchnoy"]["generated"] is False

    # ── что обязано быть отвергнуто ДО записи ────────────────────────────
    def bad(patch, why):
        try:
            build_all(dict(spec, **patch))
        except BadSpec:
            return
        raise AssertionError("пропущено: " + why)

    bad({"dag_id": "3плохое имя"}, "dag_id, из которого не выйдет имя файла")
    bad({"dag_id": ""}, "пустой dag_id")
    bad({"task_id": "не имя"}, "task_id с пробелом")
    bad({"connections": ["DbConnectMars"]}, "несуществующее соединение")
    bad({"retries": -1}, "отрицательные ретраи")
    bad({"retries": "три"}, "ретраи строкой")
    bad({"max_active_runs": 0}, "max_active_runs = 0")
    bad({"schedule": ""}, "пустое расписание")
    bad({"schedule": "dt.timedelta(minutes=10"}, "расписание с незакрытой скобкой")
    bad({"body": ""}, "пустое тело")
    bad({"body": "if True:\npass"}, "тело со сломанным отступом")
    bad({"body": "cursor.execute('x'"}, "тело с незакрытой скобкой")

    # номер строки в ошибке тела — от начала ТЕЛА, а не собранного файла:
    # иначе человек ищет ошибку не там, где её видит в редакторе
    try:
        check_body("a = 1\nb = (\n")
    except BadSpec as err:
        assert "строка 2" in str(err), str(err)
    else:
        raise AssertionError("сломанное тело принято")

    # расписание строкой-кроном — тоже выражение и тоже обязано проходить
    cron = build_all(dict(spec, schedule="'50 5,7,13 * * *'"))[0][1]
    assert "schedule_interval='50 5,7,13 * * *'" in cron, cron
    assert envelope_of(cron)["schedule"] == "'50 5,7,13 * * *'"

    # без соединений импорт Connect не пишется вовсе — пустая строка импорта
    # оставила бы `from Connect import ` и SyntaxError
    nocon = build_all(dict(spec, connections=[], body="logging.info('пусто')"))[0][1]
    assert "from Connect import" not in nocon, nocon
    compile(nocon, "x.py", "exec")

    # заметка с тройной кавычкой не должна закрывать docstring раньше времени
    tricky = build_all(dict(spec, note='а тут """ кавычки'))[0][1]
    compile(tricky, "x.py", "exec")

    # и заготовка новой процедуры обязана собираться как есть
    blank = defaults()
    blank["dag_id"] = "A99ProceduresNovaya"
    compile(build_all(blank)[0][1], "x.py", "exec")

    print("proc_builder selftest OK")
    return 0


def main(argv):
    ap = argparse.ArgumentParser(description="Даги-процедуры: сборка и разбор")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--list", action="store_true", help="перечислить процедуры")
    ap.add_argument("--show", metavar="DAG_ID", help="показать разбор дага")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    if args.list:
        for item in list_procs():
            print("{:<32} {:<10} {}".format(
                item["dag_id"],
                "конструктор" if item["generated"] else "руками",
                item["schedule"]))
        return 0
    if args.show:
        import json
        print(json.dumps(load_proc(args.show), ensure_ascii=False, indent=2))
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
