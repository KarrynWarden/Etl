#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка дерева ПЕРЕД выкладкой: синтаксис, сборка конфига, парсинг DAG'ов.

Отдельно запускать не нужно — вызывается самими командами выкладки
(`local/dev-push.sh`, `deploy/deploy-prod.sh`) и серверным pre-receive.
Ручной вызов — только чтобы посмотреть глазами:

    bash deploy/check-dags.sh              # это дерево
    bash deploy/check-dags.sh /path/to/tree

Три яруса, от дешёвого к дорогому — каждый ловит свой класс поломок:

  1. СИНТАКСИС. compile() каждого .py. Ни airflow, ни БД, ни окружения не
     нужно, полсекунды. Ловит опечатку вроде `def f(a, #b\\n , c):` — ту
     самую, что 2026-08-07 уехала на прод и положила все даги.
  2. КОНФИГ. assemble() фрагментов config/SpTableName/SpOnce. Битый json или
     дублирующийся ключ линии парсингом дагов НЕ ловится: lineEnabled глушит
     любую ошибку чтения конфига (и правильно делает — иначе кривой конфиг
     ронял бы парсинг), поэтому проверяем отдельно и явно.
  3. ПАРСИНГ DAG'ов. Настоящий DagBag по каталогу dags/. Ловит всё, что
     переживает compile(), но падает на импорте: пропавшее имя, кривая
     сигнатура на уровне модуля, неверный импорт.

Ярус 3 требует только ПАКЕТ airflow — ни scheduler'а, ни webserver'а, ни
metadata-базы. DagBag — библиотечный разбор файлов: поднимать сервер, чтобы
узнать, парсятся ли даги, не нужно. Окружение ярус 3 строит себе сам
(AIRFLOW_HOME во временном каталоге, sqlite-заглушка вместо боевой
metadata-базы) и каталог дагов передаёт в DagBag ЯВНО — чтобы «OK» означало
«я проверил вот это дерево», а не «я что-то где-то проверил».

Коды возврата:
    0 — всё прошло;
    1 — есть ошибки (или ярус 3 недоступен при --require-airflow);
    2 — ярусы 1-2 прошли, ярус 3 пропущен (нет airflow) — «проверено не всё».
"""
import argparse
import os
import sys
import tempfile
import traceback

PY_DIRS = ("Connect", "Functions", "Src", "dags", "tools")
CONFIG_NAMES = ("config", "SpTableName", "SpOnce")

# Минимальная версия python, на которой код В ПРИНЦИПЕ компилируется:
# Functions/updateLogSp.py использует match/case (3.10+). Интерпретатором старше
# проверять нельзя — он забракует валидный файл, и это не теория: именно так
# /usr/bin/python3 на сервере объявил updateLogSp.py «invalid syntax», гейт
# отклонил хороший push, и тест остался на старом коде. Лучше честно сказать
# «проверять нечем», чем выдать ложный вердикт.
_MIN_PYTHON = (3, 10)

OK, BAD, SKIP = "OK", "!!", " ~"


def _line(status, what, detail=""):
    print(f"  {status:>3}  {what}{('  — ' + detail) if detail else ''}")


# ─────────────────────────── 1. синтаксис ───────────────────────────

def checkSyntax(root):
    """compile() каждого .py. Ничего не пишет на диск (в отличие от
    compileall, который сыплет __pycache__), поэтому годится и для чужого
    дерева во временном каталоге, и для боевого клона."""
    files, errors = [], []
    for sub in PY_DIRS:
        base = os.path.join(root, sub)
        if not os.path.isdir(base):
            continue
        for dirPath, dirNames, fileNames in os.walk(base):
            dirNames[:] = [d for d in dirNames if d != "__pycache__"]
            files += [os.path.join(dirPath, f)
                      for f in fileNames if f.endswith(".py")]
    files += [os.path.join(root, f) for f in sorted(os.listdir(root))
              if f.endswith(".py") and os.path.isfile(os.path.join(root, f))]

    for path in sorted(files):
        rel = os.path.relpath(path, root)
        try:
            with open(path, "rb") as fp:
                compile(fp.read(), path, "exec")
        except SyntaxError as err:
            errors.append(f"{rel}:{err.lineno}: {err.msg}")
        except Exception as err:                     # нечитаемый/битый файл
            errors.append(f"{rel}: {type(err).__name__}: {err}")

    if errors:
        _line(BAD, f"синтаксис: {len(errors)} файл(ов) не компилируются")
        for e in errors:
            print(f"        {e}")
    else:
        _line(OK, f"синтаксис: {len(files)} .py")
    return errors


# ──────────────────────────── 2. конфиг ────────────────────────────

def checkConfig(root):
    """Фрагменты config.d/ и т.п. собираются без ошибок и дублей ключей."""
    errors = []
    try:
        from Functions.functionsFile.loadConfig import assemble
    except Exception as err:
        _line(BAD, "конфиг: не импортируется сборщик", f"{type(err).__name__}: {err}")
        return [f"loadConfig: {err}"]

    for name in CONFIG_NAMES:
        try:
            data = assemble(name)
        except Exception as err:
            errors.append(f"{name}: {type(err).__name__}: {err}")
            _line(BAD, f"конфиг {name}", str(err))
        else:
            _line(OK, f"конфиг {name}", f"записей {len(data.get('data', {}))}")
    return errors


# ───────────────────────── 3. парсинг DAG'ов ─────────────────────────

def checkDagbag(root, tmpHome):
    """DagBag по <root>/dags. Возвращает (errors, skipped).

    Окружение ставим САМИ и до импорта airflow: иначе CLI/библиотека возьмут
    dags_folder из ambient-конфига и проверят чужое дерево, отрапортовав «ошибок
    нет» (ровно так тестовый сервер и промолчал 2026-08-07).
    """
    dagsDir = os.path.join(root, "dags")
    if not os.path.isdir(dagsDir):
        _line(BAD, "парсинг DAG'ов", f"нет каталога {dagsDir}")
        return ["нет каталога dags"], False

    os.environ["AIRFLOW_HOME"] = tmpHome
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = dagsDir
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    # Боевую metadata-базу не трогаем ни на чтение: подсовываем пустой sqlite.
    # Имя ключа менялось между версиями airflow — задаём оба.
    stub = f"sqlite:///{os.path.join(tmpHome, 'check.db')}"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = stub
    os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = stub
    os.environ["AIRFLOW__LOGGING__BASE_LOG_FOLDER"] = os.path.join(tmpHome, "logs")

    try:
        from airflow.models.dagbag import DagBag
    except Exception as err:
        _line(SKIP, "парсинг DAG'ов ПРОПУЩЕН — нет пакета airflow",
              f"{type(err).__name__}: {err}")
        return [], True

    try:
        bag = DagBag(dag_folder=dagsDir, include_examples=False)
    except Exception:
        _line(BAD, "парсинг DAG'ов: сам DagBag не отработал")
        traceback.print_exc()
        return ["DagBag не отработал"], False

    if bag.import_errors:
        _line(BAD, f"парсинг DAG'ов: {len(bag.import_errors)} файл(ов) не импортируются")
        for path, err in sorted(bag.import_errors.items()):
            print(f"        {os.path.relpath(path, root)}:")
            for errLine in str(err).strip().splitlines():
                print(f"          {errLine}")
        return [f"{p}: {e}" for p, e in bag.import_errors.items()], False

    _line(OK, f"парсинг DAG'ов: {len(bag.dags)} DAG'ов, ошибок импорта нет")
    return [], False


# ──────────────────────────────── main ────────────────────────────────

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Проверка дерева перед выкладкой (синтаксис / конфиг / DAG'и)")
    parser.add_argument("root", nargs="?", default=None,
                        help="корень проверяемого дерева (по умолчанию — где лежит этот скрипт)")
    parser.add_argument("--require-airflow", action="store_true",
                        help="нет пакета airflow -> ошибка, а не предупреждение "
                             "(так вызывает серверный pre-receive)")
    args = parser.parse_args(argv)

    if sys.version_info < _MIN_PYTHON:
        need = ".".join(str(x) for x in _MIN_PYTHON)
        have = ".".join(str(x) for x in sys.version_info[:3])
        print(f"ОШИБКА: проверка запущена под python {have} ({sys.executable}), "
              f"а коду нужен {need}+.")
        print("Этим интерпретатором проверять нельзя: он забракует валидный "
              "синтаксис (match/case) и даст ложный отказ.")
        print("Запусти через deploy/check-dags.sh — он подбирает интерпретатор, "
              "либо задай ETL_CHECK_PYTHON=/путь/к/python3.10+")
        return 1

    root = os.path.abspath(args.root or
                           os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Проверяемое дерево — источник и кода, и конфигов: ETL_FULL_PATH ставим до
    # импорта проектных модулей, иначе loadConfig соберёт конфиг соседнего клона.
    os.environ["ETL_FULL_PATH"] = root + os.sep
    os.environ.setdefault("ETL_MODE", "")
    sys.path.insert(0, root)

    print(f"== проверка дерева {root} ==")
    errors = []
    errors += checkSyntax(root)
    errors += checkConfig(root)

    with tempfile.TemporaryDirectory(prefix="etl-check-") as tmpHome:
        dagErrors, skipped = checkDagbag(root, tmpHome)
    errors += dagErrors

    print()
    if errors:
        print(f"ЕСТЬ ОШИБКИ ({len(errors)}) — выкладывать нельзя.")
        return 1
    if skipped and args.require_airflow:
        print("ОШИБКА: пакет airflow недоступен, а парсинг DAG'ов обязателен.")
        return 1
    if skipped:
        print("ПРОШЛО ЧАСТИЧНО: синтаксис и конфиг чисты, даги НЕ парсились "
              "(нет airflow). Ошибку уровня импорта эта проверка не увидела.")
        return 2
    print("ПРОШЛО: синтаксис, конфиг и парсинг DAG'ов чисты.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
