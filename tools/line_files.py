#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Все файлы одной линии — чтобы выложить её отдельно от остального.

Линия живёт не в одном файле: даг, фрагмент конфига, json-структуры ведущей и
ведомой, иногда свой SELECT или пара Add/Select для справочника. Скрипт
собирает этот список по конфигу, чтобы `deploy/deploy-prod.sh --line` мог
взять с теста ровно её, не трогая остальную работу.

    python3 tools/line_files.py PRBSMOPostOrcl
    python3 tools/line_files.py SPMKBOrclPost MEDREE_PRDISPOrclPost
    python3 tools/line_files.py --root /tmp/worktree PRBSMOPostOrcl

Печатает пути относительно корня репозитория, по одному на строку; чего нет на
диске — молча пропускает. Ключ ищется во всех трёх конфигах (config,
SpTableName, SpOnce), регистр важен — он совпадает с именем линии в etl_jobs.
"""
import argparse
import glob
import json
import os
import re
import sys

CONFIGS = ("config", "SpTableName", "SpOnce")
# Ключи фрагмента, значения которых — пути внутри etlFolder/
PATH_KEYS = ("structureMaster", "structureSlave", "selectSql", "addSql")


def _fragments(root, name):
    """(путь_фрагмента, содержимое) для всех кусков конфига `name`."""
    pattern = os.path.join(root, "etlFolder", f"{name}.d", "*.json")
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path, encoding="utf-8") as fp:
                yield path, json.load(fp)
        except (OSError, ValueError) as err:
            print(f"# пропущен {path}: {err}", file=sys.stderr)


def findLine(root, key):
    """(путь фрагмента, тело линии) по ключу; (None, None) — не нашлось."""
    for name in CONFIGS:
        for path, data in _fragments(root, name):
            if key in data:
                return path, data[key]
    return None, None


def similarKeys(root, key):
    """Ключи, отличающиеся только регистром — самая частая опечатка.

    Регистр не косметика: он совпадает с регистром имени линии в etl_jobs
    (Oracle пишет ВЕРХНИМ, Postgres — нижним), поэтому подсказываем, а не
    подставляем молча.
    """
    found = []
    for name in CONFIGS:
        for _, data in _fragments(root, name):
            found.extend(k for k in data if k.lower() == key.lower())
    return found


def dagFiles(root, key, entry):
    """Даги, относящиеся к линии.

    Ищем по вхождению ключа или имени линии в etl_jobs (ключ без хвоста
    OrclPost/PostOrcl/...): даг называет её в makeEtlOperator. Соответствие
    «один файл — одна линия» не гарантировано (mocheck, справочники), поэтому
    возвращаем все совпадения.
    """
    etlJobsName = re.sub(r"(Orcl|Post)(Orcl|Post)$", "", key)
    needles = {key, etlJobsName}
    for extra in ("tableNameSlave",):
        if entry.get(extra):
            needles.add(entry[extra])
    found = []
    for path in sorted(glob.glob(os.path.join(root, "dags", "*.py"))):
        try:
            with open(path, encoding="utf-8") as fp:
                text = fp.read()
        except OSError:
            continue
        # Общие даги-диспетчеры (SpDagNew/SpOnce) гоняют ВСЕ справочники разом:
        # они относятся к линии не больше, чем к любой другой, и выкладывать их
        # вместе с одной линией нельзя — это уже изменение общего кода.
        if os.path.basename(path) in ("SpDagNew.py", "SpOnce.py", "AuditAll.py"):
            continue
        if any(n and n in text for n in needles):
            found.append(path)
    return found


def lineFiles(root, key):
    fragment, entry = findLine(root, key)
    if fragment is None:
        return None
    files = [fragment]
    for pathKey in PATH_KEYS:
        rel = entry.get(pathKey)
        if not rel or os.path.isabs(rel):
            continue
        files.append(os.path.join(root, "etlFolder", rel))
    files.extend(dagFiles(root, key, entry))
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("keys", nargs="+", help="ключи линий (как в config.d)")
    parser.add_argument("--root", default=None,
                        help="корень репозитория (по умолчанию — корень этого клона)")
    args = parser.parse_args()

    root = args.root or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    root = os.path.abspath(root)

    result, missing = [], []
    for key in args.keys:
        files = lineFiles(root, key)
        if files is None:
            missing.append(key)
            continue
        for path in files:
            rel = os.path.relpath(path, root)
            if os.path.exists(path) and rel not in result:
                result.append(rel)

    if missing:
        print("Не нашёл линии в конфигах: " + ", ".join(missing), file=sys.stderr)
        for key in missing:
            similar = similarKeys(root, key)
            if similar:
                print(f"  есть {similar[0]!r} — различие только в РЕГИСТРЕ",
                      file=sys.stderr)
        print("Ключ регистрозависим и совпадает с именем линии в etl_jobs "
              "(см. etlFolder/config.d/).", file=sys.stderr)
        return 1
    if not result:
        print("У линии нет файлов на диске — проверь пути в фрагменте конфига.",
              file=sys.stderr)
        return 1

    print("\n".join(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
