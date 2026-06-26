#!/usr/bin/env python3
"""Отобрать из папки справочников (queries/sp) только те .sql, что упомянуты
в SpTableName.json (и, при желании, в SpOnce.json).

Ничего не удаляет: копирует нужные файлы в чистую папку и показывает, чего
не хватает и что лишнее. Запускать там, где лежит ПОЛНАЯ папка sp (на сервере).

Примеры:
  # только посмотреть, что упомянуто / лишнее / отсутствует:
  python3 filter-sp-queries.py --json SpTableName.json --src /opt/airflow/airflow/etlFolder/queries/sp --list

  # скопировать только упомянутые в чистую папку:
  python3 filter-sp-queries.py --json SpTableName.json \
      --src /opt/airflow/airflow/etlFolder/queries/sp --dst ./sp_clean

  # учесть и разовые (SpOnce.json):
  python3 filter-sp-queries.py --json SpTableName.json --json SpOnce.json \
      --src .../queries/sp --dst ./sp_clean
"""
import argparse
import glob
import json
import os
import shutil


def _json_paths(inputs):
    """Развернуть аргументы --json: файл -> он сам; папка фрагментов -> все *.json."""
    paths = []
    for p in inputs:
        if os.path.isdir(p):
            paths += sorted(glob.glob(os.path.join(p, "*.json")))
        else:
            paths.append(p)
    return paths


def _entries(obj):
    """Записи из целого конфига {"data": {...}} ИЛИ из фрагмента {"<ключ>": {...}}."""
    data = obj.get("data") if isinstance(obj, dict) else None
    src = data if isinstance(data, dict) else obj
    return [v for v in src.values() if isinstance(v, dict)]


def referenced(inputs):
    """Относительные пути <имя>/<файл>.sql, упомянутые в json (после '/sp/').

    Принимает и целые SpTableName.json/SpOnce.json, и папки фрагментов
    SpTableName.d/ (источник истины после дробления).
    """
    rel = set()
    for jf in _json_paths(inputs):
        obj = json.load(open(jf, encoding="utf-8"))
        for entry in _entries(obj):
            for val in entry.values():
                if isinstance(val, str) and val.strip().endswith(".sql"):
                    v = val.strip().replace("\\", "/")
                    if "/sp/" in v:
                        rel.add(v.split("/sp/", 1)[1])
    return rel


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="append", required=True, help="SpTableName.json (можно несколько --json)")
    ap.add_argument("--src", required=True, help="полная папка queries/sp")
    ap.add_argument("--dst", help="куда скопировать отобранные (если не задано — только отчёт)")
    ap.add_argument("--list", action="store_true", help="только показать, не копировать")
    args = ap.parse_args()

    rel = referenced(args.json)
    print(f"упомянуто .sql: {len(rel)}")

    src_files = set()
    for root, _, files in os.walk(args.src):
        for f in files:
            if f.endswith(".sql"):
                p = os.path.relpath(os.path.join(root, f), args.src).replace("\\", "/")
                src_files.add(p)

    present = sorted(rel & src_files)
    missing = sorted(rel - src_files)
    extra = sorted(src_files - rel)

    print(f"в src найдено: {len(present)}; отсутствует: {len(missing)}; лишних в src: {len(extra)}")
    if missing:
        print("\n-- ОТСУТСТВУЮТ (упомянуты в json, но файла нет) --")
        for m in missing:
            print("  ", m)
    if extra:
        print("\n-- ЛИШНИЕ (файл есть, но в json не упомянут) --")
        for e in extra:
            print("  ", e)

    if args.dst:
        for p in present:
            dstp = os.path.join(args.dst, p)
            os.makedirs(os.path.dirname(dstp), exist_ok=True)
            shutil.copy2(os.path.join(args.src, p), dstp)
        print(f"\nскопировано {len(present)} файлов в {args.dst}")
    elif not args.list:
        print("\n(добавь --dst <папка>, чтобы скопировать отобранное; или --list — только отчёт)")


if __name__ == "__main__":
    main()
