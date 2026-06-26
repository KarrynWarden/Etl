#!/usr/bin/env python3
"""Сгенерировать дословный общий вид конфига из фрагментов — для просмотра.

Пишет etlFolder/<name>.json как ТОЧНУЮ сборку <name>.base.json + <name>.d/*.json
тем же сборщиком, что использует рантайм (Functions.functionsFile.loadConfig.assemble),
поэтому разойтись с тем, что реально читает airflow, не может.

Эти .json — артефакты (в .gitignore). Рантайм читает фрагменты напрямую; файл
нужен только чтобы открыть и увидеть все линии разом. Запускается при деплое
(post-receive) и локально по желанию.

  python3 tools/regen_config.py                       # config.json
  python3 tools/regen_config.py config SpTableName SpOnce
"""
import json
import os
import sys

# Базу определяем по расположению скрипта (корень репозитория), чтобы работать
# без cwd/окружения; если ETL_FULL_PATH уже задан (сервер/деплой) — он в приоритете.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
sys.path.insert(0, ROOT)

from Functions.functionsFile.loadConfig import assemble, _BASE  # noqa: E402


def main(names):
    for name in names:
        data = assemble(name)
        out = os.path.join(_BASE, f"{name}.json")
        with open(out, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
            fp.write("\n")
        print(f"{out}: записей {len(data.get('data', {}))}")


if __name__ == "__main__":
    main(sys.argv[1:] or ["config"])
