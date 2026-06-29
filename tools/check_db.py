#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка доступности БД из текущего окружения (.env + Connect).

Печатает ТОЛЬКО OK/FAIL по каждому набору реквизитов — ни логины, ни пароли,
ни IP не выводятся (сообщения драйверов psycopg2/cx_Oracle пароль не содержат,
но строка ошибки на всякий случай обрезается).

Запуск из корня клона:

    PYTHONPATH=$PWD python3 tools/check_db.py            # набор MAIN
    PYTHONPATH=$PWD python3 tools/check_db.py MAIN A56   # выбрать наборы
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("ETL_FULL_PATH", ROOT + os.sep)
sys.path.insert(0, ROOT)


def _short(e):
    return type(e).__name__ + ": " + str(e).replace("\n", " ")[:200]


def _pg_direct(name):
    import psycopg2
    p = name.upper()
    return psycopg2.connect(
        host=os.environ[f"ETL_POST_{p}_HOST"],
        port=os.environ.get(f"ETL_POST_{p}_PORT", "5432"),
        database=os.environ[f"ETL_POST_{p}_DB"],
        user=os.environ[f"ETL_POST_{p}_USER"],
        password=os.environ[f"ETL_POST_{p}_PWD"],
    )


def main(names):
    # .env -> os.environ (не затирая уже заданные переменные)
    try:
        from Src.loadEnv import loadEnv
        loadEnv()
    except Exception as e:
        print("Не удалось загрузить .env:", _short(e))

    try:
        from Connect import connectPostgres, connectOracle
        have_connect = True
    except Exception as e:
        have_connect = False
        print("Импорт Connect не удался (вероятно, нет драйвера):", _short(e))
        print("Postgres проверю напрямую через psycopg2, Oracle пропущу.")

    for name in names:
        try:
            c = connectPostgres(name) if have_connect else _pg_direct(name)
            c.close()
            print(f"Postgres {name}: OK")
        except Exception as e:
            print(f"Postgres {name}: FAIL:", _short(e))

        if have_connect:
            try:
                c = connectOracle(name)
                c.close()
                print(f"Oracle {name}: OK")
            except Exception as e:
                print(f"Oracle {name}: FAIL:", _short(e))


if __name__ == "__main__":
    main(sys.argv[1:] or ["MAIN"])
