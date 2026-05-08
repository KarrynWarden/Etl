"""Прочитать содержимое .sql-файла как строку."""


def TakeOneQuery(path):
    with open(path, encoding="utf-8") as fp:
        return fp.read()
