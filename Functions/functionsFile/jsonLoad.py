"""Загрузка json-структуры таблицы.

Структура хранится в виде массива объектов:
    {"data": [{"column_name": "...", "data_type": "...",
                "data_scale": null, "is_primary_key": "Primary Key"|null}, ...]}

Возвращается список кортежей (name, data_type, data_scale, is_primary_key).
"""
import json


def _load(path, keys):
    with open(path, encoding="utf-8") as fp:
        data = json.load(fp)
    return [tuple(item[k] for k in keys) for item in data["data"]]


def JsonLoadPost(path):
    return _load(
        path,
        ("column_name", "data_type", "data_scale", "is_primary_key"),
    )


def JsonLoadOrcl(path):
    return _load(
        path,
        ("COLUMN_NAME", "DATA_TYPE", "DATA_SCALE", "IS_PRIMARY_KEY"),
    )
