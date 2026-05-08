"""Загрузка параметров для конкретной пары (taблица + направление).

Ключ конфига формируется как tableNameEtlJobs + dbMaster + dbSlave,
например: 'reqprepmomocheckPostPost'.
"""
import json

from Src.fullPath import FULL_PATH, MODE


def LoadConfig(configKey):
    with open(f"{FULL_PATH}etlFolder{MODE}/config.json", encoding="utf-8") as fp:
        config = json.load(fp)
    if configKey not in config["data"]:
        raise KeyError(f"Конфиг для ключа {configKey} не найден")
    return config["data"][configKey]
