"""Сборка конфигов из фрагментов + загрузка параметров линии.

Источник истины для редактирования — НЕ один большой файл, а фрагменты:
    etlFolder/config.base.json     — глобальные ключи (_comment, _mocheckCommon)
    etlFolder/config.d/<ключ>.json — по файлу на линию: {"<ключ>": {...}}
Сборщик объединяет их в один словарь {...глобальное..., "data": {...все линии...}},
идентичный прежнему config.json. Так коллеги добавляют линию новым файлом, не
трогая общий — никаких merge-конфликтов.

Дословный общий вид (для просмотра) генерируется тем же сборщиком в
etlFolder/config.json скриптом tools/regen_config.py (этот файл — артефакт, в
.gitignore; рантайм читает фрагменты, а не его).

Тот же механизм переиспользуется для SpTableName / SpOnce (assemble("SpTableName")).

Ключ конфига = tableNameEtlJobs + dbMaster + dbSlave, например 'reqprepmomocheckPostPost'.
"""
import glob
import json
import os

from Src.fullPath import FULL_PATH

_BASE = f"{FULL_PATH}etlFolder"


def assemble(name):
    """Собрать конфиг `name` из <name>.base.json + <name>.d/*.json.

    name: 'config' | 'SpTableName' | 'SpOnce'. Возвращает словарь вида
    {...глобальные ключи..., "data": {...все записи...}}. Дубликат ключа между
    фрагментами — ошибка (чтобы не затереть чужую линию незаметно).
    """
    result = {}
    basePath = os.path.join(_BASE, f"{name}.base.json")
    if os.path.exists(basePath):
        with open(basePath, encoding="utf-8") as fp:
            result = json.load(fp)
    data = result.setdefault("data", {})
    for frag in sorted(glob.glob(os.path.join(_BASE, f"{name}.d", "*.json"))):
        with open(frag, encoding="utf-8") as fp:
            part = json.load(fp)
        for key, entry in part.items():
            if key in data:
                raise ValueError(
                    f"Дубликат ключа '{key}' (файл {os.path.basename(frag)}). "
                    f"Имя ключа должно быть уникальным между фрагментами."
                )
            data[key] = entry
    return result


def resolvePath(path):
    """Относительный путь -> {FULL_PATH}etlFolder/<path>; абсолютный -> как есть.

    Та же логика, что do_etl._resolveEtlPath, но без тяжёлого импорта do_etl —
    для Sp-дагов (addSql/selectSql из SpTableName/SpOnce). Один etlFolder на все
    среды: относительный путь работает и локально, и на сервере.
    """
    if not path or os.path.isabs(path):
        return path
    return f"{FULL_PATH}etlFolder/{path}"


def loadFullConfig():
    """Полный конфиг переноса (бывший config.json) — собранный из фрагментов."""
    return assemble("config")


def LoadConfig(configKey):
    """Параметры одной линии по ключу tableNameEtlJobs+dbMaster+dbSlave.

    Ключ РЕГИСТРОЗАВИСИМЫЙ и подобран так не случайно: его префикс —
    tableNameEtlJobs, то есть значение колонки tablename в etl_jobs /
    etl_log_iud_row, а сравнение с ней в SQL тоже регистрозависимое. Регистр
    задаёт триггер ведущей: Oracle пишет имена ВЕРХНИМ регистром, Postgres —
    нижним. Поэтому «подобрать» ключ без учёта регистра нельзя — линия бы
    стартовала, но не нашла бы в журнале ни одной своей записи.
    Зато про такое расхождение полезно сказать прямо.
    """
    config = loadFullConfig()
    data = config["data"]
    if configKey not in data:
        raise KeyError(f"Конфиг для ключа {configKey} не найден."
                       + _missingKeyHint(configKey, data))
    return data[configKey]


def _missingKeyHint(configKey, data):
    """Подсказка к «конфиг не найден»: регистр / направление / не тот etlFolder.

    Без неё сообщение одинаково выглядит и когда фрагмент назван не так, и
    когда рантайм вообще смотрит в ЧУЖОЙ etlFolder (ETL_FULL_PATH указывает на
    другую среду, или выкачена ветка без этого фрагмента) — а чинится это
    по-разному. Поэтому всегда печатаем реальный каталог и что в нём найдено.
    """
    fragDir = os.path.join(_BASE, "config.d")
    parts = []

    similar = [k for k in data if k.lower() == configKey.lower()]
    if similar:
        parts.append(f" Есть ключ {similar[0]!r} — различие только в РЕГИСТРЕ. "
                     f"Регистр имени линии должен совпадать с тем, что пишет в "
                     f"etl_log_iud_row/etl_jobs триггер ведущей БД (Oracle — "
                     f"ВЕРХНИЙ, Postgres — нижний); переименуй фрагмент в "
                     f"config.d, а не подгоняй даг.")

    # тот же префикс (имя линии), но другое направление — частая опечатка в даге
    line = configKey[:-8]
    if line:
        others = sorted(k for k in data
                        if k != configKey and k[:-8].lower() == line.lower())
        if others:
            parts.append(f" Для линии {line!r} есть направления: "
                         f"{', '.join(repr(k[-8:]) for k in others)}.")

    fragPath = os.path.join(fragDir, f"{configKey}.json")
    if os.path.exists(fragPath):
        parts.append(f" Файл {fragPath} существует, но ключа {configKey!r} "
                     f"внутри нет — имя файла и ключ внутри должны совпадать.")
    parts.append(f" Читалось из {fragDir} (ETL_FULL_PATH={FULL_PATH!r}), "
                 f"собрано линий: {len(data)}. Если каталог не тот — проверь "
                 f"ETL_FULL_PATH среды; если фрагмента там нет — выкати его в "
                 f"ветку деплоя вместе с дагом.")
    return "".join(parts)
