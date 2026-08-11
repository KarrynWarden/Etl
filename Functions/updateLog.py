"""Запись в etl_log — историческую таблицу с действиями ETL/Audit.

Структура etl_log:
    id_job, timeoper, status, errcode, errtext, action, tablename, idrw

action — ETL_LOG_IUD_ROW / ETL_ISOKAUDIT_4 / Audit / FLK.
errcode (он же audit) — числовой статус -4..-1, 0..1.
"""
from datetime import datetime

from Src.generalQueries import (
    selectIdEtlPostSql,
    selectIdEtlOrclSql,
    selectByIdEtlPostSql,
    selectByIdEtlOrclSql,
    addLogPostSql,
    addLogOrclSql,
    periodBind,
)


_STATUS_BY_AUDIT = {
    1: ("OK", "Обработано записей {count}"),
    0: ("OK", "Обработано записей {count}"),
    -1: ("ERR", "Ошибка в ходе выполнения ETL"),
    -2: ("ERR", "Ошибка в ходе выполнения аудита"),
    -3: ("ERR", "Разные структуры {count} таблиц"),
    -4: ("ERR", "Данные двух таблиц не идентичны"),
}


def UpdateLog(tableName, dbMaster, action, cursor, con,
              count=None, period=None, audit=None):
    isPost = dbMaster == "Post"
    selectIdSql = selectIdEtlPostSql if isPost else selectIdEtlOrclSql
    selectByIdSql = selectByIdEtlPostSql if isPost else selectByIdEtlOrclSql
    addLogSql = addLogPostSql if isPost else addLogOrclSql

    if action == "FLK":
        etlId = None
        audit = -3
    else:
        # NULL-группа ищется по сентинелу: в SelectIdEtl* обе стороны под
        # COALESCE, но сам None в бинд не отправляем (см. generalQueries).
        cursor.execute(selectIdSql,
                       {"TABLENAME": tableName, "PERIOD": periodBind(period)})
        row = cursor.fetchone()
        etlId = row[0] if row else None
        if audit is None and etlId is not None:
            cursor.execute(selectByIdSql, {"IDRW": etlId})
            res = cursor.fetchone()
            audit = res[0] if res else None

    if audit == 0:
        audit = 1
    status, errtext = _STATUS_BY_AUDIT.get(
        audit, ("ERR", f"Неизвестный статус audit={audit}"))
    errtext = errtext.format(count=count)

    cursor.execute(addLogSql, {
        "ID_JOB": etlId,
        "TIMEOPER": datetime.now(),
        "STATUS": status,
        "ERRCODE": audit,
        "ERRTEXT": errtext,
        "ACTION": action,
        "TABLENAME": tableName,
    })
    con.commit()
