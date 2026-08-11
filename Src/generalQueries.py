"""Загрузка SQL-запросов из файлов.

Все запросы — в .sql файлах (никаких многострочных f-строк в коде Python),
чтобы их можно было открывать в DBeaver и т.п. без правок.
"""
import datetime

from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH

# ── сентинел периода ──────────────────────────────────────────────────────────
# NULL — полноправная группа (у expmed поле-период какое-то время пустое, и это
# нормальное состояние данных). Сравнить NULL через `=` нельзя, поэтому в SQL обе
# стороны приводятся COALESCE'ом к этой дате. В БИНД NULL не уходит никогда:
# подстановку делает periodBind() — иначе пришлось бы гадать, каким типом драйвер
# пошлёт None (cx_Oracle по умолчанию строкой, а COALESCE(строка, дата) это
# ORA-00932). Литерал обязан совпадать со значением PERIOD_SENTINEL и с тем, что
# зашито в .sql-файлах etl_jobs/аудита — поэтому он здесь один на всех.
PERIOD_SENTINEL_SQL = "TO_DATE('1900-01-01', 'YYYY-MM-DD')"
PERIOD_SENTINEL = datetime.date(1900, 1, 1)


def periodBind(period):
    """Значение периода для бинда: NULL-группа уезжает сентинелом."""
    return PERIOD_SENTINEL if period is None else period



def _q(relative):
    return TakeOneQuery(f"{FULL_PATH}etlFolder/queries/{relative}")


# --- проверка структуры ---
structureCheckPostSql = _q("general/StructureCheckPost.sql")
structureCheckOrclSql = _q("general/StructureCheckOrcl.sql")
structureEmptyQuerySql = _q("general/StructureEmptyQuery.sql")

# --- newEtl: выбор групп / id ---
selectEtlIudPostSql = _q("general/newEtl/SelectEtlIudPost.sql")
selectEtlIudOrclSql = _q("general/newEtl/SelectEtlIudOrcl.sql")
selectDistinctPostSql = _q("general/newEtl/SelectDistinctPost.sql")
selectDistinctOrclSql = _q("general/newEtl/SelectDistinctOrcl.sql")

# --- newEtl: запись в ведомую ---
upsertPostSql = _q("general/newEtl/UpsertPost.sql")
upsertOrclSql = _q("general/newEtl/UpsertOrcl.sql")
deleteByIdPostSql = _q("general/newEtl/DeleteByIdPost.sql")
deleteByIdOrclSql = _q("general/newEtl/DeleteByIdOrcl.sql")
deletePeriodPostSql = _q("general/newEtl/DeletePeriodPost.sql")
deletePeriodOrclSql = _q("general/newEtl/DeletePeriodOrcl.sql")
insertPostSql = _q("general/newEtl/InsertPost.sql")
insertOrclSql = _q("general/newEtl/InsertOrcl.sql")
slavePeriodsByIdPostSql = _q("general/newEtl/SlavePeriodsByIdPost.sql")
slavePeriodsByIdOrclSql = _q("general/newEtl/SlavePeriodsByIdOrcl.sql")

# --- newEtl: выборка из ведущей ---
recordSelectByIdPostSql = _q("general/newEtl/RecordSelectByIdPost.sql")
recordSelectByIdOrclSql = _q("general/newEtl/RecordSelectByIdOrcl.sql")
recordSelectGroupPostSql = _q("general/newEtl/RecordSelectGroupPost.sql")
recordSelectGroupOrclSql = _q("general/newEtl/RecordSelectGroupOrcl.sql")

# --- newEtl: section_compare (mocheck/medree) ---
dateSelectMasterPostSql = _q("general/newEtl/DateSelectMasterPost.sql")
dateSelectMasterOrclSql = _q("general/newEtl/DateSelectMasterOrcl.sql")
dateSelectSlavePostSql = _q("general/newEtl/DateSelectSlavePost.sql")
dateSelectSlaveOrclSql = _q("general/newEtl/DateSelectSlaveOrcl.sql")
periodsFromIudPostSql = _q("general/newEtl/PeriodsFromIudPost.sql")
periodsFromIudOrclSql = _q("general/newEtl/PeriodsFromIudOrcl.sql")
markPeriodIudPostSql = _q("general/newEtl/MarkPeriodIudPost.sql")
markPeriodIudOrclSql = _q("general/newEtl/MarkPeriodIudOrcl.sql")
periodsIsokAudit4PostSql = _q("general/newEtl/PeriodsIsokAudit4Post.sql")
periodsIsokAudit4OrclSql = _q("general/newEtl/PeriodsIsokAudit4Orcl.sql")

# --- newEtl: служебные обновления ---
etlIdUpdatePostSql = _q("general/newEtl/EtlIdUpdatePost.sql")
etlIdUpdateOrclSql = _q("general/newEtl/EtlIdUpdateOrcl.sql")
etlStatusChangePostSql = _q("general/newEtl/EtlStatusChangePost.sql")
etlStatusChangeOrclSql = _q("general/newEtl/EtlStatusChangeOrcl.sql")
etlUpdatePostSql = _q("general/newEtl/EtlUpdatePost.sql")
etlUpdateOrclSql = _q("general/newEtl/EtlUpdateOrcl.sql")
etlErrorPostSql = _q("general/newEtl/EtlErrorPost.sql")
etlErrorOrclSql = _q("general/newEtl/EtlErrorOrcl.sql")
#newDatesPostSql = _q("general/newEtl/NewDatesPost.sql")
#newDatesOrclSql = _q("general/newEtl/NewDatesOrcl.sql")
registerPeriodPostSql = _q("general/newEtl/RegisterPeriodPost.sql")
registerPeriodOrclSql = _q("general/newEtl/RegisterPeriodOrcl.sql")

# --- log ---
selectIdEtlPostSql = _q("general/log/SelectIdEtlPost.sql")
selectIdEtlOrclSql = _q("general/log/SelectIdEtlOrcl.sql")
selectByIdEtlPostSql = _q("general/log/SelectByIdEtlPost.sql")
selectByIdEtlOrclSql = _q("general/log/SelectByIdEtlOrcl.sql")
addLogPostSql = _q("general/log/AddLogPost.sql")
addLogOrclSql = _q("general/log/AddLogOrcl.sql")


# --- audit: проверка корректности переноса (do_audit) ---
getBadAuditPostSql = _q("general/audit/GetBadAuditPost.sql")
getBadAuditOrclSql = _q("general/audit/GetBadAuditOrcl.sql")
auditRecordsMasterPostSql = _q("general/audit/AuditRecordsMasterPost.sql")
auditRecordsMasterOrclSql = _q("general/audit/AuditRecordsMasterOrcl.sql")
auditRecordsSlavePostSql = _q("general/audit/AuditRecordsSlavePost.sql")
auditRecordsSlaveOrclSql = _q("general/audit/AuditRecordsSlaveOrcl.sql")
auditUpdatePostSql = _q("general/audit/AuditUpdatePost.sql")
auditUpdateOrclSql = _q("general/audit/AuditUpdateOrcl.sql")
