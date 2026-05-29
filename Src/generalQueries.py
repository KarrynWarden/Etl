"""Загрузка SQL-запросов из файлов.

Все запросы — в .sql файлах (никаких многострочных f-строк в коде Python),
чтобы их можно было открывать в DBeaver и т.п. без правок.
"""
from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH, MODE


def _q(relative):
    return TakeOneQuery(f"{FULL_PATH}etlFolder{MODE}/queries/{relative}")


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
newDatesPostSql = _q("general/newEtl/NewDatesPost.sql")
newDatesOrclSql = _q("general/newEtl/NewDatesOrcl.sql")

# --- log ---
selectIdEtlPostSql = _q("general/log/SelectIdEtlPost.sql")
selectIdEtlOrclSql = _q("general/log/SelectIdEtlOrcl.sql")
selectByIdEtlPostSql = _q("general/log/SelectByIdEtlPost.sql")
selectByIdEtlOrclSql = _q("general/log/SelectByIdEtlOrcl.sql")
addLogPostSql = _q("general/log/AddLogPost.sql")
addLogOrclSql = _q("general/log/AddLogOrcl.sql")
