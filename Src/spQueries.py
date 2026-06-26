from Functions.functionsFile.takeOneQuery import TakeOneQuery
from Src.fullPath import FULL_PATH

selectAllSpSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/SelectAllSp.sql')
deleteAllSpSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/DeleteAllSp.sql')
spSelectPostSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/SpSelectPost.sql')
spSelectOrclSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/SpSelectOrcl.sql')
spEtlJobsPostSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/SpEtlJobsPost.sql')
spEtlJobsOrclSql = TakeOneQuery(f'{FULL_PATH}etlFolder/queries/general/sp/SpEtlJobsOrcl.sql')