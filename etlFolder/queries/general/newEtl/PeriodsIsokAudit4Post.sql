-- См. комментарий в PeriodsIsokAudit4Orcl.sql.
SELECT period FROM etl_jobs
WHERE tablename = %(tablename)s AND isokaudit = 4
