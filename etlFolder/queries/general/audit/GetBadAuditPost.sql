-- См. комментарий в GetBadAuditOrcl.sql — логика статусов isokaudit та же.
SELECT tablename, period
FROM etl_jobs
WHERE tablename = %(tablename)s
  AND (isokaudit IS NULL OR isokaudit NOT IN (1, 4))
ORDER BY period
