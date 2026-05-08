SELECT period FROM etl_jobs
WHERE tablename = %(tablename)s AND isokaudit = 4 AND period IS NOT NULL
