SELECT last_success_ts
FROM etl_jobs
WHERE tablename = %(tablename)s AND isokaudit = 0