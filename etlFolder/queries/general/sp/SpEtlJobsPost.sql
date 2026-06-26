UPDATE etl_user.etl_jobs
SET isokaudit = 1, last_success_ts = %(lastUpdate)s
WHERE tablename = %(tablename)s