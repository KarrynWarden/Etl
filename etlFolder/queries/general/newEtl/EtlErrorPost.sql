UPDATE etl_jobs
SET isokaudit = %(ISOKAUDIT)s
WHERE tablename = %(tablename)s AND period = %(PERIOD)s
