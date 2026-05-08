UPDATE etl_jobs
SET last_success_ts = %(LAST_SUCCESS_TS)s, isokaudit = 0
WHERE tablename = %(TABLENAME)s AND period = %(PERIOD)s
  AND (isokaudit != -1 OR isokaudit IS NULL)
