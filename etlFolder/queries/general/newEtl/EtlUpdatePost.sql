UPDATE etl_jobs
SET last_success_ts = %(LAST_SUCCESS_TS)s, isokaudit = 0
WHERE tablename = %(TABLENAME)s AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = %(PERIOD)s
  AND (isokaudit != -1 OR isokaudit IS NULL)
