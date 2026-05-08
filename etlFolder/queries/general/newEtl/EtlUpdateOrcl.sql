UPDATE etl_jobs
SET last_success_ts = :LAST_SUCCESS_TS, isokaudit = 0
WHERE tablename = :TABLENAME AND period = :PERIOD
  AND (isokaudit != -1 OR isokaudit IS NULL)
