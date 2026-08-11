UPDATE koknaev.etl_jobs
SET last_success_ts = :LAST_SUCCESS_TS, isokaudit = 0
WHERE tablename = :TABLENAME AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = :PERIOD
  AND (isokaudit != -1 OR isokaudit IS NULL)
