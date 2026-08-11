UPDATE koknaev.etl_jobs
SET isokaudit = :ISOKAUDIT, last_success_ts = NULL
WHERE tablename = :tablename AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = :PERIOD
