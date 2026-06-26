UPDATE Koknaev.etl_jobs
SET isokaudit = 1, last_success_ts = :lastUpdate
WHERE tablename = :tablename