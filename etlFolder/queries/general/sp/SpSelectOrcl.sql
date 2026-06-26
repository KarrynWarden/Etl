SELECT last_success_ts
FROM koknaev.etl_jobs
WHERE tablename = :tablename AND isokaudit = 0