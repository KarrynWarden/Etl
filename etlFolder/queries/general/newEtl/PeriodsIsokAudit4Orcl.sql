SELECT period FROM koknaev.etl_jobs
WHERE tablename = :tablename AND isokaudit = 4 AND period IS NOT NULL
