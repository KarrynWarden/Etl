INSERT INTO koknaev.etl_jobs (tablename, period, last_success_ts)
SELECT :tablename, :period, NULL FROM dual
WHERE NOT EXISTS (
    SELECT 1 FROM koknaev.etl_jobs
    WHERE tablename = :tablename AND period = :period
)
