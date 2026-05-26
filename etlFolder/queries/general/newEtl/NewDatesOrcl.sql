INSERT INTO koknaev.etl_jobs (tablename, period, last_success_ts)
SELECT DISTINCT :tablename, createdate, NULL FROM (
    SELECT {1} AS createdate
    FROM ( {0} ) p
    WHERE {1} NOT IN (
        SELECT period FROM koknaev.etl_jobs
        WHERE period IS NOT NULL AND tablename = :tablename
    )
)
