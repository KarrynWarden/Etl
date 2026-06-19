INSERT INTO etl_jobs (tablename, period, last_success_ts)
SELECT %(tablename)s, %(period)s, NULL::TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1 FROM etl_jobs
    WHERE tablename = %(tablename)s AND period = %(period)s
)
