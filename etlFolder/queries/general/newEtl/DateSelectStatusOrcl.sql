SELECT createdate, last_success_ts, SUM(total_count) total_count
FROM (
    SELECT p.{1} createdate, e.last_success_ts, COUNT(*) total_count, e.isokaudit
    FROM {0} p
    LEFT JOIN koknaev.etl_jobs e ON e.tablename = :tablename AND e.period = p.{1}
    WHERE e.isokaudit = 4
    GROUP BY p.{1}, e.last_success_ts, e.isokaudit
) sub
GROUP BY createdate, last_success_ts, isokaudit
