-- Завести группу, которой ещё нет. NULL-период — полноправная группа, поэтому
-- вставляется как есть (CAST задаёт тип: без него драйвер может послать None
-- строкой), а сравнение идёт через COALESCE-сентинел — `period = NULL` не
-- истинно никогда, и группа заводилась бы заново каждый прогон.
INSERT INTO koknaev.etl_jobs (tablename, period, last_success_ts)
SELECT :tablename, CAST(:period AS DATE), NULL FROM dual
WHERE NOT EXISTS (
    SELECT 1 FROM koknaev.etl_jobs
    WHERE tablename = :tablename
      AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = :period_cmp
)
