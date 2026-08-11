-- Завести группу, которой ещё нет. NULL-период — полноправная группа, поэтому
-- вставляется как есть (CAST задаёт тип: без него драйвер может послать None
-- строкой), а сравнение идёт через COALESCE-сентинел — `period = NULL` не
-- истинно никогда, и группа заводилась бы заново каждый прогон.
INSERT INTO etl_jobs (tablename, period, last_success_ts)
SELECT %(tablename)s, CAST(%(period)s AS DATE), NULL WHERE NOT EXISTS (
    SELECT 1 FROM etl_jobs
    WHERE tablename = %(tablename)s
      AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = %(period_cmp)s
)
