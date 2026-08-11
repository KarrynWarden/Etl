-- Срез ведомой: то же, что у ведущей (период, lastupdate, счётчик пары).
SELECT {period_expr} AS createdate, lastupdate AS lastupdate, COUNT(*) AS etl_cnt
FROM {tablename}
WHERE {filter}
GROUP BY {period_expr}, lastupdate
