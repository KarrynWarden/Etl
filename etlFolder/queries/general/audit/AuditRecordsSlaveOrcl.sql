-- Записи ведомой таблицы за один период.
-- {period_cond} содержит :createdate и, при наличии, filterClauseSlave
-- (тот же doctype-срез, что и при переносе).
SELECT {fields_str}
FROM {tablename} p
WHERE {period_cond}
