SELECT DISTINCT ON (period, id) id, period, timeoper, oper
FROM etl_log_iud_row
WHERE idrw = ANY(%(idlogs)s)
ORDER BY period, id, timeoper DESC
