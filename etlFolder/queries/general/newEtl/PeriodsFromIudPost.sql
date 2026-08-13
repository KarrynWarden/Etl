-- См. PeriodsFromIudOrcl.sql: граница idrw та же, что у пометки журнала.
SELECT DISTINCT period FROM etl_log_iud_row
WHERE isetl = 0 AND tablename = %(tablename)s AND idrw <= %(idrwBefore)s
