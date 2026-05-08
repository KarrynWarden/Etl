SELECT DISTINCT period FROM etl_log_iud_row
WHERE iseth = 0 AND tablename = %(tablename)s
