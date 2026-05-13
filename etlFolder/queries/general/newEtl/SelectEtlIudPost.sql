SELECT id, idrw
FROM etl_log_iud_row
WHERE isetl = 0 AND tablename = %(tablename)s
ORDER BY idrw
