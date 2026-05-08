SELECT id, idrw
FROM etl_log_iud_row
WHERE iseth = 0 AND tablename = :tablename
ORDER BY idrw
