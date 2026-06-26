SELECT id, idrw, period, timeoper, oper
FROM koknaev.etl_log_iud_row
WHERE isetl = 0 AND tablename = :tablename
ORDER BY timeoper
