SELECT DISTINCT period FROM koknaev.etl_log_iud_row
WHERE isetl = 0 AND tablename = :tablename
