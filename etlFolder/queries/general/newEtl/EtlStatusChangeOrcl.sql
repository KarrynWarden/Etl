UPDATE etl_log_iud_row SET isetl = 1
WHERE isetl = 0 AND period = :createdate AND tablename = :tablename
