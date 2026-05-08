UPDATE etl_log_iud_row SET iseth = 1
WHERE iseth = 0
  AND tablename = :tablename
  AND period = :period
  AND idrw <= :idrwBefore
