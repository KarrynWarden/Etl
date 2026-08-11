UPDATE koknaev.etl_log_iud_row SET isetl = 1
WHERE isetl = 0
  AND tablename = :tablename
  AND COALESCE(period, TO_DATE('1900-01-01', 'YYYY-MM-DD')) = :period
  AND idrw <= :idrwBefore
