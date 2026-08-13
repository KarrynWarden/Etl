-- См. MarkPeriodIudPost.sql.
UPDATE koknaev.etl_log_iud_row SET isetl = 1
WHERE isetl = 0
  AND tablename = :tablename
  AND {period_cond}
  AND idrw <= :idrwBefore
